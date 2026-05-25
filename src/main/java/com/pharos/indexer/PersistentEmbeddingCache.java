package com.pharos.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Persistent, project-scoped cache mapping SHA-256(embeddingText) → float[] vector.
 *
 * <p>Eliminates redundant ONNX forward passes during re-indexing when the embedding
 * input text has not changed. This is the common case for single-chunk methods when
 * only the chunking version or an unrelated field has changed.
 *
 * <h3>Files written</h3>
 * <ul>
 *   <li>{@code embed-cache.meta} — JSON: model fingerprint + dims (invalidation guard)</li>
 *   <li>{@code embed-cache.bin}  — binary: [int32 count][entry...] where each entry is
 *       [32 bytes SHA-256 key][dims × float32]</li>
 * </ul>
 *
 * <p>The entire cache is held in memory. For typical projects (≤ 50 k methods, 768 dims)
 * this is ≈ 150 MB — acceptable given that the embedding model itself already requires
 * several hundred MB of heap.
 */
public class PersistentEmbeddingCache {

    private static final Logger log = LoggerFactory.getLogger(PersistentEmbeddingCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path dataFile;
    private final Path metaFile;
    private final String fingerprint;
    private final int dims;
    /**
     * Concurrent so {@link #get} and {@link #put} can be called from worker
     * threads in the parallel embedding pipeline without external locking.
     */
    private final Map<String, float[]> cache;

    private final LongAdder hits   = new LongAdder();
    private final LongAdder misses = new LongAdder();

    public PersistentEmbeddingCache(Path projectIndexDir, String fingerprint, int dims) {
        this.dataFile    = projectIndexDir.resolve("embed-cache.bin");
        this.metaFile    = projectIndexDir.resolve("embed-cache.meta");
        this.fingerprint = fingerprint;
        this.dims        = dims;
        // Load into a ConcurrentHashMap so subsequent reads/writes are thread-safe.
        this.cache       = new ConcurrentHashMap<>(load());
    }

    /** Returns the cached embedding for the given text hash, or {@code null} on a miss. */
    public float[] get(String textHash) {
        float[] v = cache.get(textHash);
        if (v != null) hits.increment(); else misses.increment();
        return v;
    }

    /** Stores an embedding keyed by the text hash. Write is lazy — call {@link #save()} to persist. */
    public void put(String textHash, float[] embedding) {
        cache.put(textHash, embedding);
    }

    public int hits()   { return (int) hits.sum(); }
    public int misses() { return (int) misses.sum(); }
    public int size()   { return cache.size(); }

    /** Atomically persists the in-memory cache to disk. */
    public void save() {
        try {
            Files.createDirectories(metaFile.getParent());

            Map<String, Object> meta = Map.of("fingerprint", fingerprint, "dims", dims);
            Path tmpMeta = sibling(metaFile, ".tmp");
            MAPPER.writeValue(tmpMeta.toFile(), meta);
            atomicMove(tmpMeta, metaFile);

            Path tmpData = sibling(dataFile, ".tmp");
            try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
                    Files.newOutputStream(tmpData), 1 << 16))) {
                dos.writeInt(cache.size());
                for (Map.Entry<String, float[]> e : cache.entrySet()) {
                    dos.write(hexToBytes(e.getKey()));
                    for (float f : e.getValue()) dos.writeFloat(f);
                }
            }
            atomicMove(tmpData, dataFile);
        } catch (IOException e) {
            log.warn("Failed to save embedding cache: {}", e.getMessage());
        }
    }

    // ── Static helpers ─────────────────────────────────────────────────────────

    /** SHA-256 hex digest of the UTF-8 encoding of {@code text}. */
    public static String sha256Hex(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(text.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    // ── Private ────────────────────────────────────────────────────────────────

    private Map<String, float[]> load() {
        Map<String, float[]> result = new HashMap<>();
        if (!Files.exists(metaFile) || !Files.exists(dataFile)) return result;
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> meta = MAPPER.readValue(metaFile.toFile(), Map.class);
            String storedFp  = (String) meta.get("fingerprint");
            int    storedDims = ((Number) meta.get("dims")).intValue();
            if (!fingerprint.equals(storedFp) || dims != storedDims) {
                log.info("Embedding cache invalidated: model or dims changed");
                Files.deleteIfExists(dataFile);
                Files.deleteIfExists(metaFile);
                return result;
            }
            try (DataInputStream dis = new DataInputStream(new BufferedInputStream(
                    Files.newInputStream(dataFile), 1 << 16))) {
                int count = dis.readInt();
                for (int i = 0; i < count; i++) {
                    byte[] keyBytes = new byte[32];
                    dis.readFully(keyBytes);
                    float[] floats = new float[dims];
                    for (int j = 0; j < dims; j++) floats[j] = dis.readFloat();
                    result.put(bytesToHex(keyBytes), floats);
                }
            }
            log.debug("Loaded {} embedding cache entries", result.size());
        } catch (IOException e) {
            log.warn("Could not load embedding cache: {}", e.getMessage());
        }
        return result;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    private static byte[] hexToBytes(String hex) {
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    private static Path sibling(Path path, String suffix) {
        return path.resolveSibling(path.getFileName() + suffix);
    }

    private static void atomicMove(Path src, Path dst) throws IOException {
        Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
