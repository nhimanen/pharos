package com.pharos.cluster.warm;

import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.SyncLock;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.config.ClusterConfig;
import com.pharos.indexer.LuceneDirectoryFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A reader's lazy generations (cluster.reader.mode {@code lazy}): which Lucene files are eager, the
 * {@link LuceneDirectoryFactory} that serves a lazy generation dir as a {@link RemoteDirectory}, the
 * prefetch of a new generation, and the shared {@link BlockCache}.
 *
 * <p><b>Lazy generation dir.</b> {@code <cacheDir>/<shard>/gen-N} holds {@code manifest.json},
 * {@code index-format.json}, the compact graph, the {@value #MARKER} marker and {@code lucene/} with
 * only the eager files: {@code segments_N}, {@code *.si}, {@code *.fnm}, {@code *.cfe}, and any Lucene
 * file of at most {@code eagerMaxBytes}. {@link #open} returns a {@link RemoteDirectory} for the
 * {@code lucene/} dir of such a generation (its manifest read from {@code manifest.json}), and an
 * {@link FSDirectory} for every other dir (full-mode generations, writer dirs).
 *
 * <p><b>Block cache.</b> Created on first use at {@code <cacheDir>/}{@value #BLOCKS_DIR} by the
 * process holding the {@value #BLOCKS_LOCK} lock on the cache dir. Another process on the same cache
 * dir (a follower, a one-shot command) gets a private cache at
 * {@code <cacheDir>/}{@value #PRIVATE_BLOCKS_DIR}{@code /<pid>-<random>}, deleted when it closes; the
 * lock holder deletes private dirs of processes that are gone. Neither name can be a shard id (those
 * start with a letter or digit), so no shard dir can collide with them.
 */
public final class LazyGenerations implements LuceneDirectoryFactory, Closeable {

    private static final Logger log = LoggerFactory.getLogger(LazyGenerations.class);

    /** Marks a generation dir as lazy: only the eager Lucene files are local. */
    public static final String MARKER = ".lazy";
    public static final String BLOCKS_DIR = ".blocks";
    public static final String PRIVATE_BLOCKS_DIR = ".blocks-private";
    static final String BLOCKS_LOCK = ".blocks.lock";
    private static final String MANIFEST_FILE = "manifest.json";
    private static final Pattern PRIVATE_NAME = Pattern.compile("(\\d{1,19})-[0-9a-f]{8}");
    private static final int MANIFEST_CACHE_SIZE = 64;

    private final Path cacheDir;
    private final ClusterConfig.ReaderConfig config;
    private final ReplicaSource source;
    private final Set<String> prefetchExtensions;

    /** Parsed manifests by generation dir; a changed manifest file (a new lineage) is re-read. */
    private final Map<Path, CachedManifest> manifests = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Path, CachedManifest> eldest) {
            return size() > MANIFEST_CACHE_SIZE;
        }
    };

    private record CachedManifest(Object fileKey, long modified, long size, ShardManifest manifest) {}

    // Guarded by this.
    private BlockCache blockCache;
    private SyncLock blocksLock;
    private Path privateDir;
    private boolean closed;

    public LazyGenerations(Path cacheDir, ClusterConfig.ReaderConfig config, ReplicaSource source) {
        this.cacheDir = Objects.requireNonNull(cacheDir, "cacheDir").toAbsolutePath();
        this.config = Objects.requireNonNull(config, "config");
        this.source = Objects.requireNonNull(source, "source");
        this.prefetchExtensions = Set.copyOf(config.getPrefetchExtensions());
    }

    /** True when {@code genDir} is a lazy generation dir (it has the {@value #MARKER} marker). */
    public static boolean isLazy(Path genDir) {
        return genDir != null && Files.exists(genDir.resolve(MARKER));
    }

    /** True when a lazy generation keeps this Lucene file on local disk. */
    public boolean eager(ManifestFile f) {
        String name = f.name();
        return name.startsWith("segments_") || name.endsWith(".si") || name.endsWith(".fnm")
                || name.endsWith(".cfe") || f.length() <= config.getEagerMaxBytes();
    }

    /** How long a sync waits for {@link #prefetch} before activating the generation anyway. */
    public Duration prefetchTimeout() {
        return Duration.ofSeconds(config.getPrefetchTimeoutSeconds());
    }

    // ---------------------------------------------------------------- directory factory

    @Override
    public Directory open(Path luceneDir) throws IOException {
        Path genDir = luceneDir.toAbsolutePath().getParent();
        if (!isLazy(genDir)) return FSDirectory.open(luceneDir);
        ShardManifest manifest = manifest(genDir);
        return new RemoteDirectory(new ShardId(manifest.shard()), manifest, luceneDir, blockCache(), source, true);
    }

    /** A lazy generation exists when its manifest names a commit whose {@code segments_N} is on disk. */
    @Override
    public boolean indexExists(Path luceneDir) throws IOException {
        Path genDir = luceneDir.toAbsolutePath().getParent();
        if (!isLazy(genDir)) return LuceneDirectoryFactory.super.indexExists(luceneDir);
        for (ManifestFile f : manifest(genDir).files()) {
            if (ManifestFile.LUCENE.equals(f.role()) && f.name().startsWith("segments_")
                    && Files.isRegularFile(luceneDir.resolve(f.name()))) {
                return true;
            }
        }
        return false;
    }

    private ShardManifest manifest(Path genDir) throws IOException {
        Path file = genDir.resolve(MANIFEST_FILE);
        BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
        Object fileKey = attrs.fileKey();
        long modified = attrs.lastModifiedTime().toMillis();
        synchronized (manifests) {
            CachedManifest c = manifests.get(genDir);
            if (c != null && Objects.equals(c.fileKey(), fileKey) && c.modified() == modified
                    && c.size() == attrs.size()) {
                return c.manifest();
            }
        }
        ShardManifest m;
        try {
            m = ShardManifest.read(Files.readAllBytes(file));
        } catch (IOException e) {
            throw new IOException("unreadable manifest " + file + " of a lazy generation: " + e.getMessage(), e);
        }
        synchronized (manifests) {
            manifests.put(genDir, new CachedManifest(fileKey, modified, attrs.size(), m));
        }
        return m;
    }

    // ---------------------------------------------------------------- prefetch

    /**
     * Starts, on the cache's prefetch pool: the download of every remote (not eager) Lucene file's
     * head sidecar into {@code luceneDir} (see {@link RemoteDirectory#headFile}), and the fetch of
     * every block of the remote files whose extension is one of {@code prefetchExtensions}. The
     * future completes when all are done (blocks cached or skipped), exceptionally when one fails.
     * A head is written to a temp file and moved into place, so a sidecar is complete or absent.
     */
    public Prefetch prefetch(ShardManifest manifest, Path luceneDir) throws IOException {
        ShardId shard = new ShardId(manifest.shard());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        long bytes = 0;
        int blocks = 0;
        BlockCache cache = null;
        Path edges = luceneDir.resolve(RemoteDirectory.EDGES_DIR);
        for (ManifestFile f : manifest.files()) {
            if (!ManifestFile.LUCENE.equals(f.role()) || eager(f) || f.length() == 0) continue;
            if (cache == null) {
                cache = blockCache();
                Files.createDirectories(edges); // tasks never create it: a moved partial dir stays gone
            }
            futures.add(cache.runOnPrefetchPool(() -> downloadHead(f, luceneDir)));
            String name = f.name();
            if (!prefetchExtensions.contains(name.substring(name.lastIndexOf('.') + 1))) continue;
            CachingRemoteIndexInput.RemoteFile remote =
                    new CachingRemoteIndexInput.RemoteFile(shard, manifest.generation(), f, cache, source);
            List<CompletableFuture<Void>> fileBlocks = remote.prefetchAll();
            futures.addAll(fileBlocks);
            blocks += fileBlocks.size();
            bytes += f.length();
        }
        return new Prefetch(CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)),
                futures.size() - blocks, blocks, bytes);
    }

    private void downloadHead(ManifestFile f, Path luceneDir) throws IOException {
        Path target = RemoteDirectory.headFile(luceneDir, f.name());
        int len = (int) Math.min(RemoteDirectory.HEAD_BYTES, f.length());
        byte[] head;
        try (java.io.InputStream in = source.openRange(f, 0, len)) {
            head = in.readNBytes(len);
        }
        if (head.length != len) {
            throw new IOException("short head of " + f.key() + ": " + head.length + " of " + len + " bytes");
        }
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            Files.write(tmp, head);
            Files.move(tmp, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /**
     * A started prefetch: {@code done} completes when its {@code heads} head sidecars and its
     * {@code blocks} blocks ({@code bytes} bytes of files) are in.
     */
    public record Prefetch(CompletableFuture<Void> done, int heads, int blocks, long bytes) {}

    // ---------------------------------------------------------------- block cache

    /** The shared block cache, created on first use (see the class javadoc for where). */
    public synchronized BlockCache blockCache() throws IOException {
        if (closed) throw new IllegalStateException("lazy generations of " + cacheDir + " are closed");
        if (blockCache != null) return blockCache;
        Optional<SyncLock> lock = SyncLock.tryAcquire(cacheDir, BLOCKS_LOCK);
        Path dir;
        if (lock.isPresent()) {
            dir = cacheDir.resolve(BLOCKS_DIR);
            deleteStalePrivateDirs();
        } else {
            Path parent = Files.createDirectories(cacheDir.resolve(PRIVATE_BLOCKS_DIR));
            dir = parent.resolve(ProcessHandle.current().pid() + "-" + UUID.randomUUID().toString().substring(0, 8));
            log.info("Another process uses the block cache {}; using the private block cache {} until this one exits",
                    cacheDir.resolve(BLOCKS_DIR), dir);
        }
        try {
            blockCache = new BlockCache(dir, config.getCacheMaxBytes(), Math.toIntExact(config.getBlockSizeBytes()),
                    config.getMaxConcurrentFetches());
        } catch (IOException | RuntimeException e) {
            if (lock.isPresent()) lock.get().close();
            throw e;
        }
        blocksLock = lock.orElse(null);
        privateDir = lock.isPresent() ? null : dir;
        return blockCache;
    }

    /** The block cache's dir; null before its first use. */
    public synchronized Path blockCacheDir() {
        return blockCache == null ? null : blockCache.dir();
    }

    /** Private block caches of processes that no longer run. */
    private void deleteStalePrivateDirs() {
        Path parent = cacheDir.resolve(PRIVATE_BLOCKS_DIR);
        if (!Files.isDirectory(parent)) return;
        long self = ProcessHandle.current().pid();
        try (Stream<Path> s = Files.list(parent)) {
            for (Path p : s.toList()) {
                Matcher m = PRIVATE_NAME.matcher(p.getFileName().toString());
                if (!m.matches()) continue;
                long pid = Long.parseLong(m.group(1));
                if (pid == self || ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) continue;
                log.info("Deleting the private block cache {} of a process that is gone", p);
                deleteRecursively(p);
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Cleaning up private block caches in {} failed: {}", parent, e.toString());
        }
    }

    /** Closes the block cache (a private one is deleted) and releases the cache dir's block lock. */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        if (blockCache != null) blockCache.close();
        if (privateDir != null) {
            try {
                deleteRecursively(privateDir);
            } catch (IOException e) {
                log.warn("Deleting the private block cache {} failed: {}", privateDir, e.toString());
            }
        }
        if (blocksLock != null) {
            try {
                blocksLock.close();
            } catch (IOException e) {
                log.warn("Releasing {} failed: {}", cacheDir.resolve(BLOCKS_LOCK), e.toString());
            }
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        List<Path> paths;
        try (Stream<Path> s = Files.walk(dir)) {
            paths = s.sorted(Comparator.reverseOrder()).toList();
        }
        for (Path p : paths) Files.deleteIfExists(p);
    }
}
