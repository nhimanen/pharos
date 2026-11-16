package com.pharos.cluster.warm;

import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A read-only Lucene {@link org.apache.lucene.store.Directory} over the Lucene files of one published
 * generation. The file list and lengths come from the manifest. A file present in {@code eagerDir}
 * (the small files downloaded when the generation was opened) is read from there; every other file
 * is a {@link CachingRemoteIndexInput} reading blocks through the shared {@link BlockCache}. An eager
 * copy whose size is not the manifest's is downloaded again (with a WARN) before it is opened; the
 * download replaces it only once its whole-file checksum matches the manifest.
 *
 * <p>The directory owns neither the cache nor the source: closing it only closes the eager files'
 * {@link FSDirectory}. Every write, delete, sync or rename throws {@link UnsupportedOperationException}.
 */
public final class RemoteDirectory extends BaseDirectory {

    private static final Logger log = LoggerFactory.getLogger(RemoteDirectory.class);

    static final String READ_ONLY = "read-only remote directory";

    private final ShardId shard;
    private final long generation;
    private final Map<String, ManifestFile> files;
    private final Path eagerDir;
    private final BlockCache cache;
    private final ReplicaSource source;
    /** Serve heads from sidecars and footers from the manifest (lazy generations). */
    private final boolean edges;
    private FSDirectory eager; // guarded by this; opened on first use

    /**
     * @param eagerDir directory holding the eagerly downloaded files (it may be missing or partial);
     *                 null when every file is read remotely
     */
    public RemoteDirectory(ShardId shard, ShardManifest manifest, Path eagerDir, BlockCache cache,
                           ReplicaSource source) {
        this(shard, manifest, eagerDir, cache, source, false);
    }

    /**
     * @param edges serve each remote file's first bytes from its head sidecar
     *              ({@code <eagerDir>/}{@value #EDGES_DIR}{@code /<name>.head}, when present with the
     *              right length) and its last 16 bytes from a footer built from the manifest checksum
     *              (only for Lucene files: the manifest's checksum must be the footer's)
     */
    public RemoteDirectory(ShardId shard, ShardManifest manifest, Path eagerDir, BlockCache cache,
                           ReplicaSource source, boolean edges) {
        super(NoLockFactory.INSTANCE);
        this.edges = edges;
        this.shard = Objects.requireNonNull(shard, "shard");
        this.generation = manifest.generation();
        this.eagerDir = eagerDir;
        this.cache = Objects.requireNonNull(cache, "cache");
        this.source = Objects.requireNonNull(source, "source");
        Map<String, ManifestFile> byName = new TreeMap<>();
        for (ManifestFile f : manifest.files()) {
            if (!ManifestFile.LUCENE.equals(f.role())) continue;
            if (byName.put(f.name(), f) != null) {
                throw new IllegalArgumentException("manifest of shard " + shard + " generation " + generation
                        + " lists Lucene file " + f.name() + " twice");
            }
        }
        this.files = byName;
    }

    @Override
    public String[] listAll() {
        ensureOpen();
        return files.keySet().toArray(String[]::new); // TreeMap: sorted, as Lucene expects
    }

    @Override
    public long fileLength(String name) throws IOException {
        return file(name).length();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ManifestFile f = file(name);
        Path local = eagerDir == null ? null : eagerDir.resolve(name);
        if (local != null && Files.isRegularFile(local)) {
            if (Files.size(local) != f.length()) redownload(local, f);
            return eager().openInput(name, context);
        }
        CachingRemoteIndexInput.RemoteFile remote = new CachingRemoteIndexInput.RemoteFile(shard, generation, f,
                cache, source, edges ? head(f) : null, edges);
        return new CachingRemoteIndexInput("CachingRemoteIndexInput(" + remote.where() + ")", remote);
    }

    /**
     * Replaces an eager copy whose size is not the manifest's (a torn write, tampering) with a fresh
     * download: written next to it, length-checked, then moved over it atomically. Inputs already
     * open on the old copy keep reading it (on Linux and macOS a replaced file stays readable).
     */
    private synchronized void redownload(Path local, ManifestFile f) throws IOException {
        long size = Files.size(local);
        if (size == f.length()) return; // another thread repaired it meanwhile
        String where = "shard " + shard + " generation " + generation + " file " + f.name() + " (object key "
                + f.key() + ")";
        log.warn("eager copy {} of {} has {} bytes, the manifest says {}; downloading it again",
                local, where, size, f.length());
        Path tmp = local.resolveSibling(f.name() + ".redownload.tmp");
        try {
            Files.deleteIfExists(tmp);
            source.download(f, tmp);
            long got = Files.size(tmp);
            if (got != f.length()) {
                throw new IOException("downloaded " + got + " bytes, the manifest says " + f.length());
            }
            verifyChecksum(tmp, f);
            Files.move(tmp, local, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException | RuntimeException e) {
            throw new IOException("cannot repair the eager copy " + local + " of " + where + ": " + e.getMessage(), e);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /** Sidecar dir of the head files, inside the eager dir (invisible to Lucene: listAll is the manifest). */
    public static final String EDGES_DIR = ".edges";
    /** Bytes of a file kept in its head sidecar (Lucene's header is far shorter). */
    public static final int HEAD_BYTES = 4096;

    /** The head sidecar of file {@code name} in {@code eagerDir}. */
    public static Path headFile(Path eagerDir, String name) {
        return eagerDir.resolve(EDGES_DIR).resolve(name + ".head");
    }

    /** The head sidecar's bytes when it has exactly the expected length; null otherwise. */
    private byte[] head(ManifestFile f) {
        if (eagerDir == null) return null;
        Path p = headFile(eagerDir, f.name());
        try {
            if (!Files.isRegularFile(p) || Files.size(p) != Math.min(HEAD_BYTES, f.length())) return null;
            return Files.readAllBytes(p);
        } catch (IOException e) {
            return null; // read through the cache instead
        }
    }

    /** The whole file is checksummed against its footer, and the footer's checksum must be the manifest's. */
    private static void verifyChecksum(Path file, ManifestFile f) throws IOException {
        long actual;
        try (FSDirectory dir = FSDirectory.open(file.getParent());
             IndexInput in = dir.openInput(file.getFileName().toString(), IOContext.READONCE)) {
            actual = CodecUtil.checksumEntireFile(in);
        } catch (org.apache.lucene.index.CorruptIndexException e) {
            throw new IOException("the downloaded copy fails its checksum: " + e.getMessage(), e);
        }
        long expected;
        try {
            expected = Long.parseUnsignedLong(Objects.requireNonNull(f.checksum(), "checksum"), 16);
        } catch (NumberFormatException | NullPointerException e) {
            throw new IOException("invalid checksum '" + f.checksum() + "' in the manifest");
        }
        if (actual != expected) {
            throw new IOException("the downloaded copy has checksum " + Long.toHexString(actual)
                    + ", the manifest says " + f.checksum());
        }
    }

    private ManifestFile file(String name) throws NoSuchFileException {
        ensureOpen();
        ManifestFile f = files.get(name);
        if (f == null) {
            throw new NoSuchFileException(name, null, "not a Lucene file of shard " + shard + " generation "
                    + generation);
        }
        return f;
    }

    private synchronized FSDirectory eager() throws IOException {
        ensureOpen();
        if (eager == null) eager = FSDirectory.open(eagerDir);
        return eager;
    }

    @Override
    public Set<String> getPendingDeletions() {
        return Set.of();
    }

    @Override
    public synchronized void close() throws IOException {
        isOpen = false;
        if (eager != null) {
            eager.close();
            eager = null;
        }
    }

    @Override
    public String toString() {
        return "RemoteDirectory(shard " + shard + " generation " + generation + ")";
    }

    // ---------------------------------------------------------------- read-only

    @Override
    public void deleteFile(String name) {
        throw new UnsupportedOperationException(READ_ONLY);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw new UnsupportedOperationException(READ_ONLY);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException(READ_ONLY);
    }

    @Override
    public void sync(Collection<String> names) {
        throw new UnsupportedOperationException(READ_ONLY);
    }

    @Override
    public void syncMetaData() {
        throw new UnsupportedOperationException(READ_ONLY);
    }

    @Override
    public void rename(String source, String dest) {
        throw new UnsupportedOperationException(READ_ONLY);
    }
}
