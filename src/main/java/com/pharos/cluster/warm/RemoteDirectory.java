package com.pharos.cluster.warm;

import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A read-only Lucene {@link org.apache.lucene.store.Directory} over the Lucene files of one published
 * generation. The file list and lengths come from the manifest. A file present in {@code eagerDir}
 * (the small files downloaded when the generation was opened) is read from there; every other file
 * is a {@link CachingRemoteIndexInput} reading blocks through the shared {@link BlockCache}.
 *
 * <p>The directory owns neither the cache nor the source: closing it only closes the eager files'
 * {@link FSDirectory}. Every write, delete, sync or rename throws {@link UnsupportedOperationException}.
 */
public final class RemoteDirectory extends BaseDirectory {

    static final String READ_ONLY = "read-only remote directory";

    private final ShardId shard;
    private final long generation;
    private final Map<String, ManifestFile> files;
    private final Path eagerDir;
    private final BlockCache cache;
    private final ReplicaSource source;
    private FSDirectory eager; // guarded by this; opened on first use

    /**
     * @param eagerDir directory holding the eagerly downloaded files (it may be missing or partial);
     *                 null when every file is read remotely
     */
    public RemoteDirectory(ShardId shard, ShardManifest manifest, Path eagerDir, BlockCache cache,
                           ReplicaSource source) {
        super(NoLockFactory.INSTANCE);
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
            long size = Files.size(local);
            if (size != f.length()) {
                throw new IOException("eager copy " + local + " of shard " + shard + " generation " + generation
                        + " file " + name + " (object key " + f.key() + ") has " + size + " bytes, the manifest says "
                        + f.length());
            }
            return eager().openInput(name, context);
        }
        CachingRemoteIndexInput.RemoteFile remote =
                new CachingRemoteIndexInput.RemoteFile(shard, generation, f, cache, source);
        return new CachingRemoteIndexInput("CachingRemoteIndexInput(" + remote.where() + ")", remote);
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
