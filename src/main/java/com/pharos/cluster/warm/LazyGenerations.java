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
 * only the eager files: {@code segments_N}, {@code *.si}, {@code *.fnm}, {@code *.cfe}, any Lucene
 * file of at most {@code eagerMaxBytes} and any {@code .cfs} of at most one block. {@link #open} returns a {@link RemoteDirectory} for the
 * {@code lucene/} dir of such a generation (its manifest read from {@code manifest.json}), and an
 * {@link FSDirectory} for every other dir (full-mode generations, writer dirs).
 *
 * <p><b>Block cache.</b> Created on first use at {@code <cacheDir>/}{@value #BLOCKS_DIR} by the
 * process holding the {@value #BLOCKS_LOCK} lock on the cache dir. Another process on the same cache
 * dir (a follower, a one-shot command) gets a private cache at
 * {@code <cacheDir>/}{@value #PRIVATE_BLOCKS_DIR}{@code /<pid>-<random>}, capped at
 * {@code effectivePrivateCacheMaxBytes} and deleted when it closes; the lock holder deletes private
 * dirs of processes that are gone at startup and on every {@link #sweep}. The syncing reader takes
 * the shared cache as soon as it starts syncing ({@code ClusterRuntime}), so the process that
 * prefetches always fills the shared cache, and a process that began on a private cache moves to the
 * shared one ({@link #adoptSharedCache}) once its lock is free (the private one stays open until close).
 * Neither name can be a shard id (those
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
    /** Block keys of each shard's last prefetch, cancelled when the next one starts. */
    private final Map<String, List<BlockKey>> lastPrefetch = new java.util.HashMap<>();

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
    /** Private caches this process left for the shared one: retired readers may still use them. */
    private final List<BlockCache> retiredCaches = new ArrayList<>();
    private final List<Path> retiredDirs = new ArrayList<>();
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

    /**
     * True when a lazy generation keeps this Lucene file on local disk: the always-eager names, any
     * file of at most {@code eagerMaxBytes}, and a {@code .cfs} compound file of at most one block
     * (the warm open would fetch that block anyway, and downloading it whole also checksums it).
     */
    public boolean eager(ManifestFile f) {
        String name = f.name();
        return name.startsWith("segments_") || name.endsWith(".si") || name.endsWith(".fnm")
                || name.endsWith(".cfe") || f.length() <= config.getEagerMaxBytes()
                || (name.endsWith(".cfs") && f.length() <= config.getBlockSizeBytes());
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
     * every block of the remote files whose extension is one of {@code prefetchExtensions}. Those
     * files are taken in the order of {@code prefetchExtensions}, whole, while their total stays
     * within half the cache's cap (the rest is fetched on first read). The blocks the shard's previous
     * prefetch still has queued are cancelled first. The future completes when all are done (blocks
     * cached or skipped), exceptionally when one fails or is cancelled. A head is written to a temp
     * file and moved into place, so a sidecar is complete or absent.
     */
    public Prefetch prefetch(ShardManifest manifest, Path luceneDir) throws IOException {
        return prefetch(manifest, luceneDir, Map.of());
    }

    /**
     * {@link #prefetch(ShardManifest, Path)}, reusing head sidecars: {@code reusable} maps an object
     * key to the same file in the active generation's {@code lucene/} dir, and its head sidecar there
     * is hard-linked (copied when linking fails) instead of fetched again.
     */
    public Prefetch prefetch(ShardManifest manifest, Path luceneDir, Map<String, Path> reusable)
            throws IOException {
        ShardId shard = new ShardId(manifest.shard());
        List<ManifestFile> remoteFiles = manifest.files().stream()
                .filter(f -> ManifestFile.LUCENE.equals(f.role()) && !eager(f) && f.length() > 0)
                .toList();
        if (remoteFiles.isEmpty()) return new Prefetch(CompletableFuture.completedFuture(null), 0, 0, 0, 0);
        BlockCache cache = blockCache();
        int cancelled = cancelPrevious(shard.value(), cache);
        Path edges = luceneDir.resolve(RemoteDirectory.EDGES_DIR);
        Files.createDirectories(edges); // tasks never create it: a moved partial dir stays gone
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ManifestFile f : remoteFiles) futures.add(cache.runOnPrefetchPool(() -> downloadHead(f, luceneDir, reusable.get(f.key()))));
        int heads = futures.size();

        List<String> order = config.getPrefetchExtensions();
        List<ManifestFile> wanted = remoteFiles.stream()
                .filter(f -> prefetchExtensions.contains(extension(f.name())))
                .sorted(Comparator.comparingInt((ManifestFile f) -> order.indexOf(extension(f.name())))
                        .thenComparing(ManifestFile::name))
                .toList();
        long budget = cache.maxBytes() / 2;
        long bytes = 0;
        int blocks = 0;
        List<BlockKey> queued = new ArrayList<>();
        for (ManifestFile f : wanted) {
            if (bytes + f.length() > budget) {
                log.info("Shard {}: not prefetching {} of generation {} ({} bytes): the prefetch is capped at half"
                        + " the block cache ({} bytes)", shard, f.name(), manifest.generation(), f.length(), budget);
                continue;
            }
            CachingRemoteIndexInput.RemoteFile remote =
                    new CachingRemoteIndexInput.RemoteFile(shard, manifest.generation(), f, cache, source);
            List<CompletableFuture<Void>> fileBlocks = remote.prefetchAll();
            futures.addAll(fileBlocks);
            queued.addAll(remote.blockKeys());
            blocks += fileBlocks.size();
            bytes += f.length();
        }
        synchronized (lastPrefetch) {
            lastPrefetch.put(shard.value(), queued);
        }
        return new Prefetch(CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)), heads, blocks, bytes,
                cancelled);
    }

    /** Cancels the still-queued blocks of the shard's previous prefetch; returns how many. */
    private int cancelPrevious(String shard, BlockCache cache) {
        List<BlockKey> previous;
        synchronized (lastPrefetch) {
            previous = lastPrefetch.remove(shard);
        }
        if (previous == null) return 0;
        int n = 0;
        for (BlockKey k : previous) {
            if (cache.cancelQueued(k, BlockCache.objectHash(k.objectKey()))) n++;
        }
        if (n > 0) log.info("Shard {}: cancelled {} queued prefetch blocks of the previous generation", shard, n);
        return n;
    }

    private static String extension(String name) {
        return name.substring(name.lastIndexOf('.') + 1);
    }

    private void downloadHead(ManifestFile f, Path luceneDir, Path previous) throws IOException {
        Path target = RemoteDirectory.headFile(luceneDir, f.name());
        int len = (int) Math.min(RemoteDirectory.HEAD_BYTES, f.length());
        if (previous != null && reuseHead(previous, target, len)) return;
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

    /** Links (or copies) the head sidecar of {@code previous}, the same object in the active generation. */
    private static boolean reuseHead(Path previous, Path target, int len) {
        Path head = RemoteDirectory.headFile(previous.getParent(), previous.getFileName().toString());
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            if (!Files.isRegularFile(head) || Files.size(head) != len) return false;
            Files.deleteIfExists(tmp);
            try {
                Files.createLink(tmp, head);
            } catch (IOException | UnsupportedOperationException e) {
                Files.copy(head, tmp);
            }
            Files.move(tmp, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException | RuntimeException e) {
            log.debug("reusing the head {} failed, fetching it instead: {}", head, e.toString());
            return false;
        } finally {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // a leftover .tmp is overwritten by the next attempt
            }
        }
    }

    /**
     * A started prefetch: {@code done} completes when its {@code heads} head sidecars and its
     * {@code blocks} blocks ({@code bytes} bytes of files) are in; {@code cancelledPrevious} queued
     * blocks of the shard's previous prefetch were cancelled.
     */
    public record Prefetch(CompletableFuture<Void> done, int heads, int blocks, long bytes, int cancelledPrevious) {}

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
            long cap = lock.isPresent() ? config.getCacheMaxBytes() : config.effectivePrivateCacheMaxBytes();
            blockCache = new BlockCache(dir, cap, Math.toIntExact(config.getBlockSizeBytes()),
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

    /**
     * Housekeeping for each sync pass: the holder of the shared cache deletes the private block caches
     * of processes that are gone (they may have died without cleaning up).
     */
    public void sweep() {
        synchronized (this) {
            if (closed) return;
            adoptSharedCache();
            if (blocksLock == null) return;
        }
        deleteStalePrivateDirs();
    }

    /**
     * A process on a private block cache takes the shared one once the process that held its lock is
     * gone: opens and prefetches from then on use the shared cache. The private cache stays open (and
     * on disk) until {@link #close}, because readers opened earlier still read through it.
     *
     * @return true when the switch happened
     */
    public synchronized boolean adoptSharedCache() {
        if (closed || blockCache == null || blocksLock != null) return false;
        SyncLock lock;
        try {
            Optional<SyncLock> acquired = SyncLock.tryAcquire(cacheDir, BLOCKS_LOCK);
            if (acquired.isEmpty()) return false;
            lock = acquired.get();
        } catch (IOException | RuntimeException e) {
            log.warn("Taking the shared block cache {} failed; staying on the private one: {}",
                    cacheDir.resolve(BLOCKS_DIR), e.toString());
            return false;
        }
        BlockCache shared;
        try {
            shared = new BlockCache(cacheDir.resolve(BLOCKS_DIR), config.getCacheMaxBytes(),
                    Math.toIntExact(config.getBlockSizeBytes()), config.getMaxConcurrentFetches());
        } catch (IOException | RuntimeException e) {
            try {
                lock.close();
            } catch (IOException ignored) {
                // released when the process exits
            }
            log.warn("Opening the shared block cache {} failed; staying on the private one: {}",
                    cacheDir.resolve(BLOCKS_DIR), e.toString());
            return false;
        }
        log.info("The shared block cache {} is free: new opens use it; the private block cache {} stays until"
                + " this process exits", cacheDir.resolve(BLOCKS_DIR), privateDir);
        retiredCaches.add(blockCache);
        retiredDirs.add(privateDir);
        blockCache = shared;
        blocksLock = lock;
        privateDir = null;
        return true;
    }

    /** True when this process's block cache is the shared one; false on a private cache or none yet. */
    public synchronized boolean sharedCache() {
        return blockCache != null && blocksLock != null;
    }

    /** Counters of this process's block cache; empty before it was first used. */
    public synchronized Optional<WarmStats> stats() {
        return blockCache == null ? Optional.empty() : Optional.of(blockCache.stats());
    }

    /** The cap of this process's block cache, or the configured shared cap before it exists. */
    public synchronized long cacheMaxBytes() {
        return blockCache == null ? config.getCacheMaxBytes() : blockCache.maxBytes();
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
        retiredCaches.forEach(BlockCache::close);
        List<Path> privateDirs = new ArrayList<>(retiredDirs);
        if (privateDir != null) privateDirs.add(privateDir);
        for (Path d : privateDirs) {
            try {
                deleteRecursively(d);
            } catch (IOException e) {
                log.warn("Deleting the private block cache {} failed: {}", d, e.toString());
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
