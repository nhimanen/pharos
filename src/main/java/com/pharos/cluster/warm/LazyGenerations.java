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
 * {@code effectivePrivateCacheMaxBytes} and deleted when it closes. Its owner holds the file lock
 * {@value #PRIVATE_OWNER_LOCK} inside it for its lifetime; the shared cache's holder deletes, at
 * startup and on every {@link #sweep}, the private dirs whose lock it can take (their owner is gone). The syncing reader takes
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
    /** Held by the process that owns a private block cache, inside its dir, for the cache's lifetime. */
    public static final String PRIVATE_OWNER_LOCK = BlockCache.OWNER_LOCK;
    private static final String MANIFEST_FILE = "manifest.json";
    private static final Pattern PRIVATE_NAME = Pattern.compile("\\d{1,19}-[0-9a-f]{8}");
    /** A private cache dir being created; its lock is taken before it gets its final name. */
    private static final String STAGING_SUFFIX = ".new";
    /** A staging dir older than this whose lock is free was left by a crash. */
    private static final long STAGING_GRACE_MILLIS = 60_000;
    private static final int MANIFEST_CACHE_SIZE = 64;

    private final Path cacheDir;
    private final ClusterConfig.ReaderConfig config;
    private final ReplicaSource source;
    private final Set<String> prefetchExtensions;
    /** Bound of a read's wait for a block fetch; null for none. */
    private final Duration fetchTimeout;
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

    /** The generation each shard serves now (empty when none); set by the reader runtime. */
    private volatile java.util.function.Function<String, java.util.OptionalLong> activeGeneration =
            shard -> java.util.OptionalLong.empty();
    /**
     * Per shard, the active generation a block read found an object of missing on the bucket (it was
     * garbage-collected while this reader served it). Reported by {@link #degradation} while that
     * generation is still the active one.
     */
    private final Map<String, Long> goneGenerations = new java.util.concurrent.ConcurrentHashMap<>();

    // Guarded by this.
    private BlockCache blockCache;
    private SyncLock blocksLock;
    private Path privateDir;
    /** The lock this process holds inside {@link #privateDir}: the dir's proof of life. */
    private SyncLock privateOwner;
    /** Private caches this process left for the shared one: retired readers may still use them. */
    private final List<BlockCache> retiredCaches = new ArrayList<>();
    private final List<Path> retiredDirs = new ArrayList<>();
    private final List<SyncLock> retiredOwners = new ArrayList<>();
    private boolean closed;

    public LazyGenerations(Path cacheDir, ClusterConfig.ReaderConfig config, ReplicaSource source) {
        this(cacheDir, config, source, null);
    }

    /**
     * @param fetchTimeout how long a read waits for a block fetch before failing (the object store's
     *                     {@code readTotalTimeout}); null waits without bound
     */
    public LazyGenerations(Path cacheDir, ClusterConfig.ReaderConfig config, ReplicaSource source,
                           Duration fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
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
        return new RemoteDirectory(new ShardId(manifest.shard()), manifest, luceneDir, blockCache(), source, true,
                this::objectMissing);
    }

    // ---------------------------------------------------------------- retention

    /**
     * Tells this reader which generation each shard serves now (the reader runtime's active
     * generation dirs), so a missing object can be told apart from one of a retired generation.
     */
    public void setActiveGenerations(java.util.function.Function<String, java.util.OptionalLong> active) {
        this.activeGeneration = Objects.requireNonNull(active, "active");
    }

    /**
     * A block read of {@code shard}'s generation {@code gen} found object {@code key} missing on the
     * bucket. When {@code gen} is the shard's active generation, the generation was garbage-collected
     * under this reader (it lags more than the writer's retention): one ERROR per generation, and the
     * shard counts as degraded ({@link #degradation}) until a newer generation is activated.
     */
    void objectMissing(ShardId shard, long gen, String key) {
        java.util.OptionalLong active = activeGeneration.apply(shard.value());
        if (active.isEmpty() || active.getAsLong() != gen) return; // a retired generation: GC may take it
        Long before = goneGenerations.put(shard.value(), gen);
        if (before == null || before != gen) {
            log.error("generation {} of shard {} was garbage-collected on the bucket (object {} is gone); this reader"
                    + " is behind retention — sync a newer generation or switch to mode full"
                    + " (cluster.reader.mode \"full\"). Queries that need uncached blocks of it fail until then.",
                    gen, shard, key);
        }
    }

    /**
     * Why this reader can't serve every query, or null: the shards whose active generation lost
     * objects on the bucket. A shard drops out as soon as a newer generation is active.
     */
    public String degradation() {
        List<String> reasons = new ArrayList<>();
        for (Map.Entry<String, Long> e : goneGenerations.entrySet()) {
            java.util.OptionalLong active = activeGeneration.apply(e.getKey());
            if (active.isPresent() && active.getAsLong() == e.getValue()) {
                reasons.add("shard " + e.getKey() + ": generation " + e.getValue() + " was garbage-collected on the"
                        + " bucket; this reader is behind retention");
            } else {
                goneGenerations.remove(e.getKey(), e.getValue()); // a newer generation is active
            }
        }
        if (reasons.isEmpty()) return null;
        java.util.Collections.sort(reasons);
        return String.join("; ", reasons);
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
     * every block of the remote files whose extension is one of {@code prefetchExtensions}, and of
     * the entries with such an extension inside remote compound files ({@code .cfs}, located through
     * the eager {@code .cfe} entry table: published segments are often single compound files). Those
     * files and entries are taken in the order of {@code prefetchExtensions}, whole, while their
     * blocks stay within half the cache's cap (the rest is fetched on first read). The blocks the shard's previous
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

        // What to prefetch: whole remote files with a prefetch extension, and the byte ranges of
        // entries with one inside remote compound files (located through the eager .cfe table).
        List<String> order = config.getPrefetchExtensions();
        List<PrefetchUnit> units = new ArrayList<>();
        for (ManifestFile f : remoteFiles) {
            String ext = extension(f.name());
            if (prefetchExtensions.contains(ext)) {
                units.add(new PrefetchUnit(f, f.name(), ext, 0, f.length()));
            } else if (ext.equals("cfs")) {
                units.addAll(compoundUnits(shard, manifest.generation(), f, luceneDir));
            }
        }
        units.sort(Comparator.comparingInt((PrefetchUnit u) -> order.indexOf(u.extension()))
                .thenComparing(PrefetchUnit::label));
        long budget = cache.maxBytes() / 2;
        long bytes = 0;
        int blocks = 0;
        List<BlockKey> queued = new ArrayList<>();
        Map<String, CachingRemoteIndexInput.RemoteFile> remotes = new LinkedHashMap<>();
        Map<String, java.util.Set<Long>> taken = new java.util.HashMap<>();
        for (PrefetchUnit u : units) {
            CachingRemoteIndexInput.RemoteFile remote = remotes.computeIfAbsent(u.file().key(),
                    k -> new CachingRemoteIndexInput.RemoteFile(shard, manifest.generation(), u.file(), cache, source));
            java.util.Set<Long> seen = taken.computeIfAbsent(u.file().key(), k -> new java.util.HashSet<>());
            List<Long> fresh = new ArrayList<>();
            long unitBytes = 0;
            if (u.length() > 0) {
                for (long b = remote.blockOf(u.offset()); b <= remote.blockOf(u.offset() + u.length() - 1); b++) {
                    if (!seen.contains(b)) {
                        fresh.add(b);
                        unitBytes += remote.lengthOfBlock(b);
                    }
                }
            }
            if (fresh.isEmpty()) continue;
            if (bytes + unitBytes > budget) {
                log.info("Shard {}: not prefetching {} of generation {} ({} bytes): the prefetch is capped at half"
                        + " the block cache ({} bytes)", shard, u.label(), manifest.generation(), unitBytes, budget);
                continue;
            }
            for (long b : fresh) {
                seen.add(b);
                futures.add(remote.prefetchBlock(b));
                queued.add(remote.blockKey(b));
            }
            blocks += fresh.size();
            bytes += unitBytes;
        }
        synchronized (lastPrefetch) {
            lastPrefetch.put(shard.value(), queued);
        }
        return new Prefetch(CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)), heads, blocks, bytes,
                cancelled);
    }

    /** A byte range to prefetch: a whole file, or one entry of a compound file. */
    private record PrefetchUnit(ManifestFile file, String label, String extension, long offset, long length) {}

    /**
     * The entries of remote compound file {@code cfs} whose extension is a prefetch extension, read
     * from its eager {@code .cfe} table in {@code luceneDir}. Empty (logged) when the table is
     * missing or unreadable: the entries are then fetched on first read, as before.
     */
    private List<PrefetchUnit> compoundUnits(ShardId shard, long gen, ManifestFile cfs, Path luceneDir) {
        String cfe = cfs.name().substring(0, cfs.name().length() - "cfs".length()) + "cfe";
        Path table = luceneDir.resolve(cfe);
        if (!Files.isRegularFile(table)) return List.of();
        List<PrefetchUnit> out = new ArrayList<>();
        try {
            for (CompoundEntry e : readCompoundEntries(table)) {
                String ext = extension(e.name());
                if (!prefetchExtensions.contains(ext)) continue;
                if (e.offset() < 0 || e.length() < 0 || e.offset() + e.length() > cfs.length()) {
                    throw new IOException("entry " + e.name() + " [" + e.offset() + "+" + e.length()
                            + ") lies outside the " + cfs.length() + "-byte " + cfs.name());
                }
                out.add(new PrefetchUnit(cfs, cfs.name() + ":" + e.name(), ext, e.offset(), e.length()));
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Shard {}: cannot read the compound entry table {} of generation {} ({}); its entries are"
                    + " fetched on first read instead of prefetched", shard, cfe, gen, e.toString());
            return List.of();
        }
        return out;
    }

    /** One entry of a {@code .cfe} table: the sub-file's name and its byte range in the {@code .cfs}. */
    record CompoundEntry(String name, long offset, long length) {}

    /**
     * Reads a Lucene 9/10 compound entry table ({@code Lucene90CompoundEntries}: index header, vint
     * count, then per entry name, offset and length) and verifies its checksum footer.
     */
    static List<CompoundEntry> readCompoundEntries(Path cfe) throws IOException {
        try (Directory d = new org.apache.lucene.store.NIOFSDirectory(cfe.getParent());
             org.apache.lucene.store.ChecksumIndexInput in =
                     d.openChecksumInput(cfe.getFileName().toString())) {
            List<CompoundEntry> out = new ArrayList<>();
            Throwable failure = null;
            try {
                org.apache.lucene.codecs.CodecUtil.checkHeader(in, "Lucene90CompoundEntries", 0, 0);
                byte[] id = new byte[org.apache.lucene.util.StringHelper.ID_LENGTH];
                in.readBytes(id, 0, id.length);
                int suffixLength = in.readByte() & 0xFF;
                byte[] suffix = new byte[suffixLength];
                in.readBytes(suffix, 0, suffixLength);
                int n = in.readVInt();
                for (int i = 0; i < n; i++) {
                    String name = in.readString();
                    long offset = in.readLong();
                    long length = in.readLong();
                    out.add(new CompoundEntry(name, offset, length));
                }
            } catch (Throwable t) {
                failure = t;
            } finally {
                org.apache.lucene.codecs.CodecUtil.checkFooter(in, failure);
            }
            return out;
        }
    }

    /** Cancels the still-queued blocks of the shard's previous prefetch; returns how many. */
    private int cancelPrevious(String shard, BlockCache cache) {
        List<BlockKey> previous;
        synchronized (lastPrefetch) {
            previous = lastPrefetch.remove(shard);
        }
        if (previous == null) return 0;
        List<BlockCache> retired;
        synchronized (this) {
            retired = List.copyOf(retiredCaches);
        }
        int n = 0;
        for (BlockKey k : previous) {
            String hash = BlockCache.objectHash(k.objectKey());
            if (cache.cancelQueued(k, hash)) n++;
            for (BlockCache r : retired) {
                if (r.cancelQueued(k, hash)) n++;
            }
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
        SyncLock owner = null;
        if (lock.isPresent()) {
            dir = cacheDir.resolve(BLOCKS_DIR);
            deleteStalePrivateDirs();
        } else {
            Path parent = Files.createDirectories(cacheDir.resolve(PRIVATE_BLOCKS_DIR));
            String name = ProcessHandle.current().pid() + "-" + UUID.randomUUID().toString().substring(0, 8);
            dir = parent.resolve(name);
            owner = lockedPrivateDir(parent, name);
            log.info("Another process uses the block cache {}; using the private block cache {} until this one exits",
                    cacheDir.resolve(BLOCKS_DIR), dir);
        }
        try {
            long cap = lock.isPresent() ? config.getCacheMaxBytes() : config.effectivePrivateCacheMaxBytes();
            blockCache = new BlockCache(dir, cap, Math.toIntExact(config.getBlockSizeBytes()),
                    config.getMaxConcurrentFetches(), fetchTimeout);
        } catch (IOException | RuntimeException e) {
            if (lock.isPresent()) lock.get().close();
            if (owner != null) {
                deleteRecursively(dir);
                owner.close();
            }
            throw e;
        }
        blocksLock = lock.orElse(null);
        privateDir = lock.isPresent() ? null : dir;
        privateOwner = owner;
        return blockCache;
    }

    /**
     * Creates the private cache dir {@code <parent>/<name>} holding its {@link #PRIVATE_OWNER_LOCK}:
     * the dir is made and locked under a staging name ({@code <name>.new}) and then renamed, so a
     * sweeper never sees the final name without a held lock (an OS file lock follows the file).
     */
    private static SyncLock lockedPrivateDir(Path parent, String name) throws IOException {
        Path staging = parent.resolve(name + STAGING_SUFFIX);
        SyncLock owner = SyncLock.tryAcquire(staging, PRIVATE_OWNER_LOCK)
                .orElseThrow(() -> new IOException("cannot lock the new private block cache " + staging));
        try {
            Files.move(staging, parent.resolve(name), java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException e) {
            owner.close();
            deleteRecursively(staging);
            throw e;
        }
        return owner;
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
    public boolean adoptSharedCache() {
        synchronized (this) {
            if (closed || blockCache == null || blocksLock != null) return false;
        }
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
            shared = new BlockCache(cacheDir.resolve(BLOCKS_DIR), config.getCacheMaxBytes(), // scans the dir
                    Math.toIntExact(config.getBlockSizeBytes()), config.getMaxConcurrentFetches(), fetchTimeout);
        } catch (IOException | RuntimeException e) {
            releaseQuietly(lock);
            log.warn("Opening the shared block cache {} failed; staying on the private one: {}",
                    cacheDir.resolve(BLOCKS_DIR), e.toString());
            return false;
        }
        BlockCache retiring;
        synchronized (this) {
            if (closed || blockCache == null || blocksLock != null) { // closed or switched meanwhile
                shared.close();
                releaseQuietly(lock);
                return false;
            }
            log.info("The shared block cache {} is free: new opens use it; the private block cache {} stays until"
                    + " this process exits", cacheDir.resolve(BLOCKS_DIR), privateDir);
            retiring = blockCache;
            retiredCaches.add(retiring);
            retiredDirs.add(privateDir);
            if (privateOwner != null) retiredOwners.add(privateOwner);
            blockCache = shared;
            blocksLock = lock;
            privateDir = null;
            privateOwner = null;
        }
        // Prefetches still queued on the private cache would only fill a cache no new open reads.
        List<BlockKey> queued = new ArrayList<>();
        synchronized (lastPrefetch) {
            lastPrefetch.values().forEach(queued::addAll);
        }
        for (BlockKey k : queued) retiring.cancelQueued(k, BlockCache.objectHash(k.objectKey()));
        return true;
    }

    private static void releaseQuietly(SyncLock lock) {
        try {
            lock.close();
        } catch (IOException ignored) {
            // released when the process exits
        }
    }

    /** True when this process's block cache is the shared one; false on a private cache or none yet. */
    public synchronized boolean sharedCache() {
        return blockCache != null && blocksLock != null;
    }

    /**
     * Counters of this process's block caches, the active one plus any private cache it retired for
     * the shared one (still on disk, still read by older readers); empty before first use.
     */
    public synchronized Optional<WarmStats> stats() {
        if (blockCache == null) return Optional.empty();
        WarmStats t = blockCache.stats();
        for (BlockCache r : retiredCaches) {
            WarmStats s = r.stats();
            t = new WarmStats(t.hits() + s.hits(), t.misses() + s.misses(), t.bytesFetched() + s.bytesFetched(),
                    t.evictions() + s.evictions(), t.bytesOnDisk() + s.bytesOnDisk(), t.leasesOpen() + s.leasesOpen(),
                    t.writeFailures() + s.writeFailures(), t.thrashWarnings() + s.thrashWarnings());
        }
        return Optional.of(t);
    }

    /** The cap of this process's block cache, or the configured shared cap before it exists. */
    public synchronized long cacheMaxBytes() {
        return blockCache == null ? config.getCacheMaxBytes() : blockCache.maxBytes();
    }

    /**
     * Deletes the private block caches nobody owns: a dir under {@value #PRIVATE_BLOCKS_DIR} whose
     * {@link #PRIVATE_OWNER_LOCK} this process can take (its owner is gone: the OS released the lock),
     * or that has none. PIDs are not consulted: they lie across PID namespaces (sidecars sharing a
     * volume) and after PID reuse. A staging dir ({@code .new}) is left alone for its first minute.
     */
    private void deleteStalePrivateDirs() {
        deleteStalePrivateDirs(cacheDir);
    }

    private static void deleteStalePrivateDirs(Path cacheDir) {
        Path parent = cacheDir.resolve(PRIVATE_BLOCKS_DIR);
        if (!Files.isDirectory(parent)) return;
        try (Stream<Path> s = Files.list(parent)) {
            for (Path p : s.toList()) {
                String name = p.getFileName().toString();
                boolean staging = name.endsWith(STAGING_SUFFIX);
                String base = staging ? name.substring(0, name.length() - STAGING_SUFFIX.length()) : name;
                if (!PRIVATE_NAME.matcher(base).matches() || !Files.isDirectory(p)) continue;
                if (staging && System.currentTimeMillis() - Files.getLastModifiedTime(p).toMillis()
                        < STAGING_GRACE_MILLIS) {
                    continue;
                }
                Optional<SyncLock> owner = SyncLock.tryAcquire(p, PRIVATE_OWNER_LOCK);
                if (owner.isEmpty()) continue; // held: its owner runs (in whatever PID namespace)
                try {
                    log.info("Deleting the private block cache {}: no process holds its lock", p);
                    deleteRecursively(p);
                } finally {
                    releaseQuietly(owner.get());
                }
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Cleaning up private block caches in {} failed: {}", parent, e.toString());
        }
    }

    /**
     * For a full-mode reader: deletes what lazy mode left in {@code cacheDir} — the shared block cache
     * {@value #BLOCKS_DIR} and the private ones nobody owns — when no process holds the block cache's
     * lock (a lazy process on the same cache dir keeps it). A full-mode reader never reads them.
     *
     * @return true when the shared block cache was deleted
     */
    public static boolean deleteUnusedBlockCaches(Path cacheDir) {
        Path dir = cacheDir.toAbsolutePath();
        Path blocks = dir.resolve(BLOCKS_DIR);
        if (!Files.exists(blocks) && !Files.exists(dir.resolve(PRIVATE_BLOCKS_DIR))) return false;
        Optional<SyncLock> lock;
        try {
            lock = SyncLock.tryAcquire(dir, BLOCKS_LOCK);
        } catch (IOException | RuntimeException e) {
            log.warn("Cannot check whether the block cache {} is in use; leaving it: {}", blocks, e.toString());
            return false;
        }
        if (lock.isEmpty()) {
            log.info("Full mode: the lazy-mode block cache {} is still used by another process; leaving it", blocks);
            return false;
        }
        try {
            deleteStalePrivateDirs(dir);
            if (!Files.exists(blocks)) return false;
            log.info("Full mode: deleting the lazy-mode block cache {} (no process uses it)", blocks);
            deleteRecursively(blocks);
            return true;
        } catch (IOException | RuntimeException e) {
            log.warn("Deleting the lazy-mode block cache {} failed: {}", blocks, e.toString());
            return false;
        } finally {
            releaseQuietly(lock.get());
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
        List<SyncLock> owners = new ArrayList<>(retiredOwners);
        if (privateOwner != null) owners.add(privateOwner);
        owners.forEach(LazyGenerations::releaseQuietly); // after the delete: a sweeper never races it
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
