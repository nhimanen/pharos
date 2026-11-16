package com.pharos.cluster;

import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.cluster.warm.LazyGenerations;
import com.pharos.cluster.warm.WarmStats;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IndexVersions;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * The node's role-specific runtime: the replication backend and, on readers, the
 * {@link ReplicaSyncer} that keeps the local cache in step with the object store.
 *
 * <p>Readers: at construction the generations recorded in the reader registry
 * ({@code <cacheDir>/registry.json}) are activated, so a one-shot command (or a process that does
 * not sync) serves what is already on disk. {@link #beginServing} takes the exclusive
 * {@link SyncLock} on the cache dir: its holder runs the syncer (first pass in the background,
 * then every {@code cluster.pollIntervalSeconds}); any other process on the same cache dir runs no
 * syncer and follows the registry the holder maintains (re-activating every poll interval, and
 * taking over the lock once the holder is gone).
 *
 * <p>Readers in {@code cluster.reader.mode} {@code lazy} (the default) install a
 * {@link LazyGenerations} as the {@link LuceneIndexer}'s directory factory before anything is
 * activated: lazy generation dirs open as remote directories over the block cache, every other dir
 * as before. A full-mode reader, and every other role, keeps the default factory and never builds a
 * Phase 2 class. A full-mode reader does not activate lazy generation dirs found on disk (the syncer
 * materialises them again).
 *
 * <p>{@link #close} stops the syncer, releases the lock, closes the block cache and the backend
 * (object-store clients); it is idempotent. Shutdown order: close it only once nothing searches
 * any more ({@code Main} does so when the command returns). Once the block cache is closed, a read
 * of a lazy generation fails with "block cache ... is closed"; on a signal the JVM shutdown hook may
 * close it while a query is still running, and that query fails as the process exits.
 */
public final class ClusterRuntime implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ClusterRuntime.class);
    private static final Pattern GEN_DIR = Pattern.compile("gen-\\d{1,18}");

    private final IndexConfig config;
    private final ReplicationBackend backend;
    private final ProjectRegistry registry;
    private final LuceneIndexer luceneIndexer;
    /** Lazy mode on a reader; null otherwise. */
    private final LazyGenerations lazy;
    private final CountDownLatch firstSync = new CountDownLatch(1);
    /** Registered generation dirs already reported as not servable; their WARN is not repeated. */
    private final java.util.Set<Path> reportedUnservable = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private ReplicaSyncer syncer;
    private SyncLock syncLock;
    private ScheduledExecutorService follower;
    private Thread firstSyncThread;
    private boolean serving;
    private volatile boolean ready;
    private boolean closed;

    public ClusterRuntime(IndexConfig config, ReplicationBackend backend, ProjectRegistry registry,
                          LuceneIndexer luceneIndexer) {
        this.config = Objects.requireNonNull(config, "config");
        this.backend = Objects.requireNonNull(backend, "backend");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.luceneIndexer = Objects.requireNonNull(luceneIndexer, "luceneIndexer");
        if (isReader() && config.getCluster().getReader().isLazy()) {
            lazy = new LazyGenerations(cacheDir(), config.getCluster().getReader(), backend.source());
            luceneIndexer.setDirectoryFactory(lazy);
        } else {
            lazy = null;
        }
        if (isReader() && backend.localCache() instanceof ReaderLocalCache) {
            try {
                activateRegistered();
            } catch (RuntimeException e) {
                log.warn("Activating the registered generations failed: {}", e.toString());
            }
        }
    }

    /**
     * The registry a node serves: on readers {@code <cacheDir>/registry.json}, filled by the
     * syncer; otherwise the regular {@code registry.json}.
     */
    public static ProjectRegistry registryFor(IndexConfig config) {
        if (config.getCluster().getRole() == NodeRole.READER) {
            return ProjectRegistry.at(config.getCluster().getCacheDir().resolve("registry.json"));
        }
        return new ProjectRegistry(config);
    }

    /**
     * Readers never read source files: bodies and snippets come from the index, so an
     * unrelated or stale file at an indexed absolute path cannot leak into answers.
     */
    public static void applySourceReadPolicy(IndexConfig config) {
        SourceReader.setEnabled(config.getCluster().getRole() != NodeRole.READER);
    }

    /** Lazy mode's directory factory and block cache; null unless this is a reader (a full-mode reader reports mode {@code full} and zeros). */
    LazyGenerations lazyGenerations() {
        return lazy;
    }

    /**
     * The block cache of this process as operators see it ({@code readerCache} of
     * {@code index-status --json} and {@code /health}); null unless this is a lazy reader.
     * Fields: {@code mode, cacheMaxBytes, bytesOnDisk, hits, misses, hitRate (0..1), bytesFetched,
     * evictions, leasesOpen}, plus {@code shared}: true when this process's cache is the cache dir's
     * shared one, false when it runs a private one (or none has been created yet).
     */
    public Map<String, Object> readerCacheStats() {
        if (!isReader()) return null;
        WarmStats st = lazy == null ? new WarmStats(0, 0, 0, 0, 0, 0, 0, 0)
                : lazy.stats().orElse(new WarmStats(0, 0, 0, 0, 0, 0, 0, 0));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("mode", lazy == null ? ClusterConfig.ReaderConfig.FULL : config.getCluster().getReader().getMode());
        out.put("cacheMaxBytes", lazy == null ? 0L : lazy.cacheMaxBytes());
        out.put("bytesOnDisk", st.bytesOnDisk());
        out.put("hits", st.hits());
        out.put("misses", st.misses());
        out.put("hitRate", st.hitRate());
        out.put("bytesFetched", st.bytesFetched());
        out.put("evictions", st.evictions());
        out.put("leasesOpen", st.leasesOpen());
        out.put("shared", lazy != null && lazy.sharedCache());
        out.put("source", "process");
        return out;
    }

    /**
     * The one-line text form of {@link #readerCacheStats}:
     * {@code reader cache: 1.2 GiB / 8.0 GiB, hit rate 97.3%, 41 evictions}; {@code hit rate n/a}
     * before the first lookup, and a full-mode reader has no block cache.
     */
    public static String readerCacheLine(Map<String, Object> rc) {
        if (ClusterConfig.ReaderConfig.FULL.equals(rc.get("mode"))) return "reader cache: full mode (no block cache)";
        boolean lookups = ((Number) rc.get("hits")).longValue() + ((Number) rc.get("misses")).longValue() > 0;
        String rate = lookups ? String.format(java.util.Locale.ROOT, "%.1f%%",
                100 * ((Number) rc.get("hitRate")).doubleValue()) : "n/a";
        return String.format(java.util.Locale.ROOT, "reader cache: %s / %s, hit rate %s, %d evictions",
                gib(((Number) rc.get("bytesOnDisk")).longValue()), gib(((Number) rc.get("cacheMaxBytes")).longValue()),
                rate, ((Number) rc.get("evictions")).longValue());
    }

    private static String gib(long bytes) {
        String[] units = {"B", "KiB", "MiB", "GiB", "TiB"};
        double v = bytes;
        int i = 0;
        while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
        return i == 0 ? bytes + " B" : String.format(java.util.Locale.ROOT, "%.1f %s", v, units[i]);
    }

    public IndexPublisher publisher() {
        return backend.publisher();
    }

    private boolean isReader() {
        return config.getCluster().getRole() == NodeRole.READER;
    }

    private Path cacheDir() {
        return config.getCluster().getCacheDir();
    }

    private ReaderLocalCache readerCache() {
        if (!(backend.localCache() instanceof ReaderLocalCache cache)) {
            throw new IllegalStateException("reader role needs a ReaderLocalCache, got " + backend.localCache());
        }
        return cache;
    }

    /**
     * {@link #beginServing}, then waits until the first sync pass has finished (successfully or
     * not: failures are logged and retried every poll interval).
     */
    public void startServing() throws IOException {
        beginServing();
        awaitFirstSync();
    }

    /**
     * Readers: starts serving without waiting for the first sync, so a daemon can bind its port at
     * once ({@link #ready} turns true when the first pass finishes). The process holding the cache
     * dir's {@link SyncLock} syncs; others follow the registry. No-op on other roles and when
     * already serving.
     *
     * @throws IOException when the lock file cannot be created
     */
    public void beginServing() throws IOException {
        synchronized (this) {
            if (!isReader() || serving) return;
            if (closed) throw new IllegalStateException("cluster runtime is closed");
            ReaderLocalCache cache = readerCache();
            Optional<SyncLock> lock = SyncLock.tryAcquire(cacheDir());
            serving = true;
            if (lock.isPresent()) {
                becomeOwner(lock.get(), cache);
            } else {
                becomeFollower();
            }
        }
    }

    /** Blocks until the first sync pass of a serving reader has finished; returns at once otherwise. */
    public void awaitFirstSync() {
        synchronized (this) {
            if (!isReader() || !serving) return;
        }
        try {
            firstSync.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** False while a serving reader's first sync pass is still running; true otherwise. */
    public boolean ready() {
        return !isReader() || ready;
    }

    /** True when this process holds the cache dir's sync lock and runs the syncer. */
    public synchronized boolean ownsSync() {
        return syncer != null;
    }

    // Holding the monitor of this.
    private void becomeOwner(SyncLock lock, ReaderLocalCache cache) {
        syncLock = lock;
        if (lazy != null) {
            try {
                lazy.blockCache(); // the prefetching process takes the shared block cache, not a private one
                lazy.adoptSharedCache(); // a follower taking over starts on a private one
            } catch (IOException | RuntimeException e) {
                log.warn("Opening the block cache in {} failed (retried on first use): {}", cacheDir(), e.toString());
            }
        }
        ReplicaSyncer s = new ReplicaSyncer(backend.catalog(), backend.source(), cache, registry,
                cacheDir(), config.getSynonymsFile(),
                Duration.ofSeconds(config.getCluster().getPollIntervalSeconds()), lazy);
        syncer = s;
        firstSyncThread = new Thread(() -> firstPass(s), "pharos-replica-first-sync");
        firstSyncThread.setDaemon(true);
        firstSyncThread.start();
    }

    private void firstPass(ReplicaSyncer s) {
        try {
            s.syncOnce();
        } catch (Exception e) {
            log.warn("Replica sync from bucket '{}' failed (retrying every {} s): {}",
                    config.getCluster().getObjectStore().getBucket(),
                    config.getCluster().getPollIntervalSeconds(), e.toString());
        } finally {
            Map<ShardId, Generation> served = s.served();
            log.info("Reader {} serving {} shard(s): {}", config.getCluster().effectiveNodeId(), served.size(), served);
            warm(served.keySet().stream().map(ShardId::value).toList());
            ready = true;
            firstSync.countDown();
        }
        try {
            s.start(Duration.ofSeconds(config.getCluster().getPollIntervalSeconds()));
        } catch (IllegalStateException closedMeanwhile) {
            // close() ran during the first pass
        }
    }

    // Holding the monitor of this.
    private void becomeFollower() {
        log.info("Another process holds {}: not syncing here; serving the generations it registers in {}",
                cacheDir().resolve(SyncLock.FILE), cacheDir().resolve("registry.json"));
        warm(activateRegistered());
        ready = true;
        firstSync.countDown();
        long interval = config.getCluster().getPollIntervalSeconds();
        follower = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-replica-follower");
            t.setDaemon(true);
            return t;
        });
        follower.scheduleWithFixedDelay(this::followerTick, interval, interval, TimeUnit.SECONDS);
    }

    /** Follower poll: pick up what the lock holder registered; take over once it is gone. */
    void followerTick() {
        try {
            activateRegistered();
            synchronized (this) {
                if (closed || syncer != null) return;
            }
            Optional<SyncLock> lock = SyncLock.tryAcquire(cacheDir());
            if (lock.isEmpty()) return;
            synchronized (this) {
                if (closed || syncer != null) {
                    lock.get().close();
                    return;
                }
                log.info("Took over syncing of {} (the previous holder of the sync lock is gone)", cacheDir());
                if (follower != null) follower.shutdown(); // this task's own executor: no wait
                follower = null;
                becomeOwner(lock.get(), readerCache());
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Following the reader registry failed: {}", e.toString());
        }
    }

    /**
     * Readers: activates, in the {@link ReaderLocalCache}, the generation dir every project of the
     * reader registry points at, when it is a complete {@code <cacheDir>/<project>/gen-N} dir.
     *
     * <p>The registered generation must also be servable by this build (its {@code index-format.json};
     * e.g. after downgrading the reader it may be newer). If it is not, the newest complete,
     * servable {@code gen-*} of that shard is activated and re-registered instead, with a WARN; if
     * there is none, a WARN names the shard and both formats and the shard is not served until a
     * servable generation is synced.
     *
     * @return the projects whose active generation changed
     */
    public List<String> activateRegistered() {
        if (!isReader()) return List.of();
        ReaderLocalCache cache = readerCache();
        Path root = cacheDir().toAbsolutePath().normalize();
        List<String> changed = new ArrayList<>();
        for (ProjectMeta meta : registry.listAll()) {
            String name = meta.getName();
            if (name == null || meta.getIndexPath() == null) continue;
            try {
                new ShardId(name);
            } catch (IllegalArgumentException e) {
                continue;
            }
            Path dir = Path.of(meta.getIndexPath()).toAbsolutePath().normalize();
            if (!root.resolve(name).equals(dir.getParent()) || !completeGeneration(dir)) {
                continue;
            }
            if (!IndexFormat.servable(IndexFormat.status(dir))) {
                Optional<Path> fallback = newestServableGeneration(root.resolve(name));
                if (reportedUnservable.add(dir)) {
                    log.warn("Shard {}: registered generation {} has index format {} ({}); this reader (pharos {})"
                            + " reads index formats {}-{}. {}", name, dir.getFileName(),
                            IndexFormat.read(dir).format(), IndexFormat.status(dir).wireName(),
                            com.pharos.PharosVersion.get(), IndexFormat.MIN_READABLE_FORMAT,
                            IndexVersions.INDEX_FORMAT,
                            fallback.map(f -> "Serving " + f.getFileName() + " (index format "
                                            + IndexFormat.read(f).format() + ") instead.")
                                    .orElse("No servable generation on disk: not serving this shard until"
                                            + " one is synced (upgrade this reader, or re-publish from the writer)."));
                }
                if (fallback.isEmpty()) continue;
                dir = fallback.get();
                ProjectMeta moved = com.pharos.cluster.objectstore.ShardManifest.stripRefs(meta); // a copy
                moved.setIndexPath(dir.toString());
                try {
                    registry.register(moved); // graphs resolve through the registry: keep it in step
                } catch (RuntimeException e) {
                    log.warn("Shard {}: re-registering {} failed; not activating it: {}", name, dir, e.toString());
                    continue;
                }
            }
            if (cache.active(name).filter(dir::equals).isPresent()) continue;
            cache.activate(name, dir);
            changed.add(name);
        }
        if (!changed.isEmpty()) log.info("Activated registered generations of {}", changed);
        return changed;
    }

    /** A materialised generation dir this reader can serve (a lazy one only in lazy mode). */
    private boolean completeGeneration(Path dir) {
        return GEN_DIR.matcher(dir.getFileName().toString()).matches()
                && Files.isDirectory(dir.resolve(ReplicaSyncer.LUCENE_DIR))
                && Files.isRegularFile(dir.resolve(ReplicaSyncer.MANIFEST_FILE))
                && (lazy != null || !LazyGenerations.isLazy(dir));
    }

    /** The highest-numbered complete generation dir of a shard whose index format this build serves. */
    private Optional<Path> newestServableGeneration(Path shardRoot) {
        if (!Files.isDirectory(shardRoot)) return Optional.empty();
        try (var s = Files.list(shardRoot)) {
            return s.map(p -> p.toAbsolutePath().normalize())
                    .filter(this::completeGeneration)
                    .filter(p -> IndexFormat.servable(IndexFormat.status(p)))
                    .max(java.util.Comparator.comparingLong(
                            p -> Long.parseLong(p.getFileName().toString().substring("gen-".length()))));
        } catch (IOException e) {
            log.warn("Listing the generations in {} failed: {}", shardRoot, e.toString());
            return Optional.empty();
        }
    }

    /** Readers: one sync pass now (in addition to the polling); a follower re-reads the registry. */
    public void syncNow() throws IOException {
        ReplicaSyncer s;
        synchronized (this) {
            if (!serving) throw new IllegalStateException("not serving: call startServing() on a reader first");
            s = syncer;
        }
        if (s == null) {
            activateRegistered();
            return;
        }
        s.syncOnce();
    }

    /** Generation served per shard; empty unless this is the syncing reader. */
    public synchronized Map<ShardId, Generation> served() {
        return syncer == null ? Map.of() : syncer.served();
    }

    /** Opens the projects' readers so the first query does not pay for it. */
    private void warm(List<String> projects) {
        for (String project : projects) {
            try {
                luceneIndexer.openReader(project);
            } catch (IOException | RuntimeException e) {
                log.warn("Shard {}: opening the index reader failed: {}", project, e.toString());
            }
        }
    }

    @Override
    public void close() {
        ReplicaSyncer s;
        SyncLock lock;
        ScheduledExecutorService f;
        Thread first;
        synchronized (this) {
            if (closed) return;
            closed = true;
            s = syncer;
            syncer = null;
            lock = syncLock;
            syncLock = null;
            f = follower;
            follower = null;
            first = firstSyncThread;
        }
        if (f != null) f.shutdownNow();
        if (s != null) s.close();
        if (first != null && first != Thread.currentThread()) {
            try {
                first.join(10_000); // keep the lock until an in-flight first pass has stopped
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (lock != null) {
            try {
                lock.close();
            } catch (IOException e) {
                log.warn("Releasing the sync lock failed: {}", e.toString());
            }
        }
        if (lazy != null) lazy.close();
        try {
            backend.close();
        } catch (IOException | RuntimeException e) {
            log.warn("Closing the {} backend failed: {}", backend.kind().json(), e.toString());
        }
    }
}
