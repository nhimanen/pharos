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
 * materialises them again), and its syncing process deletes the block cache lazy mode left in the
 * cache dir when no process holds that cache's lock.
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

    private com.pharos.cluster.membership.ReaderRegistry readerRegistry;
    /** The indexer a work-queue writer's loop runs; set by Main. */
    private volatile com.pharos.indexer.ProjectIndexManager indexManager;
    /** The work queue of a writer with {@code cluster.writer.workQueue: true} while it runs. Guarded by this. */
    private com.pharos.cluster.writer.WorkLoop workLoop;
    /** Removes the writer's signal-time stop step. Guarded by this. */
    private AutoCloseable writerStopStep;
    /** {@code <workDir>/.work-loop.lock} while this process runs the work loop. */
    private SyncLock workLoopLock;
    /** The coordinator's cached shard catalog (its age goes into the heartbeat); null until built. */
    private volatile com.pharos.cluster.routing.CachedCatalog coordinatorCatalog;
    /** {@code /cluster/status} and {@code /cluster/events}; built on first use. Guarded by this. */
    private com.pharos.cluster.status.StatusService statusService;
    /** The fan-out coordinator, once {@link #internalApi(com.pharos.search.SearchEngine)} built it. */
    private com.pharos.search.distributed.DistributedSearch distributed;
    /** How often the coordinator re-reads the shard catalog (in the background) for "every project" queries. */
    static final Duration CATALOG_CACHE = Duration.ofSeconds(5);
    private volatile com.pharos.cluster.membership.LoadCounter load = new com.pharos.cluster.membership.LoadCounter();
    private ReplicaSyncer syncer;
    private SyncLock syncLock;
    private ScheduledExecutorService follower;
    private Thread firstSyncThread;
    private boolean serving;
    /**
     * True when this process serves {@code /internal/*} (the {@code pharos web} reader): only then does
     * it join reader membership (heartbeat) and register the shutdown drain. Guarded by this.
     */
    private boolean advertised;
    /** Re-reads the coordinator's shard catalog every {@link #CATALOG_CACHE}; null until a coordinator exists. */
    private ScheduledExecutorService catalogTimer;
    private volatile boolean ready;
    private boolean closed;
    /** The search models' warm-up readiness also waits for; settled at once unless given. */
    private java.util.concurrent.CompletionStage<?> models = java.util.concurrent.CompletableFuture.completedFuture(null);
    // Guarded by this: the first sync is served; and the readiness line it logs once ready.
    private boolean firstSyncServed;
    private String servedLine;

    public ClusterRuntime(IndexConfig config, ReplicationBackend backend, ProjectRegistry registry,
                          LuceneIndexer luceneIndexer) {
        this.config = Objects.requireNonNull(config, "config");
        this.backend = Objects.requireNonNull(backend, "backend");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.luceneIndexer = Objects.requireNonNull(luceneIndexer, "luceneIndexer");
        if (isReader() && config.getCluster().getReader().isLazy()) {
            ClusterConfig.ObjectStoreConfig store = config.getCluster().getObjectStore();
            lazy = new LazyGenerations(cacheDir(), config.getCluster().getReader(), backend.source(),
                    store == null ? null : store.readTotalTimeout());
            lazy.setActiveGenerations(this::activeGeneration);
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

    /**
     * Why this reader can't answer every query (served by {@code /health} as 503 {@code degraded}),
     * or null when it can: a lazy reader whose active generation was garbage-collected on the bucket.
     */
    public String degradation() {
        return lazy == null ? null : lazy.degradation();
    }

    /**
     * The shards this reader can't serve fully (sorted; empty when none): a lazy reader's shards
     * whose active generation was garbage-collected on the bucket. Reported per shard by
     * {@code /health} and the heartbeat, so routers keep sending the node its other shards.
     */
    public List<String> degradedShards() {
        return lazy == null ? List.of() : lazy.degradedShards();
    }

    /** Why {@code shard} is degraded on this reader, or null when it is not. */
    public String degradation(String shard) {
        return lazy == null ? null : lazy.degradation(shard);
    }

    /** The generation number of the shard's active generation dir; empty when none is active. */
    private java.util.OptionalLong activeGeneration(String shard) {
        if (!(backend.localCache() instanceof ReaderLocalCache cache)) return java.util.OptionalLong.empty();
        return cache.active(shard).map(p -> generationOf(p.getFileName().toString()))
                .orElse(java.util.OptionalLong.empty());
    }

    private static java.util.OptionalLong generationOf(String dirName) {
        if (!GEN_DIR.matcher(dirName).matches()) return java.util.OptionalLong.empty();
        return java.util.OptionalLong.of(Long.parseLong(dirName.substring("gen-".length())));
    }

    /** The requests a drain waits for (the web server's in-flight count); none until set. */
    private volatile java.util.function.IntSupplier inFlight = () -> 0;
    private volatile boolean draining;
    /** Removes this runtime's signal-time drain step; set while serving. Guarded by this. */
    private AutoCloseable drainStep;

    /** Where a drain reads the requests still being served (the web server's {@code inFlightRequests}). */
    public void setInFlight(java.util.function.IntSupplier inFlight) {
        this.inFlight = Objects.requireNonNull(inFlight, "inFlight");
    }

    /** True once {@link #drain} or {@link #drainAndClose} ran: {@code /health} answers 503 draining. */
    public boolean draining() {
        return draining;
    }

    /**
     * How long a router needs to notice a draining reader: one {@code /health} poll interval plus
     * the probe's timeout ({@code pharos router}: 2 s + 2 s).
     */
    public static final Duration ROUTER_NOTICE = Duration.ofSeconds(4);
    /**
     * Added to one {@code heartbeatSeconds} in the drain's notice floor: a coordinator's reader list
     * lags our draining write by up to one of its ticks plus its bucket list and get latency.
     */
    static final Duration HEARTBEAT_NOTICE_MARGIN = Duration.ofSeconds(2);

    /** The least a drain stays up after announcing: {@code max(ROUTER_NOTICE, heartbeatSeconds + 2 s)}. */
    static Duration drainNotice(int heartbeatSeconds) {
        Duration coordinators = Duration.ofSeconds(heartbeatSeconds).plus(HEARTBEAT_NOTICE_MARGIN);
        return coordinators.compareTo(ROUTER_NOTICE) > 0 ? coordinators : ROUTER_NOTICE;
    }

    /** In-flight requests must stay at 0 this long before a drain closes. */
    static final Duration FINAL_QUIET = Duration.ofSeconds(1);

    /**
     * Shutdown of a serving reader (run first on a signal by {@link com.pharos.ShutdownSequence},
     * and by the JVM shutdown hook as the fallback): announces {@code draining} at once (its
     * heartbeat, so coordinators stop routing shard calls to it, and {@code /health} 503, so the
     * router and load balancers stop sending queries), then keeps serving: it stays up at least
     * {@link #drainNotice} ({@code max(4 s, heartbeatSeconds + 2 s)}) so others can see the draining
     * state, and until no request has been in flight for {@link #FINAL_QUIET}; everything is capped
     * by {@code cluster.drainSeconds}. Requests arriving meanwhile are served. Then it
     * {@link #close}s. Any other node, or one not serving, closes at once. Idempotent.
     */
    public void drainAndClose() {
        boolean serve;
        synchronized (this) {
            serve = isReader() && serving && advertised && !closed;
        }
        if (serve) {
            drain();
            ClusterConfig c = config.getCluster();
            long start = System.nanoTime();
            long max = TimeUnit.SECONDS.toNanos(c.getDrainSeconds());
            long notice = Math.min(max, drainNotice(c.getHeartbeatSeconds()).toNanos());
            long quiet = FINAL_QUIET.toNanos();
            log.info("Draining: announced; stopping when no request has been in flight for {} ms, after at least"
                    + " {} ms, at most {} s (cluster.drainSeconds)", FINAL_QUIET.toMillis(),
                    TimeUnit.NANOSECONDS.toMillis(notice), c.getDrainSeconds());
            long idleSince = -1;
            int busy = 0;
            while (true) {
                long now = System.nanoTime();
                busy = inFlight.getAsInt();
                if (busy > 0) {
                    idleSince = -1;
                } else if (idleSince < 0) {
                    idleSince = now;
                }
                if (now - start >= max) break;
                if (now - start >= notice && idleSince >= 0 && now - idleSince >= quiet) break;
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (busy > 0) {
                log.warn("Draining: stopping after {} s with {} request(s) still in flight (cluster.drainSeconds)",
                        c.getDrainSeconds(), busy);
            } else {
                log.info("Drained: no request in flight; stopping");
            }
        }
        close();
    }

    /** Where the heartbeat takes {@code inFlight}/{@code qps} from (the web server's counter). */
    public void setLoad(com.pharos.cluster.membership.LoadCounter load) {
        this.load = Objects.requireNonNull(load, "load");
    }

    /** The live readers of the cluster; empty unless this reader is serving with a registry. */
    public List<com.pharos.cluster.membership.ReaderRegistry.LiveReader> liveReaders() {
        com.pharos.cluster.membership.ReaderRegistry r;
        synchronized (this) { r = readerRegistry; }
        return r == null ? List.of() : r.liveWithAge();
    }

    /**
     * Marks this reader draining: {@code /health} answers 503 {@code draining} from now on, and the
     * heartbeat (readers with a registry) is rewritten with {@code draining} at once.
     */
    public void drain() {
        draining = true;
        stopCatalogTimer(); // queries still refresh on demand while the drain runs
        com.pharos.cluster.membership.ReaderRegistry r;
        synchronized (this) { r = readerRegistry; }
        if (r != null) r.drain();
    }

    com.pharos.cluster.membership.ReaderHeartbeat selfHeartbeat() {
        ClusterConfig c = config.getCluster();
        // The shards actually held, so routing never guesses what "all" means; a pinned reader
        // advertises only what it holds of its pin.
        java.util.TreeSet<String> names = new java.util.TreeSet<>();
        synchronized (this) {
            if (syncer != null) syncer.served().keySet().forEach(id -> names.add(id.value()));
        }
        registry.listAll().forEach(m -> { if (m.getName() != null) names.add(m.getName()); });
        List<String> pin = c.getReader().getShards();
        if (!pin.isEmpty()) names.retainAll(pin);
        List<String> shards = new ArrayList<>(names);
        List<String> degraded = degradedShards();
        String state = draining ? com.pharos.cluster.membership.ReaderHeartbeat.DRAINING
                : !ready() ? com.pharos.cluster.membership.ReaderHeartbeat.WARMING
                : degraded.isEmpty() ? com.pharos.cluster.membership.ReaderHeartbeat.READY
                : com.pharos.cluster.membership.ReaderHeartbeat.DEGRADED;
        com.pharos.cluster.routing.CachedCatalog cat = coordinatorCatalog;
        Long catalogAge = cat == null || cat.ageSeconds() < 0 ? null : cat.ageSeconds();
        return new com.pharos.cluster.membership.ReaderHeartbeat(c.effectiveNodeId(), c.getAdvertiseUrl(), shards,
                draining, readerCacheStats(), load.inFlight(), load.qps(), 0L, state, degraded, load.warmP95Millis(),
                catalogAge);
    }

    // Holding the monitor of this.
    private void startMembership() {
        if (!(backend instanceof com.pharos.cluster.objectstore.ObjectStoreBackend os)) return;
        try {
            readerRegistry = new com.pharos.cluster.membership.ReaderRegistry(os.store(), os.layout(),
                    config.getCluster(), this::selfHeartbeat);
            readerRegistry.start();
        } catch (RuntimeException e) {
            log.warn("Starting reader membership failed: {}", e.toString());
        }
    }

    /** The indexer the work loop of a work-queue writer runs (and publishes through). */
    public void setIndexManager(com.pharos.indexer.ProjectIndexManager indexManager) {
        this.indexManager = Objects.requireNonNull(indexManager, "indexManager");
    }

    /** True on {@code role: writer} with {@code cluster.writer.workQueue: true}. */
    public boolean isWorkQueueWriter() {
        ClusterConfig c = config.getCluster();
        return c.getRole() == NodeRole.WRITER && c.getWriter().isWorkQueue();
    }

    /**
     * Why {@code pharos index} / {@code pharos publish} must not publish {@code project} by hand
     * here: on a work-queue writer only the work loop publishes, under the shard's lease. Empty on
     * every other node.
     */
    public Optional<String> manualPublishRefusal(String project) {
        return isWorkQueueWriter()
                ? Optional.of(com.pharos.cluster.writer.WorkQueueLeaseGuard.refusal(project)) : Optional.empty();
    }

    /**
     * Starts the work queue of a work-queue writer ({@code pharos web} on {@code role: writer} with
     * {@code cluster.writer.workQueue: true}): leases, heartbeat {@code catalog/writers/<node>.json}
     * and the polling loop. Its graceful stop runs on a signal before model calls stop (within
     * {@code cluster.drainSeconds}) and on {@link #close}. No-op on every other node and when running.
     *
     * <p>One process per node runs the loop: it holds {@code <workDir>/.work-loop.lock}
     * ({@link #WORK_LOOP_LOCK}, an OS file lock, freed when the process dies). A second process with
     * the same config (the daemon {@code ./pharos} starts on demand next to a systemd writer) would
     * otherwise share the node id, index dir and clones, and run git checkouts under the other's
     * parse. It logs a WARN instead, starts no loop and serves everything else.
     *
     * @return true when the work loop runs (in this process)
     */
    public boolean beginWriting() {
        if (!isWorkQueueWriter() || !(backend instanceof com.pharos.cluster.objectstore.ObjectStoreBackend os)) {
            return false;
        }
        com.pharos.indexer.ProjectIndexManager pim = indexManager;
        if (pim == null) throw new IllegalStateException("no index manager set for the work loop");
        ClusterConfig c = config.getCluster();
        com.pharos.cluster.writer.WorkLoop loop;
        synchronized (this) {
            if (closed) throw new IllegalStateException("cluster runtime is closed");
            if (workLoop != null) return true;
            ClusterConfig.WriterConfig w = c.getWriter();
            Optional<SyncLock> lock;
            try {
                lock = SyncLock.tryAcquire(w.getWorkDir(), WORK_LOOP_LOCK);
            } catch (java.io.IOException e) {
                log.warn("Not running the writer work queue: cannot open {}: {}", w.getWorkDir().resolve(WORK_LOOP_LOCK),
                        e.toString());
                return false;
            }
            if (lock.isEmpty()) {
                log.warn("Not running the writer work queue: another process is the writer for node {} (it holds {});"
                        + " this process serves everything else", c.effectiveNodeId(), w.getWorkDir().resolve(WORK_LOOP_LOCK));
                return false;
            }
            workLoopLock = lock.get();
            com.pharos.cluster.writer.DiskGuard disk = new com.pharos.cluster.writer.DiskGuard(config.getIndexDir(),
                    writerDiskDirs(config), com.pharos.cluster.writer.DiskGuard.Settings.of(w.getDisk()),
                    com.pharos.cluster.writer.DiskGuard.Probe.FILE_STORES);
            loop = new com.pharos.cluster.writer.WorkLoop(new com.pharos.cluster.writer.WorkLoop.Settings(
                    c.effectiveNodeId(), w.leaseTtl(), Duration.ofSeconds(c.getHeartbeatSeconds()),
                    Duration.ofSeconds(w.getPollSeconds()), Duration.ofSeconds(w.getMinHoldSeconds()), true, true,
                    w.getMaxConsecutiveFailures()),
                    os.store(), os.layout(), os.objectStoreCatalog(), os.source(), os.writerState(), pim,
                    luceneIndexer, registry, new com.pharos.cluster.writer.SourceCheckout(w.getWorkDir(),
                            Duration.ofSeconds(w.getLsRemoteTimeoutSeconds())),
                    java.time.Clock.systemUTC(), disk);
            os.workQueueGuard().ifPresent(g -> g.attach(loop));
            workLoop = loop;
        }
        loop.start();
        AutoCloseable step = com.pharos.ShutdownSequence.addDrainStep("writer stop", this::stopWriting);
        synchronized (this) {
            writerStopStep = step;
        }
        return true;
    }

    /**
     * What the writer disk guard watches besides the index directory: the source checkouts, the
     * model cache and, on {@code cluster.backend: peer-pull}, {@code cluster.peers.storeDir}, which
     * holds every retained generation (each counted once per file store).
     */
    public static java.util.List<Path> writerDiskDirs(IndexConfig config) {
        ClusterConfig c = config.getCluster();
        java.util.List<Path> dirs = new java.util.ArrayList<>(java.util.List.of(c.getWriter().getWorkDir(),
                com.pharos.embedding.DjlEmbeddingProvider.modelCacheDir()));
        if (c.getBackend() == BackendKind.PEER_PULL) dirs.add(c.getPeers().getStoreDir());
        return java.util.List.copyOf(dirs);
    }

    /** A peer-mode writer's disk guard (no work loop runs there); built on first use, guarded by this. */
    private com.pharos.cluster.writer.DiskGuard peerDisk;
    private long peerDiskCheckedNanos;

    /**
     * The writer disk guard's view for {@code /health} ({@code freeBytes}, {@code totalBytes},
     * {@code low}): the work loop's guard, or on a peer-mode writer a guard of its own (re-probed at
     * most every {@code cluster.writer.disk.checkSeconds}); null elsewhere.
     */
    public Map<String, Object> writerDisk() {
        com.pharos.cluster.writer.WorkLoop loop;
        synchronized (this) {
            loop = workLoop;
        }
        if (loop != null) {
            return loop.diskStatus().map(com.pharos.cluster.writer.DiskGuard.Status::toMap).orElse(null);
        }
        return peerDiskGuard().map(g -> {
            synchronized (this) {
                long now = System.nanoTime();
                if (now - peerDiskCheckedNanos >= g.settings().check().toNanos()) {
                    peerDiskCheckedNanos = now;
                    return g.check().toMap();
                }
            }
            return g.status().toMap();
        }).orElse(null);
    }

    /** The disk guard of a peer-mode writer, watching {@link #writerDiskDirs}; empty on other nodes. */
    public synchronized Optional<com.pharos.cluster.writer.DiskGuard> peerDiskGuard() {
        if (!isWriter() || config.getCluster().getBackend() != BackendKind.PEER_PULL) return Optional.empty();
        if (peerDisk == null) {
            peerDisk = new com.pharos.cluster.writer.DiskGuard(config.getIndexDir(), writerDiskDirs(config),
                    com.pharos.cluster.writer.DiskGuard.Settings.of(config.getCluster().getWriter().getDisk()),
                    com.pharos.cluster.writer.DiskGuard.Probe.FILE_STORES);
            peerDiskCheckedNanos = System.nanoTime();
        }
        return Optional.of(peerDisk);
    }

    /** The running work loop; empty unless {@link #beginWriting} started one. */
    public synchronized Optional<com.pharos.cluster.writer.WorkLoop> workLoop() {
        return Optional.ofNullable(workLoop);
    }

    /** The work-loop lock's file name in {@code cluster.writer.workDir}. */
    public static final String WORK_LOOP_LOCK = ".work-loop.lock";

    /** Graceful stop of the work loop within {@code cluster.drainSeconds}; idempotent. */
    private void stopWriting() {
        com.pharos.cluster.writer.WorkLoop loop;
        synchronized (this) {
            loop = workLoop;
        }
        if (loop != null) loop.stop(Duration.ofSeconds(config.getCluster().getDrainSeconds()));
    }

    /** Frees the work-loop lock once the loop has stopped (on {@link #close}). */
    private void releaseWorkLoopLock() {
        SyncLock lock;
        synchronized (this) {
            lock = workLoopLock;
            workLoopLock = null;
        }
        if (lock == null) return;
        try {
            lock.close();
        } catch (java.io.IOException e) {
            log.debug("Releasing the work-loop lock failed: {}", e.toString());
        }
    }

    /**
     * The {@code /cluster/status} service of this node, started on first call: built from the bucket
     * (catalog, manifests, leases, writer and reader heartbeats, the shared state
     * {@code catalog/status/state.json}) on an object-store node, from local state on a single node
     * ({@code role: all}: one node holding every shard, nothing written anywhere). Closed by
     * {@link #close}.
     */
    public synchronized com.pharos.cluster.status.StatusService statusService() {
        if (closed) throw new IllegalStateException("cluster runtime is closed");
        if (statusService != null) return statusService;
        ClusterConfig c = config.getCluster();
        ClusterConfig.StatusConfig sc = c.getStatus();
        com.pharos.cluster.status.StatusSource source;
        com.pharos.cluster.status.StatusStateStore states;
        if (backend instanceof com.pharos.cluster.objectstore.ObjectStoreBackend os) {
            source = new com.pharos.cluster.status.BucketStatusSource(os.store(), os.layout(), os.objectStoreCatalog(),
                    os.source(), Duration.ofSeconds(c.getReaderTtlSeconds()), c.getWriter().leaseTtl(),
                    java.time.Clock.systemUTC());
            states = com.pharos.cluster.status.StatusStateStore.bucket(os.store(), os.layout());
        } else {
            source = new com.pharos.cluster.status.LocalStatusSource(c.getNodeId() == null ? "local" : c.getNodeId(),
                    registry, luceneIndexer, () -> load, java.time.Clock.systemUTC());
            states = com.pharos.cluster.status.StatusStateStore.memory();
        }
        String hook = sc.getWebhookUrl();
        var events = new com.pharos.cluster.status.StatusEvents(Duration.ofSeconds(sc.getEventsKeepAliveSeconds()),
                hook == null || hook.isBlank() ? null : new com.pharos.cluster.status.Webhook(hook));
        // A single node runs no thread of its own: on demand, cached for heartbeatSeconds, and the refresh
        // timer only while an SSE client is connected (the global "single node unchanged" constraint).
        boolean cluster = backend instanceof com.pharos.cluster.objectstore.ObjectStoreBackend;
        Duration every = Duration.ofSeconds(c.getHeartbeatSeconds());
        statusService = new com.pharos.cluster.status.StatusService(source, states,
                com.pharos.cluster.status.StatusSettings.of(sc), events, every,
                cluster ? STATUS_MAX_AGE : every, System::nanoTime, cluster);
        statusService.start();
        return statusService;
    }

    /** A {@code /cluster/status} request older than this rebuilds the status first (cluster nodes). */
    static final Duration STATUS_MAX_AGE = Duration.ofSeconds(2);

    /**
     * A constant-time check of a presented {@code X-Pharos-Cluster-Token} against {@code cluster.token}
     * (never matches without one); the token itself does not leave the config.
     */
    public java.util.function.Predicate<String> clusterTokenMatcher() {
        return com.pharos.cluster.internal.InternalApi.tokenMatcher(config.getCluster().getToken());
    }

    /** The writable shard catalog (shard sources); empty on a backend without one (single node). */
    public Optional<com.pharos.cluster.objectstore.ObjectStoreCatalog> sourceCatalog() {
        return backend instanceof com.pharos.cluster.objectstore.ObjectStoreBackend os
                ? Optional.of(os.objectStoreCatalog()) : Optional.empty();
    }

    public IndexPublisher publisher() {
        return backend.publisher();
    }

    /** Built on first use; guarded by this. */
    private com.pharos.cluster.peer.ReplicationEndpoint replication;

    /**
     * The {@code /replication/*} routes of a peer-mode writer ({@code cluster.backend: peer-pull},
     * {@code role: writer}), serving its local store; empty on every other node. Its long-polls are
     * woken when the backend closes.
     */
    public synchronized Optional<com.pharos.cluster.peer.ReplicationEndpoint> replicationEndpoint() {
        if (!isWriter() || !(backend instanceof com.pharos.cluster.peer.PeerBackend peer)) return Optional.empty();
        if (replication == null) replication = peer.endpoint();
        return Optional.of(replication);
    }

    /** {@code cluster.tls} when it configures a server keystore; empty otherwise. */
    public Optional<ClusterConfig.TlsConfig> serverTls() {
        ClusterConfig c = config.getCluster();
        return c.tlsServer() ? Optional.of(c.getTls()) : Optional.empty();
    }

    /**
     * The {@code /internal/*} API of this reader, token-guarded; null on other roles. Its shard
     * search service is set once the raw retrieval exists ({@code InternalApi#service}); until then
     * the endpoints answer 503 and coordinators fail over.
     */
    public com.pharos.cluster.internal.InternalApi internalApi() {
        return isReader() ? new com.pharos.cluster.internal.InternalApi(config.getCluster().getToken()) : null;
    }

    /**
     * How long a query waits for a catalog read in flight when the held catalog is stale but no read
     * failed: 500 ms, and never more than a tenth of the query budget.
     */
    static Duration catalogWait(long queryTimeoutMillis) {
        return Duration.ofMillis(Math.max(0, Math.min(500, queryTimeoutMillis / 10)));
    }

    /** The coordinator's catalog timer while it runs; null otherwise (tests). */
    synchronized ScheduledExecutorService catalogTimer() {
        return catalogTimer;
    }

    private void stopCatalogTimer() {
        ScheduledExecutorService t;
        synchronized (this) {
            t = catalogTimer;
            catalogTimer = null;
        }
        if (t != null) t.shutdownNow();
    }

    /**
     * Readers: the {@code /internal/*} API with {@code engine}'s shard search behind it, and the
     * engine made a coordinator for shards this reader does not hold (Phase 3 fan-out): it routes over
     * the live readers of the bucket, with suspect marking, the cluster token and the configured
     * fan-out and connect timeouts. A query whose shards are all held here keeps the local path.
     * Null on other roles, which never build a Phase 3 class.
     */
    public com.pharos.cluster.internal.InternalApi internalApi(com.pharos.search.SearchEngine engine) {
        if (!isReader()) return null;
        ClusterConfig c = config.getCluster();
        com.pharos.search.distributed.LocalShardService service =
                engine.shardService(this::servesShard, modelFingerprint(engine.embedder().modelId()));
        var health = new com.pharos.cluster.membership.ReaderHealth(c.getReaderTtlSeconds());
        var client = new com.pharos.cluster.internal.ShardClient(health, c.getToken(),
                Duration.ofMillis(c.getConnectTimeoutMillis()), Duration.ofMillis(c.getFanoutTimeoutMillis()));
        var catalog = new com.pharos.cluster.routing.CachedCatalog(backend.catalog(), CATALOG_CACHE,
                Duration.ofSeconds(c.getMaxCatalogAgeSeconds()), System::nanoTime,
                r -> Thread.ofVirtual().name("pharos-catalog-refresh").start(r),
                c.getObjectStore() == null ? Duration.ofSeconds(60)
                        : c.getObjectStore().readTotalTimeout().multipliedBy(2), // the store's own bound, with slack
                catalogWait(c.effectiveQueryTimeoutMillis()));
        coordinatorCatalog = catalog;
        var router = new com.pharos.cluster.routing.ShardRouter(this::liveHeartbeats, catalog, health,
                id -> servesShard(id.value()), client::inFlight);
        var ds = new com.pharos.search.distributed.DistributedSearch(router, client, service, engine.embedder(),
                engine.vectorStrategy(), Duration.ofMillis(c.effectiveQueryTimeoutMillis()), catalog::staleSeconds);
        com.pharos.search.distributed.DistributedSearch previous;
        ScheduledExecutorService previousTimer;
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-catalog-timer");
            t.setDaemon(true);
            return t;
        });
        synchronized (this) {
            previous = distributed;
            distributed = ds;
            previousTimer = catalogTimer;
            catalogTimer = null;
            if (!closed && !draining) { // an idle coordinator keeps its catalog fresh (no first-query wait)
                long every = CATALOG_CACHE.toMillis();
                timer.scheduleWithFixedDelay(catalog::tick, every, every, TimeUnit.MILLISECONDS);
                catalogTimer = timer;
            }
        }
        if (catalogTimer() != timer) timer.shutdownNow();
        if (previousTimer != null) previousTimer.shutdownNow();
        if (previous != null) previous.close();
        engine.setDistributed(ds);
        engine.setLocalDegradation(this::degradation);
        return new com.pharos.cluster.internal.InternalApi(c.getToken()).service(service);
    }

    /**
     * The fingerprint of the configured embedding provider whose model id is {@code modelId}
     * ({@code IndexVersions.modelFingerprint}); null when none is configured under that id.
     */
    private String modelFingerprint(String modelId) {
        for (var p : config.getEmbeddingProviders()) {
            if (modelId != null && modelId.equals(p.getModelId())) return IndexVersions.modelFingerprint(p);
        }
        return null;
    }

    /** The fan-out coordinator built by {@link #internalApi(com.pharos.search.SearchEngine)}; null before. */
    public synchronized com.pharos.search.distributed.DistributedSearch distributedSearch() {
        return distributed;
    }

    /** True when this reader serves {@code shard}: it has an active generation of it. */
    public boolean servesShard(String shard) {
        return backend.localCache() instanceof ReaderLocalCache cache && cache.active(shard).isPresent();
    }

    private List<com.pharos.cluster.membership.ReaderHeartbeat> liveHeartbeats() {
        com.pharos.cluster.membership.ReaderRegistry r;
        synchronized (this) { r = readerRegistry; }
        return r == null ? List.of() : r.live();
    }

    /** {@code role: writer}: the node indexes and publishes, and serves no search of its own. */
    public boolean isWriter() {
        return config.getCluster().getRole() == NodeRole.WRITER;
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
        startServing(false);
    }

    /**
     * {@link #beginServing(java.util.concurrent.CompletionStage, boolean)} with no models to wait for,
     * then waits for the first sync pass. {@code advertise}: see there.
     */
    public void startServing(boolean advertise) throws IOException {
        beginServing(java.util.concurrent.CompletableFuture.completedFuture(null), advertise);
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
        beginServing(java.util.concurrent.CompletableFuture.completedFuture(null));
    }

    /**
     * {@link #beginServing()}, with readiness also waiting for {@code modelsLoaded}: a reader is
     * {@link #ready} (and logs "Reader … serving N shard(s)") only once its first sync is served
     * AND the search models have settled. Load balancers and the router send traffic on that
     * signal, so a reader never takes queries that would stall on a multi-second model load. The
     * stage completing exceptionally counts as settled: a model that fails to load never blocks
     * readiness (the caller bounds a hanging load with a timeout).
     */
    public void beginServing(java.util.concurrent.CompletionStage<?> modelsLoaded) throws IOException {
        beginServing(modelsLoaded, false);
    }

    /**
     * {@link #beginServing(java.util.concurrent.CompletionStage)}; with {@code advertise} the process
     * also joins reader membership (its heartbeat in {@code catalog/readers/<node>.json}, so
     * coordinators and the router send it shard calls and queries) and registers the shutdown drain.
     * Only a process that serves {@code /internal/*} may advertise: {@code pharos web}. A
     * {@code pharos mcp-server} on a reader does not: it has no {@code /internal/*} behind the
     * advertise URL, and on a shared config it would overwrite, mark draining and delete the web
     * reader's heartbeat (same node id). It searches only the shards this reader holds.
     */
    public void beginServing(java.util.concurrent.CompletionStage<?> modelsLoaded, boolean advertise)
            throws IOException {
        java.util.Objects.requireNonNull(modelsLoaded, "modelsLoaded");
        synchronized (this) {
            if (!isReader() || serving) return;
            if (closed) throw new IllegalStateException("cluster runtime is closed");
            models = modelsLoaded;
            advertised = advertise;
            ReaderLocalCache cache = readerCache();
            Optional<SyncLock> lock = SyncLock.tryAcquire(cacheDir());
            serving = true;
            if (lock.isPresent()) {
                becomeOwner(lock.get(), cache);
            } else {
                becomeFollower();
            }
            if (advertise) startMembership();
        }
        if (advertise) {
            // On a signal the drain runs before any shutdown hook (ONNX Runtime's environment closer
            // among them), while queries that call the models are still served.
            AutoCloseable step = com.pharos.ShutdownSequence.addDrainStep("reader drain", this::drainAndClose);
            synchronized (this) {
                drainStep = step;
            }
        }
        modelsLoaded.whenComplete((r, e) -> maybeBecomeReady());
    }

    /** The first sync is served; ready now if the models have settled too. */
    private void firstSyncServed(String line) {
        synchronized (this) {
            firstSyncServed = true;
            servedLine = line;
        }
        maybeBecomeReady();
    }

    private void maybeBecomeReady() {
        String line;
        synchronized (this) {
            if (ready || !firstSyncServed || !models.toCompletableFuture().isDone()) return;
            ready = true;
            line = servedLine;
        }
        log.info(line);
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

    /**
     * False while a serving reader's first sync pass is still running or its search models are
     * still loading (see {@link #beginServing(java.util.concurrent.CompletionStage)}); true otherwise.
     */
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
        if (lazy == null) {
            LazyGenerations.deleteUnusedBlockCaches(cacheDir()); // left by lazy mode; full mode never reads it
        } else {
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
        s.setRetireGrace(Duration.ofSeconds(config.getCluster().getRetireGraceSeconds()));
        s.pin(config.getCluster().getReader().getShards().stream().map(ShardId::new).toList());
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
            warm(served.keySet().stream().map(ShardId::value).toList());
            String line = "Reader " + config.getCluster().effectiveNodeId() + " serving " + served.size()
                    + " shard(s): " + served;
            if (ready) {
                log.info(line); // a follower that took over syncing: already ready
            } else {
                firstSyncServed(line);
            }
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
        List<String> activated = activateRegistered();
        warm(activated);
        firstSyncServed("Reader " + config.getCluster().effectiveNodeId() + " serving " + activated.size()
                + " shard(s): " + activated + " (following the sync lock holder)");
        firstSync.countDown();
        long interval = config.getCluster().getPollIntervalSeconds();
        follower = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-replica-follower");
            t.setDaemon(true);
            return t;
        });
        follower.scheduleWithFixedDelay(this::followerTick, interval, interval, TimeUnit.SECONDS);
    }

    /** Re-reads {@code cluster.reader.shards} into the sync owner's pin (takes effect on its next pass). */
    public void repin() {
        ReplicaSyncer s;
        synchronized (this) { s = syncer; }
        if (s != null) s.pin(config.getCluster().getReader().getShards().stream().map(ShardId::new).toList());
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
            List<String> onlyPinned = config.getCluster().getReader().getShards();
            if (!onlyPinned.isEmpty() && !onlyPinned.contains(name)) continue;
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
        // A shard the sync owner unregistered (or that the pin excludes) must stop being served here
        // before the owner deletes its directory.
        java.util.Set<String> registered = new java.util.HashSet<>();
        registry.listAll().forEach(m -> { if (m.getName() != null) registered.add(m.getName()); });
        List<String> pin = config.getCluster().getReader().getShards();
        for (String name : cache.activeProjects()) {
            if (registered.contains(name) && (pin.isEmpty() || pin.contains(name))) continue;
            cache.deactivate(name);
            log.info("Shard {} is no longer registered for this reader: stopped serving it", name);
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
        AutoCloseable step;
        com.pharos.cluster.membership.ReaderRegistry members;
        synchronized (this) {
            if (closed) return;
            closed = true;
            members = readerRegistry;
            readerRegistry = null;
            s = syncer;
            syncer = null;
            lock = syncLock;
            syncLock = null;
            f = follower;
            follower = null;
            first = firstSyncThread;
            step = drainStep;
            drainStep = null;
        }
        if (step != null) {
            try {
                step.close();
            } catch (Exception e) {
                // removing from a list: cannot fail
            }
        }
        AutoCloseable writerStep;
        synchronized (this) {
            writerStep = writerStopStep;
            writerStopStep = null;
        }
        if (writerStep != null) {
            try {
                writerStep.close();
            } catch (Exception e) {
                // removing from a list: cannot fail
            }
        }
        stopWriting(); // before the backend (the bucket client) closes
        releaseWorkLoopLock();
        if (members != null) members.close();
        com.pharos.cluster.status.StatusService status;
        synchronized (this) {
            status = statusService;
            statusService = null;
        }
        if (status != null) status.close();
        stopCatalogTimer();
        com.pharos.search.distributed.DistributedSearch ds;
        synchronized (this) {
            ds = distributed;
            distributed = null;
        }
        if (ds != null) ds.close();
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
