package com.pharos.cluster.writer;

import com.pharos.cluster.Generation;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardEntry;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.WriterStateStore;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.sharding.ProjectShards;
import com.pharos.cluster.sharding.ShardPlan;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.IndexAbort;
import com.pharos.indexer.IndexAbortedException;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The work queue of one writer ({@code cluster.writer.workQueue: true}): it holds shard leases up to
 * its fair share ({@link Balancer}) and keeps those shards published. Every
 * {@code cluster.writer.pollSeconds} ({@link #tick}):
 * <ol>
 *   <li>beat (heartbeat, live writers) and renew the held leases;</li>
 *   <li>read the catalog, take this writer's pinned shards and acquire free or expired leases up to
 *       the share;</li>
 *   <li>poll each held shard's source against the revision {@code LATEST} holds
 *       ({@link SourceCheckout#changedSince}); a shard whose revision differs is pending;</li>
 *   <li>run the pending shards <b>one at a time</b>, oldest unpublished change first: restore the
 *       writer state when this writer has no local index of the shard, check out the source, run an
 *       incremental index (full when there is nothing to build on), whose publish runs the lease
 *       check ({@link WorkQueueLeaseGuard}) and backs up the writer state; then renew the lease and,
 *       if this writer holds more than its share, release that shard (not within {@code minHold});</li>
 *   <li>release surplus shards that have nothing unpublished (a clean point too).</li>
 * </ol>
 *
 * <p><b>Work units</b> (ruling P6-R1). The loop works on projects, as the catalog's project mapping
 * ({@link ProjectShards}) lists them; a unit is keyed by {@code ShardId.ofProject(project)}. A
 * {@code [project]} project is one shard, so its unit is that shard and everything below reads exactly
 * as in Phase 4. A project split into N shards is one unit too: one source, one checkout, one local
 * index dir, one run (one parse pass feeding N shard writers) and one publish of all N shards; the
 * balancer counts it once. Its N shard leases are taken all or none (in id order; a partial acquisition
 * is released at once), renewed and released together, and a unit whose leases are not all held any
 * more (one was lost) is dropped and its remaining leases released. A split project's source is that
 * of its first shard with one, else that of the catalog entry named after the project (its source
 * before the split). When the catalog maps a held project to other shards (a reshard flip), its leases
 * are released and the new set is acquired like any free unit.
 *
 * <p><b>Shards without a source</b> (indexed by hand before work-queue mode) can only be indexed
 * where their files are. A writer whose registry has such a shard with an existing root directory
 * <i>pins</i> it: it takes the shard's lease whenever free (a real lease, so two writers that both
 * have the path cannot both publish), never releases it for balance, polls its root like a path
 * source, and announces it in {@code pinned}. Other writers never try to acquire it; the balancer
 * counts it as workable and in the pinning writer's share. A source-less shard nobody pins is not
 * workable: it doesn't inflate the share.
 *
 * <p><b>Restore</b> ({@link WriterStateStore#restore}) runs only on the loop thread, between runs
 * (one shard at a time), and only while {@link LuceneIndexer#indexExists} is false for the shard
 * ({@link #requireRestorable}): with no local index no reader or writer can have it open. After a
 * restore the project is registered with the manifest's meta under this writer's checkout and index
 * paths. {@code file-state.json} and the Lucene {@code filePath} field hold absolute paths: when the
 * restored (or local) state was built under another root than this writer's checkout, the run is a
 * FULL index (embeddings still come from the restored cache), so no document stays behind under the
 * old paths. The same holds when the restored writer state is older than {@code LATEST} (its backup
 * was lost or never uploaded): its file state doesn't describe the restored documents. A store
 * failure during restore is retried on the next poll (never a full re-embed); a missing or unusable
 * backup makes the run a full index.
 *
 * <p><b>Same node id after a restart:</b> the old process's leases name this node but their ETags
 * are unknown to the new process, so it waits them out (one TTL) like any other writer's. This is
 * deliberate: the old process may still be alive (a stuck JVM) and publishing.
 *
 * <p><b>Disk guard</b> ({@link DiskGuard}, optional): probed at every tick and every
 * {@code cluster.writer.disk.checkSeconds} on its own thread. When the disk is low, local leftovers
 * are garbage-collected first ({@link #collectLeftovers}); if it stays low the writer acquires no
 * lease, starts no run, aborts the run in progress at its next safe point (the {@link IndexAbort}
 * of a graceful stop: never a half commit), releases the leases of the shards it cannot index (not
 * pinned ones, never mid-run) and keeps renewing the shards whose last publish is still served. Its
 * heartbeat says {@code disk.low}, so the balancer of every writer stops counting it. Before each
 * run the guard also checks the run's estimated peak ({@link DiskGuard#admits}); a run that would
 * cross the threshold is refused and its shard released like a low writer's. A released shard is
 * not re-acquired by this writer for {@code max(minHold, poll)}.
 *
 * <p><b>Failing shards</b>: a surplus shard (not pinned, past {@code minHold}) whose runs or source
 * checks failed {@code cluster.writer.maxConsecutiveFailures} times in a row is released so another
 * writer can try it. A surplus shard is released when idle only after a poll that succeeded. Source
 * checks run per shard, in parallel, under a per-shard lock: an unreachable remote (its
 * {@code ls-remote} killed after {@code lsRemoteTimeoutSeconds}) stalls only its own shard.
 *
 * <p><b>Graceful stop</b> ({@link #stop}): no new run starts; a run in progress gets until the
 * budget minus a reserve to finish, then is aborted at a safe point ({@link IndexAbort}; a run past
 * its commit finishes, publish included); then every lease is released (before the lease manager is
 * closed, after which a release can no longer write) and the heartbeat is deleted.
 */
public final class WorkLoop implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WorkLoop.class);

    /**
     * Marker in a shard's index dir: its file state doesn't describe its documents (restored from an
     * older generation's backup), so its next run is FULL. Removed after a full run publishes.
     */
    static final String FULL_RUN_REQUIRED = ".full-run-required";

    /** Timing and run options. */
    public record Settings(String node, Duration leaseTtl, Duration heartbeat, Duration poll, Duration minHold,
                           boolean embed, boolean buildSynonyms, int maxConsecutiveFailures) {
        /** {@code cluster.writer.maxConsecutiveFailures} default. */
        public static final int DEFAULT_MAX_CONSECUTIVE_FAILURES = 3;

        public Settings {
            Objects.requireNonNull(node, "node");
            Objects.requireNonNull(leaseTtl, "leaseTtl");
            Objects.requireNonNull(heartbeat, "heartbeat");
            Objects.requireNonNull(poll, "poll");
            Objects.requireNonNull(minHold, "minHold");
            if (maxConsecutiveFailures < 1) {
                throw new IllegalArgumentException("maxConsecutiveFailures must be at least 1: " + maxConsecutiveFailures);
            }
        }

        public Settings(String node, Duration leaseTtl, Duration heartbeat, Duration poll, Duration minHold,
                        boolean embed, boolean buildSynonyms) {
            this(node, leaseTtl, heartbeat, poll, minHold, embed, buildSynonyms, DEFAULT_MAX_CONSECUTIVE_FAILURES);
        }
    }

    /** What one attempt to run a shard came to. */
    enum Outcome { PUBLISHED, FAILED, ABORTED, CONFLICT, REFUSED }

    /** What the loop knows about one held shard. Written by the loop thread, read by the heartbeat. */
    private static final class ShardState {
        /** The unit's plan when its leases were acquired ({@link ShardPlan#single} for {@code [project]}). */
        volatile ShardPlan plan;
        /** The shard leases this unit holds: the unit itself for {@code [project]}, else its N shards. */
        volatile List<ShardId> leaseShards = List.of();
        /** The {@code LATEST} generation of each lease shard when {@link #publishedRevision} was read. */
        volatile List<Generation> publishedGens;
        volatile ShardSource source;
        volatile boolean pinned;
        volatile boolean prepared;
        volatile String sourceRevision;
        volatile String publishedRevision;
        volatile Generation publishedGeneration;
        volatile Instant pendingSince;
        volatile Instant acquiredAt;
        /** The last poll of this shard succeeded: its {@code pendingSince} is current (M5). */
        volatile boolean lastPollOk;
        /**
         * Failed runs and failed source checks in a row (reset by a publish, and by a check that
         * finds the shard healthy and idle); M4 releases a surplus shard at the limit.
         */
        final java.util.concurrent.atomic.AtomicInteger consecutiveFailures = new java.util.concurrent.atomic.AtomicInteger();
        /**
         * Guards this shard's poll state ({@code sourceRevision}, {@code publishedRevision},
         * {@code publishedGeneration}, {@code pendingSince}, {@code lastPollOk}, {@code upgrade})
         * across the loop thread and the change detector. Per shard: a check that hangs on one
         * unreachable remote (up to {@code lsRemoteTimeoutSeconds}) never holds up another shard.
         */
        final ReentrantLock pollLock = new ReentrantLock();
        /** The index format of the {@code LATEST} manifest ({@link #publishedGeneration}); -1 when unknown. */
        volatile int publishedFormat = -1;
        /** Why the local index needs a full run (loop thread, between runs); null when it doesn't. */
        volatile String localUpgrade;
        /** Why this shard needs a full run though its source is unchanged; null when it doesn't. */
        volatile String upgrade;
        /**
         * Why a reshard step needs a full run of this project first (a takeover restored an index built
         * under another checkout path, or a writer state older than its generation); kept until a full run
         * commits, so every poll keeps the project pending ({@link #upgradeReason}) and the run happens.
         */
        volatile String fullRunFirst;
        /** {@code file-state.json} size and mtime when {@link #embeddingsOutdated} was computed. */
        long stateStamp = Long.MIN_VALUE;
        boolean stateEmbed;
        boolean embeddingsOutdated;
    }

    /** The run in progress. */
    private static final class Run {
        final ShardId shard;
        /** The shard leases of the unit being run: the ones its publish may commit. */
        final List<ShardId> leaseShards;
        final Thread thread = Thread.currentThread();
        final IndexAbort abort = new IndexAbort();
        final long startedAt;
        volatile String phase = WriterHeartbeat.PARSE;
        volatile double progress;
        /** Free bytes of the index store when the run was admitted; -1 without a guard. */
        volatile long freeAtStart = -1;
        /** What the run had consumed when the guard aborted it for low disk; -1 when it didn't. */
        volatile long consumedAtDiskAbort = -1;

        Run(ShardId shard, List<ShardId> leaseShards, long startedAt) {
            this.shard = shard;
            this.leaseShards = List.copyOf(leaseShards);
            this.startedAt = startedAt;
        }

        ProgressListener listener() {
            return (stage, current, total) -> {
                String p = phaseOf(stage);
                if (!p.equals(phase)) progress = 0;
                phase = p;
                if (total > 0) progress = Math.max(0, Math.min(1, (double) current / total));
            };
        }
    }

    /** Whether runs of {@code shard} embed: the writer's setting, unless the shard's source says no. */
    private boolean embeds(ShardId shard) {
        ShardState st = shards.get(shard);
        return settings.embed() && (st == null || st.source == null || st.source.embed());
    }

    /** Maps a {@link ProjectIndexManager} progress stage to a heartbeat phase. */
    static String phaseOf(String stage) {
        if (stage == null) return WriterHeartbeat.PARSE;
        if (stage.startsWith("Embedding")) return WriterHeartbeat.EMBED;
        if (stage.equals("Indexing") || stage.equals("Patching call graph") || stage.equals("Mining synonyms")) {
            return WriterHeartbeat.COMMIT;
        }
        if (stage.equals("Done")) return WriterHeartbeat.PUBLISH; // the run's publish follows its last stage
        return WriterHeartbeat.PARSE;
    }

    private final Settings settings;
    private final ObjectStoreCatalog catalog;
    private final ReplicaSource replicas;
    private final WriterStateStore writerState;
    private final ProjectIndexManager indexer;
    private final LuceneIndexer lucene;
    private final ProjectRegistry projects;
    private final SourceCheckout checkout;
    private final Clock clock;
    private final Balancer balancer;
    private final WriterRegistry members;
    private final LeaseManager leases;
    private final DiskGuard disk;            // null: no disk guard
    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    /** Runs the projects' reshards; null until {@link #enableResharding}. */
    private volatile com.pharos.cluster.sharding.ReshardJob reshard;
    /** The catalog mapping of the last tick. */
    private volatile ProjectShards lastMap = ProjectShards.identity(List.of());
    private final long startedAt;

    /** The held work units (see the class javadoc), keyed by {@code ShardId.ofProject(project)}. */
    private final Map<ShardId, ShardState> shards = new ConcurrentHashMap<>();
    /** The catalog's units as last read: each unit's plan. */
    private volatile Map<ShardId, ShardPlan> plans = Map.of();
    private final Map<ShardId, Integer> publishes = new ConcurrentHashMap<>();
    private volatile Set<ShardId> pinnedMine = Set.of();
    private volatile Run current;
    /** The shard runShard is working on (restore included); the disk GC leaves its dir alone. */
    private volatile ShardId busy;
    /** Shards this writer released because it could not index them: not re-acquired before the instant. */
    private final Map<ShardId, Instant> avoidUntil = new ConcurrentHashMap<>();
    /**
     * Per shard: the space a run consumed before the guard aborted it for low disk. The next
     * estimate is never below it (in memory, this writer only; cleared by a publish).
     */
    private final Map<ShardId, Long> estimateFloor = new ConcurrentHashMap<>();
    /** The index store's free bytes when {@link #admitted} last admitted a run (loop thread). */
    private long admittedFree = -1;
    private final Object diskMu = new Object();
    /** The low-disk WARN of this episode was logged (loop thread). */
    private boolean announcedLow;
    private ScheduledExecutorService diskWatch;
    /** Change detection at a fixed rate, on its own thread: it runs while the loop thread indexes. */
    private ScheduledExecutorService detector;
    /** Renews the reshard leases while their units are held (see {@link #renewReshardLeases}). */
    private ScheduledExecutorService reshardRenewer;
    private volatile boolean stopping;
    private final ReentrantLock tickLock = new ReentrantLock();
    private final Object stopMu = new Object();
    private boolean stopped;                 // under stopMu
    private ScheduledExecutorService scheduler;

    /**
     * @param clock this writer's clock: {@code minHold}, backlog ages and run start times. Lease and
     *              liveness expiry use the bucket's clock only.
     */
    public WorkLoop(Settings settings, ObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                    ReplicaSource replicas, WriterStateStore writerState, ProjectIndexManager indexer,
                    LuceneIndexer lucene, ProjectRegistry projects, SourceCheckout checkout, Clock clock) {
        this(settings, store, layout, catalog, replicas, writerState, indexer, lucene, projects, checkout, clock, null);
    }

    /** @param disk the writer disk guard; null runs without one (no {@code disk} in the heartbeat) */
    public WorkLoop(Settings settings, ObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                    ReplicaSource replicas, WriterStateStore writerState, ProjectIndexManager indexer,
                    LuceneIndexer lucene, ProjectRegistry projects, SourceCheckout checkout, Clock clock,
                    DiskGuard disk) {
        this.disk = disk;
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.replicas = Objects.requireNonNull(replicas, "replicas");
        this.writerState = Objects.requireNonNull(writerState, "writerState");
        this.indexer = Objects.requireNonNull(indexer, "indexer");
        this.lucene = Objects.requireNonNull(lucene, "lucene");
        this.projects = Objects.requireNonNull(projects, "projects");
        this.checkout = Objects.requireNonNull(checkout, "checkout");
        checkout.useProjects(s -> lastMap.projectOf(s)); // credentials by project, also for keyed shards (I3a)
        this.clock = Objects.requireNonNull(clock, "clock");
        this.balancer = new Balancer(settings.minHold());
        this.startedAt = clock.millis();
        this.members = new WriterRegistry(store, layout, settings.node(), settings.leaseTtl(), settings.heartbeat(),
                this::heartbeat);
        // The heartbeat is the bucket's "now" for lease expiry: without it no expired lease is taken over.
        this.leases = new LeaseManager(store, layout, settings.node(), settings.leaseTtl(), members::bucketNow, clock);
    }

    /** Starts the heartbeat, the lease renewer and the polling loop (daemon threads). */
    public void start() {
        synchronized (stopMu) {
            if (stopped || scheduler != null) return;
            members.start();
            leases.start();
            // the reshard lease is renewed on its own thread too: a reshard build runs inside a tick, which
            // would otherwise not renew it for the whole build (any build longer than the TTL lost it)
            if (reshard != null) startReshardRenewer();
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-writer-work-loop");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(this::tickQuietly, 0, Math.max(1, settings.poll().toMillis()),
                    TimeUnit.MILLISECONDS);
            if (disk != null) {
                // its own thread: the loop thread is busy for the whole run the guard may have to abort
                diskWatch = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "pharos-writer-disk-guard");
                    t.setDaemon(true);
                    return t;
                });
                long every = Math.max(1, disk.settings().check().toMillis());
                diskWatch.scheduleWithFixedDelay(this::checkDiskQuietly, every, every, TimeUnit.MILLISECONDS);
            }
            // fixed rate, not delay: a change is seen within one pollSeconds of its push, whatever a run takes
            detector = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-writer-change-detector");
                t.setDaemon(true);
                return t;
            });
            long poll = Math.max(1, settings.poll().toMillis());
            detector.scheduleAtFixedRate(this::detectChangesQuietly, poll, poll, TimeUnit.MILLISECONDS);
        }
        log.info("Writer {} runs the work queue (lease TTL {} s, poll every {} s, min hold {} s)", settings.node(),
                settings.leaseTtl().toSeconds(), settings.poll().toSeconds(), settings.minHold().toSeconds());
    }

    private void tickQuietly() {
        try {
            tick();
        } catch (RuntimeException | Error e) {
            log.warn("Writer work loop tick failed (retrying next poll): {}", e.toString());
        }
    }

    // ------------------------------------------------------------------------------- the loop

    /** One round (see the class javadoc). Runs on one thread at a time; returns at once while stopping. */
    public void tick() {
        tickLock.lock();
        try {
            if (stopping) return;
            checkDisk(); // before the beat: it announces the disk state peers balance by
            members.beat();
            if (!members.lastWriteOk()) return; // can't tell "peers are gone" from "the bucket is unreachable"
            leases.renewAll();
            com.pharos.cluster.sharding.ReshardJob job = reshard;
            forgetLost();
            if (job != null) {
                // a reshard lease lives only as long as its project's work unit here
                Set<ShardId> held = heldSet();
                for (Lease l : job.leases().held()) if (!held.contains(l.shard())) job.releaseLease(l.shard());
                job.leases().renewAll();
            }
            ObjectStoreCatalog.View view;
            try {
                view = catalog.view();
            } catch (IOException | RuntimeException e) {
                log.warn("Reading the shard catalog failed (retrying next poll): {}", e.toString());
                return;
            }
            lastMap = view.projects();
            Map<ShardId, ShardSource> sources = resolveSources(view.entries(), view.projects());
            if (diskLow()) {
                if (!announcedLow) {
                    announcedLow = true;
                    DiskGuard.Status d = disk.status();
                    log.warn("Writer {} is low on disk ({} free, threshold {}): acquiring no lease and starting no"
                                    + " run until it recovers", settings.node(),
                            d == null ? "?" : DiskGuard.bytes(d.freeBytes()), DiskGuard.bytes(disk.indexStoreThreshold()));
                } else {
                    log.debug("Writer {} is low on disk: acquiring no lease", settings.node());
                }
            } else {
                if (announcedLow) {
                    announcedLow = false;
                    log.info("Writer {} is no longer low on disk: acquiring leases again", settings.node());
                }
                acquire(sources);
            }
            members.beat(); // announce the leases now, before a long run
            Set<ShardId> noRun = finishFlips(view.projects());
            List<ShardId> pending = poll();
            for (ShardId shard : pending) {
                if (stopping) return;
                if (!unitHeld(shard)) continue;
                if (noRun.contains(shard)) continue;
                if (job != null && (job.builtLocally(shard.value(), view.projects()) || reshardPinned(shard))) {
                    // an index run now would publish the active version under the staged one's feet: the flip
                    // (or a cancel) comes first
                    log.debug("Project {}: its reshard is built and waits for the flip; no index run now", shard);
                    continue;
                }
                if (diskLow()) {
                    log.info("Shard {}: not indexing, writer {} is low on disk", shard, settings.node());
                    continue;
                }
                Outcome o = runShard(shard);
                if (o == Outcome.PUBLISHED) {
                    maybeRelease(shard, currentShare(sources));
                } else if (o == Outcome.FAILED) {
                    maybeReleaseFailing(shard, currentShare(sources));
                } else if (o == Outcome.REFUSED) {
                    releaseUnindexable(shard, "its run would not fit on this writer's disk");
                }
                members.beat();
            }
            reshardSteps(view.projects());
            // a shard whose source checks keep failing (an unreachable remote) is released like a failing run's
            for (ShardId shard : shards.keySet().stream().sorted(Comparator.comparing(ShardId::value)).toList()) {
                ShardState st = shards.get(shard);
                if (st != null && !st.lastPollOk) maybeReleaseFailing(shard, currentShare(sources));
            }
            releaseIdleSurplus(currentShare(sources));
            if (diskLow()) {
                for (ShardId shard : shards.keySet().stream().sorted(Comparator.comparing(ShardId::value)).toList()) {
                    ShardState st = shards.get(shard);
                    // an idle shard whose last publish is still served stays (its lease keeps being renewed)
                    if (st == null || (st.lastPollOk && st.pendingSince == null)) continue;
                    releaseUnindexable(shard, "writer " + settings.node() + " is low on disk");
                }
            }
            members.beat();
        } finally {
            tickLock.unlock();
        }
    }

    /**
     * The units (projects) of the catalog this writer could index, with their source; pins source-less
     * local ones. Also records each unit's plan, and releases held units the catalog no longer lists, or
     * maps to other shards.
     */
    private Map<ShardId, ShardSource> resolveSources(List<ShardEntry> entries, ProjectShards mapping) {
        Map<ShardId, Optional<ShardSource>> sourceOf = new LinkedHashMap<>();
        for (ShardEntry e : entries) sourceOf.put(e.id(), e.source());
        Map<ShardId, ShardSource> out = new LinkedHashMap<>();
        Map<ShardId, ShardPlan> unitPlans = new LinkedHashMap<>();
        Set<ShardId> pins = new HashSet<>();
        for (String project : mapping.projects()) {
            ShardId unit = ShardId.ofProject(project);
            ShardPlan plan = ShardPlan.of(mapping, project);
            // the entry named after the project first: registration (pharos index/publish with a source, the
            // operator's Job) always writes it, so a new url or ref wins over a split project's shards, whose
            // copies a reshard wrote (integration carry I5); a split project's shards may also carry none yet
            Optional<ShardSource> source = sourceOf.getOrDefault(unit, Optional.empty());
            for (ShardId s : plan.clusterShards()) {
                if (source.isPresent()) break;
                source = sourceOf.getOrDefault(s, Optional.empty());
            }
            if (source.isPresent()) {
                out.put(unit, source.get());
                unitPlans.put(unit, plan);
                continue;
            }
            Optional<ProjectMeta> local = projects.find(project);
            if (local.isPresent() && local.get().getRootPath() != null
                    && Files.isDirectory(Path.of(local.get().getRootPath()))) {
                out.put(unit, ShardSource.path(local.get().getRootPath()));
                unitPlans.put(unit, plan);
                pins.add(unit);
            }
        }
        plans = Map.copyOf(unitPlans);
        pinnedMine = Set.copyOf(pins);
        // a held unit that left the catalog (or lost its source here), or whose shards changed, is released
        for (ShardId held : heldSet()) {
            ShardState st = shards.get(held);
            ShardPlan plan = unitPlans.get(held);
            if (plan == null) {
                log.info("Shard {} is no longer workable here (not in the catalog, or no source); releasing it", held);
                release(held);
            } else if (st != null && !plan.clusterShards().equals(st.leaseShards)) {
                log.info("Project {} is now mapped to shards {} (was {}); releasing its leases", held,
                        plan.clusterShards(), st.leaseShards);
                release(held);
            }
        }
        for (Map.Entry<ShardId, ShardSource> e : out.entrySet()) {
            ShardState st = shards.get(e.getKey());
            if (st != null) {
                if (!e.getValue().equals(st.source)) st.prepared = false;
                st.source = e.getValue();
                st.pinned = pins.contains(e.getKey());
            }
        }
        return out;
    }

    /** {@code ceil(workable / live writers)} from the latest heartbeat listing. */
    private int currentShare(Map<ShardId, ShardSource> sources) {
        Set<String> workable = new HashSet<>();
        for (Map.Entry<ShardId, ShardSource> e : sources.entrySet()) {
            if (!pinnedMine.contains(e.getKey())) workable.add(e.getKey().value());
        }
        pinnedMine.forEach(s -> workable.add(s.value()));
        int live = 0;
        boolean self = false;
        for (WriterRegistry.LiveWriter w : members.live()) {
            if (w.heartbeat().node().equals(settings.node())) self = true;
            if (!w.heartbeat().takesShare()) continue;
            live++;
            workable.addAll(w.heartbeat().pinned());
        }
        if (!self) live++; // our own heartbeat always counts while running
        return Balancer.share(workable.size(), live);
    }

    /** Takes the pinned shards, then free or expired leases up to the share. @return the share */
    private int acquire(Map<ShardId, ShardSource> sources) {
        for (ShardId pin : pinnedMine) {
            if (!unitHeld(pin)) tryAcquire(pin, sources.get(pin));
        }
        int share = currentShare(sources);
        Set<ShardId> held = heldSet();
        int want = Balancer.wanted(share, held.size());
        if (want == 0) return share;
        Set<ShardId> pinnedByOthers = new HashSet<>();
        for (WriterRegistry.LiveWriter w : members.live()) {
            if (!w.heartbeat().node().equals(settings.node())) {
                w.heartbeat().pinned().forEach(s -> pinnedByOthers.add(new ShardId(s)));
            }
        }
        Instant now = clock.instant();
        avoidUntil.values().removeIf(until -> !now.isBefore(until));
        List<ShardId> candidates = Balancer.acquireOrder(settings.node(),
                sources.keySet().stream().filter(s -> !pinnedMine.contains(s) && !pinnedByOthers.contains(s)
                        && !avoidUntil.containsKey(s)).toList(),
                held);
        for (ShardId s : candidates) {
            if (want == 0 || stopping) break;
            if (tryAcquire(s, sources.get(s))) want--;
        }
        return share;
    }

    /**
     * Acquires every shard lease of {@code unit}, in id order, or none: a lease that cannot be taken
     * releases the ones this attempt took.
     */
    private boolean tryAcquire(ShardId shard, ShardSource source) {
        ShardPlan plan = plans.get(shard);
        if (plan == null) plan = ShardPlan.single(shard.value());
        List<ShardId> wanted = plan.clusterShards().stream().sorted(Comparator.comparing(ShardId::value)).toList();
        List<Lease> taken = new ArrayList<>();
        for (ShardId s : wanted) {
            Optional<Lease> lease;
            try {
                lease = leases.tryAcquire(s);
            } catch (IOException e) {
                log.warn("Acquiring the lease of shard {} failed: {}", s, e.toString());
                lease = Optional.empty();
            }
            if (lease.isEmpty()) {
                if (!taken.isEmpty()) {
                    log.info("Project {}: lease of shard {} not available; releasing the {} lease(s) taken (all or"
                            + " none)", shard, s, taken.size());
                }
                taken.forEach(leases::release);
                return false;
            }
            taken.add(lease.get());
        }
        ShardState st = new ShardState();
        st.plan = plan;
        st.leaseShards = plan.clusterShards();
        st.source = source;
        st.pinned = pinnedMine.contains(shard);
        st.acquiredAt = clock.instant();
        shards.put(shard, st);
        return true;
    }

    /**
     * Drops the state of units a lease of which was lost (taken over, a failed renewal), releasing the
     * unit's other leases, and releases leases that belong to no held unit.
     */
    private void forgetLost() {
        Set<ShardId> leased = leasedShards();
        for (ShardId s : new ArrayList<>(shards.keySet())) {
            ShardState st = shards.get(s);
            if (st != null && !leased.containsAll(st.leaseShards)) {
                shards.remove(s);
                for (Lease l : leases.held()) if (st.leaseShards.contains(l.shard())) leases.release(l);
                com.pharos.cluster.sharding.ReshardJob job = reshard;
                if (job != null) job.releaseLease(s);
                log.warn("Writer {} no longer holds the lease of shard {}; stopped working on it", settings.node(), s);
            }
        }
        Set<ShardId> owned = new HashSet<>();
        for (ShardState st : shards.values()) owned.addAll(st.leaseShards);
        for (Lease l : leases.held()) {
            if (!owned.contains(l.shard())) leases.release(l);
        }
    }

    /** Polls every held shard's source; returns the pending ones, oldest unpublished change first. */
    private List<ShardId> poll() {
        java.util.concurrent.atomic.AtomicBoolean announce = new java.util.concurrent.atomic.AtomicBoolean();
        List<Thread> checks = new ArrayList<>();
        for (Map.Entry<ShardId, ShardState> e : shards.entrySet()) {
            if (stopping) break;
            ShardId shard = e.getKey();
            ShardState st = e.getValue();
            try {
                if (!st.prepared) {
                    if (diskLow()) { // a first checkout (a clone) needs space this writer doesn't have
                        st.lastPollOk = false;
                        continue;
                    }
                    checkout.prepare(shard, st.source);
                    st.prepared = true;
                }
            } catch (IOException | RuntimeException ex) {
                st.lastPollOk = false;
                st.consecutiveFailures.incrementAndGet();
                log.warn("Polling the source of shard {} failed (retrying next poll): {}", shard, ex.toString());
                continue;
            }
            st.localUpgrade = localUpgradeReason(shard, st);
            checks.add(Thread.ofVirtual().name("pharos-writer-poll-" + shard.value()).start(() -> {
                if (pollShard(shard, st)) announce.set(true);
            }));
        }
        // in parallel: an unreachable remote delays only its own shard (bounded by lsRemoteTimeoutSeconds)
        for (Thread t : checks) {
            try {
                t.join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // a change this poll found first (before the detector) is announced now, not after the runs
        if (announce.get() && !stopping) members.beat();
        return shards.entrySet().stream().filter(e -> e.getValue().pendingSince != null)
                .sorted(Comparator.comparing((Map.Entry<ShardId, ShardState> e) -> e.getValue().pendingSince)
                        .thenComparing(e -> e.getKey().value()))
                .map(Map.Entry::getKey).toList();
    }

    /**
     * One read-only source check of a prepared shard ({@code git ls-remote} of its url, or the tree
     * digest of a path source; never a fetch into the checkout a run may be indexing) against the
     * revision {@code LATEST} holds. Safe on the loop thread and on the change detector. When another
     * check of the same shard is still in progress (a slow remote) this one is skipped: that check's
     * result stands. A failed check counts toward {@code maxConsecutiveFailures} (M4); a check that
     * finds the shard idle resets the count.
     *
     * @return true when this check found a new unpublished source revision: the shard turned pending,
     *     or a pending shard's source moved on again
     */
    private boolean pollShard(ShardId shard, ShardState st) {
        if (!st.pollLock.tryLock()) return false;
        try {
            boolean wasPending = st.pendingSince != null;
            String before = st.sourceRevision;
            try {
                refreshPublished(shard, st);
                Optional<String> changed = checkout.changedSince(shard, st.publishedRevision);
                st.upgrade = upgradeReason(st);
                if (changed.isPresent()) {
                    st.sourceRevision = changed.get();
                    if (st.pendingSince == null) st.pendingSince = clock.instant();
                } else if (st.upgrade != null) {
                    // the source is published, but the index must be rebuilt by this release
                    st.sourceRevision = st.publishedRevision;
                    if (st.pendingSince == null) st.pendingSince = clock.instant();
                } else {
                    st.sourceRevision = st.publishedRevision;
                    st.pendingSince = null;
                    st.consecutiveFailures.set(0); // healthy and published
                }
                st.lastPollOk = true;
            } catch (IOException | RuntimeException ex) {
                // pendingSince is now unknown: no idle release until a poll succeeds again (M5)
                st.lastPollOk = false;
                int n = st.consecutiveFailures.incrementAndGet();
                log.warn("Polling the source of shard {} failed ({} failure(s) in a row; retrying next poll): {}", shard,
                        n, ex.toString());
            }
            return st.pendingSince != null && (!wasPending || !java.util.Objects.equals(before, st.sourceRevision));
        } finally {
            st.pollLock.unlock();
        }
    }

    /**
     * The change detector: every {@code pollSeconds} at a fixed rate, on its own thread, a read-only
     * check of every prepared held shard, also while the loop thread runs another shard (or this
     * one). A shard that turns pending, or whose pending source moves on, is announced at once by a
     * heartbeat, so the backlog shows the change
     * within one poll. Each shard is checked on its own virtual thread, so one unreachable remote
     * delays only its own shard. Run order, leases and releases stay with the loop thread.
     */
    void detectChanges() {
        List<Thread> checks = new ArrayList<>();
        for (Map.Entry<ShardId, ShardState> e : shards.entrySet()) {
            if (stopping) break;
            ShardId shard = e.getKey();
            ShardState st = e.getValue();
            if (!st.prepared || !checkout.isPrepared(shard)) continue; // the loop thread checks it out first
            if (st.pollLock.isLocked()) continue; // its previous check is still running
            checks.add(Thread.ofVirtual().name("pharos-writer-detect-" + shard.value()).start(() -> {
                if (pollShard(shard, st) && !stopping) members.beat(); // at once, not after the slowest check
            }));
        }
        for (Thread t : checks) {
            try {
                t.join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); // stopping: the checks end within their git timeout
                return;
            }
        }
    }

    private void detectChangesQuietly() {
        try {
            detectChanges();
        } catch (RuntimeException | Error e) {
            log.warn("Change detection failed (retrying next poll): {}", e.toString());
        }
    }

    /** The source revision {@code LATEST} holds (from its manifest's project meta); null when unknown. */
    private void refreshPublished(ShardId shard, ShardState st) throws IOException {
        List<Generation> gens = new ArrayList<>();
        for (ShardId s : st.leaseShards) {
            Optional<Generation> latest = replicas.latest(s);
            if (latest.isEmpty()) {
                // a shard of the unit was never published: the unit is unpublished
                st.publishedGeneration = null;
                st.publishedGens = null;
                st.publishedRevision = null;
                return;
            }
            gens.add(latest.get());
        }
        if (gens.equals(st.publishedGens)) return;
        String revision = null;
        boolean oneRun = true;
        int format = Integer.MAX_VALUE;
        List<String> projectFiles = null;
        for (int i = 0; i < gens.size(); i++) {
            ShardManifest manifest = replicas.manifest(st.leaseShards.get(i), gens.get(i));
            ProjectMeta meta = manifest.projectMeta();
            String r = meta == null ? null : meta.getSourceRevision();
            // one run's content: the same project files (a reshard's rewritten shards carry a new run number,
            // P6-R21, but the content, and so the project files, of the run they split)
            List<String> files = manifest.projectFileList().stream().map(f -> f.key()).sorted().toList();
            if (i == 0) {
                revision = r;
                projectFiles = files;
            } else if (!Objects.equals(r, revision) || !Objects.equals(projectFiles, files)) {
                oneRun = false;
            }
            format = Math.min(format, manifest.indexFormatVersion());
        }
        // the shards of a split project published by different runs (a publish that stopped between two
        // shards): no revision covers the unit, so it is pending, and its next run publishes every shard
        st.publishedRevision = oneRun ? revision : null;
        st.publishedFormat = format;
        st.publishedGeneration = gens.getFirst();
        st.publishedGens = List.copyOf(gens);
    }

    /**
     * Why a shard whose source is published still needs a FULL run with this release: its
     * {@code LATEST} generation has an older index format, or its local index needs a full run
     * ({@link #localUpgradeReason}). Null when it doesn't. This is how a cluster upgrades: restart
     * the writers on the new release and every shard they hold is re-indexed and re-published.
     */
    private static String upgradeReason(ShardState st) {
        if (st.publishedGeneration != null && st.publishedFormat >= 0
                && st.publishedFormat < com.pharos.indexer.IndexVersions.INDEX_FORMAT) {
            return "the published generation " + st.publishedGeneration.value() + " has index format "
                    + st.publishedFormat + ", this writer indexes format " + com.pharos.indexer.IndexVersions.INDEX_FORMAT;
        }
        return st.localUpgrade != null ? st.localUpgrade : st.fullRunFirst;
    }

    /**
     * Why this writer's local index of {@code shard} needs a full run: the indexer's own reasons
     * ({@link ProjectIndexManager#fullRunReason}: another index format, a broken graph pointer), or
     * files embedded under another chunking version or model. Null when it doesn't, or when there is
     * no local index (the restore decides then). Loop thread only, between runs; the file state is
     * re-read only when it changed.
     */
    String localUpgradeReason(ShardId shard, ShardState st) {
        if (!lucene.indexExists(shard.value())) return null;
        String reason = indexer.fullRunReason(shard.value(), planOf(st, shard));
        if (reason != null) return reason;
        Path dir = lucene.getProjectIndexDir(shard.value());
        Path state = dir.resolve("file-state.json");
        boolean embed = embeds(shard);
        long stamp;
        try {
            stamp = Files.getLastModifiedTime(state).toMillis() * 31 + Files.size(state);
        } catch (IOException e) {
            return null; // no file state: nothing recorded to be outdated
        }
        if (stamp != st.stateStamp || embed != st.stateEmbed) {
            st.embeddingsOutdated = indexer.stateTracker(dir, embed).hasOutdatedEmbeddings();
            st.stateStamp = stamp;
            st.stateEmbed = embed;
        }
        return st.embeddingsOutdated
                ? "files were embedded under another chunking version or model than this release's" : null;
    }

    /**
     * Restore precondition, enforced: no run of any shard is in progress (the loop runs one shard at a
     * time) and this writer has no local index of {@code shard}, so nothing can have it open.
     *
     * @throws IllegalStateException otherwise
     */
    void requireRestorable(ShardId shard) {
        Run r = current;
        if (r != null) {
            throw new IllegalStateException("cannot restore shard " + shard + " while a run of shard " + r.shard
                    + " is in progress");
        }
        if (lucene.indexExists(shard.value())) {
            throw new IllegalStateException("cannot restore shard " + shard + ": this writer has a local index of it");
        }
    }

    /**
     * One run of {@code shard}: check out, ask the disk guard, restore if needed, index, publish (in
     * the index run), renew.
     */
    private Outcome runShard(ShardId shard) {
        ShardState st = shards.get(shard);
        if (st == null) return Outcome.FAILED;
        busy = shard;
        try {
            Outcome o = attempt(shard, st);
            if (o == Outcome.PUBLISHED) st.consecutiveFailures.set(0);
            else if (o == Outcome.FAILED) st.consecutiveFailures.incrementAndGet();
            return o;
        } finally {
            busy = null;
        }
    }

    private Outcome attempt(ShardId shard, ShardState st) {
        Path indexDir = lucene.getProjectIndexDir(shard.value());
        Path root;
        String revision;
        try {
            root = checkout.prepare(shard, st.source).toAbsolutePath().normalize();
            st.prepared = true;
            revision = st.source.kind() == ShardSource.Kind.GIT
                    ? checkout.checkedOutRevision(shard).orElse(null)
                    : checkout.changedSince(shard, null).orElse(null);
        } catch (IOException | RuntimeException e) {
            log.warn("Checking out shard {} failed (retrying next poll): {}", shard, e.toString());
            return Outcome.FAILED;
        }
        boolean full;
        String why;
        if (!lucene.indexExists(shard.value())) {
            // the restore downloads the published generation, then (at worst) a full run follows
            if (!admitted(shard, estimateWithoutLocalIndex(shard, root))) return Outcome.REFUSED;
            Restore r = restoreLocal(shard, st, root, indexDir);
            if (r == null) return Outcome.FAILED;
            full = r.full();
            why = r.why();
        } else {
            Optional<ProjectMeta> local = projects.find(shard.value());
            if (local.isEmpty()) {
                full = true;
                why = "the local index is not registered";
            } else if (st.upgrade != null) {
                full = true;
                why = st.upgrade;
            } else if (Files.exists(indexDir.resolve(FULL_RUN_REQUIRED))) {
                full = true;
                why = "the restored writer state was older than the restored generation, and no full run"
                        + " has published since";
            } else if (!sameRoot(local.get().getRootPath(), root)) {
                full = true;
                why = "the local index was built under " + local.get().getRootPath() + ", the checkout is at " + root;
            } else {
                full = false;
                why = "local index";
            }
            if (!admitted(shard, estimate(shard, indexDir, root, full))) return Outcome.REFUSED;
        }
        if (full) log.warn("Shard {}: running a FULL index ({})", shard, why);
        else log.info("Shard {}: incremental index from {}", shard, why);

        Run run = new Run(shard, st.leaseShards, clock.millis());
        run.freeAtStart = disk == null ? -1 : admittedFree;
        current = run;
        try {
            if (stopping) run.abort.request("stopping");
            if (diskLow()) run.abort.request("writer low on disk");
            String headAtStart = st.sourceRevision;
            indexer.index(root, shard.value(), !full, false, embeds(shard), settings.buildSynonyms(),
                    run.listener(), run.abort, revision, planOf(st, shard));
            st.pollLock.lock();
            try {
                if (full) { // rebuilt by this release: nothing left to upgrade
                    st.localUpgrade = null;
                    st.upgrade = null;
                    st.fullRunFirst = null;
                }
                st.publishedRevision = revision;
                st.publishedGeneration = null; // re-read on the next poll
                st.publishedGens = null;
                String head = st.sourceRevision;
                if (head != null && !head.equals(headAtStart) && !head.equals(revision)) {
                    // the detector saw a newer source during the run: still pending after this publish
                    if (st.pendingSince == null) st.pendingSince = clock.instant();
                } else {
                    st.sourceRevision = revision;
                    st.pendingSince = null;
                }
            } finally {
                st.pollLock.unlock();
            }
            if (full) Files.deleteIfExists(indexDir.resolve(FULL_RUN_REQUIRED));
            publishes.merge(shard, 1, Integer::sum);
            renewUnit(st);
            estimateFloor.remove(shard);
            return Outcome.PUBLISHED;
        } catch (IndexAbortedException e) {
            log.info("Run of shard {} aborted at a safe point; nothing was committed: {}", shard, e.getMessage());
            long consumed = run.consumedAtDiskAbort;
            if (consumed >= 0) {
                long floor = estimateFloor.merge(shard, consumed, Math::max);
                log.warn("Shard {}: its run had consumed {} when the disk ran low; its next run is estimated at"
                        + " no less", shard, DiskGuard.bytes(floor));
            }
            return Outcome.ABORTED;
        } catch (PublishConflictException e) {
            if (!unitHeld(shard)) {
                release(shard);
                log.warn("Shard {} not published: writer {} lost its lease and stops working on it ({})", shard,
                        settings.node(), e.getMessage());
            } else {
                st.publishedGeneration = null;
                st.publishedGens = null;
                log.warn("Shard {} not published ({}); retrying next poll from the new LATEST", shard, e.getMessage());
            }
            return Outcome.CONFLICT;
        } catch (IOException | RuntimeException e) {
            log.warn("Run of shard {} failed (retrying next poll): {}", shard, e.toString());
            return Outcome.FAILED;
        } finally {
            current = null;
        }
    }

    /** What a restore came to: whether the next run must be full, and why (or from what it resumes). */
    private record Restore(boolean full, String why) {}

    /**
     * Restores the writer state of {@code shard} (this writer has no local index of it) and registers
     * it; null when the store failed (logged; retried next poll).
     */
    private Restore restoreLocal(ShardId shard, ShardState st, Path root, Path indexDir) {
        requireRestorable(shard);
        Optional<WriterStateStore.Restored> restored;
        try {
            ShardPlan plan = planOf(st, shard);
            restored = plan.isSplit() ? writerState.restoreProject(plan, indexDir)
                    : writerState.restore(shard, indexDir);
        } catch (IOException e) {
            // a store outage: retry later rather than paying for a full re-embed now
            log.warn("Restoring the writer state of shard {} failed (retrying next poll): {}", shard, e.toString());
            return null;
        }
        if (restored.isEmpty()) return new Restore(true, "no usable writer state to resume from");
        Generation gen = restored.get().generation();
        String originalRoot;
        try {
            // before registering: a run that dies before its commit must not be followed by an
            // incremental one on the mismatched file state (the marker goes after a publish)
            if (!restored.get().stateMatches()) Files.writeString(indexDir.resolve(FULL_RUN_REQUIRED), "");
            originalRoot = registerRestored(shard, st, gen, root, indexDir);
        } catch (IOException | RuntimeException e) {
            log.warn("Registering the restored shard {} failed (retrying next poll): {}", shard, e.toString());
            return null;
        }
        if (!restored.get().stateMatches()) {
            // the file state is an older generation's: it doesn't describe the restored documents
            // (a file added since and deleted again would keep its documents in an incremental run)
            return new Restore(true, "restored generation " + gen.value() + " with the writer state of generation "
                    + restored.get().stateGeneration().value());
        }
        if (!sameRoot(originalRoot, root)) {
            return new Restore(true, "restored generation " + gen.value() + " was indexed under " + originalRoot
                    + ", this writer checks out at " + root);
        }
        return new Restore(false, "restored generation " + gen.value());
    }

    // ------------------------------------------------------------------------------ resharding

    /**
     * Turns on resharding (Phase 6 Task 4): this loop runs the {@link ReshardJob} of the projects it holds
     * (one step per tick), finishes interrupted flips before any run, and GCs retired versions.
     */
    public void enableResharding(com.pharos.cluster.objectstore.ObjectStorePublisher publisher,
                                 com.pharos.cluster.objectstore.GenerationGc gc,
                                 java.util.function.Supplier<List<com.pharos.cluster.membership.ReaderHeartbeat>> readers,
                                 com.pharos.cluster.sharding.ReshardJob.Settings reshardSettings) {
        LeaseManager reshardLeases = new LeaseManager(store, layout, settings.node(), settings.leaseTtl(),
                members::bucketNow, clock, id -> layout.reshardLeaseKey(id.value()));
        this.reshard = new com.pharos.cluster.sharding.ReshardJob(settings.node(), store, layout, catalog, publisher,
                replicas, writerState, indexer, lucene, reshardLeases, readers, gc, clock, reshardSettings);
        synchronized (stopMu) {
            if (scheduler != null) startReshardRenewer(); // enabled after start(): renew from now on
        }
    }

    /** Holding stopMu: renews the reshard leases every TTL/3 on their own thread. */
    private void startReshardRenewer() {
        if (reshardRenewer != null || stopped) return;
        reshardRenewer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-writer-reshard-lease");
            t.setDaemon(true);
            return t;
        });
        long every = Math.max(1, settings.leaseTtl().dividedBy(3).toMillis());
        reshardRenewer.scheduleWithFixedDelay(this::renewReshardLeasesQuietly, every, every, TimeUnit.MILLISECONDS);
    }

    private void renewReshardLeasesQuietly() {
        try {
            renewReshardLeases();
        } catch (RuntimeException e) {
            log.warn("Renewing the reshard leases failed (retried next interval): {}", e.toString());
        }
    }

    /**
     * Renews each reshard lease while this writer holds every shard lease of its project's work unit
     * (a reshard runs only on the unit's holder). Once the unit is lost (a lease taken over or a failed
     * renewal) the running build of that project is aborted and the reshard lease released at once, so
     * the writer that holds the project now takes the reshard over without waiting out the TTL.
     */
    void renewReshardLeases() {
        com.pharos.cluster.sharding.ReshardJob job = reshard;
        if (job == null) return;
        for (Lease l : job.leases().held()) {
            ShardId unit = l.shard();
            if (unitHeld(unit)) {
                job.leases().renew(l);
                continue;
            }
            Run r = current;
            if (r != null && r.shard.equals(unit)) {
                r.abort.request("this writer lost the shard leases of project " + unit);
            }
            log.warn("Reshard of project {}: this writer no longer holds the project's shard leases; releasing its"
                    + " reshard lease", unit);
            job.releaseLease(unit);
        }
    }

    /** Tests only: a run of {@code unit} is in progress; its abort. */
    IndexAbort runningForTest(ShardId unit) {
        tickLock.lock(); // after a tick in progress, which clears current when it ends
        try {
            Run r = new Run(unit, List.of(unit), clock.millis());
            current = r;
            return r.abort;
        } finally {
            tickLock.unlock();
        }
    }

    /** The reshard job, when {@link #enableResharding} turned it on. */
    public Optional<com.pharos.cluster.sharding.ReshardJob> reshardJob() {
        return Optional.ofNullable(reshard);
    }

    /**
     * Before any run: moves the {@code LATEST} of the held projects' pinned shards that lag their pins (a
     * flip whose finish was interrupted). Returns the units that could not be finished: no run of them
     * this tick (it would number its generations from the old {@code LATEST}).
     */
    private Set<ShardId> finishFlips(ProjectShards map) {
        Set<ShardId> unfinished = new HashSet<>();
        com.pharos.cluster.sharding.ReshardJob job = reshard;
        for (ShardId unit : heldSet()) {
            String project = unit.value();
            if (map.pinsOf(project).isEmpty()) continue;
            try {
                if (job != null) job.finish(project, map, leases);
                else unfinished.add(unit);
            } catch (IOException | RuntimeException e) {
                log.warn("Project {}: finishing its reshard flip failed (no run until it is): {}", project, e.toString());
                unfinished.add(unit);
            }
        }
        return unfinished;
    }

    /** Marks the unit pending with a full run, which stays pending until a full run commits. */
    private void needFullRunFirst(ShardState st, String why) {
        st.fullRunFirst = why;
        st.upgrade = why;
        if (st.pendingSince == null) st.pendingSince = clock.instant();
    }

    /**
     * One reshard step per held project with a reshard requested (and GC of retired versions for every
     * held project). The step needs this writer's local index of the active version's published run: a
     * pending project runs its index first; a project without a local index is restored first.
     */
    private void reshardSteps(ProjectShards map) {
        com.pharos.cluster.sharding.ReshardJob job = reshard;
        if (job == null) return;
        for (ShardId unit : heldSet().stream().sorted(Comparator.comparing(ShardId::value)).toList()) {
            if (stopping) return;
            String project = unit.value();
            job.collect(project, map);
            if (map.reshardRequestOf(project).isEmpty()) continue;
            ShardState st = shards.get(unit);
            if (st == null || diskLow()) continue;
            boolean built = job.builtLocally(project, map);
            // A pinned reshard is published: the step only waits for the readers and flips, which needs neither a
            // local build nor the source. It goes first whatever else the project waits for (a source change, a
            // restore or a full run): those are done after the flip, on the new version.
            boolean pinned = reshardPinned(unit);
            if (!pinned && st.pendingSince != null && !built) continue; // the source changed: its index run goes first
            busy = unit;
            try {
                Path indexDir = lucene.getProjectIndexDir(project);
                Path root = indexDir; // unused by a pinned step
                if (!pinned) {
                    try {
                        root = checkout.prepare(unit, st.source).toAbsolutePath().normalize();
                        st.prepared = true;
                    } catch (IOException | RuntimeException e) {
                        log.warn("Reshard of project {}: checking out the source failed (retrying next poll): {}",
                                project, e.toString());
                        continue;
                    }
                    if (!lucene.indexExists(project)) {
                        Restore r = restoreLocal(unit, st, root, indexDir);
                        if (r == null) continue;
                        if (r.full()) { // its full run goes first
                            needFullRunFirst(st, r.why());
                            continue;
                        }
                    }
                    Optional<ProjectMeta> local = projects.find(project);
                    if (Files.exists(indexDir.resolve(FULL_RUN_REQUIRED)) || local.isEmpty()
                            || !sameRoot(local.get().getRootPath(), root)) {
                        log.info("Reshard of project {}: its local index needs a full run first", project);
                        needFullRunFirst(st, local.isEmpty() ? "the local index is not registered"
                                : Files.exists(indexDir.resolve(FULL_RUN_REQUIRED))
                                ? "the restored writer state was older than the restored generation"
                                : "the local index was built under " + local.get().getRootPath() + ", the checkout is at "
                                        + root);
                        continue;
                    }
                    String localRevision = local.get().getSourceRevision();
                    if (!built && st.publishedRevision != null && !st.publishedRevision.equals(localRevision)) {
                        // the local index is not the published run: another writer published since this one last
                        // indexed the project (a lease that came back, e.g. after a writer scale-down and up, with
                        // the old index still on disk). Splitting it would publish the new version without what
                        // that writer added; the run that catches up goes first (integration e2e, run 2)
                        log.info("Reshard of project {}: the local index is at revision {}, the published run at {};"
                                + " indexing it first", project, localRevision, st.publishedRevision);
                        needFullRunFirst(st, "the local index (revision " + localRevision + ") is behind the published"
                                + " run (revision " + st.publishedRevision + ")");
                        continue;
                    }
                    if (!built && !admitted(unit, estimate(unit, indexDir, root, true))) {
                        // ruling P6-R18: the build rewrites its affected shards whole (the expunge merges
                        // them), so their old and new segments coexist until the commit: about twice their
                        // size at the peak. Estimated as a full run (2x the project's Lucene files), an
                        // upper bound: a reshard rewrites at most all of its shards. Retried next poll.
                        continue;
                    }
                    if (!built && !atPublishedRevision(unit, st)) {
                        // the reshard splits the published run: what changed since is indexed (and published) first
                        log.info("Reshard of project {}: its source moved past the published revision; indexing it"
                                + " first", project);
                        if (st.pendingSince == null) st.pendingSince = clock.instant();
                        continue;
                    }
                }
                Run run = new Run(unit, st.leaseShards, clock.millis());
                current = run;
                try {
                    if (stopping) run.abort.request("stopping");
                    com.pharos.cluster.sharding.ReshardJob.Step step = job.step(project, planOf(st, unit), st.source,
                            root, embeds(unit), run.abort, leases);
                    if (step == com.pharos.cluster.sharding.ReshardJob.Step.FLIPPED) {
                        st.publishedGeneration = null; // re-read: LATEST moved to the new version's generations
                        st.publishedGens = null;
                    }
                } finally {
                    current = null;
                }
            } finally {
                busy = null;
            }
            members.beat();
        }
    }

    /** True when the prepared checkout of {@code unit} is at the revision its published generations hold. */
    private boolean atPublishedRevision(ShardId unit, ShardState st) {
        String published = st.publishedRevision;
        if (published == null) return false;
        try {
            if (st.source.kind() == ShardSource.Kind.GIT) {
                return published.equals(checkout.checkedOutRevision(unit).orElse(null));
            }
            return checkout.changedSince(unit, published).isEmpty();
        } catch (IOException | RuntimeException e) {
            log.warn("Project {}: checking its source revision failed: {}", unit, e.toString());
            return false;
        }
    }

    // --------------------------------------------------------------------------- disk guard

    /** True when the disk guard last saw this writer low on disk. */
    private boolean diskLow() {
        return disk != null && disk.low();
    }

    /**
     * Asks the disk guard whether {@code run} fits; when it doesn't, garbage-collects local leftovers
     * first and asks again with fresh numbers.
     */
    private boolean admitted(ShardId shard, DiskGuard.EstimatedRun run) {
        if (disk == null) return true;
        disk.check();
        if (!disk.admits(run) && collectLeftovers() > 0) disk.check();
        if (disk.admits(run)) {
            admittedFree = disk.indexStoreFreeBytes();
            return true;
        }
        log.warn("Shard {}: not starting a {} run: it needs about {} and would leave {} free on the index disk"
                        + " (threshold {}){}", shard, run.full() ? "full" : "incremental",
                DiskGuard.bytes(run.peakBytes()),
                DiskGuard.bytes(Math.max(0, disk.indexStoreFreeBytes() - run.peakBytes())),
                DiskGuard.bytes(disk.indexStoreThreshold()), disk.low() ? "; the writer is low on disk" : "");
        return false;
    }

    /**
     * The run estimate of a shard with a local index ({@link DiskGuard.EstimatedRun}): sizes from the
     * index dir; the run is full when the work loop or the indexer itself
     * ({@link ProjectIndexManager#fullRunReason}: another index format, a broken graph pointer)
     * makes it full; otherwise the changed share counts what the indexer will re-index, by
     * the indexer's own tracker ({@link ProjectIndexManager#stateTracker}): content changes
     * ({@link com.pharos.indexer.FileStateTracker#isDirty}) and, when embedding, files embedded
     * under another chunking version or model
     * ({@link com.pharos.indexer.FileStateTracker#isEmbeddingOutdated}). Never below the shard's
     * floor from an earlier low-disk abort.
     */
    DiskGuard.EstimatedRun estimate(ShardId shard, Path indexDir, Path root, boolean full) {
        if (disk == null) return null;
        if (!full) {
            ShardState st = shards.get(shard);
            String reason = indexer.fullRunReason(shard.value(), planOf(st, shard));
            if (reason != null) {
                log.info("Shard {}: estimating a full run ({})", shard, reason);
                full = true;
            }
        }
        long luceneBytes = ProjectIndexManager.sizeOf(indexDir.resolve("lucene"))
                + ProjectIndexManager.sizeOf(indexDir.resolve(com.pharos.indexer.LocalShardLayout.SHARDS_DIR));
        long graphBytes = ProjectIndexManager.sizeOf(com.pharos.graph.CallGraph.projectGraphDir(indexDir));
        long cacheBytes = embedCacheBytes(indexDir);
        java.util.function.Predicate<Path> changed = null;
        if (!full) {
            boolean embed = embeds(shard);
            com.pharos.indexer.FileStateTracker t = indexer.stateTracker(indexDir, embed);
            changed = f -> t.isDirty(f) || (embed && t.isEmbeddingOutdated(f));
        }
        long[] src = sourceBytes(root, changed);
        return new DiskGuard.EstimatedRun(shard.value(), full, luceneBytes, graphBytes, cacheBytes, src[0], src[1])
                .withFloor(estimateFloor.getOrDefault(shard, 0L));
    }

    /**
     * No local index. With a published generation the restore brings it, then a full run over its
     * sizes is the worst case; with none it is a first index
     * ({@link DiskGuard.EstimatedRun#firstIndex}: a multiple of the source size).
     */
    DiskGuard.EstimatedRun estimateWithoutLocalIndex(ShardId shard, Path root) {
        if (disk == null) return null;
        long luceneBytes = 0;
        long graphBytes = 0;
        boolean published = false;
        try {
            ShardState st = shards.get(shard);
            boolean projectFilesCounted = false;
            for (ShardId s : st == null ? List.of(shard) : st.leaseShards) {
                Optional<Generation> latest = replicas.latest(s);
                if (latest.isEmpty()) continue;
                published = true;
                ShardManifest m = replicas.manifest(s, latest.get());
                for (com.pharos.cluster.objectstore.ManifestFile f : m.files()) {
                    if (com.pharos.cluster.objectstore.ManifestFile.LUCENE.equals(f.role())) luceneBytes += f.length();
                    else graphBytes += f.length();
                }
                if (!projectFilesCounted) { // one graph for the whole project
                    for (com.pharos.cluster.objectstore.ManifestFile f : m.projectFileList()) graphBytes += f.length();
                    projectFilesCounted = true;
                }
            }
        } catch (IOException | RuntimeException e) {
            log.debug("Sizing the published generation of {} failed: {}", shard, e.toString());
        }
        long[] src = sourceBytes(root, null);
        long floor = estimateFloor.getOrDefault(shard, 0L);
        if (!published) return DiskGuard.EstimatedRun.firstIndex(shard.value(), src[0]).withFloor(floor);
        long cacheBytes = luceneBytes; // the restored embedding cache: about the vectors the Lucene files hold
        return new DiskGuard.EstimatedRun(shard.value(), true, luceneBytes, graphBytes, cacheBytes, src[0], src[0],
                luceneBytes + graphBytes + cacheBytes, floor);
    }

    /** The shard's estimate floor from a low-disk abort (0 when none). */
    long estimateFloor(ShardId shard) {
        return estimateFloor.getOrDefault(shard, 0L);
    }

    private static long embedCacheBytes(Path indexDir) {
        long total = 0;
        try (var files = Files.list(indexDir)) {
            for (Path f : files.toList()) {
                if (com.pharos.cluster.objectstore.ObjectStoreWriterStateStore.EMBED_CACHE
                        .matcher(f.getFileName().toString()).matches()) {
                    total += Files.size(f);
                }
            }
        } catch (IOException e) {
            log.debug("Sizing the embedding caches in {} failed: {}", indexDir, e.toString());
        }
        return total;
    }

    /**
     * {@code [total, changed]} bytes of the regular files under {@code root} ({@code .git} skipped);
     * changed = files {@code changed} accepts (all of them without a predicate).
     */
    private static long[] sourceBytes(Path root, java.util.function.Predicate<Path> changed) {
        long[] out = {0, 0};
        try {
            Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
                @Override
                public java.nio.file.FileVisitResult preVisitDirectory(Path d,
                        java.nio.file.attribute.BasicFileAttributes a) {
                    return d.getFileName() != null && d.getFileName().toString().equals(".git") && !d.equals(root)
                            ? java.nio.file.FileVisitResult.SKIP_SUBTREE : java.nio.file.FileVisitResult.CONTINUE;
                }

                @Override
                public java.nio.file.FileVisitResult visitFile(Path f, java.nio.file.attribute.BasicFileAttributes a) {
                    if (!a.isRegularFile()) return java.nio.file.FileVisitResult.CONTINUE;
                    out[0] += a.size();
                    if (changed == null || changed.test(f)) out[1] += a.size();
                    return java.nio.file.FileVisitResult.CONTINUE;
                }

                @Override
                public java.nio.file.FileVisitResult visitFileFailed(Path f, IOException e) {
                    return java.nio.file.FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.debug("Sizing the checkout {} failed: {}", root, e.toString());
        }
        return out;
    }

    /**
     * Garbage-collects run leftovers of every locally registered shard except the one being worked
     * on ({@link ProjectIndexManager#removeRunLeftovers}: retired graph generations and stale spools,
     * under the Lucene write lock; the retention rule the index runs apply). The live commit, the
     * live graph and the persistent embedding caches stay.
     *
     * @return bytes freed
     */
    long collectLeftovers() {
        long freed = 0;
        for (ProjectMeta m : projects.listAll()) {
            String name = m.getName();
            ShardId skip = busy; // re-read per shard: the loop thread may have started on one meanwhile
            if (name == null || (skip != null && skip.value().equals(name))) continue;
            try {
                freed += Math.max(0, indexer.removeRunLeftovers(name));
            } catch (RuntimeException e) {
                log.debug("Collecting the leftovers of {} failed: {}", name, e.toString());
            }
        }
        if (freed > 0) log.info("Disk guard: garbage collection freed {}", DiskGuard.bytes(freed));
        return freed;
    }

    /**
     * Probes the disk now. When low, garbage-collects leftovers first and probes again; when still
     * low, aborts the run in progress at its next safe point (a run already past its commit finishes,
     * publish included). Runs on the disk-guard thread every {@code checkSeconds} and at each tick.
     *
     * @return true when the writer is low on disk
     */
    boolean checkDisk() {
        if (disk == null) return false;
        synchronized (diskMu) {
            DiskGuard.Status s = disk.check();
            if (s.low() && collectLeftovers() > 0) s = disk.check();
            if (s.low()) {
                Run r = current;
                if (r != null && !r.abort.requested()) {
                    long freeNow = disk.indexStoreFreeBytes();
                    if (r.abort.request("writer low on disk")) {
                        if (r.freeAtStart >= 0 && freeNow >= 0) {
                            r.consumedAtDiskAbort = Math.max(0, r.freeAtStart - freeNow);
                        }
                        log.warn("Writer {} is low on disk: aborting the run of shard {} at its next safe point",
                                settings.node(), r.shard);
                    } else {
                        log.info("Writer {} is low on disk; the run of shard {} is past its commit, letting it"
                                + " publish", settings.node(), r.shard);
                    }
                }
            }
            return s.low();
        }
    }

    private void checkDiskQuietly() {
        try {
            checkDisk();
        } catch (RuntimeException | Error e) {
            log.warn("Writer disk check failed: {}", e.toString());
        }
    }

    /** Registers a restored generation under this writer's paths; returns the root it was indexed under. */
    private String registerRestored(ShardId shard, ShardState st, Generation gen, Path root, Path indexDir)
            throws IOException {
        ShardManifest m;
        if (planOf(st, shard).isSplit()) {
            // gen is the project's run; every shard manifest carries the project's meta: take the first's
            ShardId first = st.leaseShards.getFirst();
            m = replicas.manifest(first, replicas.latest(first).orElseThrow(() -> new IOException(
                    "shard " + first + " has no published generation")));
        } else {
            m = replicas.manifest(shard, gen);
        }
        ProjectMeta meta = ShardManifest.stripRefs(m.projectMeta()); // refs stay in the restored unresolved-refs.json
        String originalRoot = meta.getRootPath();
        meta.setName(shard.value());
        // The restored documents and file state are those of originalRoot: register that root until a
        // run here commits (the index run registers this writer's root). Registering ours at once let
        // a full run that died before its commit be followed by an incremental one, which indexed every
        // file a second time under this writer's paths (Phase 4 gate re-run: lucene gen 12, 2 x docs).
        meta.setRootPath(originalRoot != null ? originalRoot : root.toString());
        meta.setIndexPath(indexDir.toString());
        projects.register(meta);
        return originalRoot;
    }

    private static boolean sameRoot(String recorded, Path root) {
        if (recorded == null) return false;
        return Path.of(recorded).toAbsolutePath().normalize().equals(root);
    }

    /** Right after a publish of {@code shard}: release it when this writer holds more than its share. */
    private void maybeRelease(ShardId shard, int share) {
        ShardState st = shards.get(shard);
        if (st == null || reshardPinned(shard)) return;
        if (balancer.mayRelease(heldSet().size(), share, st.pinned, st.acquiredAt, clock.instant())) {
            log.info("Releasing shard {} after its publish: writer {} holds {} lease(s), its share is {}", shard,
                    settings.node(), heldSet().size(), share);
            release(shard);
        }
    }

    /** Surplus shards with nothing unpublished are at a clean point too. */
    private void releaseIdleSurplus(int share) {
        List<ShardId> held = shards.keySet().stream().sorted(Comparator.comparing(ShardId::value)).toList();
        for (ShardId shard : held) {
            ShardState st = shards.get(shard);
            // only when the last poll confirmed LATEST covers the source (M5)
            if (st == null || st.pendingSince != null || !st.prepared || !st.lastPollOk) continue;
            if (reshardPinned(shard)) continue; // its holder flips it first
            if (balancer.mayRelease(heldSet().size(), share, st.pinned, st.acquiredAt, clock.instant())) {
                log.info("Releasing idle shard {}: writer {} holds {} lease(s), its share is {}", shard,
                        settings.node(), heldSet().size(), share);
                release(shard);
            }
        }
    }

    /**
     * M4: a surplus shard whose runs or source checks failed {@code maxConsecutiveFailures} times in a
     * row is released (never a pinned one, never within {@code minHold}), so another writer can try it.
     */
    private void maybeReleaseFailing(ShardId shard, int share) {
        ShardState st = shards.get(shard);
        if (st == null || st.consecutiveFailures.get() < settings.maxConsecutiveFailures()) return;
        if (balancer.mayRelease(heldSet().size(), share, st.pinned, st.acquiredAt, clock.instant())) {
            log.warn("Releasing shard {} after {} failed runs or source checks in a row so another writer can try it:"
                            + " writer {} holds"
                            + " {} lease(s), its share is {}", shard, st.consecutiveFailures.get(), settings.node(),
                    heldSet().size(), share);
            avoid(shard);
            release(shard);
        }
    }

    /**
     * Releases a shard this writer cannot index (low on disk, or its run would not fit), so a writer
     * with space can take it. Between runs only (the loop thread); never a pinned one: only this
     * writer has its files, so it stays held and its runs are refused until there is space.
     */
    private void releaseUnindexable(ShardId shard, String why) {
        ShardState st = shards.get(shard);
        if (st == null || st.pinned) return;
        log.warn("Releasing shard {} so a writer with space can index it: {}", shard, why);
        avoid(shard);
        release(shard);
    }

    /** Not re-acquired by this writer for {@code max(minHold, poll)}: someone else gets the chance. */
    private void avoid(ShardId shard) {
        Duration d = settings.minHold().compareTo(settings.poll()) > 0 ? settings.minHold() : settings.poll();
        avoidUntil.put(shard, clock.instant().plus(d));
    }

    /** Releases every lease of the unit and forgets it. */
    private void release(ShardId shard) {
        ShardState st = shards.remove(shard);
        List<ShardId> own = st != null ? st.leaseShards : List.of(shard);
        for (Lease l : leases.held()) if (own.contains(l.shard())) leases.release(l);
        com.pharos.cluster.sharding.ReshardJob job = reshard;
        if (job != null) job.releaseLease(shard);
    }

    /**
     * True when the project's reshard is published and pinned but not flipped: its unit is not released
     * at a clean point (its holder flips it), and no index run of it starts.
     */
    private boolean reshardPinned(ShardId unit) {
        ProjectShards map = lastMap;
        return map.reshardRequestOf(unit.value()).isPresent()
                && map.stagedOf(unit.value()).filter(st -> !st.abandoned() && st.pinned()).isPresent();
    }

    /** Renews every lease of the unit (right after its publish). */
    private void renewUnit(ShardState st) {
        for (Lease l : leases.held()) if (st.leaseShards.contains(l.shard())) leases.renew(l);
    }

    /** The held units: those whose every shard lease this writer holds. */
    private Set<ShardId> heldSet() {
        Set<ShardId> leased = leasedShards();
        Set<ShardId> s = new HashSet<>();
        for (Map.Entry<ShardId, ShardState> e : shards.entrySet()) {
            if (leased.containsAll(e.getValue().leaseShards)) s.add(e.getKey());
        }
        return s;
    }

    /** True when this writer holds every shard lease of the unit. */
    private boolean unitHeld(ShardId unit) {
        ShardState st = shards.get(unit);
        return st != null && leasedShards().containsAll(st.leaseShards);
    }

    private Set<ShardId> leasedShards() {
        Set<ShardId> s = new HashSet<>();
        for (Lease l : leases.held()) s.add(l.shard());
        return s;
    }

    /** The unit's plan: as acquired, else as the catalog last listed it, else {@code [project]}. */
    private ShardPlan planOf(ShardState st, ShardId unit) {
        if (st != null && st.plan != null) return st.plan;
        ShardPlan p = plans.get(unit);
        return p != null ? p : ShardPlan.single(unit.value());
    }

    // ------------------------------------------------------------------------- publish check

    /**
     * The publisher's lease check (via {@link WorkQueueLeaseGuard}): passes only for the run this loop
     * is executing, on its own thread, while the lease is held.
     */
    void assertPublishing(ShardId shard) throws PublishConflictException {
        Run r = current;
        if (r == null || r.thread != Thread.currentThread() || !r.leaseShards.contains(shard)) {
            throw new PublishConflictException(WorkQueueLeaseGuard.refusal(shard.value()));
        }
        leases.assertHeld(shard);
    }

    // ------------------------------------------------------------------------------ status

    /** This writer's heartbeat as of now. */
    public WriterHeartbeat heartbeat() {
        List<String> held = new ArrayList<>();
        for (Lease l : leases.held()) held.add(l.shard().value());
        List<String> pinned = new ArrayList<>();
        Map<String, WriterHeartbeat.Backlog> backlog = new TreeMap<>();
        Instant now = clock.instant();
        Set<ShardId> heldUnits = heldSet();
        for (Map.Entry<ShardId, ShardState> e : shards.entrySet()) {
            ShardState st = e.getValue();
            if (!heldUnits.contains(e.getKey())) continue;
            if (st.pinned) pinned.add(e.getKey().value());
            Instant since = st.pendingSince;
            backlog.put(e.getKey().value(), new WriterHeartbeat.Backlog(st.sourceRevision, st.publishedRevision,
                    since != null, since == null ? null : Math.max(0, Duration.between(since, now).toSeconds())));
        }
        pinned.sort(null);
        Run r = current;
        WriterHeartbeat.Indexing indexing = r == null ? null
                : new WriterHeartbeat.Indexing(r.shard.value(), r.phase, r.progress, r.startedAt);
        DiskGuard.Status d = disk == null ? null : disk.status();
        return new WriterHeartbeat(settings.node(), true, held, pinned, backlog, indexing, startedAt, stopping,
                d == null ? null : d.toHeartbeat());
    }

    /** The disk guard's last view; empty without a guard. */
    public Optional<DiskGuard.Status> diskStatus() {
        return disk == null ? Optional.empty() : Optional.ofNullable(disk.status());
    }

    /** The shards this writer holds the lease of, sorted. */
    public List<ShardId> heldShards() {
        return leases.held().stream().map(Lease::shard).toList();
    }

    /** Generations this loop published of {@code shard}. */
    public int publishes(ShardId shard) {
        return publishes.getOrDefault(shard, 0);
    }

    /** The shard being indexed, if any. */
    public Optional<ShardId> running() {
        Run r = current;
        return r == null ? Optional.empty() : Optional.of(r.shard);
    }

    /** The live writers as this writer last saw them. */
    public List<WriterRegistry.LiveWriter> liveWriters() {
        return members.live();
    }

    LeaseManager leaseManager() {
        return leases;
    }

    // ------------------------------------------------------------------------------- stop

    /**
     * Graceful stop within {@code budget} ({@code cluster.drainSeconds}): no new run starts; a run in
     * progress may finish until the budget minus a reserve (a quarter, at least 1 s), then is aborted
     * at a safe point (a run past its commit finishes its publish); then all leases are released
     * (before the lease manager closes) and the heartbeat is deleted. Idempotent.
     */
    public void stop(Duration budget) {
        ScheduledExecutorService s;
        synchronized (stopMu) {
            if (stopped) return;
            stopped = true;
            s = scheduler;
            scheduler = null;
            if (diskWatch != null) diskWatch.shutdownNow(); // a probe only: nothing to drain
            diskWatch = null;
            if (detector != null) detector.shutdownNow(); // read-only source checks: nothing to drain
            detector = null;
            if (reshardRenewer != null) reshardRenewer.shutdownNow(); // the leases are released below
            reshardRenewer = null;
        }
        stopping = true;
        long start = System.nanoTime();
        long total = Math.max(0, budget.toNanos());
        long reserve = Math.min(total, Math.max(TimeUnit.SECONDS.toNanos(1), total / 4));
        members.beat(); // announce stopping: peers stop counting this writer
        if (s != null) s.shutdown(); // no interrupt: a run is stopped by its abort, never mid-I/O
        boolean idle = awaitIdle(total - reserve);
        if (!idle) {
            Run r = current;
            if (r != null) {
                if (r.abort.request("graceful stop (cluster.drainSeconds " + budget.toSeconds() + " s)")) {
                    log.warn("Stopping: aborting the run of shard {} at its next safe point", r.shard);
                } else {
                    log.info("Stopping: the run of shard {} is past its commit; letting it publish", r.shard);
                }
            }
            idle = awaitIdle(total - (System.nanoTime() - start));
        }
        if (!idle) {
            log.warn("Stopping with a run still in progress after {} s; releasing its lease (its publish will fail"
                    + " the lease check)", budget.toSeconds());
        }
        for (Lease l : leases.held()) leases.release(l); // before close: after it a release cannot write
        com.pharos.cluster.sharding.ReshardJob job = reshard;
        if (job != null) {
            for (Lease l : job.leases().held()) job.leases().release(l);
            job.leases().close();
        }
        shards.clear();
        leases.close();
        members.close();
        log.info("Writer {} stopped: leases released, heartbeat removed", settings.node());
    }

    private boolean awaitIdle(long nanos) {
        try {
            if (tickLock.tryLock(Math.max(0, nanos), TimeUnit.NANOSECONDS)) {
                tickLock.unlock();
                return true;
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** {@link #stop} with no budget: a run in progress is aborted at once. */
    @Override
    public void close() {
        stop(Duration.ZERO);
    }
}
