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
 * old paths. A store failure during restore is retried on the next poll (never a full re-embed); a
 * missing or unusable backup makes the run a full index.
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
 * <p><b>Failing shards</b>: a surplus shard (not pinned, past {@code minHold}) whose runs failed
 * {@code cluster.writer.maxConsecutiveFailures} times in a row is released so another writer can
 * try it. A surplus shard is released when idle only after a poll that succeeded.
 *
 * <p><b>Graceful stop</b> ({@link #stop}): no new run starts; a run in progress gets until the
 * budget minus a reserve to finish, then is aborted at a safe point ({@link IndexAbort}; a run past
 * its commit finishes, publish included); then every lease is released (before the lease manager is
 * closed, after which a release can no longer write) and the heartbeat is deleted.
 */
public final class WorkLoop implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WorkLoop.class);

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
        /** Failed runs in a row (reset by a publish); M4 releases a surplus shard at the limit. */
        volatile int consecutiveFailures;
    }

    /** The run in progress. */
    private static final class Run {
        final ShardId shard;
        final Thread thread = Thread.currentThread();
        final IndexAbort abort = new IndexAbort();
        final long startedAt;
        volatile String phase = WriterHeartbeat.PARSE;
        volatile double progress;
        /** Free bytes of the index store when the run was admitted; -1 without a guard. */
        volatile long freeAtStart = -1;
        /** What the run had consumed when the guard aborted it for low disk; -1 when it didn't. */
        volatile long consumedAtDiskAbort = -1;

        Run(ShardId shard, long startedAt) {
            this.shard = shard;
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
    private final long startedAt;

    private final Map<ShardId, ShardState> shards = new ConcurrentHashMap<>();
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
    private ScheduledExecutorService diskWatch;
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
        this.settings = Objects.requireNonNull(settings, "settings");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.replicas = Objects.requireNonNull(replicas, "replicas");
        this.writerState = Objects.requireNonNull(writerState, "writerState");
        this.indexer = Objects.requireNonNull(indexer, "indexer");
        this.lucene = Objects.requireNonNull(lucene, "lucene");
        this.projects = Objects.requireNonNull(projects, "projects");
        this.checkout = Objects.requireNonNull(checkout, "checkout");
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
            forgetLost();
            List<ShardEntry> entries;
            try {
                entries = catalog.entries();
            } catch (IOException | RuntimeException e) {
                log.warn("Reading the shard catalog failed (retrying next poll): {}", e.toString());
                return;
            }
            Map<ShardId, ShardSource> sources = resolveSources(entries);
            if (diskLow()) {
                log.debug("Writer {} is low on disk: acquiring no lease", settings.node());
            } else {
                acquire(sources);
            }
            members.beat(); // announce the leases now, before a long run
            List<ShardId> pending = poll();
            for (ShardId shard : pending) {
                if (stopping) return;
                if (!leases.isHeld(shard)) continue;
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

    /** The shards of the catalog this writer could index, with their source; pins source-less local ones. */
    private Map<ShardId, ShardSource> resolveSources(List<ShardEntry> entries) {
        Map<ShardId, ShardSource> out = new LinkedHashMap<>();
        Set<ShardId> pins = new HashSet<>();
        for (ShardEntry e : entries) {
            if (e.source().isPresent()) {
                out.put(e.id(), e.source().get());
                continue;
            }
            Optional<ProjectMeta> local = projects.find(e.id().value());
            if (local.isPresent() && local.get().getRootPath() != null
                    && Files.isDirectory(Path.of(local.get().getRootPath()))) {
                out.put(e.id(), ShardSource.path(local.get().getRootPath()));
                pins.add(e.id());
            }
        }
        pinnedMine = Set.copyOf(pins);
        // a held shard that left the catalog (or lost its source here) is released
        for (ShardId held : heldSet()) {
            if (!out.containsKey(held)) {
                log.info("Shard {} is no longer workable here (not in the catalog, or no source); releasing it", held);
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
            if (!leases.isHeld(pin)) tryAcquire(pin, sources.get(pin));
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

    private boolean tryAcquire(ShardId shard, ShardSource source) {
        try {
            if (leases.tryAcquire(shard).isEmpty()) return false;
        } catch (IOException e) {
            log.warn("Acquiring the lease of shard {} failed: {}", shard, e.toString());
            return false;
        }
        ShardState st = new ShardState();
        st.source = source;
        st.pinned = pinnedMine.contains(shard);
        st.acquiredAt = clock.instant();
        shards.put(shard, st);
        return true;
    }

    /** Drops the state of shards whose lease was lost (taken over, a failed renewal). */
    private void forgetLost() {
        Set<ShardId> held = heldSet();
        for (ShardId s : new ArrayList<>(shards.keySet())) {
            if (!held.contains(s)) {
                shards.remove(s);
                log.warn("Writer {} no longer holds the lease of shard {}; stopped working on it", settings.node(), s);
            }
        }
    }

    /** Polls every held shard's source; returns the pending ones, oldest unpublished change first. */
    private List<ShardId> poll() {
        Instant now = clock.instant();
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
                refreshPublished(shard, st);
                Optional<String> changed = checkout.changedSince(shard, st.publishedRevision);
                if (changed.isPresent()) {
                    st.sourceRevision = changed.get();
                    if (st.pendingSince == null) st.pendingSince = now;
                } else {
                    st.sourceRevision = st.publishedRevision;
                    st.pendingSince = null;
                }
                st.lastPollOk = true;
            } catch (IOException | RuntimeException ex) {
                // pendingSince is now unknown: no idle release until a poll succeeds again (M5)
                st.lastPollOk = false;
                log.warn("Polling the source of shard {} failed (retrying next poll): {}", shard, ex.toString());
            }
        }
        return shards.entrySet().stream().filter(e -> e.getValue().pendingSince != null)
                .sorted(Comparator.comparing((Map.Entry<ShardId, ShardState> e) -> e.getValue().pendingSince)
                        .thenComparing(e -> e.getKey().value()))
                .map(Map.Entry::getKey).toList();
    }

    /** The source revision {@code LATEST} holds (from its manifest's project meta); null when unknown. */
    private void refreshPublished(ShardId shard, ShardState st) throws IOException {
        Optional<Generation> latest = replicas.latest(shard);
        if (latest.isEmpty()) {
            st.publishedGeneration = null;
            st.publishedRevision = null;
            return;
        }
        if (latest.get().equals(st.publishedGeneration)) return;
        ProjectMeta meta = replicas.manifest(shard, latest.get()).projectMeta();
        st.publishedRevision = meta == null ? null : meta.getSourceRevision();
        st.publishedGeneration = latest.get();
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
            if (o == Outcome.PUBLISHED) st.consecutiveFailures = 0;
            else if (o == Outcome.FAILED) st.consecutiveFailures++;
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
            requireRestorable(shard);
            Optional<Generation> restored;
            try {
                restored = writerState.restore(shard, indexDir);
            } catch (IOException e) {
                // a store outage: retry later rather than paying for a full re-embed now
                log.warn("Restoring the writer state of shard {} failed (retrying next poll): {}", shard, e.toString());
                return Outcome.FAILED;
            }
            if (restored.isPresent()) {
                String originalRoot;
                try {
                    originalRoot = registerRestored(shard, restored.get(), root, indexDir);
                } catch (IOException | RuntimeException e) {
                    log.warn("Registering the restored shard {} failed (retrying next poll): {}", shard, e.toString());
                    return Outcome.FAILED;
                }
                full = !sameRoot(originalRoot, root);
                why = full ? "restored generation " + restored.get().value() + " was indexed under " + originalRoot
                        + ", this writer checks out at " + root : "restored generation " + restored.get().value();
            } else {
                full = true;
                why = "no usable writer state to resume from";
            }
        } else {
            Optional<ProjectMeta> local = projects.find(shard.value());
            if (local.isEmpty()) {
                full = true;
                why = "the local index is not registered";
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

        Run run = new Run(shard, clock.millis());
        run.freeAtStart = disk == null ? -1 : admittedFree;
        current = run;
        try {
            if (stopping) run.abort.request("stopping");
            if (diskLow()) run.abort.request("writer low on disk");
            indexer.index(root, shard.value(), !full, false, embeds(shard), settings.buildSynonyms(),
                    run.listener(), run.abort, revision);
            st.publishedRevision = revision;
            st.sourceRevision = revision;
            st.pendingSince = null;
            st.publishedGeneration = null; // re-read on the next poll
            publishes.merge(shard, 1, Integer::sum);
            leases.held().stream().filter(l -> l.shard().equals(shard)).findFirst().ifPresent(leases::renew);
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
            if (!leases.isHeld(shard)) {
                shards.remove(shard);
                log.warn("Shard {} not published: writer {} lost its lease and stops working on it ({})", shard,
                        settings.node(), e.getMessage());
            } else {
                st.publishedGeneration = null;
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
            String reason = indexer.fullRunReason(shard.value());
            if (reason != null) {
                log.info("Shard {}: estimating a full run ({})", shard, reason);
                full = true;
            }
        }
        long luceneBytes = ProjectIndexManager.sizeOf(indexDir.resolve("lucene"));
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
            Optional<Generation> latest = replicas.latest(shard);
            if (latest.isPresent()) {
                published = true;
                for (com.pharos.cluster.objectstore.ManifestFile f : replicas.manifest(shard, latest.get()).files()) {
                    if (com.pharos.cluster.objectstore.ManifestFile.LUCENE.equals(f.role())) luceneBytes += f.length();
                    else graphBytes += f.length();
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
    private String registerRestored(ShardId shard, Generation gen, Path root, Path indexDir) throws IOException {
        ShardManifest m = replicas.manifest(shard, gen);
        ProjectMeta meta = ShardManifest.stripRefs(m.projectMeta()); // refs stay in the restored unresolved-refs.json
        String originalRoot = meta.getRootPath();
        meta.setName(shard.value());
        meta.setRootPath(root.toString());
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
        if (st == null) return;
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
            if (balancer.mayRelease(heldSet().size(), share, st.pinned, st.acquiredAt, clock.instant())) {
                log.info("Releasing idle shard {}: writer {} holds {} lease(s), its share is {}", shard,
                        settings.node(), heldSet().size(), share);
                release(shard);
            }
        }
    }

    /**
     * M4: a surplus shard whose runs failed {@code maxConsecutiveFailures} times in a row is released
     * (never a pinned one, never within {@code minHold}), so another writer can try it.
     */
    private void maybeReleaseFailing(ShardId shard, int share) {
        ShardState st = shards.get(shard);
        if (st == null || st.consecutiveFailures < settings.maxConsecutiveFailures()) return;
        if (balancer.mayRelease(heldSet().size(), share, st.pinned, st.acquiredAt, clock.instant())) {
            log.warn("Releasing shard {} after {} failed runs in a row so another writer can try it: writer {} holds"
                            + " {} lease(s), its share is {}", shard, st.consecutiveFailures, settings.node(),
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

    private void release(ShardId shard) {
        shards.remove(shard);
        leases.held().stream().filter(l -> l.shard().equals(shard)).findFirst().ifPresent(leases::release);
    }

    private Set<ShardId> heldSet() {
        Set<ShardId> s = new HashSet<>();
        for (Lease l : leases.held()) s.add(l.shard());
        return s;
    }

    // ------------------------------------------------------------------------- publish check

    /**
     * The publisher's lease check (via {@link WorkQueueLeaseGuard}): passes only for the run this loop
     * is executing, on its own thread, while the lease is held.
     */
    void assertPublishing(ShardId shard) throws PublishConflictException {
        Run r = current;
        if (r == null || r.thread != Thread.currentThread() || !r.shard.equals(shard)) {
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
        for (Map.Entry<ShardId, ShardState> e : shards.entrySet()) {
            ShardState st = e.getValue();
            if (!held.contains(e.getKey().value())) continue;
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
