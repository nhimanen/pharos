package com.pharos.cluster.sharding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.cluster.Generation;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.WriterStateStore;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.objectstore.StoredObject;
import com.pharos.cluster.writer.Lease;
import com.pharos.cluster.writer.LeaseGuard;
import com.pharos.cluster.writer.LeaseManager;
import com.pharos.indexer.IndexAbort;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The resharding job of one project (Phase 6 Task 4): moves a project from its active spec version N to
 * a requested version N+1 while readers keep serving N, then flips routing with one catalog CAS. It runs
 * on the writer that holds the project's work unit (ruling P6-R1: that writer owns every shard of N and
 * has the project's local index and writer state), under the per-project reshard lease
 * ({@code projects/<p>/reshard-lease}), one step per work-loop tick:
 * <ol>
 *   <li><b>Stage.</b> The request ({@code pharos reshard}, {@link ProjectShards#reshardRequestOf}) names
 *       spec version N+1. The job computes its shards from the source ({@link ProjectIndexManager#shardsFor}:
 *       every key value the files have; a ring's shards are its names) and writes them as the project's
 *       staged version. Each staged shard is registered with the project's source (decision: <b>a split
 *       project's source is recorded on every one of its shards</b>, so whichever shard the work loop reads
 *       first has it, and dropping a retired shard never loses it).</li>
 *   <li><b>Build</b> ({@link ProjectIndexManager#reshard}): only files whose shard changes are parsed again
 *       and written to their new shards, embeddings from the restored persistent cache (nothing unchanged
 *       is embedded again); shards that neither gain nor lose files are not touched.</li>
 *   <li><b>Publish</b> the shards of N+1 that changed as new generations <b>without moving their
 *       {@code LATEST}</b> ({@link ObjectStorePublisher#stageShard}), sharing N's index run, project files
 *       and meta. A shard N+1 shares with N unchanged keeps its current generation, byte-identical, which
 *       N+1 references as it is. Then the staged version's generations are pinned in the catalog: readers
 *       materialise the rewritten shards (not serving them) and activate the new ones.</li>
 *   <li><b>Flip</b> once every live reader has the new version's generations (heartbeats: served or
 *       prepared), or after {@code syncTimeout}: one CAS makes N+1 active with its generations pinned and
 *       N the project's {@code previous} version ({@code retiredAt}); then each rewritten shard's
 *       {@code LATEST} moves to its pinned generation ({@link ObjectStorePublisher#finishFlip}).</li>
 *   <li><b>Retire.</b> After the retention window, {@link #collect} deletes the shards only N had and
 *       drops N from the catalog; abandoned (cancelled) staged versions likewise.</li>
 * </ol>
 *
 * <p><b>Readers never mix versions</b> (ruling 1 carried from Task 3, design (b) with one refinement):
 * a rewritten shard keeps its id, so its N+1 generation must not be served while N is routed. It is
 * not: its {@code LATEST} does not move before the flip, and readers sync a staged version only at the
 * generations the catalog pins, activating a generation only for shards the active version does not
 * have. At the flip the router (one catalog read per query) reads N+1 at its pinned generations, falling
 * back to N (also pinned) while the readers have not activated N+1 yet; the coordinator checks every
 * shard's generation. A query therefore reads one spec version, and every file once, at every point.
 *
 * <p><b>Interruption</b> (Review Focus 1). Until the flip N is the only routed version; a half-built
 * N+1 is a staged entry without generations, which readers ignore. Every step is idempotent and its
 * progress is recorded ({@code projects/<p>/reshard/job.json}, {@link #status}): the writer that holds
 * the project next (this one after a restart, or another after a takeover, with the restored writer
 * state) resumes it, reusing every generation already published whose files equal its local build. A
 * cancelled reshard ({@code pharos reshard --cancel}) is abandoned: GC removes its shards after the
 * retention window.
 *
 * <p><b>New key values.</b> Decision: an index run never adds shards. Under {@code [project, module]} a
 * new module keeps failing the run with "reshard the project to add it" until {@code pharos reshard
 * <project> --spec "<the same spec>"} runs: a reshard always creates a new spec version, and its shards
 * come from the files, so the new module gets its shard (the other shards are unaffected).
 */
public final class ReshardJob {

    private static final Logger log = LoggerFactory.getLogger(ReshardJob.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** What one step came to. */
    public enum Step {
        /** No reshard requested for the project. */
        NONE,
        /** Another writer holds the project's reshard lease. */
        REFUSED,
        /** Published and pinned; waiting for the readers (or the timeout) before the flip. */
        WAITING,
        /** Routing now uses the new version. */
        FLIPPED,
        /** The step failed (logged and recorded); the next tick retries. */
        FAILED
    }

    /**
     * @param syncTimeout how long the flip waits for the readers to have the new version
     * @param retention   how long a retired or abandoned version is kept ({@code cluster.retentionMinutes})
     * @param minPinned   the least time between pinning the versions and the flip: every coordinator's
     *                    cached catalog is re-read within it, so none still routes the active version
     *                    without its pins after the flip (a few catalog refresh periods)
     */
    public record Settings(Duration syncTimeout, Duration retention, Duration minPinned) {
        public Settings {
            Objects.requireNonNull(syncTimeout, "syncTimeout");
            Objects.requireNonNull(retention, "retention");
            Objects.requireNonNull(minPinned, "minPinned");
        }

        public Settings(Duration syncTimeout, Duration retention) {
            this(syncTimeout, retention, Duration.ZERO);
        }
    }

    private final String node;
    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final ObjectStoreCatalog catalog;
    private final ObjectStorePublisher publisher;
    private final ReplicaSource replicas;
    private final WriterStateStore writerState;
    private final ProjectIndexManager indexer;
    private final LuceneIndexer lucene;
    private final LeaseManager leases;
    private final Supplier<List<ReaderHeartbeat>> readers;
    private final GenerationGc gc;
    private final Clock clock;
    private final Settings settings;

    /**
     * @param leases  the reshard leases ({@link ObjectStoreLayout#reshardLeaseKey}); renewed by the caller
     * @param readers the live readers (their heartbeats)
     * @param gc      purges retired shards; null keeps them
     */
    public ReshardJob(String node, ObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                      ObjectStorePublisher publisher, ReplicaSource replicas, WriterStateStore writerState,
                      ProjectIndexManager indexer, LuceneIndexer lucene, LeaseManager leases,
                      Supplier<List<ReaderHeartbeat>> readers, GenerationGc gc, Clock clock, Settings settings) {
        this.node = Objects.requireNonNull(node, "node");
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.replicas = Objects.requireNonNull(replicas, "replicas");
        this.writerState = Objects.requireNonNull(writerState, "writerState");
        this.indexer = Objects.requireNonNull(indexer, "indexer");
        this.lucene = Objects.requireNonNull(lucene, "lucene");
        this.leases = Objects.requireNonNull(leases, "leases");
        this.readers = Objects.requireNonNull(readers, "readers");
        this.gc = gc;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    /** The reshard leases this job takes (the caller renews them with its shard leases). */
    public LeaseManager leases() {
        return leases;
    }

    /** True when a reshard of {@code project} is built locally and waits for its flip (no index run now). */
    public boolean builtLocally(String project, ProjectShards map) {
        Optional<ProjectShards.Staged> st = map.stagedOf(project).filter(s -> !s.abandoned());
        if (st.isEmpty() || map.reshardRequestOf(project).isEmpty()) return false;
        ShardPlan to = new ShardPlan(project, st.get().spec(), st.get().shards());
        return lucene.localLayout(project).filter(l -> l.matches(to)).isPresent();
    }

    /**
     * After a flip: moves the {@code LATEST} of each pinned shard of {@code project} that lags its pin
     * (the flip's own finish was interrupted). Must run before any index run of the project, which would
     * otherwise number its generations from the old {@code LATEST}. Idempotent.
     */
    public void finish(String project, ProjectShards map, LeaseGuard guard) throws IOException {
        Map<ShardId, Long> pins = map.pinsOf(project);
        if (pins.isEmpty()) return;
        int moved = publisher.finishFlip(pins, guard);
        if (moved > 0) log.info("Reshard of project {}: moved {} LATEST object(s) to the flipped version's generations",
                project, moved);
    }

    /**
     * One step of the reshard of {@code project}, whose active version is {@code active} and whose
     * local index (this writer's, of {@code active}'s published run, or already of the staged version
     * when resuming) is checked out at {@code root}.
     */
    public Step step(String project, ShardPlan active, ShardSource source, Path root, boolean embed, IndexAbort abort) {
        ShardId unit = ShardId.ofProject(project);
        ProjectShards map;
        try {
            map = catalog.projectShards();
        } catch (IOException | RuntimeException e) {
            log.warn("Reshard of project {}: reading the catalog failed (retrying next poll): {}", project, e.toString());
            return Step.FAILED;
        }
        Optional<ProjectShards.ReshardRequest> request = map.reshardRequestOf(project);
        if (request.isEmpty()) {
            releaseLease(unit);
            return Step.NONE;
        }
        ProjectShards.ReshardRequest req = request.get();
        if (!leases.isHeld(unit)) {
            Optional<Lease> lease;
            try {
                lease = leases.tryAcquire(unit);
            } catch (IOException e) {
                log.warn("Reshard of project {}: acquiring its reshard lease failed: {}", project, e.toString());
                return Step.FAILED;
            }
            if (lease.isEmpty()) {
                log.info("Reshard of project {}: another writer holds its reshard lease; not running it here", project);
                return Step.REFUSED;
            }
        }
        LeaseGuard guard = shard -> leases.assertHeld(unit);
        Job job = readJob(project).filter(j -> j.spec == req.specVersion()).orElseGet(() -> new Job(req.specVersion()));
        try {
            if (req.spec().isDefault()) {
                throw new IllegalArgumentException("a reshard to [project] is not supported; use a single-shard ring,"
                        + " [project, ring(path, shards=1)]");
            }
            OptionalInt from = map.specVersionOf(project);
            Optional<ProjectShards.Staged> staged = map.stagedOf(project)
                    .filter(s -> !s.abandoned() && s.specVersion() == req.specVersion());
            ShardPlan to;
            if (staged.isPresent()) {
                to = new ShardPlan(project, req.spec(), staged.get().shards());
            } else {
                List<ShardId> shards = indexer.shardsFor(root, project, req.spec());
                catalog.stageReshard(project, req.specVersion(), from, shards, clock.instant());
                to = new ShardPlan(project, req.spec(), shards);
                log.info("Reshard of project {}: staged spec version {} ({}) with {} shards", project,
                        req.specVersion(), req.spec(), shards.size());
            }
            for (ShardId s : to.shards()) catalog.register(s, Optional.of(source));
            job.phase = "staged";
            job.from = from.isPresent() ? from.getAsInt() : null;
            job.shards = to.shards();
            writeJob(project, job);

            Map<ShardId, Long> pins;
            if (staged.isPresent() && staged.get().pinned()) {
                pins = staged.get().generations();
            } else {
                pins = buildAndPublish(project, active, to, root, embed, abort, guard, job);
                catalog.pinStaged(project, req.specVersion(), pins, activeGenerations(project, active, map));
                job.phase = "published";
                job.generations = pins;
                job.pinnedAt = clock.instant();
                writeJob(project, job);
                log.info("Reshard of project {}: spec version {} published ({} shards rewritten, {} kept); waiting for"
                        + " the readers", project, req.specVersion(), job.affected.size(), job.unaffected.size());
            }

            Instant pinnedAt = job.pinnedAt != null ? job.pinnedAt : clock.instant();
            if (job.pinnedAt == null) {
                job.pinnedAt = pinnedAt;
                writeJob(project, job);
            }
            if (clock.instant().isBefore(pinnedAt.plus(settings.minPinned()))) return Step.WAITING;
            String waiting = readersMissing(to.shards(), pins);
            boolean timedOut = !clock.instant().isBefore(pinnedAt.plus(settings.syncTimeout()));
            if (waiting != null && !timedOut) {
                if (!waiting.equals(job.waitingFor)) {
                    job.waitingFor = waiting;
                    job.phase = "waiting";
                    writeJob(project, job);
                    log.info("Reshard of project {}: waiting for the readers before the flip ({})", project, waiting);
                }
                return Step.WAITING;
            }
            if (waiting != null) {
                log.warn("Reshard of project {}: flipping after {} s although {}; queries read the previous version"
                        + " on readers without the new one", project, settings.syncTimeout().toSeconds(), waiting);
            }
            flip(project, active, to, req.specVersion(), pins, map, guard);
            job.phase = "flipped";
            job.waitingFor = null;
            job.flippedAt = clock.instant();
            writeJob(project, job);
            releaseLease(unit);
            return Step.FLIPPED;
        } catch (PublishConflictException e) {
            log.warn("Reshard of project {} to spec version {} stopped: {}", project, req.specVersion(), e.getMessage());
            job.error = e.getMessage();
            writeJobQuietly(project, job);
            releaseLease(unit);
            return Step.FAILED;
        } catch (com.pharos.indexer.IndexAbortedException e) {
            log.info("Reshard of project {} aborted at a safe point: {}", project, e.getMessage());
            releaseLease(unit);
            return Step.FAILED;
        } catch (IOException | RuntimeException e) {
            log.warn("Reshard of project {} to spec version {} failed (retrying next poll): {}", project,
                    req.specVersion(), e.toString());
            job.error = e.toString();
            writeJobQuietly(project, job);
            releaseLease(unit);
            return Step.FAILED;
        }
    }

    /**
     * Builds {@code to} locally (unless the local index already is {@code to}'s: a resumed job), publishes
     * every shard of it that differs from what {@code LATEST} serves, and returns the generation of every
     * shard of {@code to}.
     */
    private Map<ShardId, Long> buildAndPublish(String project, ShardPlan active, ShardPlan to, Path root, boolean embed,
                                               IndexAbort abort, LeaseGuard guard, Job job) throws IOException {
        Path projectDir = lucene.getProjectIndexDir(project);
        boolean built = lucene.localLayout(project).filter(l -> l.matches(to)).isPresent();
        if (!built) {
            ProjectIndexManager.ReshardBuild b = indexer.reshard(root, project, active, to, embed,
                    ProgressListener.SILENT, abort);
            job.moved = b.movedFiles();
            job.parsed = b.parsedFiles();
            job.removed = b.removed();
            job.phase = "built";
            writeJob(project, job);
        } else {
            log.info("Reshard of project {}: the local index is already spec version {}'s; publishing it", project,
                    job.spec);
        }
        ObjectStorePublisher.ReshardReference ref = publisher.reshardReference(project, active.clusterShards(),
                to.shards(), projectDir);
        Map<ShardId, Long> gens = new LinkedHashMap<>();
        List<ShardId> affected = new ArrayList<>();
        List<ShardId> unaffected = new ArrayList<>();
        Set<ShardId> fromIds = new HashSet<>(active.clusterShards());
        for (ShardId s : to.shards()) {
            abort.checkpoint();
            Optional<Generation> latest = fromIds.contains(s) ? replicas.latest(s) : Optional.empty();
            if (latest.isPresent() && publisher.publishedAs(s, latest.get(), projectDir)) {
                gens.put(s, latest.get().value()); // untouched: N+1 references N's generation as it is
                unaffected.add(s);
                continue;
            }
            Long earlier = job.generations.get(s);
            Generation g = publisher.stageShard(s, projectDir, ref,
                    earlier == null ? Optional.empty() : Optional.of(new Generation(earlier)), guard);
            gens.put(s, g.value());
            affected.add(s);
            job.generations = new LinkedHashMap<>(job.generations);
            job.generations.put(s, g.value());
            writeJob(project, job);
        }
        job.affected = affected;
        job.unaffected = unaffected;
        try {
            // a takeover restores the project's writer state of this run (with the new layout's modules)
            writerState.backupProject(project, ref.projectGeneration(), projectDir);
        } catch (IOException | RuntimeException e) {
            log.warn("Reshard of project {}: writer-state backup failed (a takeover then runs a full index): {}",
                    project, e.toString());
        }
        return gens;
    }

    /** What the active version serves: its pins when it has some, else each shard's {@code LATEST}. */
    private Map<ShardId, Long> activeGenerations(String project, ShardPlan active, ProjectShards map) throws IOException {
        Map<ShardId, Long> out = new LinkedHashMap<>(map.pinsOf(project));
        if (!out.isEmpty()) return out;
        for (ShardId s : active.clusterShards()) {
            Optional<Generation> g = replicas.latest(s);
            if (g.isEmpty()) throw new PublishConflictException("shard " + s + " of the active version has no LATEST");
            out.put(s, g.get().value());
        }
        return out;
    }

    /** The flip CAS, then each rewritten shard's {@code LATEST} to its pinned generation. */
    private void flip(String project, ShardPlan active, ShardPlan to, int version, Map<ShardId, Long> pins,
                      ProjectShards map, LeaseGuard guard) throws IOException {
        Map<ShardId, Long> previous = activeGenerations(project, active, map);
        catalog.flipReshard(project, version, previous, clock.instant());
        log.info("Reshard of project {}: flipped to spec version {} ({}); the previous version is kept for {} min",
                project, version, to.spec(), settings.retention().toMinutes());
        publisher.finishFlip(pins, guard);
    }

    /**
     * Why the flip should wait, or null when the readers are ready: every shard of the new version has a
     * live reader serving or holding it at its pinned generation, and no live reader serves a shard of
     * it at another generation without holding the pinned one (it would lag the flip). No live reader at
     * all: nothing to wait for.
     */
    private String readersMissing(List<ShardId> shards, Map<ShardId, Long> pins) {
        List<ReaderHeartbeat> live = new ArrayList<>();
        for (ReaderHeartbeat r : readers.get()) if (!r.draining()) live.add(r);
        if (live.isEmpty()) return null;
        for (ShardId s : shards) {
            long pin = pins.get(s);
            boolean any = false;
            for (ReaderHeartbeat r : live) {
                boolean has = r.generationOf(s.value()).orElse(-1) == pin
                        || Long.valueOf(pin).equals(r.prepared().get(s.value()));
                any |= has;
                if (!has && r.statesGenerations() && r.shards() != null && r.shards().contains(s.value())) {
                    return "reader " + r.node() + " serves shard " + s + " at generation "
                            + r.generationOf(s.value()).orElse(-1) + ", not yet " + pin;
                }
            }
            if (!any) return "no reader has shard " + s + " at generation " + pin + " yet";
        }
        return null;
    }

    /**
     * GC of {@code project}'s retired and abandoned versions past the retention window: deletes the
     * objects of every shard no other version of the project names, then drops the version from the
     * catalog (which unlists those shards). Never throws.
     */
    public void collect(String project, ProjectShards map) {
        Instant now = clock.instant();
        Set<ShardId> live = new HashSet<>(map.shardsOf(project));
        map.stagedOf(project).filter(s -> !s.abandoned()).ifPresent(s -> live.addAll(s.shards()));
        try {
            Optional<ProjectShards.Previous> prev = map.previousOf(project);
            if (prev.isPresent() && !now.isBefore(prev.get().retiredAt().plus(settings.retention()))) {
                for (ShardId s : prev.get().shards()) if (!live.contains(s) && gc != null) gc.purgeShard(s);
                List<ShardId> gone = catalog.dropPrevious(project, prev.get().retiredAt());
                log.info("Reshard of project {}: retired version {} dropped after the retention window (shards removed: {})",
                        project, prev.get().spec(), gone);
            }
            prev.ifPresent(p -> live.addAll(p.shards()));
            Optional<ProjectShards.Staged> st = map.stagedOf(project).filter(ProjectShards.Staged::abandoned);
            if (st.isPresent() && !now.isBefore(st.get().abandonedAt().orElseThrow().plus(settings.retention()))) {
                for (ShardId s : st.get().shards()) if (!live.contains(s) && gc != null) gc.purgeShard(s);
                catalog.dropAbandoned(project, st.get().abandonedAt().orElseThrow());
                log.info("Reshard of project {}: cancelled staged version dropped after the retention window", project);
            }
            for (ProjectShards.Abandoned a : map.abandonedOf(project)) {
                if (now.isBefore(a.since().plus(settings.retention()))) continue;
                for (ShardId s : a.shards()) if (!live.contains(s) && gc != null) gc.purgeShard(s);
                catalog.dropAbandoned(project, a.since());
            }
        } catch (IOException | RuntimeException e) {
            log.warn("Reshard GC of project {} failed (retried next poll): {}", project, e.toString());
        }
    }

    private void releaseLease(ShardId unit) {
        for (Lease l : leases.held()) if (l.shard().equals(unit)) leases.release(l);
    }

    // ---------------------------------------------------------------------------------- job record

    /** The progress of a reshard, as {@code projects/<p>/reshard/job.json} records it. */
    static final class Job {
        final int spec;
        Integer from;
        String phase = "requested";
        String writer;
        List<ShardId> shards = List.of();
        Map<ShardId, Long> generations = new LinkedHashMap<>();
        List<ShardId> affected = List.of();
        List<ShardId> unaffected = List.of();
        List<ShardId> removed = List.of();
        Integer moved;
        Integer parsed;
        Instant pinnedAt;
        Instant flippedAt;
        String waitingFor;
        String error;

        Job(int spec) {
            this.spec = spec;
        }
    }

    private Optional<Job> readJob(String project) {
        try {
            Optional<StoredObject> o = store.get(layout.reshardJobKey(project));
            if (o.isEmpty()) return Optional.empty();
            JsonNode n = JSON.readTree(o.get().data());
            Job j = new Job(n.path("spec").asInt(-1));
            j.from = n.hasNonNull("from") ? n.get("from").asInt() : null;
            j.phase = n.path("phase").asText("requested");
            j.shards = ids(n.path("shards"));
            Map<ShardId, Long> gens = new LinkedHashMap<>();
            n.path("generations").fields().forEachRemaining(e -> gens.put(new ShardId(e.getKey()), e.getValue().asLong()));
            j.generations = gens;
            j.affected = ids(n.path("affected"));
            j.unaffected = ids(n.path("unaffected"));
            j.removed = ids(n.path("removed"));
            j.moved = n.hasNonNull("movedFiles") ? n.get("movedFiles").asInt() : null;
            j.parsed = n.hasNonNull("parsedFiles") ? n.get("parsedFiles").asInt() : null;
            j.pinnedAt = n.hasNonNull("pinnedAt") ? Instant.parse(n.get("pinnedAt").asText()) : null;
            j.flippedAt = n.hasNonNull("flippedAt") ? Instant.parse(n.get("flippedAt").asText()) : null;
            j.waitingFor = n.hasNonNull("waitingFor") ? n.get("waitingFor").asText() : null;
            return Optional.of(j);
        } catch (IOException | RuntimeException e) {
            log.warn("Reshard of project {}: unreadable job record, starting it over: {}", project, e.toString());
            return Optional.empty();
        }
    }

    private static List<ShardId> ids(JsonNode arr) {
        List<ShardId> out = new ArrayList<>();
        for (JsonNode s : arr) out.add(new ShardId(s.asText()));
        return out;
    }

    private void writeJob(String project, Job j) throws IOException {
        j.writer = node;
        ObjectNode n = JSON.createObjectNode();
        n.put("spec", j.spec);
        if (j.from != null) n.put("from", j.from);
        n.put("phase", j.phase);
        n.put("writer", j.writer);
        var shards = n.putArray("shards");
        j.shards.forEach(s -> shards.add(s.value()));
        ObjectNode gens = n.putObject("generations");
        j.generations.forEach((s, g) -> gens.put(s.value(), g));
        var a = n.putArray("affected");
        j.affected.forEach(s -> a.add(s.value()));
        var u = n.putArray("unaffected");
        j.unaffected.forEach(s -> u.add(s.value()));
        var r = n.putArray("removed");
        j.removed.forEach(s -> r.add(s.value()));
        if (j.moved != null) n.put("movedFiles", j.moved);
        if (j.parsed != null) n.put("parsedFiles", j.parsed);
        if (j.pinnedAt != null) n.put("pinnedAt", j.pinnedAt.toString());
        if (j.flippedAt != null) n.put("flippedAt", j.flippedAt.toString());
        if (j.waitingFor != null) n.put("waitingFor", j.waitingFor);
        if (j.error != null) n.put("error", j.error);
        n.put("updatedAt", clock.instant().toString());
        store.put(layout.reshardJobKey(project), JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(n));
    }

    private void writeJobQuietly(String project, Job j) {
        try {
            writeJob(project, j);
        } catch (IOException | RuntimeException e) {
            log.debug("writing the reshard job record of {} failed", project, e);
        }
    }

    /**
     * The status of {@code project}'s resharding for {@code pharos reshard --status}: the catalog's view
     * (active, staged, previous, request) and the job record, as text lines.
     */
    public static List<String> status(ObjectStore store, ObjectStoreLayout layout, ProjectShards map, String project)
            throws IOException {
        List<String> out = new ArrayList<>();
        out.add("project " + project + ": spec version " + (map.specVersionOf(project).isPresent()
                ? String.valueOf(map.specVersionOf(project).getAsInt()) : "-") + " " + map.specOf(project)
                + ", " + map.shardsOf(project).size() + " shard(s)" + (map.pinsOf(project).isEmpty() ? ""
                : ", pinned generations " + new java.util.TreeMap<>(names(map.pinsOf(project)))));
        map.reshardRequestOf(project).ifPresent(r -> out.add("  requested: spec version " + r.specVersion() + " "
                + r.spec() + " at " + r.requestedAt()));
        map.stagedOf(project).ifPresent(s -> out.add("  staged: spec version " + s.specVersion() + " " + s.spec()
                + ", shards " + s.shards() + (s.abandoned() ? ", cancelled at " + s.abandonedAt().get()
                : s.pinned() ? ", published " + new java.util.TreeMap<>(names(s.generations())) : ", building")));
        map.previousOf(project).ifPresent(p -> out.add("  previous: " + p.spec() + ", shards " + p.shards()
                + ", retired at " + p.retiredAt()));
        for (ProjectShards.Abandoned a : map.abandonedOf(project)) {
            out.add("  abandoned: shards " + a.shards() + " since " + a.since());
        }
        Optional<StoredObject> job = store.get(layout.reshardJobKey(project));
        if (job.isPresent()) {
            JsonNode n = JSON.readTree(job.get().data());
            StringBuilder sb = new StringBuilder("  job: spec version " + n.path("spec").asInt() + ", "
                    + n.path("phase").asText() + " on " + n.path("writer").asText() + " (updated "
                    + n.path("updatedAt").asText() + ")");
            if (n.hasNonNull("movedFiles")) sb.append(", ").append(n.get("movedFiles").asInt()).append(" file(s) moved");
            if (n.path("affected").size() > 0 || n.path("unaffected").size() > 0) {
                sb.append(", shards rewritten ").append(n.path("affected")).append(", kept ").append(n.path("unaffected"));
            }
            if (n.hasNonNull("waitingFor")) sb.append(", waiting: ").append(n.get("waitingFor").asText());
            if (n.hasNonNull("error")) sb.append(", last error: ").append(n.get("error").asText());
            out.add(sb.toString());
        }
        return out;
    }

    private static Map<String, Long> names(Map<ShardId, Long> m) {
        Map<String, Long> out = new LinkedHashMap<>();
        m.forEach((k, v) -> out.put(k.value(), v));
        return out;
    }
}
