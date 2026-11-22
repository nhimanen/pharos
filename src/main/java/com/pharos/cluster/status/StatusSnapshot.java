package com.pharos.cluster.status;

import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.cluster.writer.WriterHeartbeat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Everything {@code /cluster/status} is built from, as read from the bucket (or from local state on a
 * single node): the catalog's shards with their {@code LATEST} manifest and lease, and the live
 * writer and reader heartbeats with their bucket LastModified. Nothing in it is node-local, so two
 * nodes that read the same objects hold equal snapshots.
 *
 * <p>{@code asOf} is the bucket's "now" of the snapshot: the newest LastModified among the objects
 * read (heartbeats, leases, {@code LATEST}s), truncated to seconds. Every age in the status is
 * measured against it, never against a node clock.
 *
 * @param mode      {@value #CLUSTER} or {@value #SINGLE_NODE}
 * @param leaseTtl  the writer lease TTL ({@code cluster.writer.leaseTtlSeconds}) expiry is judged by
 * @param nodeClock {@code asOf} is this node's clock, not the bucket's (a bucket with no timestamped
 *                  object at all): such a snapshot never writes the shared state, since a skewed
 *                  node clock stamped into it would stall every other node's updates
 * @param projects  the catalog's projects (Phase 6): spec versions, staged and retired versions, and a
 *                  reshard's job record and lease. Empty: every shard is its own {@code [project]}
 *                  project ({@link #projectsOrIdentity}), as before Phase 6 and on a single node
 * @param specs     the catalog's spec versions, in catalog order
 * @param activeVersion the active cluster-default spec version; null means {@code [project]}
 */
public record StatusSnapshot(String mode, Instant asOf, List<ShardObs> shards, List<WriterObs> writers,
                             List<ReaderObs> readers, Duration leaseTtl, boolean nodeClock,
                             List<ProjectObs> projects, List<SpecObs> specs, Integer activeVersion) {

    public static final String CLUSTER = "cluster";
    public static final String SINGLE_NODE = "single-node";

    public StatusSnapshot {
        Objects.requireNonNull(mode, "mode");
        asOf = Objects.requireNonNull(asOf, "asOf").truncatedTo(ChronoUnit.SECONDS);
        shards = shards == null ? List.of()
                : shards.stream().sorted(Comparator.comparing(ShardObs::shard)).toList();
        writers = writers == null ? List.of()
                : writers.stream().sorted(Comparator.comparing((WriterObs w) -> w.heartbeat().node())).toList();
        readers = readers == null ? List.of()
                : readers.stream().sorted(Comparator.comparing((ReaderObs r) -> r.heartbeat().node())).toList();
        leaseTtl = leaseTtl == null ? Duration.ofSeconds(30) : leaseTtl;
        projects = projects == null ? List.of()
                : projects.stream().sorted(Comparator.comparing(ProjectObs::project)).toList();
        specs = specs == null ? List.of() : List.copyOf(specs);
    }

    /** A snapshot without catalog projects: every shard is its own {@code [project]} project. */
    public StatusSnapshot(String mode, Instant asOf, List<ShardObs> shards, List<WriterObs> writers,
                          List<ReaderObs> readers, Duration leaseTtl, boolean nodeClock) {
        this(mode, asOf, shards, writers, readers, leaseTtl, nodeClock, null, null, null);
    }

    /** A snapshot dated by the bucket's clock (single node: by its own, the only clock there is). */
    public StatusSnapshot(String mode, Instant asOf, List<ShardObs> shards, List<WriterObs> writers,
                          List<ReaderObs> readers, Duration leaseTtl) {
        this(mode, asOf, shards, writers, readers, leaseTtl, false);
    }

    /**
     * One catalog shard.
     *
     * @param hasSource    the catalog entry names a {@code source} (any writer can index it)
     * @param generation   the generation {@code LATEST} points at; null when none is published
     * @param publishedAt  the bucket LastModified of {@code LATEST} (single node: the last index run)
     * @param publishedRevision the source revision the published generation holds; null when unknown
     * @param lease        the shard's lease object; null when there is none
     */
    public record ShardObs(String shard, boolean hasSource, Long generation, Long docCount, Long sizeBytes,
                           String writerNode, Instant publishedAt, String publishedRevision, LeaseObs lease) {}

    /**
     * One catalog project ({@code ProjectShards}): its active spec version ({@code null}: the implicit
     * {@code [project]}), spec and shards; the staged version of a reshard in progress, the version a flip
     * retired, the open request, versions waiting for GC; and, while a reshard is visible, its job record
     * ({@code projects/<p>/reshard/job.json}) and its reshard lease.
     */
    public record ProjectObs(String project, Integer specVersion, ShardSpec spec, List<String> shards,
                             StagedObs staged, PreviousObs previous, RequestObs request,
                             List<AbandonedObs> abandoned, JobObs job, LeaseObs reshardLease) {
        public ProjectObs {
            Objects.requireNonNull(project, "project");
            spec = spec == null ? ShardSpec.PROJECT : spec;
            shards = shards == null ? List.of() : List.copyOf(shards);
            abandoned = abandoned == null ? List.of() : List.copyOf(abandoned);
        }

        /** A project on the implicit {@code [project]} spec with its one shard, and nothing else. */
        public static ProjectObs identity(String shard) {
            return new ProjectObs(shard, null, ShardSpec.PROJECT, List.of(shard), null, null, null, null, null, null);
        }
    }

    /**
     * A staged version: spec version, spec, shards, the generation of each once pinned (empty before),
     * and when it was cancelled (null while it is not).
     */
    public record StagedObs(int specVersion, ShardSpec spec, List<String> shards, Map<String, Long> generations,
                            Instant abandonedAt) {
        public StagedObs {
            shards = shards == null ? List.of() : List.copyOf(shards);
            generations = generations == null ? Map.of() : Map.copyOf(generations);
        }
    }

    /** The version a flip retired, kept for the retention window ({@code specVersion} null: {@code [project]}). */
    public record PreviousObs(Integer specVersion, ShardSpec spec, List<String> shards, Instant retiredAt) {
        public PreviousObs {
            shards = shards == null ? List.of() : List.copyOf(shards);
        }
    }

    /** An open reshard request ({@code pharos reshard}). */
    public record RequestObs(int specVersion, ShardSpec spec, Instant requestedAt) {}

    /** A staged or retired version waiting for GC. */
    public record AbandonedObs(Integer specVersion, List<String> shards) {
        public AbandonedObs {
            shards = shards == null ? List.of() : List.copyOf(shards);
        }
    }

    /**
     * A reshard's job record: the spec version it builds, its phase as the writer records it, the writer,
     * the shards settled so far (rewritten and kept), the files the build moves and how many are written,
     * what the flip waits for, the last error, and the record's bucket LastModified.
     */
    public record JobObs(int specVersion, String phase, String writer, int shardsRewritten, int shardsKept,
                         Integer filesDone, Integer filesTotal, String waitingFor, String error,
                         Instant lastModified) {}

    /** A catalog spec version: its number, its project (null: a cluster default) and its spec. */
    public record SpecObs(int version, String project, ShardSpec spec) {}

    /** {@link #projects}, or, when the snapshot has none, every shard as its own {@code [project]} project. */
    public List<ProjectObs> projectsOrIdentity() {
        if (!projects.isEmpty()) return projects;
        return shards.stream().map(sh -> ProjectObs.identity(sh.shard())).toList();
    }

    /** A lease object: who holds it, its bucket LastModified, and whether it was released. */
    public record LeaseObs(String writer, Instant lastModified, boolean released) {}

    /** A live writer heartbeat and its bucket LastModified. */
    public record WriterObs(WriterHeartbeat heartbeat, Instant lastModified) {}

    /** A live reader heartbeat and its bucket LastModified. */
    public record ReaderObs(ReaderHeartbeat heartbeat, Instant lastModified) {}

    /** Whole seconds from {@code t} to {@link #asOf} (never negative); null when {@code t} is. */
    public Long ageSeconds(Instant t) {
        if (t == null) return null;
        return Math.max(0, Duration.between(t.truncatedTo(ChronoUnit.SECONDS), asOf).toSeconds());
    }
}
