package com.pharos.cluster.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.pharos.cluster.writer.WriterHeartbeat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The {@code GET /cluster/status} answer. Every list is sorted and every map is written in key order
 * ({@link #MAPPER}), and nothing in it is node-local, so two nodes that built it from the same
 * snapshot and state answer byte-identical JSON.
 *
 * @param mode           {@code cluster} (built from the bucket) or {@code single-node} ({@code role: all})
 * @param asOf           the bucket time of the snapshot (ISO-8601, seconds); every age is measured to it
 * @param backlogSummary {@code {shardsPending, oldestPendingAgeSeconds}}
 * @param projects       the catalog's projects (a split project is one entry over its shards), with
 *                       their staged and retired spec versions and a reshard's progress (Phase 6)
 * @param specs          every spec version of the catalog and its state
 */
@JsonPropertyOrder({"mode", "asOf", "layout", "backlogSummary", "health", "proposals", "projects", "specs"})
public record ClusterStatus(String mode, String asOf, Layout layout, BacklogSummary backlogSummary,
                            List<Health> health, List<Proposal> proposals, List<Project> projects,
                            List<SpecVersion> specs) {

    public ClusterStatus {
        projects = projects == null ? List.of() : List.copyOf(projects);
        specs = specs == null ? List.of() : List.copyOf(specs);
    }

    /** A status without the Phase 6 project and spec views ({@code projects} and {@code specs} empty). */
    public ClusterStatus(String mode, String asOf, Layout layout, BacklogSummary backlogSummary, List<Health> health,
                         List<Proposal> proposals) {
        this(mode, asOf, layout, backlogSummary, health, proposals, List.of(), List.of());
    }

    /**
     * Deterministic JSON: map entries by key, records in their declared property order, no
     * pretty-printing, unknown fields ignored on read (a newer node's state stays readable).
     */
    public static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public byte[] toJson() throws IOException {
        return MAPPER.writeValueAsBytes(this);
    }

    /**
     * The public view (served without the cluster token): every reader's {@code advertiseUrl} left
     * out, since the internal topology is for cluster members only (as on {@code /cluster/readers}).
     */
    public ClusterStatus withoutAddresses() {
        List<Reader> rs = layout.readers().stream().map(r -> new Reader(r.node(), null, r.shards(), r.state(),
                r.degradedShards(), r.readerCache(), r.inFlight(), r.qps(), r.startedAt(), r.warmP95Millis(),
                r.catalogAgeSeconds(), r.heartbeatAgeSeconds())).toList();
        return new ClusterStatus(mode, asOf, new Layout(layout.summary(), layout.shards(), layout.writers(), rs),
                backlogSummary, health, proposals, projects, specs);
    }

    /** @param summary the scale: shard, live writer and live reader counts */
    @JsonPropertyOrder({"summary", "shards", "writers", "readers"})
    public record Layout(StatusState.Summary summary, List<Shard> shards, List<Writer> writers,
                         List<Reader> readers) {}

    /**
     * One shard. {@code specVersion} and {@code keys} describe the shard spec: the spec version of the
     * project version the shard belongs to (1 for the implicit {@code [project]}) and its key values,
     * {@code project} first. {@code docCount},
     * {@code sizeBytes}, {@code generation} and {@code writer} come from the {@code LATEST}
     * manifest ({@code writer} is its {@code writerNode}); {@code ageSeconds} is the age of
     * {@code LATEST}. {@code lease} is the lease object ({@code null} without one), {@code readers}
     * the live readers serving the shard, {@code backlog} what the lease holder's heartbeat reports
     * (with the bucket-time age) or {@code null}, and {@code state} one of {@code current},
     * {@code lagging} (a change is unpublished), {@code lease-expired}, {@code unpublished}, or
     * {@code staged} (a shard only a reshard's staged version has, not yet routed to).
     *
     * <p>Phase 6: {@code project} is the project the shard serves (null for an orphan); {@code role}
     * which of its versions has it: {@code active}, {@code staged} (only the staged version of a reshard
     * in progress), {@code previous} (only the version a flip retired, kept for the retention window),
     * {@code abandoned} (waiting for GC) or {@code orphan}. {@code staged} is true when the staged
     * version names the shard (also a shard both versions share), and {@code stagedGeneration} is the
     * generation that version pins for it once published.
     */
    @JsonPropertyOrder({"shard", "specVersion", "keys", "docCount", "sizeBytes", "generation", "ageSeconds",
            "writer", "lease", "readers", "backlog", "state", "project", "role", "staged", "stagedGeneration"})
    public record Shard(String shard, int specVersion, Map<String, String> keys, Long docCount, Long sizeBytes,
                        Long generation, Long ageSeconds, String writer, Lease lease, List<String> readers,
                        Backlog backlog, String state, String project, String role, boolean staged,
                        Long stagedGeneration) {

        /**
         * A shard as a status before Phase 6 described it: an active shard of the project its
         * {@code project} key names, not staged.
         */
        public Shard(String shard, int specVersion, Map<String, String> keys, Long docCount, Long sizeBytes,
                     Long generation, Long ageSeconds, String writer, Lease lease, List<String> readers,
                     Backlog backlog, String state) {
            this(shard, specVersion, keys, docCount, sizeBytes, generation, ageSeconds, writer, lease, readers, backlog,
                    state, keys == null ? null : keys.get("project"), "active", false, null);
        }
    }

    /**
     * One catalog project: its active spec version ({@code null}: the implicit {@code [project]}), spec and
     * shards, the docs and bytes of those shards, the staged version of a reshard ({@code null} without
     * one), the version a flip retired while it is kept ({@code null} otherwise), and the reshard's
     * progress ({@code null} when none is visible).
     *
     * <p>Ruling P6-R15, the per-project view of the active shards only (never a staged, retired or
     * abandoned one, so a reshard in progress doesn't change it): {@code published} when every active
     * shard has a generation, {@code generation} the lowest of their generations (null when none has
     * one), {@code docCount} their sum, and {@code pending} when any of them has an unpublished change.
     */
    @JsonPropertyOrder({"project", "specVersion", "spec", "shards", "docCount", "sizeBytes", "staged", "previous",
            "reshard", "published", "generation", "pending"})
    public record Project(String project, Integer specVersion, String spec, List<String> shards, Long docCount,
                          Long sizeBytes, StagedVersion staged, PreviousVersion previous, Reshard reshard,
                          boolean published, Long generation, boolean pending) {}

    /** A staged spec version: its shards and {@code state} {@code building}, {@code pinned} or {@code abandoned}. */
    @JsonPropertyOrder({"specVersion", "spec", "shards", "state"})
    public record StagedVersion(int specVersion, String spec, List<String> shards, String state) {}

    /** The version a flip retired ({@code specVersion} null: {@code [project]}), and when (writer time, ISO-8601). */
    @JsonPropertyOrder({"specVersion", "spec", "shards", "retiredAt"})
    public record PreviousVersion(Integer specVersion, String spec, List<String> shards, String retiredAt) {}

    /**
     * A reshard of a project from spec version {@code from} ({@code null}: {@code [project]}) to
     * {@code specVersion}:
     * <ul>
     *   <li>{@code phase}: {@code requested} (asked for, not staged yet), {@code building} (staged, its
     *       shards being built and published), {@code pinned} (published; the flip waits for the readers),
     *       {@code flipped} (routing uses the new version; the old one is kept until retention ends) or
     *       {@code abandoned} (cancelled);</li>
     *   <li>progress in shards ({@code shardsDone} of {@code shardsTotal}: rewritten or kept as they are,
     *       {@code shardsRewritten} and {@code shardsKept}) and in files ({@code filesDone} of
     *       {@code filesTotal}: the files the build moves to another shard); a count not known yet is null;</li>
     *   <li>{@code holder}: the writer holding the reshard lease (null when none holds it), {@code lease}
     *       that lease object;</li>
     *   <li>{@code jobPhase}, {@code jobWriter}, {@code jobAgeSeconds}: the job record's phase, writer and
     *       age; {@code waitingFor} what the flip waits for; {@code error} the last failure.</li>
     * </ul>
     */
    @JsonPropertyOrder({"specVersion", "spec", "from", "phase", "requestedAt", "shardsDone", "shardsTotal",
            "shardsRewritten", "shardsKept", "filesDone", "filesTotal", "holder", "lease", "jobPhase", "jobWriter",
            "jobAgeSeconds", "waitingFor", "error"})
    public record Reshard(int specVersion, String spec, Integer from, String phase, String requestedAt,
                          Integer shardsDone, Integer shardsTotal, Integer shardsRewritten, Integer shardsKept,
                          Integer filesDone, Integer filesTotal, String holder, Lease lease, String jobPhase,
                          String jobWriter, Long jobAgeSeconds, String waitingFor, String error) {}

    /**
     * A catalog spec version: {@code project} (null: a cluster default), spec, and {@code state}:
     * {@code active}, {@code staged}, {@code requested}, {@code retired} (a flip's previous version, kept
     * for the retention window), {@code abandoned} (waiting for GC) or {@code superseded}.
     */
    @JsonPropertyOrder({"version", "project", "spec", "state"})
    public record SpecVersion(int version, String project, String spec, String state) {}

    /** A shard's lease: holder, age of its last write, expired (by the bucket's clock), released. */
    @JsonPropertyOrder({"writer", "ageSeconds", "expired", "released"})
    public record Lease(String writer, long ageSeconds, boolean expired, boolean released) {}

    /** The writer heartbeat's backlog fields, with {@code oldestUnpublishedChangeAgeSeconds} in bucket time. */
    @JsonPropertyOrder({"sourceRevision", "publishedRevision", "pending", "oldestUnpublishedChangeAgeSeconds"})
    public record Backlog(String sourceRevision, String publishedRevision, boolean pending,
                          Long oldestUnpublishedChangeAgeSeconds) {}

    /** The writer heartbeat's fields (its per-shard {@code backlog} lives on the shards), plus its age. */
    @JsonPropertyOrder({"node", "workQueue", "leases", "pinned", "indexing", "startedAt", "stopping", "disk",
            "heartbeatAgeSeconds"})
    public record Writer(String node, boolean workQueue, List<String> leases, List<String> pinned,
                         WriterHeartbeat.Indexing indexing, long startedAt, boolean stopping,
                         WriterHeartbeat.Disk disk, long heartbeatAgeSeconds) {}

    /**
     * The reader heartbeat's fields by their names, plus its age. {@code advertiseUrl} is present only
     * in the token view ({@link #withoutAddresses}).
     */
    @JsonPropertyOrder({"node", "advertiseUrl", "shards", "state", "degradedShards", "readerCache", "inFlight",
            "qps", "startedAt", "warmP95Millis", "catalogAgeSeconds", "heartbeatAgeSeconds"})
    public record Reader(String node, @JsonInclude(JsonInclude.Include.NON_NULL) String advertiseUrl,
                         List<String> shards, String state,
                         List<String> degradedShards, Map<String, Object> readerCache, int inFlight, double qps,
                         long startedAt, Long warmP95Millis, Long catalogAgeSeconds, long heartbeatAgeSeconds) {}

    @JsonPropertyOrder({"shardsPending", "oldestPendingAgeSeconds"})
    public record BacklogSummary(int shardsPending, Long oldestPendingAgeSeconds) {}

    /** A health finding: {@code kind} (e.g. {@code writer-low-disk}), its target and a message. */
    @JsonPropertyOrder({"kind", "target", "message"})
    public record Health(String kind, String target, String message) {}

    /**
     * A proposal in force. {@code id} = hex SHA-1 of {@code kind + target}; {@code since} is when its
     * condition started to hold (bucket time, ISO-8601); {@code metrics} are the snapshot's numbers;
     * {@code delta} is the signed node count of a scaling proposal ({@code +N} start, {@code -N}
     * stop, the N its {@code action} names), absent on {@code reshard} and {@code writer-disk}.
     */
    @JsonPropertyOrder({"id", "kind", "target", "reason", "metrics", "action", "delta", "since"})
    public record Proposal(String id, String kind, String target, String reason, Map<String, Object> metrics,
                           String action, @JsonInclude(JsonInclude.Include.NON_NULL) Integer delta, String since) {}
}
