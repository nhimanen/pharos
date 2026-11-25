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
 */
@JsonPropertyOrder({"mode", "asOf", "layout", "backlogSummary", "health", "proposals"})
public record ClusterStatus(String mode, String asOf, Layout layout, BacklogSummary backlogSummary,
                            List<Health> health, List<Proposal> proposals) {

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
                backlogSummary, health, proposals);
    }

    /** @param summary the scale: shard, live writer and live reader counts */
    @JsonPropertyOrder({"summary", "shards", "writers", "readers"})
    public record Layout(StatusState.Summary summary, List<Shard> shards, List<Writer> writers,
                         List<Reader> readers) {}

    /**
     * One shard. {@code specVersion} and {@code keys} describe the shard spec: every shard is keyed
     * by project under spec version 1 until custom shard keys exist (Phase 6). {@code docCount},
     * {@code sizeBytes}, {@code generation} and {@code writer} come from the {@code LATEST}
     * manifest ({@code writer} is its {@code writerNode}); {@code ageSeconds} is the age of
     * {@code LATEST}. {@code lease} is the lease object ({@code null} without one), {@code readers}
     * the live readers serving the shard, {@code backlog} what the lease holder's heartbeat reports
     * (with the bucket-time age) or {@code null}, and {@code state} one of {@code current},
     * {@code lagging} (a change is unpublished), {@code lease-expired}, {@code unpublished}.
     */
    @JsonPropertyOrder({"shard", "specVersion", "keys", "docCount", "sizeBytes", "generation", "ageSeconds",
            "writer", "lease", "readers", "backlog", "state"})
    public record Shard(String shard, int specVersion, Map<String, String> keys, Long docCount, Long sizeBytes,
                        Long generation, Long ageSeconds, String writer, Lease lease, List<String> readers,
                        Backlog backlog, String state) {}

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
     * condition started to hold (bucket time, ISO-8601); {@code metrics} are the snapshot's numbers.
     */
    @JsonPropertyOrder({"id", "kind", "target", "reason", "metrics", "action", "since"})
    public record Proposal(String id, String kind, String target, String reason, Map<String, Object> metrics,
                           String action, String since) {}
}
