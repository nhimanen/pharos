package com.pharos.cluster.writer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * One work-queue writer's announcement in {@code catalog/writers/<node>.json}, rewritten every
 * {@code cluster.heartbeatSeconds} and on every lease change. Its age (by the bucket's clock, see
 * {@link WriterRegistry}) is its liveness; the age itself is not stored.
 *
 * <ul>
 *   <li>{@code leases}: the shards whose lease this writer holds, sorted (pinned ones included);</li>
 *   <li>{@code pinned}: the held shards without a {@code source} that only this writer can index
 *       (it has their path); other writers never take them, and count them in this writer's share;</li>
 *   <li>{@code backlog}: per held shard, the revision at its source, the revision {@code LATEST}
 *       holds, whether a change is not yet published, and since when (seconds, by this writer's
 *       clock; null when nothing is pending);</li>
 *   <li>{@code indexing}: the run in progress ({@code phase} parse|embed|commit|publish,
 *       {@code progress} 0..1 within the phase, {@code startedAt} epoch millis), or null;</li>
 *   <li>{@code startedAt}: the process start (epoch millis, informational: a node clock);</li>
 *   <li>{@code stopping}: a graceful stop is under way; the balancer no longer counts this writer.</li>
 * </ul>
 * Unknown fields are ignored on read, so later fields (the disk guard's {@code disk}) need no
 * version bump.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"node", "workQueue", "leases", "pinned", "backlog", "indexing", "startedAt", "stopping"})
public record WriterHeartbeat(String node, boolean workQueue, List<String> leases, List<String> pinned,
                              Map<String, Backlog> backlog, Indexing indexing, long startedAt, boolean stopping) {

    public static final String PARSE = "parse";
    public static final String EMBED = "embed";
    public static final String COMMIT = "commit";
    public static final String PUBLISH = "publish";

    public WriterHeartbeat {
        leases = leases == null ? List.of() : List.copyOf(leases);
        pinned = pinned == null ? List.of() : List.copyOf(pinned);
        backlog = backlog == null ? Map.of() : java.util.Collections.unmodifiableMap(new TreeMap<>(backlog));
    }

    /** True when this writer counts towards the fair share. Task 4b adds {@code && !disk.low}. */
    public boolean takesShare() {
        return workQueue && !stopping;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"sourceRevision", "publishedRevision", "pending", "oldestUnpublishedChangeAgeSeconds"})
    public record Backlog(String sourceRevision, String publishedRevision, boolean pending,
                          Long oldestUnpublishedChangeAgeSeconds) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"shard", "phase", "progress", "startedAt"})
    public record Indexing(String shard, String phase, double progress, long startedAt) {}
}
