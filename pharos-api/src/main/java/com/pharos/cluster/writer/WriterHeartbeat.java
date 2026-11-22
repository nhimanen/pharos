package com.pharos.cluster.writer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * One work-queue writer's announcement in {@code catalog/writers/<node>.json}, rewritten every
 * {@code cluster.heartbeatSeconds} and on every lease change. Its age (by the bucket's clock, see
 * {@code WriterRegistry}) is its liveness; the age itself is not stored.
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
 *   <li>{@code stopping}: a graceful stop is under way; the balancer no longer counts this writer;</li>
 *   <li>{@code disk}: the disk guard's view ({@code DiskGuard}): free and total bytes of the most
 *       constrained watched file store, and whether this writer is low on disk. A low writer
 *       acquires nothing and the balancer no longer counts it. Absent when no guard runs (a heartbeat
 *       written before Task 4b reads as "not low").</li>
 * </ul>
 * Unknown fields are ignored on read, so later fields need no version bump.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"node", "workQueue", "leases", "pinned", "backlog", "indexing", "startedAt", "stopping", "disk"})
public record WriterHeartbeat(String node, boolean workQueue, List<String> leases, List<String> pinned,
                              Map<String, Backlog> backlog, Indexing indexing, long startedAt, boolean stopping,
                              @JsonInclude(JsonInclude.Include.NON_NULL) Disk disk) {

    public static final String PARSE = "parse";
    public static final String EMBED = "embed";
    public static final String COMMIT = "commit";
    public static final String PUBLISH = "publish";

    public WriterHeartbeat {
        leases = leases == null ? List.of() : List.copyOf(leases);
        pinned = pinned == null ? List.of() : List.copyOf(pinned);
        backlog = backlog == null ? Map.of() : java.util.Collections.unmodifiableMap(new TreeMap<>(backlog));
    }

    /** Without the disk guard's field (no guard, or a heartbeat from before it). */
    public WriterHeartbeat(String node, boolean workQueue, List<String> leases, List<String> pinned,
                           Map<String, Backlog> backlog, Indexing indexing, long startedAt, boolean stopping) {
        this(node, workQueue, leases, pinned, backlog, indexing, startedAt, stopping, null);
    }

    /** True when this writer counts towards the fair share: work queue, not stopping, not low on disk. */
    public boolean takesShare() {
        return workQueue && !stopping && !lowOnDisk();
    }

    /** True when the heartbeat says this writer is low on disk. */
    public boolean lowOnDisk() {
        return disk != null && disk.low();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"sourceRevision", "publishedRevision", "pending", "oldestUnpublishedChangeAgeSeconds"})
    public record Backlog(String sourceRevision, String publishedRevision, boolean pending,
                          Long oldestUnpublishedChangeAgeSeconds) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"shard", "phase", "progress", "startedAt"})
    public record Indexing(String shard, String phase, double progress, long startedAt) {}

    /** {@code disk}: free and total bytes of the most constrained watched file store; {@code low}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"freeBytes", "totalBytes", "low"})
    public record Disk(long freeBytes, long totalBytes, boolean low) {}
}
