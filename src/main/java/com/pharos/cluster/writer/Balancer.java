package com.pharos.cluster.writer;

import com.pharos.cluster.ShardId;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Fair-share rules of the work queue, as pure functions (the work loop supplies the state).
 *
 * <p>The unit of work is a project (ruling P6-R1): the work loop passes one {@link ShardId} per
 * project ({@code ShardId.ofProject}), so a project split into N shards counts once in the share and in
 * a writer's held count, and is acquired and released as a whole (all N leases or none). For a
 * {@code [project]} project the unit is its one shard, as in Phase 4.
 *
 * <ul>
 *   <li><b>Share</b> = {@code ceil(workable shards / live writers)}. Workable shards are those with a
 *       {@code source} plus the source-less shards some live writer pins; live writers are those
 *       whose heartbeat {@link WriterHeartbeat#takesShare() takes a share}.</li>
 *   <li><b>Acquire</b> free or expired leases while holding fewer than the share. Candidates are
 *       tried in an order that differs per writer (a hash of node and shard), so writers starting
 *       together don't all race for the same lease first.</li>
 *   <li><b>Release</b> a surplus lease only at a clean point of that shard (right after its publish,
 *       or when a poll finds nothing unpublished; never while a run of it is in progress), only when
 *       it was held at least {@code minHold}, and never a pinned one. With {@code minHold} this bounds
 *       how often a lease can change hands while writers join and leave (Review Focus 4).</li>
 * </ul>
 */
public final class Balancer {

    /** {@code cluster.writer.minHoldSeconds} default. */
    public static final Duration DEFAULT_MIN_HOLD = Duration.ofSeconds(120);

    private final Duration minHold;

    public Balancer(Duration minHold) {
        this.minHold = Objects.requireNonNull(minHold, "minHold");
        if (minHold.isNegative()) throw new IllegalArgumentException("minHold must not be negative: " + minHold);
    }

    public Duration minHold() { return minHold; }

    /** {@code ceil(workable / liveWriters)}; with no live writer counted, all of them. */
    public static int share(int workable, int liveWriters) {
        if (workable <= 0) return 0;
        if (liveWriters <= 0) return workable;
        return (workable + liveWriters - 1) / liveWriters;
    }

    /** How many more leases to acquire. */
    public static int wanted(int share, int held) {
        return Math.max(0, share - held);
    }

    /** The candidates (acquirable, not held) in this writer's own try order. */
    public static List<ShardId> acquireOrder(String node, Collection<ShardId> acquirable, Set<ShardId> held) {
        return acquirable.stream().filter(s -> !held.contains(s))
                .sorted(Comparator.comparing((ShardId s) -> rank(node, s)).thenComparing(ShardId::value))
                .toList();
    }

    /**
     * True when the lease of {@code shard} may be released now, at a clean point of that shard.
     *
     * @param heldCount  leases this writer holds (pinned ones included)
     * @param acquiredAt when this writer acquired it (its own clock)
     */
    public boolean mayRelease(int heldCount, int share, boolean pinned, Instant acquiredAt, Instant now) {
        if (pinned || heldCount <= share) return false;
        return acquiredAt == null || !Duration.between(acquiredAt, now).minus(minHold).isNegative();
    }

    private static String rank(String node, ShardId shard) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256")
                    .digest((node + "\n" + shard.value()).getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(d, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
