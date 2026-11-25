package com.pharos.cluster.membership;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The live readers of the cluster, as coordinators ({@code ShardRouter}), {@code pharos router} and
 * {@code /health}'s reader list consume them. Two implementations:
 * <ul>
 *   <li>{@link ReaderRegistry}: heartbeat objects in the bucket ({@code catalog/readers/}), liveness by
 *       the bucket's clock;</li>
 *   <li>{@code PeerMembership} (peer-pull, no bucket): heartbeats POSTed to every writer, each writer
 *       judging liveness by its own clock, merged across writers (a reader any live writer holds is
 *       live, with its newest heartbeat).</li>
 * </ul>
 * Both use the same liveness window ({@code cluster.readerTtlSeconds}), list this node's own entry
 * when it is a member, and keep the previous list when they cannot tell (their own write failed,
 * or no writer answered).
 */
public interface MembershipSource extends Closeable {

    /** The live readers as of the last refresh, sorted by node id, each with its age in seconds. */
    List<ReaderRegistry.LiveReader> liveWithAge();

    /** {@link #liveWithAge()} without the ages. */
    default List<ReaderHeartbeat> live() {
        return liveWithAge().stream().map(ReaderRegistry.LiveReader::heartbeat).toList();
    }

    /** Starts heartbeating (a member) or listing (an observer) in the background. Idempotent. */
    default void start() {}

    /** Announces this reader draining at once (members only). */
    default void drain() {}

    /**
     * {@link #drain()}, waiting for the announce at most {@code timeout} (a shutdown must not hang on
     * a slow bucket or writer); the draining flag is set first, so a later tick still carries it.
     */
    default void drain(java.time.Duration timeout) {
        drain();
    }

    /** Stops, and withdraws this reader's own entry so peers drop it at once (members only). */
    @Override
    default void close() {}

    /** A fixed view over {@code live} (tests, and callers holding plain heartbeats): every age is 0. */
    static MembershipSource of(Supplier<List<ReaderHeartbeat>> live) {
        Objects.requireNonNull(live, "live");
        return () -> live.get().stream().map(h -> new ReaderRegistry.LiveReader(h, 0)).toList();
    }
}
