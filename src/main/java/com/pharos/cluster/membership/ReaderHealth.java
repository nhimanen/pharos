package com.pharos.cluster.membership;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Readers a caller found unreachable. A reader's heartbeat can stay within its TTL for a while after
 * it died, so a failed call marks it suspect for one TTL: the router sends it no new shard calls.
 * A successful call clears the mark.
 */
public final class ReaderHealth {

    private final long ttlNanos;
    private final LongSupplier nanoClock;
    private final Map<String, Long> suspectUntil = new ConcurrentHashMap<>();

    public ReaderHealth(int readerTtlSeconds) {
        this(readerTtlSeconds, System::nanoTime);
    }

    public ReaderHealth(int readerTtlSeconds, LongSupplier nanoClock) {
        this.ttlNanos = readerTtlSeconds * 1_000_000_000L;
        this.nanoClock = nanoClock;
    }

    /** Excludes {@code node} for one TTL from now (a repeated failure extends it). */
    public void markSuspect(String node) {
        suspectUntil.put(node, nanoClock.getAsLong() + ttlNanos);
    }

    /** A call to {@code node} succeeded. */
    public void markHealthy(String node) {
        suspectUntil.remove(node);
    }

    /**
     * When {@code node}'s suspect mark expires ({@link Long#MAX_VALUE} when it is not suspect): the
     * earlier, the longer ago it was marked.
     */
    public long suspectUntil(String node) {
        Long until = suspectUntil.get(node);
        return until == null || !isSuspect(node) ? Long.MAX_VALUE : until;
    }

    public boolean isSuspect(String node) {
        Long until = suspectUntil.get(node);
        if (until == null) return false;
        if (nanoClock.getAsLong() - until >= 0) {
            suspectUntil.remove(node, until);
            return false;
        }
        return true;
    }
}
