package com.pharos.cluster.membership;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;

/**
 * Request latencies of the last {@value #WINDOW_SECONDS} seconds as a fixed-bucket histogram: one
 * slot per second, each with a count per latency bucket ({@link #BOUNDS_MILLIS}). Recording is two
 * atomic operations on preallocated arrays, with no allocation per request. A slot is reset by the
 * first request of a new second that lands on it; a sample recorded concurrently with that reset
 * may be lost, which only makes the percentile an approximation (it is one anyway: it reports a
 * bucket's upper bound).
 */
public final class LatencyWindow {

    public static final int WINDOW_SECONDS = 60;
    /** Upper bounds (inclusive, milliseconds) of the latency buckets; one overflow bucket follows. */
    static final long[] BOUNDS_MILLIS = {1, 2, 3, 4, 5, 7, 10, 15, 20, 25, 30, 40, 50, 75, 100, 150, 200, 250, 300,
            400, 500, 750, 1000, 1500, 2000, 3000, 5000, 7500, 10_000, 15_000, 20_000, 30_000, 60_000};
    private static final int BUCKETS = BOUNDS_MILLIS.length + 1;

    private final LongSupplier nanoClock;
    /** The epoch second each slot currently counts; -1 when never used. */
    private final AtomicLongArray slotSecond = new AtomicLongArray(WINDOW_SECONDS);
    private final AtomicLongArray counts = new AtomicLongArray(WINDOW_SECONDS * BUCKETS);

    public LatencyWindow() {
        this(System::nanoTime);
    }

    /** @param nanoClock a monotonic clock in nanoseconds (tests pass a fake one) */
    public LatencyWindow(LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
        for (int i = 0; i < WINDOW_SECONDS; i++) slotSecond.set(i, -1);
    }

    /** Records one request that took {@code latencyNanos}. */
    public void record(long latencyNanos) {
        long sec = Math.floorDiv(nanoClock.getAsLong(), 1_000_000_000L);
        int slot = (int) Math.floorMod(sec, (long) WINDOW_SECONDS);
        long current = slotSecond.get(slot);
        if (current != sec && current < sec && slotSecond.compareAndSet(slot, current, sec)) {
            int base = slot * BUCKETS;
            for (int b = 0; b < BUCKETS; b++) counts.set(base + b, 0);
        }
        counts.incrementAndGet(slot * BUCKETS + bucket(latencyNanos / 1_000_000L));
    }

    private static int bucket(long millis) {
        int lo = 0;
        int hi = BOUNDS_MILLIS.length; // overflow bucket
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (BOUNDS_MILLIS[mid] >= millis) hi = mid;
            else lo = mid + 1;
        }
        return lo;
    }

    /**
     * The 95th percentile of the last {@value #WINDOW_SECONDS} seconds, as the upper bound of the
     * bucket it falls in (the last bound for the overflow bucket); null when nothing was recorded.
     */
    public Long p95Millis() {
        return percentileMillis(0.95);
    }

    Long percentileMillis(double q) {
        long now = Math.floorDiv(nanoClock.getAsLong(), 1_000_000_000L);
        long[] sum = new long[BUCKETS];
        long total = 0;
        for (int s = 0; s < WINDOW_SECONDS; s++) {
            long sec = slotSecond.get(s);
            if (sec < 0 || now - sec >= WINDOW_SECONDS || sec > now) continue;
            int base = s * BUCKETS;
            for (int b = 0; b < BUCKETS; b++) {
                long c = counts.get(base + b);
                sum[b] += c;
                total += c;
            }
        }
        if (total == 0) return null;
        long rank = (long) Math.ceil(q * total);
        long seen = 0;
        for (int b = 0; b < BUCKETS; b++) {
            seen += sum[b];
            if (seen >= rank) return BOUNDS_MILLIS[Math.min(b, BOUNDS_MILLIS.length - 1)];
        }
        return BOUNDS_MILLIS[BOUNDS_MILLIS.length - 1];
    }
}
