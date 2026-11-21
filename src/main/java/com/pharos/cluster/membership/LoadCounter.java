package com.pharos.cluster.membership;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cheap request load of a node: requests in flight, the rate since the previous {@link #qps()} call,
 * and the request latency of the last minute ({@link LatencyWindow}, reported as {@code warmP95Millis}).
 */
public final class LoadCounter {
    private final LatencyWindow latency;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicLong started = new AtomicLong();
    private long sampledCount;
    private long sampledNanos = System.nanoTime();

    public LoadCounter() {
        this(new LatencyWindow());
    }

    public LoadCounter(LatencyWindow latency) {
        this.latency = latency;
    }

    public void begin() {
        inFlight.incrementAndGet();
        started.incrementAndGet();
    }

    public void end() {
        inFlight.updateAndGet(v -> Math.max(0, v - 1));
    }

    /** {@link #end()}, recording that the request took {@code latencyNanos}. */
    public void end(long latencyNanos) {
        end();
        if (latencyNanos >= 0) latency.record(latencyNanos);
    }

    /** The p95 request latency of the last minute (bucket upper bound, ms); null with no requests in it. */
    public Long warmP95Millis() {
        return latency.p95Millis();
    }

    public int inFlight() {
        return inFlight.get();
    }

    /** Requests per second since the last call (the heartbeat is the one caller). */
    public synchronized double qps() {
        long now = System.nanoTime();
        long count = started.get();
        double secs = (now - sampledNanos) / 1e9;
        double rate = secs <= 0 ? 0 : (count - sampledCount) / secs;
        sampledCount = count;
        sampledNanos = now;
        return Math.round(rate * 100) / 100.0;
    }
}
