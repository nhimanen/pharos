package com.pharos.cluster.membership;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Cheap request load of a node: requests in flight and the rate since the previous {@link #qps()} call. */
public final class LoadCounter {
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicLong started = new AtomicLong();
    private long sampledCount;
    private long sampledNanos = System.nanoTime();

    public void begin() {
        inFlight.incrementAndGet();
        started.incrementAndGet();
    }

    public void end() {
        inFlight.updateAndGet(v -> Math.max(0, v - 1));
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
