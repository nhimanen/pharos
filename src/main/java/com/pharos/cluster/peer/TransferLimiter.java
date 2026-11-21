package com.pharos.cluster.peer;

import java.util.Optional;
import java.util.concurrent.Semaphore;

/**
 * Caps concurrent work of one kind on a writer ({@code cluster.peers.maxTransfersPerWriter} file
 * transfers, or long-polls). {@link #tryAcquire} never waits: a request above the cap is answered
 * 503 with {@code Retry-After} at once, and the reader backs off with jitter.
 */
public final class TransferLimiter {

    private final Semaphore permits;
    private final int max;

    public TransferLimiter(int max) {
        if (max < 1) throw new IllegalArgumentException("max must be at least 1, got " + max);
        this.max = max;
        this.permits = new Semaphore(max);
    }

    /** One slot; closing it (once) gives it back. */
    public final class Permit implements AutoCloseable {
        private boolean released;

        private Permit() {}

        @Override
        public synchronized void close() {
            if (released) return;
            released = true;
            permits.release();
        }
    }

    /** A slot when one is free; empty at the cap. */
    public Optional<Permit> tryAcquire() {
        return permits.tryAcquire() ? Optional.of(new Permit()) : Optional.empty();
    }

    public int max() { return max; }

    public int inFlight() { return max - permits.availablePermits(); }
}
