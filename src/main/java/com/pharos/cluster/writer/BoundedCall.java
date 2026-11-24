package com.pharos.cluster.writer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A store call with a deadline, for the writer's shutdown path: the call runs on a daemon thread
 * and the caller gives up when the deadline passes. An S3 request on a saturated bucket can take
 * minutes (a 30 s socket timeout, three attempts) and a blocking socket read ignores interrupts, so
 * a call past its deadline is abandoned, not stopped: its daemon thread ends with the call or the
 * JVM.
 */
final class BoundedCall {

    /** A store call. */
    @FunctionalInterface
    interface IoCall<T> {
        T call() throws IOException;
    }

    private static final ExecutorService POOL = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "pharos-writer-bounded-io");
        t.setDaemon(true);
        return t;
    });

    private BoundedCall() {}

    /**
     * Runs {@code call} and waits for it at most {@code timeout}.
     *
     * @throws IOException the call's own, or one saying it gave up after {@code timeout}
     */
    static <T> T call(String what, Duration timeout, IoCall<T> call) throws IOException {
        long nanos = Math.max(0, timeout.toNanos());
        Future<T> f = POOL.submit(call::call);
        try {
            return f.get(nanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            f.cancel(true);
            throw new IOException("gave up on " + what + " after " + millis(timeout) + " ms", e);
        } catch (InterruptedException e) {
            f.cancel(true);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(what + " interrupted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException(what + " failed: " + cause, cause);
        }
    }

    static long millis(Duration d) {
        return Math.max(0, d.toMillis());
    }

    /** What is left of a deadline on {@link System#nanoTime}; never negative. */
    static Duration until(long deadlineNanos) {
        return Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime()));
    }

    static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }
}
