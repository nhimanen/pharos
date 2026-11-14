package com.pharos.embedding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps the JVM from tearing down ONNX Runtime while a native call is still running.
 *
 * <p>{@code ai.onnxruntime.OrtEnvironment} registers a JVM shutdown hook that releases the
 * native ORT environment. Shutdown hooks run concurrently and in no defined order, so a SIGTERM
 * or Ctrl+C that arrives while a thread is inside {@code OrtSession.run} / {@code createSession}
 * freed the environment under it and the JVM died with SIGSEGV in {@code libonnxruntime.so}
 * instead of exiting — every recorded pharos crash (index, web and mcp-server) was such a
 * signal-time crash.
 *
 * <p>Every native model call (load, inference, predictor close) holds a {@link Ticket}. On the
 * first such call pharos installs SIGTERM/SIGINT handlers that stop admitting new native calls,
 * wait up to {@link #DRAIN_TIMEOUT_MS} for in-flight ones, and only then exit — so ORT's hook
 * runs with nothing in flight. {@link #beginShutdown} does the same for a normal exit.
 * Processes that never load a model never install the handlers.
 */
public final class NativeInferenceGuard {

    private static final Logger log = LoggerFactory.getLogger(NativeInferenceGuard.class);

    /** Longest a shutdown waits for in-flight native calls (one embedding batch takes ~1–2 s). */
    static final long DRAIN_TIMEOUT_MS = 15_000;

    private static final AtomicInteger inFlight = new AtomicInteger();
    private static final AtomicBoolean handlersInstalled = new AtomicBoolean();
    private static final Object drained = new Object();
    private static volatile boolean closing;

    private static final Ticket TICKET = new Ticket();

    private NativeInferenceGuard() {}

    /** Held for the duration of one native model call; close it in a try-with-resources. */
    public static final class Ticket implements AutoCloseable {
        private Ticket() {}
        @Override public void close() { leave(); }
    }

    /**
     * Admits one native call.
     *
     * @throws IllegalStateException when pharos is shutting down — the call must not start
     */
    public static Ticket enter() {
        installSignalHandlersOnce();
        inFlight.incrementAndGet();
        if (closing) {
            leave();
            throw new IllegalStateException("pharos is shutting down; not starting a model call");
        }
        return TICKET;
    }

    private static void leave() {
        if (inFlight.decrementAndGet() == 0) {
            synchronized (drained) { drained.notifyAll(); }
        }
    }

    /** Number of native calls currently running (diagnostics and tests). */
    static int inFlight() { return inFlight.get(); }

    /**
     * Stops admitting native calls and waits up to {@code timeoutMs} for running ones.
     *
     * @return true when nothing is in flight anymore
     */
    public static boolean beginShutdown(long timeoutMs) {
        closing = true;
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (drained) {
            while (inFlight.get() > 0) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    log.warn("Exiting with {} model call(s) still running", inFlight.get());
                    return false;
                }
                try {
                    drained.wait(left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return inFlight.get() == 0;
                }
            }
        }
        return true;
    }

    /** {@link #beginShutdown(long)} with the default timeout. */
    public static boolean beginShutdown() {
        return beginShutdown(DRAIN_TIMEOUT_MS);
    }

    /** Test hook: admit calls again after a {@link #beginShutdown}. */
    static void resetForTests() {
        closing = false;
    }

    private static void installSignalHandlersOnce() {
        if (!handlersInstalled.compareAndSet(false, true)) return;
        for (String name : new String[]{"TERM", "INT"}) {
            try {
                Signal.handle(new Signal(name), NativeInferenceGuard::onSignal);
            } catch (IllegalArgumentException e) {
                // signal unavailable or reserved (e.g. -Xrs): keep the JVM's default behaviour
                log.debug("Cannot install SIG{} handler: {}", name, e.getMessage());
            }
        }
    }

    private static void onSignal(Signal signal) {
        // Don't block the JVM's signal-dispatch thread; drain and exit on a dedicated thread.
        Thread t = new Thread(() -> {
            beginShutdown(DRAIN_TIMEOUT_MS);
            System.exit(128 + signal.getNumber());   // 143 for TERM, 130 for INT — as without the handler
        }, "pharos-graceful-shutdown");
        t.setDaemon(false);
        t.start();
    }
}
