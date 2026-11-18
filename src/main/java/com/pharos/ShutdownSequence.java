package com.pharos;

import com.pharos.embedding.NativeInferenceGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The order a pharos process stops in on SIGTERM, SIGINT or SIGHUP, before any JVM shutdown hook
 * runs. Shutdown hooks run concurrently and in no defined order, and ONNX Runtime registers one that
 * frees its native environment: work that still calls a model while hooks run can crash the JVM
 * (SIGSEGV in {@code OrtSession.run}). So on a signal, on a dedicated thread:
 * <ol>
 *   <li>the registered drain steps run, in registration order, while the process keeps serving and
 *       model calls are still admitted (a reader announces {@code draining} and finishes its
 *       in-flight requests: {@code ClusterRuntime#drainAndClose});</li>
 *   <li>{@link NativeInferenceGuard#beginShutdown} stops admitting model calls and waits for the
 *       running ones;</li>
 *   <li>{@code System.exit(128 + signo)} (143 for TERM, 130 for INT, as without the handler): only
 *       now do the shutdown hooks, ORT's among them, start.</li>
 * </ol>
 * A second signal halts at once (no hooks run, so none frees anything under a running call).
 *
 * <p>Long-running processes that embed install it when they start ({@code web}, {@code mcp-server},
 * {@code index}, {@code embed}, {@code upgrade-indexes}, a serving reader); the first model call of
 * any other process installs it too ({@link NativeInferenceGuard#enter}). A shutdown that is no
 * signal (the command returned) keeps the old path: Main drains the model calls, then exits, and the
 * shutdown hooks are the fallback for everything else.
 */
public final class ShutdownSequence {

    private static final Logger log = LoggerFactory.getLogger(ShutdownSequence.class);

    private record Step(String name, Runnable run) {}

    private static final List<Step> steps = new ArrayList<>();          // guarded by itself
    private static final AtomicBoolean installed = new AtomicBoolean();
    private static final AtomicInteger signals = new AtomicInteger();

    private ShutdownSequence() {}

    /** Installs the TERM/INT/HUP handlers once (idempotent). */
    public static void install() {
        if (!installed.compareAndSet(false, true)) return;
        // HUP too: closing the terminal of a foreground `pharos index` is a JVM shutdown signal
        for (String name : new String[]{"TERM", "INT", "HUP"}) {
            try {
                Signal.handle(new Signal(name), ShutdownSequence::onSignal);
            } catch (IllegalArgumentException e) {
                // signal unavailable or reserved (e.g. -Xrs): keep the JVM's default behaviour
                log.debug("Cannot install SIG{} handler: {}", name, e.getMessage());
            }
        }
    }

    /**
     * Adds a step that runs on a signal before model calls stop and before any shutdown hook, and
     * installs the handlers. A step should be bounded and idempotent (the shutdown hooks may run it
     * again on a non-signal exit).
     *
     * @return removes the step (its owner closed)
     */
    public static AutoCloseable addDrainStep(String name, Runnable step) {
        Step s = new Step(name, step);
        synchronized (steps) {
            steps.add(s);
        }
        install();
        return () -> {
            synchronized (steps) {
                steps.remove(s);
            }
        };
    }

    private static void onSignal(Signal signal) {
        int code = 128 + signal.getNumber();
        NativeInferenceGuard.signalled(code);
        if (signals.incrementAndGet() > 1) {
            // Second signal (e.g. Ctrl+C again): exit now. halt() runs no shutdown hooks, so
            // ORT's environment closer can't free anything under a running call either.
            Runtime.getRuntime().halt(code);
        }
        List<Step> todo;
        synchronized (steps) {
            todo = List.copyOf(steps);
        }
        int running = NativeInferenceGuard.inFlightCalls();
        if (!todo.isEmpty() || running > 0) {
            System.err.printf("pharos: shutting down in order (draining, then %d running model call(s));"
                    + " send the signal again — e.g. Ctrl+C — to exit immediately%n", running);
        }
        // Don't block the JVM's signal-dispatch thread.
        Thread t = new Thread(() -> {
            for (Step s : todo) {
                try {
                    s.run().run();
                } catch (RuntimeException | Error e) {
                    log.warn("Shutdown step {} failed: {}", s.name(), e.toString());
                }
            }
            NativeInferenceGuard.beginShutdown();
            System.exit(code);
        }, "pharos-graceful-shutdown");
        t.setDaemon(false);
        t.start();
    }
}
