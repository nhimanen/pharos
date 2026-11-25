package com.pharos.cluster.status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Keeps this node's {@code /cluster/status} and drives {@code /cluster/events}. Every refresh:
 * <ol>
 *   <li>reads a snapshot ({@link StatusSource});</li>
 *   <li>reads the shared {@link StatusState} and advances it ({@link Proposals#advance}); when that
 *       changes it, writes it by compare-and-set. A lost race re-reads and re-advances (up to
 *       {@value #CAS_ATTEMPTS} times; then this refresh uses the state as last read). A node whose
 *       snapshot is older than the state never writes;</li>
 *   <li>builds the status ({@link StatusBuilder}, pure) from the snapshot and that state, and
 *       renders both views: the public one (no reader addresses) and the token one;</li>
 *   <li>after releasing the status lock, hands the events between the state it saw last and this
 *       one to its SSE clients (so every node streams the same transitions, whichever node recorded
 *       them), and the webhook only the transitions its own write recorded (one delivery per
 *       transition across the cluster; hooks dedupe by proposal id anyway). Both only enqueue; a
 *       dispatch lock taken before the status lock is released keeps refreshes' events in order.</li>
 * </ol>
 * A failed refresh logs a WARN and keeps the previous status.
 *
 * <p><b>When it refreshes.</b> A cluster node ({@code background}) refreshes every
 * {@code refreshEvery} on a daemon thread from {@link #start}, and on a request finding the status
 * older than {@code maxAge}. A single node runs no thread of its own: a request builds the status on
 * demand (cached for {@code maxAge}), and the refresh timer runs only while an SSE client is
 * connected (it is what produces events).
 */
public final class StatusService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StatusService.class);
    static final int CAS_ATTEMPTS = 5;
    static final String TIMER_THREAD = "pharos-cluster-status";

    private final StatusSource source;
    private final StatusStateStore states;
    private final StatusSettings settings;
    private final StatusEvents events;
    private final Duration refreshEvery;
    private final Duration maxAge;
    private final LongSupplier nanoClock;
    private final boolean background;
    /** Guards the cached status and the state the node saw last. */
    private final ReentrantLock mu = new ReentrantLock();
    /** Orders event dispatch between refreshes; taken before {@link #mu} is released. */
    private final ReentrantLock dispatch = new ReentrantLock(true);
    private final Object timerMu = new Object();
    private ScheduledExecutorService timer; // guarded by timerMu
    private boolean started;                // guarded by timerMu
    private volatile boolean closed;

    // guarded by mu
    private byte[] publicJson;
    private byte[] tokenJson;
    private ClusterStatus status;
    private long builtAt;
    private StatusState lastSeen;

    /** A cluster node's service: refreshes in the background once {@link #start}ed. */
    public StatusService(StatusSource source, StatusStateStore states, StatusSettings settings, StatusEvents events,
                         Duration refreshEvery, Duration maxAge, LongSupplier nanoClock) {
        this(source, states, settings, events, refreshEvery, maxAge, nanoClock, true);
    }

    /**
     * @param background true: {@link #start} runs the refresh timer for good; false (single node):
     *                   the timer runs only while an SSE client is connected
     */
    public StatusService(StatusSource source, StatusStateStore states, StatusSettings settings, StatusEvents events,
                         Duration refreshEvery, Duration maxAge, LongSupplier nanoClock, boolean background) {
        this.source = Objects.requireNonNull(source, "source");
        this.states = Objects.requireNonNull(states, "states");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.events = events;
        this.refreshEvery = refreshEvery;
        this.maxAge = maxAge;
        this.nanoClock = nanoClock;
        this.background = background;
        if (events != null && !background) {
            events.onClientsChanged(n -> {
                if (n > 0) startTimer();
                else stopTimer();
            });
        }
    }

    /** A cluster node: starts the background refresh. A single node: nothing (see the class comment). Idempotent. */
    public void start() {
        synchronized (timerMu) {
            started = true;
        }
        if (background) startTimer();
    }

    private void startTimer() {
        synchronized (timerMu) {
            if (timer != null || closed || !started) return;
            timer = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, TIMER_THREAD);
                t.setDaemon(true);
                return t;
            });
            timer.scheduleWithFixedDelay(this::refreshQuietly, 0, Math.max(1, refreshEvery.toMillis()),
                    TimeUnit.MILLISECONDS);
        }
    }

    private void stopTimer() {
        synchronized (timerMu) {
            if (timer == null || background) return;
            if (events != null && events.clientCount() > 0) return;
            timer.shutdownNow();
            timer = null;
        }
    }

    /** True while the refresh timer runs. */
    public boolean timerRunning() {
        synchronized (timerMu) {
            return timer != null;
        }
    }

    public StatusEvents events() {
        return events;
    }

    private void refreshQuietly() {
        try {
            refresh();
        } catch (IOException | RuntimeException e) {
            log.warn("Refreshing the cluster status failed (keeping the previous one): {}", e.toString());
        }
    }

    /** One refresh (see the class comment); returns the new status (token view). */
    public ClusterStatus refresh() throws IOException {
        List<StatusEvent> seen;
        List<StatusEvent> recorded = List.of();
        ClusterStatus built;
        boolean dispatching = false;
        mu.lock();
        try {
            StatusSnapshot snap = source.read();
            StatusStateStore.Versioned v = states.read();
            StatusState used = v.state();
            for (int attempt = 1; attempt <= CAS_ATTEMPTS; attempt++) {
                StatusState next = Proposals.advance(snap, v.state(), settings);
                if (next == v.state()) { // nothing to record, or a newer observation is already there
                    used = v.state();
                    break;
                }
                if (states.write(next, v.etag())) {
                    recorded = StatusEvent.diff(v.state(), next, Proposals.candidates(snap, next, settings));
                    used = next;
                    break;
                }
                v = states.read(); // another node wrote first: advance from what it recorded
                used = v.state();
            }
            built = StatusBuilder.build(snap, used, settings);
            byte[] full = built.toJson();
            byte[] pub = built.withoutAddresses().toJson();
            seen = lastSeen == null ? List.of()
                    : StatusEvent.diff(lastSeen, used, Proposals.candidates(snap, used, settings));
            lastSeen = used;
            status = built;
            tokenJson = full;
            publicJson = pub;
            builtAt = nanoClock.getAsLong();
            dispatch.lock(); // before mu is released: the next refresh's events come after these
            dispatching = true;
        } finally {
            mu.unlock();
        }
        try {
            if (events != null) {
                events.broadcast(seen);       // offers to per-client queues: never blocks
                events.notifyWebhook(recorded); // enqueue only
            }
        } finally {
            if (dispatching) dispatch.unlock();
        }
        return built;
    }

    /**
     * The status JSON, refreshed first when older than {@code maxAge}; a failed refresh serves the
     * previous one (and throws only when there is none).
     *
     * @param withAddresses the token view (readers' {@code advertiseUrl}); false: the public view
     */
    public byte[] currentJson(boolean withAddresses) throws IOException {
        mu.lock();
        try {
            if (publicJson == null || nanoClock.getAsLong() - builtAt > maxAge.toNanos()) {
                try {
                    refresh(); // reentrant: mu is held, so concurrent requests wait for this one build
                } catch (IOException | RuntimeException e) {
                    if (publicJson == null) throw e instanceof IOException io ? io : new IOException(e.toString(), e);
                    log.warn("Refreshing the cluster status failed (serving the previous one): {}", e.toString());
                }
            }
            return withAddresses ? tokenJson : publicJson;
        } finally {
            mu.unlock();
        }
    }

    /** The public view; see {@link #currentJson(boolean)}. */
    public byte[] currentJson() throws IOException {
        return currentJson(false);
    }

    /** The last status built (token view); null before the first refresh. */
    public ClusterStatus current() {
        mu.lock();
        try {
            return status;
        } finally {
            mu.unlock();
        }
    }

    @Override
    public void close() {
        closed = true;
        ScheduledExecutorService t;
        synchronized (timerMu) {
            t = timer;
            timer = null;
        }
        if (t != null) t.shutdownNow();
        if (events != null) events.close();
    }
}
