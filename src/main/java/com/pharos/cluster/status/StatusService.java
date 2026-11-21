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
import java.util.function.LongSupplier;

/**
 * Keeps this node's {@code /cluster/status} and drives {@code /cluster/events}. Every refresh (every
 * {@code refreshEvery} on a daemon thread, and on a request finding the status older than
 * {@code maxAge}):
 * <ol>
 *   <li>reads a snapshot ({@link StatusSource});</li>
 *   <li>reads the shared {@link StatusState} and advances it ({@link Proposals#advance}); when that
 *       changes it, writes it by compare-and-set. A lost race re-reads and re-advances (up to
 *       {@value #CAS_ATTEMPTS} times; then this refresh uses the state as last read). A node whose
 *       snapshot is older than the state never writes;</li>
 *   <li>builds the status ({@link StatusBuilder}, pure) from the snapshot and that state;</li>
 *   <li>streams the events between the state it saw last and this one to its SSE clients (so every
 *       node streams the same transitions, whichever node recorded them), and sends the webhook only
 *       the transitions its own write recorded (one delivery per transition across the cluster;
 *       hooks dedupe by proposal id anyway).</li>
 * </ol>
 * A failed refresh logs a WARN and keeps the previous status.
 */
public final class StatusService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StatusService.class);
    static final int CAS_ATTEMPTS = 5;

    private final StatusSource source;
    private final StatusStateStore states;
    private final StatusSettings settings;
    private final StatusEvents events;
    private final Duration refreshEvery;
    private final Duration maxAge;
    private final LongSupplier nanoClock;
    private ScheduledExecutorService timer;

    // guarded by this
    private byte[] json;
    private ClusterStatus status;
    private long builtAt;
    private StatusState lastSeen;

    public StatusService(StatusSource source, StatusStateStore states, StatusSettings settings, StatusEvents events,
                         Duration refreshEvery, Duration maxAge, LongSupplier nanoClock) {
        this.source = Objects.requireNonNull(source, "source");
        this.states = Objects.requireNonNull(states, "states");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.events = events;
        this.refreshEvery = refreshEvery;
        this.maxAge = maxAge;
        this.nanoClock = nanoClock;
    }

    /** Refreshes every {@code refreshEvery} on a daemon thread, starting now. Idempotent. */
    public synchronized void start() {
        if (timer != null) return;
        timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-cluster-status");
            t.setDaemon(true);
            return t;
        });
        timer.scheduleWithFixedDelay(this::refreshQuietly, 0, Math.max(1, refreshEvery.toMillis()), TimeUnit.MILLISECONDS);
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

    /** One refresh (see the class comment); returns the new status. */
    public synchronized ClusterStatus refresh() throws IOException {
        StatusSnapshot snap = source.read();
        StatusStateStore.Versioned v = states.read();
        StatusState used = v.state();
        List<StatusEvent> recorded = List.of();
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
        ClusterStatus built = StatusBuilder.build(snap, used, settings);
        byte[] bytes = built.toJson();
        List<StatusEvent> seen = lastSeen == null ? List.of()
                : StatusEvent.diff(lastSeen, used, Proposals.candidates(snap, used, settings));
        lastSeen = used;
        status = built;
        json = bytes;
        builtAt = nanoClock.getAsLong();
        if (events != null) {
            events.broadcast(seen);
            events.notifyWebhook(recorded);
        }
        return built;
    }

    /**
     * The status JSON, refreshed first when older than {@code maxAge}; a failed refresh serves the
     * previous one (and throws only when there is none).
     */
    public synchronized byte[] currentJson() throws IOException {
        if (json == null || nanoClock.getAsLong() - builtAt > maxAge.toNanos()) {
            try {
                refresh();
            } catch (IOException | RuntimeException e) {
                if (json == null) throw e instanceof IOException io ? io : new IOException(e.toString(), e);
                log.warn("Refreshing the cluster status failed (serving the previous one): {}", e.toString());
            }
        }
        return json;
    }

    /** The last status built; null before the first refresh. */
    public synchronized ClusterStatus current() {
        return status;
    }

    @Override
    public void close() {
        ScheduledExecutorService t;
        synchronized (this) {
            t = timer;
            timer = null;
        }
        if (t != null) t.shutdownNow();
        if (events != null) events.close();
    }
}
