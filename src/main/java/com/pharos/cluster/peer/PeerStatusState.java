package com.pharos.cluster.peer;

import com.pharos.cluster.status.StatusState;
import com.pharos.cluster.status.StatusStateStore;

import java.io.IOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The {@code /cluster/status} hysteresis state without a bucket (Ruling P5-R2): the live writer with
 * the lowest node id holds it in memory and serves it at {@code /replication/status-state}
 * (token-guarded, P5-R3); every node reads and writes it there with the compare-and-set semantics of
 * the bucket's {@code catalog/status/state.json}.
 *
 * <ul>
 *   <li>{@code GET}: 200 with the state JSON and its {@code ETag}; 404 when this writer holds none.</li>
 *   <li>{@code PUT} with {@code If-Match: <etag>} (replace that version) or {@code If-None-Match: *}
 *       (create): 200 with the new {@code ETag}; 412 when the state is no longer at that version.</li>
 * </ul>
 *
 * <p>An ETag names this process ({@code <nonce>-<version>}, the nonce random per process), so a
 * version read from one writer never matches on another or on a restarted one: when the holder dies,
 * the next writer's (empty) state starts over and racing nodes re-read it. Proposals may then reappear
 * after their wait window, once per holder loss (P5-R2's accepted cost).
 *
 * <p><b>Holder.</b> The lowest id among the writers the catalog tracks: live, or missing but not yet
 * dropped ({@link PeerCatalog#DROP_AFTER} missed refreshes). A writer that misses one refresh keeps the
 * state; nodes that cannot reach it keep their previous status and write nothing (no flip to another
 * writer's empty state and back).
 *
 * <p><b>Idle expiry.</b> A held state that nobody read or wrote for {@link #idleExpiry} is dropped
 * (the next {@code GET} answers 404, and the state starts over). Every node reads it on every status
 * refresh, so this only happens to a writer that was cut off from the cluster: when it returns, its
 * pre-partition state (old {@code since}s) must not fire proposals at once. Reads count, not only
 * writes: a quiet cluster whose state does not change writes nothing for long stretches.
 */
public final class PeerStatusState {

    public static final String PATH = ReplicationEndpoint.PREFIX + "/status-state";
    /** The largest state accepted on a PUT. */
    public static final int MAX_BODY_BYTES = 1024 * 1024;

    private final String nonce;
    private final long idleNanos;
    private final LongSupplier nanoClock;
    // guarded by this
    private byte[] json;
    private long version;
    private long lastAccess;

    /** With the default idle expiry ({@value #DEFAULT_IDLE_SECONDS} s). */
    public PeerStatusState() {
        this(Duration.ofSeconds(DEFAULT_IDLE_SECONDS), System::nanoTime);
    }

    /** The idle expiry of the defaults: {@code clearGraceSeconds} 60 s, above 4 × the 15 s refresh. */
    static final long DEFAULT_IDLE_SECONDS = 60;

    public PeerStatusState(Duration idleExpiry, LongSupplier nanoClock) {
        byte[] b = new byte[8];
        new SecureRandom().nextBytes(b);
        this.nonce = HexFormat.of().formatHex(b);
        this.idleNanos = Objects.requireNonNull(idleExpiry, "idleExpiry").toNanos();
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.lastAccess = nanoClock.getAsLong();
    }

    /**
     * How long a held state may go unread and unwritten before it is dropped:
     * {@code cluster.status.windows.clearGraceSeconds} (the hysteresis's own "condition gone" grace),
     * and never less than the time the catalog takes to drop a writer
     * ({@code (DROP_AFTER + 1) × cluster.peers.refreshSeconds}), so a holder that nodes still target
     * while it is missing keeps its state.
     */
    public static Duration idleExpiry(com.pharos.config.ClusterConfig cfg) {
        long grace = cfg.getStatus().getWindows().getClearGraceSeconds();
        long drop = (PeerCatalog.DROP_AFTER + 1L) * cfg.getPeers().getRefreshSeconds();
        return Duration.ofSeconds(Math.max(grace, drop));
    }

    // Holding this: drops a state idle for the expiry, then records this access.
    private void access() {
        long now = nanoClock.getAsLong();
        if (json != null && now - lastAccess > idleNanos) json = null; // version stays: old ETags never match
        lastAccess = now;
    }

    /** The held state's bytes and ETag; empty when none was written. */
    public record Held(byte[] json, String etag) {}

    public synchronized Optional<Held> get() {
        access();
        return json == null ? Optional.empty() : Optional.of(new Held(json.clone(), etag(version)));
    }

    /**
     * Writes {@code next} when the held state is at {@code ifMatch} (null: none is held yet).
     *
     * @return the new ETag; empty when the state changed meanwhile (412)
     * @throws IllegalArgumentException when {@code next} is not a status state
     */
    public synchronized Optional<String> put(byte[] next, String ifMatch) {
        if (next == null || next.length == 0 || next.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("status state must be 1.." + MAX_BODY_BYTES + " bytes");
        }
        try {
            StatusState.read(next);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("not a status state");
        }
        access();
        String current = json == null ? null : etag(version);
        if (!Objects.equals(current, ifMatch)) return Optional.empty();
        json = next.clone();
        version++;
        return Optional.of(etag(version));
    }

    private String etag(long v) {
        return "\"" + nonce + "-" + v + "\"";
    }

    // ── the client side ─────────────────────────────────────────────────────

    /**
     * The {@link StatusStateStore} of a peer-mode node: the state on the lowest-id writer among
     * {@code writers} (pass {@link #tracked}: live or missing, not dropped), looked up on every call.
     * A read or write that fails (the holder unreachable) throws, so that status refresh keeps its
     * previous status and writes nothing; it never falls over to another writer.
     */
    public static StatusStateStore remote(Supplier<List<PeerCatalog.WriterRef>> liveWriters, PeerHttp http) {
        return new Remote(liveWriters, http);
    }

    /** The writer holding the state: the lowest node id among {@code writers}; empty when there is none. */
    public static Optional<PeerCatalog.WriterRef> holder(List<PeerCatalog.WriterRef> writers) {
        return writers.stream().min(Comparator.comparing(PeerCatalog.WriterRef::node));
    }

    /** The writers {@code catalog} tracks: live, or missing but not yet dropped. */
    public static List<PeerCatalog.WriterRef> tracked(PeerCatalog catalog) {
        return catalog.writers().stream()
                .map(w -> new PeerCatalog.WriterRef(w.node(), java.net.URI.create(w.url()))).toList();
    }

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    private static final class Remote implements StatusStateStore {
        private final Supplier<List<PeerCatalog.WriterRef>> liveWriters;
        private final PeerHttp http;

        Remote(Supplier<List<PeerCatalog.WriterRef>> liveWriters, PeerHttp http) {
            this.liveWriters = Objects.requireNonNull(liveWriters, "liveWriters");
            this.http = Objects.requireNonNull(http, "http");
        }

        private PeerCatalog.WriterRef target() throws IOException {
            return holder(liveWriters.get()).orElseThrow(() ->
                    new IOException("no live writer holds the cluster status state"));
        }

        @Override
        public Versioned read() throws IOException {
            PeerCatalog.WriterRef w = target();
            HttpResponse<byte[]> r = call(w, http.request(w.url(), PATH, TIMEOUT).build());
            if (r.statusCode() == 404) return new Versioned(StatusState.EMPTY, null);
            if (r.statusCode() != 200) {
                throw new IOException("status state on writer " + w + " answered "
                        + PeerHttp.describe(r.statusCode(), r.body()));
            }
            String etag = r.headers().firstValue("ETag").orElseThrow(() ->
                    new IOException("status state on writer " + w + " has no ETag"));
            try {
                return new Versioned(StatusState.read(r.body()), etag);
            } catch (IOException e) { // unreadable: start over, replacing it by CAS on its etag
                return new Versioned(StatusState.EMPTY, etag);
            }
        }

        @Override
        public boolean write(StatusState next, String etag) throws IOException {
            PeerCatalog.WriterRef w = target();
            HttpRequest.Builder b = http.request(w.url(), PATH, TIMEOUT)
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(next.toJson()))
                    .header("Content-Type", "application/json");
            if (etag == null) b.header("If-None-Match", "*");
            else b.header("If-Match", etag);
            HttpResponse<byte[]> r = call(w, b.build());
            if (r.statusCode() == 200) return true;
            if (r.statusCode() == 412) return false;
            throw new IOException("writing the status state on writer " + w + " answered "
                    + PeerHttp.describe(r.statusCode(), r.body()));
        }

        private HttpResponse<byte[]> call(PeerCatalog.WriterRef w, HttpRequest req) throws IOException {
            try {
                return http.sendAsync(req).get(TIMEOUT.toMillis() + 1000, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.io.InterruptedIOException("interrupted calling writer " + w);
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
                Throwable c = e.getCause() == null ? e : e.getCause();
                throw new PeerHttp.WriterUnreachableException(w.toString(),
                        c instanceof IOException io ? io : new IOException(c.toString(), c));
            }
        }
    }
}
