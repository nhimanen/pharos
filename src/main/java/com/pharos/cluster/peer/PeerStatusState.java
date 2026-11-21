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
 */
public final class PeerStatusState {

    public static final String PATH = ReplicationEndpoint.PREFIX + "/status-state";
    /** The largest state accepted on a PUT. */
    public static final int MAX_BODY_BYTES = 1024 * 1024;

    private final String nonce;
    // guarded by this
    private byte[] json;
    private long version;

    public PeerStatusState() {
        byte[] b = new byte[8];
        new SecureRandom().nextBytes(b);
        this.nonce = HexFormat.of().formatHex(b);
    }

    /** The held state's bytes and ETag; empty when none was written. */
    public record Held(byte[] json, String etag) {}

    public synchronized Optional<Held> get() {
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
     * {@code liveWriters} (those that answered the catalog's last refresh), looked up on every call.
     * A read or write that fails throws, so that status refresh keeps its previous status.
     */
    public static StatusStateStore remote(Supplier<List<PeerCatalog.WriterRef>> liveWriters, PeerHttp http) {
        return new Remote(liveWriters, http);
    }

    /** The writer holding the state: the lowest node id among {@code live}; empty when there is none. */
    public static Optional<PeerCatalog.WriterRef> holder(List<PeerCatalog.WriterRef> live) {
        return live.stream().min(Comparator.comparing(PeerCatalog.WriterRef::node));
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
