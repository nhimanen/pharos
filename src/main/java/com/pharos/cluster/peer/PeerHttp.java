package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.internal.InternalApi;
import com.pharos.config.ClusterConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The HTTP client of a peer-mode reader: discovery, the catalog, {@link PeerReplicaSource}. Every
 * request carries {@code X-Pharos-Cluster-Token}; with {@code cluster.tls.truststore} set, HTTPS
 * trusts that store only (P5-R7), otherwise the JDK's default trust.
 *
 * <p>{@link #send} retries a 503 (honouring {@code Retry-After}) and a connection error with
 * exponential backoff plus jitter, up to {@link #ATTEMPTS} attempts; any other answer is returned.
 * The failure names the writer, so a cold read during a writer outage fails with a clear message.
 */
public final class PeerHttp implements AutoCloseable {

    /** Virtual threads for parallel calls (gossip, the catalog's per-writer reads). */
    static final Executor VIRTUAL = r -> Thread.ofVirtual().name("pharos-peer-http").start(r);
    static final int ATTEMPTS = 3;
    /** First backoff after a connection error; doubled per attempt, plus up to as much jitter. */
    static final Duration BASE_BACKOFF = Duration.ofMillis(200);
    /** The longest wait one {@code Retry-After} may impose on a request. */
    static final Duration MAX_RETRY_AFTER = Duration.ofSeconds(10);

    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient client;
    private final String token;
    private final Duration requestTimeout;
    private final boolean tls;
    /** Test hook: how a backoff waits. */
    private volatile Sleeper sleeper = d -> Thread.sleep(d.toMillis());

    @FunctionalInterface
    interface Sleeper {
        void sleep(Duration d) throws InterruptedException;
    }

    /**
     * @param ssl            the truststore's context; null uses the JDK default
     * @param connectTimeout a dead writer fails this fast
     * @param requestTimeout bound of one non-long-poll request (headers and body)
     */
    public PeerHttp(String token, SSLContext ssl, Duration connectTimeout, Duration requestTimeout) {
        this.token = Objects.requireNonNull(token, "token");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.tls = ssl != null;
        HttpClient.Builder b = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Objects.requireNonNull(connectTimeout, "connectTimeout"))
                .followRedirects(HttpClient.Redirect.NEVER);
        if (ssl != null) b.sslContext(ssl);
        this.client = b.build();
    }

    /** The client of a peer-pull reader built from {@code cluster.*}. */
    public static PeerHttp of(ClusterConfig c) {
        return new PeerHttp(c.effectiveToken(), sslContext(c.getTls()), Duration.ofMillis(c.getConnectTimeoutMillis()),
                REQUEST_TIMEOUT);
    }

    /** Bound of one ordinary request (catalog, manifest, one file or range). */
    public static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);

    /** True when a truststore is configured: DNS-discovered writers are reached over HTTPS. */
    public boolean tls() { return tls; }

    void setSleeper(Sleeper s) { this.sleeper = Objects.requireNonNull(s, "sleeper"); }

    /**
     * The context trusting {@code cluster.tls.truststore} only; null when none is configured.
     *
     * @throws IllegalStateException when the store cannot be read (names the path, never the password)
     */
    public static SSLContext sslContext(ClusterConfig.TlsConfig tls) {
        if (tls == null || tls.getTruststore() == null) return null;
        Path path = tls.getTruststore();
        try (InputStream in = Files.newInputStream(path)) {
            KeyStore ks = KeyStore.getInstance(tls.getKeystoreType());
            ks.load(in, tls.password());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), null);
            return ctx;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("cannot load cluster.tls.truststore " + path + ": " + e.getMessage(), e);
        }
    }

    HttpRequest.Builder request(URI base, String path, Duration timeout) {
        return HttpRequest.newBuilder(base.resolve(path)).timeout(timeout).GET()
                .header(InternalApi.TOKEN_HEADER, token);
    }

    /**
     * One request with retries: a 503 waits its {@code Retry-After} (at most {@link #MAX_RETRY_AFTER})
     * plus jitter, a connection error waits an exponential backoff plus jitter. The last 503 is
     * returned; the last connection error is thrown as {@link WriterUnreachableException}.
     *
     * @param who the writer, for messages ({@code node (url)})
     */
    <T> HttpResponse<T> send(String who, HttpRequest req, HttpResponse.BodyHandler<T> body) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            HttpResponse<T> r;
            try {
                r = client.send(req, body);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted calling writer " + who);
            } catch (IOException e) {
                last = e;
                if (attempt + 1 < ATTEMPTS) pause(backoff(attempt));
                continue;
            }
            if (r.statusCode() != 503 || attempt + 1 == ATTEMPTS) return r;
            discard(r);
            pause(retryAfter(r.headers().firstValue("Retry-After")));
        }
        throw new WriterUnreachableException(who, last);
    }

    /**
     * One attempt, no retry (a peer reader: on any failure the caller moves on to the next holder
     * or the writer instead of waiting out a backoff).
     */
    <T> HttpResponse<T> sendOnce(HttpRequest req, HttpResponse.BodyHandler<T> body) throws IOException {
        try {
            return client.send(req, body);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted calling " + req.uri().getHost());
        }
    }

    /**
     * {@code GET base+path} as JSON (200 only): one attempt bounded by {@link #CONTROL_TIMEOUT} (the
     * catalog and gossip reads of a refresh; a writer that misses one is retried on the next).
     */
    JsonNode getJson(URI base, String path) throws IOException {
        HttpResponse<byte[]> r;
        try {
            r = client.send(request(base, path, CONTROL_TIMEOUT).build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted calling writer " + base);
        } catch (IOException e) {
            throw new WriterUnreachableException(base.toString(), e);
        }
        if (r.statusCode() != 200) throw new IOException(base + path + " answered " + describe(r.statusCode(), r.body()));
        return JSON.readTree(r.body());
    }

    /** Bound of one catalog or gossip read: a hung writer must not stall a refresh. */
    static final Duration CONTROL_TIMEOUT = Duration.ofSeconds(10);

    /**
     * A body stream that is closed (so its next read fails) when no byte arrived for
     * {@link #STALL_TIMEOUT}: the JDK client has no read timeout once the headers are in, and a
     * frozen writer would otherwise hang the syncer or a query's block fetch forever.
     */
    InputStream guard(InputStream body, String what) {
        Guarded g = new Guarded(body, what);
        guarded.add(g);
        return g;
    }

    /** No byte of a body for this long fails the transfer. */
    static final Duration STALL_TIMEOUT = Duration.ofSeconds(60);
    private volatile Duration stallTimeout = STALL_TIMEOUT;

    /** Test hook: the stall bound. */
    void setStallTimeout(Duration d) { this.stallTimeout = Objects.requireNonNull(d, "stallTimeout"); }
    private final java.util.Set<Guarded> guarded = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.ScheduledExecutorService watchdog =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-peer-stall-watchdog");
                t.setDaemon(true);
                return t;
            });

    {
        watchdog.scheduleWithFixedDelay(this::closeStalled, 1, 1, java.util.concurrent.TimeUnit.SECONDS);
    }

    private void closeStalled() {
        long now = System.nanoTime();
        for (Guarded g : guarded) {
            if (now - g.lastProgress > stallTimeout.toNanos()) {
                org.slf4j.LoggerFactory.getLogger(PeerHttp.class).warn("Transfer {} stalled for {} s; aborting it",
                        g.what, stallTimeout.toSeconds());
                g.stalled = true;
                guarded.remove(g);
                g.closeQuietly();
            }
        }
    }

    private final class Guarded extends java.io.FilterInputStream {
        final String what;
        volatile long lastProgress = System.nanoTime();
        volatile boolean stalled;

        Guarded(InputStream in, String what) {
            super(in);
            this.what = what;
        }

        @Override
        public int read() throws IOException {
            int b = check(() -> super.read());
            lastProgress = System.nanoTime();
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            int n = check(() -> super.read(buf, off, len));
            lastProgress = System.nanoTime();
            return n;
        }

        private int check(IoCall c) throws IOException {
            try {
                int n = c.call();
                // closing the JDK's body stream from the watchdog ends a blocked read as end-of-stream
                if (n < 0 && stalled) throw new IOException("transfer " + what + " stalled: no byte for "
                        + stallTimeout.toSeconds() + " s");
                return n;
            } catch (IOException e) {
                if (e.getMessage() != null && e.getMessage().startsWith("transfer " + what + " stalled")) throw e;
                if (stalled) throw new IOException("transfer " + what + " stalled: no byte for "
                        + stallTimeout.toSeconds() + " s", e);
                throw e;
            }
        }

        void closeQuietly() {
            try {
                super.close();
            } catch (IOException ignored) {
                // the reader sees the failure on its next read
            }
        }

        @Override
        public void close() throws IOException {
            guarded.remove(this);
            super.close();
        }
    }

    @FunctionalInterface
    private interface IoCall {
        int call() throws IOException;
    }

    /** One asynchronous attempt (long-polls: their loop does its own backoff). */
    CompletableFuture<HttpResponse<byte[]>> sendAsync(HttpRequest req) {
        return client.sendAsync(req, HttpResponse.BodyHandlers.ofByteArray());
    }

    Duration requestTimeout() { return requestTimeout; }

    private void pause(Duration d) throws InterruptedIOException {
        try {
            sleeper.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while backing off");
        }
    }

    /** Exponential backoff with full jitter on top: {@code base·2^n + rand(0, base·2^n)}. */
    static Duration backoff(int attempt) {
        long ms = BASE_BACKOFF.toMillis() << Math.min(attempt, 6);
        return Duration.ofMillis(ms + ThreadLocalRandom.current().nextLong(ms + 1));
    }

    /** {@code Retry-After} seconds (capped; 1 s when absent or not a number) plus up to 500 ms of jitter. */
    static Duration retryAfter(Optional<String> header) {
        long s = 1;
        if (header.isPresent()) {
            try {
                s = Math.max(0, Long.parseLong(header.get().strip()));
            } catch (NumberFormatException e) {
                s = 1; // an HTTP date: not sent by our writers
            }
        }
        long ms = Math.min(s * 1000, MAX_RETRY_AFTER.toMillis());
        return Duration.ofMillis(ms + ThreadLocalRandom.current().nextLong(501));
    }

    static <T> void discard(HttpResponse<T> r) {
        if (r.body() instanceof InputStream in) {
            try {
                in.close();
            } catch (IOException ignored) {
                // nothing to release beyond the connection
            }
        }
    }

    /** {@code 503 (too many transfers ...)}: the status and the writer's error text, cut short. */
    static String describe(int status, byte[] body) {
        String text = body == null ? "" : new String(body, 0, Math.min(body.length, 300), StandardCharsets.UTF_8);
        try {
            JsonNode n = JSON.readTree(text);
            if (n != null && n.hasNonNull("error")) text = n.get("error").asText();
        } catch (IOException | RuntimeException notJson) {
            // keep the raw text
        }
        return status + (text.isBlank() ? "" : " (" + text.strip() + ")");
    }

    @Override
    public void close() {
        watchdog.shutdownNow();
        client.shutdownNow();
    }

    /** A writer that could not be reached at all (connection refused, timed out, reset). */
    public static final class WriterUnreachableException extends IOException {
        WriterUnreachableException(String who, IOException cause) {
            super("writer " + who + " is unreachable" + (cause == null ? "" : ": " + cause), cause);
        }
    }
}
