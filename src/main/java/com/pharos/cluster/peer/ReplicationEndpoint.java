package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.InternalApi;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.SynonymsMerge;
import com.pharos.config.ClusterConfig;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The writer side of the peer-pull backend: {@code /replication/*}, served by {@code pharos web} on a
 * peer-mode writer from its {@link PeerStore}. Every route requires {@code X-Pharos-Cluster-Token}
 * (constant-time, {@link InternalApi#tokenMatcher}): a before-filter answers 401 before any handler
 * runs, so no byte of the store is served without it (P5-R3).
 *
 * <ul>
 *   <li>{@code GET /replication/catalog}: {@code {"writer": node, "shards": [{"shard", "latest"}]}}, the
 *       shards this writer published that have a {@code LATEST}.</li>
 *   <li>{@code GET /replication/<shard>/latest?wait=<s>&after=<gen>}: {@code {"generation": N,
 *       "manifest": "gen-N.json", "peers": [url, …]}}, {@code peers} being up to
 *       {@value #MAX_LISTED_PEERS} live readers whose heartbeat says they hold generation N in full
 *       ({@code generationsHeld}; a reader verifies whatever they send). With {@code wait > 0} and no generation newer than {@code after}
 *       (default 0), the request waits on the {@link GenerationSignal} (no thread held) until one is
 *       published or {@code wait} seconds pass ({@value ClusterConfig.PeersConfig#MAX_LONG_POLL_SECONDS} at most),
 *       then answers with {@code LATEST} as it is then. 404 when the shard has no generation. At most
 *       {@code maxLongPolls} wait at once: more get 503 with {@code Retry-After}.</li>
 *   <li>{@code GET /replication/<shard>/manifests/gen-N.json}: the manifest bytes; 410 (with the current
 *       generation) once GC pruned it.</li>
 *   <li>{@code GET /replication/<shard>/files/<key>}: one file of a manifest by its {@code key}
 *       ({@code shards/<shard>/lucene/<file>@<crc>}, {@code …/graph/…}, {@code …/refs/…}), with a single
 *       {@code Range: bytes=…} (206 and {@code Content-Range}; 416 when unsatisfiable or not one range)
 *       or whole (200). 410 with {@code {"error", "generation"}} for a file GC pruned, so the reader
 *       re-syncs to the current {@code LATEST} instead of retrying; 503 with {@code Retry-After} above
 *       {@code maxTransfersPerWriter} concurrent transfers.</li>
 *   <li>{@code GET /replication/synonyms}: this writer's synonyms, reduced to the hand-written lines
 *       and the sections of its own shards (P5-R4), with {@code ETag} = their SHA-256 (304 on a
 *       matching {@code If-None-Match}); 404 when there are none.</li>
 *   <li>{@code GET /replication/peers}: {@code {"writer": node, "peers": [url, …]}}, the writers this one
 *       knows (its advertise URL and its seeds): seeds-mode readers gossip through it.</li>
 *   <li>{@code POST}/{@code GET /replication/readers}, {@code DELETE /replication/readers/<node>} and
 *       {@code GET}/{@code PUT /replication/status-state}: reader membership and the status hysteresis
 *       state without a bucket ({@link MembershipRoutes}, P5-R5, P5-R2).</li>
 * </ul>
 *
 * <p><b>Drain</b> ({@link #drain}, a shutdown step of the writer): every new request is answered 503
 * with {@code Retry-After}, waiting long-polls are woken, and transfers already streaming finish,
 * within {@code cluster.drainSeconds}.
 */
public final class ReplicationEndpoint {

    private static final Logger log = LoggerFactory.getLogger(ReplicationEndpoint.class);
    public static final String PREFIX = "/replication";
    /** Seconds a reader is asked to wait before retrying a 503. */
    public static final int RETRY_AFTER_SECONDS = 2;
    private static final Pattern MANIFEST_NAME = Pattern.compile("gen-(\\d{1,18})\\.json");
    private static final Pattern SINGLE_RANGE = Pattern.compile("bytes=(\\d{0,19})-(\\d{0,19})");
    private static final List<String> DATA_DIRS = List.of("lucene/", "graph/", "refs/");
    private static final int COPY_BUFFER = 64 * 1024;

    private final PeerStore peer;
    private final String writerNode;
    private final Predicate<String> token;
    private final TransferLimiter transfers;
    private final TransferLimiter longPolls;
    private final ObjectMapper mapper = new ObjectMapper();
    private final java.util.function.Supplier<List<String>> peers;
    /** Set by {@link #drain}: new requests get 503. */
    private volatile boolean draining;
    /**
     * Answers woken long-polls: never the publishing thread (which signals) nor the timeout thread,
     * so a slow reader connection cannot hold up a publish.
     */
    private final java.util.concurrent.ExecutorService answers =
            java.util.concurrent.Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("pharos-replication-longpoll-", 0).factory());

    public ReplicationEndpoint(PeerStore peer, String writerNode, Predicate<String> token,
                               int maxTransfers, int maxLongPolls) {
        this(peer, writerNode, token, maxTransfers, maxLongPolls, List::of);
    }

    /** @param peers the writer URLs {@code /replication/peers} lists */
    public ReplicationEndpoint(PeerStore peer, String writerNode, Predicate<String> token,
                               int maxTransfers, int maxLongPolls, java.util.function.Supplier<List<String>> peers) {
        this(peer, writerNode, token, maxTransfers, maxLongPolls, peers,
                new PeerReaderRegistry(java.time.Duration.ofSeconds(DEFAULT_READER_TTL_SECONDS), System::nanoTime),
                new PeerStatusState(), System::currentTimeMillis);
    }

    /** The reader TTL of an endpoint built without one ({@code cluster.readerTtlSeconds}' default). */
    static final int DEFAULT_READER_TTL_SECONDS = 30;

    /**
     * @param readers     the reader registry {@code /replication/readers} feeds and lists
     * @param statusState the status hysteresis state this writer holds when it is the lowest-id writer
     * @param wallMillis  this writer's clock as {@code /replication/readers} reports it ({@code now})
     */
    public ReplicationEndpoint(PeerStore peer, String writerNode, Predicate<String> token,
                               int maxTransfers, int maxLongPolls, java.util.function.Supplier<List<String>> peers,
                               PeerReaderRegistry readers, PeerStatusState statusState,
                               java.util.function.LongSupplier wallMillis) {
        this.membership = new MembershipRoutes(writerNode, readers, statusState, wallMillis);
        this.readers = readers;
        this.statusState = statusState;
        this.peers = Objects.requireNonNull(peers, "peers");
        this.peer = Objects.requireNonNull(peer, "peer");
        this.writerNode = Objects.requireNonNull(writerNode, "writerNode");
        this.token = Objects.requireNonNull(token, "token");
        this.transfers = new TransferLimiter(maxTransfers);
        this.longPolls = new TransferLimiter(maxLongPolls);
    }

    private final MembershipRoutes membership;
    private final PeerReaderRegistry readers;
    private final PeerStatusState statusState;

    /** The readers that heartbeat to this writer ({@code /replication/readers}). */
    public PeerReaderRegistry readers() { return readers; }

    /** The status hysteresis state held here ({@code /replication/status-state}). */
    public PeerStatusState statusState() { return statusState; }

    /** The file-transfer cap ({@code cluster.peers.maxTransfersPerWriter}). */
    public TransferLimiter transfers() { return transfers; }

    /** The long-poll cap ({@code cluster.peers.maxLongPolls}). */
    public TransferLimiter longPolls() { return longPolls; }

    /** A refused request; thrown so the web server's after-handlers (drain accounting) still run. */
    private static final class Rejected extends RuntimeException {
        final int status;
        final String error;

        Rejected(int status, String error) {
            super(error, null, false, false);
            this.status = status;
            this.error = error;
        }
    }

    public void mount(Javalin app) {
        app.exception(Rejected.class, (e, ctx) -> {
            ctx.status(e.status).contentType("application/json");
            ctx.result("{\"error\":\"" + e.error + "\"}");
        });
        app.before(PREFIX + "/*", ctx -> {
            requireToken(ctx);
            if (draining) {
                ctx.header("Retry-After", Integer.toString(RETRY_AFTER_SECONDS));
                throw new Rejected(503, "writer is shutting down");
            }
        });
        // Each route checks again: no path the router matches can ever reach a handler without the token.
        app.get(PREFIX + "/catalog", ctx -> { requireToken(ctx); catalog(ctx); });
        app.get(PREFIX + "/synonyms", ctx -> { requireToken(ctx); synonyms(ctx); });
        app.get(PREFIX + "/peers", ctx -> { requireToken(ctx); peers(ctx); });
        app.get(PREFIX + "/{shard}/latest", ctx -> { requireToken(ctx); latest(ctx); });
        app.get(PREFIX + "/{shard}/manifests/{name}", ctx -> { requireToken(ctx); manifest(ctx); });
        app.get(PREFIX + "/{shard}/files/<key>", ctx -> { requireToken(ctx); file(ctx); });
        membership.mount(app, this::requireToken); // readers and status-state (Task 2b)
    }

    private void requireToken(Context ctx) {
        if (!token.test(ctx.header(InternalApi.TOKEN_HEADER))) throw new Rejected(401, "unauthorized");
    }

    /** Wakes every waiting long-poll (each answers with the current generation). */
    public void close() {
        peer.signal().close(); // the answer executor needs no shutdown: virtual threads, none idle
        java.util.concurrent.ScheduledExecutorService w;
        synchronized (this) {
            w = watch;
            watch = null;
            watchClosed = true;
        }
        if (w != null) w.shutdownNow();
    }

    /**
     * How often {@code LATEST} of the shards with waiting long-polls is re-read. Every publish to a
     * peer-mode writer comes from another process ({@code pharos index} or {@code pharos publish}; the
     * web daemon does not index), and only in-process publishes signal, so without this watch a
     * long-poll would learn of a new generation only when its wait ran out.
     */
    static final long WATCH_MILLIS = 500;

    // Guarded by this; started by the first long-poll that waits.
    private java.util.concurrent.ScheduledExecutorService watch;
    private boolean watchClosed;

    private synchronized void startWatch() {
        if (watch != null || watchClosed) return;
        watch = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-replication-watch");
            t.setDaemon(true);
            return t;
        });
        watch.scheduleWithFixedDelay(this::watchLatest, WATCH_MILLIS, WATCH_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** Signals the shards whose {@code LATEST} on disk is newer than some waiter's {@code after}. */
    void watchLatest() {
        for (ShardId shard : peer.signal().waitingShards()) {
            try {
                Optional<Generation> gen = peer.source().latest(shard);
                if (gen.isPresent()) peer.signal().published(shard, gen.get());
            } catch (IOException | RuntimeException e) {
                log.debug("watching LATEST of shard {} failed: {}", shard, e.toString());
            }
        }
    }

    static final long LAST_CHUNK_GRACE_MILLIS = 200;

    /** True once {@link #drain} began. */
    public boolean draining() { return draining; }

    /**
     * The writer's shutdown step: refuses new {@code /replication} requests (503, {@code Retry-After}),
     * wakes the long-polls (they answer with the current generation and readers back off), and waits
     * until no transfer is in flight or {@code max} passed. Idempotent.
     *
     * @return the transfers still in flight when it returned (0 when drained)
     */
    public int drain(java.time.Duration max) {
        draining = true;
        close();
        long deadline = System.nanoTime() + max.toNanos();
        int busy;
        while ((busy = transfers.inFlight()) > 0 && System.nanoTime() - deadline < 0) {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (busy > 0) {
            log.warn("Replication drain: stopping with {} transfer(s) still in flight (cluster.drainSeconds)", busy);
        } else {
            // a transfer frees its slot just before writing its last chunk: let that chunk go out
            try {
                Thread.sleep(LAST_CHUNK_GRACE_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("Replication drained: no transfer in flight");
        }
        return busy;
    }

    // ── routes ────────────────────────────────────────────────────────────────

    private void catalog(Context ctx) throws IOException {
        List<Map<String, Object>> shards = new ArrayList<>();
        for (ShardId s : peer.catalog().shards()) {
            OptionalLong latest;
            try {
                latest = currentGeneration(s);
            } catch (IOException e) { // one unreadable LATEST must not hide the writer's other shards
                log.warn("catalog: skipping shard {}: {}", s, e.getMessage());
                continue;
            }
            if (latest.isEmpty()) continue; // registered, never committed: readers skip it
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("shard", s.value());
            row.put("latest", latest.getAsLong());
            shards.add(row);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("writer", writerNode);
        out.put("shards", shards);
        json(ctx, 200, out);
    }

    private void synonyms(Context ctx) throws IOException {
        Optional<byte[]> stored = peer.source().synonyms();
        List<String> own = new ArrayList<>();
        for (ShardId s : peer.catalog().shards()) own.add(s.value());
        String text = stored.map(b -> SynonymsMerge.retain(new String(b, StandardCharsets.UTF_8), own)).orElse("");
        if (text.isBlank()) {
            error(ctx, 404, "no synonyms on this writer");
            return;
        }
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        String etag = "\"" + sha256Hex(bytes) + "\"";
        ctx.header("ETag", etag);
        if (etag.equals(ctx.header("If-None-Match"))) {
            ctx.status(304);
            return;
        }
        ctx.status(200).contentType("text/plain; charset=utf-8").result(bytes);
    }

    private void peers(Context ctx) throws IOException {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("writer", writerNode);
        out.put("peers", List.copyOf(peers.get()));
        json(ctx, 200, out);
    }

    private void latest(Context ctx) throws IOException {
        ShardId shard = shard(ctx);
        long wait;
        long after;
        try {
            wait = longParam(ctx, "wait", 0);
            after = longParam(ctx, "after", 0);
        } catch (NumberFormatException e) {
            error(ctx, 400, "wait and after must be non-negative integers");
            return;
        }
        wait = Math.min(wait, ClusterConfig.PeersConfig.MAX_LONG_POLL_SECONDS);
        OptionalLong now = currentGeneration(shard);
        if (wait == 0 || (now.isPresent() && now.getAsLong() > after)) {
            answerLatest(ctx, shard, now);
            return;
        }
        Optional<TransferLimiter.Permit> permit = longPolls.tryAcquire();
        if (permit.isEmpty()) {
            busy(ctx, "too many long-polls on this writer (max " + longPolls.max() + ")");
            return;
        }
        CompletableFuture<Void> signalled = null;
        try {
            startWatch();
            signalled = peer.signal().awaitNewer(shard, after);
            // A publish between the read above and registering would otherwise be missed until the timeout.
            OptionalLong again = currentGeneration(shard);
            if (again.isPresent() && again.getAsLong() > after) signalled.complete(null);
        } catch (IOException | RuntimeException e) {
            if (signalled != null) signalled.cancel(false); // unregisters the waiter
            permit.get().close();
            throw e;
        }
        CompletableFuture<Void> waiting = signalled.completeOnTimeout(null, wait, TimeUnit.SECONDS);
        // Only this (authenticated) request's connection may stay silent for the wait; restored before answering.
        IdleTimeout idle = IdleTimeout.extend(ctx, TimeUnit.SECONDS.toMillis(wait) + IDLE_MARGIN_MILLIS);
        ctx.future(() -> waiting.handleAsync((v, e) -> {
            idle.restore();
            try {
                answerLatest(ctx, shard, currentGeneration(shard));
            } catch (IOException | RuntimeException ex) {
                log.warn("long-poll of shard {} failed: {}", shard, ex.toString());
                ctx.status(500).contentType("application/json").result("{\"error\":\"internal error\"}");
            } finally {
                permit.get().close();
            }
            return null;
        }, answers));
    }

    /** The most peer readers one {@code latest} answer lists. */
    static final int MAX_LISTED_PEERS = 4;

    /** Idle allowance on top of a long-poll's wait. */
    static final long IDLE_MARGIN_MILLIS = 15_000;

    /**
     * The idle timeout of one request's connection (Jetty's end point), raised for a long-poll and
     * put back afterwards so a kept-alive connection gets the connector's default again. A no-op
     * outside Jetty.
     */
    private record IdleTimeout(org.eclipse.jetty.io.EndPoint endPoint, long previous) {
        static IdleTimeout extend(Context ctx, long millis) {
            org.eclipse.jetty.server.Request base = org.eclipse.jetty.server.Request.getBaseRequest(ctx.req());
            org.eclipse.jetty.io.EndPoint ep = base == null || base.getHttpChannel() == null
                    ? null : base.getHttpChannel().getEndPoint();
            if (ep == null) return new IdleTimeout(null, 0);
            long previous = ep.getIdleTimeout();
            if (previous > 0 && previous < millis) ep.setIdleTimeout(millis); // 0 = no timeout: keep it
            return new IdleTimeout(ep, previous);
        }

        void restore() {
            if (endPoint != null) endPoint.setIdleTimeout(previous);
        }
    }

    private void manifest(Context ctx) throws IOException {
        ShardId shard = shard(ctx);
        String name = ctx.pathParam("name");
        Matcher m = MANIFEST_NAME.matcher(name);
        if (!m.matches()) {
            error(ctx, 400, "manifest name must be gen-N.json");
            return;
        }
        OptionalLong current = currentGeneration(shard);
        if (current.isEmpty()) {
            error(ctx, 404, "no generation of shard " + shard + " on this writer");
            return;
        }
        Generation gen = new Generation(Long.parseLong(m.group(1)));
        Optional<byte[]> bytes = peer.store().get(peer.layout().manifestKey(shard, gen)).map(o -> o.data());
        if (bytes.isEmpty()) {
            gone(ctx, "manifest " + name + " of shard " + shard + " was pruned", current.getAsLong());
            return;
        }
        ctx.status(200).contentType("application/json").result(bytes.get());
    }

    private void file(Context ctx) throws IOException {
        ShardId shard = shard(ctx);
        String key = ctx.pathParam("key");
        if (!validFileKey(peer.layout(), shard, key)) {
            error(ctx, 400, "not a file key of shard " + shard);
            return;
        }
        OptionalLong current = currentGeneration(shard);
        if (current.isEmpty()) {
            error(ctx, 404, "no generation of shard " + shard + " on this writer");
            return;
        }
        Optional<TransferLimiter.Permit> permit = transfers.tryAcquire();
        if (permit.isEmpty()) {
            transfersRefused.incrementAndGet();
            busy(ctx, "too many transfers on this writer (max " + transfers.max() + ")");
            return;
        }
        peakTransfers.accumulateAndGet(transfers.inFlight(), Math::max);
        try (TransferLimiter.Permit held = permit.get()) {
            Path path = peer.store().pathOf(key);
            FileChannel ch;
            try {
                ch = FileChannel.open(path, StandardOpenOption.READ);
            } catch (NoSuchFileException e) {
                gone(ctx, "file " + key + " was pruned; re-sync to the current generation", current.getAsLong());
                return;
            }
            // Open: GC may unlink the file now, the bytes stay readable until the channel closes.
            try (ch) {
                transfersServed.incrementAndGet();
                bytesServed.addAndGet(serve(ctx, ch.size(), ch::read, ctx.header("Range"), held));
            }
        }
    }

    /** File transfers this writer started (whole or ranged), for tests and metrics. */
    private final java.util.concurrent.atomic.AtomicLong transfersServed = new java.util.concurrent.atomic.AtomicLong();

    /** The file transfers ({@code /files/<key>}) this writer has started serving. */
    public long transfersServed() { return transfersServed.get(); }

    private final java.util.concurrent.atomic.AtomicLong transfersRefused = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong bytesServed = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicInteger peakTransfers = new java.util.concurrent.atomic.AtomicInteger();

    /**
     * Counters for {@code /health} ({@code peers} on a peer-pull writer): {@code maxTransfers}
     * ({@code cluster.peers.maxTransfersPerWriter}), {@code transfersInFlight} and its highest value
     * so far ({@code peakTransfersInFlight}), {@code transfersServed}, {@code transfersRefused} (503 at
     * the cap), {@code bytesServed} (file bytes written), and {@code longPolls}, the {@code latest}
     * long-polls held now.
     */
    public Map<String, Object> stats() {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("maxTransfers", transfers.max());
        out.put("transfersInFlight", transfers.inFlight());
        out.put("peakTransfersInFlight", peakTransfers.get());
        out.put("transfersServed", transfersServed.get());
        out.put("transfersRefused", transfersRefused.get());
        out.put("bytesServed", bytesServed.get());
        out.put("longPolls", longPolls.inFlight());
        return out;
    }

    /**
     * A manifest file key of {@code shard} that {@code /files/<key>} may serve: under the shard's
     * prefix, in {@code lucene/}, {@code graph/} or {@code refs/}, and made of plain segments only
     * ({@link FsObjectStore#validKey}), so it cannot name anything outside the data dirs (no
     * traversal, no manifests, no {@code LATEST}). Shared by the reader's {@link PeerFileSource}.
     */
    static boolean validFileKey(ObjectStoreLayout layout, ShardId shard, String key) {
        if (key == null) return false;
        String shardPrefix = layout.shardPrefix(shard);
        return key.startsWith(shardPrefix) && FsObjectStore.validKey(key)
                && DATA_DIRS.stream().anyMatch(key.substring(shardPrefix.length())::startsWith);
    }

    /** Positional reads of the bytes being served: a file channel, or cached blocks. */
    @FunctionalInterface
    interface Positional {
        /** Reads up to {@code buf.remaining()} bytes at {@code pos}; -1 past the end. */
        int read(java.nio.ByteBuffer buf, long pos) throws IOException;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Writes the requested bytes. {@code slot} is given back right before the last chunk is written:
     * the response completes inside that write, and a reader that issues its next range as soon as it
     * has this one must find the slot free (not a spurious 503).
     *
     * @return the bytes written (fewer than asked when the reader went away)
     */
    static long serve(Context ctx, long size, Positional ch, String range, TransferLimiter.Permit slot)
            throws IOException {
        long start = 0;
        long end = size - 1; // inclusive
        int status = 200;
        if (range != null) {
            long[] r = parseRange(range, size);
            if (r == null) {
                ctx.header("Content-Range", "bytes */" + size);
                ctx.status(416).contentType("application/json").result("{\"error\":\"range not satisfiable (one"
                        + " bytes=start-end range within " + size + " bytes)\"}");
                return 0;
            }
            start = r[0];
            end = r[1];
            status = 206;
            ctx.header("Content-Range", "bytes " + start + "-" + end + "/" + size);
        }
        long length = end - start + 1;
        if (length == 0) slot.close(); // an empty file: no chunk to write
        ctx.status(status);
        ctx.header("Accept-Ranges", "bytes");
        ctx.res().setContentType("application/octet-stream");
        ctx.res().setContentLengthLong(length);
        OutputStream raw = ctx.res().getOutputStream(); // not Javalin's (compressing) stream: exact bytes
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate((int) Math.min(COPY_BUFFER, Math.max(1, length)));
        long pos = start;
        long remaining = length;
        try {
            while (remaining > 0) {
                buf.clear().limit((int) Math.min(buf.capacity(), remaining));
                int n = ch.read(buf, pos);
                if (n < 0) throw new IOException("file shrank while serving it");
                if (n == remaining) slot.close();
                raw.write(buf.array(), 0, n);
                pos += n;
                remaining -= n;
            }
            raw.flush();
        } catch (IOException e) { // the reader went away mid-transfer; it retries the range
            log.debug("transfer aborted after {} of {} bytes: {}", length - remaining, length, e.toString());
        }
        return length - remaining;
    }

    /**
     * {@code [start, endInclusive]} of a single {@code bytes=a-b}, {@code bytes=a-} or {@code bytes=-n}
     * range clamped to {@code size}; null when it is malformed, not a single range or unsatisfiable.
     */
    static long[] parseRange(String header, long size) {
        Matcher m = SINGLE_RANGE.matcher(header.strip());
        if (!m.matches()) return null;
        String a = m.group(1);
        String b = m.group(2);
        try {
            if (a.isEmpty()) {
                if (b.isEmpty()) return null;
                long suffix = Long.parseLong(b);
                if (suffix == 0 || size == 0) return null;
                return new long[]{Math.max(0, size - suffix), size - 1};
            }
            long start = Long.parseLong(a);
            if (start >= size) return null;
            long end = b.isEmpty() ? size - 1 : Long.parseLong(b);
            if (end < start) return null;
            return new long[]{start, Math.min(end, size - 1)};
        } catch (NumberFormatException e) { // beyond a long
            return null;
        }
    }

    private void answerLatest(Context ctx, ShardId shard, OptionalLong gen) throws IOException {
        if (gen.isEmpty()) {
            error(ctx, 404, "no generation of shard " + shard + " on this writer");
            return;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("generation", gen.getAsLong());
        out.put("manifest", ObjectStoreLayout.manifestName(new Generation(gen.getAsLong())));
        // readers that hold this generation in full (their heartbeats' generationsHeld): readers
        // fetch from them first and verify every byte against the manifest (Task 3)
        out.put("peers", readers.holders(shard.value(), gen.getAsLong(), MAX_LISTED_PEERS));
        json(ctx, 200, out);
    }

    /** The validated {@code LATEST} of {@code shard}; empty when there is none. */
    private OptionalLong currentGeneration(ShardId shard) throws IOException {
        Optional<Generation> g = peer.source().latest(shard);
        return g.isPresent() ? OptionalLong.of(g.get().value()) : OptionalLong.empty();
    }

    private static ShardId shard(Context ctx) {
        try {
            return new ShardId(ctx.pathParam("shard"));
        } catch (IllegalArgumentException e) {
            throw new Rejected(400, "invalid shard id");
        }
    }

    private static long longParam(Context ctx, String name, long dflt) {
        String v = ctx.queryParam(name);
        if (v == null || v.isBlank()) return dflt;
        long n = Long.parseLong(v.strip());
        if (n < 0) throw new NumberFormatException(name + " is negative");
        return n;
    }

    private void gone(Context ctx, String message, long generation) throws IOException {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("error", message);
        out.put("generation", generation);
        out.put("manifest", ObjectStoreLayout.manifestName(new Generation(generation)));
        json(ctx, 410, out);
    }

    private void busy(Context ctx, String message) throws IOException {
        ctx.header("Retry-After", Integer.toString(RETRY_AFTER_SECONDS));
        error(ctx, 503, message);
    }

    private void error(Context ctx, int status, String message) throws IOException {
        json(ctx, status, Map.of("error", message));
    }

    private void json(Context ctx, int status, Object body) throws IOException {
        ctx.status(status).contentType("application/json").result(mapper.writeValueAsBytes(body));
    }

    private static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
