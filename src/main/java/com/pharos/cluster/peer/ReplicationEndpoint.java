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
 *       "manifest": "gen-N.json"}}. With {@code wait > 0} and no generation newer than {@code after}
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
 * </ul>
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
    /**
     * Answers woken long-polls: never the publishing thread (which signals) nor the timeout thread,
     * so a slow reader connection cannot hold up a publish.
     */
    private final java.util.concurrent.ExecutorService answers =
            java.util.concurrent.Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("pharos-replication-longpoll-", 0).factory());

    public ReplicationEndpoint(PeerStore peer, String writerNode, Predicate<String> token,
                               int maxTransfers, int maxLongPolls) {
        this.peer = Objects.requireNonNull(peer, "peer");
        this.writerNode = Objects.requireNonNull(writerNode, "writerNode");
        this.token = Objects.requireNonNull(token, "token");
        this.transfers = new TransferLimiter(maxTransfers);
        this.longPolls = new TransferLimiter(maxLongPolls);
    }

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
        app.before(PREFIX + "/*", this::requireToken);
        // Each route checks again: no path the router matches can ever reach a handler without the token.
        app.get(PREFIX + "/catalog", ctx -> { requireToken(ctx); catalog(ctx); });
        app.get(PREFIX + "/synonyms", ctx -> { requireToken(ctx); synonyms(ctx); });
        app.get(PREFIX + "/{shard}/latest", ctx -> { requireToken(ctx); latest(ctx); });
        app.get(PREFIX + "/{shard}/manifests/{name}", ctx -> { requireToken(ctx); manifest(ctx); });
        app.get(PREFIX + "/{shard}/files/<key>", ctx -> { requireToken(ctx); file(ctx); });
    }

    private void requireToken(Context ctx) {
        if (!token.test(ctx.header(InternalApi.TOKEN_HEADER))) throw new Rejected(401, "unauthorized");
    }

    /** Wakes every waiting long-poll (each answers with the current generation). */
    public void close() {
        peer.signal().close(); // the answer executor needs no shutdown: virtual threads, none idle
    }

    // ── routes ────────────────────────────────────────────────────────────────

    private void catalog(Context ctx) throws IOException {
        List<Map<String, Object>> shards = new ArrayList<>();
        for (ShardId s : peer.catalog().shards()) {
            OptionalLong latest = currentGeneration(s);
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
        ctx.future(() -> waiting.handleAsync((v, e) -> {
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
        String shardPrefix = peer.layout().shardPrefix(shard);
        if (!key.startsWith(shardPrefix) || !FsObjectStore.validKey(key)
                || DATA_DIRS.stream().noneMatch(key.substring(shardPrefix.length())::startsWith)) {
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
            busy(ctx, "too many transfers on this writer (max " + transfers.max() + ")");
            return;
        }
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
                serve(ctx, ch, ctx.header("Range"), held);
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Writes the requested bytes. {@code slot} is given back right before the last chunk is written:
     * the response completes inside that write, and a reader that issues its next range as soon as it
     * has this one must find the slot free (not a spurious 503).
     */
    private void serve(Context ctx, FileChannel ch, String range, TransferLimiter.Permit slot) throws IOException {
        long size = ch.size();
        long start = 0;
        long end = size - 1; // inclusive
        int status = 200;
        if (range != null) {
            long[] r = parseRange(range, size);
            if (r == null) {
                ctx.header("Content-Range", "bytes */" + size);
                error(ctx, 416, "range not satisfiable (one bytes=start-end range within " + size + " bytes)");
                return;
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
