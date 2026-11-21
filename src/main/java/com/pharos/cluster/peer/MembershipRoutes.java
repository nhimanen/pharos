package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The membership routes of {@link ReplicationEndpoint} (Task 2b), mounted under its token filter and
 * each checking the token again (P5-R3):
 * <ul>
 *   <li>{@code POST /replication/readers}: a reader's {@code ReaderHeartbeat} JSON, unchanged. 204;
 *       400 for a malformed body; 409 for a heartbeat of a process that withdrew; 503 when the
 *       registry is full.</li>
 *   <li>{@code GET /replication/readers}: {@code {"writer", "now", "ttlSeconds", "uptimeMillis", "readers":
 *       [{"ageMillis", "heartbeat"}], "withdrawn": [{"node", "startedAt"}]}}, the live readers by this writer's clock ({@code now}: its epoch
 *       millis, the clock {@code /cluster/status} dates a peer-mode snapshot by; {@code uptimeMillis}: how
 *       long its registry has been listening).</li>
 *   <li>{@code DELETE /replication/readers/<node>?startedAt=<millis>}: the reader process withdraws. 204.</li>
 *   <li>{@code GET} and {@code PUT /replication/status-state}: {@link PeerStatusState}.</li>
 * </ul>
 */
final class MembershipRoutes {

    static final String READERS = ReplicationEndpoint.PREFIX + "/readers";

    private final String writerNode;
    private final PeerReaderRegistry readers;
    private final PeerStatusState statusState;
    private final LongSupplier wallMillis;
    private final ObjectMapper mapper = new ObjectMapper();

    MembershipRoutes(String writerNode, PeerReaderRegistry readers, PeerStatusState statusState,
                     LongSupplier wallMillis) {
        this.writerNode = Objects.requireNonNull(writerNode, "writerNode");
        this.readers = Objects.requireNonNull(readers, "readers");
        this.statusState = Objects.requireNonNull(statusState, "statusState");
        this.wallMillis = Objects.requireNonNull(wallMillis, "wallMillis");
    }

    void mount(Javalin app, Consumer<Context> requireToken) {
        app.post(READERS, ctx -> { requireToken.accept(ctx); postReader(ctx); });
        app.get(READERS, ctx -> { requireToken.accept(ctx); listReaders(ctx); });
        app.delete(READERS + "/{node}", ctx -> { requireToken.accept(ctx); deleteReader(ctx); });
        app.get(PeerStatusState.PATH, ctx -> { requireToken.accept(ctx); getState(ctx); });
        app.put(PeerStatusState.PATH, ctx -> { requireToken.accept(ctx); putState(ctx); });
    }

    private void postReader(Context ctx) throws Exception {
        byte[] body = boundedBody(ctx, PeerReaderRegistry.MAX_BODY_BYTES);
        if (body == null) return;
        ObjectNode hb;
        try {
            hb = PeerReaderRegistry.parse(body);
        } catch (IllegalArgumentException e) {
            error(ctx, 400, e.getMessage());
            return;
        }
        switch (readers.accept(hb)) {
            case ACCEPTED -> ctx.status(204);
            case WITHDRAWN -> error(ctx, 409, "this reader process withdrew from membership");
            case FULL -> {
                ctx.header("Retry-After", Integer.toString(ReplicationEndpoint.RETRY_AFTER_SECONDS));
                error(ctx, 503, "reader registry full (max " + PeerReaderRegistry.MAX_READERS + ")");
            }
        }
    }

    private void listReaders(Context ctx) throws Exception {
        ObjectNode out = mapper.createObjectNode();
        out.put("writer", writerNode);
        out.put("now", wallMillis.getAsLong());
        out.put("ttlSeconds", readers.ttlSeconds());
        out.put("uptimeMillis", readers.uptimeMillis());
        ArrayNode rows = out.putArray("readers");
        for (PeerReaderRegistry.Entry e : readers.live()) {
            ObjectNode row = rows.addObject();
            row.put("ageMillis", e.ageMillis());
            row.set("heartbeat", e.raw());
        }
        ArrayNode gone = out.putArray("withdrawn");
        readers.withdrawn().forEach((node, startedAt) -> gone.addObject().put("node", node).put("startedAt", startedAt));
        ctx.status(200).contentType("application/json").result(mapper.writeValueAsBytes(out));
    }

    private void deleteReader(Context ctx) throws Exception {
        String node = ctx.pathParam("node");
        if (!PeerReaderRegistry.validNode(node)) {
            error(ctx, 400, "invalid node id");
            return;
        }
        long startedAt;
        try { // required: without it every process of the node, a restarted one too, would be tombstoned
            String v = ctx.queryParam("startedAt");
            if (v == null) throw new NumberFormatException("missing");
            startedAt = Long.parseLong(v.strip());
        } catch (NumberFormatException e) {
            error(ctx, 400, "startedAt (the withdrawing process's start, epoch millis) must be an integer");
            return;
        }
        readers.withdraw(node, startedAt);
        ctx.status(204);
    }

    private void getState(Context ctx) throws Exception {
        Optional<PeerStatusState.Held> h = statusState.get();
        if (h.isEmpty()) {
            error(ctx, 404, "no status state on this writer");
            return;
        }
        ctx.header("ETag", h.get().etag());
        ctx.status(200).contentType("application/json").result(h.get().json());
    }

    private void putState(Context ctx) throws Exception {
        String ifMatch = ctx.header("If-Match");
        String ifNoneMatch = ctx.header("If-None-Match");
        if ((ifMatch == null) == (ifNoneMatch == null) || (ifNoneMatch != null && !ifNoneMatch.strip().equals("*"))) {
            error(ctx, 428, "send If-Match: <etag> or If-None-Match: *");
            return;
        }
        byte[] body = boundedBody(ctx, PeerStatusState.MAX_BODY_BYTES);
        if (body == null) return;
        Optional<String> etag;
        try {
            etag = statusState.put(body, ifMatch == null ? null : ifMatch.strip());
        } catch (IllegalArgumentException e) {
            error(ctx, 400, e.getMessage());
            return;
        }
        if (etag.isEmpty()) {
            error(ctx, 412, "the status state changed; re-read it");
            return;
        }
        ctx.header("ETag", etag.get());
        ctx.status(200).contentType("application/json").result("{}");
    }

    /** The body, or null after answering 413 when it is larger than {@code max}. */
    private byte[] boundedBody(Context ctx, int max) throws Exception {
        long declared = ctx.req().getContentLengthLong();
        if (declared > max) {
            error(ctx, 413, "body larger than " + max + " bytes");
            return null;
        }
        byte[] body = ctx.req().getInputStream().readNBytes(max + 1);
        if (body.length > max) {
            error(ctx, 413, "body larger than " + max + " bytes");
            return null;
        }
        return body;
    }

    private void error(Context ctx, int status, String message) throws Exception {
        ctx.status(status).contentType("application/json").result(mapper.writeValueAsBytes(Map.of("error", message)));
    }
}
