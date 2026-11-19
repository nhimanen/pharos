package com.pharos.cluster.internal;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.ShardId;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Function;

/**
 * The reader-only {@code /internal/*} endpoints. Every one requires {@code X-Pharos-Cluster-Token}
 * (compared in constant time); a missing or wrong token gets 401 from a before-filter, so no handler
 * runs. Mounted only on reader nodes (see {@code WebServer#setInternalApi}).
 */
public final class InternalApi {

    public static final String TOKEN_HEADER = "X-Pharos-Cluster-Token";
    private static final Logger log = LoggerFactory.getLogger(InternalApi.class);

    /** Largest request body (bytes) and collection sizes accepted on {@code /internal/*}. */
    public static final int MAX_BODY_BYTES = 4 << 20;
    public static final int MAX_ITEMS = 10_000;
    public static final int MAX_VECTOR_LENGTH = 4096;
    /** The {@code error} of a 409 refusing a request analysed with other synonyms. */
    public static final String SYNONYMS_MISMATCH = "synonyms mismatch";
    /** The {@code error} of a 409 refusing a vector request the shard has no vectors for. */
    public static final String MODEL_MISMATCH = "model mismatch";

    /** Null until the shard search is wired: authenticated calls then get 503 and fail over. */
    private volatile ShardSearchService service;
    private final byte[] tokenDigest;
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** @throws IllegalArgumentException when {@code token} is null or blank: a reader never starts open */
    public InternalApi(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("cluster.token is required to serve /internal/* (shared secret of the cluster)");
        }
        this.tokenDigest = sha256(token);
    }

    /** Wires the work behind the endpoints; until then they answer 503 after authenticating. */
    public InternalApi service(ShardSearchService service) {
        this.service = Objects.requireNonNull(service, "service");
        return this;
    }

    private static byte[] sha256(String s) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Hashing first makes the comparison length-independent as well as constant-time. */
    private boolean authorized(Context ctx) {
        String presented = ctx.header(TOKEN_HEADER);
        if (presented == null) return false;
        return MessageDigest.isEqual(tokenDigest, sha256(presented));
    }

    public void mount(Javalin app) {
        app.before("/internal/*", ctx -> {
            if (!authorized(ctx)) {
                ctx.status(401).contentType("application/json").result("{\"error\":\"unauthorized\"}");
                ctx.skipRemainingHandlers();
            } else if (ctx.contentLength() > MAX_BODY_BYTES) {
                ctx.status(413).contentType("application/json").result("{\"error\":\"request too large\"}");
                ctx.skipRemainingHandlers();
            } else if (service == null) {
                ctx.status(503).contentType("application/json").result("{\"error\":\"shard search not available\"}");
                ctx.skipRemainingHandlers();
            }
        });
        app.post("/internal/term-stats", ctx -> handle(ctx, TermStatsRequest.class, TermStatsRequest::shard, null,
                false, TermStatsResponse::generation, InternalApi::check, r -> service.termStats(r)));
        app.post("/internal/shard-search", ctx -> handle(ctx, ShardSearchRequest.class, ShardSearchRequest::shard,
                ShardSearchRequest::generation, false, ShardSearchResponse::generation, InternalApi::check,
                r -> service.searchRaw(r)));
        app.post("/internal/snippets", ctx -> handle(ctx, SnippetsRequest.class, SnippetsRequest::shard,
                SnippetsRequest::generation, true, SnippetsResponse::generation, InternalApi::check,
                r -> service.snippets(r)));
        app.post("/internal/callees", ctx -> handle(ctx, CalleesRequest.class, CalleesRequest::shard,
                CalleesRequest::generation, true, CalleesResponse::generation, InternalApi::check,
                r -> service.callees(r)));
        app.post("/internal/graph", ctx -> handle(ctx, GraphRequest.class, GraphRequest::shard,
                GraphRequest::generation, false, GraphResponse::generation, InternalApi::check,
                r -> service.graph(r)));
    }

    @FunctionalInterface
    private interface Work<Q, R> { R apply(Q q) throws Exception; }

    private <Q, R> void handle(Context ctx, Class<Q> type, Function<Q, String> shardOf,
                                Function<Q, Long> generationOf, boolean generationRequired,
                                java.util.function.ToLongFunction<R> responseGeneration, Function<Q, String> validator,
                                Work<Q, R> work) throws Exception {
        Q req;
        ShardId shard;
        try {
            byte[] raw = ctx.bodyAsBytes();
            if (raw.length > MAX_BODY_BYTES) {
                error(ctx, 413, "request too large");
                return;
            }
            req = mapper.readValue(raw, type);
            shard = new ShardId(shardOf.apply(req));
        } catch (Exception e) { // malformed JSON, missing or invalid shard id
            error(ctx, 400, "bad request: " + e.getClass().getSimpleName());
            return;
        }
        String invalid = validator.apply(req);
        if (invalid != null) {
            error(ctx, 400, invalid);
            return;
        }
        Long wanted = generationOf == null ? null : generationOf.apply(req);
        if (generationRequired && wanted == null) {
            error(ctx, 400, "generation is required");
            return;
        }
        OptionalLong served = service.servedGeneration(shard);
        if (served.isEmpty()) {
            error(ctx, 404, "shard not served here: " + shard);
            return;
        }
        if (wanted != null && wanted != served.getAsLong()) {
            ctx.status(409).contentType("application/json")
                    .result(mapper.writeValueAsBytes(Map.of("error", "generation mismatch",
                            "servedGeneration", served.getAsLong())));
            return;
        }
        try {
            R response = work.apply(req);
            long read = responseGeneration.applyAsLong(response);
            if (wanted != null && wanted != read) { // a swap landed between the check above and the work
                ctx.status(409).contentType("application/json")
                        .result(mapper.writeValueAsBytes(Map.of("error", "generation mismatch", "servedGeneration", read)));
                return;
            }
            ctx.status(200).contentType("application/json").result(mapper.writeValueAsBytes(response));
        } catch (ModelMismatchException e) {
            ctx.status(409).contentType("application/json")
                    .result(mapper.writeValueAsBytes(Map.of("error", MODEL_MISMATCH, "detail", e.getMessage(),
                            "reason", e.reason().name())));
        } catch (SynonymsMismatchException e) {
            ctx.status(409).contentType("application/json")
                    .result(mapper.writeValueAsBytes(Map.of("error", SYNONYMS_MISMATCH, "detail", e.getMessage())));
        } catch (Exception e) {
            log.warn("internal call on shard '{}' failed: {}", shard, e.toString());
            error(ctx, 500, "internal error");
        }
    }

    /** Collection and vector caps; the returned text is the 400 message, null when the request is fine. */
    private static String check(Object req) {
        if (req instanceof TermStatsRequest r) {
            if (size(r.fields()) > MAX_ITEMS || size(r.terms()) > MAX_ITEMS) return "too many fields or terms";
        } else if (req instanceof ShardSearchRequest r) {
            if (r.queryVector() != null && r.queryVector().length > MAX_VECTOR_LENGTH) return "query vector too long";
            if (r.globalStats() != null && (r.globalStats().terms() != null && r.globalStats().terms().size() > MAX_ITEMS))
                return "too many terms";
        } else if (req instanceof SnippetsRequest r) {
            if (size(r.ids()) > MAX_ITEMS) return "too many ids";
            if (r.queryVector() != null && r.queryVector().length > MAX_VECTOR_LENGTH) return "query vector too long";
        } else if (req instanceof CalleesRequest r) {
            if (size(r.fqns()) > MAX_ITEMS) return "too many fqns";
        } else if (req instanceof GraphRequest r) {
            if (size(r.fqns()) > MAX_ITEMS) return "too many fqns";
            if (r.limit() < 0 || r.limit() > MAX_ITEMS) return "limit out of range";
        }
        return null;
    }

    private static int size(java.util.Collection<?> c) {
        return c == null ? 0 : c.size();
    }

    private void error(Context ctx, int status, String message) throws Exception {
        ctx.status(status).contentType("application/json").result(mapper.writeValueAsBytes(Map.of("error", message)));
    }
}
