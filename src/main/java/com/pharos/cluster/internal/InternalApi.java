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

    private final ShardSearchService service;
    private final byte[] tokenDigest;
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** @throws IllegalArgumentException when {@code token} is null or blank: a reader never starts open */
    public InternalApi(ShardSearchService service, String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("cluster.token is required to serve /internal/* (shared secret of the cluster)");
        }
        this.service = Objects.requireNonNull(service, "service");
        this.tokenDigest = sha256(token);
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
            }
        });
        app.post("/internal/term-stats", ctx -> handle(ctx, TermStatsRequest.class, TermStatsRequest::shard, null,
                false, service::termStats));
        app.post("/internal/shard-search", ctx -> handle(ctx, ShardSearchRequest.class, ShardSearchRequest::shard,
                ShardSearchRequest::generation, false, service::searchRaw));
        app.post("/internal/snippets", ctx -> handle(ctx, SnippetsRequest.class, SnippetsRequest::shard,
                SnippetsRequest::generation, true, service::snippets));
        app.post("/internal/callees", ctx -> handle(ctx, CalleesRequest.class, CalleesRequest::shard,
                CalleesRequest::generation, true, service::callees));
    }

    @FunctionalInterface
    private interface Work<Q, R> { R apply(Q q) throws Exception; }

    private <Q, R> void handle(Context ctx, Class<Q> type, Function<Q, String> shardOf,
                                Function<Q, Long> generationOf, boolean generationRequired, Work<Q, R> work) throws Exception {
        Q req;
        ShardId shard;
        try {
            req = mapper.readValue(ctx.bodyAsBytes(), type);
            shard = new ShardId(shardOf.apply(req));
        } catch (Exception e) { // malformed JSON, missing or invalid shard id
            error(ctx, 400, "bad request: " + e.getClass().getSimpleName());
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
            ctx.status(200).contentType("application/json").result(mapper.writeValueAsBytes(work.apply(req)));
        } catch (Exception e) {
            log.warn("internal call on shard '{}' failed: {}", shard, e.toString());
            error(ctx, 500, "internal error");
        }
    }

    private void error(Context ctx, int status, String message) throws Exception {
        ctx.status(status).contentType("application/json").result(mapper.writeValueAsBytes(Map.of("error", message)));
    }
}
