package com.pharos.cluster.internal;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.routing.ShardRouter.Route;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Calls a reader's {@code /internal/*} endpoints. {@link #call} tries the route's first candidate
 * and, after a connection error, a timeout or a 5xx, marks that reader suspect and retries once on
 * the next candidate; with none left it throws {@link ShardUnavailableException}. A 409 surfaces as
 * {@link GenerationMismatchException} and other 4xx as {@link ShardCallException}: another replica
 * would not change those, and neither marks the reader suspect.
 */
public final class ShardClient {

    /** One internal endpoint with its response type. */
    public record Endpoint<Q, R>(String path, Class<R> responseType) {}

    public static final Endpoint<TermStatsRequest, TermStatsResponse> TERM_STATS =
            new Endpoint<>("/internal/term-stats", TermStatsResponse.class);
    public static final Endpoint<ShardSearchRequest, ShardSearchResponse> SHARD_SEARCH =
            new Endpoint<>("/internal/shard-search", ShardSearchResponse.class);
    public static final Endpoint<SnippetsRequest, SnippetsResponse> SNIPPETS =
            new Endpoint<>("/internal/snippets", SnippetsResponse.class);
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final Endpoint<CalleesRequest, Map<String, List<String>>> CALLEES =
            new Endpoint<>("/internal/callees", (Class) Map.class);

    private final ReaderHealth health;
    private final String token;
    private final Duration requestTimeout;
    private final HttpClient http;
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final Map<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();

    public ShardClient(ReaderHealth health, String token, Duration connectTimeout, Duration requestTimeout) {
        this.health = health;
        this.token = token;
        this.requestTimeout = requestTimeout;
        this.http = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
    }

    /** Calls this client has open to {@code node}: the router's {@code coordinatorInFlight} input. */
    public int inFlight(String node) {
        AtomicInteger n = inFlight.get(node);
        return n == null ? 0 : n.get();
    }

    /** Calls the route's candidates in order, at most two attempts. */
    public <Q, R> R call(Route route, Endpoint<Q, R> endpoint, Function<ReaderHeartbeat, Q> request) {
        List<ReaderHeartbeat> candidates = route.candidates();
        if (candidates.isEmpty()) {
            throw new ShardUnavailableException(route.shard(), "no live reader serves it", null);
        }
        Exception last = null;
        for (int i = 0; i < Math.min(2, candidates.size()); i++) {
            ReaderHeartbeat reader = candidates.get(i);
            try {
                return callOn(reader, endpoint, request.apply(reader));
            } catch (RetryableFailure f) {
                health.markSuspect(reader.node());
                last = f;
            }
        }
        throw new ShardUnavailableException(route.shard(), last == null ? null : last.getMessage(), last);
    }

    /** One call to one reader, no retry: how a query keeps its later calls on the reader it pinned. */
    public <Q, R> R callOn(ReaderHeartbeat reader, Endpoint<Q, R> endpoint, Q request) {
        AtomicInteger counter = inFlight.computeIfAbsent(reader.node(), n -> new AtomicInteger());
        counter.incrementAndGet();
        try {
            HttpResponse<byte[]> resp;
            try {
                HttpRequest req = HttpRequest.newBuilder(URI.create(reader.advertiseUrl() + endpoint.path()))
                        .timeout(requestTimeout)
                        .header("Content-Type", "application/json")
                        .header(InternalApi.TOKEN_HEADER, token)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(request)))
                        .build();
                resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ShardCallException("interrupted calling " + reader.node());
            } catch (IOException e) { // connect error, timeout, reset
                throw new RetryableFailure(reader.node() + ": " + e);
            }
            int status = resp.statusCode();
            if (status >= 500) throw new RetryableFailure(reader.node() + " answered " + status);
            if (status == 409) {
                health.markHealthy(reader.node());
                throw new GenerationMismatchException(reader.node(), servedGeneration(resp.body()));
            }
            if (status != 200) throw new ShardCallException(reader.node() + " answered " + status + " for " + endpoint.path());
            health.markHealthy(reader.node());
            try {
                if (endpoint == CALLEES) {
                    @SuppressWarnings("unchecked")
                    R r = (R) mapper.readValue(resp.body(),
                            mapper.getTypeFactory().constructMapType(Map.class,
                                    mapper.getTypeFactory().constructType(String.class),
                                    mapper.getTypeFactory().constructCollectionType(List.class, String.class)));
                    return r;
                }
                return mapper.readValue(resp.body(), endpoint.responseType());
            } catch (IOException e) {
                throw new RetryableFailure(reader.node() + " sent an unreadable response: " + e.getMessage());
            }
        } finally {
            counter.decrementAndGet();
        }
    }

    private long servedGeneration(byte[] body) {
        try {
            JsonNode n = mapper.readTree(body);
            return n.path("servedGeneration").asLong(-1);
        } catch (IOException e) {
            return -1;
        }
    }

    /** Internal: a failure another replica may not share. {@link #callOn} callers see {@link ShardCallException} instead. */
    private static final class RetryableFailure extends ShardCallException {
        RetryableFailure(String message) { super(message); }
    }
}
