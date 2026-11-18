package com.pharos.cluster.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.function.ToLongFunction;

/**
 * Calls a reader's {@code /internal/*} endpoints. {@link #call} tries the route's first candidate
 * and, after a connection error, a timeout or a 5xx, marks that reader suspect and retries once on
 * the next candidate; with none left it throws {@link ShardUnavailableException}. A 409 surfaces as
 * {@link GenerationMismatchException} and other 4xx as {@link ShardCallException}: another replica
 * would not change those, and neither marks the reader suspect.
 */
public final class ShardClient {

    /**
     * One internal endpoint: its response type, the generation a request pins (null: none) and the
     * generation a response was read from.
     */
    public record Endpoint<Q, R>(String path, Class<R> responseType, Function<Q, Long> requestedGeneration,
                                 ToLongFunction<R> servedGeneration) {}

    public static final Endpoint<TermStatsRequest, TermStatsResponse> TERM_STATS =
            new Endpoint<>("/internal/term-stats", TermStatsResponse.class, q -> null, TermStatsResponse::generation);
    public static final Endpoint<ShardSearchRequest, ShardSearchResponse> SHARD_SEARCH =
            new Endpoint<>("/internal/shard-search", ShardSearchResponse.class, ShardSearchRequest::generation,
                    ShardSearchResponse::generation);
    public static final Endpoint<SnippetsRequest, SnippetsResponse> SNIPPETS =
            new Endpoint<>("/internal/snippets", SnippetsResponse.class, SnippetsRequest::generation,
                    SnippetsResponse::generation);
    public static final Endpoint<CalleesRequest, CalleesResponse> CALLEES =
            new Endpoint<>("/internal/callees", CalleesResponse.class, CalleesRequest::generation,
                    CalleesResponse::generation);

    /** Largest response body accepted by default. */
    public static final int DEFAULT_MAX_RESPONSE_BYTES = 64 << 20;

    private final ReaderHealth health;
    private final String token;
    private final Duration requestTimeout;
    private final int maxResponseBytes;
    private final HttpClient http;
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final Map<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();

    public ShardClient(ReaderHealth health, String token, Duration connectTimeout, Duration requestTimeout) {
        this(health, token, connectTimeout, requestTimeout, DEFAULT_MAX_RESPONSE_BYTES);
    }

    public ShardClient(ReaderHealth health, String token, Duration connectTimeout, Duration requestTimeout,
                       int maxResponseBytes) {
        this.maxResponseBytes = maxResponseBytes;
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

    /** A response together with the reader that gave it (the reader a query pins for the shard). */
    public record Answer<R>(ReaderHeartbeat reader, R response) {}

    public static final Endpoint<GraphRequest, GraphResponse> GRAPH =
            new Endpoint<>("/internal/graph", GraphResponse.class, GraphRequest::generation,
                    GraphResponse::generation);

    /** Calls the route's candidates in order, at most two attempts. */
    public <Q, R> R call(Route route, Endpoint<Q, R> endpoint, Function<ReaderHeartbeat, Q> request) {
        return callPreferring(route, null, endpoint, request).response();
    }

    /**
     * Like {@link #call}, trying {@code preferred} first when it is set (the reader a query pinned
     * for the shard; it need not still be among the route's candidates), then the route's other
     * candidates, at most two attempts in all. A connection error, timeout or 5xx marks the reader
     * suspect and moves on; a 404 (shard not served there) or a synonyms mismatch moves on without
     * marking it; a generation mismatch (409) is thrown at once.
     *
     * @throws ShardUnavailableException when no attempt answered
     */
    public <Q, R> Answer<R> callPreferring(Route route, ReaderHeartbeat preferred, Endpoint<Q, R> endpoint,
                                           Function<ReaderHeartbeat, Q> request) {
        List<ReaderHeartbeat> candidates = new java.util.ArrayList<>(route.candidates().size() + 1);
        if (preferred != null) candidates.add(preferred);
        for (ReaderHeartbeat r : route.candidates()) {
            if (preferred == null || !r.node().equals(preferred.node())) candidates.add(r);
        }
        if (candidates.isEmpty()) {
            throw new ShardUnavailableException(route.shard(), "no live reader serves it", null);
        }
        Exception last = null;
        for (int i = 0; i < Math.min(2, candidates.size()); i++) {
            ReaderHeartbeat reader = candidates.get(i);
            try {
                return new Answer<>(reader, callOn(reader, endpoint, request.apply(reader)));
            } catch (RetryableFailure f) {
                health.markSuspect(reader.node());
                last = f;
            } catch (NotServedException | SynonymsMismatchException n) { // not a sick reader
                last = n;
            }
        }
        throw new ShardUnavailableException(route.shard(), last == null ? null : last.getMessage(), last);
    }

    /** One call to one reader, no retry: how a query keeps its later calls on the reader it pinned. */
    public <Q, R> R callOn(ReaderHeartbeat reader, Endpoint<Q, R> endpoint, Q request) {
        byte[] payload;
        try {
            payload = mapper.writeValueAsBytes(request);
        } catch (JsonProcessingException e) { // a local bug: the reader is not at fault
            throw new ShardCallException("cannot serialise the request for " + endpoint.path() + ": " + e.getOriginalMessage());
        }
        AtomicInteger counter = inFlight.computeIfAbsent(reader.node(), n -> new AtomicInteger());
        counter.incrementAndGet();
        try {
            int status;
            byte[] body;
            try {
                HttpRequest req = HttpRequest.newBuilder(URI.create(reader.advertiseUrl() + endpoint.path()))
                        .timeout(requestTimeout)
                        .header("Content-Type", "application/json")
                        .header(InternalApi.TOKEN_HEADER, token)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                        .build();
                // The JDK's request timeout ends once the headers arrive: the whole exchange, body
                // included, is bounded here by the same duration (twice it, so a slow connect still fits).
                java.util.concurrent.CompletableFuture<HttpResponse<byte[]>> pending =
                        http.sendAsync(req, info -> new CappedBody(maxResponseBytes + 1));
                HttpResponse<byte[]> resp;
                try {
                    resp = pending.get(requestTimeout.toMillis() * 2, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.TimeoutException e) {
                    pending.cancel(true);
                    throw new IOException("no complete response within " + requestTimeout.toMillis() * 2 + " ms");
                } catch (java.util.concurrent.ExecutionException e) {
                    Throwable c = e.getCause();
                    if (c instanceof IOException io) throw io;
                    if (c instanceof IllegalArgumentException iae) throw iae;
                    throw new IOException(String.valueOf(c), c);
                }
                status = resp.statusCode();
                body = resp.body();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ShardCallException("interrupted calling " + reader.node());
            } catch (IOException | IllegalArgumentException e) { // connect error, timeout, reset, bad advertiseUrl
                throw new RetryableFailure(reader.node() + ": " + e);
            }
            if (body.length > maxResponseBytes) {
                throw new ShardCallException(reader.node() + " sent a response too large (over " + maxResponseBytes + " bytes)");
            }
            if (status >= 500) throw new RetryableFailure(reader.node() + " answered " + status);
            if (status == 409) {
                health.markHealthy(reader.node());
                if (InternalApi.SYNONYMS_MISMATCH.equals(errorOf(body))) {
                    throw new SynonymsMismatchException(reader.node() + " analyses queries with other synonyms"
                            + " (synonyms.txt differs from this coordinator's)");
                }
                throw new GenerationMismatchException(reader.node(), servedGeneration(body));
            }
            if (status == 404) throw new NotServedException(reader.node() + " does not serve the shard");
            if (status != 200) throw new ShardCallException(reader.node() + " answered " + status + " for " + endpoint.path());
            R response;
            try {
                response = mapper.readValue(body, endpoint.responseType());
            } catch (IOException e) {
                throw new RetryableFailure(reader.node() + " sent an unreadable response: " + e.getMessage());
            }
            health.markHealthy(reader.node());
            Long wanted = endpoint.requestedGeneration().apply(request);
            long served = endpoint.servedGeneration().applyAsLong(response);
            if (wanted != null && wanted != served) { // a swap landed between the reader's check and its work
                throw new GenerationMismatchException(reader.node(), served);
            }
            return response;
        } finally {
            counter.decrementAndGet();
        }
    }

    private String errorOf(byte[] body) {
        try {
            return mapper.readTree(body).path("error").asText(null);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Collects a response body up to {@code cap} bytes and then stops reading (the caller rejects
     * a body that reached the cap), so a runaway reader cannot fill the heap.
     */
    private static final class CappedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final int cap;
        private final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        private final java.util.concurrent.CompletableFuture<byte[]> result = new java.util.concurrent.CompletableFuture<>();
        private java.util.concurrent.Flow.Subscription subscription;

        CappedBody(int cap) { this.cap = cap; }

        @Override public java.util.concurrent.CompletionStage<byte[]> getBody() { return result; }

        @Override public void onSubscribe(java.util.concurrent.Flow.Subscription s) {
            subscription = s;
            s.request(Long.MAX_VALUE);
        }

        @Override public void onNext(List<java.nio.ByteBuffer> items) {
            if (result.isDone()) return;
            for (java.nio.ByteBuffer b : items) {
                int n = Math.min(b.remaining(), cap - out.size());
                byte[] chunk = new byte[n];
                b.get(chunk);
                out.write(chunk, 0, n);
                if (out.size() >= cap) {
                    subscription.cancel();
                    result.complete(out.toByteArray());
                    return;
                }
            }
        }

        @Override public void onError(Throwable t) { result.completeExceptionally(t); }

        @Override public void onComplete() { result.complete(out.toByteArray()); }
    }

    private long servedGeneration(byte[] body) {
        try {
            JsonNode n = mapper.readTree(body);
            return n.path("servedGeneration").asLong(-1);
        } catch (IOException e) {
            return -1;
        }
    }

    /** A 404: the reader does not hold the shard (any more); another replica may. */
    private static final class NotServedException extends ShardCallException {
        NotServedException(String message) { super(message); }
    }

    /** Internal: a failure another replica may not share. {@link #callOn} callers see {@link ShardCallException} instead. */
    private static final class RetryableFailure extends ShardCallException {
        RetryableFailure(String message) { super(message); }
    }
}
