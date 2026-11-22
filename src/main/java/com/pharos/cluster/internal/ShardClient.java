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
public final class ShardClient implements AutoCloseable {

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
    /** Calls one coordinator may have open to one reader at a time; the rest wait (within their deadline). */
    private final int perNodeConcurrency;
    private final Map<String, java.util.concurrent.Semaphore> permits = new ConcurrentHashMap<>();

    /** Default {@link #perNodeConcurrency}: open calls from one coordinator to one reader. */
    public static final int DEFAULT_PER_NODE_CONCURRENCY = 64;

    public ShardClient(ReaderHealth health, String token, Duration connectTimeout, Duration requestTimeout) {
        this(health, token, connectTimeout, requestTimeout, DEFAULT_MAX_RESPONSE_BYTES);
    }

    public ShardClient(ReaderHealth health, String token, Duration connectTimeout, Duration requestTimeout,
                       int maxResponseBytes) {
        this(health, token, connectTimeout, requestTimeout, maxResponseBytes, DEFAULT_PER_NODE_CONCURRENCY);
    }

    public ShardClient(ReaderHealth health, String token, Duration connectTimeout, Duration requestTimeout,
                       int maxResponseBytes, int perNodeConcurrency) {
        this.perNodeConcurrency = perNodeConcurrency;
        this.maxResponseBytes = maxResponseBytes;
        this.health = health;
        this.token = token;
        this.requestTimeout = requestTimeout;
        this.http = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
    }

    /** Aborts the calls still open and stops the HTTP client's threads. Idempotent. */
    @Override
    public void close() {
        http.shutdownNow();
    }

    /** True once {@link #close} ran and the HTTP client's threads have stopped. */
    public boolean closed() {
        return http.isTerminated();
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

    /** The suspect marks this client keeps (shared with the router). */
    public ReaderHealth health() {
        return health;
    }

    /** Calls the route's candidates in order, at most two attempts. */
    public <Q, R> R call(Route route, Endpoint<Q, R> endpoint, Function<ReaderHeartbeat, Q> request) {
        return callPreferring(route, null, endpoint, request).response();
    }

    /** The deadline of a call made without one: each of two attempts gets twice the request timeout. */
    private long defaultDeadline() {
        return System.nanoTime() + requestTimeout.toNanos() * 4;
    }

    /** {@link #callPreferring(Route, ReaderHeartbeat, Endpoint, Function, long)} without a query deadline. */
    public <Q, R> Answer<R> callPreferring(Route route, ReaderHeartbeat preferred, Endpoint<Q, R> endpoint,
                                           Function<ReaderHeartbeat, Q> request) {
        return callPreferring(route, preferred, endpoint, request, defaultDeadline());
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
                                           Function<ReaderHeartbeat, Q> request, long deadlineNanos) {
        List<ReaderHeartbeat> candidates = new java.util.ArrayList<>(route.candidates().size() + 1);
        if (preferred != null) candidates.add(preferred);
        for (ReaderHeartbeat r : route.candidates()) {
            if (preferred == null || !r.node().equals(preferred.node())) candidates.add(r);
        }
        if (candidates.isEmpty()) {
            throw new ShardUnavailableException(route.shard(), "no live reader serves it", null);
        }
        Exception last = null;
        int attempts = Math.min(2, candidates.size());
        for (int i = 0; i < attempts; i++) {
            ReaderHeartbeat reader = candidates.get(i);
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                last = new ShardCallException("the query deadline passed");
                break;
            }
            // Leave the later attempts their share, so a retry after a stalled reader still runs.
            long bound = Math.min(requestTimeout.toNanos() * 2, remaining / (attempts - i));
            try {
                return new Answer<>(reader, callOn(reader, endpoint, request.apply(reader), bound));
            } catch (RetryableFailure f) {
                if (f.suspect) health.markSuspect(reader.node()); // not for a timeout the query budget cut short
                last = f;
            } catch (NotServedException | SynonymsMismatchException | ModelMismatchException | BusyException
                     | KeyFiltersRefused n) {
                last = n; // not a sick reader
            }
        }
        throw new ShardUnavailableException(route.shard(), last == null ? null : last.getMessage(), last);
    }

    /** One call to one reader, no retry: how a query keeps its later calls on the reader it pinned. */
    public <Q, R> R callOn(ReaderHeartbeat reader, Endpoint<Q, R> endpoint, Q request) {
        return callOn(reader, endpoint, request, requestTimeout.toNanos() * 2);
    }

    /**
     * One call to one reader, the whole exchange (permit wait, connect, headers, body) bounded by
     * {@code boundNanos}. At most {@code perNodeConcurrency} calls run to one reader at a time.
     */
    public <Q, R> R callOn(ReaderHeartbeat reader, Endpoint<Q, R> endpoint, Q request, long boundNanos) {
        long start = System.nanoTime();
        java.util.concurrent.Semaphore permit = permits.computeIfAbsent(reader.node(),
                n -> new java.util.concurrent.Semaphore(perNodeConcurrency));
        try {
            if (!permit.tryAcquire(boundNanos, java.util.concurrent.TimeUnit.NANOSECONDS)) {
                throw new BusyException(perNodeConcurrency + " calls to " + reader.node() + " already open");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ShardCallException("interrupted waiting to call " + reader.node());
        }
        try {
            // Full: this attempt had the whole per-exchange allowance; a stall over it marks the reader
            // suspect (a permit wait within it does not shorten it for that purpose).
            boolean full = boundNanos >= requestTimeout.toNanos() * 2;
            return exchange(reader, endpoint, request, Math.max(1_000_000L, boundNanos - (System.nanoTime() - start)),
                    full);
        } finally {
            permit.release();
        }
    }

    private <Q, R> R exchange(ReaderHeartbeat reader, Endpoint<Q, R> endpoint, Q request, long boundNanos,
                              boolean fullBound) {
        long boundMillis = Math.max(1, boundNanos / 1_000_000L);
        Duration headerTimeout = Duration.ofMillis(Math.min(requestTimeout.toMillis(), boundMillis));
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
                        .timeout(headerTimeout)
                        .header("Content-Type", "application/json")
                        .header(InternalApi.TOKEN_HEADER, token)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                        .build();
                // The JDK's request timeout ends once the headers arrive: the whole exchange, body
                // included, is bounded here (twice the request timeout, or less within a query deadline).
                java.util.concurrent.CompletableFuture<HttpResponse<byte[]>> pending =
                        http.sendAsync(req, info -> new CappedBody(maxResponseBytes + 1));
                HttpResponse<byte[]> resp;
                try {
                    resp = pending.get(boundMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.TimeoutException e) {
                    pending.cancel(true);
                    // Only a stall over the full per-exchange allowance (twice the request timeout)
                    // says the reader is sick; a shorter bound was the query's leftover budget.
                    throw new RetryableFailure(reader.node() + ": no complete response within " + boundMillis + " ms",
                            fullBound);
                } catch (java.util.concurrent.ExecutionException e) {
                    Throwable c = e.getCause();
                    if (c instanceof java.net.http.HttpTimeoutException t
                            && !(c instanceof java.net.http.HttpConnectTimeoutException)) {
                        // The headers did not come within headerTimeout: the reader's fault only when
                        // that was the full request timeout, not a share of the query's budget.
                        throw new RetryableFailure(reader.node() + ": " + t,
                                headerTimeout.compareTo(requestTimeout) >= 0);
                    }
                    if (c instanceof IOException io) throw io;
                    if (c instanceof IllegalArgumentException iae) throw iae;
                    throw new IOException(String.valueOf(c), c);
                } catch (InterruptedException e) {
                    pending.cancel(true); // the exchange must not outlive the cancelled query
                    throw e;
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
                String error = errorOf(body);
                if (InternalApi.MODEL_MISMATCH.equals(error)) {
                    throw new ModelMismatchException(reasonOf(body), detailOf(body));
                }
                if (InternalApi.KEY_FILTERS_UNSUPPORTED.equals(error)) {
                    throw new KeyFiltersRefused(reader.node() + ": " + detailOf(body));
                }
                if (InternalApi.SYNONYMS_MISMATCH.equals(error)) {
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

    private ModelMismatchException.Reason reasonOf(byte[] body) {
        try {
            return ModelMismatchException.Reason.parse(mapper.readTree(body).path("reason").asText(null));
        } catch (IOException e) {
            return ModelMismatchException.Reason.OTHER;
        }
    }

    private String detailOf(byte[] body) {
        try {
            return mapper.readTree(body).path("detail").asText("model mismatch");
        } catch (IOException e) {
            return "model mismatch";
        }
    }

    /** The reader's index of the shard predates the key fields: it refuses a filtered query (not sick). */
    private static final class KeyFiltersRefused extends ShardCallException {
        KeyFiltersRefused(String message) { super(message); }
    }

    /** This coordinator already has {@code perNodeConcurrency} calls open to the reader: try another. */
    private static final class BusyException extends ShardCallException {
        BusyException(String message) { super(message); }
    }

    private String errorOf(byte[] body) {
        try {
            return mapper.readTree(body).path("error").asText(null);
        } catch (IOException e) {
            return null;
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

    /** A 404: the reader does not hold the shard (any more); another replica may. */
    private static final class NotServedException extends ShardCallException {
        NotServedException(String message) { super(message); }
    }

    /**
     * Internal: a failure another replica may not share. {@code suspect}: it says the reader is sick
     * (connect error, reset, 5xx, or a timeout of the full per-call allowance), so it is marked
     * suspect. {@link #callOn} callers see {@link ShardCallException} instead.
     */
    private static final class RetryableFailure extends ShardCallException {
        final boolean suspect;

        RetryableFailure(String message) { this(message, true); }

        RetryableFailure(String message, boolean suspect) {
            super(message);
            this.suspect = suspect;
        }
    }
}
