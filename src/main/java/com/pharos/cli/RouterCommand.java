package com.pharos.cli;

import com.pharos.cluster.BackendKind;
import com.pharos.cluster.membership.MembershipSource;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.membership.ReaderRegistry;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.S3ObjectStore;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * {@code pharos router}: a stateless HTTP front for the reader tier. It lists the live readers from
 * the bucket ({@code catalog/readers/}, read only), or with {@code cluster.backend: peer-pull} from
 * every writer {@code cluster.peers.discovery} finds ({@code GET /replication/readers}, merged), polls each one's {@code /health} every 2 s, and
 * round-robins {@code /api/*}, {@code /health} and {@code GET /cluster/status} over the readers whose
 * last poll answered 200. {@code GET /cluster/events} (server-sent events) is streamed through from
 * one reader without buffering, exempt from the total deadline and the body cap, and cut only when
 * the stream is idle for twice the events keep-alive ({@code cluster.status.eventsKeepAliveSeconds}). A
 * draining or warming reader answers 503 and gets no traffic; a reader with a degraded shard answers
 * 200 and keeps it (coordinators route around that shard). An idempotent request (any {@code GET},
 * and {@code POST /api/search}) is retried once on another reader after a connect error or a 5xx.
 * {@code /internal/*}, and every other path, answers 404 and is never forwarded.
 *
 * <p>It needs the cluster's object-store settings (or {@code cluster.peers.discovery}) and
 * {@code cluster.token}, and no index. It never
 * sends the token to a reader: only {@code /internal/*} takes it, and that is never forwarded.
 */
@Command(
        name = "router",
        description = "Route queries over the cluster's healthy readers (round-robin, skips readers failing /health)",
        mixinStandardHelpOptions = true
)
public class RouterCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(RouterCommand.class);

    @Option(names = {"--port"}, description = "HTTP port to listen on (default: 7080)", defaultValue = "7080")
    private int port;

    /** How often each live reader's {@code /health} is polled. */
    static final Duration POLL_EVERY = Duration.ofSeconds(2);
    /** The longest a forwarded exchange takes, response headers and (capped) body read together; past it: 504. */
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(120);

    private final IndexConfig config;

    public RouterCommand(IndexConfig config) {
        this.config = config;
    }

    @Override
    public Integer call() {
        ClusterConfig c = config.getCluster();
        String problem = configProblem(c);
        if (problem != null) {
            System.err.println("pharos router: " + problem);
            return 2;
        }
        try (MembershipSource readers = observe(c);
             Router router = new Router(readers, POLL_EVERY, Duration.ofMillis(c.getConnectTimeoutMillis()),
                     REQUEST_TIMEOUT)) {
            router.setEventsIdleTimeout(Duration.ofSeconds(2L * c.getStatus().getEventsKeepAliveSeconds()));
            readers.start();
            router.start(port);
            System.out.printf("pharos router listening on port %d (readers %s)%n", router.port(),
                    c.getBackend() == BackendKind.PEER_PULL
                            ? "of the writers at '" + c.getPeers().getDiscovery() + "'"
                            : "of bucket '" + c.getObjectStore().getBucket() + "'");
            Thread.currentThread().join(); // until Ctrl+C
            return 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 0;
        } catch (Exception e) {
            System.err.println("pharos router: " + e.getMessage());
            return 1;
        }
    }

    /**
     * The read-only reader membership a router routes over, owning its clients (closed with it): the
     * bucket's {@code catalog/readers/}, or on {@code peer-pull} every discovered writer's
     * {@code GET /replication/readers}, merged.
     */
    public static MembershipSource observe(ClusterConfig c) throws IOException {
        if (c.getBackend() == BackendKind.PEER_PULL) {
            com.pharos.cluster.peer.PeerHttp http = com.pharos.cluster.peer.PeerHttp.of(c);
            try {
                var discovery = com.pharos.cluster.peer.PeerDiscovery.of(c,
                        com.pharos.cluster.peer.PeerDiscovery.parse(c.getPeers().getDiscovery()), http); // allowHosts too
                var catalog = new com.pharos.cluster.peer.PeerCatalog(discovery, http,
                        Duration.ofSeconds(c.getPeers().getRefreshSeconds()), System::nanoTime);
                var members = com.pharos.cluster.peer.PeerMembership.observer(catalog, http, c);
                return closing(members, http);
            } catch (RuntimeException e) {
                http.close();
                throw e;
            }
        }
        ObjectStore store = new S3ObjectStore(c.getObjectStore());
        return closing(ReaderRegistry.observer(store, new ObjectStoreLayout(c.getObjectStore().getPrefix()), c), store);
    }

    /** {@code members}, also closing {@code client} when closed. */
    private static MembershipSource closing(MembershipSource members, AutoCloseable client) {
        return new MembershipSource() {
            @Override public List<com.pharos.cluster.membership.ReaderRegistry.LiveReader> liveWithAge() {
                return members.liveWithAge();
            }

            @Override public void start() { members.start(); }

            @Override
            public void close() {
                members.close();
                try {
                    client.close();
                } catch (Exception e) {
                    log.debug("Closing the router's membership client failed: {}", e.toString());
                }
            }
        };
    }

    /** Why this config can't run a router, or null. */
    static String configProblem(ClusterConfig c) {
        if (c.getBackend() == BackendKind.PEER_PULL) {
            String d = c.getPeers().getDiscovery();
            if (d == null || d.isBlank()) {
                return "needs cluster.peers.discovery on cluster.backend \"peer-pull\" (the readers are listed"
                        + " from the writers)";
            }
            try {
                com.pharos.cluster.peer.PeerDiscovery.parse(d);
            } catch (IllegalArgumentException e) {
                return e.getMessage();
            }
        } else if (c.getBackend() != BackendKind.OBJECT_STORE || c.getObjectStore() == null
                || c.getObjectStore().getBucket() == null || c.getObjectStore().getBucket().isBlank()) {
            return "needs cluster.backend \"object-store\" and cluster.objectStore.bucket (the readers are listed"
                    + " from the bucket), or cluster.backend \"peer-pull\" and cluster.peers.discovery";
        }
        if (c.getToken() == null || c.getToken().isBlank()) {
            return "cluster.token is required on a router (shared secret of the cluster)";
        }
        return null;
    }

    /** The routing core: health polling, round-robin, one retry, the path allowlist. */
    public static final class Router implements AutoCloseable {

        /** Request headers never forwarded: hop-by-hop, set by the HTTP client itself, or secret. */
        private static final Set<String> DROP_REQUEST = Set.of("host", "connection", "content-length", "expect",
                "upgrade", "keep-alive", "proxy-connection", "proxy-authorization", "te", "trailer",
                "transfer-encoding", "accept-encoding", "x-pharos-cluster-token", "http2-settings");
        /** Response headers never copied back: hop-by-hop, or set by this server. */
        private static final Set<String> DROP_RESPONSE = Set.of("connection", "content-length", "keep-alive",
                "transfer-encoding", "upgrade", "trailer", "proxy-connection", "date", "server", ":status");

        private final Supplier<List<ReaderHeartbeat>> live;
        private final Duration pollEvery;
        private final Duration requestTimeout;
        private final HttpClient http;
        /** Node → base URL of the readers whose last {@code /health} poll answered 200. */
        private final Map<String, String> healthy = new ConcurrentHashMap<>();
        private final AtomicLong turn = new AtomicLong();
        /** A reader's answer larger than this is not forwarded (502). */
        static final long MAX_RESPONSE_BYTES = 64L << 20;
        private volatile long maxResponseBytes = MAX_RESPONSE_BYTES;
        /** {@code /cluster/events}: a stream with no byte for this long is cut (2 × the SSE keep-alive). */
        static final Duration DEFAULT_EVENTS_IDLE_TIMEOUT = Duration.ofSeconds(30);
        private volatile Duration eventsIdleTimeout = DEFAULT_EVENTS_IDLE_TIMEOUT;
        private ScheduledExecutorService poller;
        private Javalin app;

        /** Routes over {@code membership}: the bucket's reader registry or the peer writers' (no bucket). */
        public Router(MembershipSource membership, Duration pollEvery, Duration connectTimeout,
                      Duration requestTimeout) {
            this(Objects.requireNonNull(membership, "membership")::live, pollEvery, connectTimeout, requestTimeout);
        }

        public Router(Supplier<List<ReaderHeartbeat>> live, Duration pollEvery, Duration connectTimeout,
                      Duration requestTimeout) {
            this.live = Objects.requireNonNull(live, "live");
            this.pollEvery = pollEvery;
            this.requestTimeout = requestTimeout;
            this.http = HttpClient.newBuilder().connectTimeout(connectTimeout)
                    .version(HttpClient.Version.HTTP_1_1)
                    .followRedirects(HttpClient.Redirect.NEVER).build();
        }

        /** Binds {@code port} (0: any free one) and starts polling every {@code pollEvery}. */
        public synchronized void start(int port) {
            app = Javalin.create(cfg -> {
                cfg.showJavalinBanner = false;
                cfg.useVirtualThreads = true;
                cfg.http.maxRequestSize = com.pharos.cluster.internal.InternalApi.MAX_BODY_BYTES;
            });
            for (HandlerType t : List.of(HandlerType.GET, HandlerType.POST, HandlerType.PUT, HandlerType.DELETE,
                    HandlerType.PATCH, HandlerType.HEAD)) {
                app.addHttpHandler(t, "/", this::route);
                app.addHttpHandler(t, "/*", this::route);
            }
            app.start(port);
            poller = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-router-health");
                t.setDaemon(true);
                return t;
            });
            poller.scheduleWithFixedDelay(this::pollNow, 0, pollEvery.toMillis(), TimeUnit.MILLISECONDS);
        }

        /** Test seam: the largest response body forwarded (default {@link #MAX_RESPONSE_BYTES}). */
        void setMaxResponseBytes(long max) {
            this.maxResponseBytes = max;
        }

        /** How long a {@code /cluster/events} stream may go without a byte before it is cut. */
        public void setEventsIdleTimeout(Duration idle) {
            this.eventsIdleTimeout = Objects.requireNonNull(idle, "idle");
        }

        public synchronized int port() {
            return app == null ? -1 : app.port();
        }

        /** One {@code /health} poll of every live reader, in parallel; replaces the healthy set. Never throws. */
        public void pollNow() {
            List<ReaderHeartbeat> readers;
            try {
                readers = live.get();
            } catch (RuntimeException e) {
                log.warn("Router: listing the live readers failed (keeping the healthy set): {}", e.toString());
                return;
            }
            Duration probeTimeout = pollEvery.compareTo(Duration.ofSeconds(2)) < 0 ? pollEvery : Duration.ofSeconds(2);
            Map<String, String> now = new ConcurrentHashMap<>();
            List<Thread> probes = new ArrayList<>();
            for (ReaderHeartbeat r : readers) {
                if (r.draining() || r.advertiseUrl() == null || r.advertiseUrl().isBlank()) continue;
                String base = stripSlash(r.advertiseUrl());
                probes.add(Thread.ofVirtual().name("pharos-router-probe").start(() -> {
                    try {
                        HttpResponse<Void> resp = http.send(HttpRequest.newBuilder(URI.create(base + "/health"))
                                .timeout(probeTimeout).GET().build(), HttpResponse.BodyHandlers.discarding());
                        if (resp.statusCode() == 200) now.put(r.node(), base);
                    } catch (IOException | RuntimeException e) {
                        // no answer: not healthy
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            for (Thread t : probes) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            Set<String> before = Set.copyOf(healthy.keySet());
            healthy.keySet().retainAll(now.keySet());
            healthy.putAll(now);
            if (!before.equals(now.keySet())) log.info("Router: healthy readers {}", new TreeSet<>(now.keySet()));
        }

        /** The nodes currently routed to, sorted. */
        public List<String> healthy() {
            return healthy.keySet().stream().sorted().toList();
        }

        private static String stripSlash(String url) {
            String u = url.trim();
            while (u.endsWith("/")) u = u.substring(0, u.length() - 1);
            return u;
        }

        /**
         * True when {@code rawPath} (as received, still percent-encoded) is one this router forwards:
         * {@code /health}, {@code /cluster/status}, {@code /cluster/events} or a path under {@code /api/},
         * with no percent-encoded byte, dot segment,
         * empty segment, path parameter ({@code ;}) or backslash, so a reader cannot resolve it to
         * anything else (never to {@code /internal/*}).
         */
        static boolean forwardable(String rawPath) {
            if (rawPath == null || rawPath.isEmpty()) return false;
            if (rawPath.indexOf('%') >= 0 || rawPath.indexOf(';') >= 0 || rawPath.indexOf('\\') >= 0
                    || rawPath.contains("//")) {
                return false;
            }
            for (String seg : rawPath.split("/", -1)) {
                if (seg.equals(".") || seg.equals("..")) return false;
            }
            if (rawPath.toLowerCase(Locale.ROOT).startsWith("/internal")) return false;
            return rawPath.equals("/health") || CLUSTER_PATHS.contains(rawPath)
                    || (rawPath.startsWith("/api/") && rawPath.length() > "/api/".length());
        }

        static final String EVENTS_PATH = "/cluster/events";
        /** Cluster paths forwarded, for {@code GET} (and {@code HEAD}) only. */
        static final Set<String> CLUSTER_PATHS = Set.of("/cluster/status", EVENTS_PATH);

        /** Any {@code GET} or {@code HEAD}, and {@code POST /api/search}: safe to send twice. */
        static boolean idempotent(String method, String rawPath) {
            return method.equals("GET") || method.equals("HEAD")
                    || (method.equals("POST") && rawPath.equals("/api/search"));
        }

        private void route(Context ctx) {
            String rawPath = ctx.req().getRequestURI();
            if (!forwardable(rawPath)) {
                ctx.status(404).json(Map.of("error", "not routed: " + rawPath));
                return;
            }
            String method = ctx.method().name();
            if (CLUSTER_PATHS.contains(rawPath) && !(method.equals("GET") || method.equals("HEAD"))) {
                ctx.status(405).json(Map.of("error", method + " " + rawPath + " is not routed (GET only)"));
                return;
            }
            List<Map.Entry<String, String>> targets = new ArrayList<>(healthy.entrySet());
            if (targets.isEmpty()) {
                ctx.status(503).json(Map.of("status", "unavailable", "error", "no healthy reader"));
                return;
            }
            targets.sort(Map.Entry.comparingByKey());
            int first = (int) Math.floorMod(turn.getAndIncrement(), (long) targets.size());
            if (rawPath.equals(EVENTS_PATH) && method.equals("GET")) {
                streamEvents(ctx, targets.get(first), ctx.req().getQueryString());
                return;
            }
            int attempts = idempotent(method, rawPath) && targets.size() > 1 ? 2 : 1;
            byte[] body = ctx.bodyAsBytes();
            String query = ctx.req().getQueryString();
            HttpResponse<byte[]> answer = null;
            Exception failure = null;
            for (int i = 0; i < attempts; i++) {
                Map.Entry<String, String> target = targets.get((first + i) % targets.size());
                try {
                    answer = forward(ctx, target.getValue(), rawPath, query, method, body);
                    failure = null;
                    if (answer == null) { // over the cap: the reader answered; not retried
                        ctx.status(502).json(Map.of("error", "the reader's answer exceeds " + maxResponseBytes
                                + " bytes; not forwarded"));
                        return;
                    }
                    if (answer.statusCode() < 500) break;
                    log.debug("Router: {} {} on reader {} answered {}", method, rawPath, target.getKey(),
                            answer.statusCode());
                } catch (ConnectException | HttpConnectTimeoutException e) {
                    answer = null;
                    failure = e;
                    healthy.remove(target.getKey(), target.getValue()); // until a poll finds it healthy again
                    log.info("Router: reader {} is unreachable ({}); skipping it until it answers /health",
                            target.getKey(), e.toString());
                } catch (ExchangeTimeout | java.net.http.HttpTimeoutException e) { // sent, maybe running: not resent
                    log.info("Router: {} {} on reader {}: {}", method, rawPath, target.getKey(), e.getMessage());
                    ctx.status(504).json(Map.of("error", "the reader did not answer in time: " + e.getMessage()));
                    return;
                } catch (IOException e) {
                    answer = null;
                    failure = e;
                    if (!(e.getCause() instanceof ConnectException)) break; // sent: it may have run; not resent
                    healthy.remove(target.getKey(), target.getValue());
                } catch (IllegalArgumentException e) { // the request's URI is not one the HTTP client accepts
                    ctx.status(400).json(Map.of("error", "malformed request URI: " + e.getMessage()));
                    return;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ctx.status(503).json(Map.of("error", "interrupted"));
                    return;
                }
            }
            if (answer == null) {
                ctx.status(502).json(Map.of("error", "no reader answered: "
                        + (failure == null ? "unknown error" : failure.toString())));
                return;
            }
            ctx.status(answer.statusCode());
            Set<String> named = connectionNamed(answer.headers().allValues("connection"));
            answer.headers().map().forEach((name, values) -> {
                String lower = name.toLowerCase(Locale.ROOT);
                if (DROP_RESPONSE.contains(lower) || named.contains(lower)) return;
                for (String v : values) ctx.res().addHeader(name, v);
            });
            ctx.result(answer.body());
        }

        /** The header names a {@code Connection} header lists (hop-by-hop: never forwarded), lower case. */
        private static Set<String> connectionNamed(List<String> values) {
            Set<String> out = new java.util.HashSet<>();
            for (String v : values) {
                for (String token : v.split(",")) {
                    String t = token.trim().toLowerCase(Locale.ROOT);
                    if (!t.isEmpty()) out.add(t);
                }
            }
            return out;
        }

        private HttpResponse<byte[]> forward(Context ctx, String base, String rawPath, String query, String method,
                                            byte[] body) throws IOException, InterruptedException {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + rawPath + (query == null ? "" : "?" + query)))
                    .timeout(requestTimeout)
                    .method(method, body.length == 0 ? HttpRequest.BodyPublishers.noBody()
                            : HttpRequest.BodyPublishers.ofByteArray(body));
            Set<String> named = connectionNamed(Collections.list(ctx.req().getHeaders("Connection")));
            String forwardedFor = null;
            for (String name : Collections.list(ctx.req().getHeaderNames())) {
                String lower = name.toLowerCase(Locale.ROOT);
                if (DROP_REQUEST.contains(lower) || named.contains(lower)) continue;
                if (lower.equals("x-forwarded-for")) {
                    forwardedFor = String.join(", ", Collections.list(ctx.req().getHeaders(name)));
                    continue;
                }
                for (String v : Collections.list(ctx.req().getHeaders(name))) {
                    try {
                        b.header(name, v);
                    } catch (IllegalArgumentException restricted) {
                        // a header the HTTP client sets itself
                    }
                }
            }
            String client = ctx.req().getRemoteAddr();
            b.header("X-Forwarded-For", forwardedFor == null || forwardedFor.isBlank() ? client
                    : forwardedFor + ", " + client);
            long cap = maxResponseBytes;
            int wanted = (int) Math.min(Integer.MAX_VALUE - 8, cap + 1);
            // The JDK's request timeout ends once the headers arrive: the whole exchange, the capped
            // body read included, is bounded here by requestTimeout (as in ShardClient).
            java.util.concurrent.CompletableFuture<HttpResponse<byte[]>> pending = http.sendAsync(b.build(),
                    info -> new com.pharos.cluster.internal.CappedBody(
                            info.headers().firstValueAsLong("content-length").orElse(-1) > cap ? 0 : wanted));
            HttpResponse<byte[]> resp;
            try {
                resp = pending.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                pending.cancel(true);
                throw new ExchangeTimeout("no complete answer within " + requestTimeout.toMillis() + " ms");
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable c = e.getCause();
                if (c instanceof IOException io) throw io;
                if (c instanceof IllegalArgumentException iae) throw iae;
                throw new IOException(String.valueOf(c), c);
            }
            long declared = resp.headers().firstValueAsLong("content-length").orElse(-1);
            if (declared > cap || resp.body().length > cap) return null;
            return resp;
        }

        /**
         * Streams {@code GET /cluster/events} from {@code target} to the client as the bytes arrive:
         * no total deadline and no body cap. The upstream's headers must arrive within the idle
         * timeout; after that the stream is cut only when no byte arrives for the idle timeout (the
         * reader sends a keep-alive comment every half of it), when the reader ends it, or when the
         * client goes away. Never retried: a client reconnects (SSE clients do so on their own).
         */
        private void streamEvents(Context ctx, Map.Entry<String, String> target, String query) {
            Duration idle = eventsIdleTimeout;
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(target.getValue() + EVENTS_PATH
                    + (query == null ? "" : "?" + query))).header("Accept", "text/event-stream").GET();
            String last = ctx.req().getHeader("Last-Event-ID");
            if (last != null) b.header("Last-Event-ID", last);
            EventPump pump = new EventPump();
            java.util.concurrent.CompletableFuture<HttpResponse<Void>> pending = http.sendAsync(b.build(), info -> pump);
            HttpResponse<Void> resp;
            try {
                resp = pending.get(idle.toMillis(), TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                pending.cancel(true);
                ctx.status(504).json(Map.of("error", "the reader did not start the event stream in time"));
                return;
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable c = e.getCause();
                if (c instanceof ConnectException || c instanceof HttpConnectTimeoutException
                        || c != null && c.getCause() instanceof ConnectException) {
                    healthy.remove(target.getKey(), target.getValue());
                }
                ctx.status(502).json(Map.of("error", "no reader answered: " + c));
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ctx.status(503).json(Map.of("error", "interrupted"));
                return;
            }
            ctx.status(resp.statusCode());
            Set<String> named = connectionNamed(resp.headers().allValues("connection"));
            resp.headers().map().forEach((name, values) -> {
                String lower = name.toLowerCase(Locale.ROOT);
                if (DROP_RESPONSE.contains(lower) || named.contains(lower)) return;
                for (String v : values) ctx.res().addHeader(name, v);
            });
            ctx.res().setHeader("Cache-Control", "no-cache");
            ctx.res().setHeader("X-Accel-Buffering", "no");
            try {
                java.io.OutputStream out = ctx.res().getOutputStream();
                out.flush(); // the headers now: the client sees the stream open
                pump.run(out, idle);
            } catch (IOException e) {
                pump.cancel(); // the client went away
            }
        }

        /**
         * The upstream event stream's body: buffers handed over as they arrive, written by the
         * request thread (so the client's socket is never written from the HTTP client's threads).
         */
        static final class EventPump implements java.net.http.HttpResponse.BodySubscriber<Void> {
            private static final List<java.nio.ByteBuffer> END = List.of();
            private final java.util.concurrent.LinkedBlockingQueue<List<java.nio.ByteBuffer>> chunks =
                    new java.util.concurrent.LinkedBlockingQueue<>();
            private volatile java.util.concurrent.Flow.Subscription sub;
            private volatile boolean cancelled;

            @Override
            public java.util.concurrent.CompletionStage<Void> getBody() {
                return java.util.concurrent.CompletableFuture.completedFuture(null); // the response at headers
            }

            @Override
            public void onSubscribe(java.util.concurrent.Flow.Subscription s) {
                sub = s;
                if (cancelled) s.cancel();
                else s.request(1);
            }

            @Override
            public void onNext(List<java.nio.ByteBuffer> item) {
                chunks.add(item);
            }

            @Override
            public void onError(Throwable t) {
                chunks.add(END);
            }

            @Override
            public void onComplete() {
                chunks.add(END);
            }

            void cancel() {
                cancelled = true;
                java.util.concurrent.Flow.Subscription s = sub;
                if (s != null) s.cancel();
            }

            /** Copies chunks to {@code out} until the upstream ends or is idle for {@code idle}. */
            void run(java.io.OutputStream out, Duration idle) throws IOException {
                try {
                    while (true) {
                        List<java.nio.ByteBuffer> item;
                        try {
                            item = chunks.poll(idle.toMillis(), TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (item == null) {
                            log.info("Router: /cluster/events idle for {} ms; closing the stream", idle.toMillis());
                            return;
                        }
                        if (item == END) return;
                        for (java.nio.ByteBuffer buf : item) {
                            byte[] bytes = new byte[buf.remaining()];
                            buf.get(bytes);
                            out.write(bytes);
                        }
                        out.flush();
                        java.util.concurrent.Flow.Subscription s = sub;
                        if (s != null) s.request(1);
                    }
                } finally {
                    cancel();
                }
            }
        }

        /** The forwarded exchange (headers and body) did not complete within the request timeout. */
        static final class ExchangeTimeout extends IOException {
            ExchangeTimeout(String message) { super(message); }
        }

        @Override
        public synchronized void close() {
            if (poller != null) poller.shutdownNow();
            poller = null;
            if (app != null) app.stop();
            app = null;
            http.close();
        }
    }
}
