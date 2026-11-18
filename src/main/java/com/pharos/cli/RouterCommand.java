package com.pharos.cli;

import com.pharos.cluster.BackendKind;
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
 * the bucket ({@code catalog/readers/}, read only), polls each one's {@code /health} every 2 s, and
 * round-robins {@code /api/*} and {@code /health} over the readers whose last poll answered 200. A
 * draining or warming reader answers 503 and gets no traffic; a reader with a degraded shard answers
 * 200 and keeps it (coordinators route around that shard). An idempotent request (any {@code GET},
 * and {@code POST /api/search}) is retried once on another reader after a connect error or a 5xx.
 * {@code /internal/*}, and every other path, answers 404 and is never forwarded.
 *
 * <p>It needs the cluster's object-store settings and {@code cluster.token}, and no index. It never
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
    /** The longest a forwarded request waits for its response headers. */
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
        try (ObjectStore store = new S3ObjectStore(c.getObjectStore());
             ReaderRegistry readers = ReaderRegistry.observer(store,
                     new ObjectStoreLayout(c.getObjectStore().getPrefix()), c);
             Router router = new Router(readers::live, POLL_EVERY, Duration.ofMillis(c.getConnectTimeoutMillis()),
                     REQUEST_TIMEOUT)) {
            readers.start();
            router.start(port);
            System.out.printf("pharos router listening on port %d (readers of bucket '%s')%n", router.port(),
                    c.getObjectStore().getBucket());
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

    /** Why this config can't run a router, or null. */
    static String configProblem(ClusterConfig c) {
        if (c.getBackend() != BackendKind.OBJECT_STORE || c.getObjectStore() == null
                || c.getObjectStore().getBucket() == null || c.getObjectStore().getBucket().isBlank()) {
            return "needs cluster.backend \"object-store\" and cluster.objectStore.bucket (the readers are listed"
                    + " from the bucket)";
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
        private ScheduledExecutorService poller;
        private Javalin app;

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
         * {@code /health} or a path under {@code /api/}, with no percent-encoded byte, dot segment,
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
            return rawPath.equals("/health") || (rawPath.startsWith("/api/") && rawPath.length() > "/api/".length());
        }

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
            List<Map.Entry<String, String>> targets = new ArrayList<>(healthy.entrySet());
            if (targets.isEmpty()) {
                ctx.status(503).json(Map.of("status", "unavailable", "error", "no healthy reader"));
                return;
            }
            targets.sort(Map.Entry.comparingByKey());
            int first = (int) Math.floorMod(turn.getAndIncrement(), (long) targets.size());
            String method = ctx.method().name();
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
            HttpResponse<java.io.InputStream> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
            long cap = maxResponseBytes;
            try (java.io.InputStream in = resp.body()) {
                long declared = resp.headers().firstValueAsLong("content-length").orElse(-1);
                if (declared > cap) return null;
                byte[] data = in.readNBytes((int) Math.min(Integer.MAX_VALUE - 8, cap + 1));
                if (data.length > cap) return null;
                return new BodyResponse(resp, data);
            }
        }

        /** A response whose body was read (and bounded) here. */
        private record BodyResponse(HttpResponse<java.io.InputStream> head, byte[] body)
                implements HttpResponse<byte[]> {
            @Override public int statusCode() { return head.statusCode(); }
            @Override public HttpRequest request() { return head.request(); }
            @Override public java.util.Optional<HttpResponse<byte[]>> previousResponse() { return java.util.Optional.empty(); }
            @Override public java.net.http.HttpHeaders headers() { return head.headers(); }
            @Override public java.util.Optional<javax.net.ssl.SSLSession> sslSession() { return head.sslSession(); }
            @Override public URI uri() { return head.uri(); }
            @Override public HttpClient.Version version() { return head.version(); }
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
