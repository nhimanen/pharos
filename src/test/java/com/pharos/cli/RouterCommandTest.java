package com.pharos.cli;

import com.pharos.cluster.membership.ReaderHeartbeat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Task 6: {@code pharos router} spreads requests over healthy readers, retries once, never forwards /internal. */
class RouterCommandTest {

    /** A stand-in reader: /health answers {@link #health}; other paths answer its name (or a set status). */
    static final class FakeReader implements AutoCloseable {
        final String name;
        final HttpServer server;
        final AtomicInteger health = new AtomicInteger(200);
        final Map<String, Integer> statusFor = new ConcurrentHashMap<>();
        final List<String> requests = new CopyOnWriteArrayList<>(); // "METHOD path?query" (not /health polls)
        final List<String> bodies = new CopyOnWriteArrayList<>();
        final List<Map<String, List<String>>> headers = new CopyOnWriteArrayList<>();
        final AtomicInteger healthCalls = new AtomicInteger();
        /** Paths whose answer sends its headers and one body byte, then stalls this long before finishing. */
        final Map<String, Integer> stallBodyMillis = new ConcurrentHashMap<>();

        FakeReader(String name) throws IOException {
            this(name, null);
        }

        FakeReader(String name, java.util.concurrent.Executor executor) throws IOException {
            this.name = name;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            if (executor != null) server.setExecutor(executor);
            server.start();
        }

        private void handle(HttpExchange ex) throws IOException {
            String path = ex.getRequestURI().getRawPath();
            String query = ex.getRequestURI().getRawQuery();
            byte[] body = ex.getRequestBody().readAllBytes();
            int status;
            String out;
            if (path.equals("/health")) {
                healthCalls.incrementAndGet();
                status = health.get();
                out = status == 200 ? "{\"status\":\"ok\",\"node\":\"" + name + "\"}" : "{\"status\":\"draining\"}";
            } else {
                requests.add(ex.getRequestMethod() + " " + path + (query == null ? "" : "?" + query));
                bodies.add(new String(body, StandardCharsets.UTF_8));
                headers.add(Map.copyOf(ex.getRequestHeaders()));
                status = statusFor.getOrDefault(path, 200);
                out = name;
            }
            byte[] bytes = out.getBytes(StandardCharsets.UTF_8);
            Integer stall = stallBodyMillis.get(path);
            if (stall != null) { // chunked: headers now, the rest of the body much later
                ex.sendResponseHeaders(status, 0);
                ex.getResponseBody().write('x');
                ex.getResponseBody().flush();
                try {
                    Thread.sleep(stall);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                try {
                    ex.getResponseBody().write(bytes);
                } catch (IOException gone) {
                    // the router gave up on the exchange
                }
                ex.close();
                return;
            }
            ex.getResponseHeaders().add("Content-Type", "text/plain");
            ex.getResponseHeaders().add("X-Pharos-Warnings", "[\"from " + name + "\"]");
            ex.sendResponseHeaders(status, bytes.length);
            ex.getResponseBody().write(bytes);
            ex.close();
        }

        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        ReaderHeartbeat heartbeat() {
            return new ReaderHeartbeat(name, url(), List.of("p"), false, null, 0, 0, 0L);
        }

        long count(String prefix) {
            return requests.stream().filter(r -> r.startsWith(prefix)).count();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private final List<AutoCloseable> open = new ArrayList<>();
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = open.size() - 1; i >= 0; i--) open.get(i).close();
    }

    private FakeReader reader(String name) throws IOException {
        FakeReader r = new FakeReader(name);
        open.add(r);
        return r;
    }

    private RouterCommand.Router router(FakeReader... readers) {
        List<ReaderHeartbeat> live = new ArrayList<>();
        for (FakeReader r : readers) live.add(r.heartbeat());
        RouterCommand.Router router = new RouterCommand.Router(() -> live, Duration.ofHours(1),
                Duration.ofMillis(500), Duration.ofSeconds(10));
        open.add(router);
        router.start(0);
        router.pollNow();
        return router;
    }

    private HttpResponse<String> send(RouterCommand.Router router, String method, String pathAndQuery, String body)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + router.port() + pathAndQuery));
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(RouterCommand.Router router, String pathAndQuery) throws Exception {
        return send(router, "GET", pathAndQuery, null);
    }

    @Test
    void routerSpreadsAcrossReaders() throws Exception {
        FakeReader a = reader("a"), b = reader("b"), c = reader("c");
        RouterCommand.Router router = router(a, b, c);
        assertThat(router.healthy()).containsExactlyInAnyOrder("a", "b", "c");
        for (int i = 0; i < 30; i++) {
            HttpResponse<String> r = get(router, "/api/projects");
            assertThat(r.statusCode()).isEqualTo(200);
        }
        assertThat(List.of(a.count("GET /api/projects"), b.count("GET /api/projects"), c.count("GET /api/projects")))
                .as("round robin").containsExactly(10L, 10L, 10L);

        HttpResponse<String> s = get(router, "/api/search?q=a%20b&limit=3");
        assertThat(s.headers().firstValue("X-Pharos-Warnings")).as("response headers pass through")
                .hasValueSatisfying(v -> assertThat(v).startsWith("[\"from "));
        assertThat(a.requests.stream().filter(x -> x.startsWith("GET /api/search")).toList()
                .stream().findFirst().or(() -> b.requests.stream().filter(x -> x.startsWith("GET /api/search")).findFirst())
                .or(() -> c.requests.stream().filter(x -> x.startsWith("GET /api/search")).findFirst()))
                .hasValue("GET /api/search?q=a%20b&limit=3");

        int before = a.healthCalls.get() + b.healthCalls.get() + c.healthCalls.get();
        HttpResponse<String> health = get(router, "/health");
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(health.body()).contains("\"status\":\"ok\"");
        assertThat(a.healthCalls.get() + b.healthCalls.get() + c.healthCalls.get()).as("/health is forwarded too")
                .isEqualTo(before + 1);

        HttpResponse<String> rate = send(router, "POST", "/api/rate", "{\"stars\":5}");
        assertThat(rate.statusCode()).isEqualTo(200);
        assertThat(List.of(a, b, c)).anySatisfy(r -> assertThat(r.bodies).contains("{\"stars\":5}"));
    }

    @Test
    void routerSkipsUnhealthyAndRetriesOnce() throws Exception {
        FakeReader a = reader("a"), b = reader("b"), c = reader("c");
        b.health.set(503); // draining (or warming)
        RouterCommand.Router router = router(a, b, c);
        assertThat(router.healthy()).containsExactlyInAnyOrder("a", "c");
        for (int i = 0; i < 10; i++) assertThat(get(router, "/api/projects").statusCode()).isEqualTo(200);
        assertThat(b.requests).as("a reader whose /health is 503 gets no traffic").isEmpty();
        assertThat(a.count("GET /api/projects") + c.count("GET /api/projects")).isEqualTo(10);

        // c fails searches with 500: each one landing on c is retried once, on a.
        c.statusFor.put("/api/search", 500);
        for (int i = 0; i < 6; i++) {
            HttpResponse<String> r = get(router, "/api/search?q=x");
            assertThat(r.statusCode()).isEqualTo(200);
            assertThat(r.body()).isEqualTo("a");
        }
        assertThat(a.count("GET /api/search")).isEqualTo(6);
        assertThat(c.count("GET /api/search")).isBetween(1L, 6L);
        // POST /api/search is idempotent too
        c.requests.clear();
        a.requests.clear();
        for (int i = 0; i < 4; i++) {
            assertThat(send(router, "POST", "/api/search", "{\"q\":\"x\"}").statusCode()).isEqualTo(200);
        }
        assertThat(a.count("POST /api/search")).isEqualTo(4);

        // Both fail: retried once only, the second answer is returned.
        a.statusFor.put("/api/search", 502);
        a.requests.clear();
        c.requests.clear();
        HttpResponse<String> failed = get(router, "/api/search?q=y");
        assertThat(failed.statusCode()).isIn(500, 502);
        assertThat(a.count("GET /api/search") + c.count("GET /api/search")).as("one try plus one retry").isEqualTo(2);

        // A non-idempotent request is never retried.
        a.statusFor.put("/api/rate", 500);
        c.statusFor.put("/api/rate", 500);
        a.requests.clear();
        c.requests.clear();
        assertThat(send(router, "POST", "/api/rate", "{}").statusCode()).isEqualTo(500);
        assertThat(a.count("POST /api/rate") + c.count("POST /api/rate")).isEqualTo(1);

        // A connect error (c died since the last poll): retried on a, and c is skipped from then on.
        a.statusFor.clear();
        c.close();
        a.requests.clear();
        for (int i = 0; i < 4; i++) {
            HttpResponse<String> r = get(router, "/api/projects");
            assertThat(r.statusCode()).isEqualTo(200);
            assertThat(r.body()).isEqualTo("a");
        }
        assertThat(a.count("GET /api/projects")).isEqualTo(4);
        assertThat(router.healthy()).containsExactly("a");

        // Nobody healthy: 503.
        a.health.set(503);
        router.pollNow();
        HttpResponse<String> none = get(router, "/api/projects");
        assertThat(none.statusCode()).isEqualTo(503);
        assertThat(none.body()).contains("no healthy reader");
    }

    @Test
    void routerRefusesInternalPaths() throws Exception {
        FakeReader a = reader("a");
        RouterCommand.Router router = router(a);
        for (String path : List.of("/internal/shard-search", "/internal/term-stats", "/internal/",
                "/api/../internal/shard-search", "/api/%2e%2e/internal/shard-search", "//internal/shard-search",
                "/api/..;/internal/shard-search", "/api/%2Finternal", "/cluster/readers")) {
            for (String method : List.of("GET", "POST")) {
                HttpResponse<String> r = send(router, method, path, "{}");
                assertThat(r.statusCode()).as(method + " " + path).isIn(400, 404);
            }
        }
        assertThat(a.requests).as("nothing reached the reader").isEmpty();

        // The router never carries a cluster token to a reader, even one a client sent.
        HttpRequest withToken = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + router.port() + "/api/projects"))
                .header("X-Pharos-Cluster-Token", "stolen").build();
        assertThat(http.send(withToken, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        assertThat(a.headers.getLast().keySet()).noneMatch(h -> h.equalsIgnoreCase("X-Pharos-Cluster-Token"));
    }

    /**
     * End to end with real reader web servers: a reader that starts draining finishes the request
     * the router sent it, the router stops sending it new ones at its next poll, and once every
     * reader drains new requests see 503 through the router.
     */
    @Test
    void drainingReaderFinishesInFlightAndNewRequestsSee503ThroughTheRouter() throws Exception {
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        List<ReaderHeartbeat> live = new ArrayList<>();
        List<java.util.concurrent.atomic.AtomicBoolean> draining = new ArrayList<>();
        for (String name : List.of("a", "b")) {
            com.pharos.web.WebServer web = new com.pharos.web.WebServer(null, null, null, null, null);
            java.util.concurrent.atomic.AtomicBoolean d = new java.util.concurrent.atomic.AtomicBoolean();
            web.setDraining(d::get);
            draining.add(d);
            io.javalin.Javalin app = web.start(0);
            open.add(app::stop);
            app.get("/api/who", ctx -> ctx.result(name));
            app.get("/api/slow", ctx -> {
                entered.countDown();
                release.await(10, java.util.concurrent.TimeUnit.SECONDS);
                ctx.result("slow from " + name);
            });
            live.add(new ReaderHeartbeat(name, "http://127.0.0.1:" + app.port(), List.of("p"), false, null, 0, 0, 0L));
        }
        RouterCommand.Router router = new RouterCommand.Router(() -> live, Duration.ofHours(1), Duration.ofMillis(500),
                Duration.ofSeconds(10));
        open.add(router);
        router.start(0);
        router.pollNow();
        assertThat(router.healthy()).containsExactly("a", "b");
        try {
            var slow = http.sendAsync(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + router.port() + "/api/slow"))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            String busy = "a"; // the rotation's first request goes to the first reader by name
            draining.get(0).set(true); // a starts draining while it serves the slow request
            router.pollNow();
            assertThat(router.healthy()).containsExactly("b");
            for (int i = 0; i < 4; i++) assertThat(get(router, "/api/who").body()).isEqualTo("b");
            release.countDown();
            HttpResponse<String> done = slow.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(done.statusCode()).isEqualTo(200);
            assertThat(done.body()).isEqualTo("slow from " + busy);

            draining.get(1).set(true);
            router.pollNow();
            assertThat(get(router, "/api/who").statusCode()).as("every reader drains").isEqualTo(503);
            assertThat(get(router, "/health").statusCode()).isEqualTo(503);
        } finally {
            release.countDown();
        }
    }

    /** Sends one raw HTTP/1.1 request (headers the JDK client refuses to send); returns the status line and headers. */
    private static String raw(RouterCommand.Router router, String requestLineAndHeaders) throws Exception {
        try (java.net.Socket sock = new java.net.Socket("127.0.0.1", router.port())) {
            sock.setSoTimeout(10_000);
            sock.getOutputStream().write((requestLineAndHeaders + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            sock.getOutputStream().flush();
            return new String(sock.getInputStream().readAllBytes(), StandardCharsets.ISO_8859_1);
        }
    }

    /** Fix round 1 minors: bad query → 400; oversized answer → 502; X-Forwarded-For; Connection-named headers dropped. */
    @Test
    void routerHardening() throws Exception {
        FakeReader a = reader("a");
        RouterCommand.Router router = router(a);

        String bad = raw(router, "GET /api/search?q=a|b{c} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n");
        assertThat(bad).as(bad).startsWith("HTTP/1.1 400");
        assertThat(a.requests).isEmpty();

        String hop = raw(router, "GET /api/projects HTTP/1.1\r\nHost: x\r\nConnection: close, X-Hop-Secret\r\n"
                + "X-Hop-Secret: s3cret\r\nX-Keep: kept\r\n");
        assertThat(hop).startsWith("HTTP/1.1 200");
        Map<String, List<String>> seen = a.headers.getLast();
        assertThat(seen.keySet()).noneMatch(h -> h.equalsIgnoreCase("X-Hop-Secret"));
        assertThat(seen.entrySet()).anySatisfy(e -> {
            assertThat(e.getKey()).isEqualToIgnoringCase("X-Keep");
            assertThat(e.getValue()).containsExactly("kept");
        });
        assertThat(seen.entrySet()).anySatisfy(e -> {
            assertThat(e.getKey()).isEqualToIgnoringCase("X-Forwarded-For");
            assertThat(e.getValue()).containsExactly("127.0.0.1");
        });

        HttpRequest chained = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + router.port() + "/api/projects"))
                .header("X-Forwarded-For", "10.0.0.7").build();
        assertThat(http.send(chained, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        assertThat(a.headers.getLast().entrySet()).anySatisfy(e -> {
            assertThat(e.getKey()).isEqualToIgnoringCase("X-Forwarded-For");
            assertThat(e.getValue()).containsExactly("10.0.0.7, 127.0.0.1");
        });

        router.setMaxResponseBytes(4);
        HttpResponse<String> big = get(router, "/api/projects"); // "a" fits
        assertThat(big.statusCode()).isEqualTo(200);
        a.statusFor.clear();
        FakeReader longName = reader("a-reader-with-a-long-name");
        RouterCommand.Router capped = router(longName);
        capped.setMaxResponseBytes(4);
        HttpResponse<String> over = get(capped, "/api/projects");
        assertThat(over.statusCode()).as("an answer over the cap").isEqualTo(502);
        assertThat(over.body()).contains("exceeds");
    }

    /** Carried minor: the forwarded exchange has one total deadline, the body read included (as ShardClient). */
    @Test
    void routerBoundsTheWholeExchangeIncludingABodyThatStalls() throws Exception {
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newCachedThreadPool();
        open.add(pool::shutdownNow);
        FakeReader a = new FakeReader("a", pool);
        open.add(a);
        a.stallBodyMillis.put("/api/slow", 5_000);
        RouterCommand.Router router = new RouterCommand.Router(() -> List.of(a.heartbeat()), Duration.ofHours(1),
                Duration.ofMillis(500), Duration.ofMillis(800));
        open.add(router);
        router.start(0);
        router.pollNow();

        long t0 = System.nanoTime();
        HttpResponse<String> slow = get(router, "/api/slow");
        long tookMs = (System.nanoTime() - t0) / 1_000_000;
        assertThat(slow.statusCode()).as(slow.body()).isEqualTo(504);
        assertThat(tookMs).as("headers arrived at once; the stalled body is cut at the deadline").isLessThan(3_000);
        assertThat(a.count("GET /api/slow")).as("a request that may have run is not resent").isEqualTo(1);

        HttpResponse<String> fast = get(router, "/api/projects");
        assertThat(fast.statusCode()).isEqualTo(200);
        assertThat(fast.body()).isEqualTo("a");
    }
}
