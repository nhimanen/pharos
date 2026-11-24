package com.pharos.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Task 6: a draining reader answers /health 503 at once, but lets its in-flight requests finish. */
class DrainTest {

    @TempDir Path tmp;

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static HttpResponse<String> get(io.javalin.Javalin app, String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.port() + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void drainingAnswers503ButFinishesInFlight() throws Exception {
        WebServer server = new WebServer(null, null, null, null, null);
        AtomicBoolean draining = new AtomicBoolean();
        server.setDraining(draining::get);
        io.javalin.Javalin app = server.start(0);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        app.get("/api/slow", ctx -> {
            entered.countDown();
            release.await(10, TimeUnit.SECONDS);
            ctx.result("done");
        });
        try {
            assertThat(get(app, "/health").statusCode()).isEqualTo(200);
            assertThat(server.inFlightRequests()).isZero();

            CompletableFuture<HttpResponse<String>> slow = HTTP.sendAsync(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + app.port() + "/api/slow")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(server.inFlightRequests()).isEqualTo(1);

            draining.set(true);
            HttpResponse<String> health = get(app, "/health");
            assertThat(health.statusCode()).as("routers and load balancers stop sending").isEqualTo(503);
            assertThat(health.body()).contains("\"status\":\"draining\"");
            assertThat(server.inFlightRequests()).as("a /health probe is not in flight work").isEqualTo(1);
            assertThat(server.awaitIdle(Duration.ofMillis(200))).as("the request still runs").isFalse();

            CompletableFuture<Boolean> idle = CompletableFuture.supplyAsync(() -> server.awaitIdle(Duration.ofSeconds(10)));
            release.countDown();
            HttpResponse<String> done = slow.get(10, TimeUnit.SECONDS);
            assertThat(done.statusCode()).as("the in-flight request finishes normally").isEqualTo(200);
            assertThat(done.body()).isEqualTo("done");
            assertThat(idle.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(server.inFlightRequests()).isZero();
        } finally {
            release.countDown();
            app.stop();
        }
    }

    /** Draining wins over every other state: a warming or degraded node that drains answers draining. */
    @Test
    void drainingComposesWithTheOtherHealthStates() {
        WebServer server = new WebServer(null, null, null, null, null);
        AtomicBoolean draining = new AtomicBoolean(true);
        AtomicBoolean ready = new AtomicBoolean(false);
        server.setDraining(draining::get);
        server.setReadiness(ready::get);
        server.setDegradedShards(() -> List.of("java"));
        assertThat(server.healthStatus()).isEqualTo(503);
        assertThat(server.health()).containsEntry("status", "draining").containsEntry("ready", false);
        ready.set(true);
        assertThat(server.health()).containsEntry("status", "draining").containsEntry("ready", true);
        assertThat(server.healthStatus()).isEqualTo(503);
        draining.set(false);
        assertThat(server.health()).containsEntry("status", "degraded");
        assertThat(server.healthStatus()).isEqualTo(200);
    }

    private static Optional<ReaderHeartbeat> heartbeat(InMemoryObjectStore store, String node) throws Exception {
        var o = store.get("pharos/catalog/readers/" + node + ".json");
        return o.isEmpty() ? Optional.empty() : Optional.of(new ObjectMapper().readValue(o.get().data(), ReaderHeartbeat.class));
    }

    private static void await(String what, java.util.concurrent.Callable<Boolean> cond) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!cond.call()) {
            if (System.nanoTime() > deadline) throw new AssertionError("timed out waiting: " + what);
            Thread.sleep(20);
        }
    }

    /**
     * The shutdown drain publishes {@code draining} in the heartbeat first (peers stop routing to the
     * node), waits for the in-flight requests, and only then stops (removing the heartbeat).
     * Fix round 1 (I2): even with nothing in flight it stays up for a quiet period (a router poll plus
     * its probe, at least one heartbeat), so the draining heartbeat and /health 503 are observable
     * before it closes; requests arriving meanwhile are still served.
     */
    @Test
    void heartbeatShowsDrainingBeforeStop() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        var reader = ClusterFixture.reader(tmp.resolve("r1"), store, "b", "r1", List.of(), c -> c.setDrainSeconds(20));
        WebServer server = new WebServer(null, null, null, null, null);
        server.setDraining(reader.runtime()::draining);
        reader.runtime().setInFlight(server::inFlightRequests);
        io.javalin.Javalin app = server.start(0);
        app.get("/api/ping", ctx -> {
            Thread.sleep(5);
            ctx.result("pong");
        });
        AtomicBoolean trickling = new AtomicBoolean(true);
        AtomicInteger served = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        Thread trickle = new Thread(() -> {
            while (trickling.get()) {
                try {
                    if (get(app, "/api/ping").statusCode() == 200) served.incrementAndGet();
                    else failed.incrementAndGet();
                    Thread.sleep(50);
                } catch (Exception e) {
                    failed.incrementAndGet();
                }
            }
        }, "trickle");
        try {
            reader.runtime().startServing(true);
            await("first heartbeat", () -> heartbeat(store, "r1").isPresent());
            assertThat(heartbeat(store, "r1").orElseThrow().draining()).isFalse();
            assertThat(reader.runtime().draining()).isFalse();
            trickle.start();

            long start = System.nanoTime();
            CompletableFuture<Void> stop = CompletableFuture.runAsync(reader.runtime()::drainAndClose);
            await("draining heartbeat", () -> heartbeat(store, "r1").map(ReaderHeartbeat::draining).orElse(false));
            assertThat(stop).as("not closed before the draining heartbeat is out").isNotDone();
            ReaderHeartbeat hb = heartbeat(store, "r1").orElseThrow();
            assertThat(hb.state()).isEqualTo(ReaderHeartbeat.DRAINING);
            HttpResponse<String> health = get(app, "/health");
            assertThat(health.statusCode()).as("/health 503 observable before close").isEqualTo(503);
            assertThat(stop).isNotDone();

            Thread.sleep(1_500);
            assertThat(stop).as("the quiet period holds the stop although requests come and go").isNotDone();
            int servedWhileDraining = served.get();
            trickling.set(false);
            trickle.join(5_000);
            assertThat(failed.get()).as("requests arriving while draining are served").isZero();
            assertThat(servedWhileDraining).isPositive();

            stop.get(15, TimeUnit.SECONDS);
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertThat(millis).as("quiet period (>= 4 s), then a quiet final second").isBetween(4_000L, 12_000L);
            assertThat(heartbeat(store, "r1")).as("stopped: the heartbeat is removed").isEmpty();
        } finally {
            trickling.set(false);
            app.stop();
            reader.runtime().close();
            reader.lucene().close();
        }
    }

    /** Waiting in-flight work holds the stop, and the stop follows once it finishes (and the quiet period). */
    @Test
    void inFlightWorkHoldsTheStop() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        var reader = ClusterFixture.reader(tmp.resolve("r3"), store, "b", "r3", List.of(), c -> c.setDrainSeconds(30));
        AtomicInteger inFlight = new AtomicInteger(1);
        reader.runtime().setInFlight(inFlight::get);
        try {
            reader.runtime().startServing(true);
            CompletableFuture<Void> stop = CompletableFuture.runAsync(reader.runtime()::drainAndClose);
            Thread.sleep(6_000);
            assertThat(stop).as("past the quiet period, still waiting for the request").isNotDone();
            assertThat(heartbeat(store, "r3")).isPresent();
            inFlight.set(0);
            stop.get(5, TimeUnit.SECONDS);
            assertThat(heartbeat(store, "r3")).isEmpty();
        } finally {
            reader.runtime().close();
            reader.lucene().close();
        }
    }

    // ---------------------------------------------------------------- Fix round 1 (I1): signal order

    /**
     * Child JVM: a reader daemon wired by {@link com.pharos.cli.WebCommand} (as {@code pharos web}),
     * with Main's fallback shutdown hook, an {@code /api/slow} route, and a "late" shutdown hook that
     * stands in for ONNX Runtime's environment closer: it reports whether the drain had already
     * finished (heartbeat removed, slow request done) when shutdown hooks started.
     */
    public static final class SignalProbe {
        public static void main(String[] args) throws Exception {
            Path home = Path.of(args[0]);
            int port = Integer.parseInt(args[1]);
            InMemoryObjectStore store = new InMemoryObjectStore();
            var reader = ClusterFixture.reader(home, store, "b", "probe", List.of(), c -> c.setDrainSeconds(15));
            var runtime = reader.runtime();
            Runtime.getRuntime().addShutdownHook(new Thread(runtime::drainAndClose, "main-fallback")); // as Main
            AtomicBoolean slowDone = new AtomicBoolean();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                boolean hb = store.get("pharos/catalog/readers/probe.json").isPresent();
                System.out.println("LATE_HOOK heartbeat=" + (hb ? "present" : "absent") + " slowDone=" + slowDone.get());
                System.out.flush();
            }, "late-hook"));
            WebServer web = new WebServer(null, null, null, null, null, com.pharos.cluster.NodeRole.READER) {
                @Override
                public io.javalin.Javalin start(int p) {
                    io.javalin.Javalin app = super.start(p);
                    app.get("/api/slow", ctx -> {
                        System.out.println("SLOW_ENTERED");
                        System.out.flush();
                        Thread.sleep(2_500);
                        slowDone.set(true);
                        ctx.result("slow done");
                    });
                    System.out.println("READY");
                    System.out.flush();
                    return app;
                }
            };
            com.pharos.cli.WebCommand cmd = new com.pharos.cli.WebCommand(web, null, runtime);
            new picocli.CommandLine(cmd).parseArgs("--port", Integer.toString(port), "--no-open");
            System.exit(cmd.call()); // serves until a signal
        }
    }

    @Test
    @org.junit.jupiter.api.condition.EnabledOnOs({org.junit.jupiter.api.condition.OS.LINUX,
            org.junit.jupiter.api.condition.OS.MAC})
    void sigtermDrainsBeforeAnyShutdownHookRuns() throws Exception {
        int port;
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Path home = tmp.resolve("probe");
        Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dpharos.home=" + home.resolve("pharos-home"), "-cp", System.getProperty("java.class.path"),
                SignalProbe.class.getName(), home.toString(), Integer.toString(port))
                .redirectErrorStream(true).start();
        List<String> out = new java.util.concurrent.CopyOnWriteArrayList<>();
        Thread pump = new Thread(() -> {
            try (var in = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()))) {
                String line;
                while ((line = in.readLine()) != null) out.add(line);
            } catch (java.io.IOException ignored) {
                // the child exited
            }
        }, "probe-output");
        pump.setDaemon(true);
        pump.start();
        try {
            await("probe ready", () -> out.contains("READY"));
            await("probe serving", () -> {
                try {
                    return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health")).build(),
                            HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
                } catch (java.io.IOException e) {
                    return false;
                }
            });
            CompletableFuture<HttpResponse<String>> slow = HTTP.sendAsync(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + port + "/api/slow")).build(), HttpResponse.BodyHandlers.ofString());
            await("slow request running", () -> out.contains("SLOW_ENTERED"));
            new ProcessBuilder("kill", "-TERM", Long.toString(p.pid())).start().waitFor();

            HttpResponse<String> done = slow.get(20, TimeUnit.SECONDS);
            assertThat(done.statusCode()).as("the in-flight request completes").isEqualTo(200);
            assertThat(done.body()).isEqualTo("slow done");
            assertThat(p.waitFor(40, TimeUnit.SECONDS)).as("the process exits").isTrue();
            assertThat(p.exitValue()).as("143, as for SIGTERM without the handler").isEqualTo(143);
            pump.join(5_000);
            assertThat(out).as(String.join("\n", out))
                    .contains("LATE_HOOK heartbeat=absent slowDone=true");
        } finally {
            p.destroyForcibly();
        }
    }

    /** In-flight work that never ends holds the stop for drainSeconds at most. */
    @Test
    void drainGivesUpAfterDrainSeconds() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        var reader = ClusterFixture.reader(tmp.resolve("r2"), store, "b", "r2", List.of(), c -> c.setDrainSeconds(1));
        reader.runtime().setInFlight(() -> 3);
        try {
            reader.runtime().startServing(true);
            long start = System.nanoTime();
            reader.runtime().drainAndClose();
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertThat(millis).isBetween(900L, 5_000L);
            assertThat(heartbeat(store, "r2")).isEmpty();
        } finally {
            reader.runtime().close();
            reader.lucene().close();
        }
    }

    // ---------------------------------------------------------------- final review I3: mcp-server on a reader

    /**
     * I3: {@code pharos mcp-server} on a reader ({@code startServing(false)}, no {@code /internal/*})
     * writes no heartbeat, and its exit neither marks draining nor deletes the heartbeat the web
     * reader of the same node id (another process, same config) keeps in the bucket. Nor does it wait
     * for a drain nobody can observe.
     */
    @Test
    void mcpModeServingWritesNoHeartbeatAndLeavesAnotherProcessesAlone() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        var reader = ClusterFixture.reader(tmp.resolve("mcp"), store, "b", "shared", List.of(), c -> c.setDrainSeconds(20));
        String key = "pharos/catalog/readers/shared.json";
        byte[] webReaders = new ObjectMapper().writeValueAsBytes(new ReaderHeartbeat("shared",
                "http://web-reader:7171", List.of("java"), false, null, 2, 1.0, 0L, ReaderHeartbeat.READY, List.of()));
        store.put(key, webReaders);
        try {
            reader.runtime().startServing(false);
            Thread.sleep(2_500); // heartbeatSeconds is 1: an advertising process would have written twice
            assertThat(new String(store.get(key).orElseThrow().data())).as("not overwritten").isEqualTo(new String(webReaders));
            assertThat(reader.runtime().liveReaders()).as("no membership at all").isEmpty();

            long start = System.nanoTime();
            reader.runtime().drainAndClose(); // the MCP client closed stdio: Main's fallback hook
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertThat(millis).as("no drain wait in a process nobody routes to").isLessThan(2_000L);
            assertThat(store.get(key)).as("the web reader's heartbeat is not deleted").isPresent();
            assertThat(new String(store.get(key).orElseThrow().data())).as("nor marked draining").isEqualTo(new String(webReaders));
        } finally {
            reader.runtime().close();
            reader.lucene().close();
        }
    }

    // ---------------------------------------------------------------- final review I2: rejected /internal/* calls

    private static int post(io.javalin.Javalin app, String path, String token, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.port() + path))
                .POST(HttpRequest.BodyPublishers.ofString(body)).header("Content-Type", "application/json");
        if (token != null) b.header(com.pharos.cluster.internal.InternalApi.TOKEN_HEADER, token);
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    /**
     * I2: an {@code /internal/*} call rejected by the API's before-filter (401 without the token, 413
     * over the size cap, 503 before the shard service is wired) still ends its in-flight count, so a
     * later shutdown drain is not held for the full {@code drainSeconds}.
     */
    @Test
    void rejectedInternalCallsDoNotLeakTheInFlightCount() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        var reader = ClusterFixture.reader(tmp.resolve("rej"), store, "b", "rej", List.of(), c -> c.setDrainSeconds(30));
        WebServer server = new WebServer(null, null, null, null, null, com.pharos.cluster.NodeRole.READER);
        server.setDraining(reader.runtime()::draining);
        server.setInternalApi(reader.runtime().internalApi()); // no service yet: authenticated calls get 503
        reader.runtime().setInFlight(server::inFlightRequests);
        io.javalin.Javalin app = server.start(0);
        try {
            for (int i = 0; i < 3; i++) assertThat(post(app, "/internal/term-stats", null, "{}")).isEqualTo(401);
            assertThat(post(app, "/internal/term-stats", "wrong", "{}")).isEqualTo(401);
            HttpResponse<String> unauthorized = HTTP.send(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + app.port() + "/internal/term-stats"))
                    .POST(HttpRequest.BodyPublishers.ofString("{}")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(unauthorized.body()).as("the body is unchanged").isEqualTo("{\"error\":\"unauthorized\"}");
            assertThat(unauthorized.headers().firstValue("Content-Type").orElse("")).startsWith("application/json");
            String big = "{\"shard\":\"java\",\"pad\":\"" + "x".repeat(com.pharos.cluster.internal.InternalApi.MAX_BODY_BYTES) + "\"}";
            assertThat(post(app, "/internal/term-stats", "test-token", big)).isEqualTo(413);
            assertThat(post(app, "/internal/term-stats", "test-token", "{\"shard\":\"java\"}")).isEqualTo(503);
            assertThat(server.inFlightRequests()).as("every rejection ended its count").isZero();

            reader.runtime().startServing(true);
            long start = System.nanoTime();
            reader.runtime().drainAndClose();
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertThat(millis).as("the quiet period, not drainSeconds (30 s)").isLessThan(12_000L);
        } finally {
            app.stop();
            reader.runtime().close();
            reader.lucene().close();
        }
    }

    /** Final review minor 5: GET /cluster/readers (the internal topology) needs the cluster token. */
    @Test
    void clusterReadersNeedsTheClusterToken() throws Exception {
        WebServer server = new WebServer(null, null, null, null, null, com.pharos.cluster.NodeRole.READER);
        server.setLiveReaders(() -> List.of(new com.pharos.cluster.membership.ReaderRegistry.LiveReader(
                new ReaderHeartbeat("r1", "http://10.0.0.1:7171", List.of("p"), false, null, 0, 0, 1L), 1)));
        server.setInternalApi(new com.pharos.cluster.internal.InternalApi("test-token"));
        io.javalin.Javalin app = server.start(0);
        try {
            HttpResponse<String> anonymous = get(app, "/cluster/readers");
            assertThat(anonymous.statusCode()).isEqualTo(401);
            assertThat(anonymous.body()).doesNotContain("10.0.0.1");
            HttpResponse<String> wrong = HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.port()
                    + "/cluster/readers")).header(com.pharos.cluster.internal.InternalApi.TOKEN_HEADER, "nope").build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(wrong.statusCode()).isEqualTo(401);
            HttpResponse<String> member = HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + app.port()
                    + "/cluster/readers")).header(com.pharos.cluster.internal.InternalApi.TOKEN_HEADER, "test-token").build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(member.statusCode()).isEqualTo(200);
            assertThat(member.body()).contains("\"advertiseUrl\":\"http://10.0.0.1:7171\"");
            assertThat(server.inFlightRequests()).isZero();
        } finally {
            app.stop();
        }
    }
}
