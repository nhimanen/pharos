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
     */
    @Test
    void heartbeatShowsDrainingBeforeStop() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        var reader = ClusterFixture.reader(tmp.resolve("r1"), store, "b", "r1", List.of(), c -> c.setDrainSeconds(20));
        AtomicInteger inFlight = new AtomicInteger(1);
        reader.runtime().setInFlight(inFlight::get);
        try {
            reader.runtime().startServing();
            await("first heartbeat", () -> heartbeat(store, "r1").isPresent());
            assertThat(heartbeat(store, "r1").orElseThrow().draining()).isFalse();
            assertThat(reader.runtime().draining()).isFalse();

            long start = System.nanoTime();
            CompletableFuture<Void> stop = CompletableFuture.runAsync(reader.runtime()::drainAndClose);
            await("draining heartbeat", () -> heartbeat(store, "r1").map(ReaderHeartbeat::draining).orElse(false));
            ReaderHeartbeat hb = heartbeat(store, "r1").orElseThrow();
            assertThat(hb.state()).isEqualTo(ReaderHeartbeat.DRAINING);
            assertThat(reader.runtime().draining()).isTrue();
            Thread.sleep(300);
            assertThat(stop).as("waits for the in-flight request").isNotDone();
            assertThat(heartbeat(store, "r1")).as("still announced while draining").isPresent();

            inFlight.set(0);
            stop.get(10, TimeUnit.SECONDS);
            assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).isLessThan(10);
            assertThat(heartbeat(store, "r1")).as("stopped: the heartbeat is removed").isEmpty();
        } finally {
            reader.runtime().close();
            reader.lucene().close();
        }
    }

    /** In-flight work that never ends holds the stop for drainSeconds at most. */
    @Test
    void drainGivesUpAfterDrainSeconds() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        var reader = ClusterFixture.reader(tmp.resolve("r2"), store, "b", "r2", List.of(), c -> c.setDrainSeconds(1));
        reader.runtime().setInFlight(() -> 3);
        try {
            reader.runtime().startServing();
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
}
