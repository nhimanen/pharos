package com.pharos.cluster.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.config.ClusterConfig;
import com.pharos.web.WebServer;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.pharos.cluster.status.StatusFixtures.T0;
import static org.assertj.core.api.Assertions.assertThat;

/** Task 5: {@code /cluster/events} (SSE) and the webhook. */
class StatusEventsTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final List<AutoCloseable> open = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = open.size() - 1; i >= 0; i--) open.get(i).close();
    }

    /** A webhook sink answering {@code status} and recording each body. */
    private HttpServer sink(AtomicInteger status, List<String> bodies, AtomicInteger hits) throws Exception {
        HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        s.createContext("/hook", ex -> {
            hits.incrementAndGet();
            bodies.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            ex.sendResponseHeaders(status.get(), -1);
            ex.close();
        });
        s.start();
        open.add(() -> s.stop(0));
        return s;
    }

    private static StatusSnapshot readers(Instant t, double... qps) {
        List<StatusSnapshot.ReaderObs> rs = new ArrayList<>();
        for (int i = 0; i < qps.length; i++) {
            rs.add(new StatusSnapshot.ReaderObs(StatusFixtures.reader("r" + (i + 1), qps[i], 20L), t));
        }
        return new StatusSnapshot(StatusSnapshot.CLUSTER, t, List.of(), List.of(), rs, Duration.ofSeconds(30));
    }

    @Test
    void sseStreamsAppearChangeClear(@TempDir Path tmp) throws Exception {
        AtomicInteger hookStatus = new AtomicInteger(204);
        List<String> hookBodies = new CopyOnWriteArrayList<>();
        AtomicInteger hits = new AtomicInteger();
        HttpServer hook = sink(hookStatus, hookBodies, hits);

        ClusterConfig.StatusConfig sc = new ClusterConfig.StatusConfig();
        sc.getWindows().setAddReadersSeconds(0); // shortened windows (as the Phase 4 gate does)
        sc.getWindows().setClearGraceSeconds(0);
        AtomicReference<StatusSnapshot> snap = new AtomicReference<>(readers(T0, 10, 10));
        StatusEvents events = new StatusEvents(Duration.ofMillis(200),
                new Webhook("http://127.0.0.1:" + hook.getAddress().getPort() + "/hook", 3, Duration.ofMillis(10),
                        Thread::sleep));
        StatusService service = new StatusService(snap::get, StatusStateStore.memory(), StatusSettings.of(sc), events,
                Duration.ofHours(1), Duration.ofHours(1), System::nanoTime);
        open.add(service);
        service.refresh(); // baseline: nothing in force

        WebServer web = new WebServer(null, null, null, null, null);
        web.setClusterStatus(service);
        io.javalin.Javalin app = web.start(0, tmp);
        open.add(app::stop);

        BlockingQueue<String[]> received = new LinkedBlockingQueue<>(); // {event, data}
        HttpClient http = HttpClient.newHttpClient();
        HttpResponse<java.util.stream.Stream<String>> resp = http.send(HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + app.port() + "/cluster/events"))
                .header("Accept", "text/event-stream").build(), // as EventSource sends it (Javalin requires it)
                HttpResponse.BodyHandlers.ofLines());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.headers().firstValue("content-type").orElse("")).startsWith("text/event-stream");
        List<String> comments = new CopyOnWriteArrayList<>();
        Thread readerThread = Thread.ofVirtual().start(() -> {
            String[] cur = new String[2];
            resp.body().forEach(line -> {
                if (line.startsWith(":")) comments.add(line);
                else if (line.startsWith("event:")) cur[0] = line.substring(6).trim();
                else if (line.startsWith("data:")) cur[1] = line.substring(5).trim();
                else if (line.isEmpty() && cur[0] != null) {
                    received.add(new String[]{cur[0], cur[1]});
                    cur[0] = null;
                    cur[1] = null;
                }
            });
        });
        open.add(readerThread::interrupt);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (events.clientCount() == 0 && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(events.clientCount()).isEqualTo(1);

        snap.set(readers(T0.plusSeconds(10), 60, 60)); // 60 > 50 QPS per reader: start 1
        service.refresh();
        String[] e1 = received.poll(5, TimeUnit.SECONDS);
        assertThat(e1).isNotNull();
        assertThat(e1[0]).isEqualTo("appeared");
        JsonNode j1 = JSON.readTree(e1[1]);
        assertThat(j1.path("type").asText()).isEqualTo("appeared");
        assertThat(j1.path("proposal").path("kind").asText()).isEqualTo(Proposals.ADD_READERS);
        assertThat(j1.path("proposal").path("action").asText()).isEqualTo("Start 1 reader");
        String id = j1.path("proposal").path("id").asText();
        assertThat(id).isEqualTo(Proposals.id(Proposals.ADD_READERS, "readers"));

        snap.set(readers(T0.plusSeconds(20), 80, 80)); // now 2 more are suggested
        service.refresh();
        String[] e2 = received.poll(5, TimeUnit.SECONDS);
        assertThat(e2[0]).isEqualTo("changed");
        assertThat(JSON.readTree(e2[1]).path("proposal").path("action").asText()).isEqualTo("Start 2 readers");
        assertThat(JSON.readTree(e2[1]).path("proposal").path("id").asText()).isEqualTo(id);

        snap.set(readers(T0.plusSeconds(30), 10, 10, 10)); // load gone and a reader added
        service.refresh();
        String[] e3 = received.poll(5, TimeUnit.SECONDS);
        assertThat(e3[0]).isEqualTo("layout");
        assertThat(JSON.readTree(e3[1]).path("summary").path("readers").asInt()).isEqualTo(3);
        String[] e4 = received.poll(5, TimeUnit.SECONDS);
        assertThat(e4[0]).isEqualTo("cleared");
        assertThat(JSON.readTree(e4[1]).path("proposal").path("id").asText()).isEqualTo(id);

        // the keep-alive comment flows while nothing happens
        Thread.sleep(500);
        assertThat(comments).anyMatch(c -> c.contains("keep-alive"));

        // the webhook received the same transitions, in order
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (hookBodies.size() < 4 && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(hookBodies).extracting(b -> JSON.readTree(b).path("type").asText())
                .containsExactly("appeared", "changed", "layout", "cleared");

        // /cluster/status serves the same service, no token needed
        HttpResponse<String> status = http.send(HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + app.port() + "/cluster/status")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(status.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(status.body()).path("layout").path("summary").path("readers").asInt()).isEqualTo(3);
        assertThat(web.inFlightRequests()).as("an open event stream is not drain work").isZero();
    }

    @Test
    void webhookRetriesThenGivesUp() throws Exception {
        AtomicInteger hookStatus = new AtomicInteger(500);
        List<String> bodies = new CopyOnWriteArrayList<>();
        AtomicInteger hits = new AtomicInteger();
        HttpServer hook = sink(hookStatus, bodies, hits);
        List<Long> sleeps = new CopyOnWriteArrayList<>();
        Webhook w = new Webhook("http://127.0.0.1:" + hook.getAddress().getPort() + "/hook?secret=x", 4,
                Duration.ofMillis(20), ms -> {
                    sleeps.add(ms);
                    Thread.sleep(ms);
                });
        open.add(w);
        StatusEvent e = new StatusEvent(StatusEvent.LAYOUT, null, new StatusState.Summary(1, 1, 1));

        long t0 = System.nanoTime();
        w.send(List.of(e));
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)).as("send only enqueues").isLessThan(100);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (w.givenUp() == 0 && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(w.givenUp()).isEqualTo(1);
        assertThat(hits.get()).as("1 try + 3 retries").isEqualTo(4);
        assertThat(sleeps).as("exponential back-off").containsExactly(20L, 40L, 80L);
        assertThat(bodies).allSatisfy(b -> assertThat(b).isEqualTo(e.toJson()));

        // the next event is tried afresh, and delivered once the sink recovers
        hookStatus.set(200);
        w.send(List.of(e));
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (w.delivered() == 0 && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(w.delivered()).isEqualTo(1);
        assertThat(hits.get()).isEqualTo(5);
    }

    @Test
    void webhookNeverBlocksStatusBuilding() throws Exception {
        // a sink that hangs: refreshes keep their pace
        HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        s.createContext("/hook", ex -> {
            try {
                Thread.sleep(5_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            ex.close();
        });
        s.start();
        open.add(() -> s.stop(0));
        ClusterConfig.StatusConfig sc = new ClusterConfig.StatusConfig();
        sc.getWindows().setAddReadersSeconds(0);
        sc.getWindows().setClearGraceSeconds(0);
        AtomicReference<StatusSnapshot> snap = new AtomicReference<>(readers(T0, 10));
        StatusEvents events = new StatusEvents(Duration.ofSeconds(15),
                new Webhook("http://127.0.0.1:" + s.getAddress().getPort() + "/hook"));
        StatusService service = new StatusService(snap::get, StatusStateStore.memory(), StatusSettings.of(sc), events,
                Duration.ofHours(1), Duration.ofHours(1), System::nanoTime);
        open.add(service);
        long t0 = System.nanoTime();
        for (int i = 1; i <= 20; i++) {
            snap.set(readers(T0.plusSeconds(i), i % 2 == 0 ? 90 : 1)); // appears and clears every refresh
            service.refresh();
        }
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)).isLessThan(2_000);
        assertThat(service.current().layout().readers()).hasSize(1);
        ReaderHeartbeat h = snap.get().readers().get(0).heartbeat();
        assertThat(h.node()).isEqualTo("r1");
    }

    /** A sink whose writes block until released: a client that never reads. */
    static final class StuckSink implements StatusEvents.Sink {
        final CountDownLatch released = new CountDownLatch(1);
        final AtomicInteger writes = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();

        private void block() {
            writes.incrementAndGet();
            try {
                released.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted");
            }
        }

        @Override public void event(String type, String data) { block(); }

        @Override public void comment(String text) { block(); }

        @Override public void onClose(Runnable r) { }

        @Override public void close() { closes.incrementAndGet(); released.countDown(); }
    }

    /** A sink that records what it gets. */
    static final class ListSink implements StatusEvents.Sink {
        final List<String> got = new CopyOnWriteArrayList<>();
        final AtomicInteger closes = new AtomicInteger();

        @Override public void event(String type, String data) { got.add(type); }

        @Override public void comment(String text) { }

        @Override public void onClose(Runnable r) { }

        @Override public void close() { closes.incrementAndGet(); }
    }

    @Test
    void slowClientIsDroppedAndNeverStallsStatus(@TempDir Path tmp) throws Exception {
        ClusterConfig.StatusConfig sc = new ClusterConfig.StatusConfig();
        sc.getWindows().setAddReadersSeconds(0);
        sc.getWindows().setClearGraceSeconds(0);
        AtomicReference<StatusSnapshot> snap = new AtomicReference<>(readers(T0, 10));
        StatusEvents events = new StatusEvents(Duration.ofHours(1), null);
        StatusService service = new StatusService(snap::get, StatusStateStore.memory(), StatusSettings.of(sc), events,
                Duration.ofHours(1), Duration.ZERO, System::nanoTime);
        open.add(service);
        service.refresh();
        StuckSink stuck = new StuckSink();
        ListSink healthy = new ListSink();
        assertThat(events.tryReserve()).isTrue();
        events.add(stuck);
        assertThat(events.tryReserve()).isTrue();
        events.add(healthy);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (stuck.writes.get() == 0 && System.nanoTime() < deadline) Thread.sleep(5);
        assertThat(stuck.writes.get()).as("the stuck client's sender is blocked in its first write").isEqualTo(1);

        WebServer web = new WebServer(null, null, null, null, null);
        web.setClusterStatus(service);
        io.javalin.Javalin app = web.start(0, tmp);
        open.add(app::stop);
        HttpClient http = HttpClient.newHttpClient();
        long t0 = System.nanoTime();
        for (int i = 1; i <= 60; i++) { // appears and clears every refresh: 60 events
            snap.set(readers(T0.plusSeconds(i), i % 2 == 1 ? 90 : 1));
            service.refresh();
            if (i % 10 == 0) {
                HttpResponse<String> st = http.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + app.port() + "/cluster/status")).build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(st.statusCode()).isEqualTo(200);
            }
        }
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)).as("refreshes and GETs stay prompt")
                .isLessThan(5_000);
        assertThat(stuck.closes.get()).as("its queue overflowed: dropped and closed").isEqualTo(1);
        assertThat(events.clientCount()).isEqualTo(1);
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (healthy.got.size() < 60 && System.nanoTime() < deadline) Thread.sleep(5);
        assertThat(healthy.got).as("the reading client got every event").hasSize(60);
        assertThat(events.tryReserve()).as("the dropped client's slot is free again").isTrue();
        events.release();
    }

    @Test
    void eventStreamsAreCappedPerNode(@TempDir Path tmp) throws Exception {
        StatusEvents events = new StatusEvents(Duration.ofHours(1), null);
        StatusService service = new StatusService(() -> readers(T0, 1), StatusStateStore.memory(),
                StatusSettings.defaults(), events, Duration.ofHours(1), Duration.ZERO, System::nanoTime);
        open.add(service);
        WebServer web = new WebServer(null, null, null, null, null);
        web.setClusterStatus(service);
        io.javalin.Javalin app = web.start(0, tmp);
        open.add(app::stop);
        for (int i = 0; i < StatusEvents.MAX_CLIENTS; i++) assertThat(events.tryReserve()).isTrue();
        assertThat(events.tryReserve()).isFalse();
        HttpClient http = HttpClient.newHttpClient();
        URI uri = URI.create("http://127.0.0.1:" + app.port() + "/cluster/events");
        HttpResponse<String> full = http.send(HttpRequest.newBuilder(uri).header("Accept", "text/event-stream")
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(full.statusCode()).isEqualTo(503);
        assertThat(full.body()).contains("max 64");
        HttpResponse<String> noAccept = http.send(HttpRequest.newBuilder(uri).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(noAccept.statusCode()).isEqualTo(406);
        events.release();
        HttpResponse<java.io.InputStream> ok = http.send(HttpRequest.newBuilder(uri)
                .header("Accept", "text/event-stream").build(), HttpResponse.BodyHandlers.ofInputStream());
        assertThat(ok.statusCode()).isEqualTo(200);
        ok.body().close();
    }
}
