package com.pharos.operator.status;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.cluster.status.StatusState;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class StatusClientTest {

    static final String TOKEN = "0123456789abcdef0123456789abcdef";
    static final String READERS = "demo-readers-hl.pharos.svc";
    static final String WRITERS = "demo-writers-hl.pharos.svc";
    static final Instant NOW = Instant.parse("2026-11-23T10:00:00Z");

    private final List<HttpServer> servers = new ArrayList<>();
    private final List<ClusterStatusClient> clients = new ArrayList<>();

    @AfterEach
    void tearDown() {
        clients.forEach(ClusterStatusClient::close);
        servers.forEach(s -> s.stop(0));
    }

    /** A pharos node on 127.0.0.1: {@code /cluster/status} answers {@code code}, with the token only. */
    private HttpServer node(int code, List<String> tokensSeen) throws IOException {
        HttpServer s = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        s.createContext("/cluster/status", ex -> {
            String token = ex.getRequestHeaders().getFirst(ClusterStatusClient.TOKEN_HEADER);
            tokensSeen.add(String.valueOf(token));
            if (code != 200) {
                respond(ex, code, "{\"error\":\"warming\"}");
            } else if (!TOKEN.equals(token)) {
                respond(ex, 401, "{\"error\":\"unauthorized\"}");
            } else {
                respond(ex, 200, new String(statusJson(), StandardCharsets.UTF_8));
            }
        });
        s.setExecutor(Executors.newCachedThreadPool()); // an open event stream must not block the status
        s.start();
        servers.add(s);
        return s;
    }

    private static byte[] statusJson() throws IOException {
        var p = new ClusterStatus.Proposal("id1", "add-readers", "readers", "qps above target",
                Map.of("qpsPerReader", 80), "start 1 reader", 1, "2026-11-23T09:55:00Z");
        return new ClusterStatus("cluster", "2026-11-23T10:00:00Z",
                new ClusterStatus.Layout(new StatusState.Summary(0, 1, 1), List.of(), List.of(), List.of()),
                new ClusterStatus.BacklogSummary(0, null), List.of(), List.of(p)).toJson();
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(b);
        }
    }

    private static URI base(HttpServer s) {
        return URI.create("http://127.0.0.1:" + s.getAddress().getPort());
    }

    private static URI closedPort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return URI.create("http://127.0.0.1:" + ss.getLocalPort());
        }
    }

    private ClusterStatusClient client(Map<String, List<URI>> dns, Duration refresh, Runnable onUpdate) {
        ClusterStatusClient c = new ClusterStatusClient("pharos/demo", List.of(READERS, WRITERS), host -> {
            List<URI> uris = dns.get(host);
            if (uris == null) throw new UnknownHostException(host);
            return uris;
        }, ClusterStatusClient.httpClient(), Clock.fixed(NOW, ZoneOffset.UTC), refresh, onUpdate);
        clients.add(c);
        return c;
    }

    @Test
    void statusClientFallsBackToWriter() throws Exception {
        List<String> readerTokens = new CopyOnWriteArrayList<>();
        List<String> writerTokens = new CopyOnWriteArrayList<>();
        HttpServer warmingReader = node(503, readerTokens);
        HttpServer writer = node(200, writerTokens);
        ClusterStatusClient c = client(Map.of(READERS, List.of(closedPort(), base(warmingReader)),
                WRITERS, List.of(base(writer))), Duration.ofHours(1), null);
        c.token(TOKEN + "\n");

        StatusReading r = c.fetch().orElseThrow();
        assertThat(readerTokens).containsExactly(TOKEN); // the readers were asked first
        assertThat(writerTokens).containsExactly(TOKEN);
        assertThat(r.from()).isEqualTo("127.0.0.1:" + writer.getAddress().getPort());
        assertThat(r.fetchedAt()).isEqualTo(NOW);
        assertThat(r.status().proposals()).singleElement().satisfies(p -> {
            assertThat(p.kind()).isEqualTo("add-readers");
            assertThat(p.delta()).isEqualTo(1);
        });
        assertThat(c.latest()).contains(r);
        assertThat(c.problem()).isNull();

        // a reader that answers is used and the writers are not asked
        HttpServer reader = node(200, readerTokens);
        ClusterStatusClient c2 = client(Map.of(READERS, List.of(base(reader)), WRITERS, List.of(base(writer))),
                Duration.ofHours(1), null);
        c2.token(TOKEN);
        writerTokens.clear();
        assertThat(c2.fetch()).isPresent();
        assertThat(writerTokens).isEmpty();
    }

    @Test
    void noNodeAnsweringLeavesNoReading() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        HttpServer down = node(503, seen);
        ClusterStatusClient c = client(Map.of(READERS, List.of(base(down)), WRITERS, List.of(closedPort())),
                Duration.ofHours(1), null);
        // no token, no request
        assertThat(c.fetch()).isEmpty();
        assertThat(seen).isEmpty();
        assertThat(c.problem()).contains("token");

        c.token(TOKEN);
        assertThat(c.fetch()).isEmpty();
        assertThat(c.latest()).isEmpty();
        assertThat(c.problem()).isEqualTo("no pharos node answered /cluster/status");
        // the services do not resolve at all (nothing created yet)
        ClusterStatusClient nothing = client(Map.of(), Duration.ofHours(1), null);
        nothing.token(TOKEN);
        assertThat(nothing.fetch()).isEmpty();
    }

    @Test
    void anEventTriggersAReread() throws Exception {
        List<String> seen = new CopyOnWriteArrayList<>();
        HttpServer s = node(200, seen);
        CountDownLatch subscribed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger eventTokens = new AtomicInteger();
        s.createContext("/cluster/events", ex -> {
            if (TOKEN.equals(ex.getRequestHeaders().getFirst(ClusterStatusClient.TOKEN_HEADER))
                    && "text/event-stream".equals(ex.getRequestHeaders().getFirst("Accept"))) {
                eventTokens.incrementAndGet();
            }
            ex.getResponseHeaders().set("Content-Type", "text/event-stream");
            ex.sendResponseHeaders(200, 0);
            OutputStream out = ex.getResponseBody();
            out.write(": keep-alive\n\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            subscribed.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            out.write("event: appeared\ndata: {\"type\":\"appeared\"}\n\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            out.close();
        });
        AtomicInteger updates = new AtomicInteger();
        ClusterStatusClient c = client(Map.of(READERS, List.of(base(s)), WRITERS, List.of()), Duration.ofHours(1),
                updates::incrementAndGet);
        c.token(TOKEN);
        c.start();

        assertThat(subscribed.await(10, TimeUnit.SECONDS)).isTrue();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (updates.get() < 1 && System.nanoTime() < deadline) Thread.sleep(20);
        assertThat(updates.get()).isEqualTo(1); // the first read; the next is an hour away
        release.countDown();
        while (updates.get() < 2 && System.nanoTime() < deadline) Thread.sleep(20);
        assertThat(updates.get()).isEqualTo(2); // the event woke the poller
        assertThat(eventTokens.get()).isEqualTo(1);
        assertThat(c.latest()).isPresent();
    }
}
