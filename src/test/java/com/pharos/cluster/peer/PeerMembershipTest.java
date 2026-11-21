package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.cli.RouterCommand;
import com.pharos.cluster.BackendKind;
import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.InternalApi;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.routing.ShardRouter;
import com.pharos.cluster.status.ClusterStatus;
import com.pharos.cluster.status.StatusService;
import com.pharos.cluster.status.StatusSettings;
import com.pharos.cluster.status.StatusState;
import com.pharos.cluster.status.StatusStateStore;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.config.ProjectRegistry;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static com.pharos.cluster.peer.PeerWriterFixture.TOKEN;
import static com.pharos.cluster.peer.PeerWriterFixture.heartbeat;
import static org.assertj.core.api.Assertions.assertThat;

/** Phase 5 Task 2b: reader membership and the status hysteresis state without a bucket. */
class PeerMembershipTest {

    private static final long CLOCK_A = Instant.parse("2026-11-17T10:00:00Z").toEpochMilli();
    private static final long CLOCK_B = Instant.parse("2026-11-17T12:00:00Z").toEpochMilli();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir Path tmp;
    private final List<AutoCloseable> open = new ArrayList<>();
    private final HttpClient client = HttpClient.newHttpClient();

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : open.reversed()) {
            try {
                c.close();
            } catch (Exception e) {
                // best effort
            }
        }
        client.close();
    }

    private PeerWriterFixture writer(String node, long wallMillis) throws Exception {
        PeerWriterFixture w = new PeerWriterFixture(node, tmp.resolve(node), 30, wallMillis).start();
        open.add(w);
        return w;
    }

    private PeerHttp http() {
        PeerHttp h = PeerWriterFixture.http();
        open.add(h);
        return h;
    }

    private PeerMembership member(PeerCatalog catalog, PeerHttp http, ReaderHeartbeat hb) {
        PeerMembership m = new PeerMembership(catalog, http, PeerWriterFixture.cfg(10, 30), () -> hb);
        open.add(m);
        return m;
    }

    private PeerMembership observer(PeerCatalog catalog, PeerHttp http) {
        PeerMembership m = PeerMembership.observer(catalog, http, PeerWriterFixture.cfg(10, 30));
        open.add(m);
        return m;
    }

    private static List<String> nodes(List<ReaderHeartbeat> live) {
        return live.stream().map(ReaderHeartbeat::node).toList();
    }

    private HttpResponse<String> call(String method, String url, String token, String body, String... headers)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).method(method,
                body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        if (token != null) b.header(InternalApi.TOKEN_HEADER, token);
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(50);
        }
    }

    // ── writer side ──────────────────────────────────────────────────────────

    @Test
    void writerJudgesLivenessByItsOwnClockAndKeepsUnknownFields() throws Exception {
        PeerWriterFixture w = writer("w", CLOCK_A);
        ObjectNode hb = MAPPER.valueToTree(heartbeat("r1", List.of("p1"), false, null, null, 0));
        hb.putArray("generationsHeld").add(7); // a later reader's field: kept and served back verbatim
        assertThat(call("POST", w.url() + "/replication/readers", TOKEN, hb.toString()).statusCode()).isEqualTo(204);

        w.advance(30); // the window counts whole seconds, as BucketClock.expired does: 30 s is still live
        HttpResponse<String> list = call("GET", w.url() + "/replication/readers", TOKEN, null);
        assertThat(list.statusCode()).isEqualTo(200);
        var n = MAPPER.readTree(list.body());
        assertThat(n.path("writer").asText()).isEqualTo("w");
        assertThat(n.path("now").asLong()).isEqualTo(CLOCK_A + 30_000);
        assertThat(n.path("ttlSeconds").asLong()).isEqualTo(30);
        assertThat(n.path("readers")).hasSize(1);
        assertThat(n.path("readers").get(0).path("ageMillis").asLong()).isEqualTo(30_000);
        assertThat(n.path("readers").get(0).path("heartbeat").path("generationsHeld").get(0).asInt()).isEqualTo(7);
        assertThat(n.path("readers").get(0).path("heartbeat").path("node").asText()).isEqualTo("r1");

        w.advance(1);
        assertThat(w.liveNodes()).isEmpty();

        // malformed heartbeats are refused without echoing them
        assertThat(call("POST", w.url() + "/replication/readers", TOKEN, "not json").statusCode()).isEqualTo(400);
        assertThat(call("POST", w.url() + "/replication/readers", TOKEN, "{\"node\":\"\"}").statusCode())
                .isEqualTo(400);
        assertThat(call("POST", w.url() + "/replication/readers", TOKEN, "[1]").statusCode()).isEqualTo(400);
        assertThat(call("POST", w.url() + "/replication/readers", TOKEN,
                "{\"node\":\"r\",\"pad\":\"" + "x".repeat(70_000) + "\"}").statusCode()).isEqualTo(413);
        assertThat(w.liveNodes()).isEmpty();

        // a withdrawal names its process
        assertThat(call("DELETE", w.url() + "/replication/readers/r1", TOKEN, null).statusCode()).isEqualTo(400);
        assertThat(call("DELETE", w.url() + "/replication/readers/r1?startedAt=x", TOKEN, null).statusCode())
                .isEqualTo(400);
        assertThat(w.readers.withdrawn()).isEmpty();
    }

    @Test
    void membershipRoutesRequireToken() throws Exception {
        PeerWriterFixture w = writer("w", CLOCK_A);
        String hb = MAPPER.writeValueAsString(heartbeat("r1", List.of("p1"), false, null, null, 0));
        String state = new String(StatusState.EMPTY.toJson());
        for (String token : new String[]{null, "", "wrong", TOKEN + "x"}) {
            List<HttpResponse<String>> answers = List.of(
                    call("POST", w.url() + "/replication/readers", token, hb),
                    call("GET", w.url() + "/replication/readers", token, null),
                    call("DELETE", w.url() + "/replication/readers/r1?startedAt=1", token, null),
                    call("GET", w.url() + "/replication/status-state", token, null),
                    call("PUT", w.url() + "/replication/status-state", token, state, "If-None-Match", "*"));
            for (HttpResponse<String> r : answers) {
                assertThat(r.statusCode()).as("%s %s", r.request().method(), r.uri()).isEqualTo(401);
                assertThat(r.body()).isEqualTo("{\"error\":\"unauthorized\"}");
            }
        }
        assertThat(w.liveNodes()).isEmpty();
        assertThat(w.statusState.get()).isEmpty();
        // a PUT needs a precondition
        assertThat(call("PUT", w.url() + "/replication/status-state", TOKEN, state).statusCode()).isEqualTo(428);
        assertThat(call("PUT", w.url() + "/replication/status-state", TOKEN, "nope", "If-None-Match", "*")
                .statusCode()).isEqualTo(400);
    }

    // ── merged view ──────────────────────────────────────────────────────────

    @Test
    void readerVisibleWhileOneWriterDown() throws Exception {
        PeerWriterFixture a = writer("a", CLOCK_A);
        PeerWriterFixture b = writer("b", CLOCK_B);
        PeerHttp http = http();
        PeerCatalog readerCatalog = PeerWriterFixture.catalog(http, a, b);
        PeerCatalog routerCatalog = PeerWriterFixture.catalog(http, a, b);
        PeerMembership r1 = member(readerCatalog, http, heartbeat("r1", List.of("p1"), false, null, null, 0));
        PeerMembership router = observer(routerCatalog, http);

        r1.tick();
        assertThat(a.liveNodes()).containsExactly("r1");
        assertThat(b.liveNodes()).containsExactly("r1");
        router.tick();
        assertThat(nodes(router.live())).containsExactly("r1");

        // writer a dies; neither catalog noticed yet: a's POST and GET fail, b's answers keep r1 live
        a.stop();
        b.advance(5);
        r1.tick();
        router.tick();
        assertThat(nodes(router.live())).containsExactly("r1");
        assertThat(router.liveWithAge().get(0).ageSeconds()).isZero(); // its newest heartbeat (b just got one)

        // once the catalogs dropped a, the reader beats to b alone and stays visible
        readerCatalog.refresh();
        routerCatalog.refresh();
        assertThat(readerCatalog.liveWriters()).extracting(PeerCatalog.WriterRef::node).containsExactly("b");
        b.advance(20);
        r1.tick();
        router.tick();
        assertThat(nodes(router.live())).containsExactly("r1");
        assertThat(b.readers.live().get(0).ageMillis()).isLessThan(1000);

        // a reader seen only by a writer that went down, and by no live one, is gone once b expires it
        b.advance(31);
        router.tick();
        assertThat(router.live()).isEmpty();
    }

    @Test
    void newestHeartbeatAcrossWritersWins() {
        PeerCatalog.WriterRef a = new PeerCatalog.WriterRef("a", URI.create("http://a:7070"));
        PeerCatalog.WriterRef b = new PeerCatalog.WriterRef("b", URI.create("http://b:7070"));
        ReaderHeartbeat old = heartbeat("r1", List.of("p1"), false, null, null, 0);
        ReaderHeartbeat fresh = heartbeat("r1", List.of("p1", "p2"), false, null, null, 3);
        long t = 1_000_000_000_000L;
        var merged = PeerMembership.merge(List.of(
                new PeerMembership.WriterReaders(a, CLOCK_A, t, 60_000, List.of(new PeerMembership.Seen(old, 9000)), null),
                new PeerMembership.WriterReaders(b, CLOCK_B, t, 60_000, List.of(new PeerMembership.Seen(fresh, 1000)), null)));
        assertThat(merged.get("r1").heartbeat()).isEqualTo(fresh);
        assertThat(merged.get("r1").receivedNanos()).isEqualTo(t - 1_000_000_000L);

        // a writer that restarted (registry younger than the window) may not have heard from r2 yet
        var prev = Map.of("r2", new PeerMembership.Merged(old, t - 5_000_000_000L));
        long ttl = Duration.ofSeconds(30).toNanos();
        var young = List.of(new PeerMembership.WriterReaders(a, CLOCK_A, t, 2_000, List.of(), null));
        assertThat(PeerMembership.carryOver(Map.of(), young, prev, t, ttl)).containsKey("r2");
        assertThat(PeerMembership.carryOver(Map.of(), young, prev, t + ttl, ttl)).isEmpty(); // aged out
        var settled = List.of(new PeerMembership.WriterReaders(a, CLOCK_A, t, 2_000, List.of(), null),
                new PeerMembership.WriterReaders(b, CLOCK_B, t, 31_000, List.of(), null));
        assertThat(PeerMembership.carryOver(Map.of(), settled, prev, t, ttl)).isEmpty(); // b's list is authoritative
        var withdrew = List.of(new PeerMembership.WriterReaders(a, CLOCK_A, t, 2_000, List.of(),
                Map.of("r2", old.startedAt())));
        assertThat(PeerMembership.carryOver(Map.of(), withdrew, prev, t, ttl)).isEmpty(); // it withdrew
    }

    // ── routing ──────────────────────────────────────────────────────────────

    @Test
    void routerUsesPeerMembership() throws Exception {
        PeerWriterFixture a = writer("a", CLOCK_A);
        PeerWriterFixture b = writer("b", CLOCK_B);
        Javalin fakeReader = Javalin.create();
        fakeReader.get("/health", ctx -> ctx.result("{\"status\":\"ready\"}"));
        fakeReader.get("/api/ping", ctx -> ctx.result("pong-r1"));
        fakeReader.start(0);
        open.add(fakeReader::stop);
        String readerUrl = "http://localhost:" + fakeReader.port();

        PeerHttp http = http();
        ReaderHeartbeat r1 = new ReaderHeartbeat("r1", readerUrl, List.of("p1"), false, Map.of(), 0, 0, 0L);
        ReaderHeartbeat r2 = new ReaderHeartbeat("r2", "http://localhost:1", List.of("p1", "p2"), true, Map.of(),
                0, 0, 0L); // draining: never routed
        member(PeerWriterFixture.catalog(http, a), http, r1).tick();
        member(PeerWriterFixture.catalog(http, b), http, r2).tick();

        PeerMembership observed = observer(PeerWriterFixture.catalog(http, a, b), http);
        observed.tick();
        assertThat(nodes(observed.live())).containsExactly("r1", "r2");

        // coordinators: ShardRouter over the peer membership
        ShardRouter shards = new ShardRouter(observed, () -> List.of(new ShardId("p1"), new ShardId("p2")),
                new ReaderHealth(30), id -> false, n -> 0);
        List<ShardRouter.Route> routes = shards.route(List.of());
        assertThat(routes).extracting(r -> r.shard().value()).containsExactly("p1", "p2");
        assertThat(routes.get(0).candidates()).extracting(ReaderHeartbeat::node).containsExactly("r1");
        assertThat(routes.get(1).candidates()).isEmpty();

        // pharos router: polls the members' /health and routes to the healthy one
        try (RouterCommand.Router router = new RouterCommand.Router(observed, Duration.ofHours(1),
                Duration.ofMillis(500), Duration.ofSeconds(10))) {
            router.start(0);
            router.pollNow();
            assertThat(router.healthy()).containsExactly("r1");
            HttpResponse<String> r = call("GET", "http://localhost:" + router.port() + "/api/ping", null, null);
            assertThat(r.statusCode()).isEqualTo(200);
            assertThat(r.body()).isEqualTo("pong-r1");
        }
    }

    // ── draining ─────────────────────────────────────────────────────────────

    @Test
    void drainingReaderLeavesPeerMembership() throws Exception {
        PeerWriterFixture w = writer("w", CLOCK_A);
        IndexConfig config = IndexConfig.defaults();
        Path h = tmp.resolve("reader-1");
        config.setIndexDir(h.resolve("indexes"));
        config.setSynonymsFile(h.resolve("synonyms.txt"));
        ClusterConfig c = new ClusterConfig();
        c.setRole(NodeRole.READER);
        c.setBackend(BackendKind.PEER_PULL);
        c.setNodeId("reader-1");
        c.setAdvertiseUrl("http://reader-1:7070");
        c.setToken(TOKEN);
        c.setCacheDir(h.resolve("cluster-cache"));
        c.setConnectTimeoutMillis(500);
        c.getPeers().setDiscovery("seeds:" + w.url());
        c.getPeers().setRefreshSeconds(1);
        c.getPeers().setLongPollSeconds(2);
        c.getReader().setMode(ClusterConfig.ReaderConfig.FULL);
        c.validate();
        config.setCluster(c);
        PeerBackend backend = new PeerBackend(config);
        ProjectRegistry registry = ClusterRuntime.registryFor(config);
        LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene);
        open.add(() -> {
            runtime.close();
            lucene.close();
        });

        runtime.beginServing(java.util.concurrent.CompletableFuture.completedFuture(null), true);
        await("the reader's heartbeat on the writer", () -> w.liveNodes().contains("reader-1"));
        await("the reader's own membership view", () -> runtime.liveReaders().stream()
                .anyMatch(l -> l.heartbeat().node().equals("reader-1")));
        assertThat(runtime.membership().live()).extracting(ReaderHeartbeat::node).containsExactly("reader-1");

        PeerHttp http = http();
        PeerMembership router = observer(PeerWriterFixture.catalog(http, w), http);
        ShardRouter shards = new ShardRouter(router, () -> List.of(new ShardId("p1")), new ReaderHealth(30),
                id -> false, n -> 0);

        runtime.drain(); // POSTs the draining heartbeat at once
        PeerReaderRegistry.Entry drained = w.readers.live().getFirst();
        assertThat(drained.heartbeat().draining()).isTrue();
        assertThat(drained.heartbeat().state()).isEqualTo(ReaderHeartbeat.DRAINING);
        router.tick();
        assertThat(router.live()).extracting(ReaderHeartbeat::draining).containsExactly(true);
        assertThat(shards.route(List.of("p1")).getFirst().candidates()).isEmpty();

        runtime.close(); // withdraws from every live writer
        assertThat(w.liveNodes()).isEmpty();
        router.tick();
        assertThat(router.live()).isEmpty();

        // a heartbeat of the closed process still in flight cannot bring it back ...
        HttpResponse<String> late = call("POST", w.url() + "/replication/readers", TOKEN, drained.raw().toString());
        assertThat(late.statusCode()).isEqualTo(409);
        assertThat(w.liveNodes()).isEmpty();
        // ... while a restarted process of the same node is accepted at once
        ObjectNode restarted = drained.raw().deepCopy();
        restarted.put("startedAt", drained.heartbeat().startedAt() + 1).put("draining", false).put("state", "ready");
        assertThat(call("POST", w.url() + "/replication/readers", TOKEN, restarted.toString()).statusCode())
                .isEqualTo(204);
        assertThat(w.liveNodes()).containsExactly("reader-1");
    }

    // ── status (P5-R1, P5-R2) ────────────────────────────────────────────────

    @Test
    void statusStateMovesToNextWriter() throws Exception {
        PeerWriterFixture a = writer("a", CLOCK_A);
        PeerWriterFixture b = writer("b", CLOCK_B);
        PeerHttp http = http();
        PeerCatalog catalog = PeerWriterFixture.catalog(http, b, a);
        StatusStateStore store = PeerStatusState.remote(catalog::liveWriters, http);

        // compare-and-set on the lowest-id live writer
        StatusStateStore.Versioned v0 = store.read();
        assertThat(v0.state()).isEqualTo(StatusState.EMPTY);
        assertThat(v0.etag()).isNull();
        StatusState s1 = new StatusState(StatusState.VERSION, 100, null, null, new StatusState.Summary(1, 2, 3));
        assertThat(store.write(s1, null)).isTrue();
        assertThat(a.statusState.get()).isPresent();
        assertThat(b.statusState.get()).isEmpty();
        assertThat(store.write(s1, null)).isFalse(); // someone created it first
        StatusStateStore.Versioned v1 = store.read();
        assertThat(v1.state().asOf()).isEqualTo(100);
        StatusState s2 = new StatusState(StatusState.VERSION, 200, null, null, new StatusState.Summary(1, 2, 4));
        assertThat(store.write(s2, "\"bogus-1\"")).isFalse();
        assertThat(store.write(s2, v1.etag())).isTrue();
        assertThat(store.write(s2, v1.etag())).isFalse(); // a racing node that read v1 too loses
        assertThat(store.read().state().asOf()).isEqualTo(200);

        // a dies: the state starts over on b; a's versions mean nothing there
        a.stop();
        catalog.refresh();
        StatusStateStore.Versioned onB = store.read();
        assertThat(onB.state()).isEqualTo(StatusState.EMPTY);
        assertThat(onB.etag()).isNull();
        assertThat(store.write(s2, v1.etag())).isFalse();
        assertThat(store.write(s1, null)).isTrue();
        assertThat(b.statusState.get()).isPresent();

        // a answers again (a partition healed): the lowest id holds the state again, as a had it
        a.start();
        catalog.refresh();
        assertThat(store.read().etag()).isEqualTo(a.statusState.get().map(PeerStatusState.Held::etag).orElseThrow());
    }

    @Test
    void peerStatusIsDatedByTheHolderAndRecordsStateThere() throws Exception {
        PeerWriterFixture a = writer("a", CLOCK_A);
        PeerWriterFixture b = writer("b", CLOCK_B);
        PeerHttp http = http();
        PeerCatalog catalog = PeerWriterFixture.catalog(http, a, b);
        member(PeerWriterFixture.catalog(http, a, b), http,
                heartbeat("r1", List.of("p1"), false, ReaderHeartbeat.WARMING, null, 0)).tick();
        PeerReplicaSource replicas = new PeerReplicaSource(catalog, http, Duration.ofSeconds(2));
        open.add(replicas::close);
        PeerStatusSource source = new PeerStatusSource(catalog, replicas, http, Duration.ofSeconds(2),
                Duration.ofSeconds(30), System::nanoTime, java.time.Clock.systemUTC());
        StatusService status = new StatusService(source, PeerStatusState.remote(catalog::liveWriters, http),
                StatusSettings.of(new ClusterConfig().getStatus()), null, Duration.ofHours(1), Duration.ZERO,
                System::nanoTime);
        open.add(status);

        ClusterStatus s = status.refresh();
        assertThat(s.mode()).isEqualTo("cluster");
        assertThat(Instant.parse(s.asOf())).isBetween(Instant.ofEpochMilli(CLOCK_A), Instant.ofEpochMilli(CLOCK_A + 5000));
        assertThat(s.layout().readers()).extracting(ClusterStatus.Reader::node).containsExactly("r1");
        assertThat(s.layout().writers()).extracting(ClusterStatus.Writer::node).containsExactly("a", "b");
        assertThat(s.layout().writers()).allSatisfy(w -> assertThat(w.leases()).isEmpty());
        assertThat(s.health()).extracting(ClusterStatus.Health::kind).contains("reader-warming");
        StatusState recorded = StatusState.read(a.statusState.get().orElseThrow().json());
        assertThat(recorded.layout()).isEqualTo(new StatusState.Summary(0, 2, 1));
        assertThat(b.statusState.get()).isEmpty();

        a.stop();
        catalog.refresh();
        b.advance(1);
        ClusterStatus after = status.refresh();
        assertThat(Instant.parse(after.asOf())).isBetween(Instant.ofEpochMilli(CLOCK_B), Instant.ofEpochMilli(CLOCK_B + 6000));
        assertThat(after.layout().readers()).extracting(ClusterStatus.Reader::node).containsExactly("r1");
        assertThat(StatusState.read(b.statusState.get().orElseThrow().json()).layout())
                .isEqualTo(new StatusState.Summary(0, 1, 1));
    }

    @Test
    void peerWriterStatusComesFromThePeers() throws Exception {
        int port;
        try (java.net.ServerSocket sock = new java.net.ServerSocket(0)) {
            port = sock.getLocalPort();
        }
        IndexConfig config = IndexConfig.defaults();
        Path h = tmp.resolve("writer-1");
        config.setIndexDir(h.resolve("indexes"));
        config.setSynonymsFile(h.resolve("synonyms.txt"));
        ClusterConfig c = new ClusterConfig();
        c.setRole(NodeRole.WRITER);
        c.setBackend(BackendKind.PEER_PULL);
        c.setNodeId("writer-1");
        c.setToken(TOKEN);
        c.setAdvertiseUrl("http://localhost:" + port); // no discovery: it lists itself
        c.getPeers().setStoreDir(h.resolve("peer-store"));
        c.validate();
        config.setCluster(c);
        PeerBackend backend = new PeerBackend(config);
        ProjectRegistry registry = ProjectRegistry.at(h.resolve("registry.json"));
        LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene);
        open.add(() -> {
            runtime.close();
            lucene.close();
        });
        Javalin app = Javalin.create();
        runtime.replicationEndpoint().orElseThrow().mount(app);
        app.start(port);
        open.add(app::stop);

        ClusterStatus s = runtime.statusService().refresh();
        assertThat(s.mode()).isEqualTo("cluster");
        assertThat(s.layout().writers()).extracting(ClusterStatus.Writer::node).containsExactly("writer-1");
        // the state it holds as the only (lowest-id) writer
        assertThat(runtime.replicationEndpoint().orElseThrow().statusState().get()).isPresent();
    }
}
