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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /** P5-R12: the token alone does not let a client advertise any URL; the address must match. */
    @Test
    void writerAdmitsAHeartbeatOnlyFromItsAdvertisedHostOrAnAllowedHost() throws Exception {
        PeerWriterFixture w = new PeerWriterFixture("w", tmp.resolve("w"), 30, CLOCK_A,
                new ReaderAddressCheck(List.of("Proxied.Example"), host -> switch (host) {
                    case "localhost" -> List.of(java.net.InetAddress.getLoopbackAddress());
                    case "elsewhere.example" -> List.of(java.net.InetAddress.ofLiteral("192.0.2.7"));
                    default -> throw new java.io.IOException("cannot resolve " + host);
                })).start();
        open.add(w);
        java.util.function.Function<String, String> hb = url -> {
            try {
                return MAPPER.writeValueAsString(new ReaderHeartbeat("r-" + Math.abs(url.hashCode()), url,
                        List.of("p1"), false, Map.of(), 0, 0, 1L));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
        // another host's address, an unresolvable host: 403, never registered
        for (String url : List.of("http://elsewhere.example:7070", "http://192.0.2.9:7070", "http://nowhere.invalid:7070")) {
            HttpResponse<String> r = call("POST", w.url() + "/replication/readers", TOKEN, hb.apply(url));
            assertThat(r.statusCode()).as(url).isEqualTo(403);
            assertThat(r.body()).contains("reader heartbeat refused");
        }
        assertThat(w.liveNodes()).isEmpty();
        // the request's own address (by name or literal), or a host in allowHosts (NAT, proxy): admitted
        for (String url : List.of("http://localhost:7070", "http://127.0.0.1:7070", "http://proxied.example:7070")) {
            assertThat(call("POST", w.url() + "/replication/readers", TOKEN, hb.apply(url)).statusCode())
                    .as(url).isEqualTo(204);
        }
        assertThat(w.liveNodes()).hasSize(3);
    }

    @Test
    void readerAddressCheckComparesResolvedAddresses() throws Exception {
        PeerDiscovery.Resolver dns = host -> switch (host) {
            case "r1.example" -> List.of(java.net.InetAddress.ofLiteral("10.0.0.5"), java.net.InetAddress.ofLiteral("fd00::5"));
            default -> throw new java.io.IOException("NXDOMAIN");
        };
        ReaderAddressCheck c = new ReaderAddressCheck(List.of("[FD00::9]"), dns);
        assertThat(c.refusal("http://r1.example:7070", "10.0.0.5")).isNull();
        assertThat(c.refusal("http://r1.example:7070", "[fd00:0:0:0:0:0:0:5]")).isNull();
        assertThat(c.refusal("http://r1.example:7070", "10.0.0.6")).isNotNull();
        assertThat(c.refusal("http://[fd00::9]:7070", "10.0.0.6")).isNull(); // allowHosts
        assertThat(c.refusal("http://r2.example:7070", "10.0.0.5")).isNotNull(); // does not resolve
        assertThat(c.refusal("http://r1.example:7070", "not-an-ip")).isNotNull();
        assertThat(c.refusal(null, "10.0.0.6")).isNull(); // names no URL
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

    /** Refreshes {@code catalog} until it dropped every writer that stopped answering. */
    private static void dropMissing(PeerCatalog catalog) {
        for (int i = 0; i < PeerCatalog.DROP_AFTER; i++) catalog.refresh();
    }

    @Test
    void statusStateMovesToNextWriter() throws Exception {
        PeerWriterFixture a = writer("a", CLOCK_A);
        PeerWriterFixture b = writer("b", CLOCK_B);
        PeerHttp http = http();
        PeerCatalog catalog = PeerWriterFixture.catalog(http, b, a);
        StatusStateStore store = PeerStatusState.remote(() -> PeerStatusState.tracked(catalog), http);

        // compare-and-set on the lowest-id writer
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

        // a misses a refresh: it still holds the state; nobody falls over to b
        a.stop();
        catalog.refresh();
        assertThat(catalog.liveWriters()).extracting(PeerCatalog.WriterRef::node).containsExactly("b");
        assertThatThrownBy(store::read).isInstanceOf(java.io.IOException.class);
        assertThatThrownBy(() -> store.write(s1, null)).isInstanceOf(java.io.IOException.class);
        assertThat(b.statusState.get()).isEmpty();

        // a is dropped: the state starts over on b; a's versions mean nothing there
        catalog.refresh();
        catalog.refresh();
        StatusStateStore.Versioned onB = store.read();
        assertThat(onB.state()).isEqualTo(StatusState.EMPTY);
        assertThat(onB.etag()).isNull();
        assertThat(store.write(s2, v1.etag())).isFalse();
        assertThat(store.write(s1, null)).isTrue();
        assertThat(b.statusState.get()).isPresent();
    }

    /** A status service over the peers with an SSE client recording the event types it gets. */
    private record Watched(StatusService service, List<String> events) {}

    private Watched watchedStatus(PeerCatalog catalog, PeerHttp http) {
        PeerReplicaSource replicas = new PeerReplicaSource(catalog, http, Duration.ofSeconds(2));
        open.add(replicas::close);
        PeerStatusSource source = new PeerStatusSource(catalog, replicas, http, Duration.ofSeconds(2),
                Duration.ofSeconds(30), System::nanoTime, java.time.Clock.systemUTC());
        com.pharos.cluster.status.StatusEvents events = new com.pharos.cluster.status.StatusEvents(
                Duration.ofMinutes(5), null);
        List<String> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        assertThat(events.tryReserve()).isTrue();
        events.add(new com.pharos.cluster.status.StatusEvents.Sink() {
            @Override public void event(String type, String data) { seen.add(type); }
            @Override public void comment(String text) { }
            @Override public void onClose(Runnable r) { }
            @Override public void close() { }
        });
        StatusService status = new StatusService(source,
                PeerStatusState.remote(() -> PeerStatusState.tracked(catalog), http),
                StatusSettings.of(new ClusterConfig().getStatus()), events, Duration.ofHours(1), Duration.ZERO,
                System::nanoTime);
        open.add(status);
        return new Watched(status, seen);
    }

    /** A state with one proposal in force since long before {@code asOf}. */
    private static byte[] activeProposal(long asOfSeconds, long sinceSeconds) throws Exception {
        String id = com.pharos.cluster.status.Proposals.id("add-readers", "readers");
        var rule = new StatusState.Rule("add-readers", "readers", sinceSeconds, null, true, "load", "Start 1 reader", 1);
        return new StatusState(StatusState.VERSION, asOfSeconds, new java.util.TreeMap<>(Map.of(id, rule)), null,
                new StatusState.Summary(0, 2, 1)).toJson();
    }

    @Test
    void holderMissingOneRefreshKeepsTheStatusAndItsEvents() throws Exception {
        PeerWriterFixture a = writer("a", CLOCK_A);
        PeerWriterFixture b = writer("b", CLOCK_B);
        PeerHttp http = http();
        PeerCatalog catalog = PeerWriterFixture.catalog(http, a, b);
        member(PeerWriterFixture.catalog(http, a, b), http, heartbeat("r1", List.of("p1"), false, null, null, 0)).tick();
        long asOf = CLOCK_A / 1000;
        assertThat(a.statusState.put(activeProposal(asOf - 10, asOf - 1000), null)).isPresent();
        Watched w = watchedStatus(catalog, http);

        ClusterStatus before = w.service().refresh();
        assertThat(before.proposals()).extracting(ClusterStatus.Proposal::kind).containsExactly("add-readers");

        // a is slow for one refresh: the refresh fails, the status stays, nothing moves to b
        a.stop();
        catalog.refresh();
        assertThatThrownBy(() -> w.service().refresh()).isInstanceOf(java.io.IOException.class);
        assertThat(w.service().current().proposals()).extracting(ClusterStatus.Proposal::kind)
                .containsExactly("add-readers");
        assertThat(b.statusState.get()).isEmpty();

        // a is back: same holder, same state, no event
        a.start();
        catalog.refresh();
        ClusterStatus after = w.service().refresh();
        assertThat(after.proposals()).extracting(ClusterStatus.Proposal::kind).containsExactly("add-readers");
        assertThat(b.statusState.get()).isEmpty();
        Thread.sleep(300); // the event sender is asynchronous
        assertThat(w.events()).doesNotContain(com.pharos.cluster.status.StatusEvent.CLEARED,
                com.pharos.cluster.status.StatusEvent.APPEARED);
    }

    @Test
    void holderReturningAfterTheWindowStartsOver() throws Exception {
        PeerWriterFixture a = writer("a", CLOCK_A);
        PeerWriterFixture b = writer("b", CLOCK_B);
        PeerHttp http = http();
        PeerCatalog catalog = PeerWriterFixture.catalog(http, a, b);
        member(PeerWriterFixture.catalog(http, a, b), http, heartbeat("r1", List.of("p1"), false, null, null, 0)).tick();
        long asOf = CLOCK_A / 1000;
        String id = com.pharos.cluster.status.Proposals.id("add-readers", "readers");
        assertThat(a.statusState.put(activeProposal(asOf - 10, asOf - 1000), null)).isPresent();

        // a is cut off for longer than the window: the cluster moves to b, and a's state expires
        a.stop();
        dropMissing(catalog);
        assertThat(PeerStatusState.tracked(catalog)).extracting(PeerCatalog.WriterRef::node).containsExactly("b");
        a.advance(PeerWriterFixture.STATE_IDLE.toSeconds() + 1);
        a.start();
        HttpResponse<String> gone = call("GET", a.url() + "/replication/status-state", TOKEN, null);
        assertThat(gone.statusCode()).isEqualTo(404);

        // a is the holder again (lowest id), but with its pre-partition proposal forgotten
        catalog.refresh();
        ClusterStatus s = watchedStatus(catalog, http).service().refresh();
        assertThat(s.proposals()).isEmpty();
        StatusState recorded = StatusState.read(a.statusState.get().orElseThrow().json());
        assertThat(recorded.rules()).doesNotContainKey(id);

        // within the window a held state survives (reads keep it, writes are not needed)
        a.advance(PeerWriterFixture.STATE_IDLE.toSeconds() - 1);
        assertThat(a.statusState.get()).isPresent();
        a.advance(PeerWriterFixture.STATE_IDLE.toSeconds() - 1);
        assertThat(a.statusState.get()).isPresent();
    }

    @Test
    void peerStatusIsDatedByTheHolderAndRecordsStateThere() throws Exception {
        PeerWriterFixture a = writer("a", CLOCK_A);
        PeerWriterFixture b = writer("b", CLOCK_B);
        PeerHttp http = http();
        PeerCatalog catalog = PeerWriterFixture.catalog(http, a, b);
        member(PeerWriterFixture.catalog(http, a, b), http,
                heartbeat("r1", List.of("p1"), false, ReaderHeartbeat.WARMING, null, 0)).tick();
        StatusService status = watchedStatus(catalog, http).service();

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
        dropMissing(catalog);
        b.advance(1);
        ClusterStatus after = status.refresh();
        assertThat(Instant.parse(after.asOf())).isBetween(Instant.ofEpochMilli(CLOCK_B), Instant.ofEpochMilli(CLOCK_B + 6000));
        assertThat(after.layout().readers()).extracting(ClusterStatus.Reader::node).containsExactly("r1");
        assertThat(StatusState.read(b.statusState.get().orElseThrow().json()).layout())
                .isEqualTo(new StatusState.Summary(0, 1, 1));
    }

    @Test
    void peerWriterWithoutDiscoveryNeverWritesTheState() throws Exception {
        int port;
        try (java.net.ServerSocket sock = new java.net.ServerSocket(0)) {
            port = sock.getLocalPort();
        }
        java.util.concurrent.atomic.AtomicInteger hooks = new java.util.concurrent.atomic.AtomicInteger();
        Javalin hook = Javalin.create();
        hook.post("/hook", ctx -> hooks.incrementAndGet());
        hook.start(0);
        open.add(hook::stop);

        IndexConfig config = IndexConfig.defaults();
        Path h = tmp.resolve("a-writer");
        config.setIndexDir(h.resolve("indexes"));
        config.setSynonymsFile(h.resolve("synonyms.txt"));
        ClusterConfig c = new ClusterConfig();
        c.setRole(NodeRole.WRITER);
        c.setBackend(BackendKind.PEER_PULL);
        c.setNodeId("a-writer"); // the lowest id: the holder of the shared state
        c.setToken(TOKEN);
        c.setAdvertiseUrl("http://localhost:" + port); // no discovery: it sees itself only
        c.getPeers().setStoreDir(h.resolve("peer-store"));
        c.getStatus().setWebhookUrl("http://localhost:" + hook.port() + "/hook");
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
        ReplicationEndpoint endpoint = runtime.replicationEndpoint().orElseThrow();
        endpoint.mount(app);
        app.start(port);
        open.add(app::stop);
        PeerWriterFixture b = writer("b-writer", CLOCK_B);

        // its own refreshes: a status of what it sees, never written into the shared state
        for (int i = 0; i < 3; i++) {
            ClusterStatus s = runtime.statusService().refresh();
            assertThat(s.mode()).isEqualTo("cluster");
            assertThat(s.layout().writers()).extracting(ClusterStatus.Writer::node).containsExactly("a-writer");
        }
        assertThat(endpoint.statusState().get()).isEmpty();

        // a node seeing both writers records the state there; the partial writer only reads it
        PeerHttp http = http();
        PeerCatalog full = new PeerCatalog(new PeerDiscovery(PeerDiscovery.parse("seeds:http://localhost:" + port
                + "," + b.url()), "http", PeerDiscovery.SYSTEM_RESOLVER, http), http, Duration.ofSeconds(1),
                System::nanoTime);
        full.refresh();
        watchedStatus(full, http).service().refresh();
        String etag = endpoint.statusState().get().orElseThrow().etag();
        for (int i = 0; i < 3; i++) runtime.statusService().refresh();
        assertThat(endpoint.statusState().get().orElseThrow().etag()).isEqualTo(etag);
        Thread.sleep(500); // webhook delivery is asynchronous
        assertThat(hooks.get()).isZero();
    }

    @Test
    void routerFollowsGossipWithinAllowHosts() throws Exception {
        PeerWriterFixture seed = writer("seed", CLOCK_A);
        PeerWriterFixture gossiped = writer("gossiped", CLOCK_B);
        String viaIp = gossiped.url().replace("localhost", "127.0.0.1");
        seed.peers.add(viaIp); // only the seed knows it, under another host name
        PeerHttp http = http();
        member(new PeerCatalog(new PeerDiscovery(PeerDiscovery.parse("seeds:" + viaIp), "http",
                PeerDiscovery.SYSTEM_RESOLVER, http), http, Duration.ofSeconds(1), System::nanoTime), http,
                heartbeat("r1", List.of("p1"), false, null, null, 0)).tick();
        assertThat(gossiped.liveNodes()).containsExactly("r1");
        assertThat(seed.liveNodes()).isEmpty();

        ClusterConfig c = new ClusterConfig();
        c.setToken(TOKEN);
        c.setBackend(BackendKind.PEER_PULL);
        c.setHeartbeatSeconds(1);
        c.setReaderTtlSeconds(30);
        c.getPeers().setDiscovery("seeds:" + seed.url());
        try (var router = RouterCommand.observe(c)) { // allowHosts unset: only the seed's host, localhost
            router.start();
            Thread.sleep(1500);
            assertThat(router.live()).isEmpty();
        }
        c.getPeers().setAllowHosts(List.of("localhost", "127.0.0.1"));
        try (var router = RouterCommand.observe(c)) {
            router.start();
            await("the gossiped writer's reader", () -> !router.live().isEmpty());
            assertThat(nodes(router.live())).containsExactly("r1");
        }
    }

    /**
     * A frozen writer (it accepts connections and never answers) must not hold a reader's shutdown:
     * the drain announce gives up at its timeout although a tick holds the membership lock in a POST
     * to that writer, and close's withdraw gives up within its default bound (1.9.0's bounded
     * shutdown, for the bucket's {@code ReaderRegistry}, applied to peer membership).
     */
    @Test
    void drainAndCloseGiveUpOnAFrozenWriter() throws Exception {
        PeerWriterFixture w = writer("w", CLOCK_A);
        PeerHttp http = http();
        // refreshed once while the writer answers, then never again during the test
        PeerCatalog catalog = new PeerCatalog(new PeerDiscovery(PeerDiscovery.parse("seeds:" + w.url()), "http",
                PeerDiscovery.SYSTEM_RESOLVER, http), http, Duration.ofHours(1), System::nanoTime);
        catalog.refresh();
        assertThat(catalog.liveWriters()).hasSize(1);
        int port = URI.create(w.url()).getPort();
        w.stop();
        java.net.ServerSocket hole = new java.net.ServerSocket();
        hole.setReuseAddress(true);
        hole.bind(new java.net.InetSocketAddress(port));
        List<java.net.Socket> held = new java.util.concurrent.CopyOnWriteArrayList<>();
        Thread.ofVirtual().start(() -> {
            try {
                while (true) held.add(hole.accept());
            } catch (java.io.IOException closed) {
                // the test is over
            }
        });
        open.add(() -> {
            hole.close();
            for (java.net.Socket sock : held) sock.close();
        });

        PeerMembership m = member(catalog, http, heartbeat("r1", List.of("p1"), false, null, null, 0));
        m.start(); // the first tick POSTs to the frozen writer, holding the lock for up to ~2 x 11 s
        await("the heartbeat reaching the frozen writer", () -> !held.isEmpty());

        long t0 = System.nanoTime();
        m.drain(Duration.ofSeconds(1));
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(Duration.ofSeconds(3));

        long t1 = System.nanoTime();
        m.close();
        assertThat(Duration.ofNanos(System.nanoTime() - t1)).isLessThan(Duration.ofSeconds(8));
    }
}
