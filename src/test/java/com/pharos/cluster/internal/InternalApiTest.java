package com.pharos.cluster.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.routing.ShardRouter.Route;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.config.ClusterConfig;
import com.pharos.search.SearchRequest;
import com.pharos.web.WebServer;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InternalApiTest {

    private static final String TOKEN = "s3cret-token";
    private static final String[] PATHS = {"/internal/term-stats", "/internal/shard-search",
            "/internal/snippets", "/internal/callees"};

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private final List<Javalin> apps = new ArrayList<>();

    @TempDir Path tmp;

    @AfterEach
    void stop() {
        apps.forEach(Javalin::stop);
    }

    private Javalin serve(FakeShardSearchService svc) {
        Javalin app = Javalin.create();
        new InternalApi(TOKEN).service(svc).mount(app);
        app.start(0);
        apps.add(app);
        return app;
    }

    private HttpResponse<String> post(Javalin app, String path, String token, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + app.port() + path))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json");
        if (token != null) b.header(InternalApi.TOKEN_HEADER, token);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String bodyFor(String path, Long generation) {
        String g = generation == null ? "" : ",\"generation\":" + generation;
        return switch (path) {
            case "/internal/term-stats" -> "{\"shard\":\"java\",\"fields\":[\"body\"],\"terms\":[]}";
            case "/internal/shard-search" -> "{\"shard\":\"java\",\"retriever\":\"keyword\"" + g + "}";
            case "/internal/snippets" -> "{\"shard\":\"java\",\"ids\":[\"id1\"],\"query\":\"q\"" + g + "}";
            default -> "{\"shard\":\"java\",\"fqns\":[\"a.B#c()\"]" + g + "}";
        };
    }

    @Test
    void noTokenIs401AndHandlerNotInvoked() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        Javalin app = serve(svc);
        for (String p : PATHS) {
            HttpResponse<String> r = post(app, p, null, bodyFor(p, 5L));
            assertThat(r.statusCode()).as(p).isEqualTo(401);
            assertThat(r.body()).doesNotContain(TOKEN);
        }
        assertThat(svc.calls).hasValue(0);
    }

    @Test
    void wrongTokenIs401() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        Javalin app = serve(svc);
        for (String p : PATHS) {
            assertThat(post(app, p, "wrong", bodyFor(p, 5L)).statusCode()).as(p).isEqualTo(401);
            assertThat(post(app, p, TOKEN + "x", bodyFor(p, 5L)).statusCode()).as(p + " longer").isEqualTo(401);
            assertThat(post(app, p, "", bodyFor(p, 5L)).statusCode()).as(p + " empty").isEqualTo(401);
        }
        assertThat(svc.calls).hasValue(0);
    }

    @Test
    void validTokenReachesEveryHandler() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        Javalin app = serve(svc);
        for (String p : PATHS) {
            HttpResponse<String> r = post(app, p, TOKEN, bodyFor(p, 5L));
            assertThat(r.statusCode()).as(p + " " + r.body()).isEqualTo(200);
        }
        assertThat(svc.calls).hasValue(4);
    }

    @Test
    void generationMismatchIs409WithServedGeneration() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        Javalin app = serve(svc);
        for (String p : new String[]{"/internal/shard-search", "/internal/snippets", "/internal/callees"}) {
            HttpResponse<String> r = post(app, p, TOKEN, bodyFor(p, 4L));
            assertThat(r.statusCode()).as(p).isEqualTo(409);
            assertThat(mapper.readTree(r.body()).get("servedGeneration").asLong()).isEqualTo(5);
        }
        assertThat(svc.calls).as("no work on a mismatch").hasValue(0);
        // shard-search without a generation is answered with whatever is served
        HttpResponse<String> any = post(app, "/internal/shard-search", TOKEN, bodyFor("/internal/shard-search", null));
        assertThat(any.statusCode()).isEqualTo(200);
        assertThat(mapper.readTree(any.body()).get("generation").asLong()).isEqualTo(5);
    }

    @Test
    void unservedShardIs404AndBadBodyIs400() throws Exception {
        Javalin app = serve(new FakeShardSearchService().serving("other", 1));
        assertThat(post(app, "/internal/shard-search", TOKEN, bodyFor("/internal/shard-search", 1L)).statusCode())
                .isEqualTo(404);
        assertThat(post(app, "/internal/shard-search", TOKEN, "{not json").statusCode()).isEqualTo(400);
        assertThat(post(app, "/internal/shard-search", TOKEN, "{\"shard\":\"../x\",\"retriever\":\"keyword\"}")
                .statusCode()).isEqualTo(400);
    }

    @Test
    void searchRequestRoundTripsOverTheWire() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        Javalin app = serve(svc);
        ReaderHeartbeat reader = reader("n1", app.port());
        ShardClient client = new ShardClient(new ReaderHealth(30), TOKEN, Duration.ofSeconds(1), Duration.ofSeconds(5));
        SearchRequest req = new SearchRequest("q", SearchRequest.SearchType.KEYWORD, null, List.of("java"), 10,
                "json", null, null, 0);
        ShardSearchResponse resp = client.call(new Route(new ShardId("java"), List.of(reader)), ShardClient.SHARD_SEARCH,
                hb -> new ShardSearchRequest("java", 5L, "keyword", req, new float[]{0.5f, 1f}, null));
        assertThat(resp.generation()).isEqualTo(5);
        assertThat(resp.candidates()).hasSize(1);
        assertThat(resp.candidates().get(0).inDegree()).isEqualTo(4);
        assertThat(resp.maxInDegree()).isEqualTo(9);
    }

    private ReaderHeartbeat reader(String node, int port) {
        return new ReaderHeartbeat(node, "http://localhost:" + port, List.of("java"), false, null, 0, 0, 0);
    }

    private static int deadPort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private ShardClient client(ReaderHealth health) {
        return new ShardClient(health, TOKEN, Duration.ofMillis(500), Duration.ofSeconds(5));
    }

    private static final java.util.function.Function<ReaderHeartbeat, TermStatsRequest> STATS =
            hb -> new TermStatsRequest("java", List.of("body"), List.of());

    @Test
    void retriesOnceOnAnotherReplicaAfterConnectError() throws Exception {
        FakeShardSearchService good = new FakeShardSearchService().serving("java", 5);
        Javalin app = serve(good);
        ReaderHealth health = new ReaderHealth(30);
        ReaderHeartbeat dead = reader("dead", deadPort());
        ReaderHeartbeat live = reader("live", app.port());

        TermStatsResponse r = client(health).call(new Route(new ShardId("java"), List.of(dead, live)),
                ShardClient.TERM_STATS, STATS);

        assertThat(r.generation()).isEqualTo(5);
        assertThat(health.isSuspect("dead")).isTrue();
        assertThat(health.isSuspect("live")).isFalse();
    }

    @Test
    void retriesOnceOnAnotherReplicaAfter5xx() throws Exception {
        FakeShardSearchService broken = new FakeShardSearchService().serving("java", 5);
        broken.failure = new IllegalStateException("boom");
        FakeShardSearchService good = new FakeShardSearchService().serving("java", 5);
        Javalin a = serve(broken);
        Javalin b = serve(good);
        ReaderHealth health = new ReaderHealth(30);

        TermStatsResponse r = client(health).call(
                new Route(new ShardId("java"), List.of(reader("bad", a.port()), reader("good", b.port()))),
                ShardClient.TERM_STATS, STATS);

        assertThat(r.generation()).isEqualTo(5);
        assertThat(broken.calls).hasValue(1);
        assertThat(good.calls).hasValue(1);
        assertThat(health.isSuspect("bad")).isTrue();
    }

    @Test
    void unavailableWhenNoReplicaLeft() throws Exception {
        ReaderHealth health = new ReaderHealth(30);
        Route one = new Route(new ShardId("java"), List.of(reader("d1", deadPort())));
        assertThatThrownBy(() -> client(health).call(one, ShardClient.TERM_STATS, STATS))
                .isInstanceOf(ShardUnavailableException.class)
                .satisfies(e -> assertThat(((ShardUnavailableException) e).shard()).isEqualTo(new ShardId("java")));

        Route two = new Route(new ShardId("java"),
                List.of(reader("d1", deadPort()), reader("d2", deadPort())));
        assertThatThrownBy(() -> client(health).call(two, ShardClient.TERM_STATS, STATS))
                .isInstanceOf(ShardUnavailableException.class);

        Route none = new Route(new ShardId("java"), List.of());
        assertThatThrownBy(() -> client(health).call(none, ShardClient.TERM_STATS, STATS))
                .isInstanceOf(ShardUnavailableException.class);
    }

    @Test
    void generationMismatchSurfacesAndDoesNotMarkSuspect() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        Javalin app = serve(svc);
        ReaderHealth health = new ReaderHealth(30);
        assertThatThrownBy(() -> client(health).call(
                new Route(new ShardId("java"), List.of(reader("n1", app.port()))), ShardClient.CALLEES,
                hb -> new CalleesRequest("java", 4L, List.of("a.B#c()"))))
                .isInstanceOfSatisfying(GenerationMismatchException.class,
                        e -> assertThat(e.servedGeneration()).isEqualTo(5));
        assertThat(health.isSuspect("n1")).isFalse();
    }

    @Test
    void wrongClientTokenIsNotRetriedOrSuspect() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        Javalin app = serve(svc);
        ReaderHealth health = new ReaderHealth(30);
        ShardClient bad = new ShardClient(health, "nope", Duration.ofMillis(500), Duration.ofSeconds(5));
        assertThatThrownBy(() -> bad.call(new Route(new ShardId("java"), List.of(reader("n1", app.port()))),
                ShardClient.TERM_STATS, STATS)).isInstanceOf(ShardCallException.class)
                .hasMessageContaining("401");
        assertThat(health.isSuspect("n1")).isFalse();
    }

    @Test
    void readerWithoutTokenFailsStartup() {
        assertThatThrownBy(() -> new InternalApi((String) null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cluster.token");
        assertThatThrownBy(() -> new InternalApi(" "))
                .isInstanceOf(IllegalArgumentException.class);

        ClusterConfig c = new ClusterConfig();
        c.setRole(NodeRole.READER);
        c.setBackend(com.pharos.cluster.BackendKind.OBJECT_STORE);
        c.getObjectStore().setBucket("b");
        c.setAdvertiseUrl("http://r:7171");
        assertThatThrownBy(c::validate).hasMessageContaining("cluster.token");
    }

    @Test
    void internalRoutesAbsentOnRoleAll() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        WebServer server = new WebServer(null, null, null, null, null, NodeRole.ALL);
        server.setInternalApi(new InternalApi(TOKEN).service(svc));
        Javalin app = server.start(0, tmp);
        apps.add(app);
        for (String p : PATHS) {
            assertThat(post(app, p, TOKEN, bodyFor(p, 5L)).statusCode()).as(p).isEqualTo(404);
        }
        assertThat(svc.calls).hasValue(0);
    }

    @Test
    void internalRoutesPresentOnReader() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        WebServer server = new WebServer(null, null, null, null, null, NodeRole.READER);
        server.setInternalApi(new InternalApi(TOKEN).service(svc));
        Javalin app = server.start(0, tmp);
        apps.add(app);
        assertThat(post(app, "/internal/term-stats", null, bodyFor(PATHS[0], null)).statusCode()).isEqualTo(401);
        assertThat(post(app, "/internal/term-stats", TOKEN, bodyFor(PATHS[0], null)).statusCode()).isEqualTo(200);
    }

    // ---- fix round 1

    @Test
    void readerWithoutServiceAnswers503AfterAuthAndCoordinatorFailsOver() throws Exception {
        Javalin noSvc = Javalin.create();
        new InternalApi(TOKEN).mount(noSvc);
        noSvc.start(0);
        apps.add(noSvc);
        assertThat(post(noSvc, PATHS[0], null, bodyFor(PATHS[0], null)).statusCode()).as("auth first").isEqualTo(401);
        HttpResponse<String> r = post(noSvc, PATHS[0], TOKEN, bodyFor(PATHS[0], null));
        assertThat(r.statusCode()).isEqualTo(503);
        assertThat(mapper.readTree(r.body()).get("error").asText()).isEqualTo("shard search not available");

        Javalin good = serve(new FakeShardSearchService().serving("java", 5));
        ReaderHealth health = new ReaderHealth(30);
        TermStatsResponse resp = client(health).call(new Route(new ShardId("java"),
                        List.of(reader("blank", noSvc.port()), reader("good", good.port()))),
                ShardClient.TERM_STATS, STATS);
        assertThat(resp.generation()).isEqualTo(5);
        assertThat(health.isSuspect("blank")).isTrue();
    }

    @Test
    void generationSwappedBetweenCheckAndWorkIs409OnTheServer() throws Exception {
        FakeShardSearchService svc = new FakeShardSearchService().serving("java", 5);
        svc.generationDuringWork = 6L;
        Javalin app = serve(svc);
        for (String p : new String[]{"/internal/shard-search", "/internal/snippets", "/internal/callees"}) {
            HttpResponse<String> r = post(app, p, TOKEN, bodyFor(p, 5L));
            assertThat(r.statusCode()).as(p).isEqualTo(409);
            assertThat(mapper.readTree(r.body()).get("servedGeneration").asLong()).isEqualTo(6);
        }
    }

    @Test
    void clientRejectsAResponseOfAnotherGenerationThanRequested() throws Exception {
        Javalin stub = Javalin.create();
        stub.post("/internal/shard-search", ctx -> ctx.contentType("application/json")
                .result("{\"generation\":6,\"maxInDegree\":1,\"candidates\":[]}"));
        stub.start(0);
        apps.add(stub);
        ReaderHealth health = new ReaderHealth(30);
        SearchRequest req = new SearchRequest("q", SearchRequest.SearchType.KEYWORD, null, null, 10, "json", null, null, 0);
        assertThatThrownBy(() -> client(health).call(new Route(new ShardId("java"), List.of(reader("n1", stub.port()))),
                ShardClient.SHARD_SEARCH, hb -> new ShardSearchRequest("java", 5L, "keyword", req, null, null)))
                .isInstanceOfSatisfying(GenerationMismatchException.class,
                        e -> assertThat(e.servedGeneration()).isEqualTo(6));
        assertThat(health.isSuspect("n1")).isFalse();
    }

    @Test
    void notServedIsRetriedOnAnotherReplicaWithoutMarkingSuspect() throws Exception {
        Javalin other = serve(new FakeShardSearchService().serving("elsewhere", 1));
        Javalin good = serve(new FakeShardSearchService().serving("java", 5));
        ReaderHealth health = new ReaderHealth(30);
        TermStatsResponse r = client(health).call(new Route(new ShardId("java"),
                List.of(reader("a", other.port()), reader("b", good.port()))), ShardClient.TERM_STATS, STATS);
        assertThat(r.generation()).isEqualTo(5);
        assertThat(health.isSuspect("a")).isFalse();
    }

    @Test
    void oversizedCollectionsAre400AndOversizedBodyIs413() throws Exception {
        Javalin app = serve(new FakeShardSearchService().serving("java", 5));
        String ids = String.join(",", java.util.Collections.nCopies(10_001, "\"x\""));
        assertThat(post(app, "/internal/snippets", TOKEN,
                "{\"shard\":\"java\",\"generation\":5,\"ids\":[" + ids + "],\"query\":\"q\"}").statusCode()).isEqualTo(400);
        String terms = String.join(",", java.util.Collections.nCopies(10_001, "{\"field\":\"body\",\"text\":\"t\"}"));
        assertThat(post(app, "/internal/term-stats", TOKEN,
                "{\"shard\":\"java\",\"fields\":[\"body\"],\"terms\":[" + terms + "]}").statusCode()).isEqualTo(400);
        String vec = String.join(",", java.util.Collections.nCopies(4097, "0.5"));
        assertThat(post(app, "/internal/shard-search", TOKEN,
                "{\"shard\":\"java\",\"retriever\":\"keyword\",\"queryVector\":[" + vec + "]}").statusCode()).isEqualTo(400);
        String ok = String.join(",", java.util.Collections.nCopies(4096, "0.5"));
        assertThat(post(app, "/internal/shard-search", TOKEN,
                "{\"shard\":\"java\",\"retriever\":\"keyword\",\"queryVector\":[" + ok + "]}").statusCode()).isEqualTo(200);
        String huge = "{\"shard\":\"java\",\"retriever\":\"" + "x".repeat(InternalApi.MAX_BODY_BYTES + 1) + "\"}";
        assertThat(post(app, "/internal/shard-search", TOKEN, huge).statusCode()).isEqualTo(413);
    }

    @Test
    void requestSerialisationFailureIsALocalBugNotASuspectReader() throws Exception {
        Javalin app = serve(new FakeShardSearchService().serving("java", 5));
        ReaderHealth health = new ReaderHealth(30);
        List<String> bad = new java.util.AbstractList<>() {
            public String get(int i) { throw new IllegalStateException("local bug"); }
            public int size() { return 1; }
        };
        assertThatThrownBy(() -> client(health).call(new Route(new ShardId("java"), List.of(reader("n1", app.port()))),
                ShardClient.TERM_STATS, hb -> new TermStatsRequest("java", bad, List.of())))
                .isInstanceOf(ShardCallException.class).isNotInstanceOf(ShardUnavailableException.class);
        assertThat(health.isSuspect("n1")).isFalse();
    }

    @Test
    void oversizedResponseIsRefusedWithoutMarkingSuspect() throws Exception {
        Javalin app = serve(new FakeShardSearchService().serving("java", 5));
        ReaderHealth health = new ReaderHealth(30);
        ShardClient small = new ShardClient(health, TOKEN, Duration.ofMillis(500), Duration.ofSeconds(5), 20);
        assertThatThrownBy(() -> small.call(new Route(new ShardId("java"), List.of(reader("n1", app.port()))),
                ShardClient.TERM_STATS, STATS)).isInstanceOf(ShardCallException.class).hasMessageContaining("too large");
        assertThat(health.isSuspect("n1")).isFalse();
    }

    /** Carried from Task 3: the JDK's request timeout ends at the headers; a stalled body is bounded too. */
    @Test
    void stalledResponseBodyIsBoundedAndMarksSuspect() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            Thread t = new Thread(() -> {
                try (var socket = server.accept()) {
                    var in = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
                    String line;
                    int contentLength = 0;
                    while ((line = in.readLine()) != null && !line.isEmpty()) {
                        if (line.toLowerCase().startsWith("content-length:")) contentLength = Integer.parseInt(line.substring(15).trim());
                    }
                    for (int i = 0; i < contentLength; i++) in.read();
                    var out = socket.getOutputStream();
                    out.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"gen"
                            .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                    out.flush();
                    Thread.sleep(10_000); // headers sent, the body never completes
                } catch (Exception ignored) {
                }
            });
            t.setDaemon(true);
            t.start();
            ReaderHealth health = new ReaderHealth(30);
            ShardClient c = new ShardClient(health, TOKEN, Duration.ofMillis(500), Duration.ofMillis(300));
            long t0 = System.nanoTime();
            assertThatThrownBy(() -> c.call(new Route(new ShardId("java"), List.of(reader("stall", server.getLocalPort()))),
                    ShardClient.TERM_STATS, STATS)).isInstanceOf(ShardUnavailableException.class);
            assertThat((System.nanoTime() - t0) / 1_000_000).as("ms until given up").isLessThan(3_000);
            assertThat(health.isSuspect("stall")).isTrue();
        }
    }

    /** A reader analysing with other synonyms answers 409 "synonyms mismatch"; the client tries the next replica, marks nobody suspect. */
    @Test
    void synonymsMismatchIsRefusedAndRetriedElsewhereWithoutSuspect() throws Exception {
        FakeShardSearchService other = new FakeShardSearchService() {
            @Override public TermStatsResponse termStats(TermStatsRequest r) {
                throw new SynonymsMismatchException("hash differs");
            }
        }.serving("java", 5);
        Javalin mismatched = serve(other);
        HttpResponse<String> raw = post(mismatched, "/internal/term-stats", TOKEN, bodyFor("/internal/term-stats", null));
        assertThat(raw.statusCode()).isEqualTo(409);
        assertThat(mapper.readTree(raw.body()).get("error").asText()).isEqualTo(InternalApi.SYNONYMS_MISMATCH);

        Javalin good = serve(new FakeShardSearchService().serving("java", 7));
        ReaderHealth health = new ReaderHealth(30);
        ShardClient.Answer<TermStatsResponse> a = client(health).callPreferring(new Route(new ShardId("java"),
                List.of(reader("syn", mismatched.port()), reader("ok", good.port()))), null, ShardClient.TERM_STATS, STATS);
        assertThat(a.response().generation()).isEqualTo(7);
        assertThat(a.reader().node()).isEqualTo("ok");
        assertThat(health.isSuspect("syn")).isFalse();
        assertThatThrownBy(() -> client(health).call(new Route(new ShardId("java"), List.of(reader("syn", mismatched.port()))),
                ShardClient.TERM_STATS, STATS)).isInstanceOf(ShardUnavailableException.class).hasMessageContaining("synonyms");
    }

    /** I3: a shard without vectors of the coordinator's model answers 409 "model mismatch"; the client tries the next replica. */
    @Test
    void modelMismatchIsRefusedAndRetriedElsewhereWithoutSuspect() throws Exception {
        FakeShardSearchService noVectors = new FakeShardSearchService() {
            @Override public ShardSearchResponse searchRaw(ShardSearchRequest r) {
                throw new ModelMismatchException("no vectors for model m");
            }
        }.serving("java", 5);
        Javalin refusing = serve(noVectors);
        Javalin good = serve(new FakeShardSearchService().serving("java", 5));
        ReaderHealth health = new ReaderHealth(30);
        SearchRequest req = new SearchRequest("q", SearchRequest.SearchType.VECTOR, null, null, 10, "json", null, null, 0);
        var request = (java.util.function.Function<ReaderHeartbeat, ShardSearchRequest>)
                hb -> new ShardSearchRequest("java", 5L, "vector-knn", req, new float[]{1f}, null, null, "m", 1, null);
        assertThatThrownBy(() -> client(health).call(new Route(new ShardId("java"), List.of(reader("nv", refusing.port()))),
                ShardClient.SHARD_SEARCH, request)).isInstanceOf(ShardUnavailableException.class)
                .hasCauseInstanceOf(ModelMismatchException.class).hasMessageContaining("no vectors for model m");
        ShardClient.Answer<ShardSearchResponse> a = client(health).callPreferring(new Route(new ShardId("java"),
                List.of(reader("nv", refusing.port()), reader("ok", good.port()))), null, ShardClient.SHARD_SEARCH, request);
        assertThat(a.reader().node()).isEqualTo("ok");
        assertThat(health.isSuspect("nv")).isFalse();
    }

    // ---------------------------------------------------------------- final review I4: suspect only on the reader's own timeout

    /** A server that reads the request, then sends headers (or not) and stalls; closed with the socket. */
    private static ServerSocket stalling(boolean sendHeaders) throws Exception {
        ServerSocket server = new ServerSocket(0);
        Thread t = new Thread(() -> {
            while (!server.isClosed()) {
                try (var socket = server.accept()) {
                    var in = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
                    String line;
                    int contentLength = 0;
                    while ((line = in.readLine()) != null && !line.isEmpty()) {
                        if (line.toLowerCase().startsWith("content-length:")) contentLength = Integer.parseInt(line.substring(15).trim());
                    }
                    for (int i = 0; i < contentLength; i++) in.read();
                    if (sendHeaders) {
                        socket.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                                + "Content-Length: 1000\r\n\r\n{\"gen").getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                        socket.getOutputStream().flush();
                    }
                    Thread.sleep(10_000);
                } catch (Exception ignored) {
                }
            }
        });
        t.setDaemon(true);
        t.start();
        return server;
    }

    /**
     * I4: a timeout whose bound was the query's leftover budget (shorter than the per-call timeout)
     * says nothing about the reader: it is not marked suspect. Headers or body, either way.
     */
    @Test
    void aTimeoutFromTheShortenedQueryBudgetDoesNotMarkSuspect() throws Exception {
        for (boolean headers : new boolean[] {false, true}) {
            try (ServerSocket server = stalling(headers)) {
                ReaderHealth health = new ReaderHealth(30);
                ShardClient c = new ShardClient(health, TOKEN, Duration.ofMillis(500), Duration.ofSeconds(3));
                Route route = new Route(new ShardId("java"), List.of(reader("slow", server.getLocalPort())));
                long t0 = System.nanoTime();
                assertThatThrownBy(() -> c.callPreferring(route, null, ShardClient.TERM_STATS, STATS,
                        System.nanoTime() + 300_000_000L)).isInstanceOf(ShardUnavailableException.class);
                assertThat((System.nanoTime() - t0) / 1_000_000).as("bounded by the budget").isLessThan(2_000);
                assertThat(health.isSuspect("slow")).as("headers sent: " + headers).isFalse();
            }
        }
    }

    /** I4 control: the same stall with the full per-call timeout marks the reader suspect. */
    @Test
    void aTimeoutWithTheFullPerCallTimeoutMarksSuspect() throws Exception {
        for (boolean headers : new boolean[] {false, true}) {
            try (ServerSocket server = stalling(headers)) {
                ReaderHealth health = new ReaderHealth(30);
                ShardClient c = new ShardClient(health, TOKEN, Duration.ofMillis(500), Duration.ofMillis(300));
                Route route = new Route(new ShardId("java"), List.of(reader("slow", server.getLocalPort())));
                assertThatThrownBy(() -> c.callPreferring(route, null, ShardClient.TERM_STATS, STATS,
                        System.nanoTime() + 10_000_000_000L)).isInstanceOf(ShardUnavailableException.class);
                assertThat(health.isSuspect("slow")).as("headers sent: " + headers).isTrue();
            }
        }
    }
}
