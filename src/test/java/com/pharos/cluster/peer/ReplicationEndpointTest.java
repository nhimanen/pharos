package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.BackendKind;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.internal.InternalApi;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import com.pharos.web.WebServer;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplicationEndpointTest {

    private static final String PROJECT = "sample-app";
    private static final String TOKEN = "peer-secret";
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path tmp;
    private Path source;
    private IndexConfig config;
    private PeerBackend backend;
    private ProjectIndexManager manager;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final List<Javalin> apps = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        source = tmp.resolve("src-copy");
        Path from = Path.of("src/test/resources/test-projects/sample-app");
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path dest = source.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(dest);
                else Files.copy(p, dest);
            }
        }
        config = writerConfig(2, 0);
    }

    @AfterEach
    void tearDown() {
        apps.forEach(Javalin::stop);
        if (backend != null) backend.close();
    }

    /** A peer-mode writer; {@code retentionMinutes} 0 lets GC prune at once. */
    private IndexConfig writerConfig(int retentionGenerations, int retentionMinutes) {
        IndexConfig c = IndexConfig.defaults();
        c.setIndexDir(tmp.resolve("indexes"));
        c.setSynonymsFile(tmp.resolve("synonyms.txt"));
        ClusterConfig cc = new ClusterConfig();
        cc.setRole(NodeRole.WRITER);
        cc.setBackend(BackendKind.PEER_PULL);
        cc.setNodeId("writer-a");
        cc.setToken(TOKEN);
        cc.setRetentionGenerations(retentionGenerations);
        cc.setRetentionMinutes(retentionMinutes);
        cc.getPeers().setStoreDir(tmp.resolve("peer-store"));
        cc.validate();
        c.setCluster(cc);
        return c;
    }

    private void open(IndexConfig c) {
        backend = new PeerBackend(c);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        LuceneIndexer lucene = new LuceneIndexer(c);
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        manager = new ProjectIndexManager(c, lucene, registry, List.of(EmbeddingProvider.create(c)), noopModules,
                List.of(new JavaCodeParser()), backend.publisher());
    }

    private void indexFull() throws IOException {
        manager.index(source, PROJECT, false, false, false, false, ProgressListener.SILENT);
    }

    /** Changes one source file and re-indexes incrementally: a new generation. */
    private void changeAndReindex(int n) throws IOException {
        Path file;
        try (Stream<Path> s = Files.walk(source)) {
            file = s.filter(p -> p.toString().endsWith("Calculator.java")).findFirst().orElseThrow();
        }
        Files.writeString(file, Files.readString(file).replace("class Calculator {",
                "class Calculator {\n    public int times" + n + "(int x) { return " + n + " * x; }\n"));
        Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() + 5000L * n));
        manager.index(source, PROJECT, true, false, false, false, ProgressListener.SILENT);
    }

    private Javalin serve(ReplicationEndpoint endpoint) {
        Javalin app = Javalin.create();
        endpoint.mount(app);
        app.start(0);
        apps.add(app);
        return app;
    }

    private ReplicationEndpoint endpoint;

    private Javalin serve() {
        endpoint = backend.endpoint();
        return serve(endpoint);
    }

    private static String base(Javalin app) {
        return "http://localhost:" + app.port() + ReplicationEndpoint.PREFIX;
    }

    private HttpRequest.Builder get(String url, String token) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).GET();
        if (token != null) b.header(InternalApi.TOKEN_HEADER, token);
        return b;
    }

    private HttpResponse<byte[]> send(HttpRequest.Builder b) throws Exception {
        return http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<byte[]> get(Javalin app, String path) throws Exception {
        return send(get(base(app) + path, TOKEN));
    }

    private static JsonNode json(HttpResponse<byte[]> r) throws IOException {
        return JSON.readTree(r.body());
    }

    private static String fileUrl(String key) {
        StringBuilder sb = new StringBuilder();
        for (String seg : key.split("/")) {
            if (!sb.isEmpty()) sb.append('/');
            sb.append(URLEncoder.encode(seg, StandardCharsets.UTF_8));
        }
        return "/" + PROJECT + "/files/" + sb;
    }

    private JsonNode manifest(Javalin app, long gen) throws Exception {
        HttpResponse<byte[]> r = get(app, "/" + PROJECT + "/manifests/gen-" + gen + ".json");
        assertThat(r.statusCode()).isEqualTo(200);
        return json(r);
    }

    /** The largest Lucene file of a manifest. */
    private static JsonNode largestLucene(JsonNode manifest) {
        JsonNode best = null;
        for (JsonNode f : manifest.get("files")) {
            if (!ManifestFile.LUCENE.equals(f.get("role").asText())) continue;
            if (best == null || f.get("length").asLong() > best.get("length").asLong()) best = f;
        }
        return best;
    }

    // ── tests ────────────────────────────────────────────────────────────────

    @Test
    void latestLongPollsUntilPublish() throws Exception {
        open(config);
        indexFull();
        Javalin app = serve();

        HttpResponse<byte[]> now = get(app, "/" + PROJECT + "/latest");
        assertThat(now.statusCode()).isEqualTo(200);
        assertThat(json(now).get("generation").asLong()).isEqualTo(1);
        assertThat(json(now).get("manifest").asText()).isEqualTo("gen-1.json");

        CompletableFuture<HttpResponse<byte[]>> waiting = http.sendAsync(
                get(base(app) + "/" + PROJECT + "/latest?wait=30&after=1", TOKEN).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        Thread.sleep(500);
        assertThat(waiting).as("no generation newer than 1 yet: the request waits").isNotDone();
        assertThat(backend.peerStore().signal().waiting()).isEqualTo(1);

        long t0 = System.nanoTime();
        changeAndReindex(1); // publishes generation 2 and signals
        HttpResponse<byte[]> woken = waiting.get(10, TimeUnit.SECONDS);
        assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - t0)).isLessThan(20);
        assertThat(woken.statusCode()).isEqualTo(200);
        assertThat(json(woken).get("generation").asLong()).isEqualTo(2);
        assertThat(json(woken).get("manifest").asText()).isEqualTo("gen-2.json");
        assertThat(backend.peerStore().signal().waiting()).isZero();
        assertThat(endpoint.longPolls().inFlight()).isZero();

        // A generation newer than `after` already exists: answered at once.
        HttpResponse<byte[]> immediate = get(app, "/" + PROJECT + "/latest?wait=30&after=1");
        assertThat(json(immediate).get("generation").asLong()).isEqualTo(2);

        // Nothing new within the wait: answered with the current generation when it times out.
        long t1 = System.nanoTime();
        HttpResponse<byte[]> timedOut = get(app, "/" + PROJECT + "/latest?wait=1&after=2");
        long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
        assertThat(waitedMs).isBetween(900L, 10_000L);
        assertThat(timedOut.statusCode()).isEqualTo(200);
        assertThat(json(timedOut).get("generation").asLong()).isEqualTo(2);

        assertThat(get(app, "/unknown-shard/latest").statusCode()).isEqualTo(404);
        assertThat(get(app, "/" + PROJECT + "/latest?wait=x").statusCode()).isEqualTo(400);
        assertThat(get(app, "/..%2Fetc/latest").statusCode()).isIn(400, 404);
    }

    /**
     * Every publish to a peer-mode writer comes from another process ({@code pharos index}, {@code pharos
     * publish}): the web daemon's long-polls must learn of it from the store, not wait out their timeout.
     */
    @Test
    void publishByAnotherProcessWakesTheLongPoll() throws Exception {
        open(config);
        indexFull();
        PeerBackend web = new PeerBackend(config); // the `pharos web` process: its own GenerationSignal
        try {
            ReplicationEndpoint served = web.endpoint();
            Javalin app = serve(served);
            CompletableFuture<HttpResponse<byte[]>> waiting = http.sendAsync(
                    get(base(app) + "/" + PROJECT + "/latest?wait=30&after=1", TOKEN).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (web.peerStore().signal().waiting() == 0 && System.nanoTime() < deadline) Thread.sleep(20);
            assertThat(web.peerStore().signal().waiting()).isEqualTo(1);

            changeAndReindex(1); // published by the `pharos index` process: signals only its own backend
            long t0 = System.nanoTime();
            HttpResponse<byte[]> woken = waiting.get(5, TimeUnit.SECONDS);
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0))
                    .isLessThanOrEqualTo(ReplicationEndpoint.WATCH_MILLIS * 4);
            assertThat(json(woken).get("generation").asLong()).isEqualTo(2);
            assertThat(web.peerStore().signal().waiting()).isZero();
            served.close();
            assertThat(served.stats()).containsEntry("longPolls", 0);
        } finally {
            web.close();
        }
    }

    @Test
    void longPollsAboveTheBoundGet503() throws Exception {
        open(config);
        indexFull();
        ReplicationEndpoint endpoint = new ReplicationEndpoint(backend.peerStore(), "writer-a",
                InternalApi.tokenMatcher(TOKEN), 8, 1);
        Javalin app = serve(endpoint);
        CompletableFuture<HttpResponse<byte[]>> first = http.sendAsync(
                get(base(app) + "/" + PROJECT + "/latest?wait=30&after=1", TOKEN).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (endpoint.longPolls().inFlight() == 0 && System.nanoTime() < deadline) Thread.sleep(20);
        assertThat(endpoint.longPolls().inFlight()).isEqualTo(1);

        HttpResponse<byte[]> second = get(app, "/" + PROJECT + "/latest?wait=30&after=1");
        assertThat(second.statusCode()).isEqualTo(503);
        assertThat(second.headers().firstValue("Retry-After")).hasValue("2");

        endpoint.close(); // wakes the waiting one with the current generation
        assertThat(first.get(10, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
    }

    @Test
    void rangeServesExactBytes() throws Exception {
        open(config);
        indexFull();
        Javalin app = serve();
        JsonNode f = largestLucene(manifest(app, 1));
        String key = f.get("key").asText();
        assertThat(key).startsWith("shards/" + PROJECT + "/lucene/").contains("@");
        byte[] local = Files.readAllBytes(tmp.resolve("indexes").resolve(PROJECT).resolve("lucene")
                .resolve(f.get("name").asText()));
        int size = local.length;
        assertThat(size).isEqualTo(f.get("length").asInt()).isGreaterThan(120);

        HttpResponse<byte[]> mid = send(get(base(app) + fileUrl(key), TOKEN).header("Range", "bytes=10-99"));
        assertThat(mid.statusCode()).isEqualTo(206);
        assertThat(mid.headers().firstValue("Content-Range")).hasValue("bytes 10-99/" + size);
        assertThat(mid.headers().firstValue("Content-Length")).hasValue("90");
        assertThat(mid.body()).isEqualTo(Arrays.copyOfRange(local, 10, 100));

        HttpResponse<byte[]> tail = send(get(base(app) + fileUrl(key), TOKEN).header("Range", "bytes=" + (size - 16) + "-"));
        assertThat(tail.statusCode()).isEqualTo(206);
        assertThat(tail.body()).isEqualTo(Arrays.copyOfRange(local, size - 16, size));

        HttpResponse<byte[]> suffix = send(get(base(app) + fileUrl(key), TOKEN).header("Range", "bytes=-7"));
        assertThat(suffix.statusCode()).isEqualTo(206);
        assertThat(suffix.headers().firstValue("Content-Range")).hasValue("bytes " + (size - 7) + "-" + (size - 1) + "/" + size);
        assertThat(suffix.body()).isEqualTo(Arrays.copyOfRange(local, size - 7, size));

        HttpResponse<byte[]> clamped = send(get(base(app) + fileUrl(key), TOKEN).header("Range", "bytes=5-" + (size + 1000)));
        assertThat(clamped.statusCode()).isEqualTo(206);
        assertThat(clamped.body()).isEqualTo(Arrays.copyOfRange(local, 5, size));

        HttpResponse<byte[]> whole = get(app, fileUrl(key));
        assertThat(whole.statusCode()).isEqualTo(200);
        assertThat(whole.headers().firstValue("Accept-Ranges")).hasValue("bytes");
        assertThat(whole.body()).isEqualTo(local);

        for (String bad : List.of("bytes=" + size + "-", "bytes=9-3", "bytes=0-1,4-5", "items=0-1", "bytes=-0")) {
            HttpResponse<byte[]> r = send(get(base(app) + fileUrl(key), TOKEN).header("Range", bad));
            assertThat(r.statusCode()).as(bad).isEqualTo(416);
            assertThat(r.headers().firstValue("Content-Range")).as(bad).hasValue("bytes */" + size);
        }

        // Keys outside the shard's data directories are refused, whatever the store holds.
        assertThat(get(app, fileUrl("shards/" + PROJECT + "/LATEST")).statusCode()).isEqualTo(400);
        assertThat(get(app, fileUrl("shards/" + PROJECT + "/manifests/gen-1.json")).statusCode()).isEqualTo(400);
        assertThat(get(app, "/" + PROJECT + "/files/shards/" + PROJECT + "/lucene/..%2F..%2F..%2Fcatalog%2Fshards.json")
                .statusCode()).isEqualTo(400);
        assertThat(get(app, "/" + PROJECT + "/files/shards/other/lucene/x").statusCode()).isEqualTo(400);
    }

    @Test
    void prunedFileIs410WithCurrentGeneration() throws Exception {
        open(config); // retentionGenerations 2, retentionMinutes 0
        indexFull();
        Javalin app = serve();
        JsonNode gen1 = manifest(app, 1);
        changeAndReindex(1);
        changeAndReindex(2); // generation 3: GC keeps 3 and 2 only
        assertThat(json(get(app, "/" + PROJECT + "/latest")).get("generation").asLong()).isEqualTo(3);

        // A file only generation 1 referenced (its segments_N commit point) is gone from the writer.
        java.util.Set<String> kept = new java.util.HashSet<>();
        for (long g : new long[]{2, 3}) {
            for (JsonNode f : manifest(app, g).get("files")) kept.add(f.get("key").asText());
        }
        String pruned = null;
        for (JsonNode f : gen1.get("files")) {
            if (!kept.contains(f.get("key").asText())) pruned = f.get("key").asText();
        }
        assertThat(pruned).as("generation 1 has a file no later generation references").isNotNull();

        HttpResponse<byte[]> gone = send(get(base(app) + fileUrl(pruned), TOKEN).header("Range", "bytes=0-9"));
        assertThat(gone.statusCode()).isEqualTo(410);
        assertThat(json(gone).get("generation").asLong()).isEqualTo(3);
        assertThat(json(gone).get("manifest").asText()).isEqualTo("gen-3.json");
        assertThat(json(gone).get("error").asText()).contains("pruned");

        HttpResponse<byte[]> manifestGone = get(app, "/" + PROJECT + "/manifests/gen-1.json");
        assertThat(manifestGone.statusCode()).isEqualTo(410);
        assertThat(json(manifestGone).get("generation").asLong()).isEqualTo(3);

        // Files of retained generations are still served.
        JsonNode kept3 = largestLucene(manifest(app, 3));
        assertThat(get(app, fileUrl(kept3.get("key").asText())).statusCode()).isEqualTo(200);
    }

    @Test
    void transferCapReturns503RetryAfter() throws Exception {
        open(config);
        indexFull();
        ReplicationEndpoint endpoint = new ReplicationEndpoint(backend.peerStore(), "writer-a",
                InternalApi.tokenMatcher(TOKEN), 1, 8);
        Javalin app = serve(endpoint);
        String key = largestLucene(manifest(app, 1)).get("key").asText();

        // Sequential transfers each give their slot back.
        for (int i = 0; i < 3; i++) assertThat(get(app, fileUrl(key)).statusCode()).isEqualTo(200);
        assertThat(endpoint.transfers().inFlight()).isZero();
        long size = largestLucene(manifest(app, 1)).get("length").asLong();
        assertThat(endpoint.stats()).containsEntry("maxTransfers", 1).containsEntry("transfersInFlight", 0)
                .containsEntry("peakTransfersInFlight", 1).containsEntry("transfersServed", 3L)
                .containsEntry("transfersRefused", 0L).containsEntry("bytesServed", 3 * size);

        try (TransferLimiter.Permit busy = endpoint.transfers().tryAcquire().orElseThrow()) {
            HttpResponse<byte[]> r = send(get(base(app) + fileUrl(key), TOKEN).header("Range", "bytes=0-9"));
            assertThat(r.statusCode()).isEqualTo(503);
            assertThat(r.headers().firstValue("Retry-After")).hasValue(String.valueOf(ReplicationEndpoint.RETRY_AFTER_SECONDS));
            assertThat(json(r).get("error").asText()).contains("too many transfers");
            // Manifests and latest are not transfers: still answered.
            assertThat(get(app, "/" + PROJECT + "/manifests/gen-1.json").statusCode()).isEqualTo(200);
        }
        assertThat(send(get(base(app) + fileUrl(key), TOKEN).header("Range", "bytes=0-9")).statusCode()).isEqualTo(206);
        assertThat(endpoint.stats()).as("the 503 is counted; a range adds its own bytes only")
                .containsEntry("transfersRefused", 1L).containsEntry("transfersServed", 4L)
                .containsEntry("bytesServed", 3 * size + 10).containsEntry("peakTransfersInFlight", 1);
    }

    @Test
    void transferInFlightHoldsItsSlot() throws Exception {
        open(config);
        indexFull();
        // A big content-named file in the shard, so the first transfer blocks on the slow reader.
        byte[] big = new byte[48 << 20];
        new java.util.Random(7).nextBytes(big);
        String key = "shards/" + PROJECT + "/lucene/big.bin@0";
        backend.peerStore().store().put(key, big);
        ReplicationEndpoint endpoint = new ReplicationEndpoint(backend.peerStore(), "writer-a",
                InternalApi.tokenMatcher(TOKEN), 1, 8);
        Javalin app = serve(endpoint);

        HttpResponse<InputStream> slow = http.send(get(base(app) + fileUrl(key), TOKEN).build(),
                HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream in = slow.body()) {
            assertThat(slow.statusCode()).isEqualTo(200);
            assertThat(in.read()).isEqualTo(big[0] & 0xff);
            HttpResponse<byte[]> second = get(app, fileUrl(key));
            assertThat(second.statusCode()).isEqualTo(503);
            assertThat(second.headers().firstValue("Retry-After")).isPresent();
            byte[] rest = in.readAllBytes();
            assertThat(rest.length).isEqualTo(big.length - 1);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (endpoint.transfers().inFlight() > 0 && System.nanoTime() < deadline) Thread.sleep(20);
        assertThat(send(get(base(app) + fileUrl(key), TOKEN).header("Range", "bytes=0-0")).statusCode()).isEqualTo(206);
    }

    @Test
    void tokenRequired() throws Exception {
        open(config);
        Files.writeString(config.getSynonymsFile(), "hand, written\n");
        indexFull();
        Javalin app = serve();
        String key = largestLucene(manifest(app, 1)).get("key").asText();
        List<String> paths = List.of("/catalog", "/synonyms", "/" + PROJECT + "/latest",
                "/" + PROJECT + "/latest?wait=5&after=0", "/" + PROJECT + "/manifests/gen-1.json", fileUrl(key),
                "/no-such-shard/latest", "/anything/else");
        for (String p : paths) {
            for (String token : new String[]{null, "", "wrong", TOKEN + "x"}) {
                HttpResponse<byte[]> r = send(get(base(app) + p, token).header("Range", "bytes=0-9"));
                assertThat(r.statusCode()).as(p + " with token " + token).isEqualTo(401);
                assertThat(new String(r.body(), StandardCharsets.UTF_8)).as(p).isEqualTo("{\"error\":\"unauthorized\"}");
            }
        }
        for (String p : paths.subList(0, 6)) {
            int status = send(get(base(app) + p, TOKEN).header("Range", "bytes=0-9")).statusCode();
            assertThat(status).as(p).isIn(200, 206);
        }
        // No configured token: nothing matches, every route is closed.
        Javalin open = serve(new ReplicationEndpoint(backend.peerStore(), "writer-a", InternalApi.tokenMatcher(null), 8, 8));
        assertThat(send(get(base(open) + "/catalog", "")).statusCode()).isEqualTo(401);
        assertThat(send(get(base(open) + "/catalog", null)).statusCode()).isEqualTo(401);
    }

    @Test
    void catalogListsThePublishedShards() throws Exception {
        open(config);
        Javalin app = serve();
        JsonNode empty = json(get(app, "/catalog"));
        assertThat(empty.get("writer").asText()).isEqualTo("writer-a");
        assertThat(empty.get("shards")).isEmpty();
        indexFull();
        changeAndReindex(1);
        JsonNode cat = json(get(app, "/catalog"));
        assertThat(cat.get("shards")).hasSize(1);
        assertThat(cat.get("shards").get(0).get("shard").asText()).isEqualTo(PROJECT);
        assertThat(cat.get("shards").get(0).get("latest").asLong()).isEqualTo(2);
    }

    @Test
    void synonymsServeOwnSectionsWithSha256Etag() throws Exception {
        open(config);
        Javalin app = serve();
        assertThat(get(app, "/synonyms").statusCode()).isEqualTo(404);
        // The local file carries a hand rule and a section of a shard this writer does not own.
        Files.writeString(config.getSynonymsFile(), "hand, written\n\n"
                + "# ── Auto-mined from 'other-proj' (2 rules)\n"
                + "foo, bar # auto:other-proj:Foo\n");
        indexFull();
        HttpResponse<byte[]> r = get(app, "/synonyms");
        assertThat(r.statusCode()).isEqualTo(200);
        String body = new String(r.body(), StandardCharsets.UTF_8);
        assertThat(body).startsWith("hand, written\n").doesNotContain("other-proj");
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(r.body()));
        assertThat(r.headers().firstValue("ETag")).hasValue("\"" + sha + "\"");

        HttpResponse<byte[]> same = send(get(base(app) + "/synonyms", TOKEN).header("If-None-Match", "\"" + sha + "\""));
        assertThat(same.statusCode()).isEqualTo(304);
        assertThat(same.body()).isEmpty();
    }

    @Test
    void tlsWhenConfigured() throws Exception {
        open(config);
        indexFull();
        char[] password = "changeit-test".toCharArray();
        Path keystore = selfSignedKeystore(password);

        WebServer web = new WebServer(null, null, null, null, null, NodeRole.WRITER);
        web.setReplicationEndpoint(backend.endpoint());
        web.setTls(new WebServer.Tls(keystore, "PKCS12", password));
        Javalin app = web.start(0, tmp.resolve("home"));
        apps.add(app);
        int port = app.port();

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore)) {
            ks.load(in, password);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(null, tmf.getTrustManagers(), null);
        HttpClient https = HttpClient.newBuilder().sslContext(ssl).connectTimeout(Duration.ofSeconds(5)).build();

        String url = "https://localhost:" + port + ReplicationEndpoint.PREFIX + "/" + PROJECT + "/latest";
        HttpResponse<String> ok = https.send(HttpRequest.newBuilder(URI.create(url))
                .header(InternalApi.TOKEN_HEADER, TOKEN).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(ok.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(ok.body()).get("generation").asLong()).isEqualTo(1);

        HttpResponse<String> noToken = https.send(HttpRequest.newBuilder(URI.create(url)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(noToken.statusCode()).isEqualTo(401);

        // Plain HTTP to the TLS port never gets an answer, let alone bytes.
        HttpRequest plain = HttpRequest.newBuilder(URI.create("http://localhost:" + port + ReplicationEndpoint.PREFIX
                + "/" + PROJECT + "/latest")).header(InternalApi.TOKEN_HEADER, TOKEN).timeout(Duration.ofSeconds(10)).build();
        assertThatThrownBy(() -> http.send(plain, HttpResponse.BodyHandlers.ofString())).isInstanceOf(IOException.class);

        // A client that does not trust the certificate is refused at the handshake.
        assertThatThrownBy(() -> HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(url))
                .header(InternalApi.TOKEN_HEADER, TOKEN).build(), HttpResponse.BodyHandlers.ofString()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void tlsWithAMissingKeystoreFailsToStart() {
        WebServer web = new WebServer(null, null, null, null, null, NodeRole.WRITER);
        web.setTls(new WebServer.Tls(tmp.resolve("absent.p12"), "PKCS12", "x".toCharArray()));
        assertThatThrownBy(() -> apps.add(web.start(0, tmp.resolve("home"))))
                .hasStackTraceContaining("cluster.tls.keystore");
    }

    @Test
    void runtimeServesTheEndpointOnPeerWritersOnly() throws Exception {
        open(config);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        try (com.pharos.cluster.ClusterRuntime rt = new com.pharos.cluster.ClusterRuntime(config, backend, registry,
                new LuceneIndexer(config))) {
            assertThat(rt.isWorkQueueWriter()).as("forced off in peer mode").isFalse();
            ReplicationEndpoint e = rt.replicationEndpoint().orElseThrow();
            assertThat(rt.replicationEndpoint()).containsSame(e);
            assertThat(rt.serverTls()).isEmpty();
        }
        IndexConfig single = IndexConfig.defaults();
        single.setIndexDir(tmp.resolve("single"));
        try (com.pharos.cluster.ClusterRuntime rt = new com.pharos.cluster.ClusterRuntime(single,
                new com.pharos.cluster.LocalBackend(single), registry, new LuceneIndexer(single))) {
            assertThat(rt.replicationEndpoint()).isEmpty();
        }
    }

    @Test
    void catalogSkipsAShardWithAnUnreadableLatest() throws Exception {
        open(config);
        indexFull();
        PeerStore peer = backend.peerStore();
        peer.catalog().register(new com.pharos.cluster.ShardId("empty-latest"));
        peer.catalog().register(new com.pharos.cluster.ShardId("garbage-latest"));
        peer.store().put("shards/empty-latest/LATEST", new byte[0]);
        peer.store().put("shards/garbage-latest/LATEST", "{not json".getBytes(StandardCharsets.UTF_8));
        Javalin app = serve();
        HttpResponse<byte[]> r = get(app, "/catalog");
        assertThat(r.statusCode()).isEqualTo(200);
        JsonNode shards = json(r).get("shards");
        assertThat(shards).hasSize(1);
        assertThat(shards.get(0).get("shard").asText()).isEqualTo(PROJECT);
    }

    @Test
    void longPollRaisesTheIdleTimeoutOfItsOwnConnectionOnly() throws Exception {
        open(config);
        indexFull();
        WebServer web = new WebServer(null, null, null, null, null, NodeRole.WRITER);
        web.setReplicationEndpoint(backend.endpoint());
        web.setIdleTimeout(Duration.ofSeconds(1));
        Javalin app = web.start(0, tmp.resolve("home"));
        apps.add(app);

        // An ordinary connection that sends nothing is cut at the (shortened) default.
        try (java.net.Socket idle = new java.net.Socket("localhost", app.port())) {
            idle.setSoTimeout(15_000);
            long t0 = System.nanoTime();
            assertThat(idle.getInputStream().read()).isEqualTo(-1);
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)).isLessThan(8_000);
        }

        // A long-poll waits past it, then its kept-alive connection gets the default back.
        try (java.net.Socket s = new java.net.Socket("localhost", app.port())) {
            s.setSoTimeout(20_000);
            s.getOutputStream().write(("GET " + ReplicationEndpoint.PREFIX + "/" + PROJECT + "/latest?wait=3&after=1 HTTP/1.1\r\n"
                    + "Host: localhost\r\n" + InternalApi.TOKEN_HEADER + ": " + TOKEN + "\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            s.getOutputStream().flush();
            long t0 = System.nanoTime();
            String response = readResponse(s.getInputStream());
            long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            assertThat(response).startsWith("HTTP/1.1 200").contains("\"generation\":1");
            assertThat(waited).as("answered at the wait's end, not cut at the 1 s idle timeout").isGreaterThanOrEqualTo(2_800);
            long t1 = System.nanoTime();
            assertThat(s.getInputStream().read()).isEqualTo(-1);
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1))
                    .as("idle timeout restored after the long-poll").isLessThan(8_000);
        }
    }

    /** Reads one HTTP/1.1 response with a Content-Length body. */
    private static String readResponse(InputStream in) throws IOException {
        StringBuilder head = new StringBuilder();
        while (!head.toString().endsWith("\r\n\r\n")) {
            int c = in.read();
            if (c < 0) throw new java.io.EOFException("connection closed after: " + head);
            head.append((char) c);
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(?i)content-length: *(\\d+)").matcher(head);
        assertThat(m.find()).as(head.toString()).isTrue();
        byte[] body = in.readNBytes(Integer.parseInt(m.group(1)));
        return head + new String(body, StandardCharsets.UTF_8);
    }

    @Test
    void storeDirOnAnotherFileStoreIsWarned() throws Exception {
        Path store = tmp.resolve("store");
        Path index = tmp.resolve("indexes");
        assertThat(PeerBackend.storeMismatch(store, index, p -> "same")).isEmpty();
        assertThat(PeerBackend.storeMismatch(store, index, p -> p.equals(store) ? "fs-a" : "fs-b")).hasValueSatisfying(w ->
                assertThat(w).contains(store.toString(), "fs-a", index.toString(), "fs-b", "copied", "doubles"));
        assertThat(PeerBackend.storeMismatch(store, index, p -> { throw new IOException("no"); })).isEmpty();
        assertThat(PeerBackend.storeMismatch(store, index, PeerBackend.FILE_STORE)).as("one temp file system").isEmpty();
    }

    @Test
    void writerDiskGuardWatchesTheStoreDir() throws Exception {
        Path storeDir = config.getCluster().getPeers().getStoreDir();
        assertThat(com.pharos.cluster.ClusterRuntime.writerDiskDirs(config)).contains(storeDir);
        IndexConfig objectStore = IndexConfig.defaults();
        assertThat(com.pharos.cluster.ClusterRuntime.writerDiskDirs(objectStore)).doesNotContain(objectStore.getCluster()
                .getPeers().getStoreDir());

        // The store dir on its own (nearly full) file store makes the writer low.
        com.pharos.cluster.writer.DiskGuard.Probe probe = new com.pharos.cluster.writer.DiskGuard.Probe() {
            @Override public Object storeOf(Path p) { return p.startsWith(storeDir) ? "store-fs" : "index-fs"; }
            @Override public com.pharos.cluster.writer.DiskGuard.Space space(Path p) {
                return p.startsWith(storeDir) ? new com.pharos.cluster.writer.DiskGuard.Space(1L << 20, 100L << 30)
                        : new com.pharos.cluster.writer.DiskGuard.Space(90L << 30, 100L << 30);
            }
        };
        com.pharos.cluster.writer.DiskGuard guard = new com.pharos.cluster.writer.DiskGuard(config.getIndexDir(),
                com.pharos.cluster.ClusterRuntime.writerDiskDirs(config),
                com.pharos.cluster.writer.DiskGuard.Settings.of(config.getCluster().getWriter().getDisk()), probe);
        assertThat(guard.low()).isTrue();

        // A peer-mode writer runtime reports its disk (with the store dir) on /health.
        open(config);
        try (com.pharos.cluster.ClusterRuntime rt = new com.pharos.cluster.ClusterRuntime(config, backend,
                ProjectRegistry.at(tmp.resolve("registry.json")), new LuceneIndexer(config))) {
            assertThat(rt.peerDiskGuard()).isPresent();
            assertThat(rt.writerDisk()).containsKeys("freeBytes", "totalBytes", "low");
        }
    }

    @Test
    void peerBackendReaderNeedsDiscovery() {
        IndexConfig c = writerConfig(3, 60);
        c.getCluster().setRole(NodeRole.READER);
        c.getCluster().setAdvertiseUrl("http://reader-1:7171");
        c.getCluster().setCacheDir(tmp.resolve("reader-cache"));
        assertThatThrownBy(() -> new PeerBackend(c)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.peers.discovery");
        c.getCluster().getPeers().setDiscovery("seeds:http://localhost:1");
        PeerBackend reader = new PeerBackend(c);
        try {
            assertThat(reader.isReader()).isTrue();
            assertThat(reader.catalog()).isInstanceOf(PeerCatalog.class);
            assertThat(reader.source()).isInstanceOf(PeerReplicaSource.class);
            assertThat(reader.localCache()).isInstanceOf(com.pharos.cluster.ReaderLocalCache.class);
            assertThatThrownBy(reader::peerStore).hasMessageContaining("reader");
        } finally {
            reader.close();
        }
    }

    @Test
    void peersListsTheAdvertiseUrlAndSeeds() throws Exception {
        config.getCluster().setAdvertiseUrl("http://writer-a.example:7070/");
        config.getCluster().getPeers().setDiscovery("seeds:http://writer-b:7070,https://writer-c");
        open(config);
        Javalin app = serve();
        HttpResponse<byte[]> r = get(app, "/peers");
        assertThat(r.statusCode()).isEqualTo(200);
        assertThat(json(r).get("writer").asText()).isEqualTo("writer-a");
        assertThat(json(r).get("peers")).extracting(JsonNode::asText)
                .containsExactly("http://writer-a.example:7070", "http://writer-b:7070", "https://writer-c");
        assertThat(send(get(base(app) + "/peers", null)).statusCode()).isEqualTo(401);
    }

    /** Task 1 carry: the writer's shutdown step lets an in-flight range finish and refuses new ones. */
    @Test
    void drainFinishesInFlightTransfersAndRefusesNewOnes() throws Exception {
        open(config);
        indexFull();
        Javalin app = serve();
        JsonNode file = largestLucene(manifest(app, 1));
        TransferLimiter.Permit held = endpoint.transfers().tryAcquire().orElseThrow(); // an in-flight transfer
        CompletableFuture<Integer> drained = CompletableFuture.supplyAsync(() -> endpoint.drain(Duration.ofSeconds(20)));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!endpoint.draining() && System.nanoTime() < deadline) Thread.sleep(10);
        HttpResponse<byte[]> refused = get(app, fileUrl(file.get("key").asText()));
        assertThat(refused.statusCode()).isEqualTo(503);
        assertThat(refused.headers().firstValue("Retry-After")).contains("2");
        assertThat(get(app, "/catalog").statusCode()).isEqualTo(503);
        assertThat(send(get(base(app) + "/catalog", null)).statusCode()).as("the token still comes first").isEqualTo(401);
        Thread.sleep(300);
        assertThat(drained).as("waits for the in-flight transfer").isNotDone();
        held.close();
        assertThat(drained.get(10, TimeUnit.SECONDS)).isZero();
        assertThat(endpoint.drain(Duration.ZERO)).as("idempotent").isZero();
    }

    @Test
    void rangeHeaderParsing() {
        assertThat(ReplicationEndpoint.parseRange("bytes=0-0", 10)).containsExactly(0, 0);
        assertThat(ReplicationEndpoint.parseRange("bytes=3-", 10)).containsExactly(3, 9);
        assertThat(ReplicationEndpoint.parseRange("bytes=-3", 10)).containsExactly(7, 9);
        assertThat(ReplicationEndpoint.parseRange("bytes=-30", 10)).containsExactly(0, 9);
        assertThat(ReplicationEndpoint.parseRange("bytes=2-99999999999999999999", 10)).isNull();
        assertThat(ReplicationEndpoint.parseRange("bytes=0-0", 0)).isNull();
        assertThat(ReplicationEndpoint.parseRange("bytes=-", 10)).isNull();
        assertThat(ReplicationEndpoint.parseRange("bytes= 1-2", 10)).isNull();
    }

    /** A throw-away self-signed PKCS12 keystore for localhost, made with the JDK's keytool. */
    private Path selfSignedKeystore(char[] password) throws Exception {
        Path ks = tmp.resolve("test-keystore.p12");
        Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
        Process p = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "pharos-test",
                "-keyalg", "EC", "-groupname", "secp256r1", "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2",
                "-storetype", "PKCS12", "-keystore", ks.toString(),
                "-storepass", new String(password), "-keypass", new String(password))
                .redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(p.waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(p.exitValue()).as(out).isZero();
        return ks;
    }
}
