package com.pharos.cluster.peer;

import com.pharos.cluster.BackendKind;
import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.Generation;
import com.pharos.cluster.LongPollingSource;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.ReaderLocalCache;
import com.pharos.cluster.ReplicaSyncer;
import com.pharos.cluster.ReplicationBackend;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ClusterTestAccess;
import com.pharos.cluster.internal.InternalApi;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ObjectStoreBackend;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.PythonCodeParser;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SourceReader;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static com.pharos.cluster.ClusterFixture.PY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Phase 5 Task 2: readers pulling from writers over {@code /replication/*}, with no bucket. */
class PeerPullTest {

    private static final String TOKEN = "peer-secret";
    private static final ShardId JAVA_SHARD = new ShardId(JAVA);

    @TempDir Path tmp;
    private final List<AutoCloseable> open = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        SourceReader.setEnabled(true);
        for (AutoCloseable c : open.reversed()) {
            try {
                c.close();
            } catch (Exception e) {
                // best effort
            }
        }
    }

    // ── fixtures ─────────────────────────────────────────────────────────────

    /** A peer-mode writer wired like {@code pharos web} on role writer, serving /replication/*. */
    private final class Writer implements AutoCloseable {
        final String node;
        final Path home;
        final Path source;
        final IndexConfig config;
        final PeerBackend backend;
        final ProjectRegistry registry;
        final LuceneIndexer lucene;
        final ProjectIndexManager manager;
        final SearchEngine engine;
        Javalin app;
        int port;

        Writer(String node) throws IOException {
            this.node = node;
            home = tmp.resolve(node);
            source = home.resolve("src");
            copy(Path.of("src/test/resources/test-projects").resolve(JAVA), source.resolve(JAVA));
            copy(Path.of("src/test/resources/test-projects").resolve(PY), source.resolve(PY));
            config = IndexConfig.defaults();
            config.setIndexDir(home.resolve("indexes"));
            config.setSynonymsFile(home.resolve("synonyms.txt"));
            ClusterConfig c = new ClusterConfig();
            c.setRole(NodeRole.WRITER);
            c.setBackend(BackendKind.PEER_PULL);
            c.setNodeId(node);
            c.setToken(TOKEN);
            c.getPeers().setStoreDir(home.resolve("peer-store"));
            c.validate();
            config.setCluster(c);
            backend = new PeerBackend(config);
            registry = ProjectRegistry.at(home.resolve("registry.json"));
            lucene = new LuceneIndexer(config, backend.localCache());
            EmbeddingProvider embedder = EmbeddingProvider.create(config);
            manager = new ProjectIndexManager(config, lucene, registry, List.of(embedder),
                    ClusterFixture.noopModules(registry), List.of(new JavaCodeParser(), new PythonCodeParser()),
                    backend.publisher());
            engine = new SearchEngine(lucene, embedder, registry);
            open.add(this);
        }

        Writer start() {
            app = Javalin.create();
            backend.endpoint().mount(app);
            app.start(port); // 0 the first time; the same port again after a stop
            port = app.port();
            return this;
        }

        /** Reserves a port without listening (a writer that is down). */
        Writer reservePort() throws IOException {
            try (ServerSocket s = new ServerSocket(0)) {
                port = s.getLocalPort();
            }
            return this;
        }

        void stop() {
            if (app != null) app.stop();
            app = null;
        }

        String url() { return "http://localhost:" + port; }

        long index(String project, boolean incremental) throws IOException {
            manager.index(source.resolve(project), project, incremental, false, false, false, ProgressListener.SILENT);
            return backend.peerStore().source().latest(new ShardId(project)).orElseThrow().value();
        }

        /** Adds a method to Calculator and re-indexes incrementally: a new generation. */
        long addMethod(String method) throws IOException {
            Path calc = source.resolve(JAVA).resolve("src/main/java/com/example/Calculator.java");
            Files.writeString(calc, Files.readString(calc).replace("class Calculator {",
                    "class Calculator {\n    public int " + method + "(int x) { return 3 * x; }\n"));
            Files.setLastModifiedTime(calc, FileTime.fromMillis(System.currentTimeMillis() + 5000));
            return index(JAVA, true);
        }

        /**
         * Indexes sample-app and re-publishes it as one segment without compound files, so a lazy
         * reader leaves its larger files remote (a compound file under one block is eager).
         */
        long publishWithoutCompoundFiles() throws IOException {
            index(JAVA, false);
            Path luceneDir = lucene.getProjectIndexDir(JAVA).resolve("lucene");
            Path fresh = luceneDir.resolveSibling("lucene.rewrite");
            org.apache.lucene.index.IndexWriterConfig iwc = new org.apache.lucene.index.IndexWriterConfig(
                    LuceneIndexer.buildAnalyzer()).setUseCompoundFile(false)
                    .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
            try (org.apache.lucene.store.FSDirectory src = org.apache.lucene.store.FSDirectory.open(luceneDir);
                 org.apache.lucene.index.DirectoryReader r = org.apache.lucene.index.DirectoryReader.open(src);
                 org.apache.lucene.store.FSDirectory dst = org.apache.lucene.store.FSDirectory.open(fresh);
                 org.apache.lucene.index.IndexWriter w = new org.apache.lucene.index.IndexWriter(dst, iwc)) {
                w.addIndexes(r.leaves().stream().map(c -> (org.apache.lucene.index.CodecReader) c.reader())
                        .toArray(org.apache.lucene.index.CodecReader[]::new));
                w.commit();
            }
            try (Stream<Path> st = Files.list(luceneDir)) {
                for (Path p : st.toList()) Files.delete(p);
            }
            try (Stream<Path> st = Files.list(fresh)) {
                for (Path p : st.toList()) Files.move(p, luceneDir.resolve(p.getFileName()));
            }
            Files.delete(fresh);
            com.pharos.config.ProjectMeta meta = registry.find(JAVA).orElseThrow();
            return backend.publisher().publish(new com.pharos.cluster.LocalSnapshot(JAVA,
                    lucene.getProjectIndexDir(JAVA), meta, "none")).orElseThrow().value();
        }

        @Override
        public void close() {
            stop();
            lucene.close();
            backend.close();
        }
    }

    /** A peer-pull reader wired like {@code Main}; not started. */
    private record Reader(IndexConfig config, PeerBackend backend, ProjectRegistry registry, LuceneIndexer lucene,
                          ClusterRuntime runtime, SearchEngine engine) {
        PeerCatalog catalog() { return backend.peerCatalog().orElseThrow(); }

        PeerReplicaSource source() { return backend.peerSource().orElseThrow(); }

        Path genDir(long gen) { return config.getCluster().getCacheDir().resolve(JAVA).resolve("gen-" + gen); }
    }

    private Reader reader(String home, String discovery, Consumer<ClusterConfig> tune) {
        return reader(home, discovery, PeerDiscovery.SYSTEM_RESOLVER, tune);
    }

    private Reader reader(String home, String discovery, PeerDiscovery.Resolver resolver, Consumer<ClusterConfig> tune) {
        IndexConfig config = IndexConfig.defaults();
        Path h = tmp.resolve(home);
        config.setIndexDir(h.resolve("indexes"));
        config.setSynonymsFile(h.resolve("synonyms.txt"));
        ClusterConfig c = new ClusterConfig();
        c.setRole(NodeRole.READER);
        c.setBackend(BackendKind.PEER_PULL);
        c.setNodeId(home);
        c.setAdvertiseUrl("http://" + home + ":7070");
        c.setToken(TOKEN);
        c.setCacheDir(h.resolve("cluster-cache"));
        c.setConnectTimeoutMillis(500);
        c.getPeers().setDiscovery(discovery);
        c.getPeers().setRefreshSeconds(1);
        c.getPeers().setLongPollSeconds(2);
        c.getReader().setMode(ClusterConfig.ReaderConfig.FULL);
        tune.accept(c);
        c.validate();
        config.setCluster(c);
        PeerBackend backend = new PeerBackend(config, resolver);
        ProjectRegistry registry = ClusterRuntime.registryFor(config);
        LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene);
        Reader r = new Reader(config, backend, registry, lucene, runtime,
                new SearchEngine(lucene, EmbeddingProvider.create(config), registry));
        open.add(() -> {
            runtime.close();
            lucene.close();
        });
        return r;
    }

    private static void lazy(ClusterConfig c) {
        ClusterConfig.ReaderConfig r = c.getReader();
        r.setMode(ClusterConfig.ReaderConfig.LAZY);
        r.setEagerMaxBytes(0);
        r.setPrefetchExtensions(List.of());
        r.setBlockSizeBytes(64 * 1024);
        r.setCacheMaxBytes(64L * 1024 * 1024);
    }

    /** A stand-in writer answering the catalog, peers, latest and synonyms routes from mutable state. */
    private final class FakeWriter implements AutoCloseable {
        final String node;
        final Map<String, Long> shards = new ConcurrentHashMap<>();
        final List<String> peers = new java.util.concurrent.CopyOnWriteArrayList<>();
        final AtomicReference<String> synonyms = new AtomicReference<>();
        volatile int latestStatus = 200;
        final java.util.concurrent.atomic.AtomicInteger requests = new java.util.concurrent.atomic.AtomicInteger();
        final Javalin app;

        FakeWriter(String node, String host, int port) {
            this.node = node;
            app = Javalin.create();
            app.before("/replication/*", ctx -> {
                requests.incrementAndGet();
                if (!TOKEN.equals(ctx.header(InternalApi.TOKEN_HEADER))) {
                    ctx.status(401).result("{\"error\":\"unauthorized\"}");
                    ctx.skipRemainingHandlers();
                }
            });
            app.get("/replication/catalog", ctx -> {
                List<Map<String, Object>> rows = new ArrayList<>();
                new TreeMap<>(shards).forEach((s, g) -> rows.add(Map.of("shard", s, "latest", g)));
                ctx.json(Map.of("writer", node, "shards", rows));
            });
            app.get("/replication/peers", ctx -> ctx.json(Map.of("writer", node, "peers", List.copyOf(peers))));
            app.get("/replication/synonyms", ctx -> {
                String s = synonyms.get();
                if (s == null) {
                    ctx.status(404).result("{\"error\":\"none\"}");
                    return;
                }
                ctx.header("ETag", "\"" + Integer.toHexString(s.hashCode()) + "\"").result(s);
            });
            app.get("/replication/{shard}/latest", ctx -> {
                Long g = shards.get(ctx.pathParam("shard"));
                if (latestStatus != 200) {
                    ctx.status(latestStatus).result("{\"error\":\"fake failure\"}");
                } else if (g == null) {
                    ctx.status(404).result("{\"error\":\"no generation\"}");
                } else {
                    ctx.json(Map.of("generation", g, "manifest", "gen-" + g + ".json"));
                }
            });
            app.start(host, port);
            open.add(this);
        }

        FakeWriter(String node) {
            this(node, "localhost", 0);
        }

        String url() { return "http://localhost:" + app.port(); }

        @Override
        public void close() {
            app.stop();
        }
    }

    private PeerCatalog catalog(String discovery, PeerDiscovery.Resolver resolver) {
        return catalog(discovery, resolver, null);
    }

    /** @param allowHosts {@code cluster.peers.allowHosts}; null: the seeds' hosts */
    private PeerCatalog catalog(String discovery, PeerDiscovery.Resolver resolver, List<String> allowHosts) {
        PeerHttp http = new PeerHttp(TOKEN, null, Duration.ofMillis(500), Duration.ofSeconds(10));
        open.add(http);
        return new PeerCatalog(new PeerDiscovery(PeerDiscovery.parse(discovery), "http", resolver, http, allowHosts),
                http, Duration.ofSeconds(1), System::nanoTime);
    }

    private static List<String> nodes(List<PeerCatalog.WriterRef> writers) {
        return writers.stream().map(PeerCatalog.WriterRef::node).toList();
    }

    private static List<String> ids(SearchEngine engine, String project, String query) throws IOException {
        return engine.search(new SearchRequest(query, SearchRequest.SearchType.KEYWORD, project, null, 10,
                "text", null, null, 0)).stream().map(SearchResult::id).toList();
    }

    private static void await(String what, BooleanSupplier condition, Duration max) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(50);
        }
    }

    private static void copy(Path from, Path to) throws IOException {
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path dest = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(dest);
                else Files.copy(p, dest);
            }
        }
    }

    // ── discovery and catalog ────────────────────────────────────────────────

    @Test
    void writersDiscoveredByDnsAndSeeds() throws Exception {
        // DNS: three writers on one port at three loopback addresses; the fake resolver answers 1, then 3, then 2
        int port;
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            port = s.getLocalPort();
        }
        List<FakeWriter> fakes = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            FakeWriter w = new FakeWriter("dns-w" + i, "127.0.0." + i, port);
            w.shards.put("shard-" + i, (long) i);
            fakes.add(w);
        }
        AtomicReference<List<String>> answer = new AtomicReference<>(List.of("127.0.0.1"));
        List<Integer> lookups = new ArrayList<>();
        PeerCatalog dns = catalog("dns:writers.test:" + port, host -> {
            assertThat(host).isEqualTo("writers.test");
            lookups.add(1);
            List<InetAddress> out = new ArrayList<>();
            for (String a : answer.get()) out.add(InetAddress.getByName(a));
            return out;
        });
        dns.refresh();
        assertThat(nodes(dns.liveWriters())).containsExactly("dns-w1");
        assertThat(dns.shards()).containsExactly(new ShardId("shard-1"));
        answer.set(List.of("127.0.0.1", "127.0.0.2", "127.0.0.3"));
        dns.refresh();
        assertThat(nodes(dns.liveWriters())).containsExactly("dns-w1", "dns-w2", "dns-w3");
        assertThat(dns.shards()).extracting(ShardId::value).containsExactly("shard-1", "shard-2", "shard-3");
        assertThat(dns.owner(new ShardId("shard-3")).orElseThrow().url()).hasToString("http://127.0.0.3:" + port);
        answer.set(List.of("127.0.0.1", "127.0.0.3"));
        dns.refresh();
        assertThat(nodes(dns.liveWriters())).containsExactly("dns-w1", "dns-w3");
        assertThat(dns.ownerLive(new ShardId("shard-2"))).as("its writer is missing").isFalse();
        assertThat(lookups).as("resolved afresh on every refresh").hasSize(3);

        // Seeds: s1 names s2, s2 names s3; all three are found within one refresh
        FakeWriter s1 = new FakeWriter("seed-1");
        FakeWriter s2 = new FakeWriter("seed-2");
        FakeWriter s3 = new FakeWriter("seed-3");
        s1.peers.add(s2.url());
        s2.peers.add(s3.url() + "/");
        s3.peers.add(s1.url());
        s3.shards.put("only-on-3", 4L);
        PeerCatalog seeds = catalog("seeds:" + s1.url(), PeerDiscovery.SYSTEM_RESOLVER, List.of("localhost", "127.0.0.1"));
        seeds.refresh();
        assertThat(nodes(seeds.liveWriters())).containsExactly("seed-1", "seed-2", "seed-3");
        assertThat(seeds.shards()).containsExactly(new ShardId("only-on-3"));
        // s2 stops naming s3: it is still polled (learned) and stays live until forgotten
        s2.peers.clear();
        seeds.refresh();
        assertThat(nodes(seeds.liveWriters())).contains("seed-3");
        // two URLs answering for one node are one writer
        s1.peers.add(s3.url().replace("localhost", "127.0.0.1"));
        seeds.refresh();
        assertThat(nodes(seeds.liveWriters())).containsExactly("seed-1", "seed-2", "seed-3");
        assertThat(seeds.conflicts()).isEmpty();
    }

    /** Review Focus 0: the higher LATEST wins, the owner never alternates, the conflict is reported. */
    @Test
    void conflictingClaimsPickHigherGenerationAndAreReported() throws Exception {
        FakeWriter a = new FakeWriter("writer-a");
        FakeWriter b = new FakeWriter("writer-b");
        a.shards.put("s1", 5L);
        b.shards.put("s1", 7L);
        a.shards.put("only-a", 1L);
        PeerCatalog cat = catalog("seeds:" + a.url() + "," + b.url(), PeerDiscovery.SYSTEM_RESOLVER);
        PeerHttp http = new PeerHttp(TOKEN, null, Duration.ofMillis(500), Duration.ofSeconds(10));
        open.add(http);
        PeerReplicaSource source = new PeerReplicaSource(cat, http, Duration.ofSeconds(2));
        ShardId s1 = new ShardId("s1");
        for (int i = 0; i < 5; i++) { // no alternation over refreshes
            cat.refresh();
            assertThat(cat.owner(s1).orElseThrow().node()).isEqualTo("writer-b");
            assertThat(source.latest(s1)).contains(new Generation(7));
        }
        assertThat(cat.conflicts()).singleElement().asString()
                .contains("shard s1 claimed by writer-a and writer-b").contains("syncing from writer-b");
        assertThat(cat.owner(new ShardId("only-a")).orElseThrow().node()).isEqualTo("writer-a");

        a.shards.put("s1", 7L); // a tie: the lowest node id (P5-R9), the same on every refresh
        for (int i = 0; i < 3; i++) {
            cat.refresh();
            assertThat(cat.owner(s1).orElseThrow().node()).isEqualTo("writer-a");
        }
        a.shards.put("s1", 8L); // a newer generation on A: A wins
        cat.refresh();
        assertThat(cat.owner(s1).orElseThrow().node()).isEqualTo("writer-a");
        assertThat(source.latest(s1)).contains(new Generation(8));
        b.shards.remove("s1"); // B stops claiming: no conflict any more
        cat.refresh();
        assertThat(cat.conflicts()).isEmpty();
        assertThat(cat.owner(s1).orElseThrow().node()).isEqualTo("writer-a");
    }

    /** P5-R4: sections of every writer merged canonically; a section served twice is a reported conflict. */
    @Test
    void synonymsConflictReported() throws Exception {
        FakeWriter a = new FakeWriter("writer-a");
        FakeWriter b = new FakeWriter("writer-b");
        a.shards.put("alpha", 3L);
        a.shards.put("shared", 2L);
        b.shards.put("beta", 1L);
        b.shards.put("shared", 9L); // B has the higher LATEST of the shared shard
        a.synonyms.set("hand, written\n\n# ── Auto-mined from 'alpha' ──\nfoo, bar # auto:alpha:x\n\n"
                + "# ── Auto-mined from 'shared' ──\nold, rule # auto:shared:x\n");
        b.synonyms.set("hand, written\n\n# ── Auto-mined from 'beta' ──\nbaz, qux # auto:beta:x\n\n"
                + "# ── Auto-mined from 'shared' ──\nnew, rule # auto:shared:x\n");
        List<String> merged = new ArrayList<>();
        for (String order : new String[]{a.url() + "," + b.url(), b.url() + "," + a.url()}) {
            PeerCatalog cat = catalog("seeds:" + order, PeerDiscovery.SYSTEM_RESOLVER);
            PeerHttp http = new PeerHttp(TOKEN, null, Duration.ofMillis(500), Duration.ofSeconds(10));
            open.add(http);
            PeerReplicaSource source = new PeerReplicaSource(cat, http, Duration.ofSeconds(2));
            cat.refresh();
            String text = new String(source.synonyms().orElseThrow(), StandardCharsets.UTF_8);
            merged.add(text);
            assertThat(source.synonymsConflicts()).singleElement().asString()
                    .isEqualTo("synonyms of shard shared served by writer-a and writer-b; using writer-b's");
            assertThat(cat.conflicts()).singleElement().asString().contains("shard shared claimed by writer-a and writer-b");
        }
        assertThat(merged.get(1)).as("identical bytes on every reader").isEqualTo(merged.get(0));
        assertThat(merged.get(0)).isEqualTo("hand, written\n\n"
                + "# ── Auto-mined from 'alpha' ──\nfoo, bar # auto:alpha:x\n\n"
                + "# ── Auto-mined from 'beta' ──\nbaz, qux # auto:beta:x\n\n"
                + "# ── Auto-mined from 'shared' ──\nnew, rule # auto:shared:x\n");
    }

    // ── sync over HTTP ───────────────────────────────────────────────────────

    @Test
    void roundTripPublishSyncSearch() throws Exception {
        Writer w = new Writer("writer-a").start();
        w.index(JAVA, false);
        w.index(PY, false);
        Reader r = reader("reader-1", "seeds:" + w.url(), c -> { });
        assertThat(r.backend().kind()).isEqualTo(BackendKind.PEER_PULL);
        r.runtime().startServing();
        assertThat(r.runtime().served()).containsOnlyKeys(JAVA_SHARD, new ShardId(PY));

        SourceReader.setEnabled(false); // a reader never reads sources: compare like with like
        assertThat(ClusterTestAccess.snapshot(r.engine(), r.registry()))
                .isEqualTo(ClusterTestAccess.snapshot(w.engine, w.registry));
        SourceReader.setEnabled(true);

        // the long-poll brings the next generation without an explicit sync
        long g2 = w.addMethod("tripleTheValue");
        await("generation " + g2 + " served", () -> r.runtime().served().get(JAVA_SHARD).value() == g2,
                Duration.ofSeconds(20));
        assertThat(ids(r.engine(), JAVA, "tripleTheValue")).contains(JAVA + ":com.example.Calculator#tripleTheValue(int)");
        Map<String, Object> peers = r.runtime().peerStatus();
        assertThat(peers).containsEntry("conflicts", List.of()).containsEntry("unavailable", List.of());
        assertThat(peers.get("writers")).asList().singleElement().asString().contains("writer-a");
    }

    @Test
    void writerJoinsAndLeaves() throws Exception {
        Writer a = new Writer("writer-a").start();
        Writer b = new Writer("writer-b").reservePort(); // not up yet
        a.index(JAVA, false);
        b.index(PY, false);
        // refreshes only when the test says so (syncNow would otherwise refresh once a second)
        Reader r = reader("reader-1", "seeds:" + a.url() + "," + b.url(), c -> c.getPeers().setRefreshSeconds(3600));
        r.runtime().startServing();
        assertThat(r.runtime().served()).containsOnlyKeys(JAVA_SHARD);

        b.start(); // joins: its shard becomes syncable on the next refresh
        r.catalog().refresh();
        r.runtime().syncNow();
        assertThat(r.runtime().served()).containsOnlyKeys(JAVA_SHARD, new ShardId(PY));
        assertThat(nodes(r.catalog().liveWriters())).containsExactly("writer-a", "writer-b");

        a.stop(); // leaves: missing for 2 refreshes, dropped on the 3rd; its generation keeps serving
        for (int i = 1; i <= PeerCatalog.DROP_AFTER; i++) {
            r.catalog().refresh();
            r.runtime().syncNow();
            boolean tracked = r.catalog().writers().stream().anyMatch(s -> s.node().equals("writer-a"));
            assertThat(tracked).as("tracked after %d missed refreshes", i).isEqualTo(i < PeerCatalog.DROP_AFTER);
            assertThat(r.runtime().served()).containsKey(JAVA_SHARD);
        }
        assertThat(r.catalog().shards()).contains(JAVA_SHARD);
        assertThat(r.catalog().orphans()).containsExactly(JAVA_SHARD);
        assertThat(r.runtime().servesShard(JAVA)).isTrue();
        assertThat(ids(r.engine(), JAVA, "Calculator")).isNotEmpty();
        assertThat(r.runtime().peerStatus().get("unavailable")).asList().isEmpty();

        // a live writer that stops listing its shard releases it: the reader stops serving it
        b.backend.peerStore().store().delete(b.backend.peerStore().layout().latestKey(new ShardId(PY)));
        r.catalog().refresh();
        r.runtime().syncNow();
        assertThat(r.runtime().served()).containsOnlyKeys(JAVA_SHARD);
    }

    @Test
    void lazyModeWorksOverPeer() throws Exception {
        Writer w = new Writer("writer-a").start();
        long gen = w.publishWithoutCompoundFiles();
        Reader lazyReader = reader("lazy", "seeds:" + w.url(), PeerPullTest::lazy);
        Reader full = reader("full", "seeds:" + w.url(), c -> { });
        lazyReader.runtime().startServing();
        full.runtime().startServing();
        assertThat(lazyReader.runtime().served()).containsEntry(JAVA_SHARD, new Generation(gen));
        ShardManifest m = w.backend.peerStore().source().manifest(JAVA_SHARD, new Generation(gen));
        List<ManifestFile> remote = m.files().stream().filter(f -> ManifestFile.LUCENE.equals(f.role()))
                .filter(f -> !Files.exists(lazyReader.genDir(gen).resolve("lucene").resolve(f.name()))).toList();
        assertThat(remote).as("files left remote by the lazy generation").isNotEmpty();
        int withHits = 0;
        for (String q : List.of("Calculator", "greeting service", "add two numbers", "user email", "getName")) {
            List<String> want = ids(full.engine(), JAVA, q);
            assertThat(ids(lazyReader.engine(), JAVA, q)).as(q).isEqualTo(want);
            if (!want.isEmpty()) withHits++;
        }
        assertThat(withHits).isGreaterThan(2);
        assertThat(((Number) lazyReader.runtime().readerCacheStats().get("bytesFetched")).longValue())
                .as("blocks fetched over HTTP ranges").isPositive();
    }

    /** Review Focus 3: the reader serves what it has on disk, reports the rest, and keeps polling. */
    @Test
    void writerDownAtStartServesOnDiskGenerations() throws Exception {
        Writer a = new Writer("writer-a").start();
        long g1 = a.index(JAVA, false);
        Reader first = reader("reader-1", "seeds:" + a.url(), c -> { });
        first.runtime().startServing();
        assertThat(first.runtime().served()).containsEntry(JAVA_SHARD, new Generation(g1));
        first.runtime().close();
        first.lucene().close();
        a.stop();

        FakeWriter ghost = new FakeWriter("ghost-writer"); // up, but its shard never syncs
        ghost.shards.put("ghost", 3L);
        ghost.latestStatus = 500;
        Reader r = reader("reader-1", "seeds:" + a.url() + "," + ghost.url(), c -> { });
        assertThat(r.runtime().servesShard(JAVA)).as("activated from disk at construction").isTrue();
        r.runtime().startServing();
        assertThat(r.runtime().ready()).isTrue();
        assertThat(ids(r.engine(), JAVA, "Calculator")).isNotEmpty();
        Map<String, Object> peers = r.runtime().peerStatus();
        assertThat(peers.get("unavailable")).asList().singleElement().asString()
                .contains("ghost", "unavailable on this reader", "never synced from writer ghost-writer");

        // the writer comes back with a new generation: picked up by the long-poll loop, no explicit sync
        a.start();
        long g2 = a.addMethod("tripleTheValue");
        await("generation " + g2 + " served after the writer came back",
                () -> new Generation(g2).equals(r.runtime().served().get(JAVA_SHARD)), Duration.ofSeconds(30));
        assertThat(ids(r.engine(), JAVA, "tripleTheValue")).isNotEmpty();
    }

    @Test
    void writerDownMidRunKeepsServingCachedAndFailsColdReadsClearly() throws Exception {
        Writer a = new Writer("writer-a").start();
        long gen = a.publishWithoutCompoundFiles();
        Reader r = reader("reader-1", "seeds:" + a.url(), c -> {
            lazy(c);
            c.getPeers().setRefreshSeconds(3600); // only the test's refreshes count towards dropping the writer
        });
        r.runtime().startServing();
        List<String> before = ids(r.engine(), JAVA, "Calculator");
        assertThat(before).isNotEmpty();

        a.stop();
        r.catalog().refresh();
        r.runtime().syncNow(); // the failed sync is logged; the shard keeps serving
        assertThat(r.runtime().served()).containsEntry(JAVA_SHARD, new Generation(gen));
        assertThat(ids(r.engine(), JAVA, "Calculator")).as("cached blocks keep serving").isEqualTo(before);

        ShardManifest m = a.backend.peerStore().source().manifest(JAVA_SHARD, new Generation(gen));
        // A cold read: what the block cache fetches through when a query needs a block it does not hold.
        // (sample-app's files fit one block, which the warm open already cached, so the fetch is made
        // directly on the reader's source, the block cache's fetcher.)
        ManifestFile big = m.files().stream().filter(f -> ManifestFile.LUCENE.equals(f.role()))
                .filter(f -> !Files.exists(r.genDir(gen).resolve("lucene").resolve(f.name())))
                .max(Comparator.comparingLong(ManifestFile::length)).orElseThrow();
        assertThat(ClusterTestAccess.lazyGenerations(r.runtime()).eager(big)).as("remote in lazy mode").isFalse();
        com.pharos.cluster.ReplicaSource source = r.backend().source();
        assertThatThrownBy(() -> source.openRange(big, 0, big.length()))
                .isInstanceOf(PeerHttp.WriterUnreachableException.class)
                .hasMessageContaining("writer writer-a (" + a.url() + ") is unreachable");
        Path target = tmp.resolve("cold-read");
        assertThatThrownBy(() -> source.downloadRange(big, 0, big.length(), target))
                .isInstanceOf(PeerHttp.WriterUnreachableException.class).hasMessageContaining("writer-a");
        assertThat(target).doesNotExist();
        assertThat(r.runtime().degradation()).as("an outage is not a garbage-collected generation").isNull();
        assertThat(r.runtime().peerStatus().get("writers")).asList().singleElement().asString()
                .contains("live=false");
    }

    @Test
    void truststoreReachesAnHttpsWriter() throws Exception {
        String password = System.getenv("HOME");
        org.junit.jupiter.api.Assumptions.assumeTrue(password != null && password.length() >= 6,
                "needs HOME as a throw-away keystore password");
        Path keystore = tmp.resolve("tls.p12");
        Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
        Process p = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", "pharos-test", "-keyalg", "EC",
                "-groupname", "secp256r1", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-validity", "2", "-storetype", "PKCS12", "-keystore", keystore.toString(), "-storepass", password,
                "-keypass", password).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(p.waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(p.exitValue()).as(out).isZero();

        Writer w = new Writer("writer-a");
        w.index(JAVA, false);
        com.pharos.web.WebServer web = new com.pharos.web.WebServer(null, null, null, null, null, NodeRole.WRITER);
        web.setReplicationEndpoint(w.backend.endpoint());
        web.setTls(new com.pharos.web.WebServer.Tls(keystore, "PKCS12", password.toCharArray()));
        Javalin app = web.start(0, tmp.resolve("web-home"));
        open.add(app::stop);
        String url = "https://localhost:" + app.port();

        ClusterConfig.TlsConfig tls = new ClusterConfig.TlsConfig();
        tls.setTruststore(keystore);
        tls.setPasswordEnv("HOME");
        Reader r = reader("reader-1", "seeds:" + url, c -> c.setTls(tls));
        r.runtime().startServing();
        assertThat(r.runtime().served()).containsOnlyKeys(JAVA_SHARD);

        PeerCatalog untrusted = catalog("seeds:" + url, PeerDiscovery.SYSTEM_RESOLVER);
        untrusted.refresh();
        assertThat(untrusted.liveWriters()).as("the JDK's default trust does not know the writer").isEmpty();
        assertThat(untrusted.lastError()).isNotNull();
    }

    @Test
    void objectStoreBackendUnchanged() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        IndexConfig config = ClusterFixture.config(tmp.resolve("os-reader"), NodeRole.READER, "bucket");
        ReplicationBackend backend = TestBackends.objectStore(config, store);
        assertThat(backend).isInstanceOf(ObjectStoreBackend.class);
        assertThat(backend.catalog()).isInstanceOf(ObjectStoreCatalog.class);
        assertThat(backend.source()).isInstanceOf(ObjectStoreReplicaSource.class).isNotInstanceOf(LongPollingSource.class);
        ProjectRegistry registry = ClusterRuntime.registryFor(config);
        LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene);
        open.add(() -> {
            runtime.close();
            lucene.close();
        });
        assertThat(runtime.peerStatus()).isNull();

        ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
        ReplicaSyncer syncer = new ReplicaSyncer(new ObjectStoreCatalog(store, layout),
                new ObjectStoreReplicaSource(store, layout), new ReaderLocalCache(tmp.resolve("os-cache")),
                ProjectRegistry.at(tmp.resolve("os-registry.json")), tmp.resolve("os-cache"), null, Duration.ofHours(1));
        open.add(syncer);
        syncer.start(Duration.ofHours(1));
        Set<String> threads = Thread.getAllStackTraces().keySet().stream().map(Thread::getName)
                .collect(java.util.stream.Collectors.toSet());
        assertThat(threads).contains("pharos-replica-syncer").doesNotContain("pharos-replica-longpoll");
        assertThat(BackendKind.fromJson("object-store")).isEqualTo(BackendKind.OBJECT_STORE);
    }

    /** A writer that freezes mid-body must not hang the syncer or a block fetch: the transfer fails. */
    @Test
    void stalledTransferFails() throws Exception {
        FakeWriter w = new FakeWriter("writer-a");
        w.shards.put("s1", 1L);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        open.add(release::countDown);
        w.app.get("/replication/s1/files/<key>", ctx -> {
            ctx.status(206).header("Content-Range", "bytes 0-99/100");
            ctx.res().setContentLengthLong(100);
            var out = ctx.res().getOutputStream();
            out.write(new byte[10]);
            out.flush();
            release.await(30, TimeUnit.SECONDS); // frozen
        });
        PeerCatalog cat = catalog("seeds:" + w.url(), PeerDiscovery.SYSTEM_RESOLVER);
        PeerHttp http = new PeerHttp(TOKEN, null, Duration.ofMillis(500), Duration.ofSeconds(10));
        open.add(http);
        http.setStallTimeout(Duration.ofSeconds(1));
        PeerReplicaSource source = new PeerReplicaSource(cat, http, Duration.ofSeconds(2));
        cat.refresh();
        ManifestFile f = new ManifestFile(ManifestFile.LUCENE, "_0.cfs", "shards/s1/lucene/_0.cfs@0a", 100, "0a");
        long start = System.nanoTime();
        assertThatThrownBy(() -> source.downloadRange(f, 0, 100, tmp.resolve("stalled")))
                .isInstanceOf(IOException.class).hasMessageContaining("stalled");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(10));
        assertThat(tmp.resolve("stalled")).doesNotExist();
    }

    /**
     * C1: the owner missing one or two refreshes keeps the shard while a lower claimer stays live:
     * no owner change, no rollback of the served generation, no flip of the synonyms section.
     */
    @Test
    void missedRefreshNeverRollsBackToALowerWriter() throws Exception {
        String section = "# ── Auto-mined from '" + JAVA + "' ──\n";
        Writer a = new Writer("writer-a").start();
        Files.writeString(a.config.getSynonymsFile(), section + "alpha, apex # auto:" + JAVA + ":x\n");
        a.index(JAVA, false);
        a.addMethod("twice");
        long high = a.addMethod("thrice");
        Writer b = new Writer("writer-b").start();
        Files.writeString(b.config.getSynonymsFile(), section + "beta, base # auto:" + JAVA + ":x\n");
        long low = b.index(JAVA, false);
        assertThat(high).isGreaterThan(low);
        Reader r = reader("reader-1", "seeds:" + a.url() + "," + b.url(), c -> c.getPeers().setRefreshSeconds(3600));
        r.runtime().startServing();
        assertThat(r.runtime().served()).containsEntry(JAVA_SHARD, new Generation(high));
        String synonyms = Files.readString(r.config().getSynonymsFile());
        assertThat(synonyms).contains("alpha, apex").doesNotContain("beta, base");

        a.stop();
        for (int miss = 1; miss < PeerCatalog.DROP_AFTER; miss++) {
            r.catalog().refresh();
            r.runtime().syncNow();
            assertThat(r.catalog().owner(JAVA_SHARD).orElseThrow().node()).as("owner after %d misses", miss)
                    .isEqualTo("writer-a");
            assertThat(r.runtime().served()).as("served after %d misses", miss)
                    .containsEntry(JAVA_SHARD, new Generation(high));
            assertThat(Files.readString(r.config().getSynonymsFile())).as("synonyms after %d misses", miss)
                    .isEqualTo(synonyms);
            assertThat(r.catalog().conflicts()).as("still reported").singleElement().asString()
                    .contains("shard " + JAVA + " claimed by writer-a and writer-b", "syncing from writer-a");
        }
        a.start(); // back before it is dropped: nothing moved
        r.catalog().refresh();
        r.runtime().syncNow();
        assertThat(r.catalog().owner(JAVA_SHARD).orElseThrow().node()).isEqualTo("writer-a");
        assertThat(r.runtime().served()).containsEntry(JAVA_SHARD, new Generation(high));
    }

    /** P5-R9: what a reader serves depends on the claims only, not on which writers it saw before. */
    @Test
    void readersWithDifferentHistoriesConverge() throws Exception {
        int portA;
        try (ServerSocket sock = new ServerSocket(0)) {
            portA = sock.getLocalPort();
        }
        FakeWriter b = new FakeWriter("writer-b");
        FakeWriter c = new FakeWriter("writer-c");
        b.shards.put("s1", 4L);
        b.synonyms.set("# ── Auto-mined from 's1' ──\nb, rule # auto:s1:x\n");
        c.shards.put("s2", 1L);
        c.synonyms.set("# ── Auto-mined from 's2' ──\nc, rule # auto:s2:x\n");
        String seeds = "seeds:http://localhost:" + portA + "," + b.url() + "," + c.url();
        PeerHttp http = new PeerHttp(TOKEN, null, Duration.ofMillis(500), Duration.ofSeconds(10));
        open.add(http);

        // reader 1 starts early: it sees B and C, then C goes away for good
        PeerCatalog cat1 = catalog(seeds, PeerDiscovery.SYSTEM_RESOLVER);
        PeerReplicaSource src1 = new PeerReplicaSource(cat1, http, Duration.ofSeconds(2));
        cat1.refresh();
        assertThat(cat1.owner(new ShardId("s1")).orElseThrow().node()).isEqualTo("writer-b");
        assertThat(new String(src1.synonyms().orElseThrow(), StandardCharsets.UTF_8)).contains("c, rule");
        c.close();
        for (int i = 0; i < PeerCatalog.DROP_AFTER; i++) cat1.refresh();
        // A comes up claiming s1 at the same LATEST as B
        FakeWriter a = new FakeWriter("writer-a", "localhost", portA);
        a.shards.put("s1", 4L);
        a.synonyms.set("# ── Auto-mined from 's1' ──\na, rule # auto:s1:x\n");
        cat1.refresh();

        // reader 2 starts now: it never saw C, and saw A and B together
        PeerCatalog cat2 = catalog(seeds, PeerDiscovery.SYSTEM_RESOLVER);
        PeerReplicaSource src2 = new PeerReplicaSource(cat2, http, Duration.ofSeconds(2));
        cat2.refresh();

        assertThat(cat1.owner(new ShardId("s1")).orElseThrow().node()).isEqualTo("writer-a");
        assertThat(cat2.owner(new ShardId("s1")).orElseThrow().node()).isEqualTo("writer-a");
        byte[] syn1 = src1.synonyms().orElseThrow();
        byte[] syn2 = src2.synonyms().orElseThrow();
        assertThat(new String(syn1, StandardCharsets.UTF_8)).isEqualTo(new String(syn2, StandardCharsets.UTF_8))
                .contains("a, rule").doesNotContain("c, rule").doesNotContain("b, rule");
    }

    /** P5-R8: the token only goes to seeds and allow-listed (or, with TLS, https) gossiped writers. */
    @Test
    void gossipedUrlsAreFollowedOnlyWhenTrusted() throws Exception {
        FakeWriter evil = new FakeWriter("evil", "127.0.0.1", 0); // host 127.0.0.1, not a seed host
        evil.shards.put("stolen", 1L);
        FakeWriter seed = new FakeWriter("seed");
        seed.peers.add("http://127.0.0.1:" + evil.app.port());
        PeerCatalog cat = catalog("seeds:" + seed.url(), PeerDiscovery.SYSTEM_RESOLVER);
        for (int i = 0; i < 3; i++) cat.refresh();
        assertThat(evil.requests.get()).as("a gossiped, non-allow-listed URL never receives a request").isZero();
        assertThat(nodes(cat.liveWriters())).containsExactly("seed");

        // allow-listed: the learned writer is followed
        PeerCatalog allowed = catalog("seeds:" + seed.url(), PeerDiscovery.SYSTEM_RESOLVER, List.of("localhost", "127.0.0.1"));
        allowed.refresh();
        assertThat(nodes(allowed.liveWriters())).containsExactly("evil", "seed");
        assertThat(allowed.shards()).contains(new ShardId("stolen"));

        // with TLS only https gossip entries count, whatever the host
        PeerHttp http = new PeerHttp(TOKEN, null, Duration.ofMillis(500), Duration.ofSeconds(10));
        open.add(http);
        PeerDiscovery tls = new PeerDiscovery(PeerDiscovery.parse("seeds:https://w1:7070"), "https",
                PeerDiscovery.SYSTEM_RESOLVER, http, List.of("w2"));
        assertThat(tls.accepted(URI.create("http://w2:7070"))).isFalse();
        assertThat(tls.accepted(URI.create("https://anywhere:7070"))).isTrue();
    }

    /** I3: the direct DNS query honours the resolv.conf search list before falling back. */
    @Test
    void dnsQueryTriesTheSearchDomains() throws Exception {
        String conf = "# generated\nnameserver 10.96.0.10\nsearch ns.svc.cluster.local svc.cluster.local cluster.local\n"
                + "options ndots:5\n";
        InetAddress w1 = InetAddress.getByName("10.1.0.7");
        List<String> queried = new ArrayList<>();
        Map<String, List<InetAddress>> dns = Map.of("pharos-writers.svc.cluster.local", List.of(w1));
        List<String> fellBack = new ArrayList<>();
        PeerDiscovery.DnsResolver resolver = new PeerDiscovery.DnsResolver(() -> conf, name -> {
            queried.add(name);
            return dns.getOrDefault(name, List.of());
        }, host -> {
            fellBack.add(host);
            return List.of(InetAddress.getByName("127.0.0.1"));
        });
        assertThat(resolver.resolve("pharos-writers")).containsExactly(w1);
        assertThat(queried).containsExactly("pharos-writers.ns.svc.cluster.local", "pharos-writers.svc.cluster.local");
        assertThat(fellBack).isEmpty();

        queried.clear();
        assertThat(resolver.resolve("pharos-writers.example.")).hasSize(1); // qualified: no search, then fallback
        assertThat(queried).containsExactly("pharos-writers.example");
        assertThat(fellBack).containsExactly("pharos-writers.example.");
        assertThat(PeerDiscovery.DnsResolver.searchDomains("domain corp.example\n")).containsExactly("corp.example");
        assertThat(PeerDiscovery.DnsResolver.searchDomains("")).isEmpty();
    }

    @Test
    void discoverySpecParsing() {
        assertThat(PeerDiscovery.parse("dns:writers.ns.svc.cluster.local")).isEqualTo(
                new PeerDiscovery.Dns("writers.ns.svc.cluster.local", PeerDiscovery.DEFAULT_PORT,
                        "dns:writers.ns.svc.cluster.local"));
        assertThat(PeerDiscovery.parse(" dns:w:9000 ")).isEqualTo(new PeerDiscovery.Dns("w", 9000, "dns:w:9000"));
        PeerDiscovery.Spec seeds = PeerDiscovery.parse("seeds:http://a:7070/, https://b ,http://a:7070");
        assertThat(((PeerDiscovery.Seeds) seeds).urls()).containsExactly(URI.create("http://a:7070"), URI.create("https://b"));
        assertThatThrownBy(() -> PeerDiscovery.parse("seeds:http://user:hunter2@a")).hasMessageNotContaining("hunter2");
    }
}
