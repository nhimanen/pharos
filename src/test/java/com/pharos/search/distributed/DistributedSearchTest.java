package com.pharos.search.distributed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.mcp.McpToolRegistry;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchRequest.SearchType;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
import com.pharos.search.distributed.ParityCorpus.HashEmbeddings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static com.pharos.cluster.ClusterFixture.PY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Phase 3 coordinator over real reader nodes in one JVM: each node syncs its pinned shards from
 * one {@link InMemoryObjectStore} bucket, heartbeats into it, and serves {@code /internal/*} on an
 * ephemeral port. Shards are indexed (and published) by the {@link ClusterFixture} writer with
 * deterministic hash embeddings, so vector and hybrid search have vectors.
 *
 * <p>The shared cluster: reader {@code a} holds {@value #PHAROS}, {@code b} holds {@code sample-app},
 * {@code all} holds both (the single-node reference: its queries never leave it). Failure tests build
 * their own small clusters.
 */
class DistributedSearchTest {

    static final String PHAROS = "pharos-fixture";
    private static final String BUCKET = "test-bucket";
    private static final HashEmbeddings EMBEDDER = new HashEmbeddings();

    private static Path shared;
    private static ClusterFixture cluster;
    private static TestReaderNode a;
    private static TestReaderNode b;
    private static TestReaderNode all;

    @TempDir Path tmp;

    /** Sample-app queries of the eval set (SingleNodeGoldenTest's categories). */
    static final List<String> SAMPLE_QUERIES = List.of("Calculator", "greeting service",
            "Computes the absolute value of an integer", "generate a greeting message for a user",
            "division by zero is not allowed", "user display name and email address",
            "checks whether user has a valid email", "methods that call getName");

    @BeforeAll
    static void startCluster() throws Exception {
        shared = Files.createTempDirectory("distributed-search");
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(shared.resolve("fixture"), store, store, BUCKET, EMBEDDER);
        Path pharosRoot = shared.resolve("pharos-src");
        ParityCorpus.unzip(ParityCorpus.PHAROS_CORPUS, pharosRoot);
        cluster.indexSource(pharosRoot, PHAROS);
        cluster.index(JAVA, false);
        cluster.indexSource(skewSource(shared.resolve("skew-a"), 100, 5, 1), SKEW_A);
        cluster.indexSource(skewSource(shared.resolve("skew-b"), 1, 5, 4), SKEW_B);
        a = new TestReaderNode(shared.resolve("a"), store, BUCKET, "a", List.of(PHAROS, SKEW_A), EMBEDDER, null);
        b = new TestReaderNode(shared.resolve("b"), store, BUCKET, "b", List.of(JAVA, SKEW_B), EMBEDDER, null);
        all = new TestReaderNode(shared.resolve("all"), store, BUCKET, "all", List.of(), EMBEDDER, null);
        for (TestReaderNode n : List.of(a, b, all)) {
            n.awaitShard(PHAROS, 2);
            n.awaitShard(JAVA, 2);
            n.awaitShard(SKEW_A, 2);
            n.awaitShard(SKEW_B, 2);
        }
        assertThat(a.parts.runtime().servesShard(PHAROS)).isTrue();
        assertThat(a.parts.runtime().servesShard(JAVA)).isFalse();
        assertThat(all.parts.runtime().servesShard(JAVA) && all.parts.runtime().servesShard(PHAROS)).isTrue();
    }

    @AfterAll
    static void stopCluster() throws Exception {
        for (TestReaderNode n : new TestReaderNode[]{a, b, all}) if (n != null) n.close();
        if (cluster != null) cluster.close();
        if (shared != null) {
            try (Stream<Path> s = Files.walk(shared)) {
                s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    static final String SKEW_A = "skew-a";
    static final String SKEW_B = "skew-b";
    static final String RARE = "zebrafinch";

    /**
     * A project of {@code classes} × {@code methods} methods (plus class documents); {@code withRare}
     * of the methods mention {@value #RARE}, every one mentions {@code quokka}. skew-a: ~500 documents,
     * the rare term in one; skew-b: 5 methods, the rare term in 4 — shard-local statistics then rank
     * the two shards' rare-term hits very differently from a single node.
     */
    static Path skewSource(Path root, int classes, int methods, int withRare) throws Exception {
        Path pkg = Files.createDirectories(root.resolve("src/main/java/skew"));
        int rare = 0;
        String prefix = root.getFileName().toString().replace("-", "");
        for (int c = 0; c < classes; c++) {
            StringBuilder sb = new StringBuilder("package skew;\n\npublic class " + prefix + "C" + c + " {\n");
            for (int m = 0; m < methods; m++) {
                String word = rare++ < withRare ? RARE + " quokka" : "quokka plain";
                sb.append("    public String m").append(m).append("() { return \"").append(word)
                        .append(" value ").append(c).append(' ').append(m).append("\"; }\n");
            }
            Files.writeString(pkg.resolve(prefix + "C" + c + ".java"), sb.append("}\n").toString());
        }
        return root;
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    static SearchResponse search(TestReaderNode node, String q, SearchType type, String project, int limit)
            throws Exception {
        return node.engine.searchWithTrace(new SearchRequest(q, type, project, null, limit, "text", null, null, 0),
                false);
    }

    static List<String> bits(List<SearchResult> rs) {
        return rs.stream().map(r -> r.id() + " " + Integer.toHexString(Float.floatToRawIntBits(r.score()))
                + " " + r.searchType()).toList();
    }

    static List<String> ids(List<SearchResult> rs) {
        return rs.stream().map(SearchResult::id).toList();
    }

    private static List<String> queries() {
        List<String> qs = new ArrayList<>(ParityCorpus.PHAROS_QUERIES);
        qs.addAll(SAMPLE_QUERIES);
        return qs;
    }

    static int ulps(float x, float y) {
        int ix = Float.floatToRawIntBits(x), iy = Float.floatToRawIntBits(y);
        if (ix < 0) ix = Integer.MIN_VALUE - ix;
        if (iy < 0) iy = Integer.MIN_VALUE - iy;
        return Math.abs(ix - iy);
    }

    static boolean near(float x, float y) {
        return x == y || ulps(x, y) <= 4 || Math.abs(x - y) <= 1e-6 * Math.max(Math.abs(x), Math.abs(y));
    }

    /**
     * The Phase 2 gate's float-tolerance rule: scores near (4 ulp or 1e-6 relative) rank by rank, ids
     * may differ only within groups of near-tied scores (the group at the last rank of a full page may
     * also trade ids with ranks past it). Null when they agree, else why not.
     */
    static String compareTolerant(List<SearchResult> x, List<SearchResult> y, int limit) {
        if (x.size() != y.size()) return "length " + x.size() + " vs " + y.size();
        for (int i = 0; i < x.size(); i++) {
            if (!near(x.get(i).score(), y.get(i).score())) {
                return "rank " + (i + 1) + ": " + x.get(i).score() + " vs " + y.get(i).score();
            }
        }
        int n = x.size();
        for (int i = 0; i < n; ) {
            int j = i;
            while (j + 1 < n && near(y.get(j).score(), y.get(j + 1).score())) j++;
            List<String> gx = ids(x.subList(i, j + 1)), gy = ids(y.subList(i, j + 1));
            if (!gx.equals(gy)) {
                boolean boundary = j == n - 1 && n == limit;
                if (!new java.util.TreeSet<>(gx).equals(new java.util.TreeSet<>(gy)) && !boundary) {
                    return "ranks " + (i + 1) + "-" + (j + 1) + ": " + gx + " vs " + gy;
                }
            }
            i = j + 1;
        }
        return null;
    }

    /** Kendall τ over the ids both rankings hold (1 when fewer than two are shared). */
    static double kendallTau(List<String> x, List<String> y) {
        List<String> common = x.stream().filter(y::contains).toList();
        if (common.size() < 2) return 1.0;
        long concordant = 0, discordant = 0;
        for (int i = 0; i < common.size(); i++) {
            for (int j = i + 1; j < common.size(); j++) {
                int dx = Integer.compare(x.indexOf(common.get(i)), x.indexOf(common.get(j)));
                int dy = Integer.compare(y.indexOf(common.get(i)), y.indexOf(common.get(j)));
                if (dx * dy > 0) concordant++;
                else if (dx * dy < 0) discordant++;
            }
        }
        return (double) (concordant - discordant) / (concordant + discordant);
    }

    private static long internalCalls(TestReaderNode... nodes) {
        long n = 0;
        for (TestReaderNode node : nodes) n += node.drainCalls().size();
        return n;
    }

    // ── tests ────────────────────────────────────────────────────────────────

    static SearchResponse search(TestReaderNode node, String q, SearchType type, String project, int limit,
                                 boolean expand) throws Exception {
        return node.engine.searchWithTrace(new SearchRequest(q, type, project, null, limit, "text", null, null, 0),
                expand);
    }

    static double jaccard(List<String> x, List<String> y) {
        Set<String> u = new LinkedHashSet<>(x);
        u.addAll(y);
        if (u.isEmpty()) return 1.0;
        return (double) x.stream().filter(y::contains).distinct().count() / u.size();
    }

    @Test
    void crossProjectFanOutMatchesSingleNode() throws Exception {
        internalCalls(a, b, all);
        List<String> vectorDiffs = new ArrayList<>();
        Map<SearchType, double[]> tau = new java.util.EnumMap<>(SearchType.class); // sum, min, n
        List<String> idSetDiffs = new ArrayList<>();
        for (String q : queries()) {
            SearchResponse fan = search(a, q, SearchType.KEYWORD, null, 10);
            SearchResponse one = search(all, q, SearchType.KEYWORD, null, 10);
            assertThat(fan.warnings()).as(q).isEmpty();
            assertThat(bits(fan.results())).as("keyword top-10 " + q).isNotEmpty().isEqualTo(bits(one.results()));

            String v = compareTolerant(search(a, q, SearchType.VECTOR, null, 10).results(),
                    search(all, q, SearchType.VECTOR, null, 10).results(), 10);
            if (v != null) vectorDiffs.add(q + ": " + v);

            for (SearchType type : List.of(SearchType.HYBRID, SearchType.UNIFIED, SearchType.AUTO)) {
                SearchResponse f = search(a, q, type, null, 10, true);
                List<String> hf = ids(f.results());
                List<String> h1 = ids(search(all, q, type, null, 10, true).results());
                assertThat(hf).as(type + " " + q).isNotEmpty();
                assertThat(f.warnings()).as(type + " " + q).isEmpty();
                if (jaccard(hf, h1) < 1.0) idSetDiffs.add(type + " " + q + ": " + hf + " vs " + h1);
                double t = kendallTau(hf, h1);
                double[] acc = tau.computeIfAbsent(type, k -> new double[]{0, 1, 0});
                acc[0] += t;
                acc[1] = Math.min(acc[1], t);
                acc[2]++;
            }
        }
        tau.forEach((type, acc) -> System.out.printf("crossProjectFanOut: %s (expand) over %d queries: mean Kendall tau %.4f, min %.4f%n",
                type, (int) acc[2], acc[0] / acc[2], acc[1]));
        System.out.printf("crossProjectFanOut: vector diffs %d, id-set diffs %d%n", vectorDiffs.size(), idSetDiffs.size());
        vectorDiffs.forEach(d -> System.out.println("  vector diff: " + d));
        idSetDiffs.forEach(d -> System.out.println("  id-set diff: " + d));
        assertThat(vectorDiffs).as("vector top-10 within the ulp tolerance").isEmpty();
        assertThat(idSetDiffs).as("same ids (Jaccard 1.0) for hybrid, unified, auto with expansion").isEmpty();
        tau.forEach((type, acc) -> assertThat(acc[0] / acc[2]).as(type + " mean Kendall tau").isGreaterThanOrEqualTo(0.95));
        List<TestReaderNode.Call> served = new ArrayList<>(b.drainCalls());
        served.addAll(all.drainCalls());
        assertThat(served).as("a fanned out to the readers of the shards it lacks").isNotEmpty()
                .allSatisfy(c -> assertThat(c.shard()).isIn(JAVA, SKEW_B));
        assertThat(a.drainCalls()).as("nobody asked a: b and all hold what they search").isEmpty();
    }

    /**
     * The control: the same keyword comparison with shard-local statistics must differ from the
     * single node on many queries, or the parity test above could not tell global statistics apart.
     */
    @Test
    void shardLocalStatisticsWouldDiffer() throws Exception {
        DistributedSearch ds = a.ds();
        int differ = 0;
        ds.globalStatistics = false;
        try {
            for (String q : queries()) {
                if (!bits(search(a, q, SearchType.KEYWORD, null, 10).results())
                        .equals(bits(search(all, q, SearchType.KEYWORD, null, 10).results()))) differ++;
            }
        } finally {
            ds.globalStatistics = true;
        }
        System.out.printf("shardLocalStatisticsWouldDiffer: %d of %d keyword top-10s differ%n", differ, queries().size());
        assertThat(differ).as("queries whose top-10 changes without global statistics").isGreaterThanOrEqualTo(20);
    }

    /** A rare term in a big shard, common in a small one: only global statistics rank it as a single node. */
    @Test
    void skewedTermNeedsGlobalStatistics() throws Exception {
        String q = RARE + " quokka";
        List<String> single = bits(search(all, q, SearchType.KEYWORD, null, 10).results());
        List<String> fan = bits(search(a, q, SearchType.KEYWORD, null, 10).results());
        assertThat(fan).anyMatch(r -> r.startsWith(SKEW_A + ":")).anyMatch(r -> r.startsWith(SKEW_B + ":"));
        assertThat(fan).as("global statistics: as a single node").isEqualTo(single);
        DistributedSearch ds = a.ds();
        ds.globalStatistics = false;
        try {
            assertThat(bits(search(a, q, SearchType.KEYWORD, null, 10).results()))
                    .as("shard-local statistics").isNotEqualTo(single);
        } finally {
            ds.globalStatistics = true;
        }
    }

    /** Review Focus 5: one shard through fan-out is bitwise that shard's own local search. */
    @Test
    void singleShardThroughFanOutIsBitwiseLocal() throws Exception {
        internalCalls(a, b, all);
        for (SearchType type : SearchType.values()) {
            for (String q : SAMPLE_QUERIES) {
                SearchResponse fan = search(a, q, type, JAVA, 10);
                SearchResponse local = search(b, q, type, JAVA, 10);
                assertThat(fan.warnings()).as(type + " " + q).isEmpty();
                assertThat(bits(fan.results())).as(type + " " + q).isEqualTo(bits(local.results()));
            }
            SearchResponse deep = search(a, "Calculator divide", type, JAVA, 25);
            assertThat(bits(deep.results())).as(type + " limit 25")
                    .isEqualTo(bits(search(b, "Calculator divide", type, JAVA, 25).results()));
        }
        List<TestReaderNode.Call> calls = new ArrayList<>(b.drainCalls());
        calls.addAll(all.drainCalls());
        assertThat(calls).as("a searched sample-app remotely").isNotEmpty()
                .allSatisfy(c -> assertThat(c.shard()).isEqualTo(JAVA));
        assertThat(calls).as("one shard: single-round retrieval, no second round trip")
                .noneSatisfy(c -> assertThat(c.requestJson().path("retriever").asText())
                        .matches(".*-(knn|recall|rescore)"));
        assertThat(a.drainCalls()).as("b never asked: it holds the shard").isEmpty();
    }

    /** Every configured pipeline fans out with no index reader at the coordinator: none of its mergers or rerankers reads the index. */
    @Test
    void mergersAndRerankersNeverReadTheIndex() throws Exception {
        for (SearchType type : SearchType.values()) {
            SearchResponse r = search(a, "interface for embedding generation", type, null, 10);
            assertThat(r.results()).as(type.name()).isNotEmpty();
            assertThat(r.warnings()).as(type.name()).isEmpty();
        }
    }

    @Test
    void localWhenCoordinatorHoldsAll() throws Exception {
        internalCalls(a, b, all);
        for (SearchType type : SearchType.values()) {
            for (String q : List.of("Calculator", "BordaMerger", "find code that maps a parsed method to a Lucene document")) {
                assertThat(search(all, q, type, null, 10).results()).as(type + " " + q).isNotEmpty();
                search(all, q, type, JAVA, 10);
                all.engine.searchWithTrace(new SearchRequest(q, type, null, null, 10, "text", null, null, 0), true);
            }
        }
        all.engine.findCallers("com.example.model.User#getName()", null, new ArrayList<>());
        all.engine.traceCallChain("com.example.model.User#getName()", 2, "callers", 100, new ArrayList<>());
        assertThat(internalCalls(a, b, all)).as("/internal calls while the coordinator holds every shard").isZero();
    }

    @Test
    void graphToolsRouteToOwningShard() throws Exception {
        String getName = "com.example.model.User#getName()";
        internalCalls(a, b, all);
        // a (holding pharos-fixture only) answers graph tools on sample-app methods...
        List<String> w = new ArrayList<>();
        List<SearchResult> callers = a.engine.findCallers(getName, null, w);
        assertThat(callers).isNotEmpty();
        String greet = callers.get(0).id().substring(JAVA.length() + 1);
        List<Object> fromA = graphAnswers(a, getName, greet, w);
        assertThat(w).isEmpty();
        List<TestReaderNode.Call> graphCalls = new ArrayList<>(b.drainCalls());
        graphCalls.addAll(all.drainCalls());
        assertThat(graphCalls).as("a asked the readers of the shards it lacks").isNotEmpty()
                .allSatisfy(c -> assertThat(c.shard()).as(c.path()).isIn(JAVA, SKEW_B))
                .anySatisfy(c -> assertThat(c.shard()).isEqualTo(JAVA));

        // ...exactly as a single node (all holds every shard) answers them locally; callers are
        // scored with the summed statistics of their one term, so bitwise
        assertThat(bits(callers)).isEqualTo(bits(all.engine.findCallers(getName, null, new ArrayList<>())));
        assertThat(fromA).isEqualTo(graphAnswers(all, getName, greet, new ArrayList<>()));
    }

    private static List<Object> graphAnswers(TestReaderNode n, String getName, String greet, List<String> w)
            throws Exception {
        ObjectMapper m = new ObjectMapper();
        List<Object> out = new ArrayList<>();
        List<SearchResult> callees = n.engine.findCallees(greet, null, w);
        assertThat(callees).as(n.id + " callees of " + greet).isNotEmpty();
        out.add(ids(callees));
        out.add(n.engine.getMethodByFqn(greet, w));
        out.add(n.engine.getByFqns(List.of(greet, getName), w));
        out.add(n.engine.findUsages(getName, "all", null, w));
        out.add(n.engine.traceCallChain(getName, 3, "callers", 200, w));
        out.add(n.engine.findTransitiveCallers(getName, 3, 100, w));
        McpToolRegistry mcp = n.mcp();
        out.add(mcp.call("get_callers", m.createObjectNode().put("fqn", getName)));
        out.add(mcp.call("get_callees", m.createObjectNode().put("fqn", greet)));
        out.add(mcp.call("get_method", m.createObjectNode().put("fqn", greet)));
        String path = mcp.call("find_call_path", m.createObjectNode().put("from_fqn", greet).put("to_fqn", getName));
        assertThat(path).as(n.id + " path").contains("\"hops\":1");
        out.add(path);
        return out;
    }

    // ── failure handling (own clusters) ──────────────────────────────────────

    /** A writer with sample-app and sample-python-app, a coordinator on python and readers on sample-app. */
    private record Small(ClusterFixture fixture, InMemoryObjectStore store, TestReaderNode coordinator,
                         List<TestReaderNode> replicas) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            for (TestReaderNode r : replicas) r.close();
            coordinator.close();
            fixture.close();
        }
    }

    private int smallClusters;

    private Small small(int replicas, String coordinatorSynonyms) throws Exception {
        return small(replicas, coordinatorSynonyms, EMBEDDER, cfg -> { });
    }

    private Small small(int replicas, String coordinatorSynonyms, com.pharos.embedding.EmbeddingProvider coordinatorEmbedder,
                        java.util.function.Consumer<com.pharos.config.ClusterConfig> coordinatorConfig) throws Exception {
        Path home = tmp.resolve("small-" + smallClusters++);
        InMemoryObjectStore store = new InMemoryObjectStore();
        ClusterFixture f = new ClusterFixture(home.resolve("fixture"), store, store, BUCKET, EMBEDDER);
        f.indexBoth();
        TestReaderNode c = new TestReaderNode(home.resolve("c"), store, BUCKET, "c", List.of(PY), coordinatorEmbedder,
                coordinatorSynonyms, coordinatorConfig);
        List<TestReaderNode> rs = new ArrayList<>();
        for (int i = 1; i <= replicas; i++) {
            rs.add(new TestReaderNode(home.resolve("r" + i), store, BUCKET, "r" + i, List.of(JAVA), EMBEDDER, null));
        }
        c.awaitShard(JAVA, replicas);
        return new Small(f, store, c, rs);
    }

    /** Review Focus 1: a replica dying without draining is retried on another; no query loses the shard. */
    @Test
    void deadReplicaRetriedElsewhere() throws Exception {
        try (Small s = small(2, null)) {
            List<String> before = bits(search(s.coordinator(), "GreetingService farewell", SearchType.KEYWORD, null, 10).results());
            assertThat(before).anyMatch(r -> r.startsWith(JAVA + ":"));
            s.replicas().get(0).kill(); // heartbeat still live
            for (int i = 0; i < 4; i++) { // the router rotates: r1 comes first at least once
                SearchResponse r = search(s.coordinator(), "GreetingService farewell", SearchType.KEYWORD, null, 10);
                assertThat(r.warnings()).as("query " + i).isEmpty();
                assertThat(bits(r.results())).as("query " + i).isEqualTo(before);
            }
            assertThat(s.coordinator().ds().suspect("r1")).as("r1 was attempted (and failed)").isTrue();
            assertThat(s.coordinator().ds().suspect("r2")).isFalse();
        }
    }

    /** Review Focus 1: with no replica left, the other shards still answer and the lost one is named. */
    @Test
    void noReplicaGivesPartialWithWarning() throws Exception {
        try (Small s = small(2, null)) {
            s.replicas().forEach(TestReaderNode::kill);
            SearchResponse r = search(s.coordinator(), "multiply", SearchType.HYBRID, null, 10);
            assertThat(r.results()).isNotEmpty().allSatisfy(x -> assertThat(x.project()).isEqualTo(PY));
            assertThat(r.warnings()).hasSize(1);
            assertThat(r.warnings().get(0)).startsWith("shard " + JAVA + " unavailable: ")
                    .endsWith("; results exclude it");

            SearchResponse only = search(s.coordinator(), "Calculator", SearchType.KEYWORD, JAVA, 10);
            assertThat(only.results()).isEmpty();
            assertThat(only.warnings()).singleElement().asString().startsWith("shard " + JAVA + " unavailable: ");
        }
    }

    /** Review Focus 3: replicas on different generations; one query's calls for a shard all hit one reader and one generation. */
    @Test
    void perQueryPinningNeverMixesGenerations() throws Exception {
        try (Small s = small(1, null)) {
            TestReaderNode r1 = s.replicas().get(0); // generation 1 of sample-app
            Path calc = s.fixture().source.resolve(JAVA).resolve("src/main/java/com/example/Calculator.java");
            Files.writeString(calc, Files.readString(calc).replace("class Calculator {",
                    "class Calculator {\n    public int tripleTheValue(int x) { return 3 * x; }\n"));
            Files.setLastModifiedTime(calc, FileTime.fromMillis(System.currentTimeMillis() + 5000));
            s.fixture().index(JAVA, true); // generation 2, which only r2 syncs
            try (TestReaderNode r2 = new TestReaderNode(tmp.resolve("r2"), s.store(), BUCKET, "r2", List.of(JAVA),
                    EMBEDDER, null)) {
                s.coordinator().awaitShard(JAVA, 2);
                Map<String, Set<Long>> generationsByNode = new HashMap<>();
                Set<String> paths = new LinkedHashSet<>();
                for (int i = 0; i < 6; i++) {
                    r1.drainCalls();
                    r2.drainCalls();
                    SearchResponse resp = s.coordinator().engine.searchWithTrace(new SearchRequest(
                            "tripleTheValue Calculator", SearchType.HYBRID, null, null, 10, "text", null, null, 0), true);
                    s.coordinator().engine.newSnippetDecorator(15, resp).decorate(resp.results(), "q");
                    List<TestReaderNode.Call> calls = new ArrayList<>(r1.drainCalls());
                    calls.addAll(r2.drainCalls());
                    assertThat(calls).as("query " + i).isNotEmpty();
                    Set<String> nodes = calls.stream().map(TestReaderNode.Call::node).collect(Collectors.toSet());
                    Set<Long> gens = calls.stream().map(c -> c.responseJson().path("generation").asLong())
                            .collect(Collectors.toSet());
                    assertThat(nodes).as("one reader per shard in query " + i).hasSize(1);
                    assertThat(gens).as("one generation in query " + i).hasSize(1);
                    assertThat(calls).extracting(TestReaderNode.Call::status).containsOnly(200);
                    calls.forEach(c -> paths.add(c.path()));
                    String node = nodes.iterator().next();
                    generationsByNode.computeIfAbsent(node, k -> new LinkedHashSet<>()).addAll(gens);
                    boolean newMethod = resp.results().stream().anyMatch(x -> x.id().contains("tripleTheValue"));
                    assertThat(newMethod).as("query " + i + " on " + node).isEqualTo(node.equals("r2"));
                }
                assertThat(paths).contains("/internal/term-stats", "/internal/shard-search", "/internal/snippets",
                        "/internal/callees");
                assertThat(generationsByNode).as("both replicas served, on their own generations").hasSize(2);
                assertThat(generationsByNode.get("r1")).doesNotContainAnyElementsOf(generationsByNode.get("r2"));
            }
        }
    }

    /** Note 4: a reader analysing with other synonyms than the coordinator refuses; its shard is reported, not mixed in. */
    @Test
    void synonymsMismatchRefused() throws Exception {
        try (Small s = small(1, "calculator, adder\n")) {
            SearchResponse r = search(s.coordinator(), "multiply", SearchType.KEYWORD, null, 10);
            assertThat(r.results()).isNotEmpty().allSatisfy(x -> assertThat(x.project()).isEqualTo(PY));
            assertThat(r.warnings()).singleElement().asString()
                    .startsWith("shard " + JAVA + " unavailable: ").contains("synonyms");
            assertThat(s.replicas().get(0).drainCalls()).extracting(TestReaderNode.Call::status).contains(409);
        }
    }

    // ── fix round 1: load, deadlines, pinned readers, mid-query death, models ───

    /**
     * I1: many concurrent queries while the replicas are slow (they answer, under the call timeout, so
     * nobody is marked suspect and the router cannot steer around them) never lose a shard, the
     * coordinator's own included: remote calls wait for a per-reader permit within the query budget,
     * local shards are searched on the query's thread, and no queue wait eats a round's deadline.
     */
    @Test
    void concurrentQueriesWithAStalledReplicaKeepHealthyShards() throws Exception {
        try (Small s = small(2, null, EMBEDDER, cfg -> {
            cfg.setFanoutTimeoutMillis(1500);
            cfg.setQueryTimeoutMillis(15000);
        })) {
            String q = "GreetingService farewell";
            List<String> expected = ids(search(s.coordinator(), q, SearchType.KEYWORD, null, 10).results());
            assertThat(expected).anyMatch(id -> id.startsWith(JAVA + ":")).anyMatch(id -> id.startsWith(PY + ":"));
            for (TestReaderNode r : s.replicas()) {
                r.stallPath = "/internal/";
                r.stallMillis = 1200; // answers, under the 1500 ms call timeout
            }
            int n = 160;
            java.util.concurrent.ExecutorService clients = java.util.concurrent.Executors.newFixedThreadPool(n);
            try {
                List<java.util.concurrent.Future<SearchResponse>> fs = new ArrayList<>();
                java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
                for (int i = 0; i < n; i++) {
                    fs.add(clients.submit(() -> {
                        go.await();
                        return search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
                    }));
                }
                go.countDown();
                for (var f : fs) {
                    SearchResponse r = f.get(60, java.util.concurrent.TimeUnit.SECONDS);
                    assertThat(r.warnings()).as("no shard lost under load").isEmpty();
                    assertThat(ids(r.results())).isEqualTo(expected);
                }
            } finally {
                clients.shutdownNow();
                s.replicas().forEach(r -> r.stallPath = null);
            }
        }
    }

    /** I2: one deadline per query covers every round; a stalled search round cannot stretch it. */
    @Test
    void queryDeadlineCoversEveryRound() throws Exception {
        try (Small s = small(2, null, EMBEDDER, cfg -> {
            cfg.setFanoutTimeoutMillis(1400); // two attempts of 1400 ms each: the query budget must cut them
            cfg.setQueryTimeoutMillis(1500);
        })) {
            for (TestReaderNode r : s.replicas()) { // term stats answer at once, the search round stalls
                r.stallPath = "/internal/shard-search";
                r.stallMillis = 10_000;
            }
            long t0 = System.nanoTime();
            SearchResponse r = search(s.coordinator(), "GreetingService multiply", SearchType.HYBRID, null, 10);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            s.replicas().forEach(x -> x.stallPath = null);
            assertThat(ms).as("query time with a 1500 ms query timeout").isLessThan(2_200);
            assertThat(r.results()).isNotEmpty().allSatisfy(x -> assertThat(x.project()).isEqualTo(PY));
            assertThat(r.warnings()).singleElement().asString().startsWith("shard " + JAVA + " unavailable: ");
        }
    }

    /** I2: a query holds a reference on its local readers: a swap closing the reader meanwhile cannot break it. */
    @Test
    void pinnedLocalReaderSurvivesItsRetirementUntilTheQueryCloses() throws Exception {
        try (Small s = small(1, null)) {
            DistributedSearch ds = s.coordinator().ds();
            String fqn;
            org.apache.lucene.index.IndexReader pinned;
            try (DistributedSearch.Query q = ds.open(List.of())) {
                assertThat(q.fansOut()).isTrue();
                SearchResult any = search(s.coordinator(), "multiply", SearchType.KEYWORD, PY, 1).results().get(0);
                fqn = any.id().substring(PY.length() + 1);
                assertThat(q.docs(List.of(fqn))).containsKey(fqn); // pins the local python shard
                pinned = s.coordinator().parts.lucene().openReader(PY);
                int refs = pinned.getRefCount();
                pinned.close(); // what LuceneIndexer's retirement does after a swap
                assertThat(q.docs(List.of(fqn))).as("still readable while pinned").containsKey(fqn);
                q.vectorResolver(List.of(any), "multiply").resolve(any); // reads the pinned reader: no AlreadyClosed
                assertThat(pinned.getRefCount()).isEqualTo(refs - 1);
            }
            assertThat(pinned.getRefCount()).as("released when the query closed").isZero();
        }
    }

    /** I4d: the pinned reader dies after the statistics round; the shard is retried on its replica (same generation). */
    @Test
    void readerDyingAfterTermStatsIsRetriedOnItsReplica() throws Exception {
        try (Small s = small(2, null)) {
            String q = "GreetingService farewell";
            List<String> expected = bits(search(s.coordinator(), q, SearchType.KEYWORD, null, 10).results());
            s.replicas().forEach(r -> { r.drainCalls(); r.dieAfterStats = true; });
            SearchResponse r = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            assertThat(r.warnings()).isEmpty();
            assertThat(bits(r.results())).isEqualTo(expected);
            List<TestReaderNode.Call> calls = new ArrayList<>(s.replicas().get(0).drainCalls());
            calls.addAll(s.replicas().get(1).drainCalls());
            TestReaderNode.Call stats = calls.stream().filter(c -> c.path().endsWith("/term-stats")).findFirst().orElseThrow();
            assertThat(s.coordinator().ds().suspect(stats.node())).as("the stats reader died (503) and was marked").isTrue();
            assertThat(calls).as("its replica answered the search, on the pinned generation")
                    .anySatisfy(c -> {
                        assertThat(c.node()).isNotEqualTo(stats.node());
                        assertThat(c.path()).endsWith("/shard-search");
                        assertThat(c.status()).isEqualTo(200);
                        assertThat(c.requestJson().path("generation").asLong())
                                .isEqualTo(stats.responseJson().path("generation").asLong());
                    })
                    .noneSatisfy(c -> {
                        assertThat(c.node()).isEqualTo(stats.node());
                        assertThat(c.path()).endsWith("/shard-search");
                        assertThat(c.status()).isEqualTo(200);
                    });
        }
        try (Small s = small(1, null)) { // no replica to take over: the shard is named
            s.replicas().get(0).dieAfterStats = true;
            SearchResponse r = search(s.coordinator(), "GreetingService multiply", SearchType.KEYWORD, null, 10);
            assertThat(r.results()).isNotEmpty().allSatisfy(x -> assertThat(x.project()).isEqualTo(PY));
            assertThat(r.warnings()).singleElement().asString().startsWith("shard " + JAVA + " unavailable: ");
        }
    }

    /** I3: shards without vectors of the coordinator's model refuse; each is named, keyword results stay. */
    @Test
    void shardsWithoutVectorsOfTheModelAreNamed() throws Exception {
        com.pharos.embedding.EmbeddingProvider other = new HashEmbeddings() {
            @Override public String modelId() { return "other-model"; }
        };
        try (Small s = small(1, null, other, cfg -> { })) {
            SearchResponse v = search(s.coordinator(), "multiply", SearchType.VECTOR, null, 10);
            assertThat(v.results()).isEmpty();
            assertThat(v.warnings()).containsExactlyInAnyOrder(
                    "shard " + PY + " has no vectors for model other-model",
                    "shard " + JAVA + " has no vectors for model other-model");
            SearchResponse h = search(s.coordinator(), "GreetingService multiply", SearchType.HYBRID, null, 10);
            assertThat(h.results()).anyMatch(x -> x.project().equals(JAVA)).anyMatch(x -> x.project().equals(PY));
            assertThat(h.warnings()).contains("shard " + JAVA + " has no vectors for model other-model");
            assertThat(s.replicas().get(0).drainCalls()).extracting(TestReaderNode.Call::status).contains(409);
        }
    }

    /** Minor: a reader that never read the catalog (bucket down since it started) still answers from its shards. */
    @Test
    void neverReadCatalogSearchesTheShardsHeldHere() throws Exception {
        try (Small s = small(1, null)) {
            s.store().failGetOn("catalog/shards.json");
            SearchResponse r = search(s.coordinator(), "multiply", SearchType.KEYWORD, null, 10);
            assertThat(r.results()).isNotEmpty().allSatisfy(x -> assertThat(x.project()).isEqualTo(PY));
            assertThat(r.warnings()).singleElement().asString().startsWith("searching only this reader's shards: ");
            SearchResponse scoped = search(s.coordinator(), "Calculator", SearchType.KEYWORD, JAVA, 10);
            assertThat(scoped.results()).as("a scoped query needs no catalog").isNotEmpty()
                    .allSatisfy(x -> assertThat(x.project()).isEqualTo(JAVA));
        }
    }

    /**
     * Fix round 2: past cluster.maxCatalogAgeSeconds a reader answers from the shards it holds (all of
     * them: complete results) with a warning naming the catalog's age and the shards not searched.
     */
    @Test
    void tooOldCatalogAnswersFromTheShardsHeldHere() throws Exception {
        try (Small s = small(1, null, EMBEDDER, cfg -> cfg.setMaxCatalogAgeSeconds(1));
             TestReaderNode full = new TestReaderNode(tmp.resolve("full"), s.store(), BUCKET, "full", List.of(),
                     EMBEDDER, null, cfg -> cfg.setMaxCatalogAgeSeconds(1))) {
            full.awaitShard(JAVA, 2);
            full.awaitShard(PY, 2);
            String q = "GreetingService multiply";
            List<String> fullBefore = ids(search(full, q, SearchType.KEYWORD, null, 10).results());
            assertThat(search(s.coordinator(), q, SearchType.KEYWORD, null, 10).warnings()).isEmpty(); // catalog read
            s.store().failGetOn("catalog/shards.json");
            Thread.sleep(2_200);

            SearchResponse onFull = search(full, q, SearchType.KEYWORD, null, 10);
            assertThat(ids(onFull.results())).as("holds every shard: complete").isEqualTo(fullBefore);
            assertThat(onFull.warnings()).singleElement().asString()
                    .startsWith("searching only this reader's shards: the shard catalog was last read")
                    .doesNotContain("not searched");
            assertThat(full.engine.findCallers("com.example.model.User#getName()", null, new ArrayList<>()))
                    .as("graph ops too").isNotEmpty();

            SearchResponse onPinned = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            assertThat(onPinned.results()).isNotEmpty().allSatisfy(x -> assertThat(x.project()).isEqualTo(PY));
            assertThat(onPinned.warnings()).singleElement().asString()
                    .contains("over cluster.maxCatalogAgeSeconds").endsWith("; not searched: " + JAVA);
        }
    }
}
