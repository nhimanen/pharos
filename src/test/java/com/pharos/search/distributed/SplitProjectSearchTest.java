package com.pharos.search.distributed;

import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.routing.ShardFilter;
import com.pharos.cluster.sharding.DocKeys;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchRequest.SearchType;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
import com.pharos.search.distributed.ParityCorpus.HashEmbeddings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6 Task 3 through real reader nodes: a split project queried through the coordinator scores
 * bitwise as the unsplit project (P6-R6), its graph tools answer from the project graph, a module
 * filter touches only the matching shards, a filter that can't prune fans out and the trace says why
 * (Review Focus 4), and a query never mixes two index runs of a project across readers.
 *
 * <p>The cluster: the pharos corpus indexed twice, as {@value #WHOLE} and split into three ring shards
 * as {@value #SPLIT}; {@value #MOD}, split by module. Reader {@code c} holds only sample-app (every split
 * shard is remote to it), reader {@code r} holds everything.
 */
class SplitProjectSearchTest {

    static final String WHOLE = "pharos-fixture";
    static final String SPLIT = "pharos-split";
    static final String MOD = "modproj";
    static final ShardSpec RING3 = ShardSpec.parse("[project, ring(path, shards=3)]");
    static final ShardSpec BY_MODULE = ShardSpec.parse("[project, module]");
    static final List<ShardId> SPLIT_SHARDS = List.of(new ShardId(SPLIT + "~ring=s01"),
            new ShardId(SPLIT + "~ring=s02"), new ShardId(SPLIT + "~ring=s03"));
    static final ShardId MOD_CORE = new ShardId(MOD + "~module=core");
    static final List<ShardId> MOD_SHARDS = List.of(MOD_CORE, new ShardId(MOD + "~module=web"),
            new ShardId(MOD + "~module=_"));
    private static final String BUCKET = "test-bucket";
    private static final HashEmbeddings EMBEDDER = new HashEmbeddings();

    private static Path shared;
    private static ClusterFixture cluster;
    private static TestReaderNode c;
    private static TestReaderNode r;

    @BeforeAll
    static void startCluster() throws Exception {
        shared = Files.createTempDirectory("split-search");
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(shared.resolve("fixture"), store, store, BUCKET, EMBEDDER);
        Path pharosRoot = shared.resolve("pharos-src");
        ParityCorpus.unzip(ParityCorpus.PHAROS_CORPUS, pharosRoot);
        cluster.indexSource(pharosRoot, WHOLE);
        cluster.indexSplit(pharosRoot, SPLIT, RING3, SPLIT_SHARDS, false);
        Path modSrc = moduleSource(shared.resolve("mod-src"));
        cluster.indexSplit(modSrc, MOD, BY_MODULE, MOD_SHARDS, false);
        cluster.indexSource(modSrc, MOD_WHOLE);
        cluster.index(JAVA, false);
        c = new TestReaderNode(shared.resolve("c"), store, BUCKET, "c", List.of(JAVA), EMBEDDER, null);
        r = new TestReaderNode(shared.resolve("r"), store, BUCKET, "r", List.of(), EMBEDDER, null);
        for (TestReaderNode n : List.of(c, r)) {
            for (ShardId s : SPLIT_SHARDS) n.awaitShard(s.value(), 1);
            for (ShardId s : MOD_SHARDS) n.awaitShard(s.value(), 1);
            n.awaitShard(WHOLE, 1);
            n.awaitShard(MOD_WHOLE, 1);
        }
        assertThat(c.parts.runtime().servesShard(SPLIT_SHARDS.get(0).value())).isFalse();
    }

    @AfterAll
    static void stopCluster() throws Exception {
        for (TestReaderNode n : new TestReaderNode[]{c, r}) if (n != null) n.close();
        if (cluster != null) cluster.close();
        deleteTree(shared);
    }

    static void deleteTree(Path root) throws Exception {
        if (root == null) return;
        try (Stream<Path> s = Files.walk(root)) {
            s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    /** Two Maven modules (core, web) and loose tools; every class mentions "widget". */
    static Path moduleSource(Path root) throws Exception {
        for (String module : List.of("core", "web")) {
            Files.createDirectories(root.resolve(module));
            Files.writeString(root.resolve(module).resolve("pom.xml"), "<project><artifactId>" + module
                    + "</artifactId></project>\n");
            for (int i = 0; i < 3; i++) {
                writeClass(root, module + "/src/main/java/com/example/" + module + "/" + cap(module) + i + ".java",
                        "com.example." + module, cap(module) + i, "widget " + module + " helper " + i);
            }
        }
        writeClass(root, "tools/com/example/tools/Tool.java", "com.example.tools", "Tool", "widget tool");
        return root;
    }

    private static String cap(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    static void writeClass(Path root, String rel, String pkg, String name, String words) throws Exception {
        Path f = root.resolve(rel);
        Files.createDirectories(f.getParent());
        Files.writeString(f, "package " + pkg + ";\n\n/** " + words + ". */\npublic class " + name + " {\n"
                + "    /** Builds a " + words + ". */\n    public String build" + name + "() { return \"" + words + "\"; }\n}\n");
    }

    static SearchResponse search(TestReaderNode node, String q, String project, ShardFilter filter) throws Exception {
        return node.engine.searchWithTrace(new SearchRequest(q, SearchType.KEYWORD, project, null, 10, "text", null,
                null, 0), false, filter);
    }

    /** The fqn part of a result id ({@code <project>:<fqn>}): the same document in either project. */
    static String fqn(String id) {
        return id.substring(id.indexOf(':') + 1);
    }

    static String bits(float f) {
        return Integer.toHexString(Float.floatToRawIntBits(f));
    }

    /**
     * Null when the split ranking is the unsplit one bitwise: the same scores rank by rank and the same
     * documents, in order except within a group of identical scores (ties break by shard, then by doc
     * id); a full page's last group may trade documents with ranks past it.
     */
    static String compareBitwise(List<SearchResult> split, List<SearchResult> whole) {
        if (split.size() != whole.size()) return "length " + split.size() + " vs " + whole.size();
        for (int i = 0; i < split.size(); i++) {
            if (Float.floatToRawIntBits(split.get(i).score()) != Float.floatToRawIntBits(whole.get(i).score())) {
                return "rank " + (i + 1) + ": " + bits(split.get(i).score()) + " vs " + bits(whole.get(i).score())
                        + " (" + split.get(i).id() + " / " + whole.get(i).id() + ")";
            }
        }
        for (int i = 0; i < split.size(); ) {
            int j = i;
            while (j + 1 < split.size() && split.get(j + 1).score() == split.get(i).score()) j++;
            Set<String> a = new TreeSet<>(), b = new TreeSet<>();
            for (int k = i; k <= j; k++) {
                a.add(fqn(split.get(k).id()));
                b.add(fqn(whole.get(k).id()));
            }
            if (!a.equals(b) && !(j == split.size() - 1 && split.size() == 10)) {
                return "ranks " + (i + 1) + "-" + (j + 1) + ": " + a + " vs " + b;
            }
            i = j + 1;
        }
        return null;
    }

    /**
     * P6-R6 and ruling P6-R11. (a) In the Phase 3 harness's style: through DistributedSearch, every query
     * scoped to the split project is bitwise (ids, scores, order) what a single node searching the same
     * shards locally returns (one MultiReader over them: the same documents and doc ids), with all three
     * shards remote and with all three held here. (b) Against the unsplit project, another index build of
     * the same files, with other doc ids: bitwise and in the same order for every query, also those with a
     * tie at the cut. Equal raw scores rank by a build-independent key ({@code KeywordSearchStrategy
     * .STABLE_ORDER}); found by the Phase 6 gate: with ties broken by doc id the two builds kept other
     * documents at the cut, and the page-relative graph boost then shifted every score on the page.
     */
    @Test
    void splitProjectScoresBitwiseAsTheUnsplitProject() throws Exception {
        SearchEngine single = new SearchEngine(r.parts.lucene(), EMBEDDER, r.parts.registry(),
                new com.pharos.search.pipeline.NoOpCrossEncoder(), new com.pharos.search.FstQueryClassifier(), r.configBase);
        com.pharos.search.KeywordSearchStrategy keyword = r.engine.shardService(x -> true, null).keyword();
        List<String> shardNames = SPLIT_SHARDS.stream().map(ShardId::value).toList();
        int compared = 0, tied = 0;
        for (String q : ParityCorpus.PHAROS_QUERIES) {
            List<SearchResult> local = single.searchWithTrace(new SearchRequest(q, SearchType.KEYWORD, null, shardNames,
                    10, "text", null, null, 0), false).results();
            // named in the project list, as the reference names the shards: no single-project affinity boost
            List<SearchResult> remote = searchList(c, q, SPLIT); // all three shards remote
            List<SearchResult> inProcess = searchList(r, q, SPLIT); // all three held here
            assertThat(remote).allSatisfy(x -> assertThat(x.project()).isEqualTo(SPLIT));
            assertThat(DistributedSearchTest.bits(remote)).as(q + " (remote)").isEqualTo(DistributedSearchTest.bits(local));
            assertThat(DistributedSearchTest.bits(inProcess)).as(q + " (in process)").isEqualTo(DistributedSearchTest.bits(local));

            List<SearchResult> whole = searchList(r, q, WHOLE); // the unsplit project, locally
            if (whole.isEmpty()) continue;
            compared++;
            // The raw keyword rankings, as the keyword pipeline's retrieval produces them: that pipeline has
            // no router, so no classification (a classified request filters by its docType, e.g. classes
            // only for "interface ...", and probes another ranking than the one the page is shaped from).
            List<RawCandidate> wholeRaw = keyword.retrieveRaw(new org.apache.lucene.search.IndexSearcher(
                    r.parts.lucene().openReader(WHOLE)), rawRequest(q, List.of(WHOLE), 10));
            List<RawCandidate> splitRaw = keyword.retrieveRaw(new org.apache.lucene.search.IndexSearcher(
                    r.parts.lucene().openMultiReader(shardNames)), rawRequest(q, shardNames, 10));
            assertThat(compareRaw(splitRaw, wholeRaw, 10)).as(q + " (raw)").isNull();
            assertThat(splitRaw.stream().map(x -> fqn(x.result().id())).toList()).as(q + " (raw order)")
                    .isEqualTo(wholeRaw.stream().map(x -> fqn(x.result().id())).toList());
            assertThat(remote.stream().map(x -> fqn(x.id()) + "=" + bits(x.score())).toList()).as(q)
                    .isEqualTo(whole.stream().map(x -> fqn(x.id()) + "=" + bits(x.score())).toList());
            List<RawCandidate> eleven = keyword.retrieveRaw(new org.apache.lucene.search.IndexSearcher(
                    r.parts.lucene().openReader(WHOLE)), rawRequest(q, List.of(WHOLE), 11));
            if (eleven.size() == 11 && eleven.get(9).rawScore() == eleven.get(10).rawScore()) tied++;
        }
        assertThat(compared).isGreaterThan(5);
        assertThat(tied).as("the corpus has queries with a tie at the cut").isPositive();
    }

    static SearchRequest rawRequest(String q, List<String> projects, int limit) {
        return new SearchRequest(q, SearchType.KEYWORD, null, projects, limit, "text", null, null, 0);
    }

    /**
     * Null when two raw top-k lists are the same ranking: identical scores rank by rank and the same
     * documents in each group of equal scores. "tied at the cut ..." when they differ only in which
     * documents of the last group (a group cut by the limit, of a full list) they keep. Else what differs.
     */
    static String compareRaw(List<RawCandidate> split, List<RawCandidate> whole, int limit) {
        if (split.size() != whole.size()) return "raw length " + split.size() + " vs " + whole.size();
        for (int i = 0; i < split.size(); i++) {
            if (Float.floatToRawIntBits(split.get(i).rawScore()) != Float.floatToRawIntBits(whole.get(i).rawScore())) {
                return "raw rank " + (i + 1) + ": " + bits(split.get(i).rawScore()) + " vs " + bits(whole.get(i).rawScore());
            }
        }
        for (int i = 0; i < split.size(); ) {
            int j = i;
            while (j + 1 < split.size() && split.get(j + 1).rawScore() == split.get(i).rawScore()) j++;
            Set<String> a = new TreeSet<>(), b = new TreeSet<>();
            for (int k = i; k <= j; k++) {
                a.add(fqn(split.get(k).result().id()));
                b.add(fqn(whole.get(k).result().id()));
            }
            if (!a.equals(b)) {
                String d = "raw ranks " + (i + 1) + "-" + (j + 1) + ": " + a + " vs " + b;
                return j == split.size() - 1 && split.size() == limit ? "tied at the cut, " + d : d;
            }
            i = j + 1;
        }
        return null;
    }

    static List<SearchResult> searchList(TestReaderNode node, String q, String project) throws Exception {
        return node.engine.searchWithTrace(new SearchRequest(q, SearchType.KEYWORD, null, List.of(project), 10, "text",
                null, null, 0), false).results();
    }

    /** Graph tools of a split project answer from its one project graph, as for the unsplit project. */
    @Test
    void graphToolsOfASplitProjectAnswerFromTheProjectGraph() throws Exception {
        String callee = null;
        List<SearchResult> wholeCallers = List.of();
        for (String q : ParityCorpus.PHAROS_QUERIES) {
            for (SearchResult x : search(r, q, WHOLE, ShardFilter.NONE).results()) {
                if (!"method".equals(x.docType())) continue;
                List<SearchResult> callers = r.engine.findCallers(fqn(x.id()), WHOLE, new ArrayList<>());
                if (callers.size() >= 2) {
                    callee = fqn(x.id());
                    wholeCallers = callers;
                    break;
                }
            }
            if (callee != null) break;
        }
        assertThat(callee).as("a method with callers in the corpus").isNotNull();
        r.drainCalls();
        List<String> w = new ArrayList<>();
        List<SearchResult> splitCallers = c.engine.findCallers(callee, SPLIT, w);
        assertThat(compareBitwise(splitCallers.subList(0, Math.min(10, splitCallers.size())),
                wholeCallers.subList(0, Math.min(10, wholeCallers.size())))).isNull();
        SearchEngine.UsageResult splitUsages = c.engine.findUsages(callee, "all", SPLIT, w);
        SearchEngine.UsageResult wholeUsages = r.engine.findUsages(callee, "all", WHOLE, new ArrayList<>());
        assertThat(splitUsages).isEqualTo(wholeUsages);
        assertThat(splitUsages.callers()).as("no duplicates from asking every shard").doesNotHaveDuplicates();
        String caller = fqn(splitCallers.getFirst().id());
        assertThat(c.engine.findCallees(caller, SPLIT, w).stream().map(x -> fqn(x.id())).toList())
                .isEqualTo(r.engine.findCallees(caller, WHOLE, new ArrayList<>()).stream().map(x -> fqn(x.id())).toList());
        assertThat(w).isEmpty();
        List<TestReaderNode.Call> usageCalls = r.drainCalls().stream()
                .filter(x -> x.path().endsWith("/graph") && "usages".equals(x.requestJson().path("op").asText())).toList();
        assertThat(usageCalls).as("graph-file lookups ask one shard of the project").hasSize(1);
    }

    /** MCP {@code list_projects} on a reader lists projects: a split project once, with its shards. */
    @Test
    void listProjectsShowsProjectsNotShardIds() throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper m = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode out = m.readTree(r.mcp().call("list_projects", m.createObjectNode()));
        List<String> names = new ArrayList<>();
        out.path("projects").forEach(p -> names.add(p.path("name").asText()));
        assertThat(names).contains(SPLIT, MOD, WHOLE, MOD_WHOLE).doesNotHaveDuplicates()
                .noneMatch(n -> n.contains("~"));
        assertThat(out.path("count").asInt()).isEqualTo(names.size());
        for (com.fasterxml.jackson.databind.JsonNode p : out.path("projects")) {
            List<String> shards = new ArrayList<>();
            p.path("shards").forEach(x -> shards.add(x.asText()));
            switch (p.path("name").asText()) {
                case SPLIT -> assertThat(shards).containsExactlyInAnyOrderElementsOf(
                        SPLIT_SHARDS.stream().map(ShardId::value).toList());
                case MOD -> assertThat(shards).containsExactlyInAnyOrderElementsOf(
                        MOD_SHARDS.stream().map(ShardId::value).toList());
                default -> assertThat(p.has("shards")).as("a [project] project keeps its shape").isFalse();
            }
            assertThat(p.path("methodCount").asInt()).as(p.path("name").asText()).isPositive();
        }
    }

    /** The gate's second half: a key-filtered query touches only the matching shards (counted at the reader). */
    @Test
    void moduleFilterTouchesOnlyMatchingShards() throws Exception {
        r.drainCalls();
        SearchResponse filtered = search(c, "widget helper", MOD, ShardFilter.module("core"));
        assertThat(filtered.results()).isNotEmpty()
                .allSatisfy(x -> assertThat(x.filePath()).contains("/core/"));
        List<TestReaderNode.Call> calls = r.drainCalls();
        // P6-R13: the search round touches only the matching shard; the statistics round asks them all
        assertThat(shardsCalled(calls, "/internal/shard-search")).containsExactly(MOD_CORE.value());
        assertThat(calls).filteredOn(x -> !x.path().endsWith("/internal/term-stats"))
                .allSatisfy(x -> assertThat(x.shard()).as(x.path()).isEqualTo(MOD_CORE.value()));
        assertThat(shardsCalled(calls, "/internal/term-stats"))
                .containsExactlyInAnyOrderElementsOf(MOD_SHARDS.stream().map(ShardId::value).toList());
        assertThat(filtered.trace().notes()).containsExactly(
                "project " + MOD + ": module 'core' selects 1 of 3 shards (" + MOD_CORE.value() + ")",
                "project " + MOD + ": stats: 3 shards, search: 1 of 3");

        SearchResponse all = search(c, "widget helper", MOD, ShardFilter.NONE);
        assertThat(shardsCalled(r.drainCalls(), "/internal/shard-search"))
                .containsExactlyInAnyOrderElementsOf(MOD_SHARDS.stream().map(ShardId::value).toList());
        assertThat(all.results().size()).isGreaterThan(filtered.results().size());
    }

    static Set<String> shardsCalled(List<TestReaderNode.Call> calls, String path) {
        return calls.stream().filter(x -> x.path().endsWith(path)).map(TestReaderNode.Call::shard)
                .collect(Collectors.toSet());
    }

    /** Review Focus 4: a path filter can't prune a ring: every shard of the project is asked, and the trace says why. */
    @Test
    void unprunableFilterFansOutAndTraces() throws Exception {
        r.drainCalls();
        SearchResponse resp = search(c, "search engine", SPLIT, ShardFilter.pathPrefix("src/main/java"));
        assertThat(resp.results()).isNotEmpty();
        assertThat(r.drainCalls().stream().map(TestReaderNode.Call::shard).collect(Collectors.toSet()))
                .containsExactlyInAnyOrderElementsOf(SPLIT_SHARDS.stream().map(ShardId::value).toList());
        assertThat(resp.trace().notes()).singleElement().asString().contains("project " + SPLIT,
                "path prefix 'src/main/java' cannot prune a ring", "fans out to all 3 shards");
        assertThat(resp.trace().format()).contains("routing: project " + SPLIT);
        // the documents are filtered on every shard: only files below the prefix
        assertThat(resp.results()).allSatisfy(x -> assertThat(x.filePath()).contains("/src/main/java/"));
    }

    static final String MOD_WHOLE = "modwhole";

    /**
     * P6-R12 and P6-R13: key filters are non-scoring. A filtered query scores each document exactly
     * (bitwise) as the unfiltered query does, and only drops the documents that don't match: for a
     * [project] project, for a split project whose spec can't prune by the filter, and for a split project
     * the filter prunes (every shard still answers the statistics round). (modproj has no calls: the
     * graph boost, relative to each list's largest in-degree, can't differ between the two lists.)
     */
    @Test
    void keyFiltersSelectDocumentsWithoutChangingScores() throws Exception {
        for (String[] pq : new String[][]{{MOD_WHOLE, "module", "core"}, {MOD, "language", "java"},
                {MOD, "module", "core"}, {MOD, "module", "web"},
                {MOD_WHOLE, "pathPrefix", "web/src"}, {MOD_WHOLE, "kind", "code"}}) {
            ShardFilter f = switch (pq[1]) {
                case "module" -> ShardFilter.module(pq[2]);
                case "language" -> new ShardFilter(null, null, pq[2], null);
                case "kind" -> new ShardFilter(null, null, null, pq[2]);
                default -> ShardFilter.pathPrefix(pq[2]);
            };
            for (TestReaderNode n : List.of(c, r)) {
                List<SearchResult> filtered = n.engine.searchWithTrace(new SearchRequest("widget helper tool",
                        SearchType.KEYWORD, pq[0], null, 50, "text", null, null, 0), false, f).results();
                List<SearchResult> all = n.engine.searchWithTrace(new SearchRequest("widget helper tool",
                        SearchType.KEYWORD, pq[0], null, 50, "text", null, null, 0), false).results();
                assertThat(filtered).as(n.id + " " + pq[0] + " " + pq[1]).isNotEmpty();
                java.util.Map<String, Float> scores = new java.util.HashMap<>();
                for (SearchResult x : all) scores.put(x.id(), x.score());
                for (SearchResult x : filtered) {
                    assertThat(scores).as(x.id()).containsKey(x.id());
                    assertThat(bits(x.score())).as(n.id + " " + x.id()).isEqualTo(bits(scores.get(x.id())));
                }
                switch (pq[1]) {
                    case "module" -> assertThat(filtered).allSatisfy(x -> assertThat(x.filePath()).contains("/" + pq[2] + "/"));
                    case "pathPrefix" -> assertThat(filtered).allSatisfy(x -> assertThat(x.filePath()).contains("/web/src/"));
                    default -> { }
                }
                if (pq[1].equals("module") || pq[1].equals("pathPrefix")) {
                    assertThat(filtered.size()).isLessThan(all.size());
                }
            }
        }
        // whole path segments only: "web/s" is not a directory of any file
        assertThat(search(r, "widget", MOD_WHOLE, ShardFilter.pathPrefix("web/s")).results()).isEmpty();
        // the module filter is the module's directory, not its artifactId; normalised like routing
        assertThat(search(r, "widget", MOD_WHOLE, ShardFilter.module("./core/")).results()).isNotEmpty()
                .allSatisfy(x -> assertThat(x.filePath()).contains("/core/"));
    }

    /** P6-R12: a filter that selects no shard of a split project says so in the response's warnings. */
    @Test
    void aFilterMatchingNoShardWarns() throws Exception {
        r.drainCalls();
        SearchResponse resp = search(c, "widget", MOD, ShardFilter.module("nowhere"));
        assertThat(r.drainCalls()).as("neither round runs").isEmpty();
        assertThat(resp.results()).isEmpty();
        assertThat(resp.warnings()).anySatisfy(w -> assertThat(w).contains("project " + MOD,
                "the key filter matches none of its 3 shards"));
    }

    /**
     * Two readers each pinned to one shard of a split project, the coordinator holding neither: a file
     * moves between the shards and only one reader has synced the new run. The query refuses to mix
     * runs: it reads the older run whole from the readers' retained generations (P6-R19), and without them
     * leaves the lagging shard out with a warning, never the moved file twice; once a reader
     * serves the lagging shard at the new run, the query is moved to it and the file is there once.
     */
    @Test
    void aQueryNeverMixesTwoIndexRunsAcrossReaders() throws Exception {
        Path tmp = Files.createTempDirectory("split-runs");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String project = "mover";
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        ShardId s01 = new ShardId(project + "~ring=s01"), s02 = new ShardId(project + "~ring=s02");
        List<ShardId> shards = List.of(s01, s02);
        String from = null, to = null;
        for (int i = 0; i < 200 && to == null; i++) {
            String a = "src/main/java/com/example/a" + i + "/Zebrafinch.java";
            for (int j = 0; j < 200 && to == null; j++) {
                String b = "src/main/java/com/example/b" + j + "/Zebrafinch.java";
                if (ring2.assign(DocKeys.of(project, a)).equals(s01) && ring2.assign(DocKeys.of(project, b)).equals(s02)) {
                    from = a;
                    to = b;
                }
            }
        }
        assertThat(to).isNotNull();
        Path src = tmp.resolve("src");
        for (int i = 0; i < 8; i++) writeClass(src, "src/main/java/com/example/p/Plain" + i + ".java", "com.example.p",
                "Plain" + i, "plain words " + i);
        String fromPkg = "com.example." + Path.of(from).getParent().getFileName();
        writeClass(src, from, fromPkg, "Zebrafinch", "zebrafinch song");
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, null)) {
            fx.indexSplit(src, project, ring2, shards, false);
            fx.index(JAVA, false);
            try (TestReaderNode x = new TestReaderNode(tmp.resolve("x"), store, BUCKET, "x", List.of(s01.value()), EMBEDDER, null);
                 TestReaderNode y = new TestReaderNode(tmp.resolve("y"), store, BUCKET, "y", List.of(s02.value()), EMBEDDER, null);
                 TestReaderNode coord = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(JAVA), EMBEDDER, null)) {
                coord.awaitShard(s01.value(), 1);
                coord.awaitShard(s02.value(), 1);
                assertThat(zebrafinch(coord, project)).as("run 1: once, from s01").isEqualTo(List.of(project));

                // run 2 moves the file into s02; only x (s01) syncs it
                Path moved = src.resolve(to);
                Files.createDirectories(moved.getParent());
                Files.writeString(moved, Files.readString(src.resolve(from)).replace(fromPkg,
                        "com.example." + moved.getParent().getFileName()));
                Files.delete(src.resolve(from));
                Files.setLastModifiedTime(moved, FileTime.fromMillis(System.currentTimeMillis() + 5000));
                fx.indexSplit(src, project, ring2, shards, true);
                x.parts.runtime().syncNow();

                // P6-R19: x still answers s01 at run 1 (its retained generation), so the query reads run 1 whole
                SearchResponse mixed = searchKeyword(coord, "zebrafinch", project);
                assertThat(mixed.results().stream().filter(res -> res.id().contains("Zebrafinch#")).count())
                        .as("never twice: read at run 1, where s01 has it").isEqualTo(1);
                assertThat(mixed.warnings()).isEmpty();

                // without the retained generation (x's grace over), s02 at run 1 is left out, never mixed
                ((com.pharos.cluster.ReaderLocalCache) x.parts.lucene().localCache()).setClock(
                        () -> System.currentTimeMillis() + 3_600_000L);
                SearchResponse left = searchKeyword(coord, "zebrafinch", project);
                assertThat(left.results()).as("never twice: s01 at run 2 lacks it, s02 at run 1 is left out").isEmpty();
                assertThat(left.warnings()).anySatisfy(w -> assertThat(w).contains("shard " + s02.value() + " unavailable",
                        "index run 2 of project " + project));

                // a reader holding the whole project at run 2: the query moves s02 to it
                try (TestReaderNode z = new TestReaderNode(tmp.resolve("z"), store, BUCKET, "z",
                        List.of(ShardId.wildcardOf(project)), EMBEDDER, null)) {
                    coord.awaitShard(s02.value(), 2);
                    assertThat(z.parts.runtime().served()).containsOnlyKeys(s01, s02);
                    for (int i = 0; i < 4; i++) { // whichever reader the router tries first for s02
                        SearchResponse fixed = searchKeyword(coord, "zebrafinch", project);
                        assertThat(fixed.results().stream().filter(res -> res.id().contains("Zebrafinch#")).count())
                                .as("the moved method, once").isEqualTo(1);
                        assertThat(fixed.warnings()).isEmpty();
                    }
                }
            }
        } finally {
            deleteTree(tmp);
        }
    }

    /**
     * I1: the coordinator holds a shard of a split project itself, but its reader lags the run the query
     * reads the project from (another reader is at the newer run). The shard is served by that replica
     * for the rest of the query: complete results, identical to the replica's own, and no warning.
     */
    @Test
    void aLaggingLocalShardIsServedByAReplicaAtTheQueryRun() throws Exception {
        Path tmp = Files.createTempDirectory("split-lag");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String project = "lagger";
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        ShardId s01 = new ShardId(project + "~ring=s01"), s02 = new ShardId(project + "~ring=s02");
        List<ShardId> shards = List.of(s01, s02);
        Path src = tmp.resolve("src");
        for (int i = 0; i < 10; i++) writeClass(src, "src/main/java/com/example/p" + i + "/Plain" + i + ".java",
                "com.example.p" + i, "Plain" + i, "plain words " + i);
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, null)) {
            fx.indexSplit(src, project, ring2, shards, false);
            fx.index(JAVA, false);
            try (TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(s01.value(), JAVA),
                    EMBEDDER, null)) {
                k.awaitShard(s02.value(), 0);
                // run 2 (every shard republished); only the replica that starts after it has it
                writeClass(src, "src/main/java/com/example/p0/Plain0.java", "com.example.p0", "Plain0", "plain words changed");
                Files.setLastModifiedTime(src.resolve("src/main/java/com/example/p0/Plain0.java"),
                        FileTime.fromMillis(System.currentTimeMillis() + 5000));
                fx.indexSplit(src, project, ring2, shards, true);
                try (TestReaderNode z = new TestReaderNode(tmp.resolve("z"), store, BUCKET, "z",
                        List.of(ShardId.wildcardOf(project)), EMBEDDER, null)) {
                    k.awaitShard(s01.value(), 2);
                    k.awaitShard(s02.value(), 1);
                    assertThat(k.parts.runtime().servesShard(s01.value())).as("k holds s01 itself (run 1)").isTrue();
                    SearchResponse reference = searchKeyword(z, "plain words", project);
                    assertThat(reference.results()).hasSize(10);
                    z.drainCalls();
                    SearchResponse fromK = searchKeyword(k, "plain words", project);
                    assertThat(fromK.warnings()).isEmpty();
                    assertThat(DistributedSearchTest.bits(fromK.results()))
                            .isEqualTo(DistributedSearchTest.bits(reference.results()));
                    assertThat(z.drainCalls()).as("z served both shards, s01 included")
                            .anySatisfy(x -> assertThat(x.shard()).isEqualTo(s01.value()))
                            .anySatisfy(x -> assertThat(x.shard()).isEqualTo(s02.value()));
                }
            }
        } finally {
            deleteTree(tmp);
        }
    }

    private static SearchResponse searchKeyword(TestReaderNode n, String q, String project) throws Exception {
        return n.engine.searchWithTrace(new SearchRequest(q, SearchType.KEYWORD, project, null, 10, "text", null,
                null, 0), false);
    }

    private static List<String> zebrafinch(TestReaderNode n, String project) throws Exception {
        return searchKeyword(n, "zebrafinch", project).results().stream()
                .filter(res -> res.id().contains("Zebrafinch#")).map(SearchResult::project).toList();
    }

}
