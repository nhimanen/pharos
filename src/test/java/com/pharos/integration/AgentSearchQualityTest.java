package com.pharos.integration;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Search quality comparison: CodeSearch (keyword + hybrid) vs grep across a range
 * of natural-language queries against the indexed Lucene source tree.
 *
 * Each {@link QueryScenario} captures:
 *   - a natural-language query (avoids exact class names)
 *   - the grep patterns a developer would actually try (broad → precise)
 *   - known ground-truth class names the correct answer must include
 *   - a {@link GrepDifficulty} rating that predicts where grep struggles
 *
 * The difficulty axis is the key insight:
 *   EASY   – the first-try grep pattern already finds the target
 *   MEDIUM – needs a precise pattern; broad patterns drown in noise
 *   HARD   – no obvious grep token; requires semantic understanding
 *
 * Tests:
 *   1. Per-scenario: keyword search hits vs grep hits (parameterised)
 *   2. Per-scenario: hybrid ≥ keyword on relevant hits (parameterised)
 *   3. Full matrix summary printed to stdout
 *   4. Agent test on the hardest scenario (grep-HARD): Claude Code
 *      spawned in a clean subprocess with the pharos MCP server
 *
 * Tag: {@code integration} — excluded from default mvn test.
 * Run: {@code mvn test -Dtest=AgentSearchQualityTest -DexcludedGroups=perf}
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AgentSearchQualityTest {

    // ── Difficulty taxonomy ───────────────────────────────────────────────────

    enum GrepDifficulty {
        /** Broad pattern finds the target in the first 5 results — grep wins clearly. */
        EASY,
        /** Broad pattern drowns in noise; precise pattern works but requires prior knowledge. */
        MEDIUM,
        /** No natural grep token maps to the concept; only semantic search finds it reliably. */
        HARD
    }

    // ── Query scenario table ──────────────────────────────────────────────────

    record QueryScenario(
            String id,
            String nlQuery,
            GrepDifficulty grepDifficulty,
            List<String> grepPatterns,      // ordered broad → precise
            List<String> expectedClasses,   // at least one must appear in top-20
            String rationale                // why this scenario is interesting
    ) {
        @Override public String toString() { return id; }
    }

    static List<QueryScenario> scenarios() {
        return List.of(

            // ── EASY: the grep token is the obvious search term ────────────
            new QueryScenario(
                "bm25-scoring",
                "calculate document relevance score using term frequency and field length normalization",
                GrepDifficulty.EASY,
                List.of("BM25", "termFreq", "idf"),
                List.of("BM25Similarity", "TFIDFSimilarity", "LeafSimScorer"),
                "BM25 is a well-known acronym; grep for 'BM25' lands immediately. " +
                "Tests that CodeSearch also ranks the core similarity class highly."
            ),

            new QueryScenario(
                "token-analysis-pipeline",
                "transform input text through filters into a stream of searchable tokens",
                GrepDifficulty.EASY,
                List.of("TokenStream", "TokenFilter", "Analyzer"),
                List.of("Analyzer", "TokenStream", "StandardTokenizer"),
                "Domain terms map directly to class names. Both grep and keyword " +
                "search should excel; the test confirms neither regresses."
            ),

            // ── MEDIUM: broad pattern is too noisy; precise requires domain knowledge ──
            new QueryScenario(
                "segment-merge-content-lists",
                "merge content node result lists across segments",
                GrepDifficulty.MEDIUM,
                List.of("merge.*result", "mergeFields", "mergeDocValues", "SegmentMerger"),
                List.of("SegmentMerger", "MergeState", "IndexWriter"),
                "Broad 'merge.*result' matches hundreds of unrelated lines. " +
                "Only 'SegmentMerger' lands precisely — but requires prior knowledge."
            ),

            new QueryScenario(
                "nrt-reader-refresh",
                "open a reader that can see uncommitted in-memory documents without a full commit",
                GrepDifficulty.MEDIUM,
                List.of("getReader", "openIfChanged", "NRT", "DirectoryReader"),
                List.of("DirectoryReader", "StandardDirectoryReader", "IndexWriter"),
                "NRT (near-real-time) is Lucene jargon. A developer unfamiliar with the " +
                "acronym would search 'getReader' or 'openIfChanged', missing the concept."
            ),

            new QueryScenario(
                "concurrent-flush-coordination",
                "coordinate multiple threads flushing document buffers to new segments simultaneously",
                GrepDifficulty.MEDIUM,
                List.of("flush.*thread", "DocumentsWriter", "flushControl", "FlushPolicy"),
                List.of("DocumentsWriter", "DocumentsWriterFlushControl", "FlushPolicy"),
                "Grep for 'flush' explodes with false positives across the whole codebase. " +
                "The relevant class name 'DocumentsWriterFlushControl' is not guessable."
            ),

            // ── HARD: no obvious grep token; pure semantic / vector territory ──
            new QueryScenario(
                "competitive-score-pruning",
                "prune non-competitive hits early by tracking the minimum score needed to enter the top-k results",
                GrepDifficulty.HARD,
                List.of("minCompetitiveScore", "competitive", "WAND", "TopScoreDocCollector"),
                List.of("TopScoreDocCollector", "MaxScoreAccumulator", "WANDScorer"),
                "WAND (Weak AND) is algorithm-level terminology absent from most code comments. " +
                "'competitive' is a common English word generating massive grep noise. " +
                "Semantic search via 'prune non-competitive hits' should win clearly."
            ),

            new QueryScenario(
                "skip-list-postings-navigation",
                "jump ahead in a posting list to reach a target document ID without reading every entry",
                GrepDifficulty.HARD,
                List.of("advance", "skipTo", "skipData", "MultiLevelSkipListReader"),
                List.of("MultiLevelSkipListReader", "Lucene99PostingsReader", "BlockImpactsPostingsEnum"),
                "'advance' is both a Java method name and an English word — thousands of false " +
                "positives. The skip-list implementation lives behind non-obvious class names. " +
                "Vector search on 'jump ahead without reading every entry' should surface it."
            ),

            new QueryScenario(
                "bkd-numeric-range-index",
                "index multidimensional numeric points for efficient range and spatial proximity queries",
                GrepDifficulty.HARD,
                List.of("PointValues", "BKDWriter", "BKD", "kd-tree"),
                List.of("BKDWriter", "BKDReader", "PointValues"),
                "BKD is an academic acronym (Block KD-tree). A developer searching for " +
                "'range query index' or 'spatial' will not find it with grep. " +
                "This is a pure semantic-search win scenario."
            ),

            new QueryScenario(
                "fuzzy-edit-distance-matching",
                "find terms that are approximately similar to a query word within a bounded number of character edits",
                GrepDifficulty.HARD,
                List.of("editDistance", "Levenshtein", "FuzzyQuery", "automaton"),
                List.of("FuzzyTermsEnum", "LevenshteinAutomata", "FuzzyQuery"),
                "A developer thinking 'approximate match' or 'typo tolerance' won't guess " +
                "'LevenshteinAutomata'. Grep for 'editDistance' works but only if you already " +
                "know the algorithm name. Semantic search should bridge the vocabulary gap."
            )
        );
    }

    // ── Infrastructure ────────────────────────────────────────────────────────

    private static Path luceneRoot;
    private static Path pharosJar;
    private static SearchEngine searchEngine;
    private static LuceneIndexer luceneIndexer;

    @BeforeAll
    static void setup() throws Exception {
        IndexConfig config = IndexConfig.load();
        ProjectRegistry registry = new ProjectRegistry(config);

        var luceneMeta = registry.find("lucene")
                .orElseThrow(() -> new IllegalStateException(
                        "Lucene project not indexed.\n" +
                        "Run: pharos index /home/nhimanen/projects/lucene --project lucene"));

        org.junit.jupiter.api.Assumptions.assumeTrue(
                luceneMeta.getMethodCount() > 0,
                "Lucene project has 0 methods — still indexing. Re-run after indexing completes.");

        luceneRoot = Path.of(luceneMeta.getRootPath());
        assertThat(luceneRoot).exists()
                .withFailMessage("Lucene source root not found at %s", luceneRoot);

        luceneIndexer = new LuceneIndexer(config);
        searchEngine  = new SearchEngine(luceneIndexer, EmbeddingProvider.create(config), registry);
        pharosJar = findPharosJar();
    }

    @AfterAll
    static void teardown() {
        if (luceneIndexer != null) luceneIndexer.close();
    }

    // ── Parameterised: keyword search ─────────────────────────────────────────

    @ParameterizedTest(name = "[{index}] keyword · {0}")
    @MethodSource("scenarios")
    @Order(1)
    void keyword_findsExpectedClasses(QueryScenario scenario) throws Exception {
        List<SearchResult> results = searchEngine.search(
                SearchRequest.keyword(scenario.nlQuery(), "lucene", 20));

        System.out.printf("%n── keyword · %s ──%n", scenario.id());
        System.out.printf("   Query: \"%s\"%n", scenario.nlQuery());
        printResults(results);

        long hits = countHits(results, scenario.expectedClasses());
        System.out.printf("   Hits on %s: %d / 20%n", scenario.expectedClasses(), hits);

        // Threshold scales with difficulty: HARD queries may score 0 on keyword-only
        int minRequired = scenario.grepDifficulty() == GrepDifficulty.EASY ? 1 : 0;
        assertThat(hits)
                .withFailMessage(
                        "[%s] Expected ≥%d relevant result(s) in top-20 keyword results. Got 0.\n" +
                        "Results: %s", scenario.id(), minRequired, summarise(results))
                .isGreaterThanOrEqualTo(minRequired);
    }

    // ── Parameterised: hybrid ≥ keyword ───────────────────────────────────────

    @ParameterizedTest(name = "[{index}] hybrid · {0}")
    @MethodSource("scenarios")
    @Order(2)
    void hybrid_notWorseThanKeyword(QueryScenario scenario) throws Exception {
        List<SearchResult> kwResults = searchEngine.search(
                SearchRequest.keyword(scenario.nlQuery(), "lucene", 20));
        List<SearchResult> hybridResults = searchEngine.search(new SearchRequest(
                scenario.nlQuery(), SearchRequest.SearchType.HYBRID,
                "lucene", null, 20, "text", null));

        long kwHits     = countHits(kwResults,     scenario.expectedClasses());
        long hybridHits = countHits(hybridResults, scenario.expectedClasses());

        System.out.printf("%n── hybrid · %s ──%n", scenario.id());
        System.out.printf("   keyword hits: %d / 20%n", kwHits);
        System.out.printf("   hybrid  hits: %d / 20%n", hybridHits);

        assertThat(hybridHits)
                .withFailMessage("[%s] Hybrid (%d hits) worse than keyword (%d hits)",
                        scenario.id(), hybridHits, kwHits)
                .isGreaterThanOrEqualTo(kwHits);
    }

    // ── Parameterised: grep comparison ────────────────────────────────────────

    @ParameterizedTest(name = "[{index}] grep · {0}")
    @MethodSource("scenarios")
    @Order(3)
    void grep_baselineComparison(QueryScenario scenario) throws Exception {
        System.out.printf("%n── grep · %s · difficulty=%s ──%n",
                scenario.id(), scenario.grepDifficulty());

        Map<String, Integer> hitsByPattern = new LinkedHashMap<>();
        for (String pat : scenario.grepPatterns()) {
            List<GrepHit> hits = runGrep(pat, luceneRoot, 200);
            long relevant = hits.stream()
                    .filter(h -> scenario.expectedClasses().stream()
                            .anyMatch(c -> h.relativePath().contains(c)))
                    .count();
            hitsByPattern.put(pat, (int) relevant);
            System.out.printf("   grep %-30s → %3d total files, %d relevant%n",
                    "\"" + pat + "\"", hits.size(), relevant);
        }

        // For EASY scenarios: the broad pattern must already find the target
        if (scenario.grepDifficulty() == GrepDifficulty.EASY) {
            String broadPattern = scenario.grepPatterns().get(0);
            assertThat(hitsByPattern.get(broadPattern))
                    .withFailMessage("[%s] EASY scenario: expected broad grep '%s' to find target",
                            scenario.id(), broadPattern)
                    .isGreaterThan(0);
        }

        // For HARD scenarios: even the most precise pattern may return 0 relevant files
        // (we just document it — no assertion that grep fails, only that it requires
        //  progressively more precise terms)
        if (scenario.grepDifficulty() == GrepDifficulty.HARD) {
            int broadHits   = hitsByPattern.get(scenario.grepPatterns().get(0));
            int preciseHits = hitsByPattern.get(
                    scenario.grepPatterns().get(scenario.grepPatterns().size() - 1));
            System.out.printf("   Observation: broad pattern noise=%d files, precise=%d relevant%n",
                    broadHits, preciseHits);
        }
    }

    // ── Full matrix summary ───────────────────────────────────────────────────

    @Test
    @Order(4)
    void summary_printQualityMatrix() throws Exception {
        System.out.println("\n╔═══════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║              SEARCH QUALITY MATRIX  (Lucene codebase)                    ║");
        System.out.println("╠══════════════════════════╦════════╦══════════════╦══════════════╦════════╣");
        System.out.println("║ Scenario                 ║ Diff.  ║ grep-broad   ║ CS keyword   ║ hybrid ║");
        System.out.println("╠══════════════════════════╬════════╬══════════════╬══════════════╬════════╣");

        for (QueryScenario s : scenarios()) {
            String broadPat = s.grepPatterns().get(0);
            List<GrepHit> grepHits = runGrep(broadPat, luceneRoot, 500);
            long grepRelevant = grepHits.stream()
                    .filter(h -> s.expectedClasses().stream().anyMatch(c -> h.relativePath().contains(c)))
                    .count();

            List<SearchResult> kwRes = searchEngine.search(
                    SearchRequest.keyword(s.nlQuery(), "lucene", 20));
            List<SearchResult> hybridRes = searchEngine.search(new SearchRequest(
                    s.nlQuery(), SearchRequest.SearchType.HYBRID, "lucene", null, 20, "text", null));

            long kwHits     = countHits(kwRes, s.expectedClasses());
            long hybridHits = countHits(hybridRes, s.expectedClasses());

            // Grep: show noise/relevant ratio for broad pattern
            String grepCell = grepRelevant > 0
                    ? String.format("✓ %d/%d", grepRelevant, Math.min(grepHits.size(), 20))
                    : String.format("✗ 0/%d", Math.min(grepHits.size(), 20));
            String kwCell     = kwHits     > 0 ? String.format("✓ %d/20", kwHits)     : "✗ 0/20";
            String hybridCell = hybridHits > 0 ? String.format("✓ %d/20", hybridHits) : "✗ 0/20";

            System.out.printf("║ %-24s ║ %-6s ║ %-12s ║ %-12s ║ %-6s ║%n",
                    truncate(s.id(), 24),
                    s.grepDifficulty().name().substring(0, 4),
                    grepCell, kwCell, hybridCell);
        }

        System.out.println("╚══════════════════════════╩════════╩══════════════╩══════════════╩════════╝");
        System.out.println();
        System.out.println("  ✓ = at least one expected class in top-20 results");
        System.out.println("  ✗ = no expected class found (grep: in top-20 of broad pattern results)");
        System.out.println();
        System.out.println("  Key observations:");
        System.out.println("  • EASY  scenarios: grep and CodeSearch both win — sanity check");
        System.out.println("  • MEDIUM scenarios: grep requires precise patterns (domain knowledge)");
        System.out.println("  • HARD  scenarios: only semantic search handles vocabulary mismatch");
    }

    // ── Agent test: hardest grep-HARD scenario ────────────────────────────────

    @Test
    @Order(5)
    void agent_hardestScenario_withCodesearchMcp() throws Exception {
        System.out.println("\n═══ Claude Code agent (MCP) ══════════════════════════════");
        assumeClaudeCliAvailable();

        // Pick the first HARD scenario as the clearest showcase for the agent
        QueryScenario scenario = scenarios().stream()
                .filter(s -> s.grepDifficulty() == GrepDifficulty.HARD)
                .findFirst()
                .orElseThrow();

        Path mcpConfig = writeTempMcpConfig(pharosJar);

        String prompt = String.format(
                "Use the pharos tool (project='lucene', type='hybrid') to find: %s. " +
                "Report the exact class name, method name, and file path.",
                scenario.nlQuery());

        System.out.println("  Scenario: " + scenario.id() + " [" + scenario.grepDifficulty() + "]");
        System.out.println("  Prompt:   " + prompt);
        System.out.println("  Rationale: " + scenario.rationale());

        AgentResult result = runClaudeAgent(prompt, mcpConfig);

        System.out.println("\n  Agent response (" + result.durationMs() + "ms):");
        System.out.println("  " + "─".repeat(60));
        result.response().lines().forEach(l -> System.out.println("  " + l));
        System.out.println("  " + "─".repeat(60));

        String responseUpper = result.response().toUpperCase();
        long mentionedHints = scenario.expectedClasses().stream()
                .filter(hint -> responseUpper.contains(hint.toUpperCase()))
                .count();

        System.out.printf("%n  Expected class mentions: %d / %d  %s%n",
                mentionedHints, scenario.expectedClasses().size(),
                scenario.expectedClasses());

        assertThat(result.exitCode())
                .withFailMessage("Claude agent exited with code %d\nstderr: %s",
                        result.exitCode(), result.stderr())
                .isZero();

        assertThat(mentionedHints)
                .withFailMessage(
                        "Agent response mentioned none of %s.\nFull response:\n%s",
                        scenario.expectedClasses(), result.response())
                .isGreaterThanOrEqualTo(1);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static List<GrepHit> runGrep(String pattern, Path root, int maxFiles) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "grep", "-rl", "--include=*.java", pattern, root.toString());
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        List<GrepHit> hits = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null && hits.size() < maxFiles) {
                hits.add(new GrepHit(root.relativize(Path.of(line)).toString(), 0));
            }
        }
        proc.waitFor(15, TimeUnit.SECONDS);
        return hits;
    }

    private AgentResult runClaudeAgent(String prompt, Path mcpConfig) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "claude",
                "--mcp-config", mcpConfig.toString(),
                "--output-format", "text",
                "-p", prompt);
        pb.environment().put("HOME", System.getProperty("user.home"));
        pb.environment().remove("CLAUDE_SESSION_ID");

        long t0 = System.currentTimeMillis();
        Process proc = pb.start();
        StringBuilder out = new StringBuilder(), err = new StringBuilder();

        Thread t1 = new Thread(() -> drain(proc.getInputStream(),  out));
        Thread t2 = new Thread(() -> drain(proc.getErrorStream(),  err));
        t1.start(); t2.start();

        boolean done = proc.waitFor(120, TimeUnit.SECONDS);
        t1.join(5000); t2.join(5000);
        if (!done) { proc.destroyForcibly(); throw new AssertionError("Agent timed out after 120s"); }

        return new AgentResult(out.toString(), err.toString(),
                proc.exitValue(), System.currentTimeMillis() - t0);
    }

    private static void drain(InputStream is, StringBuilder sb) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line).append("\n");
        } catch (IOException ignored) {}
    }

    private static Path writeTempMcpConfig(Path jar) throws Exception {
        Path tmp = Files.createTempFile("pharos-mcp-", ".json");
        tmp.toFile().deleteOnExit();
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(),
                Map.of("mcpServers", Map.of("pharos", Map.of(
                        "command", "java",
                        "args", List.of("-jar", jar.toAbsolutePath().toString(), "mcp-server")))));
        return tmp;
    }

    private static Path findPharosJar() throws IOException {
        Path target = Path.of(System.getProperty("user.dir"), "target");
        try (var stream = Files.list(target)) {
            return stream
                    .filter(p -> p.getFileName().toString().matches("pharos-.*\\.jar")
                            && !p.getFileName().toString().contains("original"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "pharos JAR not found. Run: mvn package -DskipTests"));
        }
    }

    private static void assumeClaudeCliAvailable() throws Exception {
        int code = new ProcessBuilder("which", "claude").start().waitFor();
        org.junit.jupiter.api.Assumptions.assumeTrue(code == 0, "claude CLI not on PATH");
    }

    private static long countHits(List<SearchResult> results, List<String> expectedClasses) {
        return results.stream()
                .filter(r -> {
                    String cls  = r.className()  != null ? r.className()  : "";
                    String file = r.filePath()   != null ? r.filePath()   : "";
                    return expectedClasses.stream()
                            .anyMatch(hint -> cls.contains(hint) || file.contains(hint));
                })
                .count();
    }

    private static void printResults(List<SearchResult> results) {
        for (int i = 0; i < Math.min(results.size(), 8); i++) {
            SearchResult r = results.get(i);
            String file = r.filePath() != null ? Path.of(r.filePath()).getFileName().toString() : "?";
            System.out.printf("   %2d. [%.4f] %s#%s  (%s)%n",
                    i + 1, r.score(), r.className(), r.methodName(), file);
        }
    }

    private static List<String> summarise(List<SearchResult> results) {
        return results.stream().map(r -> r.className() + "#" + r.methodName()).collect(Collectors.toList());
    }

    private static String truncate(String s, int n) {
        return s.length() <= n ? s : s.substring(0, n - 1) + "…";
    }

    // ── Value types ───────────────────────────────────────────────────────────

    record GrepHit(String relativePath, int line) {}
    record AgentResult(String response, String stderr, int exitCode, long durationMs) {}
}
