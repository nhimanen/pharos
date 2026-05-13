package com.pharos.quality;

import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end quality benchmark for Pharos keyword search.
 *
 * <p>Indexes a purpose-built corpus of 8 Java service classes (~38 methods)
 * and evaluates 20 labeled queries across three difficulty tiers:
 * <ul>
 *   <li>{@code name-lookup} — direct token/name match; establishes quality floor
 *   <li>{@code semantic} — vocabulary gap between developer phrasing and identifiers
 *   <li>{@code conceptual} — developer mental model differs from code structure
 * </ul>
 *
 * <h3>Metrics reported</h3>
 * <ul>
 *   <li><b>MRR</b> — Mean Reciprocal Rank: {@code 1/rank} of first relevant result
 *   <li><b>MAP</b> — Mean Average Precision: area under precision-recall curve
 *   <li><b>NDCG@5</b> — Normalized DCG with graded relevance in top-5
 *   <li><b>P@5</b> — Precision at 5: fraction of top-5 that are relevant
 *   <li><b>R@10</b> — Recall at 10: fraction of relevant items found in top-10
 * </ul>
 *
 * <h3>Regression gates</h3>
 * Assertions enforce minimum quality floors so regressions fail the build:
 * <ul>
 *   <li>name-lookup MRR &ge; 0.80 — direct matches must rank first
 *   <li>overall MRR &ge; 0.40 — at least one relevant result in top-3 on average
 *   <li>overall P@5 &ge; 0.15 — at least one relevant result in top-5 on average
 * </ul>
 *
 * <p>Tag: {@code quality} — excluded from default {@code mvn test}.
 * Run: {@code mvn test -Dtest=SearchQualityBenchmarkTest -DexcludedGroups=perf,integration}
 */
@Tag("quality")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SearchQualityBenchmarkTest {

    private static SearchQualityDataset.IndexedCorpus corpus;
    private static KeywordSearchStrategy strategy;

    @BeforeAll
    static void setup() throws Exception {
        corpus   = SearchQualityDataset.buildAndIndex();
        strategy = new KeywordSearchStrategy();
    }

    @AfterAll
    static void teardown() throws Exception {
        if (corpus != null) corpus.close();
    }

    // ── Per-query detail ──────────────────────────────────────────────────────

    /**
     * Prints a per-query metric table: MRR, P@5, R@10, NDCG@5, AP for each query.
     * No assertion — this is for human inspection and regression analysis.
     */
    @Test
    @Order(1)
    void benchmark_perQueryBreakdown() throws IOException {
        System.out.println();
        System.out.println(bar());
        System.out.printf("  PHAROS QUALITY BENCHMARK — per-query detail  " +
                          "(%d methods, %d classes)%n",
                corpus.methodCount(), corpus.classCount());
        System.out.println(bar());
        System.out.printf("  %-32s %-12s %6s %6s %6s %6s %6s%n",
                "Query ID", "Category", "MRR", "P@5", "R@10", "N@5", "AP");
        System.out.println("  " + "─".repeat(78));

        for (SearchQualityDataset.QueryCase qc : SearchQualityDataset.queryCases()) {
            List<SearchResult> results = searchMethods(qc.query());
            IrMetrics.RelevanceMap rel = qc.relevanceMap();

            double mrr  = IrMetrics.mrr(results, rel);
            double p5   = IrMetrics.precisionAt(results, 5, rel);
            double r10  = IrMetrics.recallAt(results, 10, rel);
            double ndcg = IrMetrics.ndcgAt(results, 5, rel);
            double ap   = IrMetrics.averagePrecision(results, rel);

            System.out.printf("  %-32s %-12s %6.3f %6.3f %6.3f %6.3f %6.3f%n",
                    qc.id(), qc.category(), mrr, p5, r10, ndcg, ap);
        }

        System.out.println(bar());
        System.out.println();
    }

    // ── Aggregate metrics by category ─────────────────────────────────────────

    /**
     * Prints aggregate metrics per category and overall.
     * No assertion — for human inspection and regression baselining.
     */
    @Test
    @Order(2)
    void benchmark_aggregateMetrics() throws IOException {
        List<SearchQualityDataset.QueryCase> allCases = SearchQualityDataset.queryCases();
        Map<String, List<SearchQualityDataset.QueryCase>> byCategory = allCases.stream()
                .collect(Collectors.groupingBy(SearchQualityDataset.QueryCase::category));

        System.out.println();
        System.out.println(bar());
        System.out.println("  PHAROS QUALITY BENCHMARK — aggregate metrics by category");
        System.out.println(bar());
        System.out.printf("  %-14s  %6s %6s %6s %6s %6s  %s%n",
                "Category", "MRR", "MAP", "NDCG@5", "P@5", "R@10", "Queries");
        System.out.println("  " + "─".repeat(66));

        for (String cat : List.of("name-lookup", "semantic", "conceptual")) {
            List<SearchQualityDataset.QueryCase> group = byCategory.getOrDefault(cat, List.of());
            if (group.isEmpty()) continue;
            double[] agg = computeAggregate(group);
            printAggRow(cat, group.size(), agg);
        }

        System.out.println("  " + "─".repeat(66));
        double[] overall = computeAggregate(allCases);
        printAggRow("OVERALL", allCases.size(), overall);
        System.out.println(bar());
        System.out.printf("  Strategy: keyword (BM25 + graph boost, field boosts 3/2/2/1.5/1/1/1.5)%n");
        System.out.println(bar());
        System.out.println();
    }

    // ── Top-result inspection ─────────────────────────────────────────────────

    /**
     * Prints the top-5 results for each query with relevance markers.
     * No assertion — for diagnosing which queries succeed or fail.
     */
    @Test
    @Order(3)
    void inspect_topResultsPerQuery() throws IOException {
        System.out.println();
        System.out.println("  TOP-5 RESULTS PER QUERY  (✓✓ = grade-2, ✓ = grade-1)");
        System.out.println("  " + "─".repeat(66));

        for (SearchQualityDataset.QueryCase qc : SearchQualityDataset.queryCases()) {
            List<SearchResult> results = searchMethods(qc.query());
            IrMetrics.RelevanceMap rel = qc.relevanceMap();

            System.out.printf("%n  [%s] %s%n  Q: \"%s\"%n",
                    qc.category().toUpperCase(), qc.id(), qc.query());

            int shown = Math.min(5, results.size());
            for (int i = 0; i < shown; i++) {
                SearchResult r = results.get(i);
                int grade = rel.grade(r);
                String mark = grade == 2 ? "✓✓" : grade == 1 ? " ✓" : "  ";
                System.out.printf("    %s [%d] %s#%s  (%.3f)%n",
                        mark, i + 1, r.className(), r.methodName(), r.score());
            }
        }
        System.out.println();
    }

    // ── Regression gates ──────────────────────────────────────────────────────

    /**
     * Name-lookup MRR must be &ge; 0.80.
     * Direct name/token matches should almost always rank first.
     */
    @Test
    @Order(4)
    void regression_nameLookup_mrrAboveFloor() throws IOException {
        List<SearchQualityDataset.QueryCase> nameLookup = SearchQualityDataset.queryCases().stream()
                .filter(qc -> "name-lookup".equals(qc.category()))
                .toList();

        double mrr = computeAggregate(nameLookup)[0];

        assertThat(mrr)
                .as("name-lookup MRR (direct name matches) must be >= 0.80; got %.3f".formatted(mrr))
                .isGreaterThanOrEqualTo(0.80);
    }

    /**
     * Overall MRR must be &ge; 0.40.
     * The first relevant result should appear in the top-3 on average.
     */
    @Test
    @Order(5)
    void regression_overall_mrrAboveFloor() throws IOException {
        double mrr = computeAggregate(SearchQualityDataset.queryCases())[0];

        assertThat(mrr)
                .as("Overall MRR must be >= 0.40; got %.3f".formatted(mrr))
                .isGreaterThanOrEqualTo(0.40);
    }

    /**
     * Overall P@5 must be &ge; 0.15.
     * At least one relevant result must appear in the top-5 on average.
     */
    @Test
    @Order(6)
    void regression_overall_precisionAt5AboveFloor() throws IOException {
        double p5 = computeAggregate(SearchQualityDataset.queryCases())[3]; // index 3

        assertThat(p5)
                .as("Overall P@5 must be >= 0.15; got %.3f".formatted(p5))
                .isGreaterThanOrEqualTo(0.15);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private List<SearchResult> searchMethods(String query) throws IOException {
        SearchRequest req = new SearchRequest(
                query, SearchRequest.SearchType.KEYWORD,
                SearchQualityDataset.PROJECT, null, 20, "text", "method", null);
        return strategy.search(corpus.reader(), req);
    }

    /**
     * Returns {@code [mrr, map, ndcg5, p5, r10]} aggregated across the given cases.
     */
    private double[] computeAggregate(
            List<SearchQualityDataset.QueryCase> cases) throws IOException {

        List<Double> mrrs  = new ArrayList<>();
        List<Double> aps   = new ArrayList<>();
        List<Double> ndcgs = new ArrayList<>();
        List<Double> p5s   = new ArrayList<>();
        List<Double> r10s  = new ArrayList<>();

        for (SearchQualityDataset.QueryCase qc : cases) {
            List<SearchResult> results = searchMethods(qc.query());
            IrMetrics.RelevanceMap rel = qc.relevanceMap();
            mrrs.add(IrMetrics.mrr(results, rel));
            aps.add(IrMetrics.averagePrecision(results, rel));
            ndcgs.add(IrMetrics.ndcgAt(results, 5, rel));
            p5s.add(IrMetrics.precisionAt(results, 5, rel));
            r10s.add(IrMetrics.recallAt(results, 10, rel));
        }

        return new double[]{
            IrMetrics.mean(mrrs),
            IrMetrics.mean(aps),
            IrMetrics.mean(ndcgs),
            IrMetrics.mean(p5s),
            IrMetrics.mean(r10s)
        };
    }

    private static void printAggRow(String label, int count, double[] agg) {
        // agg: [mrr, map, ndcg5, p5, r10]
        System.out.printf("  %-14s  %6.3f %6.3f %6.3f %6.3f %6.3f  %d%n",
                label, agg[0], agg[1], agg[2], agg[3], agg[4], count);
    }

    private static String bar() {
        return "━".repeat(70);
    }
}
