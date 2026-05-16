package com.pharos.quality;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parameter sensitivity ablation suite for Pharos keyword search.
 *
 * <p>Each test isolates one scoring component by comparing a baseline (component on)
 * against a configuration with that component disabled or varied:
 *
 * <ol>
 *   <li><b>Graph boost</b> — MRR delta when target methods have high in-degree vs. zero.
 *       Validates that the 30% graph boost actually helps when the call graph is populated.
 *
 *   <li><b>Field boost hierarchy</b> — weighted (3/2/2/1.5/1/1/1.5) vs. flat (all 1.0).
 *       Validates that boosting {@code methodName} and {@code javadoc} improves name-lookup.
 *
 *   <li><b>RRF k constant sweep</b> — k ∈ {10, 30, 60, 120} with a synthetic second list.
 *       Shows how the dampening constant affects score magnitude and rank stability.
 * </ol>
 *
 * <p>Directional assertions: each test asserts that the ablated configuration does not
 * significantly hurt the baseline. Exact magnitudes are corpus-dependent and not asserted.
 *
 * <p>Tag: {@code quality} — excluded from default {@code mvn test}.
 * Run: {@code mvn test -Dtest=SearchAblationTest -DexcludedGroups=perf,integration}
 */
@Tag("quality")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SearchAblationTest {

    private static final String[] SEARCH_FIELDS = {
        DocumentMapper.F_METHOD_NAME,
        DocumentMapper.F_JAVADOC,
        DocumentMapper.F_SIGNATURE,
        DocumentMapper.F_CLASS_NAME,
        DocumentMapper.F_BODY,
        DocumentMapper.F_ANNOTATIONS,
        DocumentMapper.F_CALLER_CONTEXT
    };

    // ── Ablation 1: Graph Boost ───────────────────────────────────────────────

    /**
     * Measures how much the in-degree graph boost contributes to MRR and NDCG@5.
     *
     * <p>Two corpora are built from the same source files:
     * <ul>
     *   <li><b>Baseline</b>: all inDegree = 0 (no graph boost applied — pure BM25)
     *   <li><b>Boosted</b>: the primary target method of each query gets inDegree = 15
     * </ul>
     *
     * <p>The boosted corpus simulates a well-connected production codebase where
     * high-value (frequently-called) methods have earned high in-degree through
     * real usage. If the graph boost is beneficial, MRR should improve.
     *
     * <p>Assertion: graph boost must not hurt MRR by more than 0.05 points.
     */
    @Test
    @Order(1)
    void ablation_graphBoost_mrrDelta() throws Exception {
        // Build in-degree override map: primary target of each query gets inDegree=15
        Map<String, Integer> boostMap = new HashMap<>();
        for (SearchQualityDataset.QueryCase qc : SearchQualityDataset.queryCases()) {
            qc.judgments().stream()
                    .filter(j -> j.grade() == 2)
                    .forEach(j -> boostMap.put(j.className() + "#" + j.methodName(), 15));
        }

        try (SearchQualityDataset.IndexedCorpus baseline = SearchQualityDataset.buildAndIndex();
             SearchQualityDataset.IndexedCorpus boosted  =
                     SearchQualityDataset.buildAndIndexWithInDegrees(boostMap)) {

            KeywordSearchStrategy strategy = new KeywordSearchStrategy();

            List<Double> baseMrrs   = new ArrayList<>();
            List<Double> boostMrrs  = new ArrayList<>();
            List<Double> baseNdcgs  = new ArrayList<>();
            List<Double> boostNdcgs = new ArrayList<>();

            for (SearchQualityDataset.QueryCase qc : SearchQualityDataset.queryCases()) {
                IrMetrics.RelevanceMap rel = qc.relevanceMap();
                SearchRequest req = methodRequest(qc.query());

                List<SearchResult> baseRes  = strategy.search(baseline.reader(), req);
                List<SearchResult> boostRes = strategy.search(boosted.reader(), req);

                baseMrrs.add(IrMetrics.mrr(baseRes, rel));
                boostMrrs.add(IrMetrics.mrr(boostRes, rel));
                baseNdcgs.add(IrMetrics.ndcgAt(baseRes, 5, rel));
                boostNdcgs.add(IrMetrics.ndcgAt(boostRes, 5, rel));
            }

            double baseMrr  = IrMetrics.mean(baseMrrs);
            double bstMrr   = IrMetrics.mean(boostMrrs);
            double baseNdcg = IrMetrics.mean(baseNdcgs);
            double bstNdcg  = IrMetrics.mean(boostNdcgs);

            ablationHeader("GRAPH BOOST ABLATION  (inDegree=0 vs. inDegree=15 for targets)");
            System.out.printf("  %-28s  MRR=%6.3f  NDCG@5=%6.3f%n",
                    "Baseline (no boost)", baseMrr, baseNdcg);
            System.out.printf("  %-28s  MRR=%6.3f  NDCG@5=%6.3f%n",
                    "Boosted (targets inDeg=15)", bstMrr, bstNdcg);
            System.out.printf("  %-28s  MRR=%+6.3f  NDCG@5=%+6.3f%n",
                    "Delta (boosted − baseline)", bstMrr - baseMrr, bstNdcg - baseNdcg);
            System.out.printf("%n  Graph boost weight: 0.3  (30%% lift at max inDegree)%n");
            System.out.printf("  Expected: positive or near-zero delta when targets have high inDegree%n");
            System.out.println();

            assertThat(bstMrr)
                    .as("Graph boost must not hurt MRR when target methods have high inDegree")
                    .isGreaterThanOrEqualTo(baseMrr - 0.05);
        }
    }

    // ── Ablation 2: Field Boost Hierarchy ─────────────────────────────────────

    /**
     * Compares Pharos weighted field boosts against uniform 1.0 weights.
     *
     * <p>Standard Pharos field boosts:
     * <pre>
     *   methodName × 3.0  javadoc × 2.0  signature × 2.0
     *   className × 1.5   callerContext × 1.5
     *   body × 1.0        annotations × 1.0
     * </pre>
     *
     * <p>Flat weights give equal importance to all fields. This should hurt
     * name-lookup queries (where the high methodName boost is critical) while
     * having less impact on semantic queries (which rely more on javadoc/body).
     *
     * <p>Assertion: weighted boosts must match or beat flat for name-lookup MRR.
     */
    @Test
    @Order(2)
    void ablation_fieldBoosts_weightedVsFlat() throws Exception {
        try (SearchQualityDataset.IndexedCorpus corpus = SearchQualityDataset.buildAndIndex()) {
            KeywordSearchStrategy weighted = new KeywordSearchStrategy();
            Analyzer analyzer = LuceneIndexer.buildAnalyzer();

            List<SearchQualityDataset.QueryCase> nameLookup = SearchQualityDataset.queryCases().stream()
                    .filter(qc -> "name-lookup".equals(qc.category()))
                    .toList();
            List<SearchQualityDataset.QueryCase> semantic = SearchQualityDataset.queryCases().stream()
                    .filter(qc -> "semantic".equals(qc.category()))
                    .toList();
            List<SearchQualityDataset.QueryCase> all = SearchQualityDataset.queryCases();

            double wNlMrr  = meanMrr(nameLookup, corpus.reader(), (qc) -> {
                try { return weighted.search(corpus.reader(), methodRequest(qc.query())); }
                catch (IOException e) { throw new RuntimeException(e); }
            });
            double wSmMrr  = meanMrr(semantic, corpus.reader(), (qc) -> {
                try { return weighted.search(corpus.reader(), methodRequest(qc.query())); }
                catch (IOException e) { throw new RuntimeException(e); }
            });
            double wAllMrr = meanMrr(all, corpus.reader(), (qc) -> {
                try { return weighted.search(corpus.reader(), methodRequest(qc.query())); }
                catch (IOException e) { throw new RuntimeException(e); }
            });

            double fNlMrr  = meanMrr(nameLookup, corpus.reader(), (qc) ->
                    flatSearch(corpus.reader(), analyzer, qc.query()));
            double fSmMrr  = meanMrr(semantic, corpus.reader(), (qc) ->
                    flatSearch(corpus.reader(), analyzer, qc.query()));
            double fAllMrr = meanMrr(all, corpus.reader(), (qc) ->
                    flatSearch(corpus.reader(), analyzer, qc.query()));

            ablationHeader("FIELD BOOST ABLATION  (weighted 3/2/2/1.5/1/1/1.5 vs. flat 1.0)");
            System.out.printf("  %-30s  MRR(name-lookup)=%6.3f  MRR(semantic)=%6.3f  MRR(all)=%6.3f%n",
                    "Weighted boosts", wNlMrr, wSmMrr, wAllMrr);
            System.out.printf("  %-30s  MRR(name-lookup)=%6.3f  MRR(semantic)=%6.3f  MRR(all)=%6.3f%n",
                    "Flat boosts (all 1.0)", fNlMrr, fSmMrr, fAllMrr);
            System.out.printf("  %-30s  MRR(name-lookup)=%+6.3f  MRR(semantic)=%+6.3f  MRR(all)=%+6.3f%n",
                    "Delta (weighted − flat)", wNlMrr - fNlMrr, wSmMrr - fSmMrr, wAllMrr - fAllMrr);
            System.out.printf("%n  Hypothesis: weighted > flat for name-lookup (methodName boost matters)%n");
            System.out.printf("  Hypothesis: weighted ≈ flat for semantic (javadoc/body carry signal)%n");
            System.out.println();

            assertThat(wNlMrr)
                    .as("Weighted field boosts must match or beat flat for name-lookup queries")
                    .isGreaterThanOrEqualTo(fNlMrr - 0.05);
        }
    }

    // ── Ablation 3: RRF k constant sweep ──────────────────────────────────────

    /**
     * Sweeps the RRF k constant (10, 30, 60, 120) using a self-fusion approach.
     *
     * <p>Since vector embeddings are unavailable in unit tests, this ablation
     * applies RRF to the keyword results and a version of the same list shifted
     * by 2 positions — simulating a secondary ranking signal with correlated but
     * slightly different ordering. This shows how k affects score magnitude and
     * whether rank order is stable across k values.
     *
     * <p>No assertion — this test prints a sweep table for manual review.
     */
    @Test
    @Order(3)
    void ablation_rrfK_sweep() throws Exception {
        int[] ks = {10, 30, 60, 120};

        try (SearchQualityDataset.IndexedCorpus corpus = SearchQualityDataset.buildAndIndex()) {
            KeywordSearchStrategy strategy = new KeywordSearchStrategy();

            ablationHeader("RRF k CONSTANT SWEEP  (keyword self-fusion, list shifted by 2 positions)");
            System.out.printf("  %-8s  %6s %6s %6s %8s%n",
                    "k", "MRR", "P@5", "MAP", "score@1");

            for (int k : ks) {
                List<Double> mrrs      = new ArrayList<>();
                List<Double> p5s       = new ArrayList<>();
                List<Double> aps       = new ArrayList<>();
                List<Double> topScores = new ArrayList<>();

                for (SearchQualityDataset.QueryCase qc : SearchQualityDataset.queryCases()) {
                    IrMetrics.RelevanceMap rel = qc.relevanceMap();
                    List<SearchResult> kw = strategy.search(corpus.reader(), methodRequest(qc.query()));

                    // Shifted list: same results but offset by 2 — simulates second signal
                    List<SearchResult> shifted = rotateList(kw, 2);
                    List<SearchResult> fused = applyRrf(kw, shifted, k, 20);

                    mrrs.add(IrMetrics.mrr(fused, rel));
                    p5s.add(IrMetrics.precisionAt(fused, 5, rel));
                    aps.add(IrMetrics.averagePrecision(fused, rel));
                    if (!fused.isEmpty()) topScores.add((double) fused.get(0).score());
                }

                System.out.printf("  k=%-6d  %6.3f %6.3f %6.3f %8.5f%n",
                        k,
                        IrMetrics.mean(mrrs),
                        IrMetrics.mean(p5s),
                        IrMetrics.mean(aps),
                        IrMetrics.mean(topScores));
            }

            System.out.printf("%n  Lower k → higher absolute scores, more rank volatility%n");
            System.out.printf("  Higher k → lower absolute scores, more stable rank order%n");
            System.out.printf("  Pharos default: k=60 (standard RRF constant)%n");
            System.out.println();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SearchRequest methodRequest(String query) {
        return new SearchRequest(
                query, SearchRequest.SearchType.KEYWORD,
                SearchQualityDataset.PROJECT, null, 20, "text", "method", null, 0);
    }

    @FunctionalInterface
    private interface QueryFn {
        List<SearchResult> search(SearchQualityDataset.QueryCase qc);
    }

    private static double meanMrr(
            List<SearchQualityDataset.QueryCase> cases,
            DirectoryReader reader,
            QueryFn searchFn) {
        List<Double> mrrs = new ArrayList<>();
        for (SearchQualityDataset.QueryCase qc : cases) {
            List<SearchResult> results = searchFn.search(qc);
            mrrs.add(IrMetrics.mrr(results, qc.relevanceMap()));
        }
        return IrMetrics.mean(mrrs);
    }

    /**
     * Keyword search with all field boosts set to 1.0 (no hierarchy).
     */
    private static List<SearchResult> flatSearch(
            DirectoryReader reader, Analyzer analyzer, String queryText) {
        Map<String, Float> flatBoosts = new HashMap<>();
        for (String f : SEARCH_FIELDS) flatBoosts.put(f, 1.0f);

        MultiFieldQueryParser parser = new MultiFieldQueryParser(SEARCH_FIELDS, analyzer, flatBoosts);
        parser.setDefaultOperator(org.apache.lucene.queryparser.classic.QueryParser.Operator.OR);

        Query query;
        try {
            query = parser.parse(MultiFieldQueryParser.escape(queryText));
        } catch (ParseException e) {
            return List.of();
        }

        BooleanQuery filtered = new BooleanQuery.Builder()
                .add(query, BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(DocumentMapper.F_PROJECT, SearchQualityDataset.PROJECT)),
                        BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "method")),
                        BooleanClause.Occur.FILTER)
                .build();

        try {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(new BM25Similarity());
            TopDocs hits = searcher.search(filtered, 20);
            return KeywordSearchStrategy.toResults(searcher, hits, "flat");
        } catch (IOException e) {
            throw new RuntimeException("flat search failed", e);
        }
    }

    /**
     * Applies Reciprocal Rank Fusion with the given k constant.
     * RRF formula: score(doc) = Σ 1/(k + rank_in_list).
     */
    private static List<SearchResult> applyRrf(
            List<SearchResult> list1, List<SearchResult> list2, int k, int limit) {

        Map<String, Double>       scores = new LinkedHashMap<>();
        Map<String, SearchResult> byId   = new HashMap<>();

        for (int i = 0; i < list1.size(); i++) {
            SearchResult r = list1.get(i);
            scores.merge(r.id(), 1.0 / (k + i + 1), Double::sum);
            byId.put(r.id(), r);
        }
        for (int i = 0; i < list2.size(); i++) {
            SearchResult r = list2.get(i);
            scores.merge(r.id(), 1.0 / (k + i + 1), Double::sum);
            byId.putIfAbsent(r.id(), r);
        }

        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(limit)
                .map(e -> {
                    SearchResult r = byId.get(e.getKey());
                    return new SearchResult(
                            r.id(), r.project(), r.packageName(), r.className(),
                            r.qualifiedClassName(), r.methodName(), r.signature(),
                            r.returnType(), r.body(), r.javadoc(), r.accessModifier(),
                            r.filePath(), r.startLine(), r.endLine(),
                            e.getValue().floatValue(), "rrf", r.docType());
                })
                .toList();
    }

    /**
     * Rotates list elements forward by {@code n} positions (wraps around).
     * Used to produce a synthetic second ranking signal for RRF sweep.
     */
    private static List<SearchResult> rotateList(List<SearchResult> list, int n) {
        if (list.isEmpty()) return list;
        int sz = list.size();
        List<SearchResult> rotated = new ArrayList<>(sz);
        for (int i = 0; i < sz; i++) {
            rotated.add(list.get((i + n) % sz));
        }
        return rotated;
    }

    private static void ablationHeader(String title) {
        System.out.println();
        System.out.println("━".repeat(70));
        System.out.printf("  %s%n", title);
        System.out.println("━".repeat(70));
    }
}
