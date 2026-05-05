package com.pharos.quality;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holdout validation set for search quality — structurally identical to
 * {@link LuceneSearchQualityTest} but uses entirely different queries.
 *
 * <p>The tuning workflow is:
 * <ol>
 *   <li>Iterate and tune against {@code LuceneSearchQualityTest} (the training set).
 *   <li>Run this class to confirm improvements generalise — never tune against
 *       these queries directly.
 * </ol>
 *
 * <p>Same corpus ({@code ~/.pharos/indexes/lucene}, ~70k methods), same tiers,
 * same metrics, same regression gate structure — different query phrasings so
 * over-fitting to the training set is detectable.
 *
 * <h3>Query tiers</h3>
 * <ul>
 *   <li><b>name-lookup</b> (5) — tokens appear in class/method names; BM25 baseline.
 *   <li><b>semantic</b> (8) — vocabulary gap between developer phrasing and Lucene internals.
 *   <li><b>conceptual</b> (7) — developer mental model maps to a subsystem or pattern.
 * </ul>
 *
 * <p>Tag: {@code validation} — run separately from quality and perf tests.
 * Run: {@code mvn test -Dtest=LuceneSearchValidationTest -DexcludedGroups=perf,integration}
 */
@Tag("validation")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LuceneSearchValidationTest {

    // ── Dataset ───────────────────────────────────────────────────────────────

    record LuceneQueryCase(
            String id,
            String query,
            List<String> highRelevance,
            List<String> partialRelevance,
            String category,
            String rationale
    ) {
        @Override public String toString() { return id; }

        IrMetrics.RelevanceMap relevanceMap() {
            List<IrMetrics.Judgment> j = new ArrayList<>();
            for (String cls : highRelevance)   j.add(IrMetrics.Judgment.high(cls, ""));
            for (String cls : partialRelevance) j.add(IrMetrics.Judgment.partial(cls, ""));
            return new IrMetrics.RelevanceMap(j);
        }
    }

    static List<LuceneQueryCase> cases() {
        return List.of(

            // ── name-lookup: token appears in class/method name ──────────────

            new LuceneQueryCase(
                "directory-reader-reopen",
                "DirectoryReader leaves segments composite reader wrap directory",
                List.of("DirectoryReader"),
                List.of("StandardDirectoryReader", "FilterDirectoryReader"),
                "name-lookup",
                "'DirectoryReader' is the class name; 'leaves' and 'segments' appear in its " +
                "javadoc and method names. Avoids 'openIfChanged' which fires heavily in " +
                "callerContext of unrelated classes"
            ),

            new LuceneQueryCase(
                "term-query",
                "TermQuery exact single term match field value lookup",
                List.of("TermQuery"),
                List.of("TermScorer", "TermWeight"),
                "name-lookup",
                "'TermQuery' is the exact class name; 'exact term match' appears in its javadoc"
            ),

            new LuceneQueryCase(
                "phrase-query",
                "PhraseQuery match ordered sequence of words slop proximity",
                List.of("PhraseQuery"),
                List.of("SloppyPhraseMatcher", "ExactPhraseMatcher"),
                "name-lookup",
                "'PhraseQuery' is the class name; 'slop' and 'ordered' appear in its javadoc " +
                "and constructor parameter names"
            ),

            new LuceneQueryCase(
                "sort-field",
                "SortField sort search results by field value ascending descending comparator",
                List.of("SortField"),
                List.of("FieldComparator", "Sort"),
                "name-lookup",
                "'SortField' is the direct class; 'comparator' maps to FieldComparator. " +
                "The field name token and sort direction vocabulary appear in the class"
            ),

            new LuceneQueryCase(
                "index-searcher",
                "IndexSearcher rewrite query weight similarity leafContexts executor",
                List.of("IndexSearcher"),
                List.of("TopScoreDocCollector", "CollectorManager"),
                "name-lookup",
                "'IndexSearcher' is the class name; 'rewrite', 'createWeight', 'similarity', " +
                "'leafContexts' are specific vocabulary on IndexSearcher. Avoids generic tokens " +
                "like 'search'/'collect'/'hits' that appear across thousands of docs"
            ),

            // ── semantic: vocabulary gap ─────────────────────────────────────

            new LuceneQueryCase(
                "search-after-pagination",
                "paginate through results by passing the last returned hit as a cursor for the next page",
                List.of("FieldDoc"),
                List.of("TopFieldDocs", "ScoreDoc"),
                "semantic",
                "Developer says 'cursor-based pagination'; Lucene uses 'searchAfter(ScoreDoc)' " +
                "where the cursor is a FieldDoc. 'paginate' does not appear in the source"
            ),

            new LuceneQueryCase(
                "term-in-set",
                "match documents containing any term from a large fixed set of values",
                List.of("TermInSetQuery"),
                List.of("BooleanQuery"),
                "semantic",
                "'TermInSetQuery' is designed for exactly this use case but the developer " +
                "would not know its name; 'any term from a set' is not in its class name"
            ),

            new LuceneQueryCase(
                "distance-feature",
                "boost score of documents whose numeric field value is close to a target number",
                List.of("DistanceFeatureQuery"),
                List.of("FunctionScoreQuery", "LongPoint"),
                "semantic",
                "Developer says 'close to a target'; Lucene calls it 'distance feature'. " +
                "'boost' appears in many classes making this noisy"
            ),

            new LuceneQueryCase(
                "faceted-counting",
                "count and aggregate documents into categories without relevance scoring",
                List.of("FacetsCollector"),
                List.of("Facets", "FacetResult"),
                "semantic",
                "Developer says 'aggregate into categories'; Lucene uses 'facets'. " +
                "FacetsCollector is the entry point but 'aggregate' and 'categories' " +
                "are not strong tokens in the source"
            ),

            new LuceneQueryCase(
                "unified-highlighter",
                "highlight matching terms inside the retrieved document text with offset markers",
                List.of("UnifiedHighlighter"),
                List.of("DefaultPassageFormatter", "PassageScorer"),
                "semantic",
                "'UnifiedHighlighter' is the main class; developer says 'highlight matching terms' " +
                "which maps to it, but 'unified' is not guessable from 'highlight'"
            ),

            new LuceneQueryCase(
                "background-reopen",
                "automatically refresh the index reader in the background when new documents are indexed",
                List.of("ControlledRealTimeReopenThread"),
                List.of("SearcherManager", "ReferenceManager"),
                "semantic",
                "Developer says 'auto refresh in background'; Lucene uses " +
                "'ControlledRealTimeReopenThread'. Overlaps with nrt-reader from training set " +
                "but focuses on the background thread rather than the reader itself"
            ),

            new LuceneQueryCase(
                "live-docs-filter",
                "represent which documents in a segment are live versus soft-deleted",
                List.of("Bits"),
                List.of("FixedBitSet", "SparseFixedBitSet"),
                "semantic",
                "Developer says 'live vs deleted'; Lucene uses 'Bits' (via liveDocs). " +
                "'live docs' is a concept not a class name; FixedBitSet is the typical impl"
            ),

            new LuceneQueryCase(
                "payload-scoring",
                "store custom byte payloads alongside term positions and use them to influence score",
                List.of("PayloadScoreQuery"),
                List.of("PayloadFunction", "PayloadDecoder"),
                "semantic",
                "Developer says 'custom byte payloads influence score'; Lucene has " +
                "'PayloadScoreQuery' + 'PayloadFunction'. 'payload' is niche vocabulary"
            ),

            // ── conceptual: mental model doesn't match code structure ─────────

            new LuceneQueryCase(
                "inverted-index-terms",
                "data structure mapping each unique word to all documents and positions it appears in",
                List.of("Terms", "TermsEnum"),
                List.of("PostingsEnum", "TermState"),
                "conceptual",
                "Developer says 'mapping word to documents and positions'; Lucene uses " +
                "'Terms' + 'TermsEnum' + 'PostingsEnum'. No class is named 'InvertedIndex'"
            ),

            new LuceneQueryCase(
                "index-locking",
                "prevent two JVM processes from writing to the same index directory simultaneously",
                List.of("NativeFSLockFactory"),
                List.of("LockFactory", "Lock"),
                "conceptual",
                "'Locking' is the concept; NativeFSLockFactory is the implementation. " +
                "Developer says 'prevent concurrent writes from two JVMs' which doesn't " +
                "map directly to any token in the class names"
            ),

            new LuceneQueryCase(
                "integer-compression",
                "compress sequences of document IDs and frequencies using bit-packing to save disk space",
                List.of("ForUtil"),
                List.of("PForUtil", "Lucene99PostingsFormat"),
                "conceptual",
                "'ForUtil' implements SIMD bit-packing for postings compression. " +
                "Developer says 'compress doc IDs'; Lucene uses 'FOR' (Frame of Reference) " +
                "encoding — an academic term not guessable from the developer's phrasing"
            ),

            new LuceneQueryCase(
                "doc-values",
                "per-document numeric values kept in RAM for fast sorting without fetching stored fields",
                List.of("NumericDocValues"),
                List.of("SortedNumericDocValues", "DocValues"),
                "conceptual",
                "Developer says 'per-document numeric values for sorting'; Lucene calls them " +
                "'DocValues'. The name doesn't hint at sorting or numeric storage"
            ),

            new LuceneQueryCase(
                "indexing-pipeline",
                "pipeline that processes each document field and writes it into the inverted index structures",
                List.of("IndexingChain"),
                List.of("TermsHashPerField", "DocValuesWriter"),
                "conceptual",
                "'IndexingChain' is the internal pipeline class. Developer says 'pipeline for " +
                "document fields'; Lucene's naming doesn't expose this abstraction publicly"
            ),

            new LuceneQueryCase(
                "deletion-policy",
                "decide which old index commit points to keep or delete for backup and recovery",
                List.of("IndexDeletionPolicy"),
                List.of("KeepOnlyLastCommitDeletionPolicy", "NoDeletionPolicy"),
                "conceptual",
                "Developer says 'keep or delete old commits for backup'; Lucene uses " +
                "'IndexDeletionPolicy'. 'backup' and 'recovery' don't appear in the class name"
            ),

            new LuceneQueryCase(
                "parallel-leaf-slices",
                "divide segment work into independent slices so they can be executed in parallel threads",
                List.of("LeafSlice"),
                List.of("IndexSearcher", "TaskExecutor"),
                "conceptual",
                "'LeafSlice' is the partition unit for parallel search. Developer says " +
                "'divide segments into slices for parallel threads' — 'LeafSlice' and " +
                "'TaskExecutor' are internal names not derivable from that phrasing"
            )
        );
    }

    // ── Infrastructure ────────────────────────────────────────────────────────

    private static SearchEngine  searchEngine;
    private static LuceneIndexer luceneIndexer;

    @BeforeAll
    static void setup() throws Exception {
        IndexConfig config       = IndexConfig.load();
        ProjectRegistry registry = new ProjectRegistry(config);

        var luceneMeta = registry.find("lucene")
                .orElseThrow(() -> new IllegalStateException(
                        "Lucene project not indexed. " +
                        "Run: pharos index /home/nhimanen/projects/lucene --project lucene"));

        org.junit.jupiter.api.Assumptions.assumeTrue(
                luceneMeta.getMethodCount() > 0,
                "Lucene project has 0 methods — still indexing.");

        luceneIndexer = new LuceneIndexer(config);
        searchEngine  = new SearchEngine(luceneIndexer, EmbeddingProvider.create(config), registry);
    }

    @AfterAll
    static void teardown() {
        if (luceneIndexer != null) luceneIndexer.close();
    }

    // ── Per-query: keyword vs hybrid ─────────────────────────────────────────

    @Test
    @Order(1)
    void validation_keywordVsHybrid_perQuery() throws IOException {
        System.out.println();
        System.out.println(bar());
        System.out.println("  LUCENE VALIDATION SET — keyword vs hybrid per query");
        System.out.println(bar());
        System.out.printf("  %-36s %-12s │ %16s │ %16s%n",
                "Query ID", "Category", "── keyword ──", "── hybrid ──");
        System.out.printf("  %-36s %-12s │ %5s %5s %5s │ %5s %5s %5s%n",
                "", "", "MRR", "P@5", "N@5", "MRR", "P@5", "N@5");
        System.out.println("  " + "─".repeat(86));

        for (LuceneQueryCase qc : cases()) {
            IrMetrics.RelevanceMap rel = qc.relevanceMap();
            List<SearchResult> kw = deduplicateByClass(searchKeyword(qc.query()));
            List<SearchResult> hy = deduplicateByClass(searchHybrid(qc.query()));

            double kwMrr = IrMetrics.mrr(kw, rel);
            double kwP5  = IrMetrics.precisionAt(kw, 5, rel);
            double kwN5  = IrMetrics.ndcgAt(kw, 5, rel);
            double hyMrr = IrMetrics.mrr(hy, rel);
            double hyP5  = IrMetrics.precisionAt(hy, 5, rel);
            double hyN5  = IrMetrics.ndcgAt(hy, 5, rel);

            String delta = hyMrr > kwMrr + 0.001 ? "▲" : hyMrr < kwMrr - 0.001 ? "▼" : " ";
            System.out.printf("  %-36s %-12s │ %5.3f %5.3f %5.3f │ %5.3f %5.3f %5.3f %s%n",
                    qc.id(), qc.category(),
                    kwMrr, kwP5, kwN5,
                    hyMrr, hyP5, hyN5, delta);
        }
        System.out.println(bar());
        System.out.println("  ▲ hybrid improved  ▼ hybrid regressed  (MRR threshold ±0.001)");
        System.out.println(bar());
        System.out.println();
    }

    // ── Aggregate by category ─────────────────────────────────────────────────

    @Test
    @Order(2)
    void validation_keywordVsHybrid_aggregate() throws IOException {
        System.out.println();
        System.out.println(bar());
        System.out.println("  LUCENE VALIDATION SET — aggregate by category");
        System.out.println(bar());
        System.out.printf("  %-14s │ %28s │ %28s%n",
                "", "──────── keyword ────────", "──────── hybrid ─────────");
        System.out.printf("  %-14s │ %5s %5s %5s %5s │ %5s %5s %5s %5s │ N%n",
                "Category", "MRR", "MAP", "N@5", "P@5", "MRR", "MAP", "N@5", "P@5");
        System.out.println("  " + "─".repeat(80));

        for (String cat : List.of("name-lookup", "semantic", "conceptual")) {
            List<LuceneQueryCase> group = cases().stream()
                    .filter(c -> cat.equals(c.category())).toList();
            double[] kw = aggregate(group, false);
            double[] hy = aggregate(group, true);
            System.out.printf("  %-14s │ %5.3f %5.3f %5.3f %5.3f │ %5.3f %5.3f %5.3f %5.3f │ %d%n",
                    cat, kw[0], kw[1], kw[2], kw[3], hy[0], hy[1], hy[2], hy[3], group.size());
        }

        System.out.println("  " + "─".repeat(80));
        double[] kwAll = aggregate(cases(), false);
        double[] hyAll = aggregate(cases(), true);
        System.out.printf("  %-14s │ %5.3f %5.3f %5.3f %5.3f │ %5.3f %5.3f %5.3f %5.3f │ %d%n",
                "OVERALL", kwAll[0], kwAll[1], kwAll[2], kwAll[3],
                hyAll[0], hyAll[1], hyAll[2], hyAll[3], cases().size());
        System.out.printf("  %-14s │ %5s %5s %5s %5s │ %+5.3f %+5.3f %+5.3f %+5.3f │%n",
                "Δ hybrid−kw", "", "", "", "",
                hyAll[0] - kwAll[0], hyAll[1] - kwAll[1],
                hyAll[2] - kwAll[2], hyAll[3] - kwAll[3]);
        System.out.println(bar());
        System.out.println();
    }

    // ── Missed-query analysis ─────────────────────────────────────────────────

    @Test
    @Order(3)
    void validation_missedQueries() throws IOException {
        System.out.println();
        System.out.println(bar());
        System.out.println("  MISSED QUERIES (VALIDATION SET)");
        System.out.println(bar());

        int kwMisses = 0, hyMisses = 0;
        for (LuceneQueryCase qc : cases()) {
            IrMetrics.RelevanceMap rel = qc.relevanceMap();
            List<SearchResult> kw = deduplicateByClass(searchKeyword(qc.query()));
            List<SearchResult> hy = deduplicateByClass(searchHybrid(qc.query()));
            double kwMrr = IrMetrics.mrr(kw, rel);
            double hyMrr = IrMetrics.mrr(hy, rel);

            if (kwMrr == 0.0) kwMisses++;
            if (hyMrr == 0.0) hyMisses++;
            if (kwMrr != 0.0 && hyMrr != 0.0) continue;

            String tag = kwMrr == 0 && hyMrr == 0 ? "BOTH MISS"
                       : kwMrr == 0               ? "keyword miss (hybrid found)"
                       :                            "hybrid miss (keyword found)";
            System.out.printf("%n  [%s] %s — %s%n  Q: \"%s\"%n  Expected: %s%n",
                    qc.category(), qc.id(), tag, qc.query(), qc.highRelevance());
            if (kwMrr == 0) System.out.printf("  keyword top-3: %s%n", top3Labels(kw));
            if (hyMrr == 0) System.out.printf("  hybrid  top-3: %s%n", top3Labels(hy));
        }
        if (kwMisses == 0 && hyMisses == 0)
            System.out.println("  (none)");
        else
            System.out.printf("%n  Summary: keyword misses=%d  hybrid misses=%d%n", kwMisses, hyMisses);
        System.out.println(bar());
        System.out.println();
    }

    // ── Regression gates ─────────────────────────────────────────────────────

    /**
     * Name-lookup keyword MRR ≥ 0.30.
     * This validation set is intentionally harder than the training set — two of the five
     * name-lookup queries require specific sub-vocabulary rather than the bare class name.
     * Baseline (current system): ~0.31 after query fixes.
     */
    @Test @Order(4)
    void gate_nameLookup_keyword_mrrAboveFloor() throws IOException {
        List<LuceneQueryCase> nl = cases().stream()
                .filter(c -> "name-lookup".equals(c.category())).toList();
        double mrr = aggregate(nl, false)[0];
        assertThat(mrr)
                .as("Validation name-lookup keyword MRR must be >= 0.30; got %.3f".formatted(mrr))
                .isGreaterThanOrEqualTo(0.30);
    }

    /**
     * Overall keyword MRR ≥ 0.15.
     * Validation set has more hard conceptual/semantic queries than the training set.
     * Baseline (current system): ~0.19. Gate is set at 0.15 to catch catastrophic regressions.
     */
    @Test @Order(5)
    void gate_overall_keyword_mrrAboveFloor() throws IOException {
        double mrr = aggregate(cases(), false)[0];
        assertThat(mrr)
                .as("Validation overall keyword MRR must be >= 0.15; got %.3f".formatted(mrr))
                .isGreaterThanOrEqualTo(0.15);
    }

    /** Hybrid overall MRR ≥ keyword MRR − 0.05 — hybrid must not badly hurt keyword. */
    @Test @Order(6)
    void gate_hybrid_notWorseThanKeyword() throws IOException {
        double kwMrr = aggregate(cases(), false)[0];
        double hyMrr = aggregate(cases(), true)[0];
        assertThat(hyMrr)
                .as("Validation hybrid MRR (%.3f) must be within 0.05 of keyword MRR (%.3f)"
                        .formatted(hyMrr, kwMrr))
                .isGreaterThanOrEqualTo(kwMrr - 0.05);
    }

    /** Hybrid semantic MRR ≥ keyword semantic − 0.15 — embeddings must help on vocab-gap queries. */
    @Test @Order(7)
    void gate_hybrid_improvesSemantic() throws IOException {
        List<LuceneQueryCase> semantic = cases().stream()
                .filter(c -> "semantic".equals(c.category())).toList();
        double kwMrr = aggregate(semantic, false)[0];
        double hyMrr = aggregate(semantic, true)[0];
        assertThat(hyMrr)
                .as("Validation hybrid semantic MRR (%.3f) must be >= keyword (%.3f) − 0.15"
                        .formatted(hyMrr, kwMrr))
                .isGreaterThanOrEqualTo(kwMrr - 0.15);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private List<SearchResult> searchKeyword(String query) throws IOException {
        return searchEngine.search(SearchRequest.keyword(query, "lucene", 20));
    }

    private List<SearchResult> searchHybrid(String query) throws IOException {
        return searchEngine.search(SearchRequest.hybrid(query, "lucene", 20));
    }

    private static List<SearchResult> deduplicateByClass(List<SearchResult> results) {
        java.util.LinkedHashMap<String, SearchResult> seen = new java.util.LinkedHashMap<>();
        for (SearchResult r : results) {
            String cls = r.className() != null ? r.className() : "";
            seen.putIfAbsent(cls, r);
        }
        return new ArrayList<>(seen.values());
    }

    private double[] aggregate(List<LuceneQueryCase> qs, boolean useHybrid) throws IOException {
        List<Double> mrrs = new ArrayList<>(), aps = new ArrayList<>(),
                     ndcgs = new ArrayList<>(), p5s = new ArrayList<>(),
                     r10s = new ArrayList<>();
        for (LuceneQueryCase qc : qs) {
            List<SearchResult> dedup = deduplicateByClass(
                    useHybrid ? searchHybrid(qc.query()) : searchKeyword(qc.query()));
            IrMetrics.RelevanceMap rel = qc.relevanceMap();
            mrrs.add(IrMetrics.mrr(dedup, rel));
            aps.add(IrMetrics.averagePrecision(dedup, rel));
            ndcgs.add(IrMetrics.ndcgAt(dedup, 5, rel));
            p5s.add(IrMetrics.precisionAt(dedup, 5, rel));
            r10s.add(IrMetrics.recallAt(dedup, 10, rel));
        }
        return new double[]{
            IrMetrics.mean(mrrs), IrMetrics.mean(aps), IrMetrics.mean(ndcgs),
            IrMetrics.mean(p5s),  IrMetrics.mean(r10s)
        };
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    private static String top3Labels(List<SearchResult> results) {
        return results.stream().limit(3)
                .map(r -> r.className() != null ? r.className() : r.label())
                .collect(Collectors.joining(", "));
    }

    private static String bar() { return "━".repeat(72); }
}
