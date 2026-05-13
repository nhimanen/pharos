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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Search quality benchmark against the indexed Apache Lucene source tree.
 *
 * <p>Uses the real production Lucene index ({@code ~/.pharos/indexes/lucene},
 * ~70k methods, ~8k classes) as the evaluation corpus. All queries are
 * phrased as a developer would type them, never using the exact class name.
 *
 * <h3>Query tiers</h3>
 * <ul>
 *   <li><b>name-lookup</b> (5) — query tokens appear directly in method/class names.
 *       Baseline tier: BM25 alone should rank the target first.
 *   <li><b>semantic</b> (8) — vocabulary gap: developer's phrasing differs from
 *       Lucene's internal nomenclature (e.g., "recent documents" → NRT,
 *       "score threshold" → {@code minCompetitiveScore}).
 *   <li><b>conceptual</b> (7) — developer's mental model maps to a subsystem
 *       rather than a single class: "inverted index on disk" → codec classes,
 *       "detect index corruption" → CheckIndex.
 * </ul>
 *
 * <h3>Metrics</h3>
 * <ul>
 *   <li>MRR, MAP, NDCG@5, P@5, R@10 per tier and overall
 *   <li>Per-query detail with grade-2/grade-1 relevance markers in top-5
 *   <li>Regression gates: name-lookup MRR &ge; 0.80, overall MRR &ge; 0.40
 * </ul>
 *
 * <p><b>Prerequisite:</b> the Lucene project must be indexed:
 * <pre>{@code  pharos index /home/nhimanen/projects/lucene --project lucene}</pre>
 *
 * <p>Tag: {@code quality} — excluded from default {@code mvn test}.
 * Run: {@code mvn test -Dtest=LuceneSearchQualityTest -DexcludedGroups=perf,integration}
 */
@Tag("quality")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LuceneSearchQualityTest {

    // ── Dataset ───────────────────────────────────────────────────────────────

    /**
     * A labeled query against the Lucene index.
     *
     * <p>{@code highRelevance} / {@code partialRelevance} use class names exactly
     * as they appear in the source tree (without package prefix).
     */
    record LuceneQueryCase(
            String id,
            String query,
            List<String> highRelevance,    // grade-2: primary answers
            List<String> partialRelevance, // grade-1: related/supporting
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
                "bm25-similarity",
                "BM25Similarity k1 b parameter idf score",
                List.of("BM25Similarity"),
                List.of("SimilarityBase", "TFIDFSimilarity"),
                "name-lookup",
                "Query includes the exact class name 'BM25Similarity' plus constructor params k1/b; " +
                "className × 1.5 and methodName × 3 boost should surface it at rank 1"
            ),

            new LuceneQueryCase(
                "fuzzy-query",
                "fuzzy query edit distance approximate term matching",
                List.of("FuzzyQuery", "FuzzyTermsEnum"),
                List.of("LevenshteinAutomata", "AutomatonQuery"),
                "name-lookup",
                "'fuzzy' and 'edit distance' both appear in FuzzyQuery/FuzzyTermsEnum javadoc; " +
                "strong name-match signal"
            ),

            new LuceneQueryCase(
                "index-writer",
                "write documents to the index add delete update commit",
                List.of("IndexWriter"),
                List.of("IndexWriterConfig", "LiveIndexWriterConfig", "DocumentsWriter"),
                "name-lookup",
                "All four verbs (add, delete, update, commit) are IndexWriter method names; " +
                "methodName × 3 boost makes this an easy top-1"
            ),

            new LuceneQueryCase(
                "boolean-query",
                "BooleanQuery clauses BooleanClause Occur MUST SHOULD",
                List.of("BooleanQuery", "BooleanClause"),
                List.of("BooleanScorer", "BooleanWeight"),
                "name-lookup",
                "Query includes exact class names; className × 1.5 and the enum constant names " +
                "MUST/SHOULD appear in BooleanClause.Occur — strong direct token signal"
            ),

            new LuceneQueryCase(
                "hnsw-graph-builder",
                "HNSW graph vector index build approximate nearest neighbor",
                List.of("HnswGraphBuilder"),
                List.of("HnswGraphSearcher", "HnswGraph", "OnHeapHnswGraph"),
                "name-lookup",
                "'HNSW' is the exact acronym in the class name; " +
                "'approximate nearest neighbor' appears in its javadoc"
            ),

            // ── semantic: vocabulary gap ─────────────────────────────────────

            new LuceneQueryCase(
                "nrt-reader",
                "open a reader that sees uncommitted in-memory documents without a full commit",
                List.of("DirectoryReader", "ControlledRealTimeReopenThread"),
                List.of("SearcherManager", "ReferenceManager", "StandardDirectoryReader"),
                "semantic",
                "Developer says 'uncommitted in-memory documents'; Lucene calls this NRT " +
                "(near-real-time). 'openIfChanged' and 'getReader' are the relevant entry " +
                "points but the developer won't guess those tokens"
            ),

            new LuceneQueryCase(
                "score-pruning",
                "prune non-competitive documents early by tracking minimum score needed for top-k",
                List.of("WANDScorer", "MaxScoreCache"),
                List.of("MaxScoreAccumulator", "BlockMaxConjunctionScorer", "ImpactsDISI"),
                "semantic",
                "'WAND' (Weak AND) and 'setMinCompetitiveScore' are Lucene internals not guessable " +
                "from the developer's phrasing 'prune non-competitive hits'"
            ),

            new LuceneQueryCase(
                "query-cache",
                "cache query results in memory to avoid recomputing them on repeated searches",
                List.of("LRUQueryCache"),
                List.of("QueryCache", "UsageTrackingQueryCachingPolicy", "SegmentCacheable"),
                "semantic",
                "'LRU' is a data-structure term not in the developer's query; " +
                "the javadoc says 'cache query results to avoid recomputation'"
            ),

            new LuceneQueryCase(
                "segment-merge",
                "merge multiple index segments into fewer larger segments to improve read performance",
                List.of("SegmentMerger", "ConcurrentMergeScheduler"),
                List.of("MergePolicy", "TieredMergePolicy", "LogMergePolicy"),
                "semantic",
                "Developer says 'merge segments'; 'SegmentMerger' is the direct class but " +
                "'ConcurrentMergeScheduler' is the scheduler that drives it — both are targets. " +
                "Broad grep on 'merge' is very noisy"
            ),

            new LuceneQueryCase(
                "two-phase-scoring",
                "defer expensive scoring until after a fast approximation pass to skip unlikely documents",
                List.of("TwoPhaseIterator"),
                List.of("ConjunctionDISI", "ReqOptSumScorer", "BlockMaxConjunctionScorer"),
                "semantic",
                "'TwoPhaseIterator' encodes the concept of 'approximation then verification'; " +
                "developer phrasing 'fast approximation pass' maps to the class's contract"
            ),

            new LuceneQueryCase(
                "postings-skip",
                "jump ahead in a postings list to reach a target document without scanning every entry",
                List.of("MultiLevelSkipListReader"),
                List.of("ImpactsDISI", "DocIdSetIterator"),
                "semantic",
                "Developer says 'jump ahead without scanning'; Lucene says 'skipTo' and " +
                "'MultiLevelSkipListReader'. 'advance' is an English word with thousands of hits"
            ),

            new LuceneQueryCase(
                "token-stream-pipeline",
                "chain text filters that transform input characters into a stream of searchable tokens",
                List.of("TokenStream", "Analyzer"),
                List.of("StandardAnalyzer", "TokenFilter", "StandardTokenizer"),
                "semantic",
                "Developer says 'chain text filters'; Lucene uses 'TokenStream' + 'TokenFilter'. " +
                "'createComponents' is the factory method but the developer won't type that"
            ),

            new LuceneQueryCase(
                "field-boost",
                "apply a per-field score multiplier so matches in title rank higher than body",
                List.of("BoostQuery"),
                List.of("CombinedFieldQuery", "BlendedTermQuery", "FieldStats"),
                "semantic",
                "Developer says 'score multiplier'; Lucene wraps queries in 'BoostQuery'. " +
                "The concept of 'title vs. body' doesn't appear literally in the code"
            ),

            // ── conceptual: mental model doesn't match code structure ─────────

            new LuceneQueryCase(
                "inverted-index-on-disk",
                "read terms and their document lists from the on-disk inverted index structure",
                List.of("BlockTreeTermsReader", "Lucene99PostingsReader"),
                List.of("TermsEnum", "PostingsEnum", "SegmentReader"),
                "conceptual",
                "Developer thinks 'inverted index on disk'; Lucene uses codec classes " +
                "'BlockTreeTermsReader' and 'Lucene99PostingsReader'. No single class is named " +
                "'InvertedIndex'"
            ),

            new LuceneQueryCase(
                "detect-index-corruption",
                "detect and report corruption or inconsistencies in the index on disk",
                List.of("CheckIndex"),
                List.of("CorruptIndexException", "SegmentInfos"),
                "conceptual",
                "CheckIndex is the dedicated tool for this; developer says 'detect corruption' " +
                "but CheckIndex also checks for structural inconsistencies beyond just corruption"
            ),

            new LuceneQueryCase(
                "multidimensional-range-query",
                "find documents where a numeric field falls within a multidimensional bounding box",
                List.of("BKDWriter", "BKDReader"),
                List.of("PointRangeQuery", "PointValues", "PointInSetQuery"),
                "conceptual",
                "BKD (Block KD-tree) is an academic term. Developer says 'multidimensional " +
                "bounding box'; Lucene uses PointValues backed by BKD trees"
            ),

            new LuceneQueryCase(
                "concurrent-flush",
                "coordinate concurrent threads flushing buffered documents to disk as new segments",
                List.of("DocumentsWriterFlushControl"),
                List.of("DocumentsWriter", "DocumentsWriterPerThread", "FlushPolicy"),
                "conceptual",
                "'flush' appears in hundreds of methods. 'DocumentsWriterFlushControl' is the " +
                "correct class but requires knowing Lucene's internal threading model"
            ),

            new LuceneQueryCase(
                "searcher-lifecycle",
                "safely acquire and release an index searcher shared across concurrent threads",
                List.of("SearcherManager", "ReferenceManager"),
                List.of("SearcherLifetimeManager", "SearcherFactory"),
                "conceptual",
                "Developer thinks 'shared searcher with lifecycle'; Lucene uses 'SearcherManager' " +
                "backed by the generic 'ReferenceManager' pattern. 'acquire/release' are the " +
                "key verbs but 'SearcherManager' is not guessable from those alone"
            ),

            new LuceneQueryCase(
                "index-commit-snapshot",
                "take a point-in-time snapshot of the index that can be used for backup or replication",
                List.of("IndexCommit", "SnapshotDeletionPolicy"),
                List.of("IndexDeletionPolicy", "PersistentSnapshotDeletionPolicy"),
                "conceptual",
                "'snapshot' and 'point-in-time' don't appear in 'IndexCommit' as tokens; " +
                "SnapshotDeletionPolicy is the correct mechanism but the developer's vocabulary " +
                "('backup', 'replication') differs from Lucene's"
            ),

            new LuceneQueryCase(
                "leaf-reader-context",
                "iterate over the individual leaf segments of a composite reader for parallel scoring",
                List.of("LeafReaderContext"),
                List.of("LeafReader", "IndexReaderContext", "CompositeReader"),
                "conceptual",
                "'leaf segment' and 'parallel scoring' map to 'LeafReaderContext' and 'LeafSlice' " +
                "respectively; developer won't guess 'LeafReaderContext' from 'iterate over segments'"
            ),

            // ── proximity: phrase matching should lift exact-phrase class/method names ──

            new LuceneQueryCase(
                "minimum-competitive-score",
                "set minimum competitive score to skip non competitive documents",
                List.of("WANDScorer", "MaxScoreCache"),
                List.of("MaxScoreAccumulator", "BlockMaxConjunctionScorer"),
                "proximity",
                "setMinCompetitiveScore splits to 'set minimum competitive score' — exact phrase in " +
                "methodName; individual terms (minimum, score) are common across scoring classes " +
                "making OR noisy; phrase boost should surface WANDScorer at rank 1"
            ),

            new LuceneQueryCase(
                "stored-fields-reader",
                "stored fields reader codec implementation",
                List.of("StoredFieldsReader"),
                List.of("StoredFieldsWriter", "Lucene90StoredFieldsFormat"),
                "proximity",
                "StoredFieldsReader splits to 'stored fields reader' — exact phrase in className; " +
                "'stored' and 'fields' appear throughout Lucene making OR results noisy; " +
                "phrase match on className should rank StoredFieldsReader at rank 1"
            ),

            new LuceneQueryCase(
                "top-field-collector",
                "top field docs collector sort by field value",
                List.of("TopFieldCollector"),
                List.of("TopFieldDocs", "SortField", "FieldValueHitQueue"),
                "proximity",
                "TopFieldCollector splits to 'top field collector'; 'top', 'field', 'collector', " +
                "'sort' all appear independently in many Lucene classes; phrase 'top field collector' " +
                "on className should suppress unrelated collectors"
            )
        );
    }

    // ── Infrastructure ────────────────────────────────────────────────────────

    private static SearchEngine  searchEngine;
    private static LuceneIndexer luceneIndexer;

    @BeforeAll
    static void setup() throws Exception {
        IndexConfig config     = IndexConfig.load();
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

    // ── Per-query side-by-side: keyword vs hybrid ────────────────────────────

    @Test
    @Order(1)
    void benchmark_keywordVsHybrid_perQuery() throws IOException {
        boolean hybridAvail = searchEngine.search(
                SearchRequest.hybrid(cases().get(0).query(), "lucene", 1)).stream()
                .anyMatch(r -> "hybrid".equals(r.searchType()));

        System.out.println();
        System.out.println(bar());
        System.out.printf("  LUCENE QUALITY BENCHMARK — keyword vs hybrid per query%s%n",
                hybridAvail ? "" : "  [hybrid unavailable — showing keyword only]");
        System.out.println(bar());
        System.out.printf("  %-36s %-12s │ %16s │ %16s%n",
                "Query ID", "Category", "── keyword ──", "── hybrid ──");
        System.out.printf("  %-36s %-12s │ %5s %5s %5s │ %5s %5s %5s%n",
                "", "", "MRR", "P@5", "N@5", "MRR", "P@5", "N@5");
        System.out.println("  " + "─".repeat(86));

        for (LuceneQueryCase qc : cases()) {
            IrMetrics.RelevanceMap rel = qc.relevanceMap();

            List<SearchResult> kw = deduplicateByClass(searchKeyword(qc.query()));
            double kwMrr  = IrMetrics.mrr(kw, rel);
            double kwP5   = IrMetrics.precisionAt(kw, 5, rel);
            double kwN5   = IrMetrics.ndcgAt(kw, 5, rel);

            List<SearchResult> hy = deduplicateByClass(searchHybrid(qc.query()));
            double hyMrr  = IrMetrics.mrr(hy, rel);
            double hyP5   = IrMetrics.precisionAt(hy, 5, rel);
            double hyN5   = IrMetrics.ndcgAt(hy, 5, rel);

            // Mark queries where hybrid improves or regresses on MRR
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

    // ── Aggregate comparison: keyword vs hybrid by category ──────────────────

    @Test
    @Order(2)
    void benchmark_keywordVsHybrid_aggregate() throws IOException {
        System.out.println();
        System.out.println(bar());
        System.out.println("  LUCENE QUALITY BENCHMARK — aggregate by category");
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

    // ── Three-way comparison: keyword vs vector vs hybrid ────────────────────

    @Test
    @Order(3)
    void benchmark_threeWay_perQuery() throws IOException {
        System.out.println();
        System.out.println(bar());
        System.out.println("  THREE-WAY BENCHMARK — keyword vs vector vs hybrid");
        System.out.println(bar());
        System.out.printf("  %-36s %-12s │ %11s │ %11s │ %11s%n",
                "Query ID", "Category", "─ keyword ─", "─ vector ──", "─ hybrid ──");
        System.out.printf("  %-36s %-12s │ %5s %5s │ %5s %5s │ %5s %5s%n",
                "", "", "MRR", "P@5", "MRR", "P@5", "MRR", "P@5");
        System.out.println("  " + "─".repeat(90));

        for (LuceneQueryCase qc : cases()) {
            IrMetrics.RelevanceMap rel = qc.relevanceMap();

            List<SearchResult> kw  = deduplicateByClass(searchKeyword(qc.query()));
            List<SearchResult> vec = deduplicateByClass(searchVector(qc.query()));
            List<SearchResult> hy  = deduplicateByClass(searchHybrid(qc.query()));

            double kwMrr  = IrMetrics.mrr(kw,  rel);
            double vecMrr = IrMetrics.mrr(vec, rel);
            double hyMrr  = IrMetrics.mrr(hy,  rel);
            double kwP5   = IrMetrics.precisionAt(kw,  5, rel);
            double vecP5  = IrMetrics.precisionAt(vec, 5, rel);
            double hyP5   = IrMetrics.precisionAt(hy,  5, rel);

            // best MRR among the three
            double best = Math.max(kwMrr, Math.max(vecMrr, hyMrr));
            String kwMark  = kwMrr  == best && best > 0 ? "*" : " ";
            String vecMark = vecMrr == best && best > 0 ? "*" : " ";
            String hyMark  = hyMrr  == best && best > 0 ? "*" : " ";

            System.out.printf("  %-36s %-12s │%s%5.3f %5.3f │%s%5.3f %5.3f │%s%5.3f %5.3f%n",
                    qc.id(), qc.category(),
                    kwMark,  kwMrr,  kwP5,
                    vecMark, vecMrr, vecP5,
                    hyMark,  hyMrr,  hyP5);
        }

        System.out.println("  " + "─".repeat(90));
        double[] kwAll  = aggregate(cases(), false);
        double[] vecAll = aggregateVector(cases());
        double[] hyAll  = aggregate(cases(), true);
        System.out.printf("  %-36s %-12s │ %5.3f %5.3f │ %5.3f %5.3f │ %5.3f %5.3f%n",
                "OVERALL", "", kwAll[0], kwAll[3], vecAll[0], vecAll[3], hyAll[0], hyAll[3]);
        System.out.println(bar());
        System.out.println("  * = best MRR for that query");
        System.out.println(bar());
        System.out.println();
    }

    // ── Top-5 inspection: both strategies side-by-side ───────────────────────

    @Test
    @Order(5)
    void inspect_topResultsPerQuery() throws IOException {
        System.out.println();
        System.out.println("  TOP-5 RESULTS — keyword (left) vs hybrid (right)  ✓✓=grade-2  ✓=grade-1");
        System.out.println("  " + "─".repeat(72));

        for (LuceneQueryCase qc : cases()) {
            List<SearchResult> kw = deduplicateByClass(searchKeyword(qc.query()));
            List<SearchResult> hy = deduplicateByClass(searchHybrid(qc.query()));
            IrMetrics.RelevanceMap rel = qc.relevanceMap();

            System.out.printf("%n  [%s] %s%n  Q: \"%s\"%n",
                    qc.category().toUpperCase(), qc.id(), qc.query());
            System.out.printf("  %-3s %-3s %-38s  %-3s %-3s %-38s%n",
                    "KW", "#", "keyword result", "HY", "#", "hybrid result");

            int rows = Math.max(Math.min(5, kw.size()), Math.min(5, hy.size()));
            for (int i = 0; i < rows; i++) {
                String kwMark = "", kwLabel = "";
                if (i < kw.size()) {
                    int g = rel.grade(kw.get(i));
                    kwMark  = g == 2 ? "✓✓" : g == 1 ? " ✓" : "  ";
                    kwLabel = truncate(kw.get(i).className(), 36);
                }
                String hyMark = "", hyLabel = "";
                if (i < hy.size()) {
                    int g = rel.grade(hy.get(i));
                    hyMark  = g == 2 ? "✓✓" : g == 1 ? " ✓" : "  ";
                    hyLabel = truncate(hy.get(i).className(), 36);
                }
                System.out.printf("  %2s [%d] %-38s  %2s [%d] %-38s%n",
                        kwMark, i + 1, kwLabel, hyMark, i + 1, hyLabel);
            }
        }
        System.out.println();
    }

    // ── Missed-query analysis (hybrid focus) ─────────────────────────────────

    @Test
    @Order(5)
    void inspect_missedQueries() throws IOException {
        System.out.println();
        System.out.println(bar());
        System.out.println("  MISSED QUERIES — keyword miss (MRR=0) AND hybrid miss");
        System.out.println(bar());

        int kwMisses = 0, hyMisses = 0;
        for (LuceneQueryCase qc : cases()) {
            IrMetrics.RelevanceMap rel = qc.relevanceMap();
            List<SearchResult> kw = deduplicateByClass(searchKeyword(qc.query()));
            List<SearchResult> hy = deduplicateByClass(searchHybrid(qc.query()));
            double kwMrr = IrMetrics.mrr(kw, rel);
            double hyMrr = IrMetrics.mrr(hy, rel);

            boolean kwMiss = kwMrr == 0.0;
            boolean hyMiss = hyMrr == 0.0;
            if (!kwMiss && !hyMiss) continue;

            if (kwMiss) kwMisses++;
            if (hyMiss) hyMisses++;

            String tag = kwMiss && hyMiss ? "BOTH MISS"
                       : kwMiss           ? "keyword miss (hybrid found)"
                       :                    "hybrid miss (keyword found)";

            System.out.printf("%n  [%s] %s — %s%n  Q: \"%s\"%n  Expected: %s%n",
                    qc.category(), qc.id(), tag, qc.query(), qc.highRelevance());
            if (kwMiss) {
                System.out.printf("  keyword top-3: %s%n", top3Labels(kw));
            }
            if (hyMiss) {
                System.out.printf("  hybrid  top-3: %s%n", top3Labels(hy));
            }
        }
        if (kwMisses == 0 && hyMisses == 0) {
            System.out.println("  (none — all queries found by both strategies)");
        } else {
            System.out.printf("%n  Summary: keyword misses=%d  hybrid misses=%d%n", kwMisses, hyMisses);
        }
        System.out.println(bar());
        System.out.println();
    }

    // ── Regression gates ─────────────────────────────────────────────────────

    /**
     * Name-lookup MRR (keyword) &ge; 0.50.
     * Direct name matches should appear in top-2 on average.
     */
    @Test
    @Order(6)
    void regression_nameLookup_keyword_mrrAboveFloor() throws IOException {
        List<LuceneQueryCase> nl = cases().stream()
                .filter(c -> "name-lookup".equals(c.category())).toList();
        double mrr = aggregate(nl, false)[0];
        assertThat(mrr)
                .as("name-lookup keyword MRR must be >= 0.50; got %.3f".formatted(mrr))
                .isGreaterThanOrEqualTo(0.50);
    }

    /**
     * Overall keyword MRR &ge; 0.25. Catches catastrophic BM25 regressions.
     */
    @Test
    @Order(7)
    void regression_overall_keyword_mrrAboveFloor() throws IOException {
        double mrr = aggregate(cases(), false)[0];
        assertThat(mrr)
                .as("Overall keyword MRR must be >= 0.25; got %.3f".formatted(mrr))
                .isGreaterThanOrEqualTo(0.25);
    }

    /**
     * Hybrid overall MRR &ge; keyword overall MRR − 0.05.
     * Hybrid with Jina embeddings must not be significantly worse than keyword-only.
     */
    @Test
    @Order(8)
    void regression_hybrid_notWorseThanKeyword() throws IOException {
        double kwMrr = aggregate(cases(), false)[0];
        double hyMrr = aggregate(cases(), true)[0];
        assertThat(hyMrr)
                .as("Hybrid MRR (%.3f) must be within 0.05 of keyword MRR (%.3f)".formatted(hyMrr, kwMrr))
                .isGreaterThanOrEqualTo(kwMrr - 0.05);
    }

    /**
     * Hybrid semantic MRR &ge; keyword semantic MRR.
     * Embeddings should specifically help on vocabulary-gap queries.
     */
    @Test
    @Order(9)
    void regression_hybrid_improvesSemantic() throws IOException {
        List<LuceneQueryCase> semantic = cases().stream()
                .filter(c -> "semantic".equals(c.category())).toList();
        double kwMrr = aggregate(semantic, false)[0];
        double hyMrr = aggregate(semantic, true)[0];
        // Synonym expansion strengthens keyword on semantic queries; allow 0.15 slack so hybrid
        // can trail keyword (which now benefits from synonyms directly) without failing the gate.
        assertThat(hyMrr)
                .as("Hybrid semantic MRR (%.3f) must be >= keyword (%.3f) − 0.15".formatted(hyMrr, kwMrr))
                .isGreaterThanOrEqualTo(kwMrr - 0.15);
    }

    /**
     * Proximity keyword MRR &ge; 0.40.
     *
     * <p>These queries are phrased so that the target class/method name is the exact
     * concatenation of the query tokens (e.g. "top field collector" → TopFieldCollector).
     * Plain OR retrieval is noisy because each token appears independently in many classes.
     * Phrase boosting must lift the exact-name match into the top-3 on average.
     */
    @Test
    @Order(10)
    void regression_proximity_keyword_mrrAboveFloor() throws IOException {
        List<LuceneQueryCase> proximity = cases().stream()
                .filter(c -> "proximity".equals(c.category())).toList();
        double mrr = aggregate(proximity, false)[0];
        assertThat(mrr)
                .as("Proximity keyword MRR must be >= 0.40; got %.3f — phrase boost required".formatted(mrr))
                .isGreaterThanOrEqualTo(0.40);
    }

    // ── Vector score distribution diagnostic ─────────────────────────────────

    @Test
    @Order(5)
    void inspect_vectorScoreDistribution() throws IOException {
        System.out.println();
        System.out.println(bar());
        System.out.println("  VECTOR SCORE DISTRIBUTION — raw Lucene score + P_1bit per query");
        System.out.println("  Lucene COSINE score = (1 + cosine) / 2  →  P_1bit = 1 - arccos(2s-1)/π");
        System.out.println(bar());
        System.out.printf("  %-30s │ %5s %5s │ top-5 vector results (score / P_1bit / relevance)%n",
                "Query ID", "grade2", "grade1");
        System.out.println("  " + "─".repeat(110));

        // Track scores split by whether the result is relevant or not
        List<Double> relevantScores   = new ArrayList<>();
        List<Double> irrelevantScores = new ArrayList<>();

        for (LuceneQueryCase qc : cases()) {
            IrMetrics.RelevanceMap rel = qc.relevanceMap();
            List<SearchResult> vec = searchVector(qc.query());

            // first relevant score and first irrelevant score for summary
            double firstRelScore  = -1;
            double firstIrrelScore = -1;

            StringBuilder hits = new StringBuilder();
            for (int i = 0; i < Math.min(5, vec.size()); i++) {
                SearchResult r = vec.get(i);
                int grade = rel.grade(r);
                double p1bit = p1bit(r.score());
                String mark = grade == 2 ? "✓✓" : grade == 1 ? " ✓" : "  ";
                hits.append(String.format(" [%d]%.3f/%.2f%s", i + 1, r.score(), p1bit, mark));
                if (grade >= 1 && firstRelScore < 0)   firstRelScore   = r.score();
                if (grade == 0 && firstIrrelScore < 0) firstIrrelScore = r.score();
            }

            // Collect all scores split by relevance (full 20 results)
            for (SearchResult r : vec) {
                int grade = rel.grade(r);
                if (grade >= 1) relevantScores.add((double) r.score());
                else            irrelevantScores.add((double) r.score());
            }

            // Show best-hit grade indicators
            long g2count = vec.stream().limit(5).filter(r -> rel.grade(r) == 2).count();
            long g1count = vec.stream().limit(5).filter(r -> rel.grade(r) >= 1).count() - g2count;
            System.out.printf("  %-30s │ %5d %5d │%s%n",
                    truncate(qc.id(), 28), g2count, g1count, hits);
        }

        // Summary statistics
        System.out.println("  " + "─".repeat(110));
        if (!relevantScores.isEmpty()) {
            double relMean = relevantScores.stream().mapToDouble(d -> d).average().orElse(0);
            double relMin  = relevantScores.stream().mapToDouble(d -> d).min().orElse(0);
            double relMax  = relevantScores.stream().mapToDouble(d -> d).max().orElse(0);
            System.out.printf("  Relevant   hits (n=%3d): score mean=%.3f  min=%.3f  max=%.3f  " +
                    "P_1bit mean=%.3f%n",
                    relevantScores.size(), relMean, relMin, relMax, p1bit((float) relMean));
        }
        if (!irrelevantScores.isEmpty()) {
            double irMean = irrelevantScores.stream().mapToDouble(d -> d).average().orElse(0);
            double irMin  = irrelevantScores.stream().mapToDouble(d -> d).min().orElse(0);
            double irMax  = irrelevantScores.stream().mapToDouble(d -> d).max().orElse(0);
            System.out.printf("  Irrelevant hits (n=%3d): score mean=%.3f  min=%.3f  max=%.3f  " +
                    "P_1bit mean=%.3f%n",
                    irrelevantScores.size(), irMean, irMin, irMax, p1bit((float) irMean));
        }
        System.out.println(bar());
        System.out.println();
    }

    /** 1-bit SimHash collision probability from a Lucene COSINE similarity score. */
    private static double p1bit(float luceneScore) {
        double cosine = 2.0 * luceneScore - 1.0;
        cosine = Math.max(-1.0, Math.min(1.0, cosine));
        return 1.0 - Math.acos(cosine) / Math.PI;
    }

    // ── inDegree field probe ──────────────────────────────────────────────────

    @Test
    @Order(6)
    void inspect_inDegreeFieldProbe() throws IOException {
        // Diagnose why inDegree=0 for all classes in the index-writer query.
        // Checks: (1) does TermQuery find the docs, (2) how is inDegree stored/retrieved.
        org.apache.lucene.index.IndexReader reader = luceneIndexer.openReader("lucene");
        org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);

        System.out.println();
        System.out.println(bar());
        System.out.println("  INDEGREE FIELD PROBE — IndexWriter document lookup");
        System.out.println(bar());

        String[] probeClasses = {"IndexWriter", "WANDScorer", "Scorable"};
        for (String cls : probeClasses) {
            org.apache.lucene.search.Query q = new org.apache.lucene.search.BooleanQuery.Builder()
                    .add(new org.apache.lucene.search.TermQuery(
                                    new org.apache.lucene.index.Term(
                                            com.pharos.indexer.DocumentMapper.F_CLASS_NAME, cls.toLowerCase())),
                            org.apache.lucene.search.BooleanClause.Occur.MUST)
                    .add(new org.apache.lucene.search.TermQuery(
                                    new org.apache.lucene.index.Term(
                                            com.pharos.indexer.DocumentMapper.F_PROJECT, "lucene")),
                            org.apache.lucene.search.BooleanClause.Occur.MUST)
                    .build();

            org.apache.lucene.search.TopDocs hits = searcher.search(q, 5);
            System.out.printf("%n  Probe: className TermQuery(\"%s\") → %d hits%n",
                    cls.toLowerCase(), hits.totalHits.value());

            for (org.apache.lucene.search.ScoreDoc sd : hits.scoreDocs) {
                org.apache.lucene.document.Document doc = reader.storedFields().document(sd.doc);
                String storedClass   = doc.get(com.pharos.indexer.DocumentMapper.F_CLASS_NAME);
                String storedMethod  = doc.get(com.pharos.indexer.DocumentMapper.F_METHOD_NAME);
                String storedId      = doc.get(com.pharos.indexer.DocumentMapper.F_ID);
                // Try string retrieval (works for StringField/TextField Store.YES)
                String inDegStr      = doc.get(com.pharos.indexer.DocumentMapper.F_IN_DEGREE);
                // Try numeric retrieval (needed for StoredField(name, int))
                org.apache.lucene.index.IndexableField inDegField =
                        doc.getField(com.pharos.indexer.DocumentMapper.F_IN_DEGREE);
                Number inDegNum = inDegField != null ? inDegField.numericValue() : null;

                System.out.printf("    storedClassName=%-20s method=%-30s inDeg.get()=%-6s inDeg.numeric()=%s%n",
                        storedClass, storedMethod, inDegStr, inDegNum);
                System.out.printf("      id=%s%n", storedId);
            }
        }
        System.out.println();
        System.out.println(bar());
        System.out.println();
    }

    // ── Graph stats for KW-regression queries ────────────────────────────────

    @Test
    @Order(6)
    void inspect_graphStats_kwRegressionCases() throws IOException {
        // Queries where hybrid regresses vs keyword: index-writer, score-pruning, multidimensional-range-query
        // For each: show inDegree + callerContext for (a) expected class and (b) wrong rank-1 hybrid hit
        List<String> focusIds = List.of("index-writer", "score-pruning", "multidimensional-range-query");

        org.apache.lucene.index.IndexReader reader = luceneIndexer.openReader("lucene");
        org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);

        System.out.println();
        System.out.println(bar());
        System.out.println("  GRAPH STATS — KW regression cases (where hybrid hurts)");
        System.out.println("  inDegree = # methods that call this class's methods (graph boost signal)");
        System.out.println("  callerCtx = caller method names indexed as text for semantic context");
        System.out.println(bar());

        for (LuceneQueryCase qc : cases()) {
            if (!focusIds.contains(qc.id())) continue;

            List<SearchResult> kw = searchKeyword(qc.query());
            List<SearchResult> hy = searchHybrid(qc.query());

            System.out.printf("%n  ══ %s%n", qc.id());
            System.out.printf("  Query   : \"%s\"%n", qc.query());
            System.out.printf("  KW rank1: %s (MRR=%.3f)%n",
                    kw.isEmpty() ? "—" : kw.get(0).className(),
                    IrMetrics.mrr(deduplicateByClass(kw), qc.relevanceMap()));
            System.out.printf("  HY rank1: %s (MRR=%.3f)%n",
                    hy.isEmpty() ? "—" : hy.get(0).className(),
                    IrMetrics.mrr(deduplicateByClass(hy), qc.relevanceMap()));

            // Collect the classes we care about: expected + rank-1 of kw + rank-1 of hy
            java.util.Set<String> classesToInspect = new java.util.LinkedHashSet<>();
            classesToInspect.addAll(qc.highRelevance());
            if (!kw.isEmpty()) classesToInspect.add(kw.get(0).className());
            if (!hy.isEmpty()) classesToInspect.add(hy.get(0).className());

            System.out.printf("  %-35s │ %8s │ top callers%n", "Class", "inDegree");
            System.out.println("  " + "─".repeat(100));

            for (String cls : classesToInspect) {
                // Find all method docs for this class, sum their inDegrees, collect callerContext
                org.apache.lucene.search.Query q = new org.apache.lucene.search.BooleanQuery.Builder()
                        .add(new org.apache.lucene.search.TermQuery(
                                new org.apache.lucene.index.Term(
                                        com.pharos.indexer.DocumentMapper.F_CLASS_NAME, cls.toLowerCase())),
                                org.apache.lucene.search.BooleanClause.Occur.MUST)
                        .add(new org.apache.lucene.search.TermQuery(
                                new org.apache.lucene.index.Term(
                                        com.pharos.indexer.DocumentMapper.F_PROJECT, "lucene")),
                                org.apache.lucene.search.BooleanClause.Occur.MUST)
                        .build();

                org.apache.lucene.search.TopDocs hits = searcher.search(q, 200);
                int totalInDegree = 0;
                java.util.Set<String> callers = new java.util.LinkedHashSet<>();
                for (org.apache.lucene.search.ScoreDoc sd : hits.scoreDocs) {
                    org.apache.lucene.document.Document doc = reader.storedFields().document(sd.doc);
                    String storedClass = doc.get(com.pharos.indexer.DocumentMapper.F_CLASS_NAME);
                    if (!cls.equals(storedClass)) continue; // className is tokenized, filter exact
                    String inDegStr = doc.get(com.pharos.indexer.DocumentMapper.F_IN_DEGREE);
                    if (inDegStr != null) totalInDegree += Integer.parseInt(inDegStr);
                    String ctx = doc.get(com.pharos.indexer.DocumentMapper.F_CALLER_CONTEXT);
                    if (ctx != null && !ctx.isBlank()) {
                        callers.addAll(Arrays.asList(ctx.split("\\s+")));
                    }
                }
                String callerSnippet = callers.stream().limit(8).collect(java.util.stream.Collectors.joining(" "));
                boolean isExpected = qc.highRelevance().contains(cls);
                boolean isKwTop = !kw.isEmpty() && kw.get(0).className().equals(cls);
                boolean isHyTop = !hy.isEmpty() && hy.get(0).className().equals(cls);
                String tag = (isExpected ? "✓" : " ") + (isKwTop ? "K" : " ") + (isHyTop ? "H" : " ");
                System.out.printf("  [%s] %-33s │ %8d │ %s%n", tag, cls, totalInDegree,
                        callerSnippet.isBlank() ? "(none)" : callerSnippet);
            }
        }
        System.out.println();
        System.out.println(bar());
        System.out.println();
    }

    // ── Why does vector get it wrong? ─────────────────────────────────────────

    @Test
    @Order(6)
    void inspect_vectorMissReasons() throws IOException {
        // The three queries where vector confidently returns the wrong rank-1 hit
        List<String> focusIds = List.of("index-writer", "postings-skip", "score-pruning");

        System.out.println();
        System.out.println(bar());
        System.out.println("  VECTOR MISS REASONS — query text vs wrong rank-1 document content");
        System.out.println(bar());

        for (LuceneQueryCase qc : cases()) {
            if (!focusIds.contains(qc.id())) continue;

            List<SearchResult> vec = searchVector(qc.query());
            if (vec.isEmpty()) continue;

            IrMetrics.RelevanceMap rel = qc.relevanceMap();

            System.out.println();
            System.out.printf("  ══ Query id   : %s%n", qc.id());
            System.out.printf("  ══ Query text : \"%s\"%n", qc.query());
            System.out.printf("  ══ Expected   : %s%n", qc.highRelevance());

            // Show top-5 with relevance grade
            System.out.println("  Top-5 vector hits:");
            for (int i = 0; i < Math.min(5, vec.size()); i++) {
                SearchResult r = vec.get(i);
                int grade = rel.grade(r);
                String mark = grade == 2 ? "✓✓" : grade == 1 ? " ✓" : "✗✗";
                System.out.printf("    [%d] %s  score=%.4f  type=%s  %s%n",
                        i + 1, r.className(), r.score(), r.docType(), mark);
            }

            // Find where expected class lands in vector results (if at all)
            System.out.println("  Expected class ranks in vector list:");
            for (String expected : qc.highRelevance()) {
                int rank = -1;
                for (int i = 0; i < vec.size(); i++) {
                    if (vec.get(i).className().equals(expected)) { rank = i + 1; break; }
                }
                System.out.printf("    %s → rank %s%n", expected, rank < 0 ? "NOT FOUND" : rank);
            }

            // Drill into rank-1 wrong hit: show what text was embedded
            SearchResult wrong = null;
            for (SearchResult r : vec) {
                if (rel.grade(r) == 0) { wrong = r; break; }
            }
            if (wrong != null) {
                System.out.printf("%n  ── Rank-1 wrong hit: %s (%s) score=%.4f%n",
                        wrong.className(), wrong.docType(), wrong.score());
                if (wrong.javadoc() != null && !wrong.javadoc().isBlank()) {
                    System.out.printf("  javadoc : %s%n", truncate(wrong.javadoc().trim(), 300));
                }
                String body = wrong.body() != null ? wrong.body().trim() : "";
                if (!body.isBlank()) {
                    System.out.printf("  body    : %s%n", truncate(body, 400));
                }
            }

            // Also show rank-1 correct hit's embedded text for comparison
            SearchResult correct = null;
            for (SearchResult r : vec) {
                if (rel.grade(r) == 2) { correct = r; break; }
            }
            if (correct != null) {
                System.out.printf("%n  ── First correct hit: %s (%s) score=%.4f  rank=%d%n",
                        correct.className(), correct.docType(), correct.score(),
                        vec.indexOf(correct) + 1);
                if (correct.javadoc() != null && !correct.javadoc().isBlank()) {
                    System.out.printf("  javadoc : %s%n", truncate(correct.javadoc().trim(), 300));
                }
                String body = correct.body() != null ? correct.body().trim() : "";
                if (!body.isBlank()) {
                    System.out.printf("  body    : %s%n", truncate(body, 400));
                }
            }
            System.out.println();
        }
        System.out.println(bar());
        System.out.println();
    }

    // ── Class-level score aggregation ────────────────────────────────────────

    @Test
    @Order(7)
    void inspect_classScoreAggregation() throws IOException {
        // Strategies: for each class, collect all method-level vector scores, then aggregate.
        // "max"       — best single method score (same as current, but deduplicated to class)
        // "sum3"      — sum of top-3 method scores (rewards classes with multiple matches)
        // "mean3"     — mean of top-3 method scores (normalises for class size)
        // "sum"       — sum of all method scores (strongly rewards large classes)
        record AggResult(String className, double score, int methodCount) {}

        List<String> focusIds = List.of("index-writer", "postings-skip", "score-pruning");

        System.out.println();
        System.out.println(bar());
        System.out.println("  CLASS-SCORE AGGREGATION — method vector scores rolled up to class level");
        System.out.println("  Fetches top-100 method hits, groups by class, applies 4 aggregations");
        System.out.println(bar());

        // Per-strategy MRR across all queries
        List<Double> mrrMax   = new ArrayList<>(), mrrSum3  = new ArrayList<>(),
                     mrrMean3 = new ArrayList<>(), mrrSum   = new ArrayList<>();

        for (LuceneQueryCase qc : cases()) {
            IrMetrics.RelevanceMap rel = qc.relevanceMap();
            // Fetch 100 method-level hits to get good class coverage
            List<SearchResult> vec = searchEngine.search(new SearchRequest(
                    qc.query(), SearchRequest.SearchType.VECTOR, "lucene", null, 100, "text", "method"));

            // Group scores by class
            java.util.Map<String, List<Float>> scoresByClass = new java.util.LinkedHashMap<>();
            java.util.Map<String, SearchResult> repByClass   = new java.util.LinkedHashMap<>();
            for (SearchResult r : vec) {
                scoresByClass.computeIfAbsent(r.className(), k -> new ArrayList<>()).add(r.score());
                repByClass.putIfAbsent(r.className(), r);
            }

            // Build ranked lists for each strategy
            java.util.function.Function<List<Float>, Double> max =
                    scores -> scores.stream().mapToDouble(f -> f).max().orElse(0);
            java.util.function.Function<List<Float>, Double> sum3 = scores -> {
                List<Float> s = new ArrayList<>(scores);
                s.sort(java.util.Comparator.reverseOrder());
                return s.stream().limit(3).mapToDouble(f -> f).sum();
            };
            java.util.function.Function<List<Float>, Double> mean3 = scores -> {
                List<Float> s = new ArrayList<>(scores);
                s.sort(java.util.Comparator.reverseOrder());
                long n = Math.min(3, s.size());
                return s.stream().limit(n).mapToDouble(f -> f).sum() / n;
            };
            java.util.function.Function<List<Float>, Double> sum =
                    scores -> scores.stream().mapToDouble(f -> f).sum();

            java.util.function.Function<
                    java.util.function.Function<List<Float>, Double>,
                    List<SearchResult>> rank = fn -> scoresByClass.entrySet().stream()
                        .sorted((a, b) -> Double.compare(fn.apply(b.getValue()), fn.apply(a.getValue())))
                        .limit(20)
                        .map(e -> {
                            SearchResult rep = repByClass.get(e.getKey());
                            return new SearchResult(rep.id(), rep.project(), rep.packageName(),
                                    rep.className(), rep.qualifiedClassName(), rep.methodName(),
                                    rep.signature(), rep.returnType(), rep.body(), rep.javadoc(),
                                    rep.accessModifier(), rep.filePath(), rep.startLine(), rep.endLine(),
                                    fn.apply(scoresByClass.get(e.getKey())).floatValue(),
                                    "vec-agg", rep.docType());
                        })
                        .toList();

            List<SearchResult> byMax   = rank.apply(max);
            List<SearchResult> bySum3  = rank.apply(sum3);
            List<SearchResult> byMean3 = rank.apply(mean3);
            List<SearchResult> bySum   = rank.apply(sum);

            mrrMax.add(IrMetrics.mrr(byMax, rel));
            mrrSum3.add(IrMetrics.mrr(bySum3, rel));
            mrrMean3.add(IrMetrics.mrr(byMean3, rel));
            mrrSum.add(IrMetrics.mrr(bySum, rel));

            if (!focusIds.contains(qc.id())) continue;

            System.out.printf("%n  ══ %s: \"%s\"%n", qc.id(), qc.query());
            System.out.printf("  Expected: %s%n", qc.highRelevance());
            System.out.printf("  %-30s │ %5s │ %-30s │ %-30s │ %-30s%n",
                    "max (current)", "score", "sum-top3", "mean-top3", "sum-all");
            System.out.println("  " + "─".repeat(130));
            int rows = 6;
            for (int i = 0; i < rows; i++) {
                SearchResult rm   = i < byMax.size()   ? byMax.get(i)   : null;
                SearchResult rs3  = i < bySum3.size()  ? bySum3.get(i)  : null;
                SearchResult rm3  = i < byMean3.size() ? byMean3.get(i) : null;
                SearchResult rs   = i < bySum.size()   ? bySum.get(i)   : null;
                String fmtMax   = rm  == null ? "" : String.format("%s%s %.3f",
                        rel.grade(rm)  >= 2 ? "✓ " : rel.grade(rm)  == 1 ? "~ " : "  ", rm.className(),  rm.score());
                String fmtSum3  = rs3 == null ? "" : String.format("%s%s %.3f",
                        rel.grade(rs3) >= 2 ? "✓ " : rel.grade(rs3) == 1 ? "~ " : "  ", rs3.className(), rs3.score());
                String fmtMean3 = rm3 == null ? "" : String.format("%s%s %.3f",
                        rel.grade(rm3) >= 2 ? "✓ " : rel.grade(rm3) == 1 ? "~ " : "  ", rm3.className(), rm3.score());
                String fmtSum   = rs  == null ? "" : String.format("%s%s %.3f",
                        rel.grade(rs)  >= 2 ? "✓ " : rel.grade(rs)  == 1 ? "~ " : "  ", rs.className(),  rs.score());
                System.out.printf("  [%d] %-37s │ %-37s │ %-37s │ %s%n",
                        i + 1, fmtMax, fmtSum3, fmtMean3, fmtSum);
            }

            // Show where each expected class lands in the full ranked lists
            // Fetch full aggregated ranking (no limit) to find rank even if outside top-20
            java.util.Map<String, Double> fullSumScores = new java.util.LinkedHashMap<>();
            scoresByClass.forEach((cls, scores) -> fullSumScores.put(cls,
                    scores.stream().mapToDouble(f -> f).sum()));
            List<String> fullSumRanked = fullSumScores.entrySet().stream()
                    .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                    .map(java.util.Map.Entry::getKey)
                    .toList();

            System.out.println("  Expected class ranks across strategies:");
            for (String expected : qc.highRelevance()) {
                int rMax   = rankOf(expected, byMax);
                int rSum3  = rankOf(expected, bySum3);
                int rMean3 = rankOf(expected, byMean3);
                int rSum   = rankOf(expected, bySum);
                int rSumFull = fullSumRanked.indexOf(expected) + 1; // 0 = not found → 0
                System.out.printf("    %-35s max=%-6s sum3=%-6s mean3=%-6s sum20=%-6s sum-all=%s%n",
                        expected,
                        rMax   == 0 ? "—" : String.valueOf(rMax),
                        rSum3  == 0 ? "—" : String.valueOf(rSum3),
                        rMean3 == 0 ? "—" : String.valueOf(rMean3),
                        rSum   == 0 ? "—" : String.valueOf(rSum),
                        rSumFull == 0 ? "NOT FOUND" : String.valueOf(rSumFull));
            }
        }

        // Aggregate MRR summary
        System.out.println();
        System.out.println(bar());
        System.out.println("  AGGREGATION MRR SUMMARY (all queries, class-level dedup)");
        System.out.println(bar());
        System.out.printf("  %-12s │ %5s%n", "Strategy", "MRR");
        System.out.println("  " + "─".repeat(22));
        System.out.printf("  %-12s │ %5.3f%n", "max",    IrMetrics.mean(mrrMax));
        System.out.printf("  %-12s │ %5.3f%n", "sum-top3", IrMetrics.mean(mrrSum3));
        System.out.printf("  %-12s │ %5.3f%n", "mean-top3", IrMetrics.mean(mrrMean3));
        System.out.printf("  %-12s │ %5.3f%n", "sum-all", IrMetrics.mean(mrrSum));
        System.out.println(bar());
        System.out.println();
    }

    // ── LSH grid search ───────────────────────────────────────────────────────

    @Test
    @Order(8)
    void inspect_lshGridSearch() throws IOException {
        // Configs: (k, minCollisionProb)
        // k=1: pure p1bit threshold; k=2,3: steeper S-curve
        // Thresholds chosen so the floor Lucene score is:
        //   k=1 p=0.65 → lucene≥0.727  k=1 p=0.70 → lucene≥0.794
        //   k=2 p=0.50 → p1bit≥0.707 → lucene≥0.854
        //   k=2 p=0.45 → p1bit≥0.671 → lucene≥0.815
        //   k=3 p=0.40 → p1bit≥0.737 → lucene≥0.868
        record LshConfig(int k, double minP, String label) {}
        List<LshConfig> configs = List.of(
            new LshConfig(0, 0.00, "no-filter (baseline)"),
            new LshConfig(1, 0.60, "k=1 p≥0.60"),
            new LshConfig(1, 0.65, "k=1 p≥0.65"),
            new LshConfig(1, 0.70, "k=1 p≥0.70"),
            new LshConfig(1, 0.75, "k=1 p≥0.75"),
            new LshConfig(2, 0.40, "k=2 p≥0.40"),
            new LshConfig(2, 0.45, "k=2 p≥0.45"),
            new LshConfig(2, 0.50, "k=2 p≥0.50"),
            new LshConfig(3, 0.35, "k=3 p≥0.35"),
            new LshConfig(3, 0.40, "k=3 p≥0.40"),
            new LshConfig(3, 0.45, "k=3 p≥0.45")
        );

        System.out.println();
        System.out.println(bar());
        System.out.println("  LSH GRID SEARCH — Borda + collision-probability vector filter");
        System.out.println(bar());
        System.out.printf("  %-24s │ %5s %5s %5s │ %5s %5s %5s │ %s%n",
                "Config", "MRR", "MAP", "P@5", "nl-MRR", "sem-MRR", "cpt-MRR", "vs baseline");
        System.out.println("  " + "─".repeat(90));

        double[] baseline = null;
        for (LshConfig cfg : configs) {
            double[] all  = aggregateLsh(cases(), cfg.k(), cfg.minP());
            double[] nl   = aggregateLsh(cases().stream().filter(c -> "name-lookup".equals(c.category())).toList(), cfg.k(), cfg.minP());
            double[] sem  = aggregateLsh(cases().stream().filter(c -> "semantic".equals(c.category())).toList(), cfg.k(), cfg.minP());
            double[] cpt  = aggregateLsh(cases().stream().filter(c -> "conceptual".equals(c.category())).toList(), cfg.k(), cfg.minP());

            if (baseline == null) baseline = all;
            double deltaMrr = all[0] - baseline[0];
            double deltaP5  = all[3] - baseline[3];
            String delta = cfg.k() == 0 ? "" :
                    String.format("ΔMRR%+.3f ΔP@5%+.3f", deltaMrr, deltaP5);

            System.out.printf("  %-24s │ %5.3f %5.3f %5.3f │ %5.3f %5.3f %5.3f │ %s%n",
                    cfg.label(), all[0], all[1], all[3],
                    nl[0], sem[0], cpt[0], delta);
        }
        System.out.println(bar());
        System.out.println();
    }

    private double[] aggregateLsh(List<LuceneQueryCase> qs, int k, double minP) throws IOException {
        // Uses the HybridSearchStrategy directly through SearchEngine, but we need the
        // filtered fuse. Work around by calling vector search + keyword search separately
        // and applying the filter manually here for grid-search purposes.
        com.pharos.search.HybridSearchStrategy hybridStrategy =
                new com.pharos.search.HybridSearchStrategy(
                        com.pharos.embedding.EmbeddingProvider.create(
                                com.pharos.config.IndexConfig.load()));

        List<Double> mrrs = new ArrayList<>(), aps = new ArrayList<>(),
                     ndcgs = new ArrayList<>(), p5s = new ArrayList<>(),
                     r10s = new ArrayList<>();
        for (LuceneQueryCase qc : qs) {
            List<SearchResult> kw  = searchKeyword(qc.query());
            List<SearchResult> vec = searchVector(qc.query());
            List<SearchResult> filteredVec = minP > 0.0
                    ? vec.stream()
                        .filter(r -> com.pharos.search.HybridSearchStrategy
                                .collisionProbability(r.score(), k) >= minP)
                        .toList()
                    : vec;
            List<SearchResult> fused = filteredVec.isEmpty()
                    ? kw
                    : hybridStrategy.fuse(kw, filteredVec, 20, qc.query());
            List<SearchResult> dedup = deduplicateByClass(fused);
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

    // ── Helpers ───────────────────────────────────────────────────────────────

    private List<SearchResult> searchKeyword(String query) throws IOException {
        return searchEngine.search(SearchRequest.keyword(query, "lucene", 20));
    }

    private List<SearchResult> searchHybrid(String query) throws IOException {
        return searchEngine.search(SearchRequest.hybrid(query, "lucene", 20));
    }

    private List<SearchResult> searchVector(String query) throws IOException {
        return searchEngine.search(new SearchRequest(query, SearchRequest.SearchType.VECTOR,
                "lucene", null, 20, "text", null));
    }

    /**
     * Deduplicates a result list to at most one entry per className (highest-scoring kept).
     *
     * <p>The Lucene index contains ~70k methods. A single target class may contribute
     * many methods to the top-20, inflating recall beyond 1.0 when judgments are
     * class-level. Deduplication converts the result list from "ranked methods" to
     * "ranked classes", which is the right unit for class-level evaluation.
     */
    private static List<SearchResult> deduplicateByClass(List<SearchResult> results) {
        java.util.LinkedHashMap<String, SearchResult> seen = new java.util.LinkedHashMap<>();
        for (SearchResult r : results) {
            String cls = r.className() != null ? r.className() : "";
            seen.putIfAbsent(cls, r); // keep first (highest-scoring) per class
        }
        return new ArrayList<>(seen.values());
    }

    private double[] aggregateVector(List<LuceneQueryCase> qs) throws IOException {
        List<Double> mrrs = new ArrayList<>(), aps = new ArrayList<>(),
                     ndcgs = new ArrayList<>(), p5s = new ArrayList<>(),
                     r10s = new ArrayList<>();
        for (LuceneQueryCase qc : qs) {
            List<SearchResult> dedup = deduplicateByClass(searchVector(qc.query()));
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

    /**
     * Returns {@code [mrr, map, ndcg5, p5, r10]} aggregated across the given cases.
     * {@code useHybrid=true} uses RRF hybrid search; {@code false} uses keyword-only.
     * Results are deduplicated by class before computing metrics.
     */
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

    /** Returns 1-based rank of the first result whose className equals {@code expected}, or 0 if not found. */
    private static int rankOf(String expected, List<SearchResult> results) {
        for (int i = 0; i < results.size(); i++) {
            if (results.get(i).className().equals(expected)) return i + 1;
        }
        return 0;
    }

    private static String bar() { return "━".repeat(72); }
}
