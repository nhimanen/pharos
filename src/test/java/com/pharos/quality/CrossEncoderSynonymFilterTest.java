package com.pharos.quality;

import com.pharos.analysis.ConceptMiner;
import com.pharos.config.IndexConfig;
import com.pharos.embedding.CrossEncoderProvider;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Evaluates a cross-encoder (Xenova/ms-marco-MiniLM-L-6-v2) as a post-filter
 * over the mined synonym rules produced by {@link ConceptMiner#mineAll}.
 *
 * <h3>Test set</h3>
 * 80 hand-labeled (trigger, className) pairs in four quality bands:
 * <ul>
 *   <li><b>A — clear positives</b> (label=1.0): trigger precisely describes the class
 *   <li><b>B — plausible positives</b> (label=0.75): correct but indirect connection
 *   <li><b>C — hard negatives</b> (label=0.25): related domain, wrong class
 *   <li><b>D — clear negatives</b> (label=0.0): garbage bigrams or completely wrong
 * </ul>
 *
 * <h3>Metrics</h3>
 * AUC-ROC, precision-recall curve, and the threshold that maximises F1 on the
 * test set. The optimal threshold is then applied to the full mined rule set to
 * show how many rules survive.
 *
 * <p><b>Prerequisite:</b> Lucene indexed: {@code pharos index /path/to/lucene --project lucene}
 * <br><b>Tag:</b> {@code quality} — run with {@code -DexcludedGroups=}
 */
@Tag("quality")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CrossEncoderSynonymFilterTest {

    // ── Test pairs ─────────────────────────────────────────────────────────────

    record TestPair(String trigger, String className, double label, String band) {}

    static final List<TestPair> TEST_PAIRS = List.of(

        // ── Band A: clear positives (label=1.0) ───────────────────────────────
        new TestPair("skiplist",               "MultiLevelSkipListReader",             1.0, "A"),
        new TestPair("weakand",                "WANDScorer",                           1.0, "A"),
        new TestPair("finitestatemachine",     "FST",                                  1.0, "A"),
        new TestPair("blockmax",               "BlockMaxConjunctionScorer",            1.0, "A"),
        new TestPair("synonymexpansion",       "SynonymGraphFilter",                   1.0, "A"),
        new TestPair("compoundword",           "DictionaryCompoundWordTokenFilter",    1.0, "A"),
        new TestPair("vectorspace",            "TFIDFSimilarity",                      1.0, "A"),
        new TestPair("wordstemmer",            "PorterStemFilter",                     1.0, "A"),
        new TestPair("kdtree",                 "BKDWriter",                            1.0, "A"),
        new TestPair("smallworld",             "HnswGraph",                            1.0, "A"),
        new TestPair("taxonomyindex",          "TaxonomyWriter",                       1.0, "A"),
        new TestPair("facettaxonomy",          "DirectoryTaxonomyWriter",              1.0, "A"),
        new TestPair("functionboost",          "FunctionScoreQuery",                   1.0, "A"),
        new TestPair("bestfield",              "DisjunctionMaxQuery",                  1.0, "A"),
        new TestPair("orderedproximity",       "SpanNearQuery",                        1.0, "A"),
        new TestPair("languagemodeling",       "LMSimilarity",                         1.0, "A"),
        new TestPair("accentstrip",            "ASCIIFoldingFilter",                   1.0, "A"),
        new TestPair("softdeleted",            "SoftDeletesRetentionMergePolicy",      1.0, "A"),
        new TestPair("hierarchicalnavigable",  "HnswGraph",                            1.0, "A"),
        new TestPair("partofspeech",           "POS",                                  1.0, "A"),

        // ── Band B: plausible positives (label=0.75) ─────────────────────────
        new TestPair("impactscoring",          "ImpactsDISI",                          0.75, "B"),
        new TestPair("deltapack",              "DeltaPackedLongValues",                0.75, "B"),
        new TestPair("memorymapped",           "MMapDirectory",                        0.75, "B"),
        new TestPair("segmentflush",           "DocumentsWriterFlushControl",          0.75, "B"),
        new TestPair("backgroundmerge",        "ConcurrentMergeScheduler",             0.75, "B"),
        new TestPair("indexgeneration",        "IndexCommit",                          0.75, "B"),
        new TestPair("charfolding",            "ASCIIFoldingFilter",                   0.75, "B"),
        new TestPair("sparsebits",             "SparseFixedBitSet",                    0.75, "B"),
        new TestPair("topkdocument",           "TopScoreDocCollector",                 0.75, "B"),
        new TestPair("queryboosting",          "BoostQuery",                           0.75, "B"),
        new TestPair("levelmerge",             "LogMergePolicy",                       0.75, "B"),
        new TestPair("offheapmmap",            "MemorySegmentIndexInput",              0.75, "B"),
        new TestPair("perdocnumeric",          "NumericDocValues",                     0.75, "B"),
        new TestPair("columnstore",            "DocValues",                            0.75, "B"),
        new TestPair("bpreorder",              "BPIndexReorderer",                     0.75, "B"),
        new TestPair("decompound",             "DictionaryCompoundWordTokenFilter",    0.75, "B"),
        new TestPair("roaringdocset",          "RoaringDocIdSet",                      0.75, "B"),
        new TestPair("logpolicy",              "LogMergePolicy",                       0.75, "B"),
        new TestPair("docvalueswrite",         "DocValuesConsumer",                    0.75, "B"),
        new TestPair("loglogistic",            "DistributionLL",                       0.75, "B"),

        // ── Band C: hard negatives (label=0.25) ───────────────────────────────
        new TestPair("skiplist",               "BKDWriter",                            0.25, "C"),
        new TestPair("weakand",                "MaxScoreCache",                        0.25, "C"),
        new TestPair("vectorspace",            "BM25Similarity",                       0.25, "C"),
        new TestPair("blockmax",               "WANDScorer",                           0.25, "C"),
        new TestPair("compoundword",           "HyphenationCompoundWordTokenFilter",   0.25, "C"),
        new TestPair("languagemodeling",       "BM25Similarity",                       0.25, "C"),
        new TestPair("facettaxonomy",          "TaxonomyWriter",                       0.25, "C"),
        new TestPair("kdtree",                 "BKDReader",                            0.25, "C"),
        new TestPair("bestfield",              "FunctionScoreQuery",                   0.25, "C"),
        new TestPair("decompound",             "HyphenationCompoundWordTokenFilter",   0.25, "C"),
        new TestPair("backgroundmerge",        "TieredMergePolicy",                    0.25, "C"),
        new TestPair("orderedproximity",       "PhraseQuery",                          0.25, "C"),
        new TestPair("segmentflush",           "IndexWriter",                          0.25, "C"),
        new TestPair("impactscoring",          "WANDScorer",                           0.25, "C"),
        new TestPair("smallworld",             "HnswGraphBuilder",                     0.25, "C"),
        new TestPair("memorymapped",           "NRTCachingDirectory",                  0.25, "C"),
        new TestPair("synonymexpansion",       "Analyzer",                             0.25, "C"),
        new TestPair("taxonomyindex",          "DirectoryTaxonomyWriter",              0.25, "C"),
        new TestPair("loglogistic",            "LMSimilarity",                         0.25, "C"),
        new TestPair("charfolding",            "LowerCaseFilter",                      0.25, "C"),

        // ── Band D: clear negatives (label=0.0) ───────────────────────────────
        new TestPair("addunpositioned",        "WANDScorer",                           0.0, "D"),
        new TestPair("tailwith",               "WANDScorer",                           0.0, "D"),
        new TestPair("processwhich",           "TFIDFSimilarity",                      0.0, "D"),
        new TestPair("storedsee",              "MultiLevelSkipListReader",             0.0, "D"),
        new TestPair("codeupdate",             "IndexWriter",                          0.0, "D"),
        new TestPair("scoring",                "HnswGraph",                            0.0, "D"),
        new TestPair("document",               "MultiLevelSkipListReader",             0.0, "D"),
        new TestPair("build",                  "BKDWriter",                            0.0, "D"),
        new TestPair("returns",                "WANDScorer",                           0.0, "D"),
        new TestPair("skiplist",               "FunctionScoreQuery",                   0.0, "D"),
        new TestPair("weakand",                "PagedBytes",                           0.0, "D"),
        new TestPair("vectorspace",            "HnswGraph",                            0.0, "D"),
        new TestPair("finitestatemachine",     "BKDWriter",                            0.0, "D"),
        new TestPair("blockmax",               "FST",                                  0.0, "D"),
        new TestPair("synonymexpansion",       "BKDWriter",                            0.0, "D"),
        new TestPair("compoundword",           "WANDScorer",                           0.0, "D"),
        new TestPair("hierarchicalnavigable",  "LMSimilarity",                         0.0, "D"),
        new TestPair("smallworld",             "TFIDFSimilarity",                      0.0, "D"),
        new TestPair("languagemodeling",       "HnswGraph",                            0.0, "D"),
        new TestPair("kdtree",                 "SynonymGraphFilter",                   0.0, "D")
    );

    // ── Infrastructure ─────────────────────────────────────────────────────────

    private static LuceneIndexer    luceneIndexer;
    private static IndexReader      luceneReader;
    private static CrossEncoderProvider ce;

    @BeforeAll
    static void setup() throws Exception {
        IndexConfig config = IndexConfig.load();
        luceneIndexer = new LuceneIndexer(config);
        luceneReader  = luceneIndexer.openMultiReader(List.of("lucene"));
        ce = new CrossEncoderProvider();
    }

    @AfterAll
    static void teardown() throws Exception {
        if (ce            != null) ce.close();
        if (luceneIndexer != null) luceneIndexer.close();
    }

    record ScoredPair(TestPair pair, float ceScore, String description) {}

    // ── Main evaluation test ───────────────────────────────────────────────────

    @Test
    @Order(1)
    void evaluate_cross_encoder_on_test_pairs() throws IOException {
        // Load class descriptions from the index
        Map<String, String> descriptions = loadDescriptions(
                TEST_PAIRS.stream().map(TestPair::className).collect(Collectors.toSet()));

        List<ScoredPair> scored = new ArrayList<>();

        for (TestPair p : TEST_PAIRS) {
            String desc = descriptions.getOrDefault(p.className(), p.className());
            float  s    = ce.score(p.trigger(), p.className() + ": " + desc);
            scored.add(new ScoredPair(p, s, desc));
        }

        // Sort by CE score descending for reporting
        scored.sort(Comparator.comparingDouble(s -> -s.ceScore()));

        // ── Per-pair output ──────────────────────────────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  CROSS-ENCODER SCORES — all 80 test pairs (sorted by score desc)");
        System.out.println(bar());
        System.out.printf("  %-28s → %-38s │ %5s │ %4s │ Band%n",
                "Trigger", "Class", "CE", "Lbl");
        System.out.println("  " + "─".repeat(90));
        for (ScoredPair sp : scored)
            System.out.printf("  %-28s → %-38s │ %5.3f │ %4.2f │ %s%n",
                    sp.pair().trigger(), sp.pair().className(),
                    sp.ceScore(), sp.pair().label(), sp.pair().band());

        // ── AUC & precision-recall ───────────────────────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  THRESHOLD SWEEP — precision / recall / F1");
        System.out.println(bar());
        System.out.printf("  %-8s │ %9s │ %6s │ %6s │ %s%n",
                "Threshold", "Precision", "Recall", "F1", "Rules kept");
        System.out.println("  " + "─".repeat(55));

        // Treat label ≥ 0.5 as positive
        int totalPos = (int) TEST_PAIRS.stream().filter(p -> p.label() >= 0.5).count();
        double bestF1 = -1; float bestThreshold = 0.5f;

        for (float t : new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f}) {
            long tp = scored.stream().filter(s -> s.ceScore() >= t && s.pair().label() >= 0.5).count();
            long fp = scored.stream().filter(s -> s.ceScore() >= t && s.pair().label()  < 0.5).count();
            long kept = tp + fp;
            double prec = kept > 0 ? (double) tp / kept : 0;
            double rec  = totalPos > 0 ? (double) tp / totalPos : 0;
            double f1   = (prec + rec) > 0 ? 2 * prec * rec / (prec + rec) : 0;
            if (f1 > bestF1) { bestF1 = f1; bestThreshold = t; }
            System.out.printf("  %-8.1f │ %9.1f%% │ %5.1f%% │ %5.3f │ %d / %d%n",
                    t, prec * 100, rec * 100, f1, kept, TEST_PAIRS.size());
        }

        double auc = computeAuc(scored);
        System.out.println("  " + "─".repeat(55));
        System.out.printf("  AUC-ROC: %.3f  |  Best threshold: %.1f  (F1=%.3f)%n",
                auc, bestThreshold, bestF1);
        System.out.println(bar());
        System.out.println();

        // ── Apply optimal threshold to full mined rules ──────────────────────
        System.out.println(bar());
        System.out.printf("  APPLYING THRESHOLD=%.1f TO FULL MINED RULES%n", bestThreshold);
        System.out.println(bar());

        ConceptMiner miner   = new ConceptMiner();
        Map<String, Set<String>> rules = miner.mineAll(luceneReader);
        int totalRules = rules.values().stream().mapToInt(Set::size).sum();

        int kept = 0, removed = 0;
        final float threshold = bestThreshold;
        for (Map.Entry<String, Set<String>> e : rules.entrySet()) {
            String term = e.getKey();
            for (String cls : e.getValue()) {
                String desc = descriptions.getOrDefault(cls, cls);
                float  s    = ce.score(term, cls + ": " + desc);
                if (s >= threshold) kept++;
                else                removed++;
            }
        }

        System.out.printf("  Total rules before:  %,d%n", totalRules);
        System.out.printf("  Rules kept (≥%.1f):   %,d (%.1f%%)%n",
                threshold, kept, 100.0 * kept / totalRules);
        System.out.printf("  Rules removed (<%.1f): %,d (%.1f%%)%n",
                threshold, removed, 100.0 * removed / totalRules);
        System.out.println(bar());
        System.out.println();
    }

    // ── Band-level summary ─────────────────────────────────────────────────────

    @Test
    @Order(2)
    void band_score_summary() throws IOException {
        Map<String, String> descriptions = loadDescriptions(
                TEST_PAIRS.stream().map(TestPair::className).collect(Collectors.toSet()));

        Map<String, List<Float>> bandScores = new TreeMap<>();
        for (TestPair p : TEST_PAIRS) {
            String desc = descriptions.getOrDefault(p.className(), p.className());
            float s = ce.score(p.trigger(), p.className() + ": " + desc);
            bandScores.computeIfAbsent(p.band(), k -> new ArrayList<>()).add(s);
        }

        System.out.println();
        System.out.println(bar());
        System.out.println("  BAND SCORE SUMMARY");
        System.out.println(bar());
        System.out.printf("  %-6s │ %5s │ %5s │ %5s │ %5s │ %s%n",
                "Band", "Min", "Mean", "Med", "Max", "Description");
        System.out.println("  " + "─".repeat(65));

        Map<String, String> bandDesc = Map.of(
                "A", "clear positives (human=1.0)",
                "B", "plausible positives (human=0.75)",
                "C", "hard negatives (human=0.25)",
                "D", "clear negatives (human=0.0)");

        for (Map.Entry<String, List<Float>> e : bandScores.entrySet()) {
            List<Float> scores = e.getValue();
            scores.sort(Comparator.naturalOrder());
            double min  = scores.get(0);
            double max  = scores.get(scores.size() - 1);
            double mean = scores.stream().mapToDouble(f -> f).average().orElse(0);
            double med  = scores.get(scores.size() / 2);
            System.out.printf("  %-6s │ %5.3f │ %5.3f │ %5.3f │ %5.3f │ %s%n",
                    e.getKey(), min, mean, med, max, bandDesc.get(e.getKey()));
        }
        System.out.println(bar());
        System.out.println();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private Map<String, String> loadDescriptions(Set<String> classNames) throws IOException {
        IndexSearcher searcher = new IndexSearcher(luceneReader);
        StoredFields  sf       = searcher.storedFields();
        Map<String, String> result = new HashMap<>();

        for (String cls : classNames) {
            TopDocs hits = searcher.search(
                    new TermQuery(new Term(DocumentMapper.F_CLASS_NAME, cls.toLowerCase())),
                    5);
            for (var sd : hits.scoreDocs) {
                var doc = sf.document(sd.doc);
                if (!"lucene".equals(doc.get(DocumentMapper.F_PROJECT))) continue;
                if (!"class".equals(doc.get(DocumentMapper.F_DOC_TYPE))) continue;
                String javadoc = doc.get(DocumentMapper.F_JAVADOC);
                if (javadoc != null && !javadoc.isBlank()) {
                    result.put(cls, firstSentence(javadoc));
                    break;
                }
            }
            result.putIfAbsent(cls, cls); // fallback to class name
        }
        return result;
    }

    private static String firstSentence(String javadoc) {
        String clean = javadoc.replaceAll("(?m)^\\s*\\*\\s?", " ").trim();
        clean = clean.replaceAll("\\{@\\w+\\s+([^}]+)\\}", "$1");
        for (int i = 0; i < clean.length(); i++) {
            char c = clean.charAt(i);
            if ((c == '.' || c == '!') &&
                    (i + 1 >= clean.length() || Character.isWhitespace(clean.charAt(i + 1))))
                return clean.substring(0, Math.min(i + 1, 200));
        }
        return clean.substring(0, Math.min(clean.length(), 200));
    }

    /** Area Under the ROC Curve via trapezoidal rule. */
    private static double computeAuc(List<ScoredPair> scored) {
        // Already sorted by score desc; walk thresholds
        record Pt(double fpr, double tpr) {}
        int pos = (int) scored.stream().filter(s -> s.pair().label() >= 0.5).count();
        int neg = scored.size() - pos;
        if (pos == 0 || neg == 0) return 0.5;

        List<Pt> curve = new ArrayList<>();
        curve.add(new Pt(0, 0));
        long tp = 0, fp = 0;
        for (var sp : scored) {
            if (sp.pair().label() >= 0.5) tp++;
            else                          fp++;
            curve.add(new Pt((double) fp / neg, (double) tp / pos));
        }
        curve.add(new Pt(1, 1));

        double auc = 0;
        for (int i = 1; i < curve.size(); i++)
            auc += (curve.get(i).fpr() - curve.get(i-1).fpr())
                 * (curve.get(i).tpr() + curve.get(i-1).tpr()) / 2.0;
        return auc;
    }

    private static String bar() { return "━".repeat(72); }
}
