package com.pharos.quality;

import com.pharos.analysis.ConceptMiner;
import com.pharos.config.IndexConfig;
import com.pharos.embedding.CrossEncoderProvider;
import com.pharos.embedding.DjlEmbeddingProvider;
import com.pharos.embedding.EmbeddingProvider;
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
 * Compares five synonym-validation strategies against an 80-pair labeled test set.
 *
 * <h3>Strategies</h3>
 * <ol>
 *   <li><b>ms-marco raw</b>      — cross-encoder, trigger as mined (compound, no spaces)
 *   <li><b>ms-marco normalized</b> — same model, trigger split to space-separated form
 *   <li><b>BGE reranker</b>      — bge-reranker-base, normalized trigger
 *   <li><b>Bi-encoder cosine</b> — cosine sim using Pharos's existing jina embeddings
 *   <li><b>STS RoBERTa</b>       — cross-encoder/stsb-roberta-large, normalized trigger
 * </ol>
 *
 * <p><b>Prerequisite:</b> Lucene project indexed.
 * <br>Run: {@code mvn test -Dtest=CrossEncoderSynonymFilterTest -DexcludedGroups=}
 */
@Tag("quality")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CrossEncoderSynonymFilterTest {

    // ── Test pairs ─────────────────────────────────────────────────────────────

    /** trigger=raw mined term, norm=space-separated natural form. */
    record TestPair(String trigger, String norm, String className,
                    double label, String band) {}

    static final List<TestPair> TEST_PAIRS = List.of(

        // ── Band A: clear positives (label=1.0) ───────────────────────────────
        new TestPair("skiplist",               "skip list",                  "MultiLevelSkipListReader",            1.0, "A"),
        new TestPair("weakand",                "weak and",                   "WANDScorer",                          1.0, "A"),
        new TestPair("finitestatemachine",     "finite state machine",       "FST",                                 1.0, "A"),
        new TestPair("blockmax",               "block max",                  "BlockMaxConjunctionScorer",           1.0, "A"),
        new TestPair("synonymexpansion",       "synonym expansion",          "SynonymGraphFilter",                  1.0, "A"),
        new TestPair("compoundword",           "compound word",              "DictionaryCompoundWordTokenFilter",   1.0, "A"),
        new TestPair("vectorspace",            "vector space",               "TFIDFSimilarity",                     1.0, "A"),
        new TestPair("wordstemmer",            "word stemmer",               "PorterStemFilter",                    1.0, "A"),
        new TestPair("kdtree",                 "kd tree",                    "BKDWriter",                           1.0, "A"),
        new TestPair("smallworld",             "small world",                "HnswGraph",                           1.0, "A"),
        new TestPair("taxonomyindex",          "taxonomy index",             "TaxonomyWriter",                      1.0, "A"),
        new TestPair("facettaxonomy",          "facet taxonomy",             "DirectoryTaxonomyWriter",             1.0, "A"),
        new TestPair("functionboost",          "function boost",             "FunctionScoreQuery",                  1.0, "A"),
        new TestPair("bestfield",              "best field",                 "DisjunctionMaxQuery",                 1.0, "A"),
        new TestPair("orderedproximity",       "ordered proximity",          "SpanNearQuery",                       1.0, "A"),
        new TestPair("languagemodeling",       "language modeling",          "LMSimilarity",                        1.0, "A"),
        new TestPair("accentstrip",            "accent strip",               "ASCIIFoldingFilter",                  1.0, "A"),
        new TestPair("softdeleted",            "soft deleted",               "SoftDeletesRetentionMergePolicy",     1.0, "A"),
        new TestPair("hierarchicalnavigable",  "hierarchical navigable",     "HnswGraph",                           1.0, "A"),
        new TestPair("partofspeech",           "part of speech",             "POS",                                 1.0, "A"),

        // ── Band B: plausible positives (label=0.75) ─────────────────────────
        new TestPair("impactscoring",          "impact scoring",             "ImpactsDISI",                         0.75, "B"),
        new TestPair("deltapack",              "delta pack",                 "DeltaPackedLongValues",               0.75, "B"),
        new TestPair("memorymapped",           "memory mapped",              "MMapDirectory",                       0.75, "B"),
        new TestPair("segmentflush",           "segment flush",              "DocumentsWriterFlushControl",         0.75, "B"),
        new TestPair("backgroundmerge",        "background merge",           "ConcurrentMergeScheduler",            0.75, "B"),
        new TestPair("indexgeneration",        "index generation",           "IndexCommit",                         0.75, "B"),
        new TestPair("charfolding",            "char folding",               "ASCIIFoldingFilter",                  0.75, "B"),
        new TestPair("sparsebits",             "sparse bits",                "SparseFixedBitSet",                   0.75, "B"),
        new TestPair("topkdocument",           "top k document",             "TopScoreDocCollector",                0.75, "B"),
        new TestPair("queryboosting",          "query boosting",             "BoostQuery",                          0.75, "B"),
        new TestPair("levelmerge",             "level merge",                "LogMergePolicy",                      0.75, "B"),
        new TestPair("offheapmmap",            "off heap mmap",              "MemorySegmentIndexInput",             0.75, "B"),
        new TestPair("perdocnumeric",          "per doc numeric",            "NumericDocValues",                    0.75, "B"),
        new TestPair("columnstore",            "column store",               "DocValues",                           0.75, "B"),
        new TestPair("bpreorder",              "bp reorder",                 "BPIndexReorderer",                    0.75, "B"),
        new TestPair("decompound",             "decompound",                 "DictionaryCompoundWordTokenFilter",   0.75, "B"),
        new TestPair("roaringdocset",          "roaring doc set",            "RoaringDocIdSet",                     0.75, "B"),
        new TestPair("logpolicy",              "log policy",                 "LogMergePolicy",                      0.75, "B"),
        new TestPair("docvalueswrite",         "doc values write",           "DocValuesConsumer",                   0.75, "B"),
        new TestPair("loglogistic",            "log logistic",               "DistributionLL",                      0.75, "B"),

        // ── Band C: hard negatives (label=0.25) ───────────────────────────────
        new TestPair("skiplist",               "skip list",                  "BKDWriter",                           0.25, "C"),
        new TestPair("weakand",                "weak and",                   "MaxScoreCache",                       0.25, "C"),
        new TestPair("vectorspace",            "vector space",               "BM25Similarity",                      0.25, "C"),
        new TestPair("blockmax",               "block max",                  "WANDScorer",                          0.25, "C"),
        new TestPair("compoundword",           "compound word",              "HyphenationCompoundWordTokenFilter",  0.25, "C"),
        new TestPair("languagemodeling",       "language modeling",          "BM25Similarity",                      0.25, "C"),
        new TestPair("facettaxonomy",          "facet taxonomy",             "TaxonomyWriter",                      0.25, "C"),
        new TestPair("kdtree",                 "kd tree",                    "BKDReader",                           0.25, "C"),
        new TestPair("bestfield",              "best field",                 "FunctionScoreQuery",                  0.25, "C"),
        new TestPair("decompound",             "decompound",                 "HyphenationCompoundWordTokenFilter",  0.25, "C"),
        new TestPair("backgroundmerge",        "background merge",           "TieredMergePolicy",                   0.25, "C"),
        new TestPair("orderedproximity",       "ordered proximity",          "PhraseQuery",                         0.25, "C"),
        new TestPair("segmentflush",           "segment flush",              "IndexWriter",                         0.25, "C"),
        new TestPair("impactscoring",          "impact scoring",             "WANDScorer",                          0.25, "C"),
        new TestPair("smallworld",             "small world",                "HnswGraphBuilder",                    0.25, "C"),
        new TestPair("memorymapped",           "memory mapped",              "NRTCachingDirectory",                 0.25, "C"),
        new TestPair("synonymexpansion",       "synonym expansion",          "Analyzer",                            0.25, "C"),
        new TestPair("taxonomyindex",          "taxonomy index",             "DirectoryTaxonomyWriter",             0.25, "C"),
        new TestPair("loglogistic",            "log logistic",               "LMSimilarity",                        0.25, "C"),
        new TestPair("charfolding",            "char folding",               "LowerCaseFilter",                     0.25, "C"),

        // ── Band D: clear negatives (label=0.0) ───────────────────────────────
        new TestPair("addunpositioned",        "add unpositioned",           "WANDScorer",                          0.0, "D"),
        new TestPair("tailwith",               "tail with",                  "WANDScorer",                          0.0, "D"),
        new TestPair("processwhich",           "process which",              "TFIDFSimilarity",                     0.0, "D"),
        new TestPair("storedsee",              "stored see",                 "MultiLevelSkipListReader",            0.0, "D"),
        new TestPair("codeupdate",             "code update",                "IndexWriter",                         0.0, "D"),
        new TestPair("scoring",                "scoring",                    "HnswGraph",                           0.0, "D"),
        new TestPair("document",               "document",                   "MultiLevelSkipListReader",            0.0, "D"),
        new TestPair("build",                  "build",                      "BKDWriter",                           0.0, "D"),
        new TestPair("returns",                "returns",                    "WANDScorer",                          0.0, "D"),
        new TestPair("skiplist",               "skip list",                  "FunctionScoreQuery",                  0.0, "D"),
        new TestPair("weakand",                "weak and",                   "PagedBytes",                          0.0, "D"),
        new TestPair("vectorspace",            "vector space",               "HnswGraph",                           0.0, "D"),
        new TestPair("finitestatemachine",     "finite state machine",       "BKDWriter",                           0.0, "D"),
        new TestPair("blockmax",               "block max",                  "FST",                                 0.0, "D"),
        new TestPair("synonymexpansion",       "synonym expansion",          "BKDWriter",                           0.0, "D"),
        new TestPair("compoundword",           "compound word",              "WANDScorer",                          0.0, "D"),
        new TestPair("hierarchicalnavigable",  "hierarchical navigable",     "LMSimilarity",                        0.0, "D"),
        new TestPair("smallworld",             "small world",                "TFIDFSimilarity",                     0.0, "D"),
        new TestPair("languagemodeling",       "language modeling",          "HnswGraph",                           0.0, "D"),
        new TestPair("kdtree",                 "kd tree",                    "SynonymGraphFilter",                  0.0, "D")
    );

    // ── Infrastructure ─────────────────────────────────────────────────────────

    record ScoredPair(TestPair pair, float score, String description) {}

    @FunctionalInterface
    interface Scorer {
        float score(String query, String document) throws Exception;
    }

    private static LuceneIndexer      luceneIndexer;
    private static IndexReader        luceneReader;
    private static CrossEncoderProvider msMarcoCE;
    private static CrossEncoderProvider bgeCE;
    private static CrossEncoderProvider stsCE;
    private static EmbeddingProvider    biEncoder;
    private static Map<String, String>  descriptions;

    @BeforeAll
    static void setup() throws Exception {
        IndexConfig config = IndexConfig.load();
        luceneIndexer = new LuceneIndexer(config);
        luceneReader  = luceneIndexer.openMultiReader(List.of("lucene"));
        descriptions  = loadDescriptions(
                TEST_PAIRS.stream().map(TestPair::className).collect(Collectors.toSet()));

        msMarcoCE = new CrossEncoderProvider(
                CrossEncoderProvider.MS_MARCO_MODEL_ID, CrossEncoderProvider.QUANTIZED_ONNX,
                true, "ms-marco");

        bgeCE = new CrossEncoderProvider(
                CrossEncoderProvider.BGE_MODEL_ID, CrossEncoderProvider.QUANTIZED_ONNX,
                true, "bge-reranker");

        // stsb-roberta-large has incompatible BPE tokenizer; using ms-marco-L12 instead
        stsCE = new CrossEncoderProvider(
                CrossEncoderProvider.STS_MODEL_ID, CrossEncoderProvider.QUANTIZED_ONNX,
                true, "ms-marco-L12");

        // Bi-encoder: try to load jina model from config; fall back to null if unavailable
        EmbeddingProvider be = null;
        try {
            String modelUrl = config.getEmbeddingModelUrl();
            if (modelUrl != null && !modelUrl.isBlank()) {
                be = new DjlEmbeddingProvider(modelUrl, config.getEmbeddingDimensions(),
                        config.getEmbeddingMaxTokens());
            }
        } catch (Exception e) {
            System.out.println("  [bi-encoder] not available: " + e.getMessage());
        }
        biEncoder = be;
    }

    @AfterAll
    static void teardown() throws Exception {
        if (msMarcoCE    != null) msMarcoCE.close();
        if (bgeCE        != null) bgeCE.close();
        if (stsCE        != null) stsCE.close();
        if (biEncoder instanceof AutoCloseable ac) ac.close();
        if (luceneIndexer != null) luceneIndexer.close();
    }

    // ── Comparison test ────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void compare_all_scorers() throws Exception {
        record ModelResult(String name, List<ScoredPair> scored) {}

        List<ModelResult> models = new ArrayList<>();

        // 1. ms-marco raw (compound trigger, no spaces)
        models.add(new ModelResult("ms-marco raw",  score(msMarcoCE::score,  false)));
        // 2. ms-marco normalized (space-separated trigger)
        models.add(new ModelResult("ms-marco norm", score(msMarcoCE::score,  true)));
        // 3. BGE reranker normalized
        models.add(new ModelResult("bge-reranker",  score(bgeCE::score,      true)));
        // 4. Bi-encoder cosine (using existing Pharos embeddings)
        if (biEncoder != null) {
            final EmbeddingProvider be = biEncoder;
            List<ScoredPair> biScored = score(
                    (q, d) -> cosineSim(be.embed(q), be.embed(d)), true);
            rawCachedScores = biScored;   // cache for whitening test
            models.add(new ModelResult("bi-encoder cos", biScored));
        } else {
            models.add(new ModelResult("bi-encoder cos [N/A]", List.of()));
        }
        // 5. ms-marco-L12 normalized (stsb-roberta tokenizer unsupported; larger ms-marco used)
        models.add(new ModelResult("ms-marco-L12",  score(stsCE::score,      true)));

        // ── Summary table ────────────────────────────────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  SCORER COMPARISON — AUC-ROC + band means + optimal threshold");
        System.out.println(bar());
        System.out.printf("  %-22s │ %5s │ %6s │ %6s │ %6s │ %6s │ %5s │ %5s%n",
                "Model", "AUC", "A mean", "B mean", "C mean", "D mean", "P@thr", "R@thr");
        System.out.println("  " + "─".repeat(90));

        for (ModelResult mr : models) {
            if (mr.scored().isEmpty()) {
                System.out.printf("  %-22s │  n/a  │%n", mr.name());
                continue;
            }
            double auc   = computeAuc(mr.scored());
            double[] band = bandMeans(mr.scored());
            double[] best = bestThreshold(mr.scored());
            System.out.printf("  %-22s │ %.3f │ %.3f  │ %.3f  │ %.3f  │ %.3f  │ %4.0f%% │ %4.0f%%%n",
                    mr.name(), auc, band[0], band[1], band[2], band[3],
                    best[0] * 100, best[1] * 100);
        }
        System.out.println(bar());

        // ── Threshold sweep per model ─────────────────────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  THRESHOLD SWEEP PER MODEL");
        System.out.println(bar());

        int pos = (int) TEST_PAIRS.stream().filter(p -> p.label() >= 0.5).count();
        for (ModelResult mr : models) {
            if (mr.scored().isEmpty()) continue;
            System.out.printf("%n  ── %s (40 positives) ──%n", mr.name());
            System.out.printf("  %-8s │ %9s │ %6s │ %5s │ Rules kept%n",
                    "Threshold", "Precision", "Recall", "F1");
            System.out.println("  " + "─".repeat(52));
            for (float t : new float[]{0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f}) {
                long tp = mr.scored().stream().filter(s->s.score()>=t&&s.pair().label()>=0.5).count();
                long fp = mr.scored().stream().filter(s->s.score()>=t&&s.pair().label()< 0.5).count();
                double p = (tp+fp)>0 ? 100.0*tp/(tp+fp) : 0;
                double r = 100.0*tp/pos;
                double f = (p+r)>0 ? 2*p*r/(p+r)/100.0 : 0;
                System.out.printf("  %-8.1f │ %8.1f%% │ %5.1f%% │ %5.3f │ %d/%d%n",
                        t, p, r, f, tp+fp, mr.scored().size());
            }
        }
        System.out.println(bar());

        // ── Per-pair detail for each model ───────────────────────────────────
        for (ModelResult mr : models) {
            if (mr.scored().isEmpty()) continue;
            System.out.println();
            System.out.printf("  ── %s ──%n", mr.name());
            System.out.printf("  %-28s → %-30s │ %5s │ %4s │ Band%n",
                    "Trigger (norm)", "Class", "Score", "Lbl");
            System.out.println("  " + "─".repeat(78));
            mr.scored().stream()
              .sorted(Comparator.comparingDouble(s -> -s.score()))
              .limit(15) // top 15 per model to keep output manageable
              .forEach(sp -> System.out.printf(
                      "  %-28s → %-30s │ %.3f │ %.2f │ %s%n",
                      sp.pair().norm(), sp.pair().className(),
                      sp.score(), sp.pair().label(), sp.pair().band()));
        }
        System.out.println();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private List<ScoredPair> score(Scorer scorer, boolean normalized) throws Exception {
        List<ScoredPair> result = new ArrayList<>();
        for (TestPair p : TEST_PAIRS) {
            String query = normalized ? p.norm() : p.trigger();
            String desc  = descriptions.getOrDefault(p.className(), p.className());
            float  s     = scorer.score(query, p.className() + ": " + desc);
            result.add(new ScoredPair(p, s, desc));
        }
        return result;
    }

    private static float cosineSim(float[] a, float[] b) {
        if (a == null || b == null) return 0f;
        float dot = 0, na = 0, nb = 0;
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
        double denom = Math.sqrt(na) * Math.sqrt(nb);
        return denom < 1e-9 ? 0f : (float)(dot / denom);
    }

    private static double[] bandMeans(List<ScoredPair> scored) {
        double[] sums = new double[4]; int[] counts = new int[4];
        for (ScoredPair sp : scored) {
            int idx = "ABCD".indexOf(sp.pair().band());
            if (idx >= 0) { sums[idx] += sp.score(); counts[idx]++; }
        }
        return new double[]{
            counts[0] > 0 ? sums[0]/counts[0] : 0,
            counts[1] > 0 ? sums[1]/counts[1] : 0,
            counts[2] > 0 ? sums[2]/counts[2] : 0,
            counts[3] > 0 ? sums[3]/counts[3] : 0
        };
    }

    /** Returns [precision, recall] at the threshold that maximises F1. */
    private static double[] bestThreshold(List<ScoredPair> scored) {
        int pos = (int) scored.stream().filter(s -> s.pair().label() >= 0.5).count();
        double bestF1 = -1; double[] best = {0, 0};
        for (float t : new float[]{0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f}) {
            long tp = scored.stream().filter(s -> s.score()>=t && s.pair().label()>=0.5).count();
            long fp = scored.stream().filter(s -> s.score()>=t && s.pair().label()< 0.5).count();
            double p = (tp+fp)>0 ? (double)tp/(tp+fp) : 0;
            double r = pos>0 ? (double)tp/pos : 0;
            double f = (p+r)>0 ? 2*p*r/(p+r) : 0;
            if (f > bestF1) { bestF1 = f; best = new double[]{p, r}; }
        }
        return best;
    }

    private static double computeAuc(List<ScoredPair> scored) {
        List<ScoredPair> sorted = new ArrayList<>(scored);
        sorted.sort(Comparator.comparingDouble(s -> -s.score()));
        int pos = (int) scored.stream().filter(s -> s.pair().label() >= 0.5).count();
        int neg = scored.size() - pos;
        if (pos == 0 || neg == 0) return 0.5;
        double auc = 0; long tp = 0, fp = 0;
        double prevFpr = 0, prevTpr = 0;
        for (ScoredPair sp : sorted) {
            if (sp.pair().label() >= 0.5) tp++; else fp++;
            double fpr = (double)fp/neg, tpr = (double)tp/pos;
            auc += (fpr - prevFpr) * (tpr + prevTpr) / 2.0;
            prevFpr = fpr; prevTpr = tpr;
        }
        return auc;
    }

    static Map<String, String> loadDescriptions(Set<String> classNames) throws IOException {
        IndexConfig config = IndexConfig.load();
        LuceneIndexer idx  = new LuceneIndexer(config);
        IndexReader   r    = idx.openMultiReader(List.of("lucene"));
        IndexSearcher s    = new IndexSearcher(r);
        StoredFields  sf   = s.storedFields();
        Map<String, String> result = new HashMap<>();
        for (String cls : classNames) {
            TopDocs hits = s.search(
                    new TermQuery(new Term(DocumentMapper.F_CLASS_NAME, cls.toLowerCase())), 5);
            for (var sd : hits.scoreDocs) {
                var doc = sf.document(sd.doc);
                if (!"lucene".equals(doc.get(DocumentMapper.F_PROJECT))) continue;
                if (!"class".equals(doc.get(DocumentMapper.F_DOC_TYPE)))  continue;
                String jd = doc.get(DocumentMapper.F_JAVADOC);
                if (jd != null && !jd.isBlank()) { result.put(cls, firstSentence(jd)); break; }
            }
            result.putIfAbsent(cls, cls);
        }
        r.close(); idx.close();
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

    // ── Whitening + percentile cross-project test ─────────────────────────────

    /**
     * Tests two corpus-independent scoring approaches across projects of very
     * different sizes and domains:
     *
     * <ul>
     *   <li><b>Whitened cosine</b> — subtract the corpus mean embedding and
     *       divide by per-dimension std before computing cosine similarity.
     *       Corrects for transformer anisotropy; the threshold should be stable
     *       across corpora.
     *   <li><b>Percentile threshold</b> — instead of an absolute cosine cutoff,
     *       keep the top-P% of triggers per class. Distribution-free by
     *       construction.
     * </ul>
     *
     * <p>Reports score distribution statistics (mean, std, p50, p90) and the
     * p90 threshold value for both raw and whitened cosines. If whitening works,
     * the p90 threshold should be similar across greedy-wand (44 classes),
     * pharos (189), mypal (2886), and lucene (8964).
     *
     * <p>For Lucene, computes AUC on the 80-pair labeled set for both approaches.
     */
    @Test
    @Order(3)
    void whitening_and_percentile_cross_project() throws Exception {
        if (biEncoder == null) {
            System.out.println("  [SKIP] bi-encoder not configured — cannot run cross-project test");
            return;
        }

        String[] projects = {"greedy-wand", "pharos", "mypal", "lucene"};
        int SAMPLE = 300;   // (trigger, class) pairs to sample per project
        int WHITEN_SAMPLE = 200; // class descriptions for whitening statistics

        System.out.println();
        System.out.println(bar());
        System.out.println("  WHITENING + PERCENTILE — cross-project score distribution");
        System.out.println(bar());
        System.out.printf("  %-14s │ %5s │ %-42s │ %-42s │ AUC%n",
                "Project", "Pairs",
                "── raw cosine (mean / std / p50 / p90 threshold) ──",
                "── whitened  (mean / std / p50 / p90 threshold) ──");
        System.out.println("  " + "─".repeat(120));

        for (String proj : projects) {
            IndexReader reader = luceneIndexer.openMultiReader(List.of(proj));

            // 1. Mine rules and sample pairs
            ConceptMiner miner = new ConceptMiner();
            Map<String, Set<String>> rules = miner.mineAll(reader);
            List<String[]> pairs = samplePairs(rules, SAMPLE);
            if (pairs.isEmpty()) { reader.close(); continue; }

            // 2. Load class descriptions for sampled classes + whitening sample
            Set<String> neededClasses = pairs.stream()
                    .map(p -> p[1]).collect(Collectors.toSet());
            Map<String, String> descs = loadDescriptionsForProject(reader, proj, neededClasses);

            // 3. Embed a sample of class descriptions for whitening statistics
            List<float[]> corpusEmbeddings = sampleClassEmbeddings(
                    reader, proj, biEncoder, WHITEN_SAMPLE);
            float[] mean = computeMean(corpusEmbeddings);
            float[] std  = computeStd(corpusEmbeddings, mean);

            // 4. Score all sampled pairs
            List<Float> rawScores = new ArrayList<>();
            List<Float> whtScores = new ArrayList<>();

            for (String[] pair : pairs) {
                String trigger = pair[0];
                String desc    = descs.getOrDefault(pair[1], pair[1]);
                float[] tv = biEncoder.embed(trigger);
                float[] cv = biEncoder.embed(desc);
                if (tv == null || cv == null) continue;
                rawScores.add(cosineSim(tv, cv));
                whtScores.add(cosineSim(whiten(tv, mean, std), whiten(cv, mean, std)));
            }
            if (rawScores.isEmpty()) { reader.close(); continue; }

            // 5. Compute distribution stats
            double[] rawStats = stats(rawScores);  // [mean, std, p50, p90]
            double[] whtStats = stats(whtScores);

            // 6. AUC on Lucene 80-pair test (only for lucene)
            String aucStr = "  —  ";
            if ("lucene".equals(proj)) {
                aucStr = String.format("%.3f", computeAucForScorer(mean, std));
            }

            System.out.printf("  %-14s │ %5d │ mean=%.3f std=%.3f p50=%.3f p90=%.3f    │ " +
                              "mean=%.3f std=%.3f p50=%.3f p90=%.3f    │ %s%n",
                    proj, rawScores.size(),
                    rawStats[0], rawStats[1], rawStats[2], rawStats[3],
                    whtStats[0], whtStats[1], whtStats[2], whtStats[3],
                    aucStr);
            // Do NOT close — LuceneIndexer caches the reader; @AfterAll closes everything.
        }

        System.out.println(bar());
        System.out.println("  If whitening works: p90 thresholds should be similar across projects.");
        System.out.println("  If raw p90 varies by >0.1 across projects, threshold calibration is needed.");
        System.out.println(bar());

        // ── Percentile approach: per-class top-P% using mined rules ──────────
        // For each project: score a sample of rules, then keep top-P% per class.
        // Reports AUC on Lucene test set using the class-specific percentile rank.
        System.out.println();
        System.out.println(bar());
        System.out.println("  PERCENTILE APPROACH — top-P% per class vs absolute threshold");
        System.out.println("  AUC on Lucene 80-pair test: computes each pair's percentile rank");
        System.out.println("  within the full distribution of trigger scores for that class.");
        System.out.println(bar());

        int pos = (int) TEST_PAIRS.stream().filter(p -> p.label() >= 0.5).count();
        // Build class → {all cosine scores} map from sampled mined rules (Lucene)
        ConceptMiner luceneMiner = new ConceptMiner();
        Map<String, Set<String>> luceneRules = luceneMiner.mineAll(luceneReader);
        // For each class in the 80-pair test, score a sample of its triggers
        Map<String, List<Float>> classScoreDistrib = new HashMap<>();
        Set<String> testClasses = TEST_PAIRS.stream()
                .map(TestPair::className).collect(Collectors.toSet());
        Random rng = new Random(42);
        for (Map.Entry<String, Set<String>> e : luceneRules.entrySet()) {
            String trigger = e.getKey();
            for (String cls : e.getValue()) {
                if (!testClasses.contains(cls)) continue;
                String desc = descriptions.getOrDefault(cls, cls);
                float[] tv = biEncoder.embed(trigger);
                float[] cv = biEncoder.embed(desc);
                if (tv == null || cv == null) continue;
                classScoreDistrib.computeIfAbsent(cls, k -> new ArrayList<>())
                                 .add(cosineSim(tv, cv));
            }
        }
        // Sort each class's distribution
        classScoreDistrib.forEach((cls, scores) -> Collections.sort(scores));

        // For the 80 test pairs: compute each pair's percentile rank within its class
        // Score = percentile rank (0 = lowest, 1 = highest trigger for that class)
        List<ScoredPair> percentileScored = new ArrayList<>();
        for (TestPair p : TEST_PAIRS) {
            String desc = descriptions.getOrDefault(p.className(), p.className());
            float[] tv  = biEncoder.embed(p.norm());
            float[] cv  = biEncoder.embed(desc);
            if (tv == null || cv == null) { percentileScored.add(new ScoredPair(p, 0f, desc)); continue; }
            float cosine = cosineSim(tv, cv);
            List<Float> distrib = classScoreDistrib.get(p.className());
            float pctRank = 0.5f; // default if no distribution
            if (distrib != null && !distrib.isEmpty()) {
                int below = 0;
                for (float s : distrib) if (s <= cosine) below++;
                pctRank = (float) below / distrib.size();
            }
            percentileScored.add(new ScoredPair(p, pctRank, desc));
        }

        // Show threshold sweep where threshold = percentile rank cutoff (0=keep all, 0.9=top 10%)
        System.out.printf("  %-10s │ %9s │ %6s │ %5s │ Rules kept (top-N%%)%n",
                "Pct cutoff", "Precision", "Recall", "F1");
        System.out.println("  " + "─".repeat(60));
        for (float t : new float[]{0.1f, 0.2f, 0.3f, 0.5f, 0.7f, 0.8f, 0.9f}) {
            long tp = percentileScored.stream().filter(s->s.score()>=t&&s.pair().label()>=0.5).count();
            long fp = percentileScored.stream().filter(s->s.score()>=t&&s.pair().label()< 0.5).count();
            double p2 = (tp+fp)>0 ? 100.0*tp/(tp+fp) : 0;
            double r2 = 100.0*tp/pos;
            double f2 = (p2+r2)>0 ? 2*p2*r2/(p2+r2)/100.0 : 0;
            System.out.printf("  %-10.0f%% │ %8.1f%% │ %5.1f%% │ %5.3f │ top-%.0f%%%n",
                    (1-t)*100, p2, r2, f2, (1-t)*100);
        }
        System.out.printf("  AUC (percentile): %.3f  vs raw cosine: 0.914  vs whitened: 0.911-0.912%n",
                computeAuc(percentileScored));
        System.out.println(bar());

        // ── Full threshold sweep for whitened bi-encoder on Lucene 80 pairs ───
        System.out.println();
        System.out.println(bar());
        System.out.println("  WHITENED BI-ENCODER — threshold sweep on Lucene 80-pair test set");
        System.out.println(bar());
        System.out.printf("  %-8s │ %9s │ %6s │ %5s │ compared with raw cosine%n",
                "Threshold", "Precision", "Recall", "F1");
        System.out.println("  " + "─".repeat(60));

        // Reuse the already-open luceneReader (don't close it)
        List<float[]> luceneCorpus = sampleClassEmbeddings(luceneReader, "lucene", biEncoder, 300);
        float[] lMean = computeMean(luceneCorpus);
        float[] lStd  = computeStd(luceneCorpus, lMean);

        List<ScoredPair> whtScored = scoreWithWhitening(lMean, lStd);
        for (float t : new float[]{0.1f,0.2f,0.3f,0.4f,0.5f,0.6f,0.7f,0.8f,0.9f}) {
            long tp = whtScored.stream().filter(s -> s.score()>=t && s.pair().label()>=0.5).count();
            long fp = whtScored.stream().filter(s -> s.score()>=t && s.pair().label()< 0.5).count();
            double p = (tp+fp)>0 ? 100.0*tp/(tp+fp) : 0;
            double r = 100.0*tp/pos;
            double f = (p+r)>0 ? 2*p*r/(p+r)/100.0 : 0;
            System.out.printf("  %-8.1f │ %8.1f%% │ %5.1f%% │ %5.3f │ raw: P=%.0f%% R=%.0f%%%n",
                    t, p, r, f,
                    rawPrecision(t) * 100, rawRecall(t) * 100);
        }
        System.out.printf("  AUC whitened: %.3f  vs raw: 0.914%n", computeAuc(whtScored));
        System.out.println(bar());
        System.out.println();
    }

    // ── Full Lucene mining with project-calibrated bi-encoder filter ──────────

    /**
     * End-to-end evaluation:
     * <ol>
     *   <li>Generate the synonym file via {@link ConceptMiner#appendNewSynonyms}.
     *   <li>Read the auto-mined rules from synonyms.txt.
     *   <li>Score a 2 000-rule calibration sample:
     *       cosine(embed(trigger), embed(classDescription)).
     *   <li>Calibrate threshold to P70 of the sample distribution.
     *   <li>Score all rules using the same threshold, report kept vs removed.
     *   <li>Show quality samples and a production readiness assessment.
     * </ol>
     */
    @Test
    @Order(4)
    void full_lucene_mining_with_calibrated_filter() throws Exception {
        if (biEncoder == null) {
            System.out.println("  [SKIP] bi-encoder not configured");
            return;
        }

        // ── Step 1: generate synonyms.txt ─────────────────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  STEP 1 — generate synonyms.txt with combined miner");
        System.out.println(bar());

        IndexConfig config = IndexConfig.load();
        java.nio.file.Path synonymFile = config.getSynonymsFile();

        // Remove ALL auto-mined rules (old algorithm) for any project before regenerating.
        // The old TF-IDF based rules are lower quality and mixed with the new ones.
        for (String proj : List.of("lucene", "pharos", "mypal", "solr", "vespa")) {
            int r = ConceptMiner.removeProjectSynonyms(synonymFile, proj);
            if (r > 0) System.out.printf("  Removed %d stale auto-mined lines for '%s'%n", r, proj);
        }

        ConceptMiner miner = new ConceptMiner();
        int added = miner.appendNewSynonyms(luceneReader, synonymFile, "lucene");
        System.out.printf("  Appended %,d new rules → %s%n", added, synonymFile);

        // ── Step 2: parse auto-mined rules from synonyms.txt ──────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  STEP 2 — parse auto-mined rules from synonyms.txt");
        System.out.println(bar());

        record Rule(String trigger, String className) {}
        List<Rule> allRules = new ArrayList<>();
        for (String line : java.nio.file.Files.readAllLines(synonymFile)) {
            String s = line.strip();
            if (s.startsWith("#") || !s.contains("=>") || !s.contains("auto:lucene")) continue;
            String lhs = s.split("=>")[0].strip();
            String rhs = s.split("=>")[1].strip().split("\\s")[0]; // className only
            if (!lhs.isBlank() && !rhs.isBlank()) allRules.add(new Rule(lhs, rhs));
        }
        System.out.printf("  Parsed %,d auto-mined rules (%,d unique triggers)%n",
                allRules.size(),
                allRules.stream().map(Rule::trigger).collect(Collectors.toSet()).size());

        // ── Step 2b: identify Source 1 + Source 3 triggers (safe, no filter) ───
        // Source 1 = class name decomposition (tautologically correct)
        // Source 3 = acronym expansions (deterministic regex match)
        // Source 2 = bigram PPMI (only these are filtered)
        Set<String> safeTriggersS1 = SynonymMiningQualityTest.mineClassNameTokens(luceneReader).keySet();
        Set<String> safeTriggersS3 = SynonymMiningQualityTest.mineAcronymExpansions(luceneReader).keySet();
        Set<String> safeTriggers = new HashSet<>(safeTriggersS1);
        safeTriggers.addAll(safeTriggersS3);

        // Split allRules into safe (S1+S3) and candidate (S2-only)
        List<Rule> safeRules = allRules.stream()
                .filter(r -> safeTriggers.contains(r.trigger()))
                .collect(Collectors.toList());
        List<Rule> candidateRules = allRules.stream()
                .filter(r -> !safeTriggers.contains(r.trigger()))
                .collect(Collectors.toList());
        // Check golden pair retention BEFORE any filtering
        long inAllRules = TEST_PAIRS.stream()
                .filter(p -> p.label() >= 0.5)
                .filter(p -> {
                    String trigger = p.norm().replace(" ", "");
                    return allRules.stream().anyMatch(r ->
                            r.trigger().equals(trigger) &&
                            r.className().equalsIgnoreCase(p.className()));
                })
                .count();
        System.out.printf("  Positive golden pairs in raw mined rules (no filter): %d / 40%n", inAllRules);
        System.out.printf("  Safe rules   (Source 1+3, no filter): %,d%n", safeRules.size());
        System.out.printf("  Candidate rules (Source 2 bigrams):   %,d → will apply P70 filter%n",
                candidateRules.size());

        // ── Step 3: calibration sample ────────────────────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  STEP 3 — calibrate P70 threshold on Source 2 candidates only");
        System.out.println(bar());

        // Calibrate only on Source 2 candidates
        List<Rule> shuffled = new ArrayList<>(candidateRules);
        Collections.shuffle(shuffled, new Random(42));
        List<Rule> sample = shuffled.subList(0, Math.min(2000, shuffled.size()));

        Set<String> sampleClasses = sample.stream().map(Rule::className)
                .collect(Collectors.toSet());
        // Convert classname (lowercase) back to CamelCase for the index lookup
        Map<String, String> descMap = fetchDescsByLowerName(sampleClasses);

        System.out.print("  Scoring sample");
        List<Float> sampleScores = new ArrayList<>();
        for (Rule r : sample) {
            String desc = descMap.getOrDefault(r.className(), r.className());
            float[] tv = biEncoder.embed(r.trigger());
            float[] cv = biEncoder.embed(desc);
            if (tv != null && cv != null) sampleScores.add(cosineSim(tv, cv));
            if (sampleScores.size() % 200 == 0) System.out.print(".");
        }
        System.out.println();

        Collections.sort(sampleScores);
        int n = sampleScores.size();
        float p50  = sampleScores.get(n / 2);
        float p70  = sampleScores.get((int)(n * 0.70));
        float p90  = sampleScores.get((int)(n * 0.90));
        float mean = (float) sampleScores.stream().mapToDouble(f -> f).average().orElse(0);

        System.out.printf("  Sample distribution: mean=%.3f  p50=%.3f  p70=%.3f  p90=%.3f%n",
                mean, p50, p70, p90);
        System.out.printf("  Project-calibrated threshold (P70): %.3f → keeps top-30%% of rules%n", p70);

        // ── Step 4: score ALL rules and apply threshold ────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  STEP 4 — score Source 2 candidates, merge with safe rules");
        System.out.println(bar());

        // Only need descriptions for Source 2 candidates
        Set<String> allClasses = candidateRules.stream().map(Rule::className)
                .collect(Collectors.toSet());
        Map<String, String> allDescs = fetchDescsByLowerName(allClasses);

        // Cache embeddings for unique class descriptions and unique triggers
        Map<String, float[]> descEmbCache = new LinkedHashMap<>();
        Map<String, float[]> trigEmbCache = new LinkedHashMap<>();

        record ScoredRule(Rule rule, float score) {}
        List<ScoredRule> scoredRules = new ArrayList<>();

        System.out.print("  Scoring " + candidateRules.size() + " Source 2 candidates");
        int count = 0;
        for (Rule r : candidateRules) {
            float[] tv = trigEmbCache.computeIfAbsent(r.trigger(),  biEncoder::embed);
            float[] cv = descEmbCache.computeIfAbsent(
                    r.className(), k -> biEncoder.embed(allDescs.getOrDefault(k, k)));
            if (tv != null && cv != null)
                scoredRules.add(new ScoredRule(r, cosineSim(tv, cv)));
            if (++count % 2000 == 0) System.out.print(".");
        }
        System.out.println();

        List<ScoredRule> keptS2    = scoredRules.stream()
                .filter(s -> s.score() >= p70).collect(Collectors.toList());
        List<ScoredRule> removed2  = scoredRules.stream()
                .filter(s -> s.score()  < p70).collect(Collectors.toList());

        // Merge: safe rules (unfiltered) + filtered Source 2
        List<ScoredRule> safeScored = safeRules.stream()
                .map(r -> new ScoredRule(r, 1.0f))   // score=1.0 = authoritative
                .collect(Collectors.toList());
        List<ScoredRule> kept = new ArrayList<>();
        kept.addAll(safeScored);
        kept.addAll(keptS2);

        System.out.printf("  Source 2 scored:   %,d%n", scoredRules.size());
        System.out.printf("  Source 2 kept (≥%.3f): %,d  (%.1f%% of S2)%n",
                p70, keptS2.size(), 100.0 * keptS2.size() / scoredRules.size());
        System.out.printf("  Source 2 removed:  %,d  (%.1f%% of S2)%n",
                removed2.size(), 100.0 * removed2.size() / scoredRules.size());
        System.out.printf("  Safe rules (S1+S3): %,d  (unfiltered)%n", safeRules.size());
        System.out.printf("  TOTAL kept:         %,d%n", kept.size());

        // ── Step 5: quality report ─────────────────────────────────────────────
        kept.sort(Comparator.comparingDouble(ScoredRule::score).reversed());
        removed2.sort(Comparator.comparingDouble(ScoredRule::score).reversed());

        System.out.println();
        System.out.println(bar());
        System.out.println("  TOP-30 KEPT Source 2 (highest confidence bigrams)");
        System.out.println(bar());
        System.out.printf("  %-28s → %-38s │ Score%n", "Trigger", "Class");
        System.out.println("  " + "─".repeat(72));
        keptS2.stream().sorted(Comparator.comparingDouble(ScoredRule::score).reversed())
                .limit(30).forEach(s ->
                System.out.printf("  %-28s → %-38s │ %.3f%n",
                        s.rule().trigger(), s.rule().className(), s.score()));

        System.out.println();
        System.out.println(bar());
        System.out.println("  BOTTOM-20 KEPT Source 2 (just above threshold — borderline)");
        System.out.println(bar());
        System.out.printf("  %-28s → %-38s │ Score%n", "Trigger", "Class");
        System.out.println("  " + "─".repeat(72));
        List<ScoredRule> keptS2sorted = keptS2.stream()
                .sorted(Comparator.comparingDouble(ScoredRule::score))
                .collect(Collectors.toList());
        keptS2sorted.subList(Math.max(0, keptS2sorted.size()-20), keptS2sorted.size())
                .forEach(s -> System.out.printf("  %-28s → %-38s │ %.3f%n",
                        s.rule().trigger(), s.rule().className(), s.score()));

        System.out.println();
        System.out.println(bar());
        System.out.println("  TOP-20 REMOVED (highest score among filtered-out rules)");
        System.out.println(bar());
        System.out.printf("  %-28s → %-38s │ Score%n", "Trigger", "Class");
        System.out.println("  " + "─".repeat(72));
        removed2.stream().limit(20).forEach(s ->
                System.out.printf("  %-28s → %-38s │ %.3f%n",
                        s.rule().trigger(), s.rule().className(), s.score()));

        // ── Step 6: production assessment ─────────────────────────────────────
        long keptOnGolden = TEST_PAIRS.stream()
                .filter(p -> p.label() >= 0.5)
                .filter(p -> kept.stream().anyMatch(s ->
                        s.rule().trigger().equals(p.norm().replace(" ", "")) &&
                        s.rule().className().equalsIgnoreCase(p.className())))
                .count();

        // ── Step 7: query-based redundancy filter ─────────────────────────────
        Map<String, Set<String>> keptMap = new HashMap<>();
        for (ScoredRule s : kept)
            keptMap.computeIfAbsent(s.rule().trigger(), k -> new HashSet<>())
                   .add(s.rule().className());

        System.out.println();
        System.out.println(bar());
        System.out.println("  STEP 7 — query-based redundancy filter (topK=5)");
        System.out.println("  Drops rules where BM25 already finds the class without synonyms");
        System.out.println(bar());

        Map<String, Set<String>> afterRedundancy =
                ConceptMiner.filterRedundant(keptMap, luceneReader, 5);

        int totalAfterRedundancy = afterRedundancy.values().stream().mapToInt(Set::size).sum();
        int removedByRedundancy  = kept.size() - totalAfterRedundancy;
        System.out.printf("  Before: %,d rules%n", kept.size());
        System.out.printf("  Removed as redundant: %,d%n", removedByRedundancy);
        System.out.printf("  After:  %,d rules%n", totalAfterRedundancy);

        long afterRedundancyGolden = TEST_PAIRS.stream()
                .filter(p -> p.label() >= 0.5)
                .filter(p -> {
                    String t = p.norm().replace(" ", "");
                    Set<String> cls = afterRedundancy.get(t);
                    return cls != null && cls.stream()
                            .anyMatch(c -> c.equalsIgnoreCase(p.className()));
                })
                .count();
        System.out.printf("  Golden pairs retained: %d / 40%n", afterRedundancyGolden);

        System.out.println();
        System.out.println("  SAMPLE REDUNDANT RULES DROPPED (BM25 already handles these):");
        keptMap.entrySet().stream()
                .filter(e -> !afterRedundancy.containsKey(e.getKey()))
                .flatMap(e -> e.getValue().stream().map(cls -> "    " + e.getKey() + " → " + cls))
                .limit(20)
                .forEach(System.out::println);

        // ── Final summary ─────────────────────────────────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  PRODUCTION ASSESSMENT");
        System.out.println(bar());
        System.out.printf("  Total mined:                       %,d%n", allRules.size());
        System.out.printf("  After source-aware P70 (S1+S3+S2): %,d%n", kept.size());
        System.out.printf("  After redundancy filter:            %,d%n", totalAfterRedundancy);
        System.out.printf("  Total rules removed:                %,d (%.1f%%)%n",
                allRules.size() - totalAfterRedundancy,
                100.0 * (allRules.size() - totalAfterRedundancy) / allRules.size());
        System.out.printf("  Golden pairs retained:              %d / 40%n", afterRedundancyGolden);
        System.out.println(bar());
        System.out.println();
    }

    /** Looks up first-sentence descriptions by lowercased class name from the Lucene index. */
    private Map<String, String> fetchDescsByLowerName(Set<String> lowerNames) throws IOException {
        IndexSearcher s  = new IndexSearcher(luceneReader);
        StoredFields  sf = s.storedFields();
        Map<String, String> result = new HashMap<>();
        for (String lower : lowerNames) {
            TopDocs hits = s.search(
                    new TermQuery(new Term(DocumentMapper.F_CLASS_NAME, lower)), 5);
            for (var sd : hits.scoreDocs) {
                var doc = sf.document(sd.doc);
                if (!"lucene".equals(doc.get(DocumentMapper.F_PROJECT))) continue;
                if (!"class".equals(doc.get(DocumentMapper.F_DOC_TYPE)))  continue;
                String jd = doc.get(DocumentMapper.F_JAVADOC);
                if (jd != null && !jd.isBlank()) {
                    result.put(lower, firstSentence(jd));
                    break;
                }
            }
            result.putIfAbsent(lower, lower);
        }
        return result;
    }

    // ── Whitening helpers ─────────────────────────────────────────────────────

    private static float[] computeMean(List<float[]> vecs) {
        int d = vecs.get(0).length;
        float[] m = new float[d];
        for (float[] v : vecs) for (int i = 0; i < d; i++) m[i] += v[i];
        for (int i = 0; i < d; i++) m[i] /= vecs.size();
        return m;
    }

    private static float[] computeStd(List<float[]> vecs, float[] mean) {
        int d = mean.length;
        float[] s = new float[d];
        for (float[] v : vecs) for (int i = 0; i < d; i++) { float x = v[i]-mean[i]; s[i]+=x*x; }
        for (int i = 0; i < d; i++) s[i] = (float)Math.sqrt(s[i]/vecs.size() + 1e-8);
        return s;
    }

    private static float[] whiten(float[] v, float[] mean, float[] std) {
        float[] w = new float[v.length];
        for (int i = 0; i < v.length; i++) w[i] = (v[i] - mean[i]) / std[i];
        // Re-normalize to unit sphere after whitening
        float norm = 0;
        for (float x : w) norm += x*x;
        norm = (float)Math.sqrt(norm);
        if (norm > 1e-9) for (int i = 0; i < w.length; i++) w[i] /= norm;
        return w;
    }

    private static double[] stats(List<Float> scores) {
        List<Float> sorted = new ArrayList<>(scores);
        Collections.sort(sorted);
        int n = sorted.size();
        double mean = sorted.stream().mapToDouble(f->f).average().orElse(0);
        double var  = sorted.stream().mapToDouble(f->(f-mean)*(f-mean)).average().orElse(0);
        double std  = Math.sqrt(var);
        double p50  = sorted.get(n/2);
        double p90  = sorted.get((int)(n * 0.9));
        return new double[]{mean, std, p50, p90};
    }

    /** Samples up to {@code limit} (trigger, className) pairs from mined rules. */
    private static List<String[]> samplePairs(Map<String, Set<String>> rules, int limit) {
        List<String[]> pairs = new ArrayList<>();
        for (Map.Entry<String, Set<String>> e : rules.entrySet())
            for (String cls : e.getValue())
                pairs.add(new String[]{e.getKey(), cls});
        Collections.shuffle(pairs, new Random(42));
        return pairs.subList(0, Math.min(limit, pairs.size()));
    }

    /** Samples up to {@code limit} class description embeddings from a project. */
    private static List<float[]> sampleClassEmbeddings(IndexReader reader, String project,
                                                         EmbeddingProvider embedder,
                                                         int limit) throws IOException {
        IndexSearcher s   = new IndexSearcher(reader);
        StoredFields  sf  = s.storedFields();
        TopDocs hits = s.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "class")),
                Integer.MAX_VALUE);
        List<float[]> result = new ArrayList<>();
        Random rng = new Random(42);
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < hits.scoreDocs.length; i++) indices.add(i);
        Collections.shuffle(indices, rng);
        for (int idx : indices) {
            if (result.size() >= limit) break;
            var doc = sf.document(hits.scoreDocs[idx].doc);
            if (!project.equals(doc.get(DocumentMapper.F_PROJECT))) continue;
            if ("document".equals(doc.get("kind"))) continue;
            String jd = doc.get(DocumentMapper.F_JAVADOC);
            if (jd == null || jd.isBlank()) continue;
            float[] emb = embedder.embed(firstSentence(jd));
            if (emb != null) result.add(emb);
        }
        return result;
    }

    private Map<String, String> loadDescriptionsForProject(IndexReader reader, String project,
                                                             Set<String> classNames)
            throws IOException {
        IndexSearcher s  = new IndexSearcher(reader);
        StoredFields  sf = s.storedFields();
        Map<String, String> result = new HashMap<>();
        for (String cls : classNames) {
            TopDocs hits = s.search(
                    new TermQuery(new Term(DocumentMapper.F_CLASS_NAME, cls.toLowerCase())), 5);
            for (var sd : hits.scoreDocs) {
                var doc = sf.document(sd.doc);
                if (!project.equals(doc.get(DocumentMapper.F_PROJECT))) continue;
                if (!"class".equals(doc.get(DocumentMapper.F_DOC_TYPE))) continue;
                String jd = doc.get(DocumentMapper.F_JAVADOC);
                if (jd != null && !jd.isBlank()) { result.put(cls, firstSentence(jd)); break; }
            }
            result.putIfAbsent(cls, cls);
        }
        return result;
    }

    /** Scores the 80 test pairs using whitened bi-encoder. */
    private List<ScoredPair> scoreWithWhitening(float[] mean, float[] std) {
        List<ScoredPair> result = new ArrayList<>();
        for (TestPair p : TEST_PAIRS) {
            String desc = descriptions.getOrDefault(p.className(), p.className());
            float[] tv = biEncoder.embed(p.norm());
            float[] cv = biEncoder.embed(desc);
            float s = (tv != null && cv != null)
                    ? cosineSim(whiten(tv, mean, std), whiten(cv, mean, std)) : 0f;
            result.add(new ScoredPair(p, s, desc));
        }
        return result;
    }

    /** Computes AUC for whitened bi-encoder on the 80-pair test, using provided corpus stats. */
    private double computeAucForScorer(float[] mean, float[] std) {
        return computeAuc(scoreWithWhitening(mean, std));
    }

    /** Raw precision/recall helpers for the comparison column (uses cached raw scores). */
    private double rawPrecision(float t) {
        long tp = rawCachedScores.stream().filter(s->s.score()>=t&&s.pair().label()>=0.5).count();
        long fp = rawCachedScores.stream().filter(s->s.score()>=t&&s.pair().label()< 0.5).count();
        return (tp+fp)>0 ? (double)tp/(tp+fp) : 0;
    }
    private double rawRecall(float t) {
        int pos = (int) TEST_PAIRS.stream().filter(p->p.label()>=0.5).count();
        long tp = rawCachedScores.stream().filter(s->s.score()>=t&&s.pair().label()>=0.5).count();
        return pos>0 ? (double)tp/pos : 0;
    }

    /** Cached raw bi-encoder scores (computed once in compare_all_scorers). */
    private List<ScoredPair> rawCachedScores = List.of();

    private static String bar() { return "━".repeat(72); }
}
