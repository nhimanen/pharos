package com.pharos.quality;

import com.pharos.config.IndexConfig;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.jupiter.api.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Compares four synonym-mining strategies against a hand-curated golden set of
 * 100 (term, expectedClass) pairs extracted from the Lucene codebase.
 *
 * <p>Each strategy produces a {@code Map<String, Set<String>>} — lowercase term
 * to the set of simple class names it should surface. Recall is the primary metric:
 * what fraction of the 100 golden pairs does each strategy discover?
 *
 * <h3>Strategies</h3>
 * <ol>
 *   <li><b>Baseline</b> — current production: TF-IDF over full javadoc + body text
 *   <li><b>FirstSentence</b> — TF-IDF restricted to the first sentence of javadoc only
 *   <li><b>LinkGraph</b> — Solr-KG-style: follow {@code @link}/{@code @see} edges;
 *       first-sentence terms of class A become synonyms for any class B that A links to
 *   <li><b>CoCallers</b> — basket analysis: class names that share callers (Jaccard ≥ 0.05)
 *       become undirected synonyms for each other
 * </ol>
 *
 * <p><b>Prerequisite:</b> the Lucene project must be indexed:
 * <pre>{@code  pharos index /home/nhimanen/projects/lucene --project lucene}</pre>
 *
 * <p>Tag: {@code quality} — excluded from default {@code mvn test}.
 * Run: {@code mvn test -Dtest=SynonymMiningQualityTest -Dgroups=quality}
 */
@Tag("quality")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SynonymMiningQualityTest {

    // ── Golden dataset ────────────────────────────────────────────────────────

    record GoldenPair(String term, String expectedClass, String domain) {}

    static List<GoldenPair> GOLDEN = List.of(
        // ── NRT / reading (7) — trigger absent, target in index ──────────────
        new GoldenPair("ramresident",       "NRTCachingDirectory",                        "nrt"),
        new GoldenPair("cachingnrt",        "NRTCachingDirectory",                        "nrt"),
        new GoldenPair("offheapstorage",    "OffHeapFSTStore",                            "nrt"),
        new GoldenPair("indexreplica",      "ReplicaNode",                                "nrt"),
        new GoldenPair("replicapull",       "ReplicaNode",                                "nrt"),
        new GoldenPair("lazysearcher",      "SearcherManager",                            "nrt"),
        new GoldenPair("readerwarmer",      "IndexReaderWarmer",                          "nrt"),

        // ── Codec / storage (20) ─────────────────────────────────────────────
        new GoldenPair("skiplist",          "MultiLevelSkipListReader",                   "codec"),
        new GoldenPair("kdtree",            "BKDWriter",                                  "codec"),
        new GoldenPair("blockmax",          "BlockMaxConjunctionScorer",                  "codec"),
        new GoldenPair("deltapack",         "DeltaPackedLongValues",                      "codec"),
        new GoldenPair("offheapmmap",       "MemorySegmentIndexInput",                    "codec"),
        new GoldenPair("memorymapped",      "MMapDirectory",                              "codec"),
        new GoldenPair("mmapfile",          "MMapDirectory",                              "codec"),
        new GoldenPair("mmapread",          "MMapDirectory",                              "codec"),
        new GoldenPair("finitestatemachine","FST",                                        "codec"),
        new GoldenPair("bytestore",         "PagedBytes",                                 "codec"),
        new GoldenPair("pagedmemory",       "PagedBytes",                                 "codec"),
        new GoldenPair("packedarray",       "PackedLongValues",                           "codec"),
        new GoldenPair("sparsebits",        "SparseFixedBitSet",                          "codec"),
        new GoldenPair("roaringdocset",     "RoaringDocIdSet",                            "codec"),
        new GoldenPair("compressedstorage", "StoredFieldsFormat",                         "codec"),
        new GoldenPair("storedformat",      "StoredFieldsFormat",                         "codec"),
        new GoldenPair("bulkread",          "StoredFieldsReader",                         "codec"),
        new GoldenPair("directpostings",    "DirectPostingsFormat",                       "codec"),
        new GoldenPair("inlinepostings",    "DirectPostingsFormat",                       "codec"),
        new GoldenPair("termdictwrite",     "TermsHashPerField",                          "codec"),

        // ── Analysis (9) ─────────────────────────────────────────────────────
        new GoldenPair("synonymexpansion",  "SynonymGraphFilter",                         "analysis"),
        new GoldenPair("compoundword",      "DictionaryCompoundWordTokenFilter",           "analysis"),
        new GoldenPair("hyphendecomp",      "HyphenationCompoundWordTokenFilter",          "analysis"),
        new GoldenPair("wordstemmer",       "PorterStemFilter",                           "analysis"),
        new GoldenPair("charfolding",       "ASCIIFoldingFilter",                         "analysis"),
        new GoldenPair("accentstrip",       "ASCIIFoldingFilter",                         "analysis"),
        new GoldenPair("unicodefold",       "ASCIIFoldingFilter",                         "analysis"),
        new GoldenPair("splitwords",        "WordBreakSpellChecker",                      "analysis"),
        new GoldenPair("keywordmark",       "KeywordMarkerFilter",                        "analysis"),

        // ── Scoring / queries (16) ────────────────────────────────────────────
        new GoldenPair("weakand",           "WANDScorer",                                 "scoring"),
        new GoldenPair("vectorspace",       "TFIDFSimilarity",                            "scoring"),
        new GoldenPair("bestfield",         "DisjunctionMaxQuery",                        "scoring"),
        new GoldenPair("functionboost",     "FunctionScoreQuery",                         "scoring"),
        new GoldenPair("impactscoring",     "ImpactsDISI",                                "scoring"),
        new GoldenPair("maxscoreblock",     "MaxScoreCache",                              "scoring"),
        new GoldenPair("topkdocument",      "TopScoreDocCollector",                       "scoring"),
        new GoldenPair("queryboosting",     "BoostQuery",                                 "scoring"),
        new GoldenPair("rescorewindow",     "QueryRescorer",                              "scoring"),
        new GoldenPair("queryrewrite",      "MultiTermQuery",                             "scoring"),
        new GoldenPair("termexpansion",     "MultiTermQuery",                             "scoring"),
        new GoldenPair("wildcardexpand",    "WildcardQuery",                              "scoring"),
        new GoldenPair("regexmatch",        "RegexpQuery",                                "scoring"),
        new GoldenPair("orderedproximity",  "SpanNearQuery",                              "scoring"),
        new GoldenPair("spancontain",       "SpanContainingQuery",                        "scoring"),
        new GoldenPair("positionrange",     "SpanPositionRangeQuery",                     "scoring"),

        // ── Facet (9) ────────────────────────────────────────────────────────
        new GoldenPair("taxonomyindex",     "TaxonomyWriter",                             "facet"),
        new GoldenPair("facettaxonomy",     "DirectoryTaxonomyWriter",                    "facet"),
        new GoldenPair("categorylabel",     "FacetLabel",                                 "facet"),
        new GoldenPair("labelpath",         "FacetLabel",                                 "facet"),
        new GoldenPair("drillcount",        "DrillSideways",                              "facet"),
        new GoldenPair("sidewayfacet",      "DrillSideways",                              "facet"),
        new GoldenPair("fastfacet",         "SortedSetDocValuesFacetCounts",              "facet"),
        new GoldenPair("facetcollect",      "FacetsCollector",                            "facet"),
        new GoldenPair("floatassociation",  "TaxonomyFacetFloatAssociations",             "facet"),

        // ── Indexing (15) ────────────────────────────────────────────────────
        new GoldenPair("softdeleted",       "SoftDeletesRetentionMergePolicy",            "indexing"),
        new GoldenPair("nonadjacent",       "TieredMergePolicy",                          "indexing"),
        new GoldenPair("levelmerge",        "LogMergePolicy",                             "indexing"),
        new GoldenPair("logpolicy",         "LogMergePolicy",                             "indexing"),
        new GoldenPair("compactionpolicy",  "MergePolicy",                                "indexing"),
        new GoldenPair("segmentflush",      "DocumentsWriterFlushControl",                "indexing"),
        new GoldenPair("backgroundmerge",   "ConcurrentMergeScheduler",                   "indexing"),
        new GoldenPair("indexgeneration",   "IndexCommit",                                "indexing"),
        new GoldenPair("segmentcommit",     "SegmentCommitInfo",                          "indexing"),
        new GoldenPair("docsperfield",      "FieldInfo",                                  "indexing"),
        new GoldenPair("fieldoptions",      "FieldInfo",                                  "indexing"),
        new GoldenPair("keeplatest",        "KeepOnlyLastCommitDeletionPolicy",           "indexing"),
        new GoldenPair("indexsorting",      "IndexSortSortedNumericDocValuesRangeQuery",  "indexing"),
        new GoldenPair("bpreorder",         "BPIndexReorderer",                           "indexing"),
        new GoldenPair("updatebydocvalue",  "DocValuesUpdate",                            "indexing"),

        // ── Suggest (8) ──────────────────────────────────────────────────────
        new GoldenPair("morethreshold",     "MoreLikeThis",                               "suggest"),
        new GoldenPair("weightedlookup",    "AnalyzingSuggester",                         "suggest"),
        new GoldenPair("infixmatch",        "AnalyzingInfixSuggester",                    "suggest"),
        new GoldenPair("contextcompletion", "ContextSuggestField",                        "suggest"),
        new GoldenPair("fuzzycompletion",   "FuzzyCompletionQuery",                       "suggest"),
        new GoldenPair("regexcompletion",   "RegexCompletionQuery",                       "suggest"),
        new GoldenPair("wfstlookup",        "WFSTCompletionLookup",                       "suggest"),
        new GoldenPair("spellsuggest",      "SpellChecker",                               "suggest"),

        // ── Vector / kNN (8) ─────────────────────────────────────────────────
        new GoldenPair("graphnav",          "HnswGraph",                                  "vector"),
        new GoldenPair("hnswsearch",        "HnswGraphSearcher",                          "vector"),
        new GoldenPair("heapgraph",         "OnHeapHnswGraph",                            "vector"),
        new GoldenPair("offheapvector",     "OffHeapFloatVectorValues",                   "vector"),
        new GoldenPair("approximateknn",    "HnswGraphSearcher",                          "vector"),
        new GoldenPair("spatialpoint",      "LatLonPoint",                                "vector"),
        new GoldenPair("latlonbox",         "LatLonBoundingBox",                          "vector"),
        new GoldenPair("columnstore",       "DocValues",                                  "vector"),

        // ── DocValues / misc (8) ─────────────────────────────────────────────
        new GoldenPair("perdocnumeric",     "NumericDocValues",                           "misc"),
        new GoldenPair("docvalueswrite",    "DocValuesConsumer",                          "misc"),
        new GoldenPair("multivaluedfield",  "SortedSetDocValues",                         "misc"),
        new GoldenPair("lockfree",          "FSLockFactory",                              "misc"),
        new GoldenPair("nativelock",        "NativeFSLock",                               "misc"),
        new GoldenPair("indexlocking",      "LockFactory",                                "misc"),
        new GoldenPair("rotatinglog",       "InfoStream",                                 "misc"),
        new GoldenPair("termvectorread",    "TermVectors",                                "misc")
    );

    // ── Test infrastructure ───────────────────────────────────────────────────

    private static LuceneIndexer luceneIndexer;
    private static IndexReader   luceneReader;

    @BeforeAll
    static void openIndex() throws IOException {
        IndexConfig config = IndexConfig.load();
        luceneIndexer = new LuceneIndexer(config);
        luceneReader  = luceneIndexer.openMultiReader(List.of("lucene"));
    }

    @AfterAll
    static void closeIndex() throws IOException {
        if (luceneIndexer != null) luceneIndexer.close();
    }

    // ── Main comparison test ──────────────────────────────────────────────────

    @Test
    @Order(1)
    void compare_all_strategies() throws IOException {
        record StrategyResult(String name, Map<String, Set<String>> map) {}

        List<StrategyResult> strategies = List.of(
            new StrategyResult("Baseline (TF-IDF full)",   mineBaseline(luceneReader)),
            new StrategyResult("FirstSentence TF-IDF",     mineFirstSentence(luceneReader)),
            new StrategyResult("LinkGraph (@link/@see)",    mineLinkGraph(luceneReader)),
            new StrategyResult("CoCallers (Jaccard≥0.05)", mineCoCallers(luceneReader)),
            new StrategyResult("ClassName tokens+bigrams",  mineClassNameTokens(luceneReader)),
            new StrategyResult("Method name tokens",        mineMethodNames(luceneReader)),
            new StrategyResult("Javadoc prose bigrams",     mineJavadocBigrams(luceneReader)),
            new StrategyResult("Package path segment",      minePackagePath(luceneReader)),
            new StrategyResult("PPMI (no minDf, topN=20)",  minePpmi(luceneReader)),
            new StrategyResult("Context window (skip-gram)", mineContextWindow(luceneReader)),
            new StrategyResult("PPMI bigrams+hyphens",      minePpmiBigramsInclStopwords(luceneReader)),
            new StrategyResult("★ Combined",               mineCombined(luceneReader)),
            new StrategyResult("Acronym expansions",        mineAcronymExpansions(luceneReader))
        );

        System.out.println();
        System.out.println(bar());
        System.out.println("  SYNONYM MINING QUALITY — 11 strategies vs 100 golden pairs");  // header: B F L C N M J P I K Q
        System.out.println(bar());
        System.out.printf("  %-30s │ %6s │ %6s │ %7s │ %s%n",
                "Strategy", "Rules", "Recall", "Prec@hit", "Found pairs");
        System.out.println("  " + "─".repeat(100));

        List<Set<Integer>> foundSets = new ArrayList<>();
        for (StrategyResult sr : strategies) {
            Set<Integer> found = new HashSet<>();
            for (int i = 0; i < GOLDEN.size(); i++) {
                GoldenPair gp = GOLDEN.get(i);
                Set<String> mapped = sr.map().get(gp.term());
                if (mapped != null && mapped.stream()
                        .anyMatch(c -> c.equalsIgnoreCase(gp.expectedClass()))) {
                    found.add(i);
                }
            }
            foundSets.add(found);

            int totalRules   = sr.map().values().stream().mapToInt(Set::size).sum();
            double recall    = (double) found.size() / GOLDEN.size();
            double precision = found.isEmpty() ? 0.0 : (double) found.size() / totalRules;

            System.out.printf("  %-30s │ %6d │ %5.1f%% │ %6.2f%% │ %d/%d%n",
                    sr.name(), totalRules,
                    recall * 100, precision * 100,
                    found.size(), GOLDEN.size());
        }

        // Per-domain breakdown
        System.out.println();
        System.out.println(bar());
        System.out.println("  RECALL BY DOMAIN");
        System.out.println(bar());
        List<String> domains = GOLDEN.stream().map(GoldenPair::domain).distinct().toList();
        System.out.printf("  %-12s │ %4s │ %-22s │ %-22s │ %-22s │ %-22s%n",
                "Domain", "N", strategies.get(0).name().substring(0, 10),
                strategies.get(1).name().substring(0, 10),
                strategies.get(2).name().substring(0, 10),
                strategies.get(3).name().substring(0, 10));
        System.out.println("  " + "─".repeat(110));

        for (String domain : domains) {
            List<Integer> domainIdx = new ArrayList<>();
            for (int i = 0; i < GOLDEN.size(); i++) {
                if (GOLDEN.get(i).domain().equals(domain)) domainIdx.add(i);
            }
            System.out.printf("  %-12s │ %4d │", domain, domainIdx.size());
            for (Set<Integer> found : foundSets) {
                long hits = domainIdx.stream().filter(found::contains).count();
                System.out.printf(" %4.0f%%                 │", 100.0 * hits / domainIdx.size());
            }
            System.out.println();
        }

        // Per-pair detail: which strategies find each pair
        System.out.println();
        System.out.println(bar());
        System.out.println("  PER-PAIR DETAIL  (B=Baseline  F=First  L=Link  C=CoCallers  N=ClassName  M=Method  J=Bigrams  P=Pkg  I=PPMI  K=CtxWindow  Q=PPMIBigrams)");
        System.out.println(bar());
        System.out.printf("  %-28s → %-38s │ B F L C N M J P I K Q │ Domain%n", "Term", "Expected class");
        System.out.println("  " + "─".repeat(100));

        for (int i = 0; i < GOLDEN.size(); i++) {
            GoldenPair gp = GOLDEN.get(i);
            StringBuilder flags = new StringBuilder();
            for (Set<Integer> found : foundSets) {
                flags.append(found.contains(i) ? "✓" : "·");
                flags.append(" ");
            }
            System.out.printf("  %-28s → %-38s │%s│ %s%n",
                    gp.term(), gp.expectedClass(), flags, gp.domain());
        }
        System.out.println(bar());

        // Unique coverage: pairs found only by one strategy
        System.out.println();
        System.out.println("  UNIQUE CONTRIBUTIONS (pairs found by exactly one strategy):");
        for (int si = 0; si < strategies.size(); si++) {
            int finalSi = si;
            long unique = foundSets.get(si).stream()
                    .filter(i -> {
                        for (int j = 0; j < foundSets.size(); j++) {
                            if (j != finalSi && foundSets.get(j).contains(i)) return false;
                        }
                        return true;
                    }).count();
            System.out.printf("  %-30s : %d unique pairs%n", strategies.get(si).name(), unique);
        }
        System.out.println(bar());
        System.out.println();
    }

    // ── Rule-reduction sweep ──────────────────────────────────────────────────

    /**
     * Sweeps bigram PPMI threshold and method topK against the golden set.
     * sentTopN=20 and fullTopN=15 are held fixed — earlier sweeps showed
     * tightening them saves very few rules while hurting recall more than
     * the bigram threshold does.
     */
    @Test
    @Order(3)
    void reduction_sweep() throws IOException {
        record Config(String label, double bigramPpmiThreshold,
                      int sentTopN, int fullTopN, double fullPpmiMin, int methodTopK) {}

        // Sweep top-P% PPMI percentile for Source 2 (javadoc bigrams).
        // Percentile is corpus-agnostic: 10% of 200 bigrams = 20; 10% of 20 = 2.
        List<Config> configs = List.of(
            new Config("top-5%",   5.0, 20, 15, 0.5, 5),
            new Config("top-10%", 10.0, 20, 15, 0.5, 5),
            new Config("top-15%", 15.0, 20, 15, 0.5, 5),
            new Config("top-20%", 20.0, 20, 15, 0.5, 5),
            new Config("top-25%", 25.0, 20, 15, 0.5, 5),
            new Config("top-30%", 30.0, 20, 15, 0.5, 5),
            new Config("top-50%", 50.0, 20, 15, 0.5, 5),
            new Config("top-100% (all positive-ppmi)", 100.0, 20, 15, 0.5, 5)
        );

        System.out.println();
        System.out.println(bar());
        System.out.println("  RULE REDUCTION SWEEP — topK × bigramPPMI threshold");
        System.out.println(bar());
        System.out.printf("  %-38s │ %8s │ %6s │ %6s │ %s%n",
                "Config", "Rules", "Recall", "Prec", "vs baseline");
        System.out.println("  " + "─".repeat(85));

        int baselineRules   = mineBaseline(luceneReader).values().stream().mapToInt(Set::size).sum();
        double baselineRecall = recall(mineBaseline(luceneReader));

        for (Config c : configs) {
            Map<String, Set<String>> map = mineCombinedParametric(
                    luceneReader, c.bigramPpmiThreshold(), c.sentTopN(),
                    c.fullTopN(), c.fullPpmiMin(), c.methodTopK());
            int rules   = map.values().stream().mapToInt(Set::size).sum();
            double rec  = recall(map);
            double prec = rules > 0 ? (double) hits(map) / rules : 0;
            System.out.printf("  %-38s │ %8d │ %5.1f%% │ %5.3f%% │ %+d rules, %+.1f%% recall%n",
                    c.label(), rules, rec * 100, prec * 100,
                    rules - baselineRules, (rec - baselineRecall) * 100);
        }
        System.out.println(bar());
        System.out.println();
    }

    private double recall(Map<String, Set<String>> map) {
        long found = GOLDEN.stream().filter(gp -> {
            Set<String> mapped = map.get(gp.term());
            return mapped != null && mapped.stream()
                    .anyMatch(c -> c.equalsIgnoreCase(gp.expectedClass()));
        }).count();
        return (double) found / GOLDEN.size();
    }

    private int hits(Map<String, Set<String>> map) {
        return (int) GOLDEN.stream().filter(gp -> {
            Set<String> mapped = map.get(gp.term());
            return mapped != null && mapped.stream()
                    .anyMatch(c -> c.equalsIgnoreCase(gp.expectedClass()));
        }).count();
    }

    /**
     * Two-source parameterised variant: Source 1 (class name tokens+bigrams)
     * + Source 2 (javadoc prose bigrams with minimum PPMI threshold).
     * The other parameters are kept in the signature for the sweep Config record
     * but are unused — the sweep only varies bigramPpmiThreshold.
     */
    static Map<String, Set<String>> mineCombinedParametric(
            IndexReader reader,
            double bigramPpmiThreshold, int sentTopN, int fullTopN, double fullPpmiMin,
            int methodTopK)
            throws IOException {

        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Set<String>> result = new HashMap<>();

        // Source 1: class name tokens + bigrams + trigrams
        for (ClassDoc d : docs) {
            String[] toks = splitIdentifier(d.simpleName).split("\\s+");
            for (String t : toks) if (normalizeTerm(t) != null) put(result, t, d.simpleName);
            for (int i = 0; i < toks.length - 1; i++) {
                String bi = toks[i] + toks[i + 1];
                if (bi.length() >= 4) put(result, bi, d.simpleName);
            }
            for (int i = 0; i < toks.length - 2; i++) {
                String tri = toks[i] + toks[i + 1] + toks[i + 2];
                if (tri.length() >= 6) put(result, tri, d.simpleName);
            }
        }

        // Source 2: javadoc prose bigrams — top-P% PPMI per class (corpus-agnostic)
        // bigramPpmiThreshold is reused as the percentile value (1–100)
        double bigramTopPct = bigramPpmiThreshold;
        Map<String, Integer> bigramDf = new HashMap<>();
        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            for (String b : new HashSet<>(hybridBigrams(text))) bigramDf.merge(b, 1, Integer::sum);
        }
        int totalDocs = docs.size();
        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            List<String> bigrams = hybridBigrams(text);
            Map<String, Integer> tf = new HashMap<>();
            for (String b : bigrams) tf.merge(b, 1, Integer::sum);
            int docLen = bigrams.size();
            if (docLen == 0) continue;
            List<Map.Entry<String, Double>> scored = new ArrayList<>();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                int df = bigramDf.getOrDefault(e.getKey(), 1);
                double ppmi = Math.log(((double) e.getValue() / docLen)
                                       / ((double) df / totalDocs));
                if (ppmi > 0) scored.add(Map.entry(e.getKey(), ppmi));
            }
            if (scored.isEmpty()) continue;
            scored.sort(Map.Entry.<String, Double>comparingByValue().reversed());
            int keep = Math.max(1, (int) Math.ceil(scored.size() * bigramTopPct / 100.0));
            scored.subList(0, Math.min(keep, scored.size()))
                  .forEach(e -> put(result, e.getKey(), d.simpleName));
        }

        // Post-filter: max length 20, no digit-start
        result.entrySet().removeIf(e -> {
            String t = e.getKey();
            return t.length() > 20 || Character.isDigit(t.charAt(0));
        });
        return result;
    }

    // ── Corpus-independence test ──────────────────────────────────────────────

    enum BigramScoreMode {
        /** Raw PPMI ≥ threshold. Value shifts with log(totalDocs); not portable. */
        ABSOLUTE,
        /** PPMI / log(totalDocs) ≥ threshold. Roughly corpus-size independent. */
        NPMI,
        /** Top-P% of per-class positive-PPMI bigrams. Adapts to any distribution. */
        PERCENTILE
    }

    /**
     * Runs the combined strategy against a specific project with a choice of
     * bigram scoring mode, then evaluates:
     * <ul>
     *   <li>Lucene: recall against the golden set (ground truth available)
     *   <li>Other projects: rule count + qualitative sample (no ground truth)
     * </ul>
     *
     * <p>The three modes address portability across corpora of different sizes:
     * <dl>
     *   <dt>ABSOLUTE</dt><dd>Raw PPMI threshold — breaks on small corpora because
     *       max achievable PPMI ≈ log(totalDocs), which collapses for small N.</dd>
     *   <dt>NPMI</dt><dd>Divides by log(totalDocs) → corpus-size normalised score
     *       in roughly [0, 1]. A threshold ≈ 0.67 is equivalent to threshold=6.0
     *       on Lucene (8964 docs) and auto-scales to pharos (189 docs).</dd>
     *   <dt>PERCENTILE</dt><dd>Emit top-P% of positive-PPMI bigrams per class.
     *       Fully distribution-agnostic; naturally adapts to class javadoc length.</dd>
     * </dl>
     */
    @Test
    @Order(4)
    void corpus_independence_test() throws IOException {
        // NPMI threshold calibrated from Lucene: 6.0 / log(8964) ≈ 0.659
        double npmiTarget = 6.0 / Math.log(8964);

        record ModeConfig(String label, BigramScoreMode mode, double param) {}
        List<ModeConfig> modes = List.of(
            new ModeConfig("ABSOLUTE  threshold=6.0",              BigramScoreMode.ABSOLUTE,   6.0),
            new ModeConfig("ABSOLUTE  threshold=3.0",              BigramScoreMode.ABSOLUTE,   3.0),
            new ModeConfig(String.format("NPMI  threshold=%.3f (≈6.0 on Lucene)", npmiTarget),
                                                                   BigramScoreMode.NPMI,       npmiTarget),
            new ModeConfig("PERCENTILE  top=5%",                   BigramScoreMode.PERCENTILE, 5.0),
            new ModeConfig("PERCENTILE  top=10%",                  BigramScoreMode.PERCENTILE, 10.0)
        );

        System.out.println();
        System.out.println(bar());
        System.out.println("  CORPUS INDEPENDENCE — Lucene (8964 classes) vs Pharos (189 classes)");
        System.out.println(bar());

        // ── Lucene: recall against golden set ────────────────────────────────
        System.out.printf("  %-45s │ %8s │ %6s │ %8s%n",
                "Mode", "Rules", "Recall", "BigramRls");
        System.out.println("  " + "─".repeat(75));

        IndexConfig config  = IndexConfig.load();
        LuceneIndexer idx   = new LuceneIndexer(config);
        IndexReader pharosR = idx.openMultiReader(List.of("pharos"));

        for (ModeConfig mc : modes) {
            // Lucene
            Map<String, Set<String>> luceneMap = mineCombinedForProject(
                    luceneReader, "lucene", mc.mode(), mc.param(), 6, 20, 15, 0.5);
            int luceneRules   = luceneMap.values().stream().mapToInt(Set::size).sum();
            double luceneRec  = recall(luceneMap);
            long luceneBigram = countBigramRules(luceneMap, luceneReader, "lucene");

            System.out.printf("  Lucene  %-37s │ %8d │ %5.1f%% │ %8d%n",
                    mc.label(), luceneRules, luceneRec * 100, luceneBigram);

            // Pharos
            Map<String, Set<String>> pharosMap = mineCombinedForProject(
                    pharosR, "pharos", mc.mode(), mc.param(), 6, 20, 15, 0.5);
            int pharosRules   = pharosMap.values().stream().mapToInt(Set::size).sum();
            long pharosBigram = countBigramRules(pharosMap, pharosR, "pharos");

            System.out.printf("  Pharos  %-37s │ %8d │ %5s  │ %8d%n",
                    mc.label(), pharosRules, "—", pharosBigram);
            System.out.println();
        }

        // ── Pharos qualitative sample for NPMI mode ───────────────────────────
        System.out.println(bar());
        System.out.println("  PHAROS SAMPLE RULES — NPMI mode (top 15 per class)");
        System.out.println(bar());

        Map<String, Set<String>> pharosNpmi = mineCombinedForProject(
                pharosR, "pharos", BigramScoreMode.NPMI, npmiTarget, 6, 20, 15, 0.5);

        List<String> pharosFocus = List.of("ConceptMiner", "SearchEngine",
                "LuceneIndexer", "KeywordSearchStrategy", "SynonymProvider",
                "ProjectIndexManager", "CallGraphBuilder");

        Map<String, TreeSet<String>> pharosInv = new TreeMap<>();
        for (Map.Entry<String, Set<String>> e : pharosNpmi.entrySet())
            for (String cls : e.getValue())
                pharosInv.computeIfAbsent(cls, k -> new TreeSet<>()).add(e.getKey());

        for (String cls : pharosFocus) {
            TreeSet<String> terms = pharosInv.get(cls);
            if (terms == null) { System.out.printf("  # %s  (no rules)%n%n", cls); continue; }
            List<String> top = terms.stream().sorted().limit(15).toList();
            System.out.printf("  # %s (%d rules)%n  %s%n%n",
                    cls, terms.size(), String.join(", ", top));
        }

        System.out.println(bar());
        idx.close();
        pharosR.close();
        System.out.println();
    }

    /** Full combined pipeline parameterised by project name and bigram scoring mode. */
    static Map<String, Set<String>> mineCombinedForProject(
            IndexReader reader, String project,
            BigramScoreMode bigramMode, double bigramParam,
            int methodTopK, int sentTopN, int fullTopN, double fullPpmiMin)
            throws IOException {

        List<ClassDoc> docs = loadClassDocsForProject(reader, project);
        Map<String, Set<String>> result = new HashMap<>();

        // Corpus-level frequency tables
        Map<String, Integer> tokenDf  = new HashMap<>();
        Map<String, Integer> bigramDf = new HashMap<>();
        for (ClassDoc d : docs) {
            String full = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            for (String t : uniqueTerms(full))               tokenDf.merge(t, 1, Integer::sum);
            for (String b : new HashSet<>(hybridBigrams(full))) bigramDf.merge(b, 1, Integer::sum);
        }
        int totalDocs = docs.size();

        // Source 1: class name tokens + bigrams + trigrams
        for (ClassDoc d : docs) {
            String[] toks = splitIdentifier(d.simpleName).split("\\s+");
            for (String t : toks) if (normalizeTerm(t) != null) put(result, t, d.simpleName);
            for (int i = 0; i < toks.length - 1; i++) {
                String bi = toks[i] + toks[i + 1];
                if (bi.length() >= 4) put(result, bi, d.simpleName);
            }
            for (int i = 0; i < toks.length - 2; i++) {
                String tri = toks[i] + toks[i + 1] + toks[i + 2];
                if (tri.length() >= 6) put(result, tri, d.simpleName);
            }
        }

        // Source 2: top-K public method tokens by inDegree
        emitTopPublicMethodTokensForProject(reader, project, methodTopK, result);

        // Source 3: package path segments
        for (ClassDoc d : docs) {
            if (d.qualName == null || d.qualName.isBlank()) continue;
            int ld = d.qualName.lastIndexOf('.');
            if (ld < 0) continue;
            for (String seg : d.qualName.substring(0, ld).split("\\."))
                if (seg.length() >= 3 && !GENERIC_PKG_SEGMENTS.contains(seg))
                    put(result, seg.toLowerCase(), d.simpleName);
        }

        // Source 4: @link / @see cross-references
        Map<String, ClassDoc> byName = new HashMap<>();
        for (ClassDoc d : docs) byName.put(d.simpleName.toLowerCase(), d);
        for (ClassDoc src : docs)
            for (String linked : src.links)
                if (byName.containsKey(linked.toLowerCase())) {
                    put(result, src.simpleName.toLowerCase(), linked);
                    put(result, linked.toLowerCase(), src.simpleName);
                }

        // Sources 5 & 6: PPMI on first sentence and full text
        for (ClassDoc d : docs) {
            emitPpmi(firstSentence(d.javadoc), tokenDf, totalDocs, d.simpleName, sentTopN, 0.0, result);
            String full = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            emitPpmi(full, tokenDf, totalDocs, d.simpleName, fullTopN, fullPpmiMin, result);
        }

        // Source 7: bigrams with selected scoring mode
        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            List<String> bigrams = hybridBigrams(text);
            Map<String, Integer> tf = new HashMap<>();
            for (String b : bigrams) tf.merge(b, 1, Integer::sum);
            int docLen = bigrams.size();
            if (docLen == 0) continue;

            // Score all bigrams
            List<Map.Entry<String, Double>> scored = new ArrayList<>();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                int df = bigramDf.getOrDefault(e.getKey(), 1);
                double ppmi = Math.log(((double) e.getValue() / docLen)
                                       / ((double) df / totalDocs));
                if (ppmi > 0) scored.add(Map.entry(e.getKey(), ppmi));
            }

            switch (bigramMode) {
                case ABSOLUTE -> scored.stream()
                    .filter(e -> e.getValue() >= bigramParam)
                    .forEach(e -> put(result, e.getKey(), d.simpleName));

                case NPMI -> scored.stream()
                    .filter(e -> (e.getValue() / Math.log(totalDocs)) >= bigramParam)
                    .forEach(e -> put(result, e.getKey(), d.simpleName));

                case PERCENTILE -> {
                    if (scored.isEmpty()) break;
                    scored.sort(Map.Entry.<String, Double>comparingByValue().reversed());
                    int keep = Math.max(1, (int) Math.ceil(scored.size() * bigramParam / 100.0));
                    scored.subList(0, Math.min(keep, scored.size()))
                          .forEach(e -> put(result, e.getKey(), d.simpleName));
                }
            }
        }

        // Post-processing: max length 20, no digit-start — free 15% rule reduction
        result.entrySet().removeIf(e -> {
            String t = e.getKey();
            return t.length() > 20 || Character.isDigit(t.charAt(0));
        });
        return result;
    }

    /** Counts bigram-source rules by checking which terms contain no spaces and
     *  are longer than the longest single camelCase token for that class. */
    private static long countBigramRules(Map<String, Set<String>> map,
                                          IndexReader reader, String project)
            throws IOException {
        // Proxy: count rules whose term looks like a bigram (≥8 chars, all lowercase,
        // not a plain class-name token). Not perfect but good enough for comparison.
        return map.entrySet().stream()
                  .filter(e -> e.getKey().length() >= 8)
                  .mapToLong(e -> e.getValue().size())
                  .sum();
    }

    // Project-aware versions of document loaders

    private static List<ClassDoc> loadClassDocsForProject(IndexReader reader,
                                                            String project)
            throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "class")),
                Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();
        List<ClassDoc> docs = new ArrayList<>();
        for (ScoreDoc sd : hits.scoreDocs) {
            Document doc    = sf.document(sd.doc);
            if (!project.equals(doc.get(DocumentMapper.F_PROJECT))) continue;
            if ("document".equals(nvl(doc.get("kind")))) continue;
            String cn = doc.get(DocumentMapper.F_CLASS_NAME);
            if (cn == null || cn.isBlank()) continue;
            docs.add(new ClassDoc(cn,
                    nvl(doc.get(DocumentMapper.F_QUALIFIED_CLASS)),
                    nvl(doc.get(DocumentMapper.F_JAVADOC)),
                    nvl(doc.get(DocumentMapper.F_BODY)),
                    extractLinks(nvl(doc.get(DocumentMapper.F_JAVADOC)))));
        }
        return docs;
    }

    private static void emitTopPublicMethodTokensForProject(IndexReader reader,
                                                             String project,
                                                             int topK,
                                                             Map<String, Set<String>> result)
            throws IOException {
        record ME(String name, int deg) {}
        Map<String, List<ME>> byClass = new HashMap<>();
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "method")),
                Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();
        for (ScoreDoc sd : hits.scoreDocs) {
            Document doc = sf.document(sd.doc);
            if (!project.equals(doc.get(DocumentMapper.F_PROJECT))) continue;
            if ("document".equals(nvl(doc.get("kind")))) continue;
            if (!"public".equals(doc.get(DocumentMapper.F_ACCESS))) continue;
            String cn = doc.get(DocumentMapper.F_CLASS_NAME);
            String mn = doc.get(DocumentMapper.F_METHOD_NAME);
            if (cn == null || mn == null) continue;
            int deg = 0;
            var f = doc.getField(DocumentMapper.F_IN_DEGREE);
            if (f != null && f.numericValue() != null) deg = f.numericValue().intValue();
            byClass.computeIfAbsent(cn, k -> new ArrayList<>()).add(new ME(mn, deg));
        }
        for (Map.Entry<String, List<ME>> e : byClass.entrySet())
            e.getValue().stream()
             .sorted(Comparator.comparingInt(ME::deg).reversed())
             .limit(topK)
             .forEach(me -> {
                 for (String tok : splitIdentifier(me.name()).split("\\s+"))
                     if (normalizeTerm(tok) != null) put(result, tok, e.getKey());
             });
    }

    // ── Performance benchmark ─────────────────────────────────────────────────

    /**
     * Times the combined strategy and baseline across multiple runs to produce
     * stable wall-clock measurements. Reports rules/second and the overhead
     * ratio relative to the baseline.
     */
    @Test
    @Order(4)
    void performance_combined_vs_baseline() throws IOException {
        int warmup = 2;
        int runs   = 5;

        System.out.println();
        System.out.println(bar());
        System.out.println("  PERFORMANCE — combined vs baseline  (warmup=" + warmup + "  runs=" + runs + ")");
        System.out.println(bar());

        // Warm up
        for (int i = 0; i < warmup; i++) {
            mineBaseline(luceneReader);
            mineCombined(luceneReader);
        }

        // Baseline timing
        long[] baseTimes = new long[runs];
        for (int i = 0; i < runs; i++) {
            long t0 = System.currentTimeMillis();
            Map<String, Set<String>> m = mineBaseline(luceneReader);
            baseTimes[i] = System.currentTimeMillis() - t0;
        }

        // Combined timing
        long[] combTimes = new long[runs];
        int combRules = 0;
        for (int i = 0; i < runs; i++) {
            long t0 = System.currentTimeMillis();
            Map<String, Set<String>> m = mineCombined(luceneReader);
            combTimes[i] = System.currentTimeMillis() - t0;
            if (i == runs - 1) combRules = m.values().stream().mapToInt(Set::size).sum();
        }

        int baseRules = mineBaseline(luceneReader).values().stream().mapToInt(Set::size).sum();

        long baseMedian = median(baseTimes);
        long combMedian = median(combTimes);

        System.out.printf("  %-26s │ %6s ms │ %8s rules │ %s rules/ms%n",
                "Strategy", "median", "count", "throughput");
        System.out.println("  " + "─".repeat(65));
        System.out.printf("  %-26s │ %6d ms │ %8d rules │ %.0f rules/ms%n",
                "Baseline (TF-IDF full)", baseMedian, baseRules,
                (double) baseRules / baseMedian);
        System.out.printf("  %-26s │ %6d ms │ %8d rules │ %.0f rules/ms%n",
                "★ Combined", combMedian, combRules,
                (double) combRules / combMedian);
        System.out.println("  " + "─".repeat(65));
        System.out.printf("  Overhead: %.1fx slower, %.1fx more rules, +%.1f%% recall%n",
                (double) combMedian / baseMedian,
                (double) combRules  / baseRules,
                (recall(mineCombined(luceneReader)) - recall(mineBaseline(luceneReader))) * 100);

        System.out.println();
        System.out.printf("  Run times (ms) — baseline: %s%n", Arrays.toString(baseTimes));
        System.out.printf("  Run times (ms) — combined: %s%n", Arrays.toString(combTimes));
        System.out.println(bar());
        System.out.println();
    }

    // ── Noise analysis ────────────────────────────────────────────────────────

    /**
     * Analyses what makes combined rules noisy: fan-out distribution (how many
     * classes a term maps to), term-length distribution, and the impact of a
     * max-fan-out cap on rule count vs recall.
     *
     * <p>Fan-out is the key signal: a term that maps to 200 classes is useless as
     * a synonym — it expands every query to everything.  A cap of 5–10 removes
     * the generic noise while keeping the discriminative rules.
     */
    @Test
    @Order(7)
    void noise_analysis() throws IOException {
        Map<String, Set<String>> combined = mineCombined(luceneReader);
        int totalRules = combined.values().stream().mapToInt(Set::size).sum();

        // ── Fan-out distribution ──────────────────────────────────────────────
        int[] fanoutBuckets = {1, 2, 3, 5, 10, 20, 50, Integer.MAX_VALUE};
        String[] bucketLabels = {"=1", "=2", "=3", "4–5", "6–10", "11–20", "21–50", "50+"};
        int[] counts = new int[fanoutBuckets.length];
        int[] ruleCounts = new int[fanoutBuckets.length];

        for (Map.Entry<String, Set<String>> e : combined.entrySet()) {
            int fanout = e.getValue().size();
            for (int i = 0; i < fanoutBuckets.length; i++) {
                if (fanout <= fanoutBuckets[i]) {
                    counts[i]++;
                    ruleCounts[i] += fanout;
                    break;
                }
            }
        }

        System.out.println();
        System.out.println(bar());
        System.out.println("  NOISE ANALYSIS — fan-out distribution (term → N classes)");
        System.out.println(bar());
        System.out.printf("  %-8s │ %8s │ %10s │ %8s │ %s%n",
                "Fanout", "Terms", "Rules", "% terms", "Sample terms");
        System.out.println("  " + "─".repeat(90));
        for (int i = 0; i < bucketLabels.length; i++) {
            if (counts[i] == 0) continue;
            // Sample: pick a few terms in this bucket to show
            int finalI = i;
            int lo = i == 0 ? 1 : fanoutBuckets[i - 1] + 1;
            List<String> samples = combined.entrySet().stream()
                    .filter(e -> e.getValue().size() >= lo && e.getValue().size() <= fanoutBuckets[finalI])
                    .map(Map.Entry::getKey)
                    .sorted()
                    .limit(5)
                    .toList();
            System.out.printf("  %-8s │ %8d │ %10d │ %7.1f%% │ %s%n",
                    bucketLabels[i], counts[i], ruleCounts[i],
                    100.0 * counts[i] / combined.size(),
                    String.join(", ", samples));
        }
        System.out.printf("  %-8s │ %8d │ %10d │%n", "TOTAL", combined.size(), totalRules);

        // ── Term length distribution ──────────────────────────────────────────
        System.out.println();
        System.out.println("  TERM LENGTH DISTRIBUTION");
        System.out.println("  " + "─".repeat(60));
        System.out.printf("  %-12s │ %8s │ %8s │ %s%n", "Length", "Terms", "% terms", "Sample");
        int[] lenBuckets = {4, 5, 6, 7, 8, 10, 15, Integer.MAX_VALUE};
        String[] lenLabels = {"4", "5", "6", "7", "8", "9–10", "11–15", "16+"};
        int[] lenCounts = new int[lenBuckets.length];
        for (String term : combined.keySet()) {
            int len = term.length();
            for (int i = 0; i < lenBuckets.length; i++) {
                if (len <= lenBuckets[i]) { lenCounts[i]++; break; }
            }
        }
        for (int i = 0; i < lenLabels.length; i++) {
            if (lenCounts[i] == 0) continue;
            int lo2 = i == 0 ? 0 : lenBuckets[i - 1] + 1;
            int finalI2 = i;
            List<String> s = combined.keySet().stream()
                    .filter(t -> t.length() >= lo2 && t.length() <= lenBuckets[finalI2])
                    .sorted().limit(4).toList();
            System.out.printf("  %-12s │ %8d │ %7.1f%% │ %s%n",
                    lenLabels[i], lenCounts[i],
                    100.0 * lenCounts[i] / combined.size(),
                    String.join(", ", s));
        }

        // ── Fan-out cap: recall vs rule count ─────────────────────────────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  FAN-OUT CAP — recall vs rule count (lower cap = fewer, sharper rules)");
        System.out.println(bar());
        System.out.printf("  %-14s │ %8s │ %8s │ %6s │ %s%n",
                "Max fan-out", "Terms", "Rules", "Recall", "Rules removed");
        System.out.println("  " + "─".repeat(65));

        int baselineRules = combined.values().stream().mapToInt(Set::size).sum();
        for (int cap : new int[]{1, 2, 3, 5, 10, 20, 50, Integer.MAX_VALUE}) {
            Map<String, Set<String>> capped = new HashMap<>();
            for (Map.Entry<String, Set<String>> e : combined.entrySet())
                if (e.getValue().size() <= cap) capped.put(e.getKey(), e.getValue());
            int cappedRules = capped.values().stream().mapToInt(Set::size).sum();
            double rec = recall(capped);
            String capLabel = cap == Integer.MAX_VALUE ? "∞ (all)" : String.valueOf(cap);
            System.out.printf("  %-14s │ %8d │ %8d │ %5.1f%% │ -%d (%+.1f%%)%n",
                    capLabel, capped.size(), cappedRules, rec * 100,
                    baselineRules - cappedRules,
                    100.0 * (cappedRules - baselineRules) / baselineRules);
        }
        // ── Targeted filters: measure each independently and combined ────────
        System.out.println();
        System.out.println(bar());
        System.out.println("  TARGETED FILTERS — rules removed vs recall lost");
        System.out.println(bar());
        System.out.printf("  %-38s │ %8s │ %6s │ %s%n",
                "Filter", "Rules", "Recall", "Rules removed");
        System.out.println("  " + "─".repeat(72));

        // Filter definitions
        record Filter(String name, java.util.function.Predicate<String> keep) {}
        List<Filter> filters = List.of(
            new Filter("none (baseline combined)",      t -> true),
            new Filter("no numeric-start (^[0-9])",    t -> !Character.isDigit(t.charAt(0))),
            new Filter("max length 20",                 t -> t.length() <= 20),
            new Filter("max length 16",                 t -> t.length() <= 16),
            new Filter("min length 5",                  t -> t.length() >= 5),
            new Filter("fanout ≤ 50",                   t -> combined.getOrDefault(t, Set.of()).size() <= 50),
            new Filter("fanout ≤ 20",                   t -> combined.getOrDefault(t, Set.of()).size() <= 20),
            new Filter("no-digit + maxLen20",           t -> !Character.isDigit(t.charAt(0)) && t.length() <= 20),
            new Filter("no-digit + maxLen20 + minLen5", t -> !Character.isDigit(t.charAt(0)) && t.length() <= 20 && t.length() >= 5),
            new Filter("no-digit + maxLen16 + minLen5", t -> !Character.isDigit(t.charAt(0)) && t.length() <= 16 && t.length() >= 5)
        );

        for (Filter f : filters) {
            Map<String, Set<String>> filtered = new HashMap<>();
            for (Map.Entry<String, Set<String>> e : combined.entrySet())
                if (f.keep().test(e.getKey())) filtered.put(e.getKey(), e.getValue());
            int rules  = filtered.values().stream().mapToInt(Set::size).sum();
            double rec = recall(filtered);
            System.out.printf("  %-38s │ %8d │ %5.1f%% │ -%d%n",
                    f.name(), rules, rec * 100, totalRules - rules);
        }
        System.out.println(bar());
        System.out.println();
    }

    // ── Query validation ──────────────────────────────────────────────────────

    /**
     * Evaluates the "corpus presence" filter: keep only terms that appear at
     * least once as an indexed token in the Lucene corpus.
     *
     * <p>Terms with df=0 across all text fields were never written anywhere in
     * the codebase — no developer mentioned them — so users are unlikely to
     * type them as queries.  The key question is whether this also incorrectly
     * removes useful bigrams (e.g. {@code weakand}, {@code skiplist}) that we
     * generated as compound forms from prose.
     *
     * <p>Reports:
     * <ul>
     *   <li>Overall filter impact (rules removed, recall lost)
     *   <li>Which golden-pair terms get filtered out
     *   <li>Split: single-word terms vs bigrams
     * </ul>
     */
    @Test
    @Order(8)
    void query_validation_analysis() throws IOException {
        Map<String, Set<String>> combined = mineCombined(luceneReader);

        System.out.println();
        System.out.println(bar());
        System.out.println("  QUERY VALIDATION — target class existence filter");
        System.out.println("  A rule 'term => ClassName' is dead if className is not");
        System.out.println("  in F_CLASS_NAME — the SynonymGraphFilter expansion finds nothing.");
        System.out.println(bar());

        // For each rule term → {ClassA, ClassB, ...}, keep only target classes
        // whose lowercase name actually appears as a token in F_CLASS_NAME.
        Map<String, Set<String>> validated   = new HashMap<>();
        Map<String, Set<String>> deadTargets = new HashMap<>();  // term → dead classes

        for (Map.Entry<String, Set<String>> e : combined.entrySet()) {
            String term = e.getKey();
            Set<String> live = new HashSet<>();
            Set<String> dead = new HashSet<>();
            for (String cls : e.getValue()) {
                org.apache.lucene.index.Term t =
                        new org.apache.lucene.index.Term(
                                DocumentMapper.F_CLASS_NAME, cls.toLowerCase());
                if (luceneReader.docFreq(t) > 0) live.add(cls);
                else                               dead.add(cls);
            }
            if (!live.isEmpty()) validated.put(term, live);
            if (!dead.isEmpty()) deadTargets.put(term, dead);
        }

        int totalRules     = combined.values().stream().mapToInt(Set::size).sum();
        int validatedRules = validated.values().stream().mapToInt(Set::size).sum();
        int removedRules   = totalRules - validatedRules;
        double recBefore   = recall(combined);
        double recAfter    = recall(validated);

        System.out.printf("  Before: %d terms → %d rules  (%.1f%% recall)%n",
                combined.size(), totalRules, recBefore * 100);
        System.out.printf("  After:  %d terms → %d rules  (%.1f%% recall)%n",
                validated.size(), validatedRules, recAfter * 100);
        System.out.printf("  Removed %d dead-target rules (%.1f%% of total),"
                        + " recall loss %.1f%%%n",
                removedRules, 100.0 * removedRules / totalRules,
                (recBefore - recAfter) * 100);

        // Which golden pairs lose their target?
        System.out.println();
        System.out.println("  GOLDEN PAIRS WHOSE TARGET IS NOT IN INDEX:");
        System.out.println("  " + "─".repeat(65));
        boolean anyLost = false;
        for (GoldenPair gp : GOLDEN) {
            org.apache.lucene.index.Term t =
                    new org.apache.lucene.index.Term(
                            DocumentMapper.F_CLASS_NAME, gp.expectedClass().toLowerCase());
            if (luceneReader.docFreq(t) == 0) {
                System.out.printf("  %-28s → %-34s [%s]%n",
                        gp.term(), gp.expectedClass(), gp.domain());
                anyLost = true;
            }
        }
        if (!anyLost) System.out.println("  (all golden target classes exist in index)");

        // How many unique dead class names?
        Set<String> deadClasses = deadTargets.values().stream()
                .flatMap(Set::stream).collect(Collectors.toSet());
        System.out.println();
        System.out.printf("  %d unique class names not in index (sample):%n", deadClasses.size());
        deadClasses.stream().sorted().limit(20)
                   .forEach(c -> System.out.printf("    %s%n", c));

        System.out.println(bar());
        System.out.println();
    }

    // ── Golden set integrity check ────────────────────────────────────────────

    /**
     * Validates each golden pair (trigger, expectedClass) against the live index:
     * <ol>
     *   <li><b>Target exists</b> — {@code expectedClass.toLowerCase()} must appear
     *       in {@code F_CLASS_NAME}.  If not, the pair is stale (class renamed/removed).
     *   <li><b>Trigger is a vocabulary gap</b> — {@code trigger} must NOT appear as
     *       a token in any text field.  If it does, BM25 already handles it without
     *       synonyms — the pair doesn't test synonym bridging at all.
     * </ol>
     */
    @Test
    @Order(9)
    void golden_set_integrity_check() throws IOException {
        String[] textFields = {
            DocumentMapper.F_BODY, DocumentMapper.F_JAVADOC, DocumentMapper.F_SIGNATURE,
            DocumentMapper.F_METHOD_NAME, DocumentMapper.F_CLASS_NAME
        };

        System.out.println();
        System.out.println(bar());
        System.out.println("  GOLDEN SET INTEGRITY");
        System.out.println("  PASS = target in index AND trigger NOT in index");
        System.out.println(bar());
        System.out.printf("  %-28s → %-38s │ target │ trigger │ Domain%n",
                "Trigger", "Expected class");
        System.out.println("  " + "─".repeat(100));

        int staleTarget = 0, triggerInIndex = 0, both = 0, clean = 0;

        for (GoldenPair gp : GOLDEN) {
            // 1. Target exists?
            long targetDf = luceneReader.docFreq(
                    new org.apache.lucene.index.Term(
                            DocumentMapper.F_CLASS_NAME, gp.expectedClass().toLowerCase()));
            boolean targetMissing = targetDf == 0;

            // 2. Trigger is absent from index (vocabulary gap)?
            long triggerDf = 0;
            for (String field : textFields)
                triggerDf += luceneReader.docFreq(
                        new org.apache.lucene.index.Term(field, gp.term()));
            boolean triggerPresent = triggerDf > 0;

            String targetMark  = targetMissing  ? "MISSING" : String.format("df=%-4d", targetDf);
            String triggerMark = triggerPresent ? String.format("df=%-5d", triggerDf) : "absent ";

            if (targetMissing || triggerPresent) {
                String flag = targetMissing && triggerPresent ? "BOTH BAD"
                            : targetMissing ? "STALE TARGET"
                            : "TRIGGER IN IDX";
                System.out.printf("  %-28s → %-38s │ %-7s │ %-7s │ %-8s  ← %s%n",
                        gp.term(), gp.expectedClass(),
                        targetMark, triggerMark, gp.domain(), flag);
                if (targetMissing && triggerPresent) both++;
                else if (targetMissing)   staleTarget++;
                else                      triggerInIndex++;
            }
        }

        System.out.println("  " + "─".repeat(100));
        System.out.printf("  Stale targets (class not in index):               %d%n", staleTarget);
        System.out.printf("  Trigger already in index (BM25 handles it):       %d%n", triggerInIndex);
        System.out.printf("  Both issues:                                       %d%n", both);
        System.out.printf("  Clean pairs (true gap: target exists, trigger not): %d/%d%n",
                GOLDEN.size() - staleTarget - triggerInIndex - both, GOLDEN.size());

        System.out.println();
        System.out.println("  CLEAN PAIRS — the only ones that truly test synonym bridging:");
        System.out.println("  " + "─".repeat(65));
        for (GoldenPair gp : GOLDEN) {
            long targetDf  = luceneReader.docFreq(new org.apache.lucene.index.Term(
                    DocumentMapper.F_CLASS_NAME, gp.expectedClass().toLowerCase()));
            long triggerDf = 0;
            for (String field : textFields)
                triggerDf += luceneReader.docFreq(
                        new org.apache.lucene.index.Term(field, gp.term()));
            if (targetDf > 0 && triggerDf == 0)
                System.out.printf("  %-28s → %-34s [%s]%n",
                        gp.term(), gp.expectedClass(), gp.domain());
        }
        System.out.println(bar());
        System.out.println();
    }

    // ── Golden set candidate scanner ─────────────────────────────────────────

    /**
     * Scans a large hand-curated candidate list and prints those that pass the
     * two integrity constraints:
     * <ol>
     *   <li>Trigger df = 0 across all text fields (true vocabulary gap)
     *   <li>Target class df > 0 in F_CLASS_NAME (class exists in index)
     * </ol>
     * Run this to build/refresh the GOLDEN list.
     */
    @Test
    @Order(10)
    void golden_candidate_scan() throws IOException {
        record Candidate(String trigger, String target, String domain) {}

        List<Candidate> candidates = List.of(
            // ── NRT / reading ────────────────────────────────────────────────
            new Candidate("ramresident",        "NRTCachingDirectory",                    "nrt"),
            new Candidate("offheapstorage",     "OffHeapFSTStore",                        "nrt"),
            new Candidate("cachingnrt",         "NRTCachingDirectory",                    "nrt"),
            new Candidate("nrtreplication",     "ReplicaNode",                            "nrt"),
            new Candidate("indexreplica",       "ReplicaNode",                            "nrt"),
            new Candidate("replicapull",        "ReplicaNode",                            "nrt"),
            new Candidate("lazysearcher",       "SearcherManager",                        "nrt"),
            new Candidate("warmup",             "IndexReaderWarmer",                      "nrt"),
            new Candidate("readerwarmer",       "IndexReaderWarmer",                      "nrt"),
            new Candidate("openifchanged",      "DirectoryReader",                        "nrt"),
            // ── Codec / storage ──────────────────────────────────────────────
            new Candidate("skiplist",           "MultiLevelSkipListReader",               "codec"),
            new Candidate("kdtree",             "BKDWriter",                              "codec"),
            new Candidate("blockmax",           "BlockMaxConjunctionScorer",              "codec"),
            new Candidate("deltapack",          "DeltaPackedLongValues",                  "codec"),
            new Candidate("offheapmmap",        "MemorySegmentIndexInput",                "codec"),
            new Candidate("memorymapped",       "MMapDirectory",                          "codec"),
            new Candidate("mmapfile",           "MMapDirectory",                          "codec"),
            new Candidate("packedints",         "PackedInts",                             "codec"),
            new Candidate("finitestatemachine", "FST",                                    "codec"),
            new Candidate("bytestore",          "PagedBytes",                             "codec"),
            new Candidate("pagedmemory",        "PagedBytes",                             "codec"),
            new Candidate("fixedbitset",        "FixedBitSet",                            "codec"),
            new Candidate("sparsebits",         "SparseFixedBitSet",                      "codec"),
            new Candidate("roaringdocset",      "RoaringDocIdSet",                        "codec"),
            new Candidate("docbitset",          "FixedBitSet",                            "codec"),
            // ── Analysis ─────────────────────────────────────────────────────
            new Candidate("synonymexpansion",   "SynonymGraphFilter",                     "analysis"),
            new Candidate("decompound",         "DictionaryCompoundWordTokenFilter",       "analysis"),
            new Candidate("compoundword",       "DictionaryCompoundWordTokenFilter",       "analysis"),
            new Candidate("hyphendecomp",       "HyphenationCompoundWordTokenFilter",      "analysis"),
            new Candidate("edgengram",          "EdgeNGramTokenFilter",                   "analysis"),
            new Candidate("prefixtoken",        "EdgeNGramTokenFilter",                   "analysis"),
            new Candidate("wordstemmer",        "PorterStemFilter",                       "analysis"),
            new Candidate("snowballstem",       "SnowballFilter",                         "analysis"),
            new Candidate("charfolding",        "ASCIIFoldingFilter",                     "analysis"),
            new Candidate("accentstrip",        "ASCIIFoldingFilter",                     "analysis"),
            new Candidate("unicodefold",        "ASCIIFoldingFilter",                     "analysis"),
            new Candidate("wordbreaking",       "WordBreakSpellChecker",                  "analysis"),
            new Candidate("splitwords",         "WordBreakSpellChecker",                  "analysis"),
            new Candidate("patternreplace",     "PatternReplaceFilter",                   "analysis"),
            new Candidate("phonetic",           "BeiderMorseFilter",                      "analysis"),
            // ── Scoring / queries ────────────────────────────────────────────
            new Candidate("weakand",            "WANDScorer",                             "scoring"),
            new Candidate("vectorspace",        "TFIDFSimilarity",                        "scoring"),
            new Candidate("morethreshold",      "MoreLikeThis",                           "search"),
            new Candidate("dismax",             "DisjunctionMaxQuery",                    "scoring"),
            new Candidate("disjunctionmax",     "DisjunctionMaxQuery",                    "scoring"),
            new Candidate("bestfield",          "DisjunctionMaxQuery",                    "scoring"),
            new Candidate("functionboost",      "FunctionScoreQuery",                     "scoring"),
            new Candidate("customscore",        "FunctionScoreQuery",                     "scoring"),
            new Candidate("spannear",           "SpanNearQuery",                          "scoring"),
            new Candidate("spanwithin",         "SpanWithinQuery",                        "scoring"),
            new Candidate("impactscoring",      "ImpactsDISI",                            "scoring"),
            new Candidate("maxscoreblock",      "MaxScoreCache",                          "scoring"),
            new Candidate("earlyterm",          "EarlyTerminatingSortingCollector",       "scoring"),
            new Candidate("sortedearlyterm",    "EarlyTerminatingSortingCollector",       "scoring"),
            new Candidate("topkdocument",       "TopScoreDocCollector",                   "scoring"),
            // ── Facet ────────────────────────────────────────────────────────
            new Candidate("taxonomyindex",      "TaxonomyWriter",                         "facet"),
            new Candidate("facettaxonomy",      "DirectoryTaxonomyWriter",                "facet"),
            new Candidate("categorylabel",      "FacetLabel",                             "facet"),
            new Candidate("drillcount",         "DrillSideways",                          "facet"),
            new Candidate("sidewayfacet",       "DrillSideways",                          "facet"),
            new Candidate("fastfacet",          "SortedSetDocValuesFacetCounts",          "facet"),
            new Candidate("facetcollect",       "FacetsCollector",                        "facet"),
            new Candidate("labelpath",          "FacetLabel",                             "facet"),
            new Candidate("hierarchyfacet",     "TaxonomyFacetCounts",                    "facet"),
            new Candidate("floatassociation",   "TaxonomyFacetFloatAssociations",         "facet"),
            // ── Indexing ─────────────────────────────────────────────────────
            new Candidate("softdeleted",        "SoftDeletesRetentionMergePolicy",        "indexing"),
            new Candidate("nonadjacent",        "TieredMergePolicy",                      "indexing"),
            new Candidate("segmentflush",       "DocumentsWriterFlushControl",            "indexing"),
            new Candidate("flushthread",        "DocumentsWriterFlushControl",            "indexing"),
            new Candidate("backgroundmerge",    "ConcurrentMergeScheduler",               "indexing"),
            new Candidate("mergethread",        "ConcurrentMergeScheduler",               "indexing"),
            new Candidate("indexgeneration",    "IndexCommit",                            "indexing"),
            new Candidate("segmentcommit",      "SegmentCommitInfo",                      "indexing"),
            new Candidate("docsperfield",       "FieldInfo",                              "indexing"),
            new Candidate("fieldoptions",       "FieldInfo",                              "indexing"),
            new Candidate("normvalue",          "Norms",                                  "indexing"),
            new Candidate("deletionpolicy",     "IndexDeletionPolicy",                    "indexing"),
            new Candidate("keeplatest",         "KeepOnlyLastCommitDeletionPolicy",       "indexing"),
            new Candidate("indexsorting",       "IndexSortSortedNumericDocValuesRangeQuery", "indexing"),
            new Candidate("bpreorder",          "BPIndexReorderer",                       "indexing"),
            // ── Suggest ──────────────────────────────────────────────────────
            new Candidate("weightedlookup",     "AnalyzingSuggester",                     "suggest"),
            new Candidate("surfaceform",        "AnalyzingSuggester",                     "suggest"),
            new Candidate("infixmatch",         "AnalyzingInfixSuggester",                "suggest"),
            new Candidate("contextcompletion",  "ContextSuggestField",                    "suggest"),
            new Candidate("fuzzycompletion",    "FuzzyCompletionQuery",                   "suggest"),
            new Candidate("regexcompletion",    "RegexCompletionQuery",                   "suggest"),
            new Candidate("tstlookup",          "TSTLookup",                              "suggest"),
            new Candidate("wfstlookup",         "WFSTCompletionLookup",                   "suggest"),
            new Candidate("fstcompletion",      "FSTCompletion",                          "suggest"),
            new Candidate("inputiterator",      "InputIterator",                          "suggest"),
            // ── Vector / kNN ─────────────────────────────────────────────────
            new Candidate("scalarquantize",     "ScalarQuantizer",                        "vector"),
            new Candidate("binaryquantize",     "BinaryQuantizer",                        "vector"),
            new Candidate("vectorsimilarity",   "VectorSimilarityFunction",               "vector"),
            new Candidate("quantizedvector",    "ScalarQuantizedVectorValues",            "vector"),
            new Candidate("graphnav",           "HnswGraph",                              "vector"),
            new Candidate("hnswsearch",         "HnswGraphSearcher",                      "vector"),
            new Candidate("offheapvector",      "OffHeapFloatVectorValues",               "vector"),
            new Candidate("onheapvector",       "OnHeapFloatVectorValues",                "vector"),
            new Candidate("densevector",        "KnnVectorField",                         "vector"),
            new Candidate("vectorencoding",     "VectorEncoding",                         "vector"),
            // ── Span / interval ──────────────────────────────────────────────
            new Candidate("spancontain",        "SpanContainingQuery",                    "span"),
            new Candidate("spannot",            "SpanNotQuery",                           "span"),
            new Candidate("spanfirst",          "SpanFirstQuery",                         "span"),
            new Candidate("intervalfilter",     "IntervalFilter",                         "span"),
            new Candidate("containedby",        "SpanWithinQuery",                        "span"),
            new Candidate("orderedproximity",   "SpanNearQuery",                          "span"),
            // ── DocValues ────────────────────────────────────────────────────
            new Candidate("sortednumeric",      "SortedNumericDocValues",                 "docvalues"),
            new Candidate("perdocnumeric",      "NumericDocValues",                       "docvalues"),
            new Candidate("columnar",           "DocValues",                              "docvalues"),
            new Candidate("columnstore",        "DocValues",                              "docvalues"),
            new Candidate("docvalueswrite",     "DocValuesConsumer",                      "docvalues"),
            new Candidate("binaryfield",        "BinaryDocValues",                        "docvalues"),
            // ── Misc ─────────────────────────────────────────────────────────
            new Candidate("lockfree",           "FSLockFactory",                          "misc"),
            new Candidate("nativelock",         "NativeFSLock",                           "misc"),
            new Candidate("indexlocking",       "LockFactory",                            "misc"),
            new Candidate("rotatinglog",        "InfoStream",                             "misc"),
            new Candidate("geohash",            "GeoHashUtils",                           "misc"),
            new Candidate("spatialpoint",       "LatLonPoint",                            "misc"),
            new Candidate("latlonbox",          "LatLonBoundingBox",                      "misc"),
            new Candidate("geopolygon",         "Polygon",                                "misc"),
            // ── Second batch ─────────────────────────────────────────────────
            new Candidate("heapgraph",          "OnHeapHnswGraph",                        "vector"),
            new Candidate("approximateknn",     "HnswGraphSearcher",                      "vector"),
            new Candidate("compressedstorage",  "StoredFieldsFormat",                     "codec"),
            new Candidate("storedformat",       "StoredFieldsFormat",                     "codec"),
            new Candidate("bulkread",           "StoredFieldsReader",                     "codec"),
            new Candidate("termdictwrite",      "TermsHashPerField",                      "codec"),
            new Candidate("payloadfilter",      "DelimitedPayloadTokenFilter",            "analysis"),
            new Candidate("shinglefilter",      "ShingleFilter",                          "analysis"),
            new Candidate("elisionfilter",      "ElisionFilter",                          "analysis"),
            new Candidate("keywordmark",        "KeywordMarkerFilter",                    "analysis"),
            new Candidate("tokentype",          "TypeAttribute",                          "analysis"),
            new Candidate("queryrescorer",      "QueryRescorer",                          "scoring"),
            new Candidate("queryboosting",      "BoostQuery",                             "scoring"),
            new Candidate("rescorewindow",      "QueryRescorer",                          "scoring"),
            new Candidate("levelmerge",         "LogMergePolicy",                         "indexing"),
            new Candidate("logpolicy",          "LogMergePolicy",                         "indexing"),
            new Candidate("compactionpolicy",   "MergePolicy",                            "indexing"),
            new Candidate("updatebydocvalue",   "DocValuesUpdate",                        "indexing"),
            new Candidate("docvalueupdate",     "DocValuesUpdate",                        "indexing"),
            new Candidate("labelcount",         "TaxonomyFacetCounts",                    "facet"),
            new Candidate("completionterm",     "CompletionTerms",                        "suggest"),
            new Candidate("spellsuggest",       "SpellChecker",                           "suggest"),
            new Candidate("termsuggestion",     "SuggestWord",                            "suggest"),
            new Candidate("intervalquery",      "IntervalQuery",                          "span"),
            new Candidate("positionrange",      "SpanPositionRangeQuery",                 "span"),
            new Candidate("multivaluedfield",   "SortedSetDocValues",                     "docvalues"),
            new Candidate("storedfield",        "StoredFields",                           "misc"),
            new Candidate("termvectorread",     "TermVectors",                            "misc"),
            new Candidate("fieldstats",         "FieldInfo",                              "misc"),
            new Candidate("indexstats",         "IndexWriter",                            "misc"),
            new Candidate("lockdirectory",      "LockFactory",                            "misc"),
            new Candidate("mmapread",           "MMapDirectory",                          "codec"),
            new Candidate("packedarray",        "PackedLongValues",                       "codec"),
            new Candidate("directpostings",     "DirectPostingsFormat",                   "codec"),
            new Candidate("inlinepostings",     "DirectPostingsFormat",                   "codec"),
            new Candidate("rampostings",        "RAMPostingsFormat",                      "codec"),
            new Candidate("queryrewrite",       "MultiTermQuery",                         "scoring"),
            new Candidate("termexpansion",      "MultiTermQuery",                         "scoring"),
            new Candidate("wildcardexpand",     "WildcardQuery",                          "scoring"),
            new Candidate("regexmatch",         "RegexpQuery",                            "scoring")
        );

        String[] textFields = {
            DocumentMapper.F_BODY, DocumentMapper.F_JAVADOC, DocumentMapper.F_SIGNATURE,
            DocumentMapper.F_METHOD_NAME, DocumentMapper.F_CLASS_NAME
        };

        System.out.println();
        System.out.println(bar());
        System.out.println("  GOLDEN CANDIDATE SCAN — trigger df=0, target df>0");
        System.out.println(bar());
        System.out.printf("  %-28s → %-40s │ %6s │ %6s │ %s%n",
                "Trigger", "Target", "trgDf", "tgtDf", "Domain");
        System.out.println("  " + "─".repeat(95));

        int clean = 0;
        List<Candidate> cleanList = new ArrayList<>();
        for (Candidate c : candidates) {
            long triggerDf = 0;
            for (String f : textFields)
                triggerDf += luceneReader.docFreq(
                        new org.apache.lucene.index.Term(f, c.trigger()));
            long targetDf = luceneReader.docFreq(
                    new org.apache.lucene.index.Term(
                            DocumentMapper.F_CLASS_NAME, c.target().toLowerCase()));

            String status = triggerDf == 0 && targetDf > 0 ? "✓ CLEAN"
                          : triggerDf  > 0 && targetDf > 0 ? "trigger in idx"
                          : targetDf  == 0                  ? "target missing"
                          :                                   "both bad";
            if (triggerDf == 0 && targetDf > 0) { clean++; cleanList.add(c); }
            System.out.printf("  %-28s → %-40s │ %6d │ %6d │ %-8s  %s%n",
                    c.trigger(), c.target(), triggerDf, targetDf, c.domain(), status);
        }
        System.out.println("  " + "─".repeat(95));
        System.out.printf("  Clean pairs: %d / %d%n", clean, candidates.size());
        System.out.println();
        System.out.println("  CLEAN LIST (copy into GOLDEN):");
        cleanList.forEach(c -> System.out.printf(
                "  new GoldenPair(\"%-28s\", \"%-40s\", \"%s\"),%n",
                c.trigger() + "\"", c.target() + "\"", c.domain()));
        System.out.println(bar());
        System.out.println();
    }

    // ── Rule inspection ───────────────────────────────────────────────────────

    /**
     * Inverts the combined map and prints per-class synonym rules in synonyms.txt
     * format so the output quality can be read directly.
     *
     * <p>Two sections:
     * <ol>
     *   <li>Focused view — full rule list for a hand-picked set of classes that
     *       exercise every source (NRT, codec, scoring, analysis, suggest).
     *   <li>Random sample — 20 additional classes chosen alphabetically so the
     *       output isn't biased toward well-known classes.
     * </ol>
     */
    @Test
    @Order(5)
    void inspect_combined_rules() throws IOException {
        Map<String, Set<String>> map = mineCombined(luceneReader);

        // Invert: className → sorted set of terms that point to it
        Map<String, TreeSet<String>> inverted = new TreeMap<>();
        for (Map.Entry<String, Set<String>> e : map.entrySet()) {
            String term = e.getKey();
            for (String cls : e.getValue())
                inverted.computeIfAbsent(cls, k -> new TreeSet<>()).add(term);
        }

        List<String> focused = List.of(
            "WANDScorer", "DirectoryReader", "IndexWriter", "BM25Similarity",
            "MultiLevelSkipListReader", "TieredMergePolicy", "Analyzer",
            "FuzzyQuery", "UnifiedHighlighter", "AnalyzingSuggester",
            "HnswGraphBuilder", "CheckIndex", "SearcherManager", "LRUQueryCache"
        );

        System.out.println();
        System.out.println(bar());
        System.out.println("  COMBINED RULES — focused classes");
        System.out.println(bar());
        printClassRules(focused, inverted);

        // Alphabetical sample: every 25th class not already in focused list
        List<String> sample = inverted.keySet().stream()
                .filter(c -> !focused.contains(c))
                .sorted()
                .filter(c -> inverted.get(c).size() >= 3) // only classes with real coverage
                .toList();
        List<String> stride = new ArrayList<>();
        for (int i = 0; i < sample.size(); i += Math.max(1, sample.size() / 20))
            stride.add(sample.get(i));

        System.out.println(bar());
        System.out.println("  COMBINED RULES — alphabetical stride sample");
        System.out.println(bar());
        printClassRules(stride, inverted);
        System.out.println(bar());
        System.out.printf("  Total: %d classes with rules, %d unique terms%n",
                inverted.size(), map.size());
        System.out.println(bar());
        System.out.println();
    }

    /**
     * Runs a tagged version of the combined strategy that records which source
     * produced each rule, then prints up to {@code limit} rules per class with
     * a source tag suffix: [N]=ClassName, [M]=MethodName, [P]=Package,
     * [L]=Link, [S]=FirstSentencePPMI, [F]=FullPPMI, [B]=BigramPPMI.
     */
    @Test
    @Order(6)
    void inspect_rules_with_source_tags() throws IOException {
        List<String> focused = List.of(
            "WANDScorer", "DirectoryReader", "IndexWriter", "BM25Similarity",
            "MultiLevelSkipListReader", "TieredMergePolicy", "Analyzer",
            "FuzzyQuery", "UnifiedHighlighter", "SearcherManager"
        );

        // Run tagged mining
        Map<String, Map<String, String>> tagged = mineCombinedTagged(luceneReader, focused);

        System.out.println();
        System.out.println(bar());
        System.out.println("  COMBINED RULES WITH SOURCE TAGS (max 25/class)");
        System.out.println("  [N]=ClassName [M]=Method [P]=Package [L]=Link");
        System.out.println("  [S]=1stSentPPMI [F]=FullPPMI [B]=BigramPPMI");
        System.out.println(bar());

        for (String cls : focused) {
            Map<String, String> terms = tagged.get(cls);
            if (terms == null || terms.isEmpty()) {
                System.out.printf("  %-40s (no rules)%n", cls);
                continue;
            }
            System.out.printf("  # %s (%d rules total)%n", cls, terms.size());
            // Sort: name-derived first ([N],[M],[P],[L]) then text ([S],[F],[B])
            terms.entrySet().stream()
                .sorted(Comparator.comparing((Map.Entry<String,String> e) ->
                        sourceOrder(e.getValue()))
                    .thenComparing(Map.Entry::getKey))
                .limit(25)
                .forEach(e -> System.out.printf("    %-35s [%s]%n", e.getKey(), e.getValue()));
            System.out.println();
        }
        System.out.println(bar());
        System.out.println();
    }

    private static int sourceOrder(String tag) {
        return switch (tag) {
            case "N" -> 0; case "M" -> 1; case "P" -> 2; case "L" -> 3;
            case "S" -> 4; case "F" -> 5; case "B" -> 6;
            default  -> 7;
        };
    }

    /** Variant of mineCombined that records the source tag per (term, class) rule. */
    private static Map<String, Map<String, String>> mineCombinedTagged(
            IndexReader reader, List<String> targetClasses) throws IOException {

        Set<String> targets = new HashSet<>(targetClasses);
        // Map: className → (term → sourceTag)
        Map<String, Map<String, String>> tagged = new HashMap<>();
        for (String c : targetClasses) tagged.put(c, new LinkedHashMap<>());

        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Integer> tokenDf  = new HashMap<>();
        Map<String, Integer> bigramDf = new HashMap<>();
        for (ClassDoc d : docs) {
            String full = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            for (String t : uniqueTerms(full))               tokenDf.merge(t, 1, Integer::sum);
            for (String b : new HashSet<>(hybridBigrams(full))) bigramDf.merge(b, 1, Integer::sum);
        }
        int N = docs.size();

        for (ClassDoc d : docs) {
            if (!targets.contains(d.simpleName)) continue;
            Map<String, String> m = tagged.get(d.simpleName);

            // [N] class name tokens + bigrams
            String[] toks = splitIdentifier(d.simpleName).split("\\s+");
            for (String t : toks) if (normalizeTerm(t) != null) m.putIfAbsent(t, "N");
            for (int i = 0; i < toks.length - 1; i++) {
                String bi = toks[i] + toks[i+1];
                if (bi.length() >= 4) m.putIfAbsent(bi, "N");
            }

            // [S] first-sentence PPMI
            String sent = firstSentence(d.javadoc);
            scoredTerms(sent, tokenDf, N, 20, 0.0)
                .forEach(t -> m.putIfAbsent(t, "S"));

            // [F] full-text PPMI
            String full = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            scoredTerms(full, tokenDf, N, 15, 0.5)
                .forEach(t -> m.putIfAbsent(t, "F"));

            // [B] bigram PPMI
            List<String> bigrams = hybridBigrams(full);
            Map<String, Integer> btf = new HashMap<>();
            for (String b : bigrams) btf.merge(b, 1, Integer::sum);
            int docLen = bigrams.size();
            if (docLen > 0)
                btf.entrySet().stream()
                   .filter(e -> Math.log(((double)e.getValue()/docLen)
                                / ((double)bigramDf.getOrDefault(e.getKey(),1)/N)) >= 1.0)
                   .forEach(e -> m.putIfAbsent(e.getKey(), "B"));
        }

        // [M] method names
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs mhits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "method")), Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();
        // Group public methods by class, sort by inDegree, take top-10
        record ME(String name, int deg) {}
        Map<String, List<ME>> methodsByClass = new HashMap<>();
        for (ScoreDoc sd : mhits.scoreDocs) {
            Document doc = sf.document(sd.doc);
            if (!"lucene".equals(doc.get(DocumentMapper.F_PROJECT))) continue;
            if ("document".equals(nvl(doc.get("kind")))) continue;
            if (!"public".equals(doc.get(DocumentMapper.F_ACCESS))) continue;
            String cn = doc.get(DocumentMapper.F_CLASS_NAME);
            String mn = doc.get(DocumentMapper.F_METHOD_NAME);
            if (cn == null || mn == null || !targets.contains(cn)) continue;
            int deg = 0;
            var f = doc.getField(DocumentMapper.F_IN_DEGREE);
            if (f != null && f.numericValue() != null) deg = f.numericValue().intValue();
            methodsByClass.computeIfAbsent(cn, k -> new ArrayList<>()).add(new ME(mn, deg));
        }
        for (Map.Entry<String, List<ME>> e : methodsByClass.entrySet()) {
            Map<String, String> m = tagged.get(e.getKey());
            e.getValue().stream()
             .sorted(Comparator.comparingInt(ME::deg).reversed())
             .limit(10)
             .forEach(me -> {
                 for (String t : splitIdentifier(me.name()).split("\\s+"))
                     if (normalizeTerm(t) != null) m.putIfAbsent(t, "M");
             });
        }

        // [P] package segments
        for (ClassDoc d : docs) {
            if (!targets.contains(d.simpleName) || d.qualName == null) continue;
            int ld = d.qualName.lastIndexOf('.');
            if (ld < 0) continue;
            Map<String, String> m = tagged.get(d.simpleName);
            for (String seg : d.qualName.substring(0, ld).split("\\."))
                if (seg.length() >= 3 && !GENERIC_PKG_SEGMENTS.contains(seg))
                    m.putIfAbsent(seg.toLowerCase(), "P");
        }

        // [L] @link cross-references
        Map<String, ClassDoc> byName = new HashMap<>();
        for (ClassDoc d : docs) byName.put(d.simpleName.toLowerCase(), d);
        for (ClassDoc src : docs) {
            for (String linked : src.links) {
                if (targets.contains(linked) && byName.containsKey(src.simpleName.toLowerCase()))
                    tagged.get(linked).putIfAbsent(src.simpleName.toLowerCase(), "L");
                if (targets.contains(src.simpleName) && byName.containsKey(linked.toLowerCase()))
                    tagged.get(src.simpleName).putIfAbsent(linked.toLowerCase(), "L");
            }
        }

        return tagged;
    }

    /** Returns top-N PPMI-scored terms from text (no side effects). */
    private static List<String> scoredTerms(String text, Map<String, Integer> docFreq,
                                             int totalDocs, int topN, double minPpmi) {
        Map<String, Integer> tf = termFreq(text);
        int docLen = tf.values().stream().mapToInt(i -> i).sum();
        if (docLen == 0) return List.of();
        return tf.entrySet().stream()
                .filter(e -> normalizeTerm(e.getKey()) != null)
                .map(e -> {
                    int df = docFreq.getOrDefault(e.getKey(), 1);
                    double ppmi = Math.log(((double)e.getValue()/docLen)/((double)df/totalDocs));
                    return Map.entry(e.getKey(), ppmi);
                })
                .filter(e -> e.getValue() >= minPpmi)
                .sorted(Map.Entry.<String,Double>comparingByValue().reversed())
                .limit(topN)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private static void printClassRules(List<String> classes,
                                         Map<String, TreeSet<String>> inverted) {
        for (String cls : classes) {
            TreeSet<String> terms = inverted.get(cls);
            if (terms == null || terms.isEmpty()) {
                System.out.printf("  # %-40s  (no rules)%n", cls);
                continue;
            }
            List<String> termList = new ArrayList<>(terms);
            int chunkSize = 6;
            for (int i = 0; i < termList.size(); i += chunkSize) {
                List<String> chunk = termList.subList(i, Math.min(i + chunkSize, termList.size()));
                if (i == 0) System.out.printf("  %-58s => %s%n", String.join(", ", chunk), cls);
                else        System.out.printf("  %-58s%n", "  " + String.join(", ", chunk));
            }
            System.out.println();
        }
    }

    private static long median(long[] times) {
        long[] sorted = Arrays.copyOf(times, times.length);
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    // ── Strategy 1: Baseline (TF-IDF on full javadoc + body) ─────────────────

    static Map<String, Set<String>> mineBaseline(IndexReader reader) throws IOException {
        return mineTfIdf(reader, false);
    }

    // ── Strategy 2: First-sentence TF-IDF ────────────────────────────────────

    static Map<String, Set<String>> mineFirstSentence(IndexReader reader) throws IOException {
        return mineTfIdf(reader, true);
    }

    // ── Strategy 3: LinkGraph ─────────────────────────────────────────────────

    /**
     * Solr Knowledge Graph analogy: follow @link/@see edges.
     *
     * <p>For every class A that @links to class B: the first-sentence terms of A
     * become synonyms for B.simpleClassName, and A.simpleClassName becomes a synonym
     * for B.simpleClassName (undirected within the link cluster).
     */
    static Map<String, Set<String>> mineLinkGraph(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);

        // Build name lookup: simpleName.lower → ClassDoc
        Map<String, ClassDoc> bySimpleName = new HashMap<>();
        for (ClassDoc d : docs) bySimpleName.put(d.simpleName.toLowerCase(), d);

        Map<String, Set<String>> result = new HashMap<>();

        for (ClassDoc src : docs) {
            if (src.links.isEmpty()) continue;
            List<String> srcTerms = firstSentenceTerms(src.javadoc);

            for (String linkedName : src.links) {
                String lk = linkedName.toLowerCase();
                ClassDoc tgt = bySimpleName.get(lk);
                if (tgt == null) continue;

                // First-sentence terms of src → target's simple name
                for (String term : srcTerms) {
                    result.computeIfAbsent(term, k -> new HashSet<>()).add(tgt.simpleName);
                }

                // src class name → target class name (undirected)
                result.computeIfAbsent(src.simpleName.toLowerCase(), k -> new HashSet<>())
                      .add(tgt.simpleName);
                result.computeIfAbsent(tgt.simpleName.toLowerCase(), k -> new HashSet<>())
                      .add(src.simpleName);
            }
        }
        return result;
    }

    // ── Strategy 4: CoCallers ─────────────────────────────────────────────────

    /**
     * Basket analysis on the call graph: two classes are related if many of the same
     * caller methods invoke both of them (Jaccard similarity ≥ 0.05).
     *
     * <p>The callerContext field stores the simple method names of all callers for each
     * method document. We aggregate these per class, then compare class pairs.
     */
    static Map<String, Set<String>> mineCoCallers(IndexReader reader) throws IOException {
        // Aggregate callerContext tokens per class (from METHOD documents)
        Map<String, Set<String>> classCallers = new HashMap<>();
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs methodHits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "method")),
                Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();

        for (ScoreDoc sd : methodHits.scoreDocs) {
            Document doc      = sf.document(sd.doc);
            String project    = doc.get(DocumentMapper.F_PROJECT);
            if (!"lucene".equals(project)) continue;
            String kind       = nvl(doc.get("kind"));
            if ("document".equals(kind)) continue;
            String className  = doc.get(DocumentMapper.F_CLASS_NAME);
            String callerCtx  = nvl(doc.get(DocumentMapper.F_CALLER_CONTEXT));
            if (className == null || callerCtx.isBlank()) continue;

            Set<String> callers = classCallers.computeIfAbsent(className, k -> new HashSet<>());
            for (String token : callerCtx.split("\\s+")) {
                if (!token.isBlank()) callers.add(token.toLowerCase());
            }
        }

        // Compute pairwise Jaccard and emit synonym pairs above threshold
        double threshold = 0.05;
        List<String> classNames = new ArrayList<>(classCallers.keySet());
        Map<String, Set<String>> result = new HashMap<>();

        for (int i = 0; i < classNames.size(); i++) {
            String a = classNames.get(i);
            Set<String> callersA = classCallers.get(a);
            if (callersA.size() < 3) continue; // skip classes with very few callers

            for (int j = i + 1; j < classNames.size(); j++) {
                String b = classNames.get(j);
                Set<String> callersB = classCallers.get(b);
                if (callersB.size() < 3) continue;

                double jaccard = jaccard(callersA, callersB);
                if (jaccard >= threshold) {
                    result.computeIfAbsent(a.toLowerCase(), k -> new HashSet<>()).add(b);
                    result.computeIfAbsent(b.toLowerCase(), k -> new HashSet<>()).add(a);
                }
            }
        }
        return result;
    }

    // ── Strategy 5: ClassName tokens + adjacent bigrams ──────────────────────

    /**
     * Decomposes each class name with the existing {@link DocumentMapper#splitIdentifier}
     * (already used at index time for BM25 matching) and emits:
     * <ul>
     *   <li>Each individual token → className (e.g. "wand" → WANDScorer)
     *   <li>Each adjacent bigram → className (e.g. "skiplist" from Multi|Level|Skip|List|Reader)
     *   <li>Each adjacent trigram → className (e.g. "skiplistreader")
     * </ul>
     * Tokens shorter than 3 chars or in the stopword list are skipped.
     * This directly solves the camelcase compound mismatch and abbreviation gaps.
     */
    static Map<String, Set<String>> mineClassNameTokens(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Set<String>> result = new HashMap<>();

        for (ClassDoc d : docs) {
            String split = splitIdentifier(d.simpleName);
            String[] tokens = split.split("\\s+");

            // Individual tokens
            for (String tok : tokens) {
                if (normalizeTerm(tok) != null) {
                    result.computeIfAbsent(tok, k -> new HashSet<>()).add(d.simpleName);
                }
            }

            // Adjacent bigrams: tok[i] + tok[i+1]
            for (int i = 0; i < tokens.length - 1; i++) {
                String bigram = tokens[i] + tokens[i + 1];
                if (bigram.length() >= 4) {
                    result.computeIfAbsent(bigram, k -> new HashSet<>()).add(d.simpleName);
                }
            }

            // Adjacent trigrams: tok[i] + tok[i+1] + tok[i+2]
            for (int i = 0; i < tokens.length - 2; i++) {
                String trigram = tokens[i] + tokens[i + 1] + tokens[i + 2];
                if (trigram.length() >= 6) {
                    result.computeIfAbsent(trigram, k -> new HashSet<>()).add(d.simpleName);
                }
            }
        }
        return result;
    }

    // ── Strategy 6: Method name tokens + bigrams ─────────────────────────────

    /**
     * Decomposes the public method names of each class into tokens and adjacent
     * bigrams, mapping them to the class name.
     *
     * <p>Targets Gap A: {@code addDocument→IndexWriter}, {@code commit→IndexWriter},
     * {@code acquire→SearcherManager}, {@code waitForGeneration→ControlledRealTimeReopenThread}.
     * These are vocabulary that lives in the API surface, not in javadoc prose.
     */
    static Map<String, Set<String>> mineMethodNames(IndexReader reader) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "method")),
                Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();

        Map<String, Set<String>> result = new HashMap<>();
        for (ScoreDoc sd : hits.scoreDocs) {
            Document doc    = sf.document(sd.doc);
            if (!"lucene".equals(doc.get(DocumentMapper.F_PROJECT))) continue;
            if ("document".equals(nvl(doc.get("kind")))) continue;
            String className  = doc.get(DocumentMapper.F_CLASS_NAME);
            String methodName = doc.get(DocumentMapper.F_METHOD_NAME);
            if (className == null || methodName == null) continue;

            String split  = splitIdentifier(methodName);
            String[] toks = split.split("\\s+");

            for (String tok : toks) {
                if (normalizeTerm(tok) != null)
                    result.computeIfAbsent(tok, k -> new HashSet<>()).add(className);
            }
            for (int i = 0; i < toks.length - 1; i++) {
                String bigram = toks[i] + toks[i + 1];
                if (bigram.length() >= 4)
                    result.computeIfAbsent(bigram, k -> new HashSet<>()).add(className);
            }
            for (int i = 0; i < toks.length - 2; i++) {
                String trigram = toks[i] + toks[i + 1] + toks[i + 2];
                if (trigram.length() >= 6)
                    result.computeIfAbsent(trigram, k -> new HashSet<>()).add(className);
            }
        }
        return result;
    }

    // ── Strategy 7: Javadoc prose adjacent-word bigrams ───────────────────────

    /**
     * For each class, tokenizes the full javadoc into words and emits every
     * adjacent pair of non-stopword tokens as a compound bigram.
     *
     * <p>Targets Gap B: {@code editdistance} from "edit distance",
     * {@code weakand} from "Weak AND", {@code drilldown} from "drill down",
     * {@code nonadjacent} from "non-adjacent", {@code vectorspace} from "Vector Space".
     */
    static Map<String, Set<String>> mineJavadocBigrams(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Set<String>> result = new HashMap<>();

        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc;
            // Tokenize: split on non-alphanumeric (hyphens become word boundaries)
            String[] rawTokens = text.split("[^a-zA-Z0-9]+");

            // Collect normalized tokens in order, preserving positions
            List<String> tokens = new ArrayList<>();
            for (String raw : rawTokens) {
                String t = normalizeTerm(raw);
                tokens.add(t); // null = stopword/short, kept as position marker
            }

            // Emit bigrams where both positions are non-null (non-stopword, non-short)
            for (int i = 0; i < tokens.size() - 1; i++) {
                String t1 = tokens.get(i);
                String t2 = tokens.get(i + 1);
                if (t1 != null && t2 != null && (t1.length() + t2.length()) >= 6) {
                    result.computeIfAbsent(t1 + t2, k -> new HashSet<>()).add(d.simpleName);
                }
            }
        }
        return result;
    }

    // ── Strategy 8: Package path last meaningful segment ─────────────────────

    /**
     * Extracts the last segment(s) of each class's package path and maps them
     * to the class name.
     *
     * <p>Targets Gap C: {@code blocktree→BlockTreeTermsReader} from package
     * {@code org.apache.lucene.codecs.blocktree}, {@code bkd→BKDWriter} from
     * {@code org.apache.lucene.util.bkd}.
     */
    private static final Set<String> GENERIC_PKG_SEGMENTS = Set.of(
        "org", "apache", "lucene", "java", "javax", "com", "net",
        "util", "core", "search", "index", "store", "analysis", "common",
        "codec", "codecs", "document", "query", "queries", "test", "tests"
    );

    static Map<String, Set<String>> minePackagePath(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Set<String>> result = new HashMap<>();

        for (ClassDoc d : docs) {
            if (d.qualName == null || d.qualName.isBlank()) continue;
            // qualName = "org.apache.lucene.codecs.blocktree.BlockTreeTermsReader"
            int lastDot = d.qualName.lastIndexOf('.');
            if (lastDot < 0) continue;
            String pkg = d.qualName.substring(0, lastDot); // strip class name

            for (String seg : pkg.split("\\.")) {
                if (seg.length() >= 3 && !GENERIC_PKG_SEGMENTS.contains(seg)) {
                    result.computeIfAbsent(seg.toLowerCase(), k -> new HashSet<>())
                          .add(d.simpleName);
                }
            }
        }
        return result;
    }

    // ── Strategy 9: PPMI (Positive Pointwise Mutual Information) ─────────────

    /**
     * Ranks terms by PPMI instead of TF-IDF.
     *
     * <p>PMI(term, class) = log[ P(term|class) / P(term) ]
     *                     = log[ tf_norm * totalDocs / df ]
     *
     * <p>Key differences from TF-IDF:
     * <ul>
     *   <li>No minDf floor — rare terms with df=1 are fine; PMI captures their
     *       uniqueness naturally (very low P(term) → very high PMI when tf > 0)
     *   <li>Only PPMI kept (negative values discarded) — these are terms less
     *       common in this class than in the corpus average
     *   <li>topN raised to 20 — PMI is more selective, so more candidates are safe
     * </ul>
     *
     * <p>Targets: {@code okapi→BM25Similarity}, {@code damerau→FuzzyQuery} —
     * terms that appear in text but fall outside TF-IDF's top-10 due to a long
     * javadoc with many competing technical terms.
     */
    static Map<String, Set<String>> minePpmi(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);

        Map<String, Integer> docFreq = new HashMap<>();
        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            for (String t : uniqueTerms(text)) docFreq.merge(t, 1, Integer::sum);
        }
        int totalDocs = docs.size();

        Map<String, Set<String>> result = new HashMap<>();
        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            Map<String, Integer> tf = termFreq(text);
            int docLen = tf.values().stream().mapToInt(i -> i).sum();
            if (docLen == 0) continue;

            List<Map.Entry<String, Double>> scored = new ArrayList<>();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                if (normalizeTerm(e.getKey()) == null) continue;
                int df = docFreq.getOrDefault(e.getKey(), 1);
                double pTermGivenClass = (double) e.getValue() / docLen;
                double pTerm           = (double) df / totalDocs;
                double ppmi            = Math.log(pTermGivenClass / pTerm);
                if (ppmi > 0) scored.add(Map.entry(e.getKey(), ppmi));
            }

            scored.sort(Map.Entry.<String, Double>comparingByValue().reversed());
            for (int i = 0; i < Math.min(20, scored.size()); i++) {
                result.computeIfAbsent(scored.get(i).getKey(), k -> new HashSet<>())
                      .add(d.simpleName);
            }
        }
        return result;
    }

    // ── Strategy 10: Context window (skip-gram) ───────────────────────────────

    /**
     * For each class, collects context words that appear within a ±5-token window
     * around any occurrence of the class's own name tokens in the full corpus
     * (all class javadocs and bodies).
     *
     * <p>This is the distributional semantics insight from word2vec's skip-gram
     * model: a word is defined by the company it keeps. If "acquire" and "release"
     * consistently appear near "SearcherManager" across many class docs, they
     * become strong synonym candidates even if they're absent from SearcherManager's
     * own javadoc.
     *
     * <p>Targets: {@code acquire→SearcherManager}, {@code reopen→DirectoryReader},
     * {@code weakand→WANDScorer} — terms that appear as context of the class
     * name in other classes' documentation.
     */
    static Map<String, Set<String>> mineContextWindow(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);
        int windowSize = 5;

        // Build: lowercase class name token → canonical simple name
        // Include both the full name and each camelcase token
        Map<String, String> nameToClass = new HashMap<>();
        for (ClassDoc d : docs) {
            nameToClass.put(d.simpleName.toLowerCase(), d.simpleName);
            for (String tok : splitIdentifier(d.simpleName).split("\\s+")) {
                if (tok.length() >= 4 && normalizeTerm(tok) != null) {
                    nameToClass.putIfAbsent(tok, d.simpleName);
                }
            }
        }

        // For each class, scan entire corpus for occurrences of its name tokens,
        // collect ±windowSize context words
        // Aggregate: className → Map<contextTerm, occurrenceCount>
        Map<String, Map<String, Integer>> contextCounts = new HashMap<>();

        for (ClassDoc src : docs) {
            String[] allWords = (src.javadoc + " " + src.body)
                    .toLowerCase()
                    .split("[^a-zA-Z0-9]+");

            for (int i = 0; i < allWords.length; i++) {
                String cls = nameToClass.get(allWords[i]);
                if (cls == null) continue;

                Map<String, Integer> counts = contextCounts
                        .computeIfAbsent(cls, k -> new HashMap<>());

                int lo = Math.max(0, i - windowSize);
                int hi = Math.min(allWords.length - 1, i + windowSize);
                for (int j = lo; j <= hi; j++) {
                    if (j == i) continue;
                    String ctx = normalizeTerm(allWords[j]);
                    if (ctx != null) counts.merge(ctx, 1, Integer::sum);
                }
            }
        }

        // Emit top-15 context terms per class as synonym candidates
        Map<String, Set<String>> result = new HashMap<>();
        for (Map.Entry<String, Map<String, Integer>> e : contextCounts.entrySet()) {
            String className = e.getKey();
            e.getValue().entrySet().stream()
                    .filter(te -> te.getValue() >= 2) // seen in context at least twice
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(15)
                    .forEach(te -> result.computeIfAbsent(te.getKey(), k -> new HashSet<>())
                                         .add(className));
        }
        return result;
    }

    // ── Strategy 11: PPMI bigrams — no stopword filter, hyphens as merge ────────

    /**
     * Generates bigrams from prose without filtering stopwords or short tokens,
     * and treats hyphens as compound-merge operators.  Scores every bigram by
     * PPMI so that only class-discriminative compounds survive.
     *
     * <h3>Two bigram sources</h3>
     * <ol>
     *   <li><b>Hyphen-merged</b> — every {@code word-word} run in the raw text
     *       becomes a single concatenated token (e.g. {@code "KD-tree"→"kdtree"},
     *       {@code "non-adjacent"→"nonadjacent"}, {@code "Weak-AND"→"weakand"}).
     *   <li><b>Adjacent-word bigrams with no stopword gate</b> — consecutive
     *       lowercase tokens of any length are joined (e.g. "Weak AND"→"weakand",
     *       "drill down"→"drilldown", "edit distance"→"editdistance").
     *       Minimum individual token length: 2 chars (avoids single-letter noise).
     * </ol>
     *
     * <p>Both sources are scored by PPMI; only the top-20 positively-scored
     * bigrams per class are emitted as synonym candidates.
     */
    private static final Pattern HYPHEN_COMPOUND =
            Pattern.compile("([a-zA-Z0-9]{1,})-([a-zA-Z0-9]{1,})");

    static Map<String, Set<String>> minePpmiBigramsInclStopwords(IndexReader reader)
            throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);

        // Build per-doc bigram lists
        Map<String, List<String>> docBigrams = new HashMap<>();
        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            List<String> bigrams = new ArrayList<>();

            // Source 1: hyphen-merged compounds from raw text
            Matcher hm = HYPHEN_COMPOUND.matcher(text);
            while (hm.find()) {
                String merged = (hm.group(1) + hm.group(2)).toLowerCase();
                if (merged.length() >= 4) bigrams.add(merged);
            }

            // Source 2: adjacent-word bigrams — NO stopword/short-token filter on either side
            String[] tokens = text.toLowerCase().split("[^a-zA-Z0-9]+");
            for (int i = 0; i < tokens.length - 1; i++) {
                String t1 = tokens[i], t2 = tokens[i + 1];
                if (t1.length() >= 2 && t2.length() >= 2) {
                    String bigram = t1 + t2;
                    if (bigram.length() >= 4) bigrams.add(bigram);
                }
            }

            docBigrams.put(d.simpleName, bigrams);
        }

        // Document frequency per bigram
        Map<String, Integer> bigramDf = new HashMap<>();
        for (List<String> bigrams : docBigrams.values()) {
            for (String b : new HashSet<>(bigrams)) bigramDf.merge(b, 1, Integer::sum);
        }
        int totalDocs = docs.size();

        // PPMI scoring — emit top-20 positively-discriminative bigrams per class
        Map<String, Set<String>> result = new HashMap<>();
        for (ClassDoc d : docs) {
            List<String> bigrams = docBigrams.get(d.simpleName);
            if (bigrams == null || bigrams.isEmpty()) continue;

            Map<String, Integer> tf = new HashMap<>();
            for (String b : bigrams) tf.merge(b, 1, Integer::sum);
            int docLen = bigrams.size();

            List<Map.Entry<String, Double>> scored = new ArrayList<>();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                int df = bigramDf.getOrDefault(e.getKey(), 1);
                double pBigramGivenClass = (double) e.getValue() / docLen;
                double pBigram           = (double) df / totalDocs;
                double ppmi              = Math.log(pBigramGivenClass / pBigram);
                if (ppmi > 0) scored.add(Map.entry(e.getKey(), ppmi));
            }

            scored.sort(Map.Entry.<String, Double>comparingByValue().reversed());
            for (int i = 0; i < Math.min(20, scored.size()); i++) {
                result.computeIfAbsent(scored.get(i).getKey(), k -> new HashSet<>())
                      .add(d.simpleName);
            }
        }
        return result;
    }

    // ── ★ Combined strategy ───────────────────────────────────────────────────

    /**
     * Combines every signal source that contributes unique recall, discarding
     * strategies that are superseded or add only noise:
     *
     * <ol>
     *   <li><b>Class name decomposition</b> — camelCase tokens + adjacent bigrams
     *       + trigrams. Authoritative: zero false positives.
     *   <li><b>Method name decomposition</b> — public method name tokens + bigrams.
     *       Captures API-surface vocabulary absent from javadoc.
     *   <li><b>Package path segments</b> — non-generic last package segments
     *       (e.g. "blocktree", "bkd"). Free discriminative signal.
     *   <li><b>{@literal @}link/{@literal @}see cross-references</b> — author-stated
     *       conceptual relationships become undirected synonym pairs.
     *   <li><b>First-sentence PPMI</b> — top-20 PPMI-ranked tokens from the first
     *       javadoc sentence. Dense signal with no minDf gate.
     *   <li><b>Full-text PPMI</b> — top-15 PPMI-ranked tokens from full javadoc+body,
     *       threshold≥0.5 to reduce noise from long documents.
     *   <li><b>PPMI prose bigrams</b> — hyphen-merged compounds plus adjacent-word
     *       bigrams where at least one token is a non-stopword of ≥3 chars (the
     *       hybrid filter that captures "Weak AND"→"weakand" while suppressing
     *       pure-stopword noise). Scored by PPMI≥1.0, capped at 30 per class.
     * </ol>
     */
    /** Delegates to the production implementation so test and code never diverge. */
    static Map<String, Set<String>> mineCombined(IndexReader reader) throws IOException {
        return new com.pharos.analysis.ConceptMiner().mineAll(reader);
    }

    // kept for internal strategies that need the same pipeline with tweaked params
    static Map<String, Set<String>> mineCombinedInternal(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Set<String>> result = new HashMap<>();

        // ── Build corpus-level frequency tables for PPMI ──────────────────────

        Map<String, Integer> tokenDf  = new HashMap<>();
        Map<String, Integer> bigramDf = new HashMap<>();

        for (ClassDoc d : docs) {
            String fullText = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            for (String t : uniqueTerms(fullText))
                tokenDf.merge(t, 1, Integer::sum);
            for (String b : new HashSet<>(hybridBigrams(fullText)))
                bigramDf.merge(b, 1, Integer::sum);
        }
        int totalDocs = docs.size();

        // ── Source 1: class name tokens + bigrams + trigrams ──────────────────

        for (ClassDoc d : docs) {
            String[] toks = splitIdentifier(d.simpleName).split("\\s+");
            for (String tok : toks)
                if (normalizeTerm(tok) != null)
                    put(result, tok, d.simpleName);
            for (int i = 0; i < toks.length - 1; i++) {
                String bi = toks[i] + toks[i + 1];
                if (bi.length() >= 4) put(result, bi, d.simpleName);
            }
            for (int i = 0; i < toks.length - 2; i++) {
                String tri = toks[i] + toks[i + 1] + toks[i + 2];
                if (tri.length() >= 6) put(result, tri, d.simpleName);
            }
        }

        // ── Source 2: top-K public method tokens ranked by inDegree ─────────────

        emitTopPublicMethodTokens(reader, 10, result);

        // ── Source 3: package path segments ──────────────────────────────────

        for (ClassDoc d : docs) {
            if (d.qualName == null || d.qualName.isBlank()) continue;
            int lastDot = d.qualName.lastIndexOf('.');
            if (lastDot < 0) continue;
            for (String seg : d.qualName.substring(0, lastDot).split("\\."))
                if (seg.length() >= 3 && !GENERIC_PKG_SEGMENTS.contains(seg))
                    put(result, seg.toLowerCase(), d.simpleName);
        }

        // ── Source 4: @link / @see cross-references ───────────────────────────

        Map<String, ClassDoc> bySimpleName = new HashMap<>();
        for (ClassDoc d : docs) bySimpleName.put(d.simpleName.toLowerCase(), d);

        for (ClassDoc src : docs) {
            for (String linked : src.links) {
                if (bySimpleName.containsKey(linked.toLowerCase())) {
                    put(result, src.simpleName.toLowerCase(), linked);
                    put(result, linked.toLowerCase(), src.simpleName);
                }
            }
        }

        // ── Sources 5 & 6: PPMI on first sentence and full text ───────────────

        for (ClassDoc d : docs) {
            // 5. First-sentence PPMI (topN=20, no threshold)
            String sent = firstSentence(d.javadoc);
            emitPpmi(sent, tokenDf, totalDocs, d.simpleName, 20, 0.0, result);

            // 6. Full-text PPMI (topN=15, threshold=0.5 to suppress long-doc noise)
            String full = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            emitPpmi(full, tokenDf, totalDocs, d.simpleName, 15, 0.5, result);
        }

        // ── Source 7: PPMI prose bigrams (hybrid filter + threshold=1.0) ──────

        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            List<String> bigrams = hybridBigrams(text);

            Map<String, Integer> tf = new HashMap<>();
            for (String b : bigrams) tf.merge(b, 1, Integer::sum);
            int docLen = bigrams.size();
            if (docLen == 0) continue;

            List<Map.Entry<String, Double>> scored = new ArrayList<>();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                int df = bigramDf.getOrDefault(e.getKey(), 1);
                double ppmi = Math.log(((double) e.getValue() / docLen)
                                       / ((double) df / totalDocs));
                if (ppmi >= 1.0) scored.add(Map.entry(e.getKey(), ppmi));
            }
            for (Map.Entry<String, Double> e : scored)
                put(result, e.getKey(), d.simpleName);
        }

        // Post-processing: max length 20, no digit-start
        result.entrySet().removeIf(e -> {
            String t = e.getKey();
            return t.length() > 20 || Character.isDigit(t.charAt(0));
        });
        return result;
    }

    /** Hybrid bigram generator: hyphen-merged compounds + adjacent pairs where
     *  at least one token is a non-stopword of ≥ 3 chars. */
    private static List<String> hybridBigrams(String text) {
        List<String> bigrams = new ArrayList<>();

        // Hyphen-merged compounds
        Matcher hm = HYPHEN_COMPOUND.matcher(text);
        while (hm.find()) {
            String merged = (hm.group(1) + hm.group(2)).toLowerCase();
            if (merged.length() >= 4) bigrams.add(merged);
        }

        // Adjacent pairs with hybrid stopword gate
        String[] tokens = text.toLowerCase().split("[^a-zA-Z0-9]+");
        for (int i = 0; i < tokens.length - 1; i++) {
            String t1 = tokens[i], t2 = tokens[i + 1];
            boolean t1ok = t1.length() >= 3 && !STOPWORDS.contains(t1);
            boolean t2ok = t2.length() >= 3 && !STOPWORDS.contains(t2);
            if ((t1ok || t2ok) && t1.length() >= 2 && t2.length() >= 2) {
                String bi = t1 + t2;
                if (bi.length() >= 4) bigrams.add(bi);
            }
        }
        return bigrams;
    }

    /** Emit top-N PPMI-scored tokens from text into result. */
    private static void emitPpmi(String text, Map<String, Integer> docFreq,
                                  int totalDocs, String className,
                                  int topN, double minPpmi,
                                  Map<String, Set<String>> result) {
        Map<String, Integer> tf = termFreq(text);
        int docLen = tf.values().stream().mapToInt(i -> i).sum();
        if (docLen == 0) return;

        tf.entrySet().stream()
          .filter(e -> normalizeTerm(e.getKey()) != null)
          .map(e -> {
              int df = docFreq.getOrDefault(e.getKey(), 1);
              double ppmi = Math.log(((double) e.getValue() / docLen)
                                     / ((double) df / totalDocs));
              return Map.entry(e.getKey(), ppmi);
          })
          .filter(e -> e.getValue() >= minPpmi)
          .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
          .limit(topN)
          .forEach(e -> put(result, e.getKey(), className));
    }

    private static void put(Map<String, Set<String>> map, String term, String className) {
        map.computeIfAbsent(term, k -> new HashSet<>()).add(className);
    }

    // ── Diagnostic: PPMI scores for specific golden terms ─────────────────────

    /**
     * Shows the actual PPMI scores for the golden terms that are hardest to find,
     * so we can see whether they're present but below the topN cutoff, or absent
     * entirely.
     */
    @Test
    @Order(2)
    void diagnostic_ppmi_scores_for_hard_terms() throws IOException {
        List<ClassDoc> docs = loadClassDocs(luceneReader);

        // Build full bigram inventory (same as minePpmiBigramsInclStopwords)
        Map<String, List<String>> docBigrams = new HashMap<>();
        for (ClassDoc d : docs) {
            String text = d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
            List<String> bigrams = new ArrayList<>();
            Matcher hm = HYPHEN_COMPOUND.matcher(text);
            while (hm.find()) {
                String merged = (hm.group(1) + hm.group(2)).toLowerCase();
                if (merged.length() >= 4) bigrams.add(merged);
            }
            String[] tokens = text.toLowerCase().split("[^a-zA-Z0-9]+");
            for (int i = 0; i < tokens.length - 1; i++) {
                String t1 = tokens[i], t2 = tokens[i + 1];
                if (t1.length() >= 2 && t2.length() >= 2) {
                    String bigram = t1 + t2;
                    if (bigram.length() >= 4) bigrams.add(bigram);
                }
            }
            docBigrams.put(d.simpleName, bigrams);
        }
        Map<String, Integer> bigramDf = new HashMap<>();
        for (List<String> bl : docBigrams.values()) {
            for (String b : new HashSet<>(bl)) bigramDf.merge(b, 1, Integer::sum);
        }
        int totalDocs = docs.size();

        // Probe specific (term, className) pairs
        record Probe(String term, String className) {}
        List<Probe> probes = List.of(
            new Probe("weakand",     "WANDScorer"),
            new Probe("nonadjacent", "TieredMergePolicy"),
            new Probe("kdtree",      "BKDWriter"),
            new Probe("editdistance","FuzzyQuery"),
            new Probe("drilldown",   "DrillSideways"),
            new Probe("vectorspace", "TFIDFSimilarity"),
            new Probe("okapi",       "BM25Similarity"),
            new Probe("damerau",     "FuzzyQuery"),
            new Probe("corruption",  "CheckIndex"),
            new Probe("snippet",     "UnifiedHighlighter")
        );

        System.out.println();
        System.out.println(bar());
        System.out.println("  PPMI BIGRAM DIAGNOSTIC — scores for hard golden terms");
        System.out.println(bar());
        System.out.printf("  %-20s %-34s │ %6s │ %5s │ %6s │ %s%n",
                "Term", "Expected class", "df", "rank", "ppmi", "in top-20?");
        System.out.println("  " + "─".repeat(80));

        for (Probe p : probes) {
            List<String> bigrams = docBigrams.get(p.className());
            if (bigrams == null || bigrams.isEmpty()) {
                System.out.printf("  %-20s %-34s │ %6s │ %5s │ %6s │ %s%n",
                        p.term(), p.className(), "—", "—", "—", "CLASS NOT LOADED");
                continue;
            }

            Map<String, Integer> tf = new HashMap<>();
            for (String b : bigrams) tf.merge(b, 1, Integer::sum);
            int docLen = bigrams.size();

            // Score all bigrams for this class, rank them
            List<Map.Entry<String, Double>> scored = new ArrayList<>();
            for (Map.Entry<String, Integer> e : tf.entrySet()) {
                int df = bigramDf.getOrDefault(e.getKey(), 1);
                double pBigramGivenClass = (double) e.getValue() / docLen;
                double pBigram           = (double) df / totalDocs;
                double ppmi              = Math.log(pBigramGivenClass / pBigram);
                if (ppmi > 0) scored.add(Map.entry(e.getKey(), ppmi));
            }
            scored.sort(Map.Entry.<String, Double>comparingByValue().reversed());

            int termTf  = tf.getOrDefault(p.term(), 0);
            int termDf  = bigramDf.getOrDefault(p.term(), 0);
            int rank    = -1;
            double ppmi = Double.NaN;
            for (int i = 0; i < scored.size(); i++) {
                if (scored.get(i).getKey().equals(p.term())) {
                    rank = i + 1;
                    ppmi = scored.get(i).getValue();
                    break;
                }
            }

            String inTop20 = rank > 0 && rank <= 20 ? "YES" :
                             rank > 0 ? "rank=" + rank :
                             termTf > 0 ? "ppmi≤0 (tf=" + termTf + ")" : "ABSENT";

            System.out.printf("  %-20s %-34s │ %6d │ %5s │ %6s │ %s%n",
                    p.term(), p.className(),
                    termDf,
                    rank > 0 ? String.valueOf(rank) : "—",
                    Double.isNaN(ppmi) ? "—" : String.format("%.2f", ppmi),
                    inTop20);
        }
        System.out.println(bar());
        System.out.println();
    }

    // ── Strategy: Acronym / parenthetical expansion ───────────────────────────

    /**
     * Extracts expansion bigrams from two parenthetical patterns in the first
     * javadoc sentence and maps them to the class name:
     * <ul>
     *   <li><b>Expansion (ABBREV)</b> — e.g. "finite state machine (FST)"
     *       → bigrams "finitestatemachine", "statemachine" → FST
     *   <li><b>ABBREV (Expansion)</b> — e.g. "WAND (Weak AND)"
     *       → bigram "weakand" → WANDScorer
     * </ul>
     * Both directions handle the same underlying concept: the author wrote the
     * full name and the short name together, so both should retrieve the class.
     */
    private static final Pattern PAT_EXPANSION_ABBREV =
            Pattern.compile("([a-zA-Z][\\w\\s-]{3,35})\\s*\\(([A-Z]{2,10})\\)");
    private static final Pattern PAT_ABBREV_EXPANSION =
            Pattern.compile("\\b([A-Z]{2,10})\\s+\\(([a-zA-Z][\\w\\s-]{3,35})\\)");

    static Map<String, Set<String>> mineAcronymExpansions(IndexReader reader) throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);
        Map<String, Set<String>> result = new HashMap<>();

        for (ClassDoc d : docs) {
            String sent = firstSentence(d.javadoc);
            if (sent.isBlank()) continue;

            // Pattern 1: expansion (ABBREV) — extract bigrams/trigrams from expansion
            Matcher m1 = PAT_EXPANSION_ABBREV.matcher(sent);
            while (m1.find()) {
                emitNgrams(m1.group(1).trim().toLowerCase(), d.simpleName, result);
            }

            // Pattern 2: ABBREV (expansion) — extract bigrams/trigrams from expansion
            Matcher m2 = PAT_ABBREV_EXPANSION.matcher(sent);
            while (m2.find()) {
                emitNgrams(m2.group(2).trim().toLowerCase(), d.simpleName, result);
            }
        }
        return result;
    }

    /** Splits phrase on spaces/hyphens, emits adjacent bigrams and trigrams. */
    private static void emitNgrams(String phrase, String className,
                                    Map<String, Set<String>> result) {
        String[] words = phrase.split("[\\s-]+");
        for (int i = 0; i < words.length - 1; i++) {
            if (words[i].length() < 2 || words[i + 1].length() < 2) continue;
            String bi = words[i] + words[i + 1];
            if (bi.length() >= 4) result.computeIfAbsent(bi, k -> new HashSet<>()).add(className);
        }
        for (int i = 0; i < words.length - 2; i++) {
            if (words[i].length() < 2) continue;
            String tri = words[i] + words[i + 1] + words[i + 2];
            if (tri.length() >= 6) result.computeIfAbsent(tri, k -> new HashSet<>()).add(className);
        }
    }

    // ── Method name helper ────────────────────────────────────────────────────

    /**
     * Loads all {@code public} method documents for the lucene project, groups
     * by class, sorts each class's methods by {@code inDegree} descending (most
     * called = most user-facing), keeps the top {@code topK}, then emits their
     * camelCase-split tokens into {@code result}.  No bigrams — single tokens only.
     *
     * <p>inDegree is the number of other methods in the call graph that call this
     * method.  High-inDegree public methods are the stable API entry points users
     * actually invoke; low-inDegree public methods are internal helpers.
     */
    static void emitTopPublicMethodTokens(IndexReader reader, int topK,
                                           Map<String, Set<String>> result)
            throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "method")),
                Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();

        // className → list of (methodName, inDegree), public only
        record MethodEntry(String name, int inDegree) {}
        Map<String, List<MethodEntry>> byClass = new HashMap<>();

        for (ScoreDoc sd : hits.scoreDocs) {
            Document doc = sf.document(sd.doc);
            if (!"lucene".equals(doc.get(DocumentMapper.F_PROJECT))) continue;
            if ("document".equals(nvl(doc.get("kind")))) continue;
            if (!"public".equals(doc.get(DocumentMapper.F_ACCESS))) continue;
            String cn = doc.get(DocumentMapper.F_CLASS_NAME);
            String mn = doc.get(DocumentMapper.F_METHOD_NAME);
            if (cn == null || mn == null) continue;

            int inDegree = 0;
            var f = doc.getField(DocumentMapper.F_IN_DEGREE);
            if (f != null && f.numericValue() != null)
                inDegree = f.numericValue().intValue();

            byClass.computeIfAbsent(cn, k -> new ArrayList<>())
                   .add(new MethodEntry(mn, inDegree));
        }

        // For each class: sort by inDegree DESC, take top-K, emit tokens
        for (Map.Entry<String, List<MethodEntry>> e : byClass.entrySet()) {
            String cn = e.getKey();
            e.getValue().stream()
             .sorted(Comparator.comparingInt(MethodEntry::inDegree).reversed())
             .limit(topK)
             .forEach(me -> {
                 for (String tok : splitIdentifier(me.name()).split("\\s+"))
                     if (normalizeTerm(tok) != null) put(result, tok, cn);
             });
        }
    }

    // ── Shared TF-IDF mining logic ────────────────────────────────────────────

    private static Map<String, Set<String>> mineTfIdf(IndexReader reader,
                                                       boolean firstSentenceOnly)
            throws IOException {
        List<ClassDoc> docs = loadClassDocs(reader);

        // Build doc-frequency table
        Map<String, Integer> docFreq = new HashMap<>();
        for (ClassDoc d : docs) {
            String text = textFor(d, firstSentenceOnly);
            for (String term : uniqueTerms(text)) docFreq.merge(term, 1, Integer::sum);
        }

        int totalDocs = docs.size();
        int minDf     = 2;
        int maxDf     = Math.max(1, totalDocs * 30 / 100);

        Map<String, Set<String>> result = new HashMap<>();
        for (ClassDoc d : docs) {
            String text = textFor(d, firstSentenceOnly);
            for (String term : topTermsByTfIdf(text, docFreq, totalDocs, minDf, maxDf, 10)) {
                result.computeIfAbsent(term, k -> new HashSet<>()).add(d.simpleName);
            }
        }
        return result;
    }

    private static String textFor(ClassDoc d, boolean firstSentenceOnly) {
        if (firstSentenceOnly) return firstSentence(d.javadoc);
        return d.javadoc.isBlank() ? d.body : d.javadoc + "\n" + d.body;
    }

    // ── Document loading ──────────────────────────────────────────────────────

    record ClassDoc(String simpleName, String qualName, String javadoc,
                    String body, List<String> links) {}

    private static List<ClassDoc> loadClassDocs(IndexReader reader) throws IOException {
        IndexSearcher searcher  = new IndexSearcher(reader);
        TopDocs hits = searcher.search(
                new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "class")),
                Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();

        List<ClassDoc> docs = new ArrayList<>(hits.scoreDocs.length);
        for (ScoreDoc sd : hits.scoreDocs) {
            Document doc     = sf.document(sd.doc);
            String project   = doc.get(DocumentMapper.F_PROJECT);
            if (!"lucene".equals(project)) continue;
            String kind      = nvl(doc.get("kind"));
            if ("document".equals(kind)) continue;
            String className = doc.get(DocumentMapper.F_CLASS_NAME);
            if (className == null || className.isBlank()) continue;
            String qualName  = nvl(doc.get(DocumentMapper.F_QUALIFIED_CLASS));
            String javadoc   = nvl(doc.get(DocumentMapper.F_JAVADOC));
            String body      = nvl(doc.get(DocumentMapper.F_BODY));
            docs.add(new ClassDoc(className, qualName, javadoc, body, extractLinks(javadoc)));
        }
        return docs;
    }

    // ── TF-IDF helpers ────────────────────────────────────────────────────────

    private static List<String> topTermsByTfIdf(String text, Map<String, Integer> docFreq,
                                                  int totalDocs, int minDf, int maxDf,
                                                  int topN) {
        Map<String, Integer> tf = termFreq(text);
        int docLen = tf.values().stream().mapToInt(i -> i).sum();
        if (docLen == 0) return List.of();

        return tf.entrySet().stream()
                .filter(e -> {
                    int df = docFreq.getOrDefault(e.getKey(), 0);
                    return df >= minDf && df <= maxDf;
                })
                .map(e -> {
                    double tfNorm = (double) e.getValue() / docLen;
                    double idf    = Math.log((double) totalDocs / docFreq.get(e.getKey()));
                    return Map.entry(e.getKey(), tfNorm * idf);
                })
                .filter(e -> e.getValue() >= 0.05)
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(topN)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private static Map<String, Integer> termFreq(String text) {
        Map<String, Integer> freq = new HashMap<>();
        for (String raw : text.split("[^a-zA-Z0-9]+")) {
            String t = normalizeTerm(raw);
            if (t != null) freq.merge(t, 1, Integer::sum);
        }
        return freq;
    }

    private static Set<String> uniqueTerms(String text) {
        Set<String> s = new HashSet<>();
        for (String raw : text.split("[^a-zA-Z0-9]+")) {
            String t = normalizeTerm(raw);
            if (t != null) s.add(t);
        }
        return s;
    }

    private static List<String> firstSentenceTerms(String javadoc) {
        return new ArrayList<>(uniqueTerms(firstSentence(javadoc)));
    }

    // ── Text helpers ──────────────────────────────────────────────────────────

    private static String firstSentence(String javadoc) {
        if (javadoc == null || javadoc.isBlank()) return "";
        // Strip leading * from javadoc lines
        String clean = javadoc.replaceAll("(?m)^\\s*\\*\\s?", " ").trim();
        // End at first sentence boundary (. ! ?)
        int end = -1;
        for (int i = 0; i < clean.length(); i++) {
            char c = clean.charAt(i);
            if ((c == '.' || c == '!' || c == '?') &&
                    (i + 1 >= clean.length() || Character.isWhitespace(clean.charAt(i + 1)))) {
                end = i;
                break;
            }
        }
        return end > 0 ? clean.substring(0, end) : clean.substring(0, Math.min(clean.length(), 200));
    }

    private static final Pattern INLINE_LINK = Pattern.compile("\\{@link\\s+([\\w.#()]+)\\}");
    private static final Pattern SEE_TAG     = Pattern.compile("@see\\s+([\\w.#()]+)");

    private static List<String> extractLinks(String javadoc) {
        List<String> links = new ArrayList<>();
        Matcher m1 = INLINE_LINK.matcher(javadoc);
        while (m1.find()) links.add(simpleClassName(m1.group(1)));
        Matcher m2 = SEE_TAG.matcher(javadoc);
        while (m2.find()) links.add(simpleClassName(m2.group(1)));
        return links.stream()
                .filter(s -> !s.isBlank() && Character.isUpperCase(s.charAt(0)))
                .distinct()
                .collect(Collectors.toList());
    }

    private static String simpleClassName(String ref) {
        int hash = ref.indexOf('#');
        if (hash >= 0) ref = ref.substring(0, hash);
        int dot = ref.lastIndexOf('.');
        if (dot >= 0) ref = ref.substring(dot + 1);
        int lt = ref.indexOf('<');
        if (lt >= 0) ref = ref.substring(0, lt);
        return ref.trim();
    }

    private static final Set<String> STOPWORDS = Set.of(
        "a","an","the","and","or","but","if","in","on","at","to","for","of","with",
        "by","from","as","is","was","are","were","be","been","have","has","had",
        "do","does","did","will","would","could","should","that","this","it","its",
        "not","also","just","only","over","into","about","per","i","e","g","etc",
        "returns","return","param","throws","see","since","note","use","used","using",
        "given","new","creates","provides","represents","contains","implements",
        "extends","wraps","allows","calls","method","class","interface","object",
        "instance","type","value","result","list","map","set","null","true","false",
        "int","long","string","boolean","void","public","private","protected",
        "static","final","abstract","default","super","exception","error","runtime"
    );

    private static String normalizeTerm(String raw) {
        if (raw.length() < 3) return null;
        String lower = raw.toLowerCase().replaceAll("^\\d+|\\d+$", "");
        if (lower.length() < 3) return null;
        if (STOPWORDS.contains(lower)) return null;
        if (lower.matches("\\d+(\\.\\d+)*")) return null;
        return lower;
    }

    // ── Co-caller helpers ─────────────────────────────────────────────────────

    private static double jaccard(Set<String> a, Set<String> b) {
        if (a.isEmpty() || b.isEmpty()) return 0.0;
        Set<String> intersection = new HashSet<>(a);
        intersection.retainAll(b);
        Set<String> union = new HashSet<>(a);
        union.addAll(b);
        return (double) intersection.size() / union.size();
    }

    // ── Misc ──────────────────────────────────────────────────────────────────

    private static String splitIdentifier(String name) {
        if (name == null || name.isBlank()) return name == null ? "" : name;
        String[] parts = name.split("_");
        StringBuilder result = new StringBuilder();
        for (String part : parts) {
            if (!part.isEmpty()) {
                String split = part
                        .replaceAll("([a-z])([A-Z])", "$1 $2")
                        .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2");
                if (!result.isEmpty()) result.append(' ');
                result.append(split.toLowerCase());
            }
        }
        return result.toString();
    }

    private static String nvl(String s) { return s != null ? s : ""; }
    private static String bar() { return "━".repeat(72); }
}
