package com.pharos.search;

import com.pharos.config.IndexConfig;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BasicStats;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.SimilarityBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * BM25 keyword search using MultiFieldQueryParser with field-level boosting.
 *
 * Field boost hierarchy (query-time multipliers):
 * - methodName: 4x  (direct method name match — highest field signal)
 * - className:  4x  (class name match — equally strong as methodName; class docs additionally
 *                    get post-BM25 docTypeBonus ×1.8 so they rank above individual methods)
 * - signature:  1.5x (type information adds context)
 * - annotations: 1x
 * - callerContext: 1x (callers are ambient context)
 * - body: 0.5x      (implementation details; long field, mostly noise)
 * - javadoc: 2.0x   (kept at original; single-term name matches (4x) already beat
 *                    single-term javadoc (2x); phrase matches over all terms legitimately win)
 *
 * Post-BM25 multipliers (applied in toResults()):
 * - docType=class:            ×1.8 so class documents rank above individual methods
 * - access=public:            ×1.0 (base)
 * - access=protected:         ×0.85
 * - access=package-private:  ×0.75
 * - access=private:           ×0.5
 *
 * Per-field similarity (via PerFieldSimilarityWrapper):
 * - methodName, className, signature: IDF-only — identifier-heavy fields; presence of the
 *   term is what matters, not frequency or field length
 * - javadoc, callerContext: BM25 k1=1.0, b=0.5  — natural-language-ish text
 * - body, annotations: BM25 k1=1.2, b=0.9  — long fields; aggressive length norm so long
 *   implementations don't beat short, focused methods
 */
public class KeywordSearchStrategy {

    private static final Logger log = LoggerFactory.getLogger(KeywordSearchStrategy.class);

    /**
     * Weight applied to the graph-derived boost on top of BM25 score.
     * Final score = bm25 * (1 + GRAPH_BOOST_WEIGHT * log(1+inDeg)/log(1+maxInDeg))
     * At 0.3, a method called by 100 others gets ~30% score lift over a method never
     * called. Keeps relevance dominant while surfacing hub methods.
     */
    private static final float GRAPH_BOOST_WEIGHT = 0.3f;

    /**
     * Score multiplier applied to the min-should-match-tier SHOULD clause.
     * A document that contains at least {@code ceil(n * MIN_MATCH_RATIO)} of the n query tokens
     * gets this bonus on top of the base OR score.
     */
    private static final float MIN_MATCH_BOOST = 1.5f;

    /**
     * Fraction of query tokens that must be present for the middle tier to fire.
     * 0.75 → for a 4-token query, 3 tokens required; for 3 tokens, all 3 required.
     * Floored at 2 so a 2-token query always requires both terms.
     */
    private static final float MIN_MATCH_RATIO = 0.75f;

    /**
     * Score multiplier applied to the phrase-tier SHOULD clause (terms adjacent/near each other).
     * A document where query tokens appear consecutively (within PHRASE_SLOP positions) in a
     * key field gets this bonus, making it rank above equally-relevant but scattered matches.
     */
    private static final float PHRASE_BOOST = 3.0f;

    /**
     * Maximum edit distance (position swaps) allowed in the phrase query.
     * slop=2 tolerates stop-word gaps (e.g. "find by email" → positions 0,1,3 after stop removal,
     * distance=1) without accepting reversed-order matches (which need ≥3 swaps for 3-term queries).
     */
    private static final int PHRASE_SLOP = 2;

    /**
     * Fields on which phrase queries are built — high-signal, relatively short fields where
     * term adjacency is meaningful. Body is excluded: it's too long and noisy for phrase scoring.
     */
    private static final String[] PHRASE_FIELDS = {
            DocumentMapper.F_METHOD_NAME,
            DocumentMapper.F_CLASS_NAME,
            DocumentMapper.F_JAVADOC,
            DocumentMapper.F_SIGNATURE,
    };

    /**
     * IDF-only similarity for identifier fields (methodName, className).
     * Score = log((N+1)/(df+1)) + 1 — purely how rare/specific the term is in the corpus.
     * Term frequency and field length are ignored: one occurrence is as good as ten,
     * and a short class name is not penalized against a long one.
     */
    private static final class IdfOnlySimilarity extends SimilarityBase {
        @Override
        public double score(BasicStats stats, double freq, double docLen) {
            return Math.log((stats.getNumberOfDocuments() + 1.0) / (stats.getDocFreq() + 1.0)) + 1.0;
        }
        @Override public String toString() { return "IDF"; }
    }

    private static final Similarity PER_FIELD_SIMILARITY = new PerFieldSimilarityWrapper() {
        private final Similarity     idSim   = new IdfOnlySimilarity();
        private final BM25Similarity textSim = new BM25Similarity(1.0f, 0.5f);
        private final BM25Similarity bodySim = new BM25Similarity(1.2f, 0.9f);

        @Override
        public Similarity get(String field) {
            return switch (field) {
                case DocumentMapper.F_METHOD_NAME,
                     DocumentMapper.F_CLASS_NAME,
                     DocumentMapper.F_SIGNATURE     -> idSim;
                case DocumentMapper.F_JAVADOC,
                     DocumentMapper.F_CALLER_CONTEXT -> textSim;
                default                            -> bodySim;
            };
        }
    };

    private static final String[] SEARCH_FIELDS = {
            DocumentMapper.F_METHOD_NAME,
            DocumentMapper.F_JAVADOC,
            DocumentMapper.F_SIGNATURE,
            DocumentMapper.F_CLASS_NAME,
            DocumentMapper.F_BODY,
            DocumentMapper.F_ANNOTATIONS,
            DocumentMapper.F_CALLER_CONTEXT
    };

    private static final Map<String, Float> FIELD_BOOSTS = new HashMap<>();
    static {
        FIELD_BOOSTS.put(DocumentMapper.F_METHOD_NAME,    4.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_CLASS_NAME,     4.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_SIGNATURE,      1.5f);
        FIELD_BOOSTS.put(DocumentMapper.F_ANNOTATIONS,    1.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_CALLER_CONTEXT, 1.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_BODY,           0.5f);
        FIELD_BOOSTS.put(DocumentMapper.F_JAVADOC,        2.0f);
    }

    /**
     * Path to the synonym file — watched for changes between searches.
     * Stored so the hot-reload check doesn't need to rebuild the path each time.
     */
    private final Path synonymFile;

    private Analyzer analyzer;
    /** Last-modified timestamp of synonyms.txt at the time the analyzer was built. */
    private long synonymFileLastModified = -1;

    public KeywordSearchStrategy() {
        this(IndexConfig.DEFAULT_BASE.resolve("synonyms.txt"));
    }

    /** Package-private constructor for tests that need a custom synonym file path. */
    KeywordSearchStrategy(Path synonymFile) {
        this.synonymFile = synonymFile;
        reloadAnalyzer();
    }

    /**
     * Rebuilds the query analyzer from the current synonyms.txt.
     * Called at construction and whenever the file's last-modified time advances.
     */
    private void reloadAnalyzer() {
        this.analyzer = LuceneIndexer.buildQueryAnalyzer(synonymFile.getParent());
        try {
            this.synonymFileLastModified = Files.exists(synonymFile)
                    ? Files.getLastModifiedTime(synonymFile).toMillis() : -1;
        } catch (IOException e) {
            this.synonymFileLastModified = -1;
        }
    }

    /**
     * Checks whether synonyms.txt has been modified since the analyzer was last built,
     * and rebuilds the analyzer in-place if so. Called at the start of every search —
     * the file-stat is a single syscall and is negligible compared to Lucene I/O.
     */
    private void reloadIfChanged() {
        try {
            long current = Files.exists(synonymFile)
                    ? Files.getLastModifiedTime(synonymFile).toMillis() : -1;
            if (current != synonymFileLastModified) {
                log.info("synonyms.txt changed — reloading query analyzer");
                reloadAnalyzer();
            }
        } catch (IOException e) {
            log.warn("Could not stat synonyms.txt: {}", e.getMessage());
        }
    }

    public List<SearchResult> search(IndexReader reader, SearchRequest req) throws IOException {
        reloadIfChanged();
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(PER_FIELD_SIMILARITY);

        Query query;
        try {
            String escaped = MultiFieldQueryParser.escape(req.query());

            // Stage 3 (base): OR — any term matches; provides recall for partial queries.
            MultiFieldQueryParser orParser = new MultiFieldQueryParser(SEARCH_FIELDS, analyzer, FIELD_BOOSTS);
            orParser.setDefaultOperator(QueryParser.Operator.OR);
            Query orQuery = orParser.parse(escaped);

            List<String> terms = analyzeTerms(req.query());
            if (terms.size() >= 2) {
                // Stage 2: min-should-match — ceil(n * 0.75) tokens must be present somewhere.
                // Built per-token so each token expands across all fields with its boost,
                // avoiding the multi-token callerContext phrase-query artifact from a bulk parse.
                BooleanQuery.Builder minMatchBuilder = new BooleanQuery.Builder();
                MultiFieldQueryParser perTermParser =
                        new MultiFieldQueryParser(SEARCH_FIELDS, analyzer, FIELD_BOOSTS);
                perTermParser.setDefaultOperator(QueryParser.Operator.OR);
                for (String term : terms) {
                    try {
                        minMatchBuilder.add(
                                perTermParser.parse(MultiFieldQueryParser.escape(term)),
                                BooleanClause.Occur.SHOULD);
                    } catch (ParseException e) {
                        log.warn("Per-term parse error for '{}': {}", term, e.getMessage());
                    }
                }
                int minMatch = Math.max(2, (int) Math.ceil(terms.size() * MIN_MATCH_RATIO));
                minMatchBuilder.setMinimumNumberShouldMatch(minMatch);
                Query minMatchQuery = minMatchBuilder.build();

                // Stage 1: Phrase — terms appear close together in a key field; highest bonus.
                Query phraseQuery = buildPhraseQuery(terms);

                BooleanQuery.Builder tiered = new BooleanQuery.Builder();
                tiered.add(orQuery, BooleanClause.Occur.SHOULD);
                tiered.add(new BoostQuery(minMatchQuery, MIN_MATCH_BOOST), BooleanClause.Occur.SHOULD);
                if (phraseQuery != null) {
                    tiered.add(new BoostQuery(phraseQuery, PHRASE_BOOST), BooleanClause.Occur.SHOULD);
                }
                query = tiered.build();
            } else {
                query = orQuery;
            }
        } catch (ParseException e) {
            log.warn("Query parse error: {}", e.getMessage());
            return List.of();
        }

        // Optional project + docType + scope filters
        BooleanQuery.Builder filtered = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST);
        boolean hasFilter = false;
        if (req.project() != null && !req.project().isEmpty()) {
            filtered.add(new TermQuery(new Term(DocumentMapper.F_PROJECT, req.project())),
                    BooleanClause.Occur.FILTER);
            hasFilter = true;
        }
        if (req.docType() != null && !req.docType().isEmpty()) {
            filtered.add(new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, req.docType())),
                    BooleanClause.Occur.FILTER);
            hasFilter = true;
        }
        if (req.scope() != null && !req.scope().isEmpty()) {
            filtered.add(new TermQuery(new Term(DocumentMapper.F_SCOPE, req.scope())),
                    BooleanClause.Occur.FILTER);
            hasFilter = true;
        }
        if (hasFilter) query = filtered.build();

        TopDocs hits = searcher.search(query, req.limit());
        return toResults(searcher, hits, "keyword");
    }

    /**
     * Runs the query analyzer over {@code text} and returns the resulting tokens.
     * Used to build per-term AND and phrase queries from the raw query string.
     */
    private List<String> analyzeTerms(String text) {
        List<String> terms = new ArrayList<>();
        try (TokenStream ts = analyzer.tokenStream(DocumentMapper.F_BODY, text)) {
            CharTermAttribute attr = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                terms.add(attr.toString());
            }
            ts.end();
        } catch (IOException e) {
            log.warn("Term analysis failed: {}", e.getMessage());
        }
        return terms;
    }

    /**
     * Builds a phrase query across PHRASE_FIELDS — each field becomes a SHOULD clause
     * so matching any single field earns the phrase bonus.
     *
     * Returns null if fewer than 2 terms are provided (phrase is undefined for 1 term).
     */
    private Query buildPhraseQuery(List<String> terms) {
        if (terms.size() < 2) return null;
        BooleanQuery.Builder phraseBuilder = new BooleanQuery.Builder();
        for (String field : PHRASE_FIELDS) {
            PhraseQuery.Builder pqb = new PhraseQuery.Builder();
            pqb.setSlop(PHRASE_SLOP);
            for (int i = 0; i < terms.size(); i++) {
                pqb.add(new Term(field, terms.get(i)), i);
            }
            float fieldBoost = FIELD_BOOSTS.getOrDefault(field, 1.0f);
            phraseBuilder.add(new BoostQuery(pqb.build(), fieldBoost), BooleanClause.Occur.SHOULD);
        }
        return phraseBuilder.build();
    }

    public static List<SearchResult> toResults(IndexSearcher searcher, TopDocs hits, String type) throws IOException {
        if (hits.scoreDocs.length == 0) return List.of();

        StoredFields storedFields = searcher.storedFields();

        // First pass: load all docs + find max in-degree for normalization
        record RawHit(Document doc, float rawScore) {}
        List<RawHit> rawHits = new ArrayList<>(hits.scoreDocs.length);
        int maxInDegree = 1; // avoid division by zero
        for (ScoreDoc sd : hits.scoreDocs) {
            Document doc = storedFields.document(sd.doc);
            int inDeg = getInt(doc, DocumentMapper.F_IN_DEGREE);
            if (inDeg > maxInDegree) maxInDegree = inDeg;
            rawHits.add(new RawHit(doc, sd.score));
        }

        // Second pass: apply log-normalized in-degree boost + source-path penalty
        //   + doc-type bonus (class > method) + access-modifier multiplier, then re-sort
        final int maxDeg = maxInDegree;
        return rawHits.stream()
                .map(hit -> {
                    int inDeg = getInt(hit.doc(), DocumentMapper.F_IN_DEGREE);
                    float normBoost = (float) (Math.log1p(inDeg) / Math.log1p(maxDeg));
                    String filePath  = hit.doc().get(DocumentMapper.F_FILE_PATH);
                    float penalty    = sourcePathPenalty(filePath);
                    float typeBonus  = docTypeBonus(hit.doc().get(DocumentMapper.F_DOC_TYPE), filePath);
                    float accessMult = accessMultiplier(hit.doc().get(DocumentMapper.F_ACCESS));
                    float boostedScore = hit.rawScore()
                            * (1.0f + GRAPH_BOOST_WEIGHT * normBoost)
                            * penalty * typeBonus * accessMult;
                    return docToResult(hit.doc(), boostedScore, type);
                })
                .sorted(Comparator.comparingDouble(SearchResult::score).reversed())
                .collect(Collectors.toList());
    }

    static SearchResult docToResult(Document doc, float score, String searchType) {
        String docType = doc.get(DocumentMapper.F_DOC_TYPE);
        if (docType == null) docType = "method"; // backwards compat with pre-docType indexes
        return new SearchResult(
                doc.get(DocumentMapper.F_ID),
                doc.get(DocumentMapper.F_PROJECT),
                doc.get(DocumentMapper.F_PACKAGE),
                doc.get(DocumentMapper.F_CLASS_NAME),
                doc.get(DocumentMapper.F_QUALIFIED_CLASS),
                doc.get(DocumentMapper.F_METHOD_NAME),
                doc.get(DocumentMapper.F_SIGNATURE),
                doc.get(DocumentMapper.F_RETURN_TYPE),
                doc.get(DocumentMapper.F_BODY),
                doc.get(DocumentMapper.F_JAVADOC),
                doc.get(DocumentMapper.F_ACCESS),
                doc.get(DocumentMapper.F_FILE_PATH),
                getInt(doc, DocumentMapper.F_START_LINE),
                getInt(doc, DocumentMapper.F_END_LINE),
                score,
                searchType,
                docType
        );
    }

    /**
     * Penalizes test, benchmark, and non-source files so production classes
     * rank above ancillary code that matches the same tokens.
     *
     * Multipliers (applied after BM25 + graph boost):
     *   /src/test/   → 0.50   (unit/integration tests)
     *   /benchmark/  → 0.30   (JMH benchmarks — heavy false-positive source)
     *   /jmh/        → 0.30
     *   .md / .txt   → 0.20   (documentation files)
     *   otherwise    → 1.00
     */
    /**
     * Bonus multiplier by document type so the ranking order is:
     *   class &gt; method/chunk
     * Applied on top of BM25 so a class whose name matches the query always
     * ranks above individual methods in that class for the same BM25 score.
     */
    static float docTypeBonus(String docType, String filePath) {
        if (!"class".equals(docType)) return 1.0f;
        if (filePath != null) {
            String p = filePath.replace('\\', '/');
            if (p.contains("/src/test/") || p.contains("/test/")) return 1.0f; // test classes don't get class bonus
        }
        return 1.8f;
    }

    /**
     * Multiplier by access modifier so the ranking order is:
     *   public &gt; protected &gt; package-private &gt; private
     * Private implementation details rank last unless the BM25 score is
     * substantially higher (i.e. the query is very specific to that method).
     */
    static float accessMultiplier(String access) {
        if (access == null || access.isBlank()) return 0.75f; // package-private
        return switch (access.toLowerCase()) {
            case "public"    -> 1.00f;
            case "protected" -> 0.85f;
            case "private"   -> 0.50f;
            default          -> 0.75f; // package-private or unrecognised
        };
    }

    static float sourcePathPenalty(String filePath) {
        if (filePath == null) return 1.0f;
        String p = filePath.replace('\\', '/');
        if (p.contains("/benchmark/")) return 0.25f;
        if (p.contains("/jmh/"))        return 0.25f;
        if (p.contains("/src/test/"))   return 0.30f; // stronger — tests are rarely the target
        if (p.contains("/test/"))       return 0.30f;
        if (p.contains("/.github/"))    return 0.10f;
        if (p.endsWith(".md") || p.endsWith(".txt") || p.endsWith(".sh") || p.endsWith(".yml")) return 0.10f;
        return 1.0f;
    }

    private static int getInt(Document doc, String field) {
        String val = doc.get(field);
        if (val == null) return 0;
        try { return Integer.parseInt(val); } catch (NumberFormatException e) { return 0; }
    }
}
