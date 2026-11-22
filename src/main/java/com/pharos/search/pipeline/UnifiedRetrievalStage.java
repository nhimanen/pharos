package com.pharos.search.pipeline;

import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.QueryClassification;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import com.pharos.search.distributed.CandidateShaper;
import com.pharos.search.distributed.RawCandidate;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified retrieval: candidate pool = BM25 hits ∪ KNN hits, ranked by vector similarity.
 *
 * <h3>Design</h3>
 * <ol>
 *   <li><b>BM25 recall pass</b> — standard keyword search fetches up to
 *       {@code limit × OVERSAMPLE} candidates.  Documents reach this set by
 *       matching query terms in any indexed field.</li>
 *   <li><b>KNN recall pass</b> — HNSW approximate nearest-neighbour search on
 *       the document-level {@code vectorEmbedding} field fetches up to
 *       {@code limit × OVERSAMPLE} semantic candidates.  These may have zero
 *       BM25 score — they enter the pool solely on semantic grounds.</li>
 *   <li><b>Union</b> — the two candidate sets are merged by Lucene internal doc ID;
 *       duplicates are deduplicated.  BM25 score is kept as a tiebreaker but is
 *       NOT the primary ranking signal.</li>
 *   <li><b>Late-interaction rescoring</b> — for every doc in the union,
 *       {@link LateInteractionRescorer} computes
 *       {@code MAX(cosine(queryVec, chunkVec_i))} over the stored chunk vectors
 *       ({@code chunkVectors} field).  The final score is this similarity value;
 *       the BM25 first-pass score is discarded from ranking.</li>
 * </ol>
 *
 * <h3>Why this is better than the old approach</h3>
 * The old {@code UnifiedRetrievalStage} boosted BM25 scores by a vector multiplier
 * ({@code bm25 × (1 + 0.3 × cosine)}).  Documents with zero BM25 match scored zero
 * regardless of semantic relevance.  This design eliminates that bias: both recall
 * mechanisms contribute candidates on equal footing; ranking is a uniform vector
 * similarity over the combined pool.
 *
 * <h3>Graceful degradation</h3>
 * <ul>
 *   <li>If the embedder is unavailable, returns pure BM25 results.</li>
 *   <li>If a document has no {@code chunkVectors} field, the rescorer falls back to
 *       a small fraction of the BM25 score so the document is not completely hidden.</li>
 * </ul>
 */
public class UnifiedRetrievalStage implements RetrievalStage {

    private static final Logger log = LoggerFactory.getLogger(UnifiedRetrievalStage.class);

    /** How many candidates to fetch from each recall pass before merging. */
    private static final int OVERSAMPLE = 5;

    /**
     * Maximum contribution of BM25 to the final score.
     * {@code vectorSim ∈ [0,1]}, so a bonus of 0.3 means keyword-hit documents
     * can score up to 1.3 vs a maximum of 1.0 for pure semantic hits.
     */
    private static final float BM25_BONUS = 0.3f;

    /**
     * BM25 score at which the tanh saturation reaches ~76 % of its maximum.
     * Calibrated to typical relevant-document BM25 scores in the benchmark
     * (50–300 range).  Scores above ~450 saturate to ≈ BM25_BONUS with no
     * further benefit.
     */
    private static final float BM25_SCALE = 150f;

    /** Fallback weight for documents without chunk vectors (old indexes). */
    private static final float BM25_FALLBACK_WEIGHT = 0.01f;

    private final KeywordSearchStrategy keywordStrategy;
    private final EmbeddingProvider     embedder;
    private final String vectorField;
    private final String chunkVectorField;
    /** Creates the searcher of every search; a plain {@link IndexSearcher} unless injected. */
    private final SearcherFactory searcherFactory;

    /** Search type of unified results. */
    public static final String TYPE = "unified";

    public UnifiedRetrievalStage(KeywordSearchStrategy keywordStrategy,
                                  EmbeddingProvider embedder) {
        this(keywordStrategy, embedder, new SearcherFactory());
    }

    public UnifiedRetrievalStage(KeywordSearchStrategy keywordStrategy,
                                  EmbeddingProvider embedder, SearcherFactory searcherFactory) {
        this.keywordStrategy  = keywordStrategy;
        this.searcherFactory  = searcherFactory;
        this.embedder         = embedder;
        this.vectorField      = DocumentMapper.vectorFieldFor(embedder.modelId());
        this.chunkVectorField = DocumentMapper.chunkVectorFieldFor(embedder.modelId());
    }

    @Override
    public List<SearchResult> retrieve(IndexReader reader, SearchRequest req,
                                        SearchTrace trace) throws IOException {
        RawRetrieval raw = retrieveRaw(searcherFactory.newSearcher(reader, null), req, null, true, trace);
        return CandidateShaper.shape(raw.candidates(), TYPE, raw.shapingIntent());
    }

    /**
     * Raw candidates, the query embedded here (BM25 only when the embedder is unavailable). Sets the
     * BM25 similarity on {@code searcher}: use one searcher per call.
     */
    @Override
    public List<RawCandidate> retrieveRaw(IndexSearcher searcher, SearchRequest req,
                                          SearchTrace trace) throws IOException {
        return retrieveRaw(searcher, req, null, true, trace).candidates();
    }

    /**
     * Raw candidates for an already embedded query (a shard serving a coordinator, which embeds
     * once); {@code queryVec == null} means BM25 only. Returns the intent to shape them with, which
     * depends on whether the vector pass ran. Sets the BM25 similarity on {@code searcher}: use one
     * searcher per call.
     */
    public RawRetrieval retrieveRaw(IndexSearcher searcher, SearchRequest req, float[] queryVec,
                                    SearchTrace trace) throws IOException {
        return retrieveRaw(searcher, req, queryVec, false, trace);
    }

    /**
     * Raw candidates and the intent {@link CandidateShaper#shape} must use for them: the router's
     * intent ({@code "default"} without a classification) when the vector pass ran, none for the
     * BM25-only fallback.
     */
    public record RawRetrieval(List<RawCandidate> candidates, String shapingIntent) {}

    private static String vectorPassIntent(SearchRequest req) {
        return req.classification() != null ? req.classification().intent() : "default";
    }

    private RawRetrieval retrieveRaw(IndexSearcher searcher, SearchRequest req, float[] givenVec,
                            boolean embedHere, SearchTrace trace) throws IOException {
        long t0 = System.currentTimeMillis();
        TopDocs bm25Docs = bm25Recall(searcher, req);

        if (embedHere && !embedder.isAvailable()) {
            traceStep(trace, "unified retrieval (bm25 only — no embedder)", t0);
            return bm25Only(searcher, req, bm25Docs);
        }

        float[] queryVec = embedHere ? embedder.embed(req.query()) : givenVec;
        if (queryVec == null) {
            traceStep(trace, embedHere ? "unified retrieval (embed failed)"
                                    : "unified retrieval (bm25 only — no query vector)", t0);
            return bm25Only(searcher, req, bm25Docs);
        }

        TopDocs knnDocs = knnRecall(searcher, req, queryVec);
        RawRetrieval out = rescore(searcher, req, queryVec, bm25Docs.scoreDocs, knnDocs.scoreDocs);
        traceStep(trace, "unified retrieval (bm25∪knn → vector rerank, intent=" + out.shapingIntent() + ")", t0);
        return out;
    }

    /** Candidates each recall pass fetches for a result limit. */
    public static int candidateSize(int limit) {
        return limit * OVERSAMPLE;
    }

    /**
     * The BM25 recall pass: the top {@link #candidateSize} by plain BM25 (an IndexSearcher's
     * default), whatever the searcher was set to before. Sets the similarity on {@code searcher}.
     */
    public TopDocs bm25Recall(IndexSearcher searcher, SearchRequest req) throws IOException {
        int  candidateSize = candidateSize(req.limit());
        searcher.setSimilarity(new BM25Similarity());

        Query bm25Query = keywordStrategy.buildQuery(new SearchRequest(
                req.query(), SearchRequest.SearchType.KEYWORD, req.project(),
                req.projects(), candidateSize, req.outputFormat(),
                req.docType(), req.scope(), req.oversampleFactor()).withKeyFilter(req.keyFilter()));

        return (bm25Query != null)
                ? searcher.search(bm25Query, candidateSize)
                : new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    }

    /** The KNN recall pass: the top {@link #candidateSize} by document-vector similarity. */
    public TopDocs knnRecall(IndexSearcher searcher, SearchRequest req, float[] queryVec) throws IOException {
        int candidateSize = candidateSize(req.limit());
        Query filter = buildFilter(req);
        Query knnQuery = filter != null
                ? new KnnFloatVectorQuery(vectorField, queryVec, candidateSize, filter)
                : new KnnFloatVectorQuery(vectorField, queryVec, candidateSize);
        return searcher.search(knnQuery, candidateSize);
    }

    /**
     * The union of the two recall passes' candidates (BM25 order first, then KNN-only ones), rescored
     * by late interaction with the intent's BM25 bonus; the top {@code req.limit()} with the intent to
     * shape them with. A coordinator passes the globally selected candidates of one shard.
     */
    public RawRetrieval rescore(IndexSearcher searcher, SearchRequest req, float[] queryVec, ScoreDoc[] bm25,
                                ScoreDoc[] knn) throws IOException {
        // ── Union (BM25 ∪ KNN) ────────────────────────────────────────────────
        // Key: Lucene internal doc ID.  Value: BM25 score (0 for KNN-only hits).
        // LinkedHashMap preserves BM25 insertion order (higher BM25 scores first)
        // so that for ties in vector score the BM25 ordering is a stable tiebreaker.
        Map<Integer, Float> unionMap = new LinkedHashMap<>();
        for (ScoreDoc sd : bm25) unionMap.put(sd.doc, sd.score);
        for (ScoreDoc sd : knn)  unionMap.putIfAbsent(sd.doc, 0f);

        ScoreDoc[] unionArray = new ScoreDoc[unionMap.size()];
        int idx = 0;
        for (Map.Entry<Integer, Float> e : unionMap.entrySet()) {
            unionArray[idx++] = new ScoreDoc(e.getKey(), e.getValue());
        }
        TopDocs unionDocs = new TopDocs(
                new TotalHits(unionArray.length, TotalHits.Relation.EQUAL_TO), unionArray);

        // ── Late-interaction rescoring over the unified pool ──────────────────
        // Wraps the single query vector as a 1-row multi-vector.
        // SUM_MAX_SIM with one row = MAX(cosine(queryVec, chunkVec_i)).
        float[][] queryMultiVec = new float[][] { queryVec };

        // Adaptive BM25 bonus from QueryRouter classification — or default if no router ran
        float bm25Bonus = adaptiveBm25Bonus(req.classification());
        String intentLabel = vectorPassIntent(req);

        TopDocs reranked = new UnifiedRescorer(chunkVectorField, queryMultiVec, bm25Bonus)
                .rescore(searcher, unionDocs, req.limit());
        return new RawRetrieval(KeywordSearchStrategy.rawCandidates(searcher, reranked, TYPE), intentLabel);
    }

    /** The BM25-only result (no query vector): the BM25 recall candidates trimmed to the limit. */
    public RawRetrieval bm25Only(IndexSearcher searcher, SearchRequest req, ScoreDoc[] bm25) throws IOException {
        return bm25Only(searcher, req, new TopDocs(new TotalHits(bm25.length, TotalHits.Relation.EQUAL_TO), bm25));
    }

    /** The BM25 recall pass trimmed to the limit, shaped without an intent. */
    private static RawRetrieval bm25Only(IndexSearcher searcher, SearchRequest req, TopDocs bm25Docs)
            throws IOException {
        int n = Math.min(req.limit(), bm25Docs.scoreDocs.length);
        TopDocs trimmed = new TopDocs(bm25Docs.totalHits,
                Arrays.copyOf(bm25Docs.scoreDocs, n));
        return new RawRetrieval(KeywordSearchStrategy.rawCandidates(searcher, trimmed, TYPE), null);
    }

    private static void traceStep(SearchTrace trace, String step, long t0) {
        if (trace != null) trace.record(step, t0);
    }

    // ── Adaptive BM25 weight ──────────────────────────────────────────────────

    /**
     * Maps the query intent detected by the {@link com.pharos.search.QueryRouter} to
     * the appropriate BM25 contribution in the scoring formula
     * {@code score = vectorSim + bm25Bonus × tanh(bm25 / BM25_SCALE)}.
     *
     * <ul>
     *   <li>Behavioral / interface queries need strong vector recall — small BM25 bonus.</li>
     *   <li>Config / lifecycle / technical-phrase queries have strong exact keyword signal — large BM25 bonus.</li>
     *   <li>Single-token / CamelCase queries are balanced (current default wins Cat A).</li>
     * </ul>
     */
    private static float adaptiveBm25Bonus(com.pharos.search.QueryClassification c) {
        if (c == null || c.intent() == null) return BM25_BONUS;
        return switch (c.intent()) {
            // Vector-heavy: class type / behavioral queries need semantic recall
            case "BEHAVIORAL"        -> 0.15f;
            case "INTERFACE",
                 "ABSTRACT",
                 "ENUM",
                 "RECORD",
                 "ANNOTATION"        -> 0.05f;  // heavy vector — abstract/specific type queries
            case "HYBRID"            -> 0.10f;  // NL with stop words — vector-leaning
            case "IMPLEMENTATION"    -> 0.20f;  // concrete class names partly in BM25
            // Balanced / BM25-heavy: keyword signal is strong
            case "KEYWORD"           -> 0.30f;
            case "JAVADOC"           -> 0.50f;
            case "LIFECYCLE"         -> 0.50f;
            case "KEYWORD_TECHNICAL" -> 0.70f;
            case "CONFIG"            -> 0.80f;
            default                  -> BM25_BONUS;
        };
    }

    // ── Rescorer ──────────────────────────────────────────────────────────────

    private static final class UnifiedRescorer extends LateInteractionRescorer {

        private final float bm25Bonus;

        UnifiedRescorer(String chunkVectorField, float[][] queryMultiVec, float bm25Bonus) {
            super(new LateInteractionFloatValuesSource(
                    chunkVectorField, queryMultiVec,
                    VectorSimilarityFunction.COSINE));
            this.bm25Bonus = bm25Bonus;
        }

        /**
         * Adaptive combined score: {@code vectorSim + bm25Bonus × tanh(bm25 / BM25_SCALE)}.
         * The {@code bm25Bonus} is selected per-query by the router's intent classification.
         */
        @Override
        protected float combine(float bm25Score, boolean hasVec, double vectorSim) {
            if (!hasVec) {
                return bm25Score > 0
                        ? (float) Math.tanh(bm25Score / BM25_SCALE) * bm25Bonus * BM25_FALLBACK_WEIGHT
                        : 0f;
            }
            float vecScore = (float) vectorSim;
            float bm25Norm = bm25Score > 0 ? (float) Math.tanh(bm25Score / BM25_SCALE) : 0f;
            return vecScore + bm25Bonus * bm25Norm;
        }
    }

    // ── Filter builder ────────────────────────────────────────────────────────

    private static Query buildFilter(SearchRequest req) {
        BooleanQuery.Builder filter = new BooleanQuery.Builder();
        boolean hasFilter = false;

        if (req.project() != null && !req.project().isEmpty()) {
            filter.add(new TermQuery(new Term(DocumentMapper.F_PROJECT, req.project())),
                    BooleanClause.Occur.MUST);
            hasFilter = true;
        }
        if (req.docType() != null && !req.docType().isEmpty()) {
            filter.add(new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, req.docType())),
                    BooleanClause.Occur.MUST);
            hasFilter = true;
        }
        if (req.scope() != null && !req.scope().isEmpty()) {
            filter.add(new TermQuery(new Term(DocumentMapper.F_SCOPE, req.scope())),
                    BooleanClause.Occur.MUST);
            hasFilter = true;
        }
        org.apache.lucene.search.Query keys = DocumentMapper.keyFilterQuery(req.keyFilter());
        if (keys != null) {
            filter.add(keys, BooleanClause.Occur.MUST);
            hasFilter = true;
        }
        return hasFilter ? filter.build() : null;
    }
}
