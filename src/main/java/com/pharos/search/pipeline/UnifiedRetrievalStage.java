package com.pharos.search.pipeline;

import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.QueryClassification;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.*;
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

    public UnifiedRetrievalStage(KeywordSearchStrategy keywordStrategy,
                                  EmbeddingProvider embedder) {
        this.keywordStrategy = keywordStrategy;
        this.embedder        = embedder;
    }

    @Override
    public List<SearchResult> retrieve(IndexReader reader, SearchRequest req,
                                        SearchTrace trace) throws IOException {
        long t0 = System.currentTimeMillis();
        int  candidateSize = req.limit() * OVERSAMPLE;

        IndexSearcher searcher = new IndexSearcher(reader);

        // ── BM25 recall pass ──────────────────────────────────────────────────
        Query bm25Query = keywordStrategy.buildQuery(new SearchRequest(
                req.query(), SearchRequest.SearchType.KEYWORD, req.project(),
                req.projects(), candidateSize, req.outputFormat(),
                req.docType(), req.scope(), req.oversampleFactor()));

        TopDocs bm25Docs = (bm25Query != null)
                ? searcher.search(bm25Query, candidateSize)
                : new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);

        if (!embedder.isAvailable()) {
            trace.record("unified retrieval (bm25 only — no embedder)", t0);
            int n = Math.min(req.limit(), bm25Docs.scoreDocs.length);
            TopDocs trimmed = new TopDocs(bm25Docs.totalHits,
                    Arrays.copyOf(bm25Docs.scoreDocs, n));
            return KeywordSearchStrategy.toResults(searcher, trimmed, "unified");
        }

        float[] queryVec = embedder.embed(req.query());
        if (queryVec == null) {
            trace.record("unified retrieval (embed failed)", t0);
            int n = Math.min(req.limit(), bm25Docs.scoreDocs.length);
            TopDocs trimmed = new TopDocs(bm25Docs.totalHits,
                    Arrays.copyOf(bm25Docs.scoreDocs, n));
            return KeywordSearchStrategy.toResults(searcher, trimmed, "unified");
        }

        // ── KNN recall pass ───────────────────────────────────────────────────
        Query filter = buildFilter(req);
        Query knnQuery = filter != null
                ? new KnnFloatVectorQuery(DocumentMapper.F_VECTOR, queryVec, candidateSize, filter)
                : new KnnFloatVectorQuery(DocumentMapper.F_VECTOR, queryVec, candidateSize);
        TopDocs knnDocs = searcher.search(knnQuery, candidateSize);

        // ── Union (BM25 ∪ KNN) ────────────────────────────────────────────────
        // Key: Lucene internal doc ID.  Value: BM25 score (0 for KNN-only hits).
        // LinkedHashMap preserves BM25 insertion order (higher BM25 scores first)
        // so that for ties in vector score the BM25 ordering is a stable tiebreaker.
        Map<Integer, Float> unionMap = new LinkedHashMap<>();
        for (ScoreDoc sd : bm25Docs.scoreDocs) unionMap.put(sd.doc, sd.score);
        for (ScoreDoc sd : knnDocs.scoreDocs)  unionMap.putIfAbsent(sd.doc, 0f);

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
        String intentLabel = req.classification() != null ? req.classification().intent() : "default";

        TopDocs reranked = new UnifiedRescorer(queryMultiVec, bm25Bonus)
                .rescore(searcher, unionDocs, req.limit());

        trace.record("unified retrieval (bm25∪knn → vector rerank, intent=" + intentLabel + ")", t0);
        return KeywordSearchStrategy.toResults(searcher, reranked, "unified");
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
            case "BEHAVIORAL"        -> 0.15f;  // vector-leaning — some BM25 recall helps
            case "INTERFACE"         -> 0.05f;  // heavy vector — abstract types need semantics
            case "HYBRID"            -> 0.10f;  // NL with stop words — vector-leaning
            case "KEYWORD"           -> 0.30f;  // balanced — works well for Cat A (exact names)
            case "JAVADOC"           -> 0.50f;  // javadoc field has strong BM25 term signal
            case "LIFECYCLE"         -> 0.50f;  // exception/lifecycle class names are exact matches
            case "KEYWORD_TECHNICAL" -> 0.70f;  // multi-word tech phrases — BM25-friendly Cat B/H
            case "CONFIG"            -> 0.80f;  // setter/getter names — exact keyword matches
            default                  -> BM25_BONUS;
        };
    }

    // ── Rescorer ──────────────────────────────────────────────────────────────

    private static final class UnifiedRescorer extends LateInteractionRescorer {

        private final float bm25Bonus;

        UnifiedRescorer(float[][] queryMultiVec, float bm25Bonus) {
            super(new LateInteractionFloatValuesSource(
                    DocumentMapper.F_CHUNK_VECTORS, queryMultiVec,
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
        return hasFilter ? filter.build() : null;
    }
}
