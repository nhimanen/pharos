package com.pharos.search.pipeline;

import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.*;
import org.apache.lucene.search.LateInteractionRescorer;
import org.apache.lucene.search.LateInteractionFloatValuesSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Unified retrieval stage: a single BM25 pass with late-interaction vector similarity
 * applied as a multiplicative boost — producing one ranked list without a separate
 * vector retrieval or merge step.
 *
 * <p>Score formula (per document):
 * <pre>
 *   score = bm25_score × (1 + VECTOR_WEIGHT × lateInteractionSimilarity)
 * </pre>
 * where {@code lateInteractionSimilarity} = MAX(cosine(queryVec, chunkVec_i)) over
 * all stored chunk vectors for the document.
 *
 * <p>Falls back gracefully:
 * <ul>
 *   <li>If the embedding provider is unavailable, returns pure BM25 results.</li>
 *   <li>If a document has no {@code chunkVectors} field (old index), the vector
 *       term is 0 and the score equals the BM25 score unchanged.</li>
 * </ul>
 *
 * <p>This stage is intended for use as a single retriever in the pipeline with
 * no subsequent merge step ({@link com.pharos.search.SearchRequest.SearchType#UNIFIED}).
 */
public class UnifiedRetrievalStage implements RetrievalStage {

    private static final Logger log = LoggerFactory.getLogger(UnifiedRetrievalStage.class);

    /**
     * Weight of the late-interaction vector score relative to BM25.
     * {@code score = bm25 × (1 + VECTOR_WEIGHT × vectorSim)}.
     * At 0.3: a perfect vector match lifts BM25 by up to 30 %.
     */
    private static final float VECTOR_WEIGHT = 0.3f;

    /**
     * Oversampling factor for the BM25 first pass before rescoring.
     * Wider candidate set lets the vector signal promote results that ranked
     * just outside the top-K on BM25 alone.
     */
    private static final int OVERSAMPLE = 5;

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

        // BM25 first pass — fetch more candidates than needed so the vector
        // signal can promote results that BM25 ranked just outside the top-K.
        SearchRequest wideReq = new SearchRequest(
                req.query(), SearchRequest.SearchType.KEYWORD, req.project(),
                req.projects(), req.limit() * OVERSAMPLE, req.outputFormat(),
                req.docType(), req.scope(), req.oversampleFactor());
        List<SearchResult> bm25Results = keywordStrategy.search(reader, wideReq);

        if (bm25Results.isEmpty() || !embedder.isAvailable()) {
            trace.record("unified retrieval (bm25 only)", t0);
            return bm25Results.subList(0, Math.min(req.limit(), bm25Results.size()));
        }

        // Embed the query as a single vector — truncation is handled inside the embedder.
        float[] queryVec = embedder.embed(req.query());
        if (queryVec == null) {
            trace.record("unified retrieval (embed failed)", t0);
            return bm25Results.subList(0, Math.min(req.limit(), bm25Results.size()));
        }

        // Reconstruct TopDocs from BM25 results for the rescorer.
        // The rescorer needs doc-internal Lucene doc IDs, so we re-run BM25
        // through the searcher to get proper ScoreDoc objects.
        IndexSearcher searcher  = new IndexSearcher(reader);
        Query         bm25Query = keywordStrategy.buildQuery(wideReq);
        if (bm25Query == null) {
            trace.record("unified retrieval (query build failed)", t0);
            return bm25Results.subList(0, Math.min(req.limit(), bm25Results.size()));
        }
        TopDocs firstPass = searcher.search(bm25Query, req.limit() * OVERSAMPLE);

        // Late-interaction rescoring: combined score = bm25 × (1 + α × vectorSim).
        float[][] queryMultiVec = new float[][] { queryVec };
        TopDocs reranked = new UnifiedRescorer(queryMultiVec)
                .rescore(searcher, firstPass, req.limit());

        trace.record("unified retrieval", t0);
        return KeywordSearchStrategy.toResults(searcher, reranked, "unified");
    }

    // -------------------------------------------------------------------------
    // Custom rescorer — combines BM25 and late-interaction multiplicatively
    // -------------------------------------------------------------------------

    private static final class UnifiedRescorer extends LateInteractionRescorer {

        UnifiedRescorer(float[][] queryMultiVec) {
            super(new LateInteractionFloatValuesSource(
                    DocumentMapper.F_CHUNK_VECTORS, queryMultiVec,
                    VectorSimilarityFunction.COSINE));
        }

        @Override
        protected float combine(float firstPassScore, boolean valuePresent, double sourceValue) {
            float vectorSim = valuePresent ? (float) sourceValue : 0f;
            // Multiplicative boost: preserves BM25 ordering baseline while letting
            // a strong vector match lift a result by up to VECTOR_WEIGHT × 100 %.
            return firstPassScore * (1f + VECTOR_WEIGHT * vectorSim);
        }
    }
}
