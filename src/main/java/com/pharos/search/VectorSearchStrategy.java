package com.pharos.search;

import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import org.apache.lucene.document.LateInteractionField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.LateInteractionRescorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Semantic vector search using Lucene's built-in HNSW KNN index.
 *
 * <p><b>Two-phase when {@code chunkVectors} is present (new indexes):</b>
 * <ol>
 *   <li>HNSW retrieval on the representative {@code vectorEmbedding} field — fast
 *       approximate nearest-neighbour, fetches {@code limit × RESCORE_FACTOR} candidates.</li>
 *   <li>Late-interaction rescoring via {@link LateInteractionRescorer}: for each candidate
 *       the stored {@code chunkVectors} field is compared against the single query vector
 *       using MAX(cosine(query, chunk_i)) — the best-matching chunk wins.  Falls back to
 *       the first-pass score when a document has no {@code chunkVectors} field.</li>
 * </ol>
 *
 * <p><b>Single-phase fallback for old indexes</b> (no {@code chunkVectors} field):
 * plain HNSW on {@code vectorEmbedding}, identical to the previous behaviour.
 */
public class VectorSearchStrategy {

    private static final Logger log = LoggerFactory.getLogger(VectorSearchStrategy.class);

    /** Oversampling factor for first-pass HNSW retrieval before late-interaction rescoring. */
    private static final int RESCORE_FACTOR = 3;

    private final EmbeddingProvider embedder;

    public VectorSearchStrategy(EmbeddingProvider embedder) {
        this.embedder = embedder;
    }

    public List<SearchResult> search(IndexReader reader, SearchRequest req) throws IOException {
        if (!embedder.isAvailable()) {
            log.warn("Vector search requested but no embedding provider configured.");
            return List.of();
        }

        float[] queryVector = embedder.embed(req.query());
        if (queryVector == null) {
            log.warn("Embedding returned null for query: {}", req.query());
            return List.of();
        }

        Query filter      = buildFilter(req);
        int   limit       = req.limit();
        int   firstPassK  = limit * RESCORE_FACTOR;

        Query vectorQuery = filter != null
                ? new KnnFloatVectorQuery(DocumentMapper.F_VECTOR, queryVector, firstPassK, filter)
                : new KnnFloatVectorQuery(DocumentMapper.F_VECTOR, queryVector, firstPassK);

        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs firstPass = searcher.search(vectorQuery, firstPassK);

        // Late-interaction rescoring when chunkVectors field is present
        if (hasChunkVectors(reader) && firstPass.totalHits.value() > 0) {
            // Wrap single query vector as float[][] — the rescorer treats it as a 1-element
            // multi-vector, so SUM_MAX_SIM becomes MAX(cosine(queryVec, chunkVec_i)).
            float[][] queryMultiVec = new float[][] { queryVector };
            TopDocs reranked = LateInteractionRescorer
                    .withFallbackToFirstPassScore(
                            DocumentMapper.F_CHUNK_VECTORS, queryMultiVec,
                            VectorSimilarityFunction.COSINE)
                    .rescore(searcher, firstPass, limit);
            return KeywordSearchStrategy.toResults(searcher, reranked, "vector");
        }

        // Fallback: old index or no chunks — truncate first-pass to limit
        if (firstPass.scoreDocs.length > limit) {
            firstPass = new TopDocs(firstPass.totalHits,
                    java.util.Arrays.copyOf(firstPass.scoreDocs, limit));
        }
        return KeywordSearchStrategy.toResults(searcher, firstPass, "vector");
    }

    /**
     * Builds a {@link SnippetResolver} that identifies the best-matching chunk
     * for each result by computing argmax cosine similarity between the query
     * vector and each stored chunk vector.
     *
     * <p>The resolver is lazy — it reads stored fields and computes similarities only
     * for the results that survive to the final ranked set, not for the full HNSW pool.
     *
     * @param reader   the index reader used for the current search (cached, stays open)
     * @param queryVec the embedded query vector
     */
    public SnippetResolver buildVectorResolver(IndexReader reader, float[] queryVec) {
        if (queryVec == null) return SnippetResolver.none();
        return result -> {
            try {
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs hits = searcher.search(
                        new TermQuery(new Term(DocumentMapper.F_ID, result.id())), 1);
                if (hits.scoreDocs.length == 0) return null;

                var storedFields = searcher.storedFields();
                var doc = storedFields.document(hits.scoreDocs[0].doc);

                org.apache.lucene.util.BytesRef chunkVecsRef = doc.getBinaryValue(DocumentMapper.F_CHUNK_VECTORS);
                byte[] rangesBytes = doc.getBinaryValue(DocumentMapper.F_CHUNK_LINE_RANGES) != null
                        ? doc.getBinaryValue(DocumentMapper.F_CHUNK_LINE_RANGES).bytes : null;
                if (chunkVecsRef == null || rangesBytes == null) return null;

                float[][] chunkVectors = LateInteractionField.decode(chunkVecsRef);
                int[][] ranges = DocumentMapper.decodeLineRanges(rangesBytes);
                if (chunkVectors.length == 0 || ranges.length == 0) return null;

                // argmax cosine(queryVec, chunkVec_i)
                int bestIdx = 0;
                float bestSim = -1f;
                for (int i = 0; i < chunkVectors.length; i++) {
                    float sim = cosine(queryVec, chunkVectors[i]);
                    if (sim > bestSim) { bestSim = sim; bestIdx = i; }
                }
                if (bestIdx >= ranges.length) return null;
                return new Snippet(null, ranges[bestIdx][0], ranges[bestIdx][1]);
            } catch (Exception e) {
                log.debug("Vector resolver failed for {}: {}", result.label(), e.getMessage());
                return null;
            }
        };
    }

    private static float cosine(float[] a, float[] b) {
        float dot = 0, na = 0, nb = 0;
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) { dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i]; }
        float denom = (float) (Math.sqrt(na) * Math.sqrt(nb));
        return denom < 1e-8f ? 0f : dot / denom;
    }

    /** Returns true when the index contains the {@code chunkVectors} late-interaction field. */
    private static boolean hasChunkVectors(IndexReader reader) {
        return reader.leaves().stream().anyMatch(
                ctx -> ctx.reader().getFieldInfos().fieldInfo(DocumentMapper.F_CHUNK_VECTORS) != null);
    }

    private static Query buildFilter(SearchRequest req) {
        boolean hasProject = req.project() != null && !req.project().isEmpty();
        boolean hasDocType = req.docType() != null && !req.docType().isEmpty();
        boolean hasScope   = req.scope()   != null && !req.scope().isEmpty();

        if (!hasProject && !hasDocType && !hasScope) return null;
        if (hasProject && !hasDocType && !hasScope)
            return new TermQuery(new Term(DocumentMapper.F_PROJECT, req.project()));
        if (!hasProject && hasDocType && !hasScope)
            return new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, req.docType()));
        if (!hasProject && !hasDocType)
            return new TermQuery(new Term(DocumentMapper.F_SCOPE, req.scope()));

        BooleanQuery.Builder b = new BooleanQuery.Builder();
        if (hasProject) b.add(new TermQuery(new Term(DocumentMapper.F_PROJECT, req.project())), BooleanClause.Occur.FILTER);
        if (hasDocType) b.add(new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, req.docType())), BooleanClause.Occur.FILTER);
        if (hasScope)   b.add(new TermQuery(new Term(DocumentMapper.F_SCOPE,    req.scope())),   BooleanClause.Occur.FILTER);
        return b.build();
    }
}
