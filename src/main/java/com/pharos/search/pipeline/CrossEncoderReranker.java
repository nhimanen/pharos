package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;

import java.util.List;

/**
 * Post-merge reranker: scores the already-merged list with a cross-encoder model
 * and fuses its ranking with the incoming first-stage order via equal-weight
 * RRF (k=60) rather than replacing the first-stage score (see {@link RrfFusion}).
 *
 * <p>When the encoder is unavailable ({@link CrossEncoder#isAvailable()} returns false),
 * the input list is returned unchanged — this makes {@code HYBRID_RERANKED} degrade
 * transparently to plain {@code HYBRID}.
 */
public final class CrossEncoderReranker implements RerankStage {

    private final CrossEncoder encoder;

    public CrossEncoderReranker(CrossEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public List<SearchResult> rerank(List<SearchResult> results, SearchRequest req,
                                     IndexReader reader, int limit, SearchTrace trace) {
        if (!encoder.isAvailable() || results.isEmpty()) return results;

        long t = System.currentTimeMillis();

        List<String> passages = results.stream().map(PassageBuilder::build).toList();
        float[] scores = encoder.score(req.query(), passages);

        List<SearchResult> reranked = RrfFusion.fuse(results, scores, null);

        if (trace != null) trace.record("cross-encoder rerank", t);
        return reranked.subList(0, Math.min(limit, reranked.size()));
    }
}
