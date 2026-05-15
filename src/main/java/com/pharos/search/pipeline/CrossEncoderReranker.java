package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Post-merge reranker: scores the already-merged list with a cross-encoder model
 * and re-sorts by cross-encoder relevance.
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

        List<SearchResult> reranked = new ArrayList<>(results.size());
        for (int i = 0; i < results.size(); i++) {
            SearchResult r = results.get(i);
            reranked.add(new SearchResult(
                    r.id(), r.project(), r.packageName(), r.className(),
                    r.qualifiedClassName(), r.methodName(), r.signature(),
                    r.returnType(), r.body(), r.javadoc(), r.accessModifier(),
                    r.filePath(), r.startLine(), r.endLine(),
                    scores[i], r.searchType(), r.docType()
            ));
        }
        reranked.sort(Comparator.comparingDouble(SearchResult::score).reversed());

        if (trace != null) trace.record("cross-encoder rerank", t);
        return reranked.subList(0, Math.min(limit, reranked.size()));
    }
}
