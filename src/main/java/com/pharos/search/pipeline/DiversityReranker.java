package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Diversity reranker that penalises consecutive hits of the same doc-type
 * (method / class / chunk) so the returned list interleaves types.
 *
 * <p>Algorithm (one-pass exponential penalty):
 * <ol>
 *   <li>Iterate candidates in descending score order.</li>
 *   <li>For each candidate: {@code adjustedScore = score × penalty^count[docType]}</li>
 *   <li>Increment {@code count[docType]}.</li>
 *   <li>Re-sort by adjusted score and apply limit.</li>
 * </ol>
 *
 * <p>At {@code penalty=0.5} (default) each additional hit of the same type loses half
 * its score relative to the previous one. At {@code penalty=1.0} this is a no-op;
 * at {@code penalty=0.0} only the first hit of each type survives with its original score.
 */
public final class DiversityReranker implements RerankStage {

    private final float penalty;

    /** @param penalty decay factor per repeated doc-type; must be in [0, 1] */
    public DiversityReranker(float penalty) {
        if (penalty < 0f || penalty > 1f) throw new IllegalArgumentException("penalty must be in [0, 1]");
        this.penalty = penalty;
    }

    @Override
    public List<SearchResult> rerank(List<SearchResult> results, SearchRequest req,
                                     IndexReader reader, int limit, SearchTrace trace) {
        if (results.isEmpty()) return results;

        long t = System.currentTimeMillis();

        List<SearchResult> sorted = new ArrayList<>(results);
        sorted.sort(Comparator.comparingDouble(SearchResult::score).reversed());

        Map<String, Integer> counts = new HashMap<>();
        List<SearchResult> reranked = new ArrayList<>(sorted.size());

        for (SearchResult r : sorted) {
            String key = r.docType() != null ? r.docType() : "unknown";
            int n = counts.getOrDefault(key, 0);
            float adjusted = r.score() * (float) Math.pow(penalty, n);
            counts.put(key, n + 1);
            reranked.add(new SearchResult(
                    r.id(), r.project(), r.packageName(), r.className(),
                    r.qualifiedClassName(), r.methodName(), r.signature(),
                    r.returnType(), r.body(), r.javadoc(), r.accessModifier(),
                    r.filePath(), r.startLine(), r.endLine(),
                    adjusted, r.searchType(), r.docType()
            ));
        }

        reranked.sort(Comparator.comparingDouble(SearchResult::score).reversed());

        if (trace != null) trace.record("diversity rerank", t);
        return reranked.subList(0, Math.min(limit, reranked.size()));
    }
}
