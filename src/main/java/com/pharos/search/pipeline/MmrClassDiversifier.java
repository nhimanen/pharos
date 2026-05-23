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
 * MMR-inspired class diversity reranker.
 *
 * <p>Penalises results from the same {@code qualifiedClassName} using exponential
 * score decay, so that the merged list surfaces results from more classes before
 * returning a fifth overload of the same method. Only the ordering and score
 * values change; no candidates are ever dropped before the limit.
 *
 * <p>Algorithm (one-pass exponential penalty, same structure as {@link DiversityReranker}):
 * <ol>
 *   <li>Sort candidates by descending score.</li>
 *   <li>For each candidate: {@code adjustedScore = score × penalty^count[qualifiedClass]}</li>
 *   <li>Increment {@code count[qualifiedClass]}.</li>
 *   <li>Re-sort by adjusted score and apply limit.</li>
 * </ol>
 *
 * <p>At the default {@code penalty=0.5}: the 2nd result from a class keeps 50% of its
 * score, the 3rd keeps 25%, etc. A result from a different class must score at most 2×
 * lower to beat the 2nd result from the dominant class — a fair bar for conceptual queries.
 *
 * <p>This reranker is deliberately cheap: it uses only stored fields already present in
 * {@link SearchResult}, with no embedding lookups or cross-encoder calls.
 */
public final class MmrClassDiversifier implements RerankStage {

    private final float penalty;

    /** @param penalty decay factor per repeated class; must be in [0, 1] */
    public MmrClassDiversifier(float penalty) {
        if (penalty < 0f || penalty > 1f) throw new IllegalArgumentException("penalty in [0,1]");
        this.penalty = penalty;
    }

    @Override
    public List<SearchResult> rerank(List<SearchResult> results, SearchRequest req,
                                     IndexReader reader, int limit, SearchTrace trace) {
        if (results.isEmpty()) return results;
        long t = System.currentTimeMillis();

        List<SearchResult> sorted = new ArrayList<>(results);
        sorted.sort(Comparator.comparingDouble(SearchResult::score).reversed());

        Map<String, Integer> classCounts = new HashMap<>();
        List<SearchResult> reranked = new ArrayList<>(sorted.size());

        for (SearchResult r : sorted) {
            String key = r.qualifiedClassName();
            if (key == null || key.isBlank()) key = r.className();
            if (key == null || key.isBlank()) key = r.id();
            int n = classCounts.getOrDefault(key, 0);
            float adjusted = r.score() * (float) Math.pow(penalty, n);
            classCounts.put(key, n + 1);
            reranked.add(new SearchResult(
                    r.id(), r.project(), r.packageName(), r.className(),
                    r.qualifiedClassName(), r.methodName(), r.signature(),
                    r.returnType(), r.body(), r.javadoc(), r.accessModifier(),
                    r.filePath(), r.startLine(), r.endLine(),
                    adjusted, r.searchType(), r.docType()
            ));
        }

        reranked.sort(Comparator.comparingDouble(SearchResult::score).reversed());
        if (trace != null) trace.record("mmr class diversity", t);
        return reranked.subList(0, Math.min(limit, reranked.size()));
    }
}
