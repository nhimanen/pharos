package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;

import java.util.*;

/**
 * Fuses multiple ranked result lists using Reciprocal Rank Fusion (RRF).
 *
 * <p>Score formula (Cormack et al., SIGIR 2009):
 * <pre>
 *   RRF(d) = Σ  1 / (k + rank_L(d))
 *           L∈lists
 * </pre>
 * where {@code k=60} is the standard smoothing constant. Documents absent
 * from a list contribute 0 from that list.
 *
 * <p>Unlike {@link BordaMerger}, RRF needs no per-intent weights — it is
 * parameter-free beyond {@code k} and is robust to score-scale differences
 * between keyword and vector retrievers.
 *
 * <p>When exactly two lists are fused, documents present in both receive a
 * 1.5× agreement bonus (matching the Borda convention so comparisons are fair).
 */
public final class RrfMerger implements MergeStage {

    private static final int    K              = 60;
    private static final double AGREEMENT_BONUS = 1.5;

    @Override
    public List<SearchResult> merge(List<List<SearchResult>> candidates,
                                    SearchRequest req, SearchTrace trace) {
        long t = System.currentTimeMillis();
        List<SearchResult> result = fuse(candidates, req.limit());
        if (trace != null) trace.record("rrf merge", t);
        return result;
    }

    private static List<SearchResult> fuse(List<List<SearchResult>> lists, int limit) {
        Map<String, Double>       scores     = new LinkedHashMap<>();
        Map<String, SearchResult> byId       = new HashMap<>();
        List<Set<String>>         membership = new ArrayList<>(lists.size());

        for (List<SearchResult> list : lists) {
            Set<String> inThis = new HashSet<>();
            for (int i = 0; i < list.size(); i++) {
                SearchResult r = list.get(i);
                scores.merge(r.id(), 1.0 / (K + i + 1), Double::sum);
                byId.putIfAbsent(r.id(), r);
                inThis.add(r.id());
            }
            membership.add(inThis);
        }

        // Agreement bonus when a document appears in every list
        if (lists.size() == 2) {
            Set<String> inFirst  = membership.get(0);
            Set<String> inSecond = membership.get(1);
            scores.replaceAll((id, score) ->
                    inFirst.contains(id) && inSecond.contains(id)
                            ? score * AGREEMENT_BONUS
                            : score);
        }

        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(limit)
                .map(e -> {
                    SearchResult r = byId.get(e.getKey());
                    return new SearchResult(
                            r.id(), r.project(), r.packageName(), r.className(),
                            r.qualifiedClassName(), r.methodName(), r.signature(),
                            r.returnType(), r.body(), r.javadoc(), r.accessModifier(),
                            r.filePath(), r.startLine(), r.endLine(),
                            e.getValue().floatValue(), "hybrid", r.docType()
                    );
                })
                .toList();
    }
}
