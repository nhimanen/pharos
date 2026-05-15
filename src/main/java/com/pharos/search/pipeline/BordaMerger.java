package com.pharos.search.pipeline;

import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;

import java.util.*;

/**
 * Fuses multiple ranked result lists using Borda count with agreement bonus.
 *
 * <p>For exactly two lists (keyword + vector), adaptive weights are applied:
 * CamelCase queries favour keyword (0.8/0.2); natural-language queries use equal weights (0.5/0.5).
 * Documents present in both lists receive a 1.5× agreement bonus.
 *
 * <p>For N > 2 lists, equal weights are used and the agreement bonus is not applied.
 *
 * @see com.pharos.search.HybridSearchStrategy
 */
public final class BordaMerger implements MergeStage {

    private static final double AGREEMENT_BONUS = 1.5;
    private static final double KW_WEIGHT_NAME  = 0.8;
    private static final double VEC_WEIGHT_NAME = 0.2;
    private static final double KW_WEIGHT_NL    = 0.5;
    private static final double VEC_WEIGHT_NL   = 0.5;

    @Override
    public List<SearchResult> merge(List<List<SearchResult>> candidates,
                                    int limit, String query, SearchTrace trace) {
        long t = System.currentTimeMillis();
        List<SearchResult> result = candidates.size() == 2
                ? fuseTwoLists(candidates.get(0), candidates.get(1), limit, query)
                : fuseEqualWeight(candidates, limit);
        if (trace != null) trace.record("borda merge", t);
        return result;
    }

    private List<SearchResult> fuseTwoLists(List<SearchResult> kwResults,
                                             List<SearchResult> vecResults,
                                             int limit, String query) {
        boolean nameLookup = containsCamelCase(query);
        double kwWeight  = nameLookup ? KW_WEIGHT_NAME : KW_WEIGHT_NL;
        double vecWeight = nameLookup ? VEC_WEIGHT_NAME : VEC_WEIGHT_NL;

        int kwN  = kwResults.size();
        int vecN = vecResults.size();

        Map<String, Double> scores = new LinkedHashMap<>();
        Map<String, SearchResult> resultById = new HashMap<>();
        Set<String> inKeyword = new HashSet<>();
        Set<String> inVector  = new HashSet<>();

        for (int i = 0; i < kwN; i++) {
            SearchResult r = kwResults.get(i);
            scores.merge(r.id(), kwWeight * (kwN - i), Double::sum);
            resultById.put(r.id(), r);
            inKeyword.add(r.id());
        }
        for (int i = 0; i < vecN; i++) {
            SearchResult r = vecResults.get(i);
            scores.merge(r.id(), vecWeight * (vecN - i), Double::sum);
            resultById.putIfAbsent(r.id(), r);
            inVector.add(r.id());
        }

        scores.replaceAll((id, score) ->
                inKeyword.contains(id) && inVector.contains(id)
                        ? score * AGREEMENT_BONUS
                        : score);

        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(limit)
                .map(e -> {
                    SearchResult r = resultById.get(e.getKey());
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

    private List<SearchResult> fuseEqualWeight(List<List<SearchResult>> lists, int limit) {
        double weight = 1.0 / lists.size();
        Map<String, Double> scores = new LinkedHashMap<>();
        Map<String, SearchResult> resultById = new HashMap<>();

        for (List<SearchResult> list : lists) {
            int n = list.size();
            for (int i = 0; i < n; i++) {
                SearchResult r = list.get(i);
                scores.merge(r.id(), weight * (n - i), Double::sum);
                resultById.putIfAbsent(r.id(), r);
            }
        }

        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(limit)
                .map(e -> {
                    SearchResult r = resultById.get(e.getKey());
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

    static boolean containsCamelCase(String query) {
        if (query == null || query.isBlank()) return false;
        for (String token : query.split("\\s+")) {
            if (token.matches("[A-Z][a-zA-Z0-9]*[a-z][a-zA-Z0-9]*")) return true;
            if (token.matches("[A-Z]{3,}")) return true;
        }
        return false;
    }
}
