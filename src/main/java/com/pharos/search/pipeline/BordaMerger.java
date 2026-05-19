package com.pharos.search.pipeline;

import com.pharos.search.QueryClassification;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;

import java.util.*;

/**
 * Fuses multiple ranked result lists using Borda count with agreement bonus.
 *
 * <h3>Dynamic weights (when a {@link QueryClassification} is present)</h3>
 * The KW/vector weight pair is chosen from a continuous gradient keyed on the
 * detected intent, rather than the legacy binary CamelCase switch:
 *
 * <pre>
 *   KEYWORD            0.85 / 0.15  — single identifier, exact match
 *   KEYWORD_TECHNICAL  0.75 / 0.25  — multi-word tech phrase (Cat B)
 *   CONFIG             0.70 / 0.30  — config setter / tuning
 *   LIFECYCLE          0.65 / 0.35  — error/lifecycle class names
 *   JAVADOC            0.60 / 0.40  — doc-style description
 *   HYBRID             0.45 / 0.55  — NL with stop words
 *   INTERFACE          0.35 / 0.65  — abstract type queries (Cat G)
 *   BEHAVIORAL         0.30 / 0.70  — behavioral intent (Cat D)
 * </pre>
 *
 * <h3>Legacy fallback (no classification)</h3>
 * CamelCase → 0.8/0.2; otherwise 0.5/0.5.
 *
 * <p>Documents present in both lists receive a 1.5× agreement bonus.
 * For N > 2 lists, equal weights are used and the bonus is not applied.
 */
public final class BordaMerger implements MergeStage {

    private static final double AGREEMENT_BONUS = 1.5;

    // Legacy fallback weights (used when no QueryClassification is available)
    private static final double KW_WEIGHT_NAME = 0.8;
    private static final double VEC_WEIGHT_NAME = 0.2;
    private static final double KW_WEIGHT_NL   = 0.5;
    private static final double VEC_WEIGHT_NL   = 0.5;

    @Override
    public List<SearchResult> merge(List<List<SearchResult>> candidates,
                                    SearchRequest req, SearchTrace trace) {
        long t = System.currentTimeMillis();
        List<SearchResult> result = candidates.size() == 2
                ? fuseTwoLists(candidates.get(0), candidates.get(1), req)
                : fuseEqualWeight(candidates, req.limit());
        if (trace != null) trace.record("borda merge", t);
        return result;
    }

    private List<SearchResult> fuseTwoLists(List<SearchResult> kwResults,
                                             List<SearchResult> vecResults,
                                             SearchRequest req) {
        double[] weights = resolveWeights(req);
        double kwWeight  = weights[0];
        double vecWeight = weights[1];

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

        int limit = req.limit();
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

    /**
     * Resolves the (kwWeight, vecWeight) pair from the request's classification.
     * Falls back to the legacy CamelCase heuristic when no classification is present.
     */
    /**
     * Resolves (kwWeight, vecWeight) from the query's intent classification.
     *
     * <p>Weights are calibrated by grid search over cached keyword/vector results
     * (eval/weight_search.py), measuring MRR@10 improvement vs the 0.5/0.5 baseline:
     * <pre>
     *   KEYWORD_TECHNICAL  0.9 / 0.1  — multi-word tech phrases, BM25 dominant (+0.065)
     *   HYBRID             0.9 / 0.1  — NL with stop words, BM25 still primary  (+0.032)
     *   CONFIG             0.8 / 0.2  — config setter names, exact BM25 match   (+0.021)
     *   BEHAVIORAL         0.7 / 0.3  — "find code that…", BM25 still needed    (+0.022)
     *   INTERFACE          0.6 / 0.4  — abstract types                           (~0)
     *   JAVADOC/LIFECYCLE  0.5 / 0.5  — balanced, no gain from change
     *   KEYWORD (fallback) 0.8 / 0.2  — CamelCase single-token, keyword dominant
     * </pre>
     */
    private static double[] resolveWeights(SearchRequest req) {
        QueryClassification c = req.classification();
        if (c != null && c.intent() != null) {
            return switch (c.intent()) {
                case "KEYWORD_TECHNICAL"        -> new double[]{0.90, 0.10};
                case "HYBRID"                   -> new double[]{0.90, 0.10};
                case "CONFIG"                   -> new double[]{0.80, 0.20};
                case "BEHAVIORAL"               -> new double[]{0.70, 0.30};
                case "INTERFACE"                -> new double[]{0.60, 0.40};
                case "JAVADOC", "LIFECYCLE",
                     "KEYWORD"                  -> new double[]{0.50, 0.50};
                default                         -> new double[]{0.50, 0.50};
            };
        }
        // Fallback: CamelCase heuristic (used when no router has classified the query)
        boolean nameLookup = containsCamelCase(req.query());
        return nameLookup
                ? new double[]{KW_WEIGHT_NAME, VEC_WEIGHT_NAME}
                : new double[]{KW_WEIGHT_NL,   VEC_WEIGHT_NL};
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
