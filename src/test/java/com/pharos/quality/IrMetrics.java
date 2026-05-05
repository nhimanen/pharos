package com.pharos.quality;

import com.pharos.search.SearchResult;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Standard information-retrieval ranking metrics for evaluating search quality.
 *
 * <p>All metric methods accept a {@link RelevanceMap} that maps
 * {@code (className, methodName)} pairs to graded relevance scores:
 * <ul>
 *   <li>0 = irrelevant
 *   <li>1 = partially relevant (related concept or class)
 *   <li>2 = highly relevant (primary / intended answer)
 * </ul>
 *
 * Binary relevance threshold: grade &ge; 1 counts as "relevant" for MRR, P@k, R@k, AP.
 * NDCG uses the full grade scale (0/1/2) for richer rank discrimination.
 */
public final class IrMetrics {

    private IrMetrics() {}

    // ── Relevance representation ──────────────────────────────────────────────

    /**
     * A single relevance judgment: how relevant a specific method is to a query.
     * Use the static factories {@link #high} and {@link #partial} for brevity.
     */
    public record Judgment(String className, String methodName, int grade) {

        /** Grade 2: the primary / intended answer for this query. */
        public static Judgment high(String className, String methodName) {
            return new Judgment(className, methodName, 2);
        }

        /** Grade 1: related — same class, same concept, or useful supporting answer. */
        public static Judgment partial(String className, String methodName) {
            return new Judgment(className, methodName, 1);
        }
    }

    /**
     * Ground-truth relevance map built from a list of {@link Judgment}s.
     * Matching is by (className, methodName) — stable across FQN variations.
     */
    public static final class RelevanceMap {

        private final Map<String, Integer> grades;

        public RelevanceMap(List<Judgment> judgments) {
            this.grades = new HashMap<>();
            for (Judgment j : judgments) {
                grades.put(key(j.className(), j.methodName()), j.grade());
            }
        }

        /** Returns the relevance grade for a search result (0 if not in the map).
         *
         * <p>Matching strategy:
         * <ol>
         *   <li>Exact (className + methodName) — used by quality-corpus tests
         *   <li>Class-only fallback (judgment has empty methodName) — used by
         *       Lucene corpus tests where any method in the target class counts
         * </ol>
         */
        public int grade(SearchResult result) {
            if (result.className() == null) return 0;
            String mn = result.methodName() != null ? result.methodName() : "";
            // 1. Exact match on (className, methodName)
            int exact = grades.getOrDefault(key(result.className(), mn), 0);
            if (exact > 0) return exact;
            // 2. Class-level match: judgment has empty methodName → any method in that class counts
            return grades.getOrDefault(key(result.className(), ""), 0);
        }

        /** Number of judgments with grade &ge; 1. */
        public long totalRelevant() {
            return grades.values().stream().filter(g -> g >= 1).count();
        }

        /**
         * All non-zero grades sorted descending — used for ideal-DCG computation.
         */
        public List<Integer> sortedGrades() {
            return grades.values().stream()
                    .filter(g -> g > 0)
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
        }

        private static String key(String cls, String method) {
            return cls + "#" + method;
        }
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    /**
     * Mean Reciprocal Rank: {@code 1 / rank} of the first result with grade &ge; 1.
     * Returns 0.0 if no relevant result appears in the list.
     */
    public static double mrr(List<SearchResult> results, RelevanceMap relevance) {
        for (int i = 0; i < results.size(); i++) {
            if (relevance.grade(results.get(i)) >= 1) {
                return 1.0 / (i + 1);
            }
        }
        return 0.0;
    }

    /**
     * Precision@k: fraction of the top-k results with grade &ge; 1.
     * Returns 0.0 if {@code k == 0}.
     */
    public static double precisionAt(List<SearchResult> results, int k, RelevanceMap relevance) {
        if (k <= 0) return 0.0;
        int relevant = 0;
        for (int i = 0; i < Math.min(k, results.size()); i++) {
            if (relevance.grade(results.get(i)) >= 1) relevant++;
        }
        return (double) relevant / k;
    }

    /**
     * Recall@k: fraction of all relevant items found in the top-k results.
     * Returns 1.0 when no relevant items exist (vacuously true).
     */
    public static double recallAt(List<SearchResult> results, int k, RelevanceMap relevance) {
        long total = relevance.totalRelevant();
        if (total == 0) return 1.0;
        long found = 0;
        for (int i = 0; i < Math.min(k, results.size()); i++) {
            if (relevance.grade(results.get(i)) >= 1) found++;
        }
        return (double) found / total;
    }

    /**
     * NDCG@k: Normalized Discounted Cumulative Gain using graded relevance.
     * Grade 2 contributes 3 gain units (2²−1), grade 1 contributes 1 unit.
     * Returns 0.0 when no relevant items exist.
     */
    public static double ndcgAt(List<SearchResult> results, int k, RelevanceMap relevance) {
        double dcg  = computeDcg(results, k, relevance);
        double idcg = computeIdealDcg(relevance.sortedGrades(), k);
        return idcg == 0.0 ? 0.0 : dcg / idcg;
    }

    /**
     * Average Precision: the mean of P@i computed at each rank {@code i} where
     * a relevant result appears. Equivalent to the area under the precision-recall curve.
     * Returns 0.0 when no relevant items exist.
     */
    public static double averagePrecision(List<SearchResult> results, RelevanceMap relevance) {
        long total = relevance.totalRelevant();
        if (total == 0) return 0.0;
        double ap    = 0.0;
        int    found = 0;
        for (int i = 0; i < results.size(); i++) {
            if (relevance.grade(results.get(i)) >= 1) {
                found++;
                ap += (double) found / (i + 1);
            }
        }
        return ap / total;
    }

    // ── Aggregators ───────────────────────────────────────────────────────────

    /** Returns the arithmetic mean of a collection of metric values. */
    public static double mean(Collection<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    // ── DCG helpers ───────────────────────────────────────────────────────────

    private static double computeDcg(List<SearchResult> results, int k, RelevanceMap relevance) {
        double dcg = 0.0;
        for (int i = 0; i < Math.min(k, results.size()); i++) {
            double g = relevance.grade(results.get(i));
            dcg += (Math.pow(2, g) - 1) / (Math.log(i + 2) / Math.log(2));
        }
        return dcg;
    }

    private static double computeIdealDcg(List<Integer> sortedGrades, int k) {
        double idcg = 0.0;
        for (int i = 0; i < Math.min(k, sortedGrades.size()); i++) {
            double g = sortedGrades.get(i);
            idcg += (Math.pow(2, g) - 1) / (Math.log(i + 2) / Math.log(2));
        }
        return idcg;
    }
}
