package com.pharos.search.pipeline;

import com.pharos.search.SearchResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Equal-weight reciprocal-rank fusion of a first-stage ranking with cross-encoder scores.
 *
 * <p>Cross-encoder probabilities saturate on code (most candidates land near 0 or 1), so they are
 * used only as a <em>rank</em>: {@code score = 0.5/(K+firstRank) + 0.5/(K+ceRank)} with 1-based
 * ranks. Ties are broken by cross-encoder rank, then first-stage rank, so the order is
 * deterministic, and the returned scores are non-increasing in the returned order.
 */
final class RrfFusion {

    static final int K = 60;

    private RrfFusion() {}

    /**
     * @param firstStage candidates in first-stage order (best first)
     * @param ceScores   cross-encoder score per candidate, aligned with {@code firstStage}
     * @param searchType searchType to stamp on results, or null to keep each result's own
     * @return all candidates sorted by fused score, descending
     */
    static List<SearchResult> fuse(List<SearchResult> firstStage, float[] ceScores, String searchType) {
        int n = firstStage.size();
        Integer[] byCe = new Integer[n];
        for (int i = 0; i < n; i++) byCe[i] = i;
        java.util.Arrays.sort(byCe, (x, y) -> {
            int c = Float.compare(ceScores[y], ceScores[x]);
            return c != 0 ? c : Integer.compare(x, y);
        });
        int[] ceRank = new int[n];
        for (int r = 0; r < n; r++) ceRank[byCe[r]] = r + 1;

        record Fused(int idx, double score) {}
        List<Fused> fused = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            fused.add(new Fused(i, 0.5 / (K + i + 1) + 0.5 / (K + ceRank[i])));
        }
        fused.sort((a, b) -> {
            int c = Double.compare(b.score(), a.score());
            if (c != 0) return c;
            c = Integer.compare(ceRank[a.idx()], ceRank[b.idx()]);
            return c != 0 ? c : Integer.compare(a.idx(), b.idx());
        });

        List<SearchResult> out = new ArrayList<>(n);
        for (Fused f : fused) {
            SearchResult r = firstStage.get(f.idx());
            out.add(new SearchResult(
                    r.id(), r.project(), r.packageName(), r.className(),
                    r.qualifiedClassName(), r.methodName(), r.signature(),
                    r.returnType(), r.body(), r.javadoc(), r.accessModifier(),
                    r.filePath(), r.startLine(), r.endLine(),
                    (float) f.score(), searchType != null ? searchType : r.searchType(), r.docType()));
        }
        return out;
    }
}
