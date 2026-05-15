package com.pharos.search;

import com.pharos.embedding.EmbeddingProvider;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.*;

/**
 * Hybrid search combining BM25 keyword search and KNN vector search.
 *
 * @deprecated Use {@link com.pharos.search.pipeline.SearchPipeline} with
 *     {@link com.pharos.search.pipeline.BordaMerger} instead.
 *
 * <p>Fusion strategy: <b>Borda count with agreement bonus</b>.
 * Each result list contributes linear Borda points: rank 1 scores {@code listSize} points,
 * rank 2 scores {@code listSize - 1}, etc.  Documents that appear in <em>both</em> lists
 * receive an additional {@code AGREEMENT_BONUS} multiplier — rewarding consensus without
 * penalising documents that only one strategy finds.
 *
 * <pre>
 *   base_score(doc) = kwWeight * (n - rank_kw)   [if in keyword list]
 *                   + vecWeight * (n - rank_vec)  [if in vector list]
 *   final_score(doc) = base_score * AGREEMENT_BONUS  [if in both lists]
 *                    = base_score                    [otherwise]
 * </pre>
 *
 * <p>Weights are adaptive:
 * <ul>
 *   <li>CamelCase query ("BooleanQuery"): kw=0.8, vec=0.2 — exact name matches dominate.
 *   <li>Natural-language query: kw=0.5, vec=0.5 — equal weight for vocab-gap bridging.
 * </ul>
 *
 * <p>Degrades gracefully to keyword-only when embeddings are unavailable.
 */
@Deprecated
public class HybridSearchStrategy {

    /** Multiplier applied to docs present in both keyword and vector lists. */
    private static final double AGREEMENT_BONUS = 1.5;

    /** Borda keyword weight when query contains a CamelCase identifier token. */
    private static final double KW_WEIGHT_NAME  = 0.8;
    private static final double VEC_WEIGHT_NAME = 0.2;

    /** Borda weights for natural-language / conceptual queries. */
    private static final double KW_WEIGHT_NL    = 0.5;
    private static final double VEC_WEIGHT_NL   = 0.5;


    private final KeywordSearchStrategy keywordStrategy;
    private final VectorSearchStrategy vectorStrategy;
    private final EmbeddingProvider embedder;

    public HybridSearchStrategy(EmbeddingProvider embedder) {
        this.keywordStrategy = new KeywordSearchStrategy();
        this.vectorStrategy = new VectorSearchStrategy(embedder);
        this.embedder = embedder;
    }

    public List<SearchResult> search(IndexReader reader, SearchRequest req) throws IOException {
        List<SearchResult> keywordResults = keywordStrategy.search(reader, req);

        if (!embedder.isAvailable()) {
            // No embeddings — return keyword results directly
            return keywordResults;
        }

        List<SearchResult> vectorResults = vectorStrategy.search(reader, req);

        if (vectorResults.isEmpty()) {
            return keywordResults;
        }

        return fuse(keywordResults, vectorResults, req.limit(), req.query());
    }

    /**
     * Fuses two ranked result lists using Borda count with agreement bonus.
     *
     * <p>Each list contributes linear Borda points proportional to its weight.
     * Documents present in both lists receive an {@code AGREEMENT_BONUS} multiplier,
     * surfacing consensus results without penalising single-strategy finds.
     *
     * <p>When {@code query} contains a CamelCase token the keyword list is weighted
     * 0.8 vs 0.2 for vector, preserving exact name-match dominance.
     *
     * Exposed for use by {@link SearchEngine#searchWithTrace} to measure fusion independently.
     */
    public List<SearchResult> fuse(List<SearchResult> keywordResults,
                             List<SearchResult> vectorResults, int limit, String query) {
        boolean nameLookup = containsCamelCase(query);
        double kwWeight  = nameLookup ? KW_WEIGHT_NAME  : KW_WEIGHT_NL;
        double vecWeight = nameLookup ? VEC_WEIGHT_NAME : VEC_WEIGHT_NL;

        int kwN  = keywordResults.size();
        int vecN = vectorResults.size();

        Map<String, Double> scores = new LinkedHashMap<>();
        Map<String, SearchResult> resultById = new HashMap<>();
        Set<String> inKeyword = new HashSet<>();
        Set<String> inVector  = new HashSet<>();

        for (int i = 0; i < kwN; i++) {
            SearchResult r = keywordResults.get(i);
            scores.merge(r.id(), kwWeight * (kwN - i), Double::sum);
            resultById.put(r.id(), r);
            inKeyword.add(r.id());
        }
        for (int i = 0; i < vecN; i++) {
            SearchResult r = vectorResults.get(i);
            scores.merge(r.id(), vecWeight * (vecN - i), Double::sum);
            resultById.putIfAbsent(r.id(), r);
            inVector.add(r.id());
        }

        // Boost docs confirmed by both strategies
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

    /**
     * Multi-hash SimHash collision probability for a Lucene COSINE similarity score.
     *
     * <p>Lucene's COSINE similarity returns {@code (1 + cosine) / 2 ∈ [0,1]}.
     * The 1-bit SimHash probability is {@code p = 1 - arccos(cosine) / π}.
     * With {@code k} independent hash functions: {@code P_k = p^k}.
     *
     * @param luceneScore raw score from a {@code KnnFloatVectorQuery} result
     * @param k           number of hash functions (1 = raw p1bit; higher = steeper S-curve)
     */
    public static double collisionProbability(float luceneScore, int k) {
        double cosine = Math.max(-1.0, Math.min(1.0, 2.0 * luceneScore - 1.0));
        double p1bit  = 1.0 - Math.acos(cosine) / Math.PI;
        return k == 1 ? p1bit : Math.pow(p1bit, k);
    }

    /**
     * Returns true if the query contains at least one CamelCase identifier token
     * (e.g. "BooleanQuery", "HnswGraphBuilder", "IndexWriter").
     * Single-word all-caps (e.g. "HNSW") is also treated as a name token.
     */
    static boolean containsCamelCase(String query) {
        if (query == null || query.isBlank()) return false;
        for (String token : query.split("\\s+")) {
            if (token.matches("[A-Z][a-zA-Z0-9]*[a-z][a-zA-Z0-9]*")) return true;
            if (token.matches("[A-Z]{3,}")) return true;
        }
        return false;
    }

}
