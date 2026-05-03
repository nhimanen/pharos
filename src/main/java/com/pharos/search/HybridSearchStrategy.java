package com.pharos.search;

import com.pharos.embedding.EmbeddingProvider;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.*;

/**
 * Hybrid search combining BM25 keyword search and KNN vector search
 * using Reciprocal Rank Fusion (RRF, k=60).
 *
 * RRF formula: score(doc) = Σ 1/(k + rank_in_list)
 * where k=60 is the standard constant that dampens the impact of high ranks.
 *
 * Degrades gracefully to keyword-only when embeddings are unavailable.
 */
public class HybridSearchStrategy {

    private static final int RRF_K = 60;

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

        return fuse(keywordResults, vectorResults, req.limit());
    }

    /**
     * Applies Reciprocal Rank Fusion to two ranked result lists.
     * Exposed for use by {@link SearchEngine#searchWithTrace} to measure fusion independently.
     */
    List<SearchResult> fuse(List<SearchResult> keywordResults,
                             List<SearchResult> vectorResults, int limit) {
        Map<String, Double> rrfScores = new LinkedHashMap<>();
        Map<String, SearchResult> resultById = new HashMap<>();

        for (int i = 0; i < keywordResults.size(); i++) {
            SearchResult r = keywordResults.get(i);
            rrfScores.merge(r.id(), 1.0 / (RRF_K + i + 1), Double::sum);
            resultById.put(r.id(), r);
        }
        for (int i = 0; i < vectorResults.size(); i++) {
            SearchResult r = vectorResults.get(i);
            rrfScores.merge(r.id(), 1.0 / (RRF_K + i + 1), Double::sum);
            resultById.putIfAbsent(r.id(), r);
        }

        return rrfScores.entrySet().stream()
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
}
