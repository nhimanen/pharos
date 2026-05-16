package com.pharos.search;

import java.util.List;

/**
 * Parameters for a code search query.
 */
public record SearchRequest(
        String query,
        SearchType type,
        String project,        // null = search all projects
        List<String> projects, // explicit list of projects (null = all from registry)
        int limit,
        String outputFormat,   // "text" or "json"
        String docType,        // null = all, "method", "class", "chunk"
        String scope,          // null = all, "prod", "test", "docs"
        int oversampleFactor   // 0 = use pipeline default; >0 overrides retriever fetch multiplier
) {
    public enum SearchType {
        KEYWORD, VECTOR, HYBRID,
        /** BordaMerge followed by cross-encoder reranking. Degrades to HYBRID when no encoder is configured. */
        HYBRID_RERANKED,
        /** Cross-encoder scores all candidates and acts as the merge step. Degrades to deduplicated pool when no encoder is configured. */
        HYBRID_CROSS_ENCODER_MERGE,
        /** Borda-merge followed by doc-type diversity reranking. Always available. */
        HYBRID_DIVERSE,
        /** Borda-merge → cross-encoder reranking → doc-type diversity reranking. CE step degrades gracefully when no encoder is configured. */
        HYBRID_RERANKED_DIVERSE;

        public static SearchType from(String s) {
            return switch (s.toLowerCase()) {
                case "keyword"                                        -> KEYWORD;
                case "vector"                                         -> VECTOR;
                case "hybrid"                                         -> HYBRID;
                case "hybrid-reranked", "hybrid_reranked"             -> HYBRID_RERANKED;
                case "hybrid-ce-merge",
                     "hybrid_cross_encoder_merge"                     -> HYBRID_CROSS_ENCODER_MERGE;
                case "hybrid-diverse", "hybrid_diverse"               -> HYBRID_DIVERSE;
                case "hybrid-reranked-diverse",
                     "hybrid_reranked_diverse"                        -> HYBRID_RERANKED_DIVERSE;
                default -> HYBRID;
            };
        }
    }

    public static SearchRequest keyword(String query, String project, int limit) {
        return new SearchRequest(query, SearchType.KEYWORD, project, null, limit, "text", null, null, 0);
    }

    public static SearchRequest hybrid(String query, String project, int limit) {
        return new SearchRequest(query, SearchType.HYBRID, project, null, limit, "text", null, null, 0);
    }
}
