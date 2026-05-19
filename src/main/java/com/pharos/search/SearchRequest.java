package com.pharos.search;

import java.util.List;

/**
 * Parameters for a code search query, including an optional {@link QueryClassification}
 * populated by the {@link QueryRouter} at the start of pipeline execution.
 */
public record SearchRequest(
        String query,
        SearchType type,
        String project,              // null = search all projects
        List<String> projects,       // explicit list of projects (null = all from registry)
        int limit,
        String outputFormat,         // "text" or "json"
        String docType,              // null = all, "method", "class", "chunk"
        String scope,                // null = all, "prod", "test", "docs"
        int oversampleFactor,        // 0 = use pipeline default; >0 overrides retriever fetch multiplier
        QueryClassification classification  // null until a QueryRouter runs; read-only after that
) {
    /** Backward-compatible constructor without classification (most callers use this). */
    public SearchRequest(String query, SearchType type, String project, List<String> projects,
                         int limit, String outputFormat, String docType, String scope,
                         int oversampleFactor) {
        this(query, type, project, projects, limit, outputFormat, docType, scope, oversampleFactor, null);
    }

    /**
     * Returns a copy with the given classification attached.
     * If the classification carries a docType and none was set on this request,
     * the classification's docType is applied (intent-driven filter injection).
     */
    public SearchRequest withClassification(QueryClassification c) {
        String effectiveDocType = (docType == null && c.docType() != null) ? c.docType() : docType;
        return new SearchRequest(query, type, project, projects, limit, outputFormat,
                effectiveDocType, scope, oversampleFactor, c);
    }

    public enum SearchType {
        /** Automatically select KEYWORD or HYBRID based on query shape — dispatched by {@link QueryRouter}. */
        AUTO,
        /** Single BM25∪KNN pass with late-interaction vector reranking and adaptive BM25/vector weights. */
        UNIFIED,
        KEYWORD, VECTOR, HYBRID,
        /** BordaMerge followed by cross-encoder reranking. Degrades to HYBRID when no encoder is configured. */
        HYBRID_RERANKED,
        /** Cross-encoder scores all candidates and acts as the merge step. */
        HYBRID_CROSS_ENCODER_MERGE,
        /** Borda-merge followed by doc-type diversity reranking. Always available. */
        HYBRID_DIVERSE,
        /** Borda-merge → cross-encoder reranking → doc-type diversity reranking. */
        HYBRID_RERANKED_DIVERSE;

        public static SearchType from(String s) {
            return switch (s.toLowerCase()) {
                case "unified"                                        -> UNIFIED;
                case "auto"                                           -> AUTO;
                case "keyword"                                        -> KEYWORD;
                case "vector"                                         -> VECTOR;
                case "hybrid"                                         -> HYBRID;
                case "hybrid-reranked", "hybrid_reranked"             -> HYBRID_RERANKED;
                case "hybrid-ce-merge",
                     "hybrid_cross_encoder_merge"                     -> HYBRID_CROSS_ENCODER_MERGE;
                case "hybrid-diverse", "hybrid_diverse"               -> HYBRID_DIVERSE;
                case "hybrid-reranked-diverse",
                     "hybrid_reranked_diverse"                        -> HYBRID_RERANKED_DIVERSE;
                default -> AUTO;
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
