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
        String scope           // null = all, "prod", "test", "docs"
) {
    public enum SearchType {
        KEYWORD, VECTOR, HYBRID;

        public static SearchType from(String s) {
            return switch (s.toLowerCase()) {
                case "keyword" -> KEYWORD;
                case "vector"  -> VECTOR;
                case "hybrid"  -> HYBRID;
                default -> HYBRID;
            };
        }
    }

    public static SearchRequest keyword(String query, String project, int limit) {
        return new SearchRequest(query, SearchType.KEYWORD, project, null, limit, "text", null, null);
    }

    public static SearchRequest hybrid(String query, String project, int limit) {
        return new SearchRequest(query, SearchType.HYBRID, project, null, limit, "text", null, null);
    }
}
