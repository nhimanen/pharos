package com.pharos.search;

/**
 * A single search result with all stored field values and relevance score.
 */
public record SearchResult(
        String id,
        String project,
        String packageName,
        String className,
        String qualifiedClassName,
        String methodName,
        String signature,
        String returnType,
        String body,
        String javadoc,
        String accessModifier,
        String filePath,
        int startLine,
        int endLine,
        float score,
        String searchType,  // "keyword", "vector", "hybrid", "related", "exact", "graph"
        String docType      // "method" | "class"
) {
    /**
     * Short display label.
     * For methods: "com.example.MyClass#myMethod"
     * For classes: "com.example.MyClass"
     * For document chunks: "docs.README § Installation"
     */
    public String label() {
        if ("class".equals(docType) || "document".equals(docType)) return qualifiedClassName;
        if ("chunk".equals(docType)) return qualifiedClassName + " \u00a7 " + methodName;
        return qualifiedClassName + "#" + methodName;
    }
}
