package com.pharos.search;

/**
 * A single search result with all stored field values and relevance score.
 *
 * {@link #snippet} is null unless a {@link SnippetDecorator} (or other
 * {@link SearchResultDecorator}) has been applied after retrieval.
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
        String docType,     // "method" | "class"
        Snippet snippet     // null unless decorated by SnippetDecorator
) {
    /** Backward-compatible constructor — sets snippet to null. */
    public SearchResult(String id, String project, String packageName, String className,
                        String qualifiedClassName, String methodName, String signature,
                        String returnType, String body, String javadoc, String accessModifier,
                        String filePath, int startLine, int endLine, float score,
                        String searchType, String docType) {
        this(id, project, packageName, className, qualifiedClassName, methodName, signature,
                returnType, body, javadoc, accessModifier, filePath, startLine, endLine,
                score, searchType, docType, null);
    }

    /** Returns a copy of this result with the given snippet attached. */
    public SearchResult withSnippet(Snippet snippet) {
        return new SearchResult(id, project, packageName, className, qualifiedClassName,
                methodName, signature, returnType, body, javadoc, accessModifier,
                filePath, startLine, endLine, score, searchType, docType, snippet);
    }

    /**
     * Short display label.
     * For methods: "com.example.MyClass#myMethod"
     * For classes: "com.example.MyClass"
     * For document chunks: "docs.README § Installation"
     */
    public String label() {
        if ("class".equals(docType) || "document".equals(docType)) return qualifiedClassName;
        if ("chunk".equals(docType)) return qualifiedClassName + " § " + methodName;
        return qualifiedClassName + "#" + methodName;
    }
}
