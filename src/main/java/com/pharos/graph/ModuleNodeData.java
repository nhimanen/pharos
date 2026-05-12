package com.pharos.graph;

/**
 * Immutable snapshot of a module node read from the ArcadeDB module graph.
 * Replaces the mutable {@link ModuleNode} JGraphT vertex.
 */
public record ModuleNodeData(
        String moduleKey,     // "groupId:artifactId"
        String groupId,
        String artifactId,
        String version,
        String status,        // "INDEXED" | "EXTERNAL"
        String projectName    // non-null when status == "INDEXED"
) {
    public boolean isIndexed() {
        return "INDEXED".equals(status);
    }
}
