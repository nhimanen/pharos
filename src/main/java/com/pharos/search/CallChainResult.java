package com.pharos.search;

import java.util.List;

/**
 * Result of a multi-hop call-graph traversal produced by
 * {@link SearchEngine#traceCallChain}.
 *
 * {@link #nodes} is a flat list in BFS order (depth 0 first).
 * Each node carries its immediate {@link ChainNode#children} as a list of FQNs
 * so callers can reconstruct the tree without re-traversing the list.
 */
public record CallChainResult(
        String root,
        String direction,   // "callers" | "callees" | "both"
        int maxDepth,
        int totalNodes,
        boolean truncated,
        List<ChainNode> nodes
) {
    public record ChainNode(
            String fqn,
            String label,
            int depth,
            String signature,
            String filePath,
            int startLine,
            int endLine,
            String body,            // null if not indexed; may be truncated by maxBodyChars
            List<String> children   // FQNs of next-level nodes (direction-dependent)
    ) {}
}
