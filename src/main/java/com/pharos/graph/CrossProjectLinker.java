package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Links call graphs across projects by resolving previously-unresolved call references.
 *
 * When project A calls a method from project B (an external dependency),
 * JavaParser cannot resolve the type during A's indexing pass.
 * This linker runs after both projects are indexed and linked via `pharos link`,
 * matching unresolved calls in A against known packages in B.
 */
public class CrossProjectLinker {

    private static final Logger log = LoggerFactory.getLogger(CrossProjectLinker.class);

    private final IndexConfig config;
    private final ProjectRegistry registry;
    private final CallGraphSerializer serializer;

    public CrossProjectLinker(IndexConfig config, ProjectRegistry registry) {
        this.config = config;
        this.registry = registry;
        this.serializer = new CallGraphSerializer();
    }

    /**
     * Build a merged cross-project call graph by combining graphs from
     * all mutually-linked projects and resolving cross-project call edges.
     */
    public CallGraph buildCrossProjectGraph(String project1, String project2) throws IOException {
        ProjectMeta meta1 = registry.find(project1)
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + project1));
        ProjectMeta meta2 = registry.find(project2)
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + project2));

        // Load individual graphs
        Path graphPath1 = Path.of(meta1.getIndexPath()).resolve("graph.graphml");
        Path graphPath2 = Path.of(meta2.getIndexPath()).resolve("graph.graphml");

        CallGraph merged = new CallGraph();
        mergeInto(merged, serializer.load(graphPath1));
        mergeInto(merged, serializer.load(graphPath2));

        // Resolve cross-project edges: unresolved refs in project1 that match packages in project2
        int resolved = 0;
        for (ProjectMeta.UnresolvedRef ref : meta1.getUnresolvedRefs()) {
            String candidate = findInProject(ref.calleeMethodName, meta2);
            if (candidate != null) {
                merged.addCall(ref.callerFqn, candidate);
                resolved++;
                log.debug("Cross-project link: {} → {}", ref.callerFqn, candidate);
            }
        }

        // Also resolve refs in project2 calling project1
        for (ProjectMeta.UnresolvedRef ref : meta2.getUnresolvedRefs()) {
            String candidate = findInProject(ref.calleeMethodName, meta1);
            if (candidate != null) {
                merged.addCall(ref.callerFqn, candidate);
                resolved++;
            }
        }

        log.info("Cross-project linking: {} new edges between '{}' and '{}'",
                resolved, project1, project2);

        // Persist the merged graph
        Path crossGraphPath = IndexConfig.DEFAULT_BASE.resolve("cross-project-graph.graphml");
        serializer.save(merged, crossGraphPath);

        return merged;
    }

    /**
     * Loads the cross-project graph (previously built via link command).
     * Falls back to an empty graph if not yet built.
     */
    public CallGraph loadCrossProjectGraph() throws IOException {
        Path crossGraphPath = IndexConfig.DEFAULT_BASE.resolve("cross-project-graph.graphml");
        if (!Files.exists(crossGraphPath)) {
            return new CallGraph();
        }
        return serializer.load(crossGraphPath);
    }

    /** Find a method in the given project's graph whose simple name matches. */
    private String findInProject(String methodName, ProjectMeta targetMeta) {
        // Heuristic: match any method in the target project whose FQN ends with "#methodName(...)"
        Path graphPath = Path.of(targetMeta.getIndexPath()).resolve("graph.graphml");
        if (!Files.exists(graphPath)) return null;

        try {
            CallGraph graph = serializer.load(graphPath);
            return graph.allMethods().stream()
                    .filter(fqn -> {
                        int hash = fqn.indexOf('#');
                        if (hash < 0) return false;
                        String methodPart = fqn.substring(hash + 1);
                        int paren = methodPart.indexOf('(');
                        String simpleName = paren > 0 ? methodPart.substring(0, paren) : methodPart;
                        return simpleName.equals(methodName);
                    })
                    .findFirst()
                    .orElse(null);
        } catch (IOException e) {
            log.debug("Could not load call graph for project '{}': {}",
                    targetMeta.getName(), e.getMessage());
            return null;
        }
    }

    private void mergeInto(CallGraph target, CallGraph source) {
        source.allMethods().forEach(target::addMethod);
        source.getInternalGraph().edgeSet().forEach(edge -> {
            String src = source.getInternalGraph().getEdgeSource(edge);
            String tgt = source.getInternalGraph().getEdgeTarget(edge);
            target.addCall(src, tgt);
        });
    }
}
