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
        return buildCrossProjectGraph(List.of(project1, project2));
    }

    /**
     * Build a merged cross-project call graph across all given projects.
     * Resolves unresolved refs in each project against all other projects.
     * All projects must already be registered and indexed.
     *
     * @param projectNames names of projects to link (must be ≥ 2)
     */
    public CallGraph buildCrossProjectGraph(List<String> projectNames) throws IOException {
        if (projectNames.size() < 2) {
            throw new IllegalArgumentException("Need at least 2 projects to link");
        }

        List<ProjectMeta> metas = new ArrayList<>();
        CallGraph merged = new CallGraph();

        for (String name : projectNames) {
            ProjectMeta meta = registry.find(name)
                    .orElseThrow(() -> new IllegalArgumentException("Project not found: " + name));
            metas.add(meta);
            Path graphPath = Path.of(meta.getIndexPath()).resolve("graph.graphml");
            if (Files.exists(graphPath)) {
                mergeInto(merged, serializer.load(graphPath));
            }
        }

        // Resolve unresolved refs in each project against every other project
        int resolved = 0;
        for (int i = 0; i < metas.size(); i++) {
            for (int j = 0; j < metas.size(); j++) {
                if (i == j) continue;
                for (ProjectMeta.UnresolvedRef ref : metas.get(i).getUnresolvedRefs()) {
                    String candidate = findInProject(ref.calleeMethodName, metas.get(j));
                    if (candidate != null) {
                        merged.addCall(ref.callerFqn, candidate);
                        resolved++;
                        log.debug("Cross-project link: {} → {}", ref.callerFqn, candidate);
                    }
                }
            }
        }

        log.info("Cross-project linking: {} new edges across {} projects", resolved, projectNames.size());

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
