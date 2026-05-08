package com.pharos.graph;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.search.SearchEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Computes the boundary between a module and its external callers/callees.
 * Provides the drill-down link from the module graph to the code-level call graph.
 *
 * Entry points: methods in this project's call graph that are called from at least
 *   one other linked project — i.e., cross-project in-edges.
 *
 * Exit points: unresolved call references stored in ProjectMeta.unresolvedRefs.
 *   These are calls that left the project's type-resolution scope during indexing,
 *   indicating calls to external modules.
 */
public class ModuleBoundaryAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(ModuleBoundaryAnalyzer.class);

    private final ProjectRegistry registry;
    private final CallGraphSerializer serializer;

    public ModuleBoundaryAnalyzer(ProjectRegistry registry, SearchEngine searchEngine) {
        this.registry     = registry;
        this.serializer   = new CallGraphSerializer();
    }

    // ---------------------------------------------------------------------------
    // Public types
    // ---------------------------------------------------------------------------

    public record BoundaryResult(
            String projectName,
            List<String> entryPoints,    // FQNs in this module called by other linked modules
            List<ExitPoint> exitPoints   // unresolved calls leaving this module
    ) {}

    public record ExitPoint(
            String callerFqn,
            String calleeSimpleName,
            int line
    ) {}

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /**
     * Compute the boundary for {@code projectName}.
     * Entry points are found by loading all linked projects' call graphs and
     * looking for edges that target methods in this project.
     * Exit points come from the stored unresolvedRefs in ProjectMeta.
     */
    public BoundaryResult analyze(String projectName) throws Exception {
        ProjectMeta meta = registry.find(projectName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Project not found: " + projectName));

        // Load this project's method set
        Path graphFile = Path.of(meta.getIndexPath()).resolve("graph.graphml");
        CallGraph projectGraph = serializer.load(graphFile);
        Set<String> ownMethods = new HashSet<>(projectGraph.allMethods());

        // Entry points: scan linked projects for callers of our methods
        Set<String> entryPointSet = new LinkedHashSet<>();
        for (String linkedName : meta.getLinkedProjects()) {
            ProjectMeta linkedMeta = registry.find(linkedName).orElse(null);
            if (linkedMeta == null) continue;
            Path linkedGraphFile = Path.of(linkedMeta.getIndexPath()).resolve("graph.graphml");
            try {
                CallGraph linkedGraph = serializer.load(linkedGraphFile);
                for (String ownMethod : ownMethods) {
                    Set<String> callers = linkedGraph.getCallers(ownMethod);
                    if (!callers.isEmpty()) {
                        entryPointSet.add(ownMethod);
                    }
                }
            } catch (Exception e) {
                log.debug("Could not load call graph for linked project '{}': {}",
                        linkedName, e.getMessage());
            }
        }

        // Exit points: unresolved refs recorded during indexing
        List<ExitPoint> exitPoints = meta.getUnresolvedRefs().stream()
                .map(r -> new ExitPoint(r.callerFqn, r.calleeMethodName, r.line))
                .collect(Collectors.toList());

        return new BoundaryResult(
                projectName,
                entryPointSet.stream().sorted().collect(Collectors.toList()),
                exitPoints
        );
    }
}
