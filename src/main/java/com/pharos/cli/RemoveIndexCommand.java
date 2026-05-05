package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNode;
import com.pharos.indexer.LuceneIndexer;
import picocli.CommandLine.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Removes all data for a project: Lucene index, call graph, file-state,
 * module-graph entry, cross-project graph, and registry entry.
 */
@Command(
        name = "remove-index",
        description = "Remove a project's index and all associated data",
        mixinStandardHelpOptions = true
)
public class RemoveIndexCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Name of the project to remove")
    private String projectName;

    private final ProjectRegistry registry;
    private final LuceneIndexer luceneIndexer;
    private final ModuleGraphBuilder moduleGraphBuilder;

    public RemoveIndexCommand(ProjectRegistry registry, LuceneIndexer luceneIndexer,
                              ModuleGraphBuilder moduleGraphBuilder) {
        this.registry = registry;
        this.luceneIndexer = luceneIndexer;
        this.moduleGraphBuilder = moduleGraphBuilder;
    }

    @Override
    public Integer call() throws Exception {
        Optional<ProjectMeta> meta = registry.find(projectName);
        if (meta.isEmpty()) {
            System.err.println("Project not found: " + projectName);
            return 1;
        }

        // 1. Downgrade module-graph node from INDEXED → EXTERNAL (preserves dep edges)
        try {
            ModuleGraph moduleGraph = moduleGraphBuilder.load();
            moduleGraph.findByProjectName(projectName).ifPresent(ModuleNode::downgrade);
            moduleGraphBuilder.save(moduleGraph);
        } catch (IOException e) {
            System.err.println("Warning: could not update module graph: " + e.getMessage());
        }

        // 2. Delete cross-project graph (stale after removal; rebuilt on next link)
        Path crossProjectGraph = IndexConfig.DEFAULT_BASE.resolve("cross-project-graph.graphml");
        Files.deleteIfExists(crossProjectGraph);

        // 3. Close and delete Lucene index
        luceneIndexer.deleteIndex(projectName);

        // 4. Delete the entire project index directory (graph.graphml, file-state.json, etc.)
        Path projectDir = luceneIndexer.getProjectIndexDir(projectName);
        if (Files.exists(projectDir)) {
            try (var stream = Files.walk(projectDir)) {
                stream.sorted(Comparator.reverseOrder())
                      .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
            }
        }

        // 5. Clean up linkedProjects references in other registry entries
        registry.unlinkAll(projectName);

        // 6. Remove from registry
        registry.unregister(projectName);

        System.out.println("Removed project '" + projectName + "'.");
        return 0;
    }
}
