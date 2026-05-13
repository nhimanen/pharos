package com.pharos.cli;

import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.ProjectIndexManager;
import picocli.CommandLine.*;

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
    private final ProjectIndexManager indexManager;

    public RemoveIndexCommand(ProjectRegistry registry, ProjectIndexManager indexManager) {
        this.registry = registry;
        this.indexManager = indexManager;
    }

    @Override
    public Integer call() throws Exception {
        if (registry.find(projectName).isEmpty()) {
            System.err.println("Project not found: " + projectName);
            return 1;
        }
        indexManager.removeProject(projectName);
        System.out.println("Removed project '" + projectName + "'.");
        return 0;
    }
}
