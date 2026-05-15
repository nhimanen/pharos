package com.pharos.cli;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.ProjectIndexManager;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Removes all data for a project: Lucene index, call graph, file-state,
 * module-graph entry, cross-project graph, and registry entry.
 *
 * <p>With {@code --all}, removes every indexed project in the registry.
 */
@Command(
        name = "remove-index",
        description = "Remove a project's index and all associated data",
        mixinStandardHelpOptions = true
)
public class RemoveIndexCommand implements Callable<Integer> {

    @Parameters(index = "0", arity = "0..1",
            description = "Name of the project to remove (omit when --all is set)")
    private String projectName;

    @Option(names = "--all",
            description = "Remove every indexed project")
    private boolean all;

    private final ProjectRegistry registry;
    private final ProjectIndexManager indexManager;

    public RemoveIndexCommand(ProjectRegistry registry, ProjectIndexManager indexManager) {
        this.registry = registry;
        this.indexManager = indexManager;
    }

    @Override
    public Integer call() throws Exception {
        if (all && projectName != null) {
            System.err.println("Specify either a project name or --all, not both.");
            return 2;
        }
        if (!all && projectName == null) {
            System.err.println("Missing project name. Pass a project or use --all.");
            return 2;
        }

        if (all) return removeAll();
        return removeOne(projectName);
    }

    private int removeOne(String name) throws Exception {
        if (registry.find(name).isEmpty()) {
            System.err.println("Project not found: " + name);
            return 1;
        }
        indexManager.removeProject(name);
        System.out.println("Removed project '" + name + "'.");
        return 0;
    }

    private int removeAll() throws Exception {
        // Snapshot the project list — removeProject mutates the registry mid-iteration.
        List<ProjectMeta> all = registry.listAll();
        if (all.isEmpty()) {
            System.out.println("No projects to remove.");
            return 0;
        }
        int removed = 0;
        int failed  = 0;
        for (ProjectMeta meta : all) {
            try {
                indexManager.removeProject(meta.getName());
                System.out.println("Removed project '" + meta.getName() + "'.");
                removed++;
            } catch (Exception e) {
                System.err.println("Failed to remove '" + meta.getName() + "': " + e.getMessage());
                failed++;
            }
        }
        System.out.printf("Done: %d removed%s.%n",
                removed, failed > 0 ? ", " + failed + " failed" : "");
        return failed > 0 ? 1 : 0;
    }
}
