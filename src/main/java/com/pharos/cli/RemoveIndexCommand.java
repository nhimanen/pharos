package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.cluster.RoleGuard;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.indexer.ProjectNames;
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
    private final IndexConfig config;

    public RemoveIndexCommand(ProjectRegistry registry, ProjectIndexManager indexManager, IndexConfig config) {
        this.registry = registry;
        this.indexManager = indexManager;
        this.config = config;
    }

    @Override
    public Integer call() throws Exception {
        try {
            RoleGuard.requireIndexing(config);
            RoleGuard.requireNoWorkQueue(config, "pharos remove-index");
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            return 2;
        }
        if (all && projectName != null) {
            System.err.println("Specify either a project name or --all, not both.");
            return 2;
        }
        if (!all && projectName == null) {
            System.err.println("Missing project name. Pass a project or use --all.");
            return 2;
        }

        try {
            if (all) return removeAll();
            return removeOne(projectName);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private int removeOne(String name) throws Exception {
        if (registry.find(name).isEmpty()) {
            System.err.println("Project not found: " + name);
            return 1;
        }
        if (!ProjectNames.isValid(name)) {
            // Its index dir may be the indexes root: unregister only, delete nothing on disk.
            indexManager.unregisterOnly(name);
            System.out.println("Unregistered invalid project name '" + name + "' (registry entry, link "
                    + "references and synonyms only). Nothing was deleted from disk; any leftover files "
                    + "in the index directory must be removed by hand (see UPGRADING.md).");
            return 0;
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
                if (!ProjectNames.isValid(meta.getName())) {
                    indexManager.unregisterOnly(meta.getName());
                    System.out.println("Unregistered invalid project name '" + meta.getName()
                            + "' (nothing deleted from disk).");
                    removed++;
                    continue;
                }
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
