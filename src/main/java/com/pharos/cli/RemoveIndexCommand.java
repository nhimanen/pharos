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
 *
 * <p>With {@code --from-catalog}, removes only the project from the cluster's shard catalog
 * (object-store backend), in one write: every shard of a split project, its staged and retired versions
 * and an open reshard request with it. Work-queue writers release it and readers stop serving it. It deletes no
 * index data, locally or in the store, so a work-queue writer runs it too.
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

    @Option(names = "--from-catalog",
            description = "Cluster: only remove the project (every shard of a split project) from the shard catalog, so work-queue writers stop"
                    + " indexing it and readers stop serving it. Deletes no index data (locally or in the object"
                    + " store); registering the project again resumes from its last generation")
    private boolean fromCatalog;

    private final ProjectRegistry registry;
    private final ProjectIndexManager indexManager;
    private final IndexConfig config;
    /** The cluster runtime ({@code --from-catalog}); null when not wired. */
    private final com.pharos.cluster.ClusterRuntime runtime;

    public RemoveIndexCommand(ProjectRegistry registry, ProjectIndexManager indexManager, IndexConfig config) {
        this(registry, indexManager, config, null);
    }

    public RemoveIndexCommand(ProjectRegistry registry, ProjectIndexManager indexManager, IndexConfig config,
                              com.pharos.cluster.ClusterRuntime runtime) {
        this.registry = registry;
        this.indexManager = indexManager;
        this.config = config;
        this.runtime = runtime;
    }

    @Override
    public Integer call() throws Exception {
        if (fromCatalog) return removeFromCatalog();
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

    /** {@code --from-catalog}: 0 removed or not listed, 1 the store failed, 2 unusable options. */
    private int removeFromCatalog() {
        try {
            RoleGuard.requireIndexing(config); // readers never change the catalog
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            return 2;
        }
        if (all || projectName == null) {
            System.err.println("--from-catalog removes one project: pass its name (not --all).");
            return 2;
        }
        var catalog = runtime == null ? java.util.Optional.<com.pharos.cluster.objectstore.ObjectStoreCatalog>empty()
                : runtime.sourceCatalog();
        if (catalog.isEmpty()) {
            System.err.println("--from-catalog needs a cluster object-store backend (cluster.backend 'object-store');"
                    + " this node has none.");
            return 2;
        }
        if (!com.pharos.cluster.ShardId.isProjectName(projectName)) {
            System.err.println("Error: '" + projectName + "' is not a valid cluster project name");
            return 2;
        }
        try {
            // the whole project in one catalog write: a split project's shards and its versions too (I4)
            List<com.pharos.cluster.ShardId> removed = catalog.get().removeProject(projectName);
            if (!removed.isEmpty()) {
                System.out.println("Removed project '" + projectName + "' from the cluster catalog"
                        + (removed.size() == 1 ? "." : " (" + removed.size() + " shards)."));
            } else {
                System.out.println("Project '" + projectName + "' is not in the cluster catalog.");
            }
            return 0;
        } catch (java.io.IOException e) {
            System.err.println("Removing " + projectName + " from the cluster catalog failed: " + e.getMessage());
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
