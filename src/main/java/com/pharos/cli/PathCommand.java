package com.pharos.cli;

import com.pharos.indexer.IncompatibleIndexException;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.GraphView;
import com.pharos.graph.GraphViews;
import com.pharos.graph.CrossProjectLinker;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "path",
        description = "Find the shortest call path between two method FQNs",
        mixinStandardHelpOptions = true
)
public class PathCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Source method FQN")
    private String fromFqn;

    @Parameters(index = "1", description = "Target method FQN")
    private String toFqn;

    private final ProjectRegistry registry;
    private final IndexConfig config;

    public PathCommand(ProjectRegistry registry, IndexConfig config) {
        this.registry = registry;
        this.config = config;
    }

    @Override
    public Integer call() {
        try {
            List<String> path = findPath();

            if (path.isEmpty()) {
                System.out.printf("No call path found from:%n  %s%nto:%n  %s%n", fromFqn, toFqn);
            } else {
                System.out.printf("Call path (%d hops):%n", path.size() - 1);
                for (int i = 0; i < path.size(); i++) {
                    String prefix = i == 0 ? "START" : i == path.size() - 1 ? "END  " : "  →  ";
                    System.out.printf("  %s  %s%n", prefix, path.get(i));
                }
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private List<String> findPath() throws Exception {
        // Search each per-project graph individually
        for (ProjectMeta meta : registry.listAll()) {
            // GraphViews checks the index format before opening anything
            try (GraphView graph = GraphViews.open(meta)) {
                List<String> path = graph.shortestPath(fromFqn, toFqn);
                if (!path.isEmpty()) return path;
            } catch (IncompatibleIndexException e) {
                System.err.println("Skipping '" + meta.getName() + "': " + e.getMessage());
            } catch (Exception e) {
                // Graph not available for this project
            }
        }
        // Fall back to cross-project graph
        CrossProjectLinker linker = new CrossProjectLinker(config, registry);
        try (CallGraph cross = linker.loadCrossProjectGraph()) {
            return cross.shortestPath(fromFqn, toFqn);
        } catch (Exception e) {
            return List.of();
        }
    }
}
