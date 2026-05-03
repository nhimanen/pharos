package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CallGraphSerializer;
import com.pharos.graph.CrossProjectLinker;
import picocli.CommandLine.*;

import java.nio.file.Path;
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

    @Option(names = {"--cross-project"},
            description = "Include cross-project call edges (requires linked projects)")
    private boolean crossProject = false;

    private final ProjectRegistry registry;
    private final IndexConfig config;

    public PathCommand(ProjectRegistry registry, IndexConfig config) {
        this.registry = registry;
        this.config = config;
    }

    @Override
    public Integer call() {
        try {
            CallGraph graph = buildGraph();
            List<String> path = graph.findPath(fromFqn, toFqn);

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

    private CallGraph buildGraph() throws Exception {
        if (crossProject) {
            CrossProjectLinker linker = new CrossProjectLinker(config, registry);
            return linker.loadCrossProjectGraph();
        }

        // Build merged graph from all project graphs
        CallGraph merged = new CallGraph();
        CallGraphSerializer serializer = new CallGraphSerializer();
        for (ProjectMeta meta : registry.listAll()) {
            Path graphFile = Path.of(meta.getIndexPath()).resolve("graph.graphml");
            CallGraph projectGraph = serializer.load(graphFile);
            // Merge all edges
            projectGraph.allMethods().forEach(merged::addMethod);
            projectGraph.getInternalGraph().edgeSet().forEach(edge -> {
                String src = projectGraph.getInternalGraph().getEdgeSource(edge);
                String tgt = projectGraph.getInternalGraph().getEdgeTarget(edge);
                merged.addCall(src, tgt);
            });
        }
        return merged;
    }
}
