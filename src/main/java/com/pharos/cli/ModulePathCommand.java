package com.pharos.cli;

import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNode;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "module-path",
        description = "Find the shortest dependency path between two modules",
        mixinStandardHelpOptions = true
)
public class ModulePathCommand implements Callable<Integer> {

    @Parameters(index = "0",
            description = "Source module (groupId:artifactId or project name)")
    private String from;

    @Parameters(index = "1",
            description = "Target module (groupId:artifactId or project name)")
    private String to;

    private final ModuleGraphBuilder builder;

    public ModulePathCommand(ModuleGraphBuilder builder) {
        this.builder = builder;
    }

    @Override
    public Integer call() {
        try {
            ModuleGraph graph = builder.load();

            String fromKey = resolveKey(graph, from);
            String toKey   = resolveKey(graph, to);
            if (fromKey == null) { System.err.println("Module not found: " + from); return 1; }
            if (toKey   == null) { System.err.println("Module not found: " + to);   return 1; }

            List<ModuleNode> path = graph.findPath(fromKey, toKey);
            if (path.isEmpty()) {
                System.out.printf("No dependency path from '%s' to '%s'%n", from, to);
                System.out.println("(Use 'pharos module-deps' to explore connections manually.)");
            } else {
                System.out.printf("Dependency path (%d hop%s):%n",
                        path.size() - 1, path.size() - 1 == 1 ? "" : "s");
                for (int i = 0; i < path.size(); i++) {
                    ModuleNode n = path.get(i);
                    String marker = i == 0 ? "START" : i == path.size() - 1 ? "END  " : "  →  ";
                    System.out.printf("  %s  %-55s  [%s]%n",
                            marker, n.getModuleKey(),
                            n.isIndexed() ? "indexed/" + n.getProjectName() : "external");
                }
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static String resolveKey(ModuleGraph graph, String ref) {
        if (graph.findByKey(ref) != null) return ref;
        return graph.findByProjectName(ref).map(ModuleNode::getModuleKey).orElse(null);
    }
}
