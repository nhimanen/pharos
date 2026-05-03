package com.pharos.cli;

import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNode;
import picocli.CommandLine.*;

import java.util.*;
import java.util.concurrent.Callable;

@Command(
        name = "module-deps",
        description = "Show direct dependencies and dependents of a module",
        mixinStandardHelpOptions = true
)
public class ModuleDepsCommand implements Callable<Integer> {

    @Parameters(index = "0",
            description = "Module key (groupId:artifactId) or project name")
    private String moduleRef;

    @Option(names = {"--transitive"},
            description = "Show all transitive (reachable) deps and dependents via BFS")
    private boolean transitive = false;

    private final ModuleGraphBuilder builder;

    public ModuleDepsCommand(ModuleGraphBuilder builder) {
        this.builder = builder;
    }

    @Override
    public Integer call() {
        try {
            ModuleGraph graph = builder.load();
            ModuleNode node = resolve(graph, moduleRef);
            if (node == null) {
                System.err.printf("Module not found: %s%n" +
                        "Use 'pharos modules' to list available modules.%n", moduleRef);
                return 1;
            }

            System.out.printf("Module: %s  [%s]  version: %s%n%n",
                    node.getModuleKey(),
                    node.isIndexed() ? "indexed/" + node.getProjectName() : "external",
                    node.getVersion());

            Set<ModuleNode> deps = transitive
                    ? bfsReach(graph, node, true)
                    : graph.getDependencies(node);
            System.out.printf("Dependencies (%s%d):%n", transitive ? "transitive, " : "", deps.size());
            deps.stream().sorted(Comparator.comparing(ModuleNode::getModuleKey))
                    .forEach(d -> System.out.printf("  → %-55s  [%s]  %s%n",
                            d.getModuleKey(),
                            d.isIndexed() ? "indexed" : "external",
                            d.getVersion()));

            Set<ModuleNode> dependents = transitive
                    ? bfsReach(graph, node, false)
                    : graph.getDependents(node);
            System.out.printf("%nUsed by (%s%d):%n", transitive ? "transitive, " : "", dependents.size());
            dependents.stream().sorted(Comparator.comparing(ModuleNode::getModuleKey))
                    .forEach(d -> System.out.printf("  ← %-55s  [%s]  %s%n",
                            d.getModuleKey(),
                            d.isIndexed() ? "indexed" : "external",
                            d.getVersion()));

            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static ModuleNode resolve(ModuleGraph graph, String ref) {
        ModuleNode n = graph.findByKey(ref);
        if (n != null) return n;
        return graph.findByProjectName(ref).orElse(null);
    }

    private static Set<ModuleNode> bfsReach(ModuleGraph graph, ModuleNode start, boolean outbound) {
        Set<ModuleNode> visited = new LinkedHashSet<>();
        Deque<ModuleNode> queue = new ArrayDeque<>();
        queue.add(start);
        while (!queue.isEmpty()) {
            ModuleNode cur = queue.pop();
            Set<ModuleNode> next = outbound
                    ? graph.getDependencies(cur)
                    : graph.getDependents(cur);
            for (ModuleNode n : next) {
                if (visited.add(n)) queue.add(n);
            }
        }
        visited.remove(start);
        return visited;
    }
}
