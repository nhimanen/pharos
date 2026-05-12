package com.pharos.cli;

import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNodeData;
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

    private final ModuleGraphBuilder builder;

    public ModuleDepsCommand(ModuleGraphBuilder builder) {
        this.builder = builder;
    }

    @Override
    public Integer call() {
        try (ModuleGraph graph = builder.open()) {
            ModuleNodeData node = resolve(graph, moduleRef);
            if (node == null) {
                System.err.printf("Module not found: %s%n" +
                        "Use 'pharos modules' to list available modules.%n", moduleRef);
                return 1;
            }

            System.out.printf("Module: %s  [%s]  version: %s%n%n",
                    node.moduleKey(),
                    node.isIndexed() ? "indexed/" + node.projectName() : "external",
                    node.version());

            boolean transitive = false;
            Set<ModuleNodeData> deps = transitive
                    ? bfsReach(graph, node, true)
                    : graph.dependencies(node.moduleKey());
            System.out.printf("Dependencies (%s%d):%n", transitive ? "transitive, " : "", deps.size());
            deps.stream().sorted(Comparator.comparing(ModuleNodeData::moduleKey))
                    .forEach(d -> System.out.printf("  → %-55s  [%s]  %s%n",
                            d.moduleKey(),
                            d.isIndexed() ? "indexed" : "external",
                            d.version()));

            Set<ModuleNodeData> dependents = transitive
                    ? bfsReach(graph, node, false)
                    : graph.dependents(node.moduleKey());
            System.out.printf("%nUsed by (%s%d):%n", transitive ? "transitive, " : "", dependents.size());
            dependents.stream().sorted(Comparator.comparing(ModuleNodeData::moduleKey))
                    .forEach(d -> System.out.printf("  ← %-55s  [%s]  %s%n",
                            d.moduleKey(),
                            d.isIndexed() ? "indexed" : "external",
                            d.version()));

            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static ModuleNodeData resolve(ModuleGraph graph, String ref) {
        Optional<ModuleNodeData> n = graph.findByKey(ref);
        if (n.isPresent()) return n.get();
        return graph.findByProjectName(ref).orElse(null);
    }

    private static Set<ModuleNodeData> bfsReach(ModuleGraph graph, ModuleNodeData start, boolean outbound) {
        Set<ModuleNodeData> visited = new LinkedHashSet<>();
        Deque<ModuleNodeData> queue = new ArrayDeque<>();
        queue.add(start);
        while (!queue.isEmpty()) {
            ModuleNodeData cur = queue.pop();
            Set<ModuleNodeData> next = outbound
                    ? graph.dependencies(cur.moduleKey())
                    : graph.dependents(cur.moduleKey());
            for (ModuleNodeData n : next) {
                if (visited.add(n)) queue.add(n);
            }
        }
        visited.remove(start);
        return visited;
    }
}
