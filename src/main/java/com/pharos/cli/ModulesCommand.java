package com.pharos.cli;

import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNode;
import picocli.CommandLine.*;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "modules",
        description = "List all known modules in the dependency graph (indexed and external)",
        mixinStandardHelpOptions = true
)
public class ModulesCommand implements Callable<Integer> {

    @Option(names = {"--indexed-only"},
            description = "Show only modules with indexed source")
    private boolean indexedOnly = false;

    @Option(names = {"--external-only"},
            description = "Show only external (non-indexed) dependency modules")
    private boolean externalOnly = false;

    private final ModuleGraphBuilder builder;

    public ModulesCommand(ModuleGraphBuilder builder) {
        this.builder = builder;
    }

    @Override
    public Integer call() {
        try {
            ModuleGraph graph = builder.load();
            if (graph.nodeCount() == 0) {
                System.out.println("No modules registered yet. Run 'pharos index' on a Maven project first.");
                return 0;
            }

            List<ModuleNode> nodes = graph.allNodes().stream()
                    .filter(n -> !indexedOnly  || n.isIndexed())
                    .filter(n -> !externalOnly || !n.isIndexed())
                    .sorted(Comparator.comparing(ModuleNode::getModuleKey))
                    .toList();

            System.out.printf("%-55s  %-14s  %4s  %4s  %s%n",
                    "MODULE", "STATUS", "DEPS", "USED", "VERSION");
            System.out.println("-".repeat(95));

            for (ModuleNode n : nodes) {
                int depCount = graph.getDependencies(n).size();
                int usedBy   = graph.getDependents(n).size();
                String status = n.isIndexed()
                        ? "indexed[" + n.getProjectName() + "]"
                        : "external";
                System.out.printf("%-55s  %-14s  %4d  %4d  %s%n",
                        n.getModuleKey(), status, depCount, usedBy, n.getVersion());
            }
            System.out.printf("%n%d node(s), %d edge(s) total%n",
                    graph.nodeCount(), graph.edgeCount());
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
