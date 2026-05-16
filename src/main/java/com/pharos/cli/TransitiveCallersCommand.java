package com.pharos.cli;

import com.pharos.search.SearchEngine;
import picocli.CommandLine.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

@Command(
        name = "impact",
        description = "Find all methods that transitively call the given method (impact analysis). " +
                      "Returns a flat deduplicated set grouped by hop distance — no source bodies.",
        mixinStandardHelpOptions = true
)
public class TransitiveCallersCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Method FQN: com.example.MyClass#myMethod(String)")
    private String fqn;

    @Option(names = {"--depth", "-d"}, description = "Max hops to traverse (default: 5, max: 10)", defaultValue = "5")
    private int depth;

    @Option(names = {"--max", "-m"}, description = "Cap on total unique callers (default: 2000)", defaultValue = "2000")
    private int maxCallers;

    private final SearchEngine searchEngine;

    public TransitiveCallersCommand(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    @Override
    public Integer call() {
        try {
            int clampedDepth = Math.min(10, Math.max(1, depth));
            SearchEngine.TransitiveCallersResult result =
                    searchEngine.findTransitiveCallers(fqn, clampedDepth, maxCallers);

            if (result.totalCallers() == 0) {
                System.out.printf("No callers found for: %s%n", fqn);
                return 0;
            }

            System.out.printf("Transitive callers of: %s%n", result.root());
            System.out.printf("Depth: %d | Total unique callers: %d%s%n%n",
                    result.maxDepth(), result.totalCallers(),
                    result.truncated() ? " (truncated at " + maxCallers + ")" : "");

            // Group by depth for display
            Map<Integer, List<String>> byDepth = new TreeMap<>();
            for (var entry : result.callers()) {
                byDepth.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(entry.getKey());
            }
            for (var depthEntry : byDepth.entrySet()) {
                System.out.printf("Depth %d (%d callers):%n", depthEntry.getKey(), depthEntry.getValue().size());
                depthEntry.getValue().forEach(c -> System.out.printf("  %s%n", c));
                System.out.println();
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
