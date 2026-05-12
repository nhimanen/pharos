package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchResult;
import picocli.CommandLine.*;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

@Command(
        name = "callers",
        description = "Show all methods that call the given method FQN",
        mixinStandardHelpOptions = true
)
public class CallersCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Fully qualified method: com.example.MyClass#myMethod(String,int)")
    private String fqn;

    @Option(names = {"--project", "-p"}, description = "Restrict to a specific project")
    private String project;

    @Option(names = {"--depth", "-d"},
            description = "Traversal depth (default: 1)",
            defaultValue = "1")
    private int depth;

    private final SearchEngine searchEngine;
    private final ProjectRegistry registry;

    public CallersCommand(SearchEngine searchEngine, ProjectRegistry registry, IndexConfig config) {
        this.searchEngine = searchEngine;
        this.registry = registry;
    }

    @Override
    public Integer call() {
        try {
            // First try graph-based lookup (fast)
            Set<String> callers = getCallersFromGraph(fqn);

            if (callers.isEmpty()) {
                // Fall back to Lucene index lookup
                List<SearchResult> results = searchEngine.findCallers(fqn, project);
                if (results.isEmpty()) {
                    System.out.println("No callers found for: " + fqn);
                } else {
                    System.out.printf("Callers of %s (%d):%n", fqn, results.size());
                    results.forEach(r -> System.out.printf("  %s [%s:%d]%n",
                            r.label(), r.filePath(), r.startLine()));
                }
            } else {
                System.out.printf("Callers of %s (%d):%n", fqn, callers.size());
                callers.forEach(c -> System.out.printf("  %s%n", c));
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private Set<String> getCallersFromGraph(String targetFqn) {
        List<ProjectMeta> projects = project != null
                ? registry.find(project).map(List::of).orElse(List.of())
                : registry.listAll();

        Set<String> allCallers = new LinkedHashSet<>();
        for (ProjectMeta meta : projects) {
            Path dbDir = Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb");
            try (CallGraph graph = CallGraph.open(dbDir)) {
                allCallers.addAll(graph.callers(targetFqn));
            } catch (Exception e) {
                // Graph not available for this project
            }
        }
        return allCallers;
    }
}
