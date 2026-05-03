package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CallGraphSerializer;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchResult;
import picocli.CommandLine.*;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

@Command(
        name = "callees",
        description = "Show all methods called by the given method FQN",
        mixinStandardHelpOptions = true
)
public class CalleesCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Fully qualified method: com.example.MyClass#myMethod(String,int)")
    private String fqn;

    @Option(names = {"--project", "-p"}, description = "Restrict to a specific project")
    private String project;

    private final SearchEngine searchEngine;
    private final ProjectRegistry registry;
    private final IndexConfig config;

    public CalleesCommand(SearchEngine searchEngine, ProjectRegistry registry, IndexConfig config) {
        this.searchEngine = searchEngine;
        this.registry = registry;
        this.config = config;
    }

    @Override
    public Integer call() {
        try {
            // Try graph-based lookup first
            Set<String> callees = getCalleesFromGraph(fqn);

            if (callees.isEmpty()) {
                // Fall back to Lucene
                List<SearchResult> results = searchEngine.findCallees(fqn, project);
                if (results.isEmpty()) {
                    System.out.println("No callees found for: " + fqn);
                } else {
                    System.out.printf("Methods called by %s (%d):%n", fqn, results.size());
                    results.forEach(r -> System.out.printf("  %s [%s:%d]%n",
                            r.label(), r.filePath(), r.startLine()));
                }
            } else {
                System.out.printf("Methods called by %s (%d):%n", fqn, callees.size());
                callees.forEach(c -> System.out.printf("  %s%n", c));
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private Set<String> getCalleesFromGraph(String callerFqn) {
        CallGraphSerializer serializer = new CallGraphSerializer();
        List<ProjectMeta> projects = project != null
                ? registry.find(project).map(List::of).orElse(List.of())
                : registry.listAll();

        Set<String> allCallees = new LinkedHashSet<>();
        for (ProjectMeta meta : projects) {
            Path graphFile = Path.of(meta.getIndexPath()).resolve("graph.graphml");
            try {
                CallGraph graph = serializer.load(graphFile);
                allCallees.addAll(graph.getCallees(callerFqn));
            } catch (Exception e) {
                // Graph not available for this project
            }
        }
        return allCallees;
    }
}
