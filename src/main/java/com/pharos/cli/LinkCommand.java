package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CrossProjectLinker;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

@Command(
        name = "link",
        description = "Link two projects to enable cross-project call graph traversal",
        mixinStandardHelpOptions = true
)
public class LinkCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "First project name")
    private String project1;

    @Parameters(index = "1", description = "Second project name")
    private String project2;

    private final ProjectRegistry registry;
    private final IndexConfig config;

    public LinkCommand(ProjectRegistry registry, IndexConfig config) {
        this.registry = registry;
        this.config = config;
    }

    @Override
    public Integer call() {
        try {
            registry.link(project1, project2);
            System.out.printf("Linked '%s' and '%s'%n", project1, project2);

            // Build cross-project call graph
            System.out.println("Building cross-project call graph...");
            CrossProjectLinker linker = new CrossProjectLinker(config, registry);
            linker.buildCrossProjectGraph(project1, project2);
            System.out.println("Cross-project graph built.");
            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
