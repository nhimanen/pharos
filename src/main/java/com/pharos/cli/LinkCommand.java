package com.pharos.cli;

import com.pharos.cluster.RoleGuard;
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

    /** Set by picocli; the web API runs this command with its own writers (see {@code WebServer} admin commands). */
    @picocli.CommandLine.Spec
    picocli.CommandLine.Model.CommandSpec spec;
    private java.io.PrintWriter stdout;
    private java.io.PrintWriter stderr;

    private java.io.PrintWriter out() {
        if (spec != null) return spec.commandLine().getOut();
        if (stdout == null) stdout = new java.io.PrintWriter(System.out, true);
        return stdout;
    }

    private java.io.PrintWriter err() {
        if (spec != null) return spec.commandLine().getErr();
        if (stderr == null) stderr = new java.io.PrintWriter(System.err, true);
        return stderr;
    }

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
            return run();
        } finally {
            out().flush();
            err().flush();
        }
    }

    private Integer run() {
        try {
            RoleGuard.requireIndexing(config);
        } catch (IllegalStateException e) {
            err().println(e.getMessage());
            return 2;
        }
        try {
            registry.link(project1, project2);
            out().printf("Linked '%s' and '%s'%n", project1, project2);

            // Build cross-project call graph
            out().println("Building cross-project call graph...");
            CrossProjectLinker linker = new CrossProjectLinker(config, registry);
            linker.buildCrossProjectGraph(project1, project2);
            out().println("Cross-project graph built.");
            return 0;
        } catch (Exception e) {
            err().println("Error: " + e.getMessage());
            return 1;
        }
    }
}
