package com.pharos.cli;

import com.pharos.indexer.IncompatibleIndexException;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.GraphView;
import com.pharos.graph.GraphViews;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.index.DirectoryReader;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "stats",
        description = "Show index statistics for one or all projects",
        mixinStandardHelpOptions = true
)
public class StatsCommand implements Callable<Integer> {

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

    @Option(names = {"--project", "-p"}, description = "Show stats for a specific project")
    private String project;

    private final ProjectRegistry registry;
    private final LuceneIndexer luceneIndexer;

    public StatsCommand(ProjectRegistry registry, LuceneIndexer luceneIndexer) {
        this.registry = registry;
        this.luceneIndexer = luceneIndexer;
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
        List<ProjectMeta> projects = project != null
                ? registry.find(project).map(List::of).orElse(List.of())
                : registry.listAll();

        if (projects.isEmpty()) {
            out().println("No indexed projects found.");
            return 0;
        }

        for (ProjectMeta meta : projects) {
            out().printf("Project: %s%n", meta.getName());
            out().printf("  Root:         %s%n", meta.getRootPath());
            out().printf("  Index:        %s%n", meta.getIndexPath());
            out().printf("  Methods:      %d%n", meta.getMethodCount());
            out().printf("  Classes:      %d%n", meta.getClassCount());
            out().printf("  Files:        %d%n", meta.getFileCount());
            out().printf("  Packages:     %s%n", meta.getKnownPackages().size());
            out().printf("  Last indexed: %s%n", meta.getLastIndexed());
            if (!meta.getLinkedProjects().isEmpty()) {
                out().printf("  Linked:       %s%n", String.join(", ", meta.getLinkedProjects()));
            }

            // Lucene doc count
            try {
                DirectoryReader reader = luceneIndexer.openReader(meta.getName());
                out().printf("  Lucene docs:  %d%n", reader.numDocs());
            } catch (Exception e) {
                out().printf("  Lucene docs:  (unavailable)%n");
            }

            // Graph stats
            try (GraphView graph = GraphViews.open(meta)) { // checks the index format first
                out().printf("  Graph methods: %d%n", graph.methodCount());
                out().printf("  Graph calls:   %d%n", graph.callCount());
            } catch (IncompatibleIndexException e) {
                out().printf("  Graph:         (index format incompatible: %s)%n", e.status().wireName());
            } catch (Exception e) {
                out().printf("  Graph:         (unavailable)%n");
            }

            out().printf("  Unresolved:   %d call refs (for cross-project linking)%n",
                    registry.refsOf(meta).size());
            out().println();
        }
        return 0;
    }
}
