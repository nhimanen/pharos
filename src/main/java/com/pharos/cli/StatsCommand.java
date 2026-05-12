package com.pharos.cli;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.index.DirectoryReader;
import picocli.CommandLine.*;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "stats",
        description = "Show index statistics for one or all projects",
        mixinStandardHelpOptions = true
)
public class StatsCommand implements Callable<Integer> {

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
        List<ProjectMeta> projects = project != null
                ? registry.find(project).map(List::of).orElse(List.of())
                : registry.listAll();

        if (projects.isEmpty()) {
            System.out.println("No indexed projects found.");
            return 0;
        }

        for (ProjectMeta meta : projects) {
            System.out.printf("Project: %s%n", meta.getName());
            System.out.printf("  Root:         %s%n", meta.getRootPath());
            System.out.printf("  Index:        %s%n", meta.getIndexPath());
            System.out.printf("  Methods:      %d%n", meta.getMethodCount());
            System.out.printf("  Classes:      %d%n", meta.getClassCount());
            System.out.printf("  Files:        %d%n", meta.getFileCount());
            System.out.printf("  Packages:     %s%n", meta.getKnownPackages().size());
            System.out.printf("  Last indexed: %s%n", meta.getLastIndexed());
            if (!meta.getLinkedProjects().isEmpty()) {
                System.out.printf("  Linked:       %s%n", String.join(", ", meta.getLinkedProjects()));
            }

            // Lucene doc count
            try {
                DirectoryReader reader = luceneIndexer.openReader(meta.getName());
                System.out.printf("  Lucene docs:  %d%n", reader.numDocs());
            } catch (Exception e) {
                System.out.printf("  Lucene docs:  (unavailable)%n");
            }

            // Graph stats
            try (CallGraph graph = CallGraph.open(Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb"))) {
                System.out.printf("  Graph methods: %d%n", graph.methodCount());
                System.out.printf("  Graph calls:   %d%n", graph.callCount());
            } catch (Exception e) {
                System.out.printf("  Graph:         (unavailable)%n");
            }

            System.out.printf("  Unresolved:   %d call refs (for cross-project linking)%n",
                    meta.getUnresolvedRefs().size());
            System.out.println();
        }
        return 0;
    }
}
