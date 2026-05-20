package com.pharos.cli;

import com.pharos.analysis.ConceptMiner;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.index.IndexReader;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

@Command(
        name = "mine",
        description = "Mine characteristic vocabulary and synonym candidates from an indexed project",
        mixinStandardHelpOptions = true
)
public class MineCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Project name to mine")
    private String project;

    private final ProjectRegistry registry;
    private final LuceneIndexer luceneIndexer;

    public MineCommand(ProjectRegistry registry, LuceneIndexer luceneIndexer) {
        this.registry = registry;
        this.luceneIndexer = luceneIndexer;
    }

    @Override
    public Integer call() throws Exception {
        if (registry.find(project).isEmpty()) {
            System.err.println("Project not indexed: " + project);
            System.err.println("Run 'pharos index <path>' first.");
            return 1;
        }
        IndexReader reader = luceneIndexer.openReader(project);
        new ConceptMiner().mine(reader);
        return 0;
    }
}
