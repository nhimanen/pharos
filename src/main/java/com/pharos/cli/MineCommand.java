package com.pharos.cli;

import com.pharos.analysis.ConceptMiner;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.index.IndexReader;
import picocli.CommandLine.*;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(
        name = "mine",
        description = "Mine characteristic vocabulary and synonym candidates from an indexed project",
        mixinStandardHelpOptions = true
)
public class MineCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Project name to mine")
    private String project;

    @Option(names = "--sources", defaultValue = "15",
            description = "Bitmask of sources to run: 1=class-names, 2=javadoc-bigrams, 4=acronyms, 8=wormhole (default: 15 = all)")
    private int sources;

    @Option(names = "--write", defaultValue = "false",
            description = "Append new synonym rules to synonyms.txt (default: dry-run, report only)")
    private boolean write;

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
        if (write) {
            IndexConfig config = IndexConfig.load();
            Path synonymFile = config.getSynonymsFile();
            int added = new ConceptMiner().appendNewSynonyms(reader, synonymFile, project, sources);
            System.out.printf("Appended %d new synonym rules for '%s' (sources=0b%s)%n",
                    added, project, Integer.toBinaryString(sources));
        } else {
            new ConceptMiner().mine(reader);
        }
        return 0;
    }
}
