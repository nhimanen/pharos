package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.cluster.RoleGuard;
import com.pharos.indexer.EmbeddingCacheBackfiller;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

@Command(
        name = "backfill-embedding-cache",
        description = "Seed the embedding cache from the existing Lucene index (no ONNX calls)",
        mixinStandardHelpOptions = true
)
public class BackfillEmbeddingCacheCommand implements Callable<Integer> {

    @Parameters(index = "0", arity = "0..1",
            description = "Project name (as shown in 'pharos projects'); omit when --all is set")
    private String projectName;

    @Option(names = "--all", description = "Backfill every registered project")
    private boolean all;

    private final EmbeddingCacheBackfiller backfiller;
    private final IndexConfig config;

    public BackfillEmbeddingCacheCommand(EmbeddingCacheBackfiller backfiller, IndexConfig config) {
        this.backfiller = backfiller;
        this.config = config;
    }

    @Override
    public Integer call() throws Exception {
        try {
            RoleGuard.requireIndexing(config);
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            return 2;
        }
        if (all && projectName != null) {
            System.err.println("Specify either a project name or --all, not both.");
            return 2;
        }
        if (!all && projectName == null) {
            System.err.println("Missing project name. Pass a project name or use --all.");
            return 2;
        }
        int added = all ? backfiller.backfillAll() : backfiller.backfill(projectName);
        return added >= 0 ? 0 : 1;
    }
}
