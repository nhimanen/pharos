package com.pharos.cli;

import com.pharos.indexer.EmbeddingCacheBackfiller;
import picocli.CommandLine.*;

import java.util.concurrent.Callable;

/**
 * Populates the persistent embedding cache from stored Lucene vectors without
 * calling the ONNX model, so that subsequent full re-indexes can skip re-embedding
 * unchanged Java files.
 */
@Command(
        name = "backfill-embedding-cache",
        description = "Seed the embedding cache from the existing Lucene index (no ONNX calls)",
        mixinStandardHelpOptions = true
)
public class BackfillEmbeddingCacheCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Project name (as shown in 'pharos projects')")
    private String projectName;

    private final EmbeddingCacheBackfiller backfiller;

    public BackfillEmbeddingCacheCommand(EmbeddingCacheBackfiller backfiller) {
        this.backfiller = backfiller;
    }

    @Override
    public Integer call() throws Exception {
        int added = backfiller.backfill(projectName);
        return added >= 0 ? 0 : 1;
    }
}
