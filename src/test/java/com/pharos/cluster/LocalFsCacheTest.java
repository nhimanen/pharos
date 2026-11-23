package com.pharos.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.pharos.config.IndexConfig;
import com.pharos.indexer.LuceneIndexer;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFsCacheTest {

    @TempDir Path tmp;

    @Test
    void luceneIndexerResolvesThroughCache() throws Exception {
        IndexConfig config = new IndexConfig();
        config.setIndexDir(tmp.resolve("indexes"));
        LocalCache cache = project -> tmp.resolve("elsewhere").resolve(project);
        try (LuceneIndexer indexer = new LuceneIndexer(config, cache)) {
            assertEquals(tmp.resolve("elsewhere/p/lucene"), indexer.getLucenePath("p"));
            assertEquals(tmp.resolve("elsewhere/p"), indexer.getProjectIndexDir("p"));
        }
    }

    @Test
    void defaultCacheMatchesIndexDir() throws Exception {
        IndexConfig config = new IndexConfig();
        config.setIndexDir(tmp.resolve("indexes"));
        assertEquals(tmp.resolve("indexes/p"), new LocalFsCache(config).shardDir("p"));
        try (LuceneIndexer indexer = new LuceneIndexer(config)) {
            assertEquals(tmp.resolve("indexes/p/lucene"), indexer.getLucenePath("p"));
            assertEquals(tmp.resolve("indexes/p"), indexer.getProjectIndexDir("p"));
        }
    }
}
