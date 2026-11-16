package com.pharos.indexer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Index files are memory-mapped (Lucene's 64-bit default), not copied into heap buffers by NIOFSDirectory. */
class LuceneDirectoryTest {

    @TempDir Path tmp;

    @Test
    void indexesAreOpenedWithMMapDirectory() throws Exception {
        try (LuceneIndexer indexer = new LuceneIndexer(com.pharos.config.IndexConfig.defaults());
             Directory dir = indexer.openDirectory(tmp)) {
            assertThat(indexer.directoryFactory()).isSameAs(LuceneDirectoryFactory.DEFAULT);
            assertThat(dir).isInstanceOf(MMapDirectory.class);
        }
    }

    @Test
    void directoriesAreOpenedThroughTheInstalledFactory() throws Exception {
        java.util.List<Path> opened = new java.util.ArrayList<>();
        try (LuceneIndexer indexer = new LuceneIndexer(com.pharos.config.IndexConfig.defaults())) {
            indexer.setDirectoryFactory(p -> {
                opened.add(p);
                return new org.apache.lucene.store.ByteBuffersDirectory();
            });
            try (Directory dir = indexer.openDirectory(tmp)) {
                assertThat(dir).isInstanceOf(org.apache.lucene.store.ByteBuffersDirectory.class);
            }
        }
        assertThat(opened).containsExactly(tmp);
    }
}
