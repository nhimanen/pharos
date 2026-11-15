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
        try (Directory dir = LuceneIndexer.openDirectory(tmp)) {
            assertThat(dir).isInstanceOf(MMapDirectory.class);
        }
    }
}
