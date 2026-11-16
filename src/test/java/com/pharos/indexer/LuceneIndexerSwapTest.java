package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.StoredFields;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class LuceneIndexerSwapTest {

    private static final String PROJECT = "p";

    @TempDir Path tmp;

    private final AtomicReference<Path> shardDir = new AtomicReference<>();

    private void writeIndex(Path dir, String... ids) throws IOException {
        shardDir.set(dir);
        try (LuceneIndexer w = new LuceneIndexer(IndexConfig.defaults(), project -> shardDir.get());
             IndexWriter iw = w.openWriterFresh(PROJECT)) {
            for (String id : ids) {
                Document d = new Document();
                d.add(new StringField("id", id, Field.Store.YES));
                iw.addDocument(d);
            }
            iw.commit();
        }
    }

    private static List<String> ids(DirectoryReader r) throws IOException {
        List<String> out = new ArrayList<>();
        StoredFields sf = r.storedFields();
        for (int i = 0; i < r.maxDoc(); i++) out.add(sf.document(i).get("id"));
        return out;
    }

    @Test
    void retireDelayIsSixtySeconds() {
        assertThat(LuceneIndexer.RETIRE_DELAY).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void pathChangeOpensNewReader() throws Exception {
        Path a = tmp.resolve("gen-1");
        Path b = tmp.resolve("gen-2");
        writeIndex(a, "a1", "a2");
        writeIndex(b, "b1");
        shardDir.set(a);
        try (LuceneIndexer indexer = new LuceneIndexer(IndexConfig.defaults(), project -> shardDir.get(),
                Duration.ofMillis(300))) {
            DirectoryReader old = indexer.openReader(PROJECT);
            assertThat(ids(old)).containsExactly("a1", "a2");
            assertThat(indexer.openReader(PROJECT)).isSameAs(old);

            shardDir.set(b);
            DirectoryReader fresh = indexer.openReader(PROJECT);
            assertThat(fresh).isNotSameAs(old);
            assertThat(ids(fresh)).containsExactly("b1");
            assertThat(indexer.openReader(PROJECT)).isSameAs(fresh);

            // still usable by an in-flight search until the retire delay passes
            assertThat(old.getRefCount()).isPositive();
            assertThat(ids(old)).containsExactly("a1", "a2");

            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (old.getRefCount() > 0 && System.nanoTime() < deadline) Thread.sleep(20);
            assertThat(old.getRefCount()).isZero();
            assertThat(fresh.getRefCount()).isPositive();
        }
    }

    @Test
    void closeClosesRetired() throws Exception {
        Path a = tmp.resolve("gen-1");
        Path b = tmp.resolve("gen-2");
        writeIndex(a, "a1");
        writeIndex(b, "b1");
        shardDir.set(a);
        LuceneIndexer indexer = new LuceneIndexer(IndexConfig.defaults(), project -> shardDir.get());
        DirectoryReader old = indexer.openReader(PROJECT);
        shardDir.set(b);
        DirectoryReader fresh = indexer.openReader(PROJECT);
        assertThat(old.getRefCount()).isPositive();

        indexer.close();

        assertThat(old.getRefCount()).isZero();
        assertThat(fresh.getRefCount()).isZero();
    }
}
