package com.pharos.indexer;

import com.pharos.cluster.ShardId;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Found by the Phase 6 gate: the shards a reshard rewrites kept the moved files' deleted documents, which
 * Lucene still counts in the term statistics ({@code docFreq}, {@code docCount}) until a merge drops them,
 * so every BM25 score shifted after a ring change. {@link ShardedWriter#expungeDeletes} leaves none.
 */
class ShardedWriterExpungeTest {

    @Test
    void expungeDeletesLeavesNoDeletedDocumentInTheTermStatistics() throws IOException {
        Map<ShardId, Directory> dirs = new LinkedHashMap<>();
        Map<ShardId, IndexWriter> writers = new LinkedHashMap<>();
        for (String name : new String[] {"lib~ring=s01", "lib~ring=s02"}) {
            ShardId s = new ShardId(name);
            Directory d = new ByteBuffersDirectory();
            try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new StandardAnalyzer()))) {
                for (int i = 0; i < 40; i++) {
                    Document doc = new Document();
                    doc.add(new StringField(DocumentMapper.F_FILE_PATH, "/src/F" + (i % 8) + ".java", Field.Store.YES));
                    w.addDocument(doc);
                }
                w.commit();
            }
            dirs.put(s, d);
            IndexWriterConfig c = new IndexWriterConfig(new StandardAnalyzer());
            c.setCommitOnClose(false);
            writers.put(s, new IndexWriter(d, c));
        }
        try (ShardedWriter writer = ShardedWriter.split(writers, null)) {
            // a moved file leaves its old shard: 12.5 % deleted, below the merge policy's natural
            // deletes threshold (20 %), so nothing but expungeDeletes merges them away
            writer.deleteFile("/src/F1.java");
            writer.expungeDeletes(IndexAbort.none());
            writer.commit();
        }
        for (Map.Entry<ShardId, Directory> e : dirs.entrySet()) {
            try (DirectoryReader r = DirectoryReader.open(e.getValue())) {
                assertThat(r.numDeletedDocs()).as(e.getKey() + " deleted documents").isZero();
                assertThat(r.numDocs()).isEqualTo(35);
                assertThat(MultiTerms.getTerms(r, DocumentMapper.F_FILE_PATH).getDocCount())
                        .as(e.getKey() + " term statistics count live documents only").isEqualTo(35);
            }
        }
    }

    /**
     * Ruling P6-R18: the expunge's merges run in the background while the abort is polled. An abort in the
     * middle of a merge throws at once; rollback() then stops the merge, and the last commit stays as it
     * was (the deletes and the merge are dropped).
     */
    @Test
    void anAbortDuringTheExpungeRollsBackCleanly() throws Exception {
        java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        Directory d = new ByteBuffersDirectory();
        try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new StandardAnalyzer()))) {
            for (int i = 0; i < 40; i++) {
                Document doc = new Document();
                doc.add(new StringField(DocumentMapper.F_FILE_PATH, "/src/F" + (i % 8) + ".java", Field.Store.YES));
                w.addDocument(doc);
            }
            w.commit();
        }
        IndexWriterConfig c = new IndexWriterConfig(new StandardAnalyzer());
        c.setCommitOnClose(false);
        c.setMergeScheduler(new org.apache.lucene.index.ConcurrentMergeScheduler() {
            @Override
            protected void doMerge(MergeSource source, org.apache.lucene.index.MergePolicy.OneMerge merge)
                    throws IOException {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                super.doMerge(source, merge);
            }
        });
        Map<ShardId, IndexWriter> writers = new LinkedHashMap<>();
        writers.put(new ShardId("lib~ring=s01"), new IndexWriter(d, c));
        IndexAbort abort = new IndexAbort();
        ShardedWriter writer = ShardedWriter.split(writers, null);
        writer.deleteFile("/src/F1.java");
        java.util.concurrent.CompletableFuture<Throwable> expunge = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                writer.expungeDeletes(abort);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });
        assertThat(started.await(20, java.util.concurrent.TimeUnit.SECONDS)).as("the expunge merge started").isTrue();
        abort.request("test: stop the reshard");
        Throwable thrown = expunge.get(20, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(thrown).as("the abort is seen while the merge runs").isInstanceOf(IndexAbortedException.class);
        release.countDown();
        writer.rollback();
        writer.close();
        try (DirectoryReader r = DirectoryReader.open(d)) {
            assertThat(r.numDocs()).as("the last commit, unchanged").isEqualTo(40);
            assertThat(r.numDeletedDocs()).isZero();
        }
        try (IndexWriter again = new IndexWriter(d, new IndexWriterConfig(new StandardAnalyzer()))) {
            assertThat(again.getDocStats().numDocs).as("the write lock was released").isEqualTo(40);
        }
    }
}
