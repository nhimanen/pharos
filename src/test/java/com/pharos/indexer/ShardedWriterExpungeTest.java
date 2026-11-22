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
            writer.expungeDeletes();
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
}
