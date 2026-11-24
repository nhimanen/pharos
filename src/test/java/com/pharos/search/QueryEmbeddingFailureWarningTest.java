package com.pharos.search;

import com.pharos.embedding.EmbeddingException;
import com.pharos.embedding.EmbeddingProvider;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryEmbeddingFailureWarningTest {
    static class Failing implements EmbeddingProvider {
        public String modelId() { return "granite"; }
        public int dimensions() { return 4; }
        public boolean isAvailable() { return true; }
        public float[] embed(String t) { throw new EmbeddingException("tokenizer broke", null); }
    }

    private SearchTrace run(SearchRequest.SearchType type) throws Exception {
        var dir = new ByteBuffersDirectory();
        new IndexWriter(dir, new IndexWriterConfig()).close();
        try (var reader = DirectoryReader.open(dir)) {
            SearchTrace trace = new SearchTrace();
            var results = new VectorSearchStrategy(new Failing()).search(reader,
                    new SearchRequest("q", type, null, null, 5, "text", null, null, 0), trace);
            assertThat(results).isEmpty();
            return trace;
        }
    }

    @Test
    void hybridWarnsKeywordOnly() throws Exception {
        assertThat(run(SearchRequest.SearchType.HYBRID).warnings())
                .containsExactly("vector embedding failed for model granite; results are keyword-only");
    }

    @Test
    void vectorTypeWarnsNoVectorResults() throws Exception {
        assertThat(run(SearchRequest.SearchType.VECTOR).warnings())
                .containsExactly("vector embedding failed for model granite; no vector results");
    }
}
