package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import com.pharos.search.VectorSearchStrategy;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.List;

public final class VectorRetrievalStage implements RetrievalStage {

    private final VectorSearchStrategy strategy;

    public VectorRetrievalStage(VectorSearchStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public List<SearchResult> retrieve(IndexReader reader, SearchRequest req,
                                       SearchTrace trace) throws IOException {
        long t = System.currentTimeMillis();
        List<SearchResult> results = strategy.search(reader, req);
        if (trace != null) trace.record("vector search (incl. embed)", t);
        return results;
    }
}
