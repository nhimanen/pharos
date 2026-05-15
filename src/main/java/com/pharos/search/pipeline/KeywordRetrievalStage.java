package com.pharos.search.pipeline;

import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.List;

public final class KeywordRetrievalStage implements RetrievalStage {

    private final KeywordSearchStrategy strategy;

    public KeywordRetrievalStage(KeywordSearchStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public List<SearchResult> retrieve(IndexReader reader, SearchRequest req,
                                       SearchTrace trace) throws IOException {
        long t = System.currentTimeMillis();
        List<SearchResult> results = strategy.search(reader, req);
        if (trace != null) trace.record("keyword search", t);
        return results;
    }
}
