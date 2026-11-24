package com.pharos.search.pipeline;

import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import com.pharos.search.distributed.RawCandidate;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

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

    @Override
    public List<RawCandidate> retrieveRaw(IndexSearcher searcher, SearchRequest req,
                                          SearchTrace trace) throws IOException {
        long t = System.currentTimeMillis();
        List<RawCandidate> raw = strategy.retrieveRaw(searcher, req);
        if (trace != null) trace.record("keyword search (raw)", t);
        return raw;
    }
}
