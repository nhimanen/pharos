package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.List;

public interface RerankStage {
    List<SearchResult> rerank(List<SearchResult> results, SearchRequest req,
                              IndexReader reader, int limit, SearchTrace trace)
            throws IOException;
}
