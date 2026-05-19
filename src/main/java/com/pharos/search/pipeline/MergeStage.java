package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;

import java.util.List;

public interface MergeStage {
    /**
     * Merges multiple candidate lists into one ranked list of at most {@code req.limit()} results.
     * Implementations may read {@link SearchRequest#classification()} for adaptive fusion weights.
     */
    List<SearchResult> merge(List<List<SearchResult>> candidates,
                             SearchRequest req, SearchTrace trace);
}
