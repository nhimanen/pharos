package com.pharos.search.pipeline;

import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;

import java.util.List;

public interface MergeStage {
    List<SearchResult> merge(List<List<SearchResult>> candidates,
                             int limit, String query, SearchTrace trace);
}
