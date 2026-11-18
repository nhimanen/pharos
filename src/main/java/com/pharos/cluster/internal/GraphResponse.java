package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.SearchResult;
import com.pharos.search.distributed.RawCandidate;

import java.util.List;
import java.util.Map;

/** The answer to a {@link GraphRequest}; {@code generation} is the one read. Unused parts are empty. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GraphResponse(long generation, Map<String, List<String>> lists, Map<String, SearchResult> docs,
                            List<RawCandidate> candidates) {
    public GraphResponse {
        lists = lists == null ? Map.of() : lists;
        docs = docs == null ? Map.of() : docs;
        candidates = candidates == null ? List.of() : candidates;
    }
}
