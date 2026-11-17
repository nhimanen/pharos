package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/** {@code POST /internal/term-stats}: collection stats of {@code fields} and the stats of {@code terms}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TermStatsRequest(String shard, List<String> fields, List<Term> terms) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Term(String field, String text) {}
}
