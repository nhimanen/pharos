package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * {@code POST /internal/term-stats}: collection stats of {@code fields} and the stats of {@code terms}.
 * {@code synonymsHash}, when set, must equal the reader's own ({@code KeywordSearchStrategy#synonymsHash}):
 * terms analysed with other synonyms would not be the terms the shard scores (409 otherwise).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TermStatsRequest(String shard, List<String> fields, List<Term> terms, String synonymsHash) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Term(String field, String text) {}

    public TermStatsRequest(String shard, List<String> fields, List<Term> terms) {
        this(shard, fields, terms, null);
    }
}
