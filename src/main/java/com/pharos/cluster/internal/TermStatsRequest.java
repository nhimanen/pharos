package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * {@code POST /internal/term-stats}: collection stats of {@code fields} and the stats of {@code terms}.
 * {@code synonymsHash}, when set, must equal the reader's own ({@code KeywordSearchStrategy#synonymsHash}):
 * terms analysed with other synonyms would not be the terms the shard scores (409 otherwise).
 *
 * <p>Ruling P6-R19 (both optional; an older reader ignores them and answers its served generation):
 * {@code generation} asks for that generation of the shard (the served one, or the previous one the reader
 * still retains; another one is refused 409 "generation mismatch"); {@code run} asks for the generation at
 * that index run of a split project ({@code projectGeneration}): the served one if it is at that run, else
 * the retained previous one if that is, else the served one (the answer states which).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TermStatsRequest(String shard, List<String> fields, List<Term> terms, String synonymsHash,
                               @JsonInclude(JsonInclude.Include.NON_NULL) Long generation,
                               @JsonInclude(JsonInclude.Include.NON_NULL) Long run,
                               @JsonInclude(JsonInclude.Include.NON_NULL) Integer specVersion) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Term(String field, String text) {}

    public TermStatsRequest(String shard, List<String> fields, List<Term> terms, String synonymsHash) {
        this(shard, fields, terms, synonymsHash, null, null, null);
    }

    public TermStatsRequest(String shard, List<String> fields, List<Term> terms) {
        this(shard, fields, terms, null);
    }

    /** This request for generation {@code generation} (null: whatever the reader serves). */
    public TermStatsRequest at(Long generation) {
        return new TermStatsRequest(shard, fields, terms, synonymsHash, generation, run, specVersion);
    }

    /**
     * Ruling P6-R21: this request for the shard as spec version {@code v} of its project has it (the version the
     * coordinator planned; null: none). A reader whose catalog has another active version answers with the
     * generation version {@code v} names (the previous version's, from its retained generation), or refuses.
     * Ignored once a generation is asked for.
     */
    public TermStatsRequest atVersion(Integer v) {
        return new TermStatsRequest(shard, fields, terms, synonymsHash, generation, run, v);
    }

    /** This request for index run {@code run} (see the class comment). */
    public TermStatsRequest atRun(Long run) {
        return new TermStatsRequest(shard, fields, terms, synonymsHash, generation, run, specVersion);
    }
}
