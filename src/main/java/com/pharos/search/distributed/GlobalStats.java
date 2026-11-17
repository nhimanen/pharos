package com.pharos.search.distributed;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * Term and collection statistics, per shard on the wire and summed across shards by the coordinator
 * (Task 5) so every shard scores BM25 against the same corpus. Minimal wire shape; Task 4 adds the
 * summing and the searcher that consumes it.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GlobalStats(Map<String, FieldStats> fields, List<TermStat> terms) {

    /** Collection statistics of one field. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FieldStats(long docCount, long sumTotalTermFreq, long sumDocFreq) {}

    /** Document and total frequency of one term of one field. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TermStat(String field, String text, long docFreq, long totalTermFreq) {}
}
