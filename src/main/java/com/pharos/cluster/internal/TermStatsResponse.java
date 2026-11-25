package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.pharos.search.distributed.GlobalStats;

/**
 * A shard's statistics together with the generation they were read from, and for a shard of a split
 * project the index run ({@code projectGeneration}) that generation belongs to (omitted otherwise, so
 * a {@code [project]} answer is what it was): the coordinator keeps all shards of a project on one run.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TermStatsResponse(long generation, GlobalStats stats,
                                @JsonInclude(JsonInclude.Include.NON_NULL) Long projectGeneration) {
    public TermStatsResponse(long generation, GlobalStats stats) {
        this(generation, stats, null);
    }
}
