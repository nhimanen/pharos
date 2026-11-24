package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.distributed.GlobalStats;

/** A shard's statistics together with the generation they were read from. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TermStatsResponse(long generation, GlobalStats stats) {}
