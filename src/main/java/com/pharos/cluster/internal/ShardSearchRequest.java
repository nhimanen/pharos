package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.SearchRequest;
import com.pharos.search.distributed.GlobalStats;

/**
 * {@code POST /internal/shard-search}. {@code retriever} names one retrieval stage ({@code keyword},
 * {@code vector}, {@code unified}); {@code generation}, when set, must match the served one.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ShardSearchRequest(String shard, Long generation, String retriever, SearchRequest request,
                                 float[] queryVector, GlobalStats globalStats) {}
