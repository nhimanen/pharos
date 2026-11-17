package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.distributed.RawCandidate;

import java.util.List;

/** {@code maxInDegree} is the shard's own; the coordinator takes the maximum across shards. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ShardSearchResponse(long generation, int maxInDegree, List<RawCandidate> candidates) {}
