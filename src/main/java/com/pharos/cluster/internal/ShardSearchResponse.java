package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.distributed.RawCandidate;

import java.util.List;

/**
 * {@code maxInDegree} is the shard's own; the coordinator takes the maximum across shards.
 * {@code shapingIntent} is the intent the stage's own shaping would use (unified: it depends on
 * whether the vector pass ran); null for keyword and vector, whose intent the coordinator knows.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ShardSearchResponse(long generation, int maxInDegree, List<RawCandidate> candidates,
                                  String shapingIntent) {

    public ShardSearchResponse(long generation, int maxInDegree, List<RawCandidate> candidates) {
        this(generation, maxInDegree, candidates, null);
    }
}
