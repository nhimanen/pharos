package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.distributed.RawCandidate;

import java.util.List;

/**
 * {@code maxInDegree} is the shard's own; the coordinator takes the maximum across shards.
 * {@code shapingIntent} is the intent the stage's own shaping would use (unified: it depends on
 * whether the vector pass ran); null for keyword and vector, whose intent the coordinator knows.
 * {@code recall}/{@code knn}/{@code chunkVectors} answer the first round of a two-round retriever.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ShardSearchResponse(long generation, int maxInDegree, List<RawCandidate> candidates,
                                  String shapingIntent, List<com.pharos.search.distributed.ScoredId> recall,
                                  List<com.pharos.search.distributed.ScoredId> knn, boolean chunkVectors) {

    public ShardSearchResponse(long generation, int maxInDegree, List<RawCandidate> candidates) {
        this(generation, maxInDegree, candidates, null);
    }

    public ShardSearchResponse(long generation, int maxInDegree, List<RawCandidate> candidates,
                               String shapingIntent) {
        this(generation, maxInDegree, candidates, shapingIntent, null, null, false);
    }

    /** A first round's answer: candidate ids with scores ({@code vector-knn}, {@code unified-recall}). */
    public static ShardSearchResponse recall(long generation, List<com.pharos.search.distributed.ScoredId> recall,
                                             List<com.pharos.search.distributed.ScoredId> knn, boolean chunkVectors) {
        return new ShardSearchResponse(generation, 1, List.of(), null, recall, knn, chunkVectors);
    }
}
