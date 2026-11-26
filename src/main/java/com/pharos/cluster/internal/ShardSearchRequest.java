package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.pharos.search.SearchRequest;
import com.pharos.search.distributed.GlobalStats;

/**
 * {@code POST /internal/shard-search}. {@code retriever} names one retrieval stage ({@code keyword},
 * {@code vector}, {@code unified}); {@code generation}, when set, must match the served one. The
 * {@code request} carries the coordinator's classification (classified once there). {@code synonymsHash},
 * when set, must equal the reader's own (409 otherwise): coordinator and shard analyse alike.
 * {@code modelId}, {@code dims} and {@code modelFingerprint} describe the model {@code queryVector}
 * was embedded with; a shard without vectors of that model (or configured with another fingerprint
 * or dimension count for it) refuses a vector request with 409 {@code "model mismatch"}.
 *
 * <p>Two-round retrievers (exact merges of the candidate pools across shards): {@code vector-knn}
 * and {@code unified-recall} return the shard's first-pass / recall candidates as ids with scores;
 * {@code vector-rescore} and {@code unified-rescore} then score only the globally selected ones of
 * the shard ({@code selected}: first-pass or BM25 candidates with their scores, in global order;
 * {@code selectedKnn}: unified's KNN candidates), {@code chunkVectors} telling whether any shard has
 * chunk vectors (vector rescoring applies then).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ShardSearchRequest(String shard, Long generation, String retriever, SearchRequest request,
                                 float[] queryVector, GlobalStats globalStats, String synonymsHash,
                                 String modelId, int dims, String modelFingerprint,
                                 java.util.List<com.pharos.search.distributed.ScoredId> selected,
                                 java.util.List<com.pharos.search.distributed.ScoredId> selectedKnn,
                                 boolean chunkVectors,
                                 @com.fasterxml.jackson.annotation.JsonInclude(
                                         com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                                 StatsCheck statsCheck) {

    /**
     * Asks the shard to also return its own statistics of {@code fields} and {@code terms}
     * ({@link ShardSearchResponse#shardStats}), read from the generation it searched exactly as
     * {@code /internal/term-stats} reads them: how a coordinator that scored with cached statistics
     * proves they are the ones a statistics round would have gathered. A reader without it ignores it.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record StatsCheck(java.util.List<String> fields, java.util.List<TermStatsRequest.Term> terms) {}

    public ShardSearchRequest(String shard, Long generation, String retriever, SearchRequest request,
                              float[] queryVector, GlobalStats globalStats, String synonymsHash,
                              String modelId, int dims, String modelFingerprint,
                              java.util.List<com.pharos.search.distributed.ScoredId> selected,
                              java.util.List<com.pharos.search.distributed.ScoredId> selectedKnn,
                              boolean chunkVectors) {
        this(shard, generation, retriever, request, queryVector, globalStats, synonymsHash, modelId, dims,
                modelFingerprint, selected, selectedKnn, chunkVectors, null);
    }

    public ShardSearchRequest(String shard, Long generation, String retriever, SearchRequest request,
                              float[] queryVector, GlobalStats globalStats, String synonymsHash,
                              String modelId, int dims, String modelFingerprint) {
        this(shard, generation, retriever, request, queryVector, globalStats, synonymsHash, modelId, dims,
                modelFingerprint, null, null, false);
    }

    public ShardSearchRequest(String shard, Long generation, String retriever, SearchRequest request,
                              float[] queryVector, GlobalStats globalStats) {
        this(shard, generation, retriever, request, queryVector, globalStats, null);
    }

    public ShardSearchRequest(String shard, Long generation, String retriever, SearchRequest request,
                              float[] queryVector, GlobalStats globalStats, String synonymsHash) {
        this(shard, generation, retriever, request, queryVector, globalStats, synonymsHash, null, 0, null);
    }
}
