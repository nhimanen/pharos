package com.pharos.cluster.internal;

import com.pharos.cluster.ShardId;

import java.util.OptionalLong;

/**
 * What a reader does for the {@code /internal/*} endpoints. {@link InternalApi} owns auth, parsing
 * and the generation check; implementations only do the work. The raw-retrieval implementation
 * arrives with the pipeline split (Task 4). Requests carry the expected generation: an implementation
 * may refuse early when it already serves another, and must report the generation it actually read
 * (a swap can land between the check and the work; {@link InternalApi} and {@link ShardClient}
 * compare it with the requested one).
 */
public interface ShardSearchService {

    /** Generation this reader currently serves for {@code shard}; empty when it does not serve it. */
    OptionalLong servedGeneration(ShardId shard);

    TermStatsResponse termStats(TermStatsRequest request) throws Exception;

    ShardSearchResponse searchRaw(ShardSearchRequest request) throws Exception;

    SnippetsResponse snippets(SnippetsRequest request) throws Exception;

    CalleesResponse callees(CalleesRequest request) throws Exception;
}
