package com.pharos.cluster.internal;

import com.pharos.cluster.ShardId;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * What a reader does for the {@code /internal/*} endpoints. {@link InternalApi} owns auth, parsing
 * and the generation check; implementations only do the work. The raw-retrieval implementation
 * arrives with the pipeline split (Task 4).
 */
public interface ShardSearchService {

    /** Generation this reader currently serves for {@code shard}; empty when it does not serve it. */
    OptionalLong servedGeneration(ShardId shard);

    TermStatsResponse termStats(TermStatsRequest request) throws Exception;

    ShardSearchResponse searchRaw(ShardSearchRequest request) throws Exception;

    SnippetsResponse snippets(SnippetsRequest request) throws Exception;

    Map<String, List<String>> callees(CalleesRequest request) throws Exception;
}
