package com.pharos.cluster.internal;

import com.pharos.cluster.ShardId;

import java.util.OptionalLong;

/**
 * What a reader does for the {@code /internal/*} endpoints. {@link InternalApi} owns auth, parsing
 * and the generation check; implementations only do the work. The raw-retrieval implementation
 * arrives with the pipeline split (Task 4). Requests carry the expected generation: an implementation
 * may refuse early when it already serves another, and must report the generation it actually read
 * (a swap can land between the check and the work; {@link InternalApi} and {@link ShardClient}
 * compare it with the requested one). An implementation that analyses queries refuses a request whose
 * synonyms hash differs from its own with {@link SynonymsMismatchException} (answered 409).
 */
public interface ShardSearchService {

    /** Generation this reader currently serves for {@code shard}; empty when it does not serve it. */
    OptionalLong servedGeneration(ShardId shard);

    /**
     * Ruling P6-R19: true when a request for {@code generation} of {@code shard} is answered here: the served
     * generation, or (an implementation that retains them) the previous one for its grace.
     */
    default boolean servesGeneration(ShardId shard, long generation) {
        OptionalLong served = servedGeneration(shard);
        return served.isPresent() && served.getAsLong() == generation;
    }

    TermStatsResponse termStats(TermStatsRequest request) throws Exception;

    ShardSearchResponse searchRaw(ShardSearchRequest request) throws Exception;

    SnippetsResponse snippets(SnippetsRequest request) throws Exception;

    CalleesResponse callees(CalleesRequest request) throws Exception;

    /** One call-graph or lookup operation ({@link GraphRequest}) on the shard. */
    GraphResponse graph(GraphRequest request) throws Exception;
}
