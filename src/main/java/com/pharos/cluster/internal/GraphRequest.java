package com.pharos.cluster.internal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * {@code POST /internal/graph}: one call-graph or lookup operation on one shard, so a coordinator
 * can answer graph tools for shards it does not hold. {@code op}:
 * <ul>
 *   <li>{@code neighbors}: {@code fqns} → callers/callees ({@code direction} {@code callers},
 *       {@code callees} or {@code both}) in {@link GraphResponse#lists}</li>
 *   <li>{@code usages}: usages of {@code fqns[0]} by kind ({@code kind} as in find_usages) in {@code lists}</li>
 *   <li>{@code docs}: the method or class documents of {@code fqns} in {@link GraphResponse#docs}</li>
 *   <li>{@code called}: the stored called methods of each of {@code fqns} found on the shard, in
 *       {@code lists} (as {@code /internal/callees}, but without a pinned generation: a graph call's
 *       first contact)</li>
 *   <li>{@code callers}: raw candidates of the methods calling {@code fqns[0]} (top {@code limit}),
 *       scored with {@code globalStats} when given</li>
 *   <li>{@code path}: the shortest call path {@code fqns[0]} → {@code to}, as {@code lists["path"]}</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GraphRequest(String shard, Long generation, String op, List<String> fqns, String direction,
                           String kind, String to, int limit, com.pharos.search.distributed.GlobalStats globalStats) {

    public GraphRequest(String shard, Long generation, String op, List<String> fqns, String direction,
                        String kind, String to, int limit) {
        this(shard, generation, op, fqns, direction, kind, to, limit, null);
    }
}
