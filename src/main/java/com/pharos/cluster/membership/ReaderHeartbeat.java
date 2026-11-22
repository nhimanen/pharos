package com.pharos.cluster.membership;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

/**
 * One reader's announcement in {@code catalog/readers/<node>.json}. {@code startedAt} is the
 * reader process's start (epoch millis, informational: it is a node clock). The age of a
 * heartbeat is not stored; see {@link ReaderRegistry.LiveReader}.
 *
 * <p>{@code state} is the node's {@code /health} state: {@value #WARMING}, {@value #READY},
 * {@value #DEGRADED} (some shards can't be served fully: {@code degradedShards}; the node stays
 * routable for the others) or {@value #DRAINING}. A heartbeat written by an older reader has
 * neither field: it reads as {@value #READY} (or {@value #DRAINING}) with no degraded shards.
 *
 * <p>{@code generations} names the generation served of each shard, {@code prepared} the generations
 * materialised for a reshard's staged version but not served yet (Phase 6 Task 4): the router pins a
 * query to readers serving the generations its version is made of, and a reshard flips once the readers
 * have prepared the new version. Both are omitted when empty (an older reader's heartbeat has neither).
 *
 * <p>{@code warmP95Millis} is the p95 latency of the {@code /api/*} requests of the last minute
 * ({@link LoadCounter#warmP95Millis}); {@code catalogAgeSeconds} the age of the coordinator's cached
 * shard catalog. Both are nullable and omitted when null: a heartbeat from an older reader has
 * neither (and its "add readers" rule falls back to QPS only), a reader without requests in the last
 * minute has no p95, and one that never read the catalog has no age.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                              Map<String, Object> readerCache, int inFlight, double qps, long startedAt,
                              String state, List<String> degradedShards,
                              @JsonInclude(JsonInclude.Include.NON_NULL) Long warmP95Millis,
                              @JsonInclude(JsonInclude.Include.NON_NULL) Long catalogAgeSeconds,
                              @JsonInclude(JsonInclude.Include.NON_NULL) Boolean filters,
                              @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Long> generations,
                              @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Long> prepared) {

    public static final String WARMING = "warming";
    public static final String READY = "ready";
    public static final String DEGRADED = "degraded";
    public static final String DRAINING = "draining";

    /**
     * A heartbeat without generations (an older reader's): routing can't tell which generation it serves,
     * so it is assumed to serve whatever a query pins.
     */
    public ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                           Map<String, Object> readerCache, int inFlight, double qps, long startedAt,
                           String state, List<String> degradedShards, Long warmP95Millis, Long catalogAgeSeconds,
                           Boolean filters) {
        this(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt, state, degradedShards,
                warmP95Millis, catalogAgeSeconds, filters, null, null);
    }

    /**
     * The generation this reader serves of {@code shard}; empty when it doesn't say (an older reader) or
     * doesn't serve it.
     */
    public java.util.OptionalLong generationOf(String shard) {
        Long g = generations.get(shard);
        return g == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(g);
    }

    /** True when the heartbeat states served generations (a Phase 6 Task 4 reader). */
    public boolean statesGenerations() {
        return !generations.isEmpty();
    }

    /**
     * A heartbeat without the {@code filters} capability (key filters on {@code /internal} search, P6-R12):
     * an older reader's, which would ignore a filter it does not know and answer unfiltered.
     */
    public ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                           Map<String, Object> readerCache, int inFlight, double qps, long startedAt,
                           String state, List<String> degradedShards, Long warmP95Millis, Long catalogAgeSeconds) {
        this(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt, state, degradedShards,
                warmP95Millis, catalogAgeSeconds, null);
    }

    /** True when the reader applies key filters on {@code /internal} search requests. */
    public boolean supportsFilters() {
        return Boolean.TRUE.equals(filters);
    }

    public ReaderHeartbeat {
        if (draining) state = DRAINING;
        else if (state == null || state.isBlank()) state = READY;
        degradedShards = degradedShards == null ? List.of() : List.copyOf(degradedShards);
        generations = generations == null ? Map.of() : Map.copyOf(generations);
        prepared = prepared == null ? Map.of() : Map.copyOf(prepared);
    }

    /** A heartbeat without latency or catalog age (an older reader's). */
    public ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                           Map<String, Object> readerCache, int inFlight, double qps, long startedAt,
                           String state, List<String> degradedShards) {
        this(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt, state, degradedShards,
                null, null);
    }

    /** A heartbeat without health state: ready (or draining), nothing degraded. */
    public ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                           Map<String, Object> readerCache, int inFlight, double qps, long startedAt) {
        this(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt, null, null);
    }

    /** This heartbeat as a membership writes it: {@code draining} and the process start set. */
    public ReaderHeartbeat with(boolean draining, long startedAt) {
        return new ReaderHeartbeat(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt,
                state, degradedShards, warmP95Millis, catalogAgeSeconds, filters, generations, prepared);
    }
}
