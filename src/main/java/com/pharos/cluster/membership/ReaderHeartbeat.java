package com.pharos.cluster.membership;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

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
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                              Map<String, Object> readerCache, int inFlight, double qps, long startedAt,
                              String state, List<String> degradedShards) {

    public static final String WARMING = "warming";
    public static final String READY = "ready";
    public static final String DEGRADED = "degraded";
    public static final String DRAINING = "draining";

    public ReaderHeartbeat {
        if (draining) state = DRAINING;
        else if (state == null || state.isBlank()) state = READY;
        degradedShards = degradedShards == null ? List.of() : List.copyOf(degradedShards);
    }

    /** A heartbeat without health state: ready (or draining), nothing degraded. */
    public ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                           Map<String, Object> readerCache, int inFlight, double qps, long startedAt) {
        this(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt, null, null);
    }

    ReaderHeartbeat with(boolean draining, long startedAt) {
        return new ReaderHeartbeat(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt,
                state, degradedShards);
    }
}
