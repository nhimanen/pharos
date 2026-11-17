package com.pharos.cluster.membership;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * One reader's announcement in {@code catalog/readers/<node>.json}. {@code startedAt} is the
 * reader process's start (epoch millis, informational: it is a node clock). The age of a
 * heartbeat is not stored; see {@link ReaderRegistry.LiveReader}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                              Map<String, Object> readerCache, int inFlight, double qps, long startedAt) {

    ReaderHeartbeat with(boolean draining, long startedAt) {
        return new ReaderHeartbeat(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, startedAt);
    }
}
