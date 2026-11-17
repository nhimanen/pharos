package com.pharos.cluster.membership;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * One reader's announcement in {@code catalog/readers/<node>.json}. {@code ageSeconds} is not
 * stored: it is filled in on read from the object's LastModified (the bucket's clock).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReaderHeartbeat(String node, String advertiseUrl, List<String> shards, boolean draining,
                              Map<String, Object> readerCache, int inFlight, double qps, long ageSeconds) {

    public ReaderHeartbeat withAge(long age) {
        return new ReaderHeartbeat(node, advertiseUrl, shards, draining, readerCache, inFlight, qps, age);
    }
}
