package com.pharos.cluster.routing;

import com.pharos.cluster.ShardId;

import java.util.List;

/**
 * The last good read of the shard catalog is older than {@code cluster.maxCatalogAgeSeconds}: a
 * query is not routed to other readers with it. {@link #lastKnown} is that catalog, so a reader can
 * still answer from the shards it holds and name the others.
 */
public class CatalogTooOldException extends CatalogUnavailableException {
    private final List<ShardId> lastKnown;
    private final long ageSeconds;

    public CatalogTooOldException(String message, Throwable cause, List<ShardId> lastKnown, long ageSeconds) {
        super(message, cause);
        this.lastKnown = List.copyOf(lastKnown);
        this.ageSeconds = ageSeconds;
    }

    public List<ShardId> lastKnown() { return lastKnown; }

    /** Seconds since the last good read of the catalog. */
    public long ageSeconds() { return ageSeconds; }
}
