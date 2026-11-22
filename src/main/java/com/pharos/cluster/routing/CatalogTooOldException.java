package com.pharos.cluster.routing;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.sharding.ProjectShards;

import java.util.List;

/**
 * The last good read of the shard catalog is older than {@code cluster.maxCatalogAgeSeconds}: a
 * query is not routed to other readers with it. {@link #lastKnown} is that catalog, so a reader can
 * still answer from the shards it holds and name the others.
 */
public class CatalogTooOldException extends CatalogUnavailableException {
    private final ProjectShards lastKnown;
    private final long ageSeconds;

    public CatalogTooOldException(String message, Throwable cause, List<ShardId> lastKnown, long ageSeconds) {
        this(message, cause, ProjectShards.identity(lastKnown), ageSeconds);
    }

    public CatalogTooOldException(String message, Throwable cause, ProjectShards lastKnown, long ageSeconds) {
        super(message, cause);
        this.lastKnown = lastKnown;
        this.ageSeconds = ageSeconds;
    }

    /** Every shard the last good read listed. */
    public List<ShardId> lastKnown() { return lastKnown.listed(); }

    /** The last good read as a whole: its project ↔ shards mapping too. */
    public ProjectShards lastKnownProjects() { return lastKnown; }

    /** Seconds since the last good read of the catalog. */
    public long ageSeconds() { return ageSeconds; }
}
