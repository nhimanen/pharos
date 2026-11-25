package com.pharos.cluster;

import java.util.Collection;
import java.util.Map;

/**
 * A {@link ReplicaSource} that can tell when a shard has a newer generation (the peer-pull
 * backend's {@code latest} long-poll). With one, {@link ReplicaSyncer} waits on {@link #awaitChange}
 * between passes instead of sleeping {@code pollInterval}.
 */
public interface LongPollingSource {

    /**
     * Blocks until one of {@code wanted} has a generation newer than both {@code served} and any
     * generation this method already reported, or until the source's refresh interval passed (the
     * caller then runs a pass anyway: catalog changes and retries).
     *
     * @throws InterruptedException when the syncer is closing
     */
    void awaitChange(Collection<ShardId> wanted, Map<ShardId, Generation> served) throws InterruptedException;

    /** Stops every outstanding long-poll. */
    void cancelPolls();
}
