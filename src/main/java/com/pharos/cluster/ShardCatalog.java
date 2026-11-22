package com.pharos.cluster;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** The cluster's list of published shards. */
public interface ShardCatalog {
    List<ShardId> shards() throws IOException;

    /**
     * The shards when the catalog says so authoritatively, or empty when it cannot: readers drop
     * shards that are not listed only on a present listing. By default an empty list is not
     * authoritative (it may be a transient read); a catalog that can tell an absent catalog from one
     * listing no shard overrides this.
     */
    default Optional<List<ShardId>> authoritativeShards() throws IOException {
        List<ShardId> s = shards();
        return s.isEmpty() ? Optional.empty() : Optional.of(s);
    }
}
