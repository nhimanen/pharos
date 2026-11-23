package com.pharos.cluster;

import com.pharos.cluster.sharding.ProjectShards;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** The cluster's list of published shards. */
public interface ShardCatalog {
    List<ShardId> shards() throws IOException;

    /**
     * The project ↔ shards mapping of one catalog read ({@link ProjectShards}): which shards serve
     * a project and which project a shard belongs to. Every consumer resolves projects and shards
     * through it; nothing derives a project from a shard id. The default is the {@code [project]}
     * identity of {@link #shards()}, the whole truth for a catalog without spec versions (the peer
     * writers' catalog, P6-R9); the bucket's catalog and the coordinator's cache override it.
     */
    default ProjectShards projectShards() throws IOException {
        return ProjectShards.identity(shards());
    }

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

    /**
     * The raw ids of catalog entries this build cannot parse as a shard id (e.g. a newer release's keyed
     * ids), which {@link #shards()} skips. Empty by default.
     */
    default List<String> unparseableEntries() throws IOException {
        return List.of();
    }
}
