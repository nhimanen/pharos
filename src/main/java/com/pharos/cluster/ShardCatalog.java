package com.pharos.cluster;

import com.pharos.cluster.sharding.ProjectShards;

import java.io.IOException;
import java.util.List;

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
}
