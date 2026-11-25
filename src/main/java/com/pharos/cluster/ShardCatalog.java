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
     * One catalog read: the project mapping ({@link #projectShards}) and whether its listing is
     * authoritative, i.e. whether readers may drop the shards it does not list. Not authoritative: no
     * catalog object at all (may be transient), or entries this build cannot parse (a newer release's id
     * shape after a downgrade: keep serving rather than drop shards it cannot even name). A catalog that
     * exists and lists no shard is authoritative (its last project was removed).
     */
    record MappingRead(ProjectShards map, boolean authoritative) {}

    /**
     * The mapping and its authority from one read (integration carry I3b, the operator's empty-catalog
     * rule inside Phase 6's mapping). By default a listing is authoritative when it lists a shard (an empty
     * list may be a transient read); a catalog that can tell an absent catalog from one listing no shard
     * overrides this.
     */
    default MappingRead mappingRead() throws IOException {
        ProjectShards map = projectShards();
        return new MappingRead(map, !map.listed().isEmpty());
    }

    /**
     * The shards when the catalog says so authoritatively ({@link #mappingRead}), or empty when it cannot.
     */
    default Optional<List<ShardId>> authoritativeShards() throws IOException {
        MappingRead read = mappingRead();
        return read.authoritative() ? Optional.of(read.map().listed()) : Optional.empty();
    }

    /**
     * The raw ids of catalog entries this build cannot parse as a shard id (e.g. a newer release's keyed
     * ids), which {@link #shards()} skips. Empty by default.
     */
    default List<String> unparseableEntries() throws IOException {
        return List.of();
    }
}
