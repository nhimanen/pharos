package com.pharos.cluster.sharding;

import com.pharos.cluster.ShardId;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * How one index run of a project splits its documents (ruling P6-R1: the project is the indexing
 * unit): the project's active spec and its shards, as the catalog lists them. One parse pass feeds
 * one Lucene writer per shard; {@link #assign} picks a file's shard.
 *
 * <p>{@link #single(String)} is the {@code [project]} plan of Phases 1–5: one shard whose id is the
 * project name, the project's {@code lucene/} dir, and the Phase 4 publish and writer-state paths,
 * all unchanged.
 *
 * @param project the cluster project name (also the local index dir name)
 * @param spec    the project's active spec
 * @param shards  the project's shards under {@code spec}, in catalog order; empty for the default
 *                spec, whose one shard is the project name (on a single node that name need not be a
 *                valid {@link ShardId}: only publishers that address a store require one)
 */
public record ShardPlan(String project, ShardSpec spec, List<ShardId> shards) {

    public ShardPlan {
        Objects.requireNonNull(project, "project");
        Objects.requireNonNull(spec, "spec");
        shards = List.copyOf(shards);
        if (spec.isDefault()) {
            if (!shards.isEmpty()) {
                throw new IllegalArgumentException("spec [project] lists no shards (its shard is the project): " + shards);
            }
        } else {
            if (shards.isEmpty()) throw new IllegalArgumentException("project '" + project + "' has no shards");
            if (new LinkedHashSet<>(shards).size() != shards.size()) {
                throw new IllegalArgumentException("project '" + project + "' lists a shard twice: " + shards);
            }
        }
    }

    /** The {@code [project]} plan: one shard, the project itself. Accepts any local project name. */
    public static ShardPlan single(String project) {
        return new ShardPlan(project, ShardSpec.PROJECT, List.of());
    }

    /**
     * The plan of {@code project} under the catalog mapping: its active spec and shards, or
     * {@link #single} when the catalog does not split it (unknown projects included).
     */
    public static ShardPlan of(ProjectShards mapping, String project) {
        ShardSpec spec = mapping.specOf(project);
        List<ShardId> shards = mapping.shardsOf(project);
        if (spec.isDefault() || shards.isEmpty()) return single(project);
        return new ShardPlan(project, spec, shards);
    }

    /** True when the project has its own spec with several (or keyed) shards, not {@code [project]}. */
    public boolean isSplit() {
        return !spec.isDefault();
    }

    /**
     * The shards a cluster addresses: {@link #shards} of a split plan, {@code [project]} (which must then be
     * a valid shard id) for the default one.
     */
    public List<ShardId> clusterShards() {
        return isSplit() ? shards : List.of(ShardId.ofProject(project));
    }

    /** The shards, as a set. */
    public Set<ShardId> shardSet() {
        return Set.copyOf(shards);
    }

    /**
     * The shard of a file of this project.
     *
     * @throws IllegalArgumentException when the spec maps the file to a shard the catalog does not list
     *         for the project (for example a new module under {@code [project, module]}): the project
     *         needs a reshard, which no index run does on its own
     */
    public ShardId assign(DocKeys keys) {
        if (!isSplit()) throw new IllegalStateException("a [project] plan has no shard routing");
        ShardId id = spec.assign(keys);
        if (!shards.contains(id)) {
            throw new IllegalArgumentException("file '" + keys.relativePath() + "' of project '" + project
                    + "' belongs to shard " + id + " under spec " + spec + ", which the catalog does not list for"
                    + " the project " + shards + "; reshard the project to add it");
        }
        return id;
    }
}
