package com.pharos.cluster;

import com.pharos.cluster.sharding.ShardPlan;
import com.pharos.config.ProjectMeta;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A committed local project index, ready to be published to a replication backend. It carries
 * the plain project name: only publishers that address the shard in a store (the object-store
 * publisher) turn it into a {@link ShardId}, so single-node installs accept any directory name.
 *
 * @param shardDir the project's index dir; for a split project ({@link ShardPlan#isSplit}) it holds the
 *                 project-level state, and each shard's Lucene index is under
 *                 {@link com.pharos.indexer.LocalShardLayout#luceneDir}
 * @param plan     how the run split the project: {@link ShardPlan#single} for {@code [project]}
 */
public record LocalSnapshot(String project, Path shardDir, ProjectMeta meta, String modelFingerprint,
                            ShardPlan plan) {

    public LocalSnapshot {
        Objects.requireNonNull(plan, "plan");
        if (!plan.project().equals(project)) {
            throw new IllegalArgumentException("plan of project " + plan.project() + " for snapshot of " + project);
        }
    }

    /** A {@code [project]} snapshot (Phases 1–5). */
    public LocalSnapshot(String project, Path shardDir, ProjectMeta meta, String modelFingerprint) {
        this(project, shardDir, meta, modelFingerprint, ShardPlan.single(project));
    }
}
