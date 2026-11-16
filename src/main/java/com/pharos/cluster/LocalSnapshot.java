package com.pharos.cluster;

import com.pharos.config.ProjectMeta;
import java.nio.file.Path;

/**
 * A committed local project index, ready to be published to a replication backend. It carries
 * the plain project name: only publishers that address the shard in a store (the object-store
 * publisher) turn it into a {@link ShardId}, so single-node installs accept any directory name.
 */
public record LocalSnapshot(String project, Path shardDir, ProjectMeta meta, String modelFingerprint) {}
