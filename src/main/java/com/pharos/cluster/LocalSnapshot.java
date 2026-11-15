package com.pharos.cluster;

import com.pharos.config.ProjectMeta;
import java.nio.file.Path;

/** A committed local shard, ready to be published to a replication backend. */
public record LocalSnapshot(ShardId shard, Path shardDir, ProjectMeta meta, String modelFingerprint) {}
