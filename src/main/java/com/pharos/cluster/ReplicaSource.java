package com.pharos.cluster;

import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/** Where a reader fetches published shard generations from. */
public interface ReplicaSource {

    /** The generation {@code LATEST} points at, validated; empty when the shard has none yet. */
    Optional<Generation> latest(ShardId shard) throws IOException;

    /** The validated manifest of {@code gen}. */
    ShardManifest manifest(ShardId shard, Generation gen) throws IOException;

    /** Downloads one file of a manifest (by its {@code key}) to {@code target}. */
    void download(ManifestFile file, Path target) throws IOException;

    /** The cluster-wide synonyms file; empty when none was shipped. */
    Optional<byte[]> synonyms() throws IOException;
}
