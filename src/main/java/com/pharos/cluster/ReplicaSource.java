package com.pharos.cluster;

import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;

import java.io.IOException;
import java.io.InputStream;
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

    /** Downloads {@code length} bytes at {@code offset} of one manifest file to {@code target}. */
    void downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException;

    /**
     * Opens a stream over {@code length} bytes at {@code offset} of one manifest file (see
     * {@link com.pharos.cluster.objectstore.ObjectStore#openRange}); the caller checks it read exactly
     * {@code length} bytes.
     */
    InputStream openRange(ManifestFile file, long offset, long length) throws IOException;

    /** The cluster-wide synonyms file; empty when none was shipped. */
    Optional<byte[]> synonyms() throws IOException;
}
