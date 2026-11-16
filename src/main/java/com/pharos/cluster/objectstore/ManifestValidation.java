package com.pharos.cluster.objectstore;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;

import java.io.IOException;

/**
 * Checks applied to every manifest read back from the bucket before anything acts on it, shared by
 * {@link GenerationGc} (before deleting) and {@link ObjectStoreReplicaSource} (before downloading).
 */
public final class ManifestValidation {

    private ManifestValidation() {}

    /** A manifest that parsed leniently but cannot be trusted to describe its generation. */
    public static final class InvalidManifestException extends IOException {
        public InvalidManifestException(String message) { super(message); }
    }

    /**
     * Rejects an unsupported {@code formatVersion}, a {@code generation} or {@code shard} not
     * matching where the manifest was read from, an empty file list, and any file entry whose key
     * is blank or outside this shard's prefix.
     *
     * @param key the object key the manifest was read from (for messages)
     */
    public static void check(ShardId shard, Generation gen, ShardManifest m, ObjectStoreLayout layout, String key)
            throws InvalidManifestException {
        if (m == null) throw new InvalidManifestException("manifest " + key + " is empty");
        if (m.formatVersion() != ShardManifest.FORMAT_VERSION) {
            throw new InvalidManifestException("manifest " + key + " has unsupported formatVersion " + m.formatVersion());
        }
        if (m.generation() != gen.value()) {
            throw new InvalidManifestException("manifest " + key + " has generation " + m.generation()
                    + ", expected " + gen.value());
        }
        if (!shard.value().equals(m.shard())) {
            throw new InvalidManifestException("manifest " + key + " belongs to shard " + m.shard());
        }
        if (m.files().isEmpty()) throw new InvalidManifestException("manifest " + key + " lists no files");
        String prefix = layout.shardPrefix(shard);
        for (ManifestFile f : m.files()) {
            if (f == null || f.key() == null || f.key().isBlank() || !f.key().startsWith(prefix)) {
                throw new InvalidManifestException("manifest " + key + " has an invalid file entry " + f);
            }
        }
    }

    /** {@link #check} with the key derived from the layout. */
    public static void check(ShardId shard, Generation gen, ShardManifest m, ObjectStoreLayout layout)
            throws InvalidManifestException {
        check(shard, gen, m, layout, layout.manifestKey(shard, gen));
    }
}
