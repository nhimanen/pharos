package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * The index of one writer-state backup ({@code writer-state/gen-N/index.json}), uploaded after
 * every file it lists: a backup without its index does not exist. Each file is listed with the
 * SHA-256 of its bytes, checked on restore.
 *
 * @param chunkingVersion {@code IndexVersions.CHUNKING_VERSION} of the writer that made the backup
 *                        (informational: the file state itself records it per file)
 */
@JsonPropertyOrder({"formatVersion", "shard", "generation", "createdAt", "writerNode", "chunkingVersion", "files"})
public record WriterStateIndex(int formatVersion, String shard, long generation, Instant createdAt,
                               String writerNode, int chunkingVersion, List<Entry> files) {

    public static final int FORMAT_VERSION = 1;

    /**
     * @param name   the file's name in the writer's index dir
     * @param key    the object holding its bytes (a {@code blobs/<sha256>} key for embedding caches)
     * @param sha256 lower-case hex SHA-256 of the bytes
     */
    @JsonPropertyOrder({"name", "key", "length", "sha256"})
    public record Entry(String name, String key, long length, String sha256) {}

    public WriterStateIndex {
        files = files == null ? List.of() : List.copyOf(files);
    }

    public byte[] toJson() throws IOException {
        return ShardManifest.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
    }

    public static WriterStateIndex read(byte[] json) throws IOException {
        return ShardManifest.MAPPER.readValue(json, WriterStateIndex.class);
    }
}
