package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * One file of a published generation.
 *
 * @param role     {@code "lucene"}, {@code "graph"} or {@code "refs"}
 * @param name     local file name in the shard directory (the Lucene file name for {@code lucene})
 * @param key      object-store key holding the file's bytes
 * @param length   size in bytes
 * @param checksum lower-case hex CRC32: the Lucene footer checksum for {@code lucene} files, the
 *                 CRC32 of the whole uploaded (gzip) file otherwise
 */
@JsonPropertyOrder({"role", "name", "key", "length", "checksum"})
public record ManifestFile(String role, String name, String key, long length, String checksum) {
    public static final String LUCENE = "lucene";
    public static final String GRAPH = "graph";
    public static final String REFS = "refs";
}
