package com.pharos.cluster.objectstore;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;

/** Key layout of a pharos cluster inside an object-store bucket, all under one {@code prefix}. */
public final class ObjectStoreLayout {

    private final String root;

    /** @param prefix key prefix inside the bucket; blank means the bucket root */
    public ObjectStoreLayout(String prefix) {
        String p = prefix == null ? "" : prefix.strip();
        while (p.startsWith("/")) p = p.substring(1);
        while (p.endsWith("/")) p = p.substring(0, p.length() - 1);
        this.root = p.isEmpty() ? "" : p + "/";
    }

    public String catalogKey() { return root + "catalog/shards.json"; }

    public String synonymsKey() { return root + "catalog/synonyms.txt"; }

    public String shardPrefix(ShardId shard) { return root + "shards/" + shard.value() + "/"; }

    public String latestKey(ShardId shard) { return shardPrefix(shard) + "LATEST"; }

    public String manifestKey(ShardId shard, Generation gen) {
        return shardPrefix(shard) + "manifests/" + manifestName(gen);
    }

    /** File name of a manifest as referenced from {@code LATEST}. */
    public static String manifestName(Generation gen) { return "gen-" + gen.value() + ".json"; }

    public String luceneKey(ShardId shard, String fileName, String checksumHex) {
        return shardPrefix(shard) + "lucene/" + fileName + "@" + checksumHex;
    }

    /**
     * Content-named per-generation graph: two runs racing for the same generation never overwrite
     * each other's bytes. The manifest's {@code key} is the source of truth for readers.
     */
    public String graphKey(ShardId shard, Generation gen, String checksumHex) {
        return shardPrefix(shard) + "graph/gen-" + gen.value() + "@" + checksumHex + ".cgf.gz";
    }

    /** Content-named per-generation unresolved refs; see {@link #graphKey}. */
    public String refsKey(ShardId shard, Generation gen, String checksumHex) {
        return shardPrefix(shard) + "refs/gen-" + gen.value() + "@" + checksumHex + ".json.gz";
    }
}
