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

    /** Heartbeat of reader {@code node}: {@code catalog/readers/<node>.json}. */
    public String readerKey(String node) { return readersPrefix() + node + ".json"; }

    public String readersPrefix() { return root + "catalog/readers/"; }

    /** Heartbeat of writer {@code node} (work-queue mode): {@code catalog/writers/<node>.json}. */
    public String writerKey(String node) { return writersPrefix() + node + ".json"; }

    public String writersPrefix() { return root + "catalog/writers/"; }

    /**
     * The shared state of {@code /cluster/status} (proposal hysteresis, first-seen backlog stamps):
     * {@code catalog/status/state.json}, updated by compare-and-set by every node.
     */
    public String statusStateKey() { return root + "catalog/status/state.json"; }

    public String synonymsKey() { return root + "catalog/synonyms.txt"; }

    /**
     * {@code shards/<shard>/}, the id URL-safe encoded ({@link ShardId#pathSegment()}): a project id
     * gives the Phase 1–5 key byte for byte. Every per-shard key below builds on it.
     */
    public String shardPrefix(ShardId shard) { return root + "shards/" + shard.pathSegment() + "/"; }

    public String latestKey(ShardId shard) { return shardPrefix(shard) + "LATEST"; }

    /** Writer lease of a shard (work-queue mode): {@code shards/<shard>/lease}. */
    public String leaseKey(ShardId shard) { return shardPrefix(shard) + "lease"; }

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

    /** Writer-only state of a shard (embedding caches, file state): {@code shards/<shard>/writer-state/}. */
    public String writerStatePrefix(ShardId shard) { return shardPrefix(shard) + "writer-state/"; }

    /** Per-generation writer-state files: {@code writer-state/gen-N/}. */
    public String writerStateGenPrefix(ShardId shard, Generation gen) {
        return writerStatePrefix(shard) + "gen-" + gen.value() + "/";
    }

    /** The index of a writer-state backup, written last: {@code writer-state/gen-N/index.json}. */
    public String writerStateIndexKey(ShardId shard, Generation gen) {
        return writerStateGenPrefix(shard, gen) + "index.json";
    }

    /** Content-addressed writer-state blob (embedding cache files): {@code writer-state/blobs/<sha256>}. */
    public String writerStateBlobKey(ShardId shard, String sha256Hex) {
        return writerStatePrefix(shard) + "blobs/" + sha256Hex;
    }

    // ---- project-level keys (split projects, Phase 6) ---------------------------------------

    /**
     * {@code projects/<project>/}: what belongs to a split project as a whole rather than to one shard
     * (ruling P6-R2): its call graph and refs per index run, and its writer state. The name is checked
     * as a project name ({@link ShardId#ofProject}) and encoded like a shard id (a project name encodes
     * to itself).
     */
    public String projectPrefix(String project) {
        return root + "projects/" + ShardId.ofProject(project).pathSegment() + "/";
    }

    /** The project's call graph of index run {@code projectGen}, content-named like {@link #graphKey}. */
    public String projectGraphKey(String project, long projectGen, String checksumHex) {
        return projectPrefix(project) + "graph/gen-" + projectGen + "@" + checksumHex + ".cgf.gz";
    }

    /** The project's unresolved refs of index run {@code projectGen}, content-named. */
    public String projectRefsKey(String project, long projectGen, String checksumHex) {
        return projectPrefix(project) + "refs/gen-" + projectGen + "@" + checksumHex + ".json.gz";
    }

    /** A split project's writer state: {@code projects/<project>/writer-state/} (once per project). */
    public String projectWriterStatePrefix(String project) {
        return projectPrefix(project) + "writer-state/";
    }

    /** The per-project reshard lease ({@code projects/<project>/reshard-lease}): one reshard job at a time. */
    public String reshardLeaseKey(String project) {
        return projectPrefix(project) + "reshard-lease";
    }

    /** The state of a project's reshard job ({@code projects/<project>/reshard/job.json}), for status and resume. */
    public String reshardJobKey(String project) {
        return projectPrefix(project) + "reshard/job.json";
    }
}
