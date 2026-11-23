package com.pharos.indexer;

import com.pharos.cluster.ShardId;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The Lucene writers of one index run (ruling P6-R1: one parse pass feeds N shard writers).
 *
 * <p>A {@code [project]} run has exactly one writer, the project's own, and every call goes straight
 * to it: the Phases 1–5 behaviour. A split project's run has one writer per shard:
 * <ul>
 *   <li>{@link #addDocument} routes by the document's {@code filePath} ({@link FileShardRouter}), so
 *       all documents of a file land in the file's shard (its key under the current spec);</li>
 *   <li>{@link #deleteFile} deletes the file's documents from <b>every</b> shard. That includes the
 *       shard of the file's previous key, so a file whose key changed (moved to another module, or
 *       renamed onto another ring range) leaves its old shard in the same run that adds it to the new
 *       one, also when the previous key can no longer be computed (its module dir is gone). A delete
 *       term matching nothing in a shard costs a term lookup.</li>
 * </ul>
 * {@link #commit} commits the shards one after another, in plan order. A crash between two shard
 * commits leaves {@link LocalShardLayout#COMMITTING} behind, and the next run is a full one.
 */
final class ShardedWriter implements Closeable {

    private final Map<ShardId, IndexWriter> writers;
    /** The one writer of a {@code [project]} run; null for a split run. */
    private final IndexWriter single;
    private final FileShardRouter router;

    private ShardedWriter(IndexWriter single, Map<ShardId, IndexWriter> writers, FileShardRouter router) {
        this.single = single;
        this.writers = writers;
        this.router = router;
    }

    /** The {@code [project]} run: one writer, no routing ({@code keys} only gives documents their key fields). */
    static ShardedWriter single(IndexWriter writer, FileShardRouter keys) {
        return new ShardedWriter(writer, Map.of(), keys);
    }

    /** A split run over {@code writers} (one per shard of the plan, in plan order). */
    static ShardedWriter split(Map<ShardId, IndexWriter> writers, FileShardRouter router) {
        return new ShardedWriter(null, new LinkedHashMap<>(writers), router);
    }

    /**
     * Adds a document with its file's key fields ({@link DocumentMapper#addKeyFields}: module, path,
     * language, kind; what key filters match), to the shard of its file in a split run.
     */
    void addDocument(org.apache.lucene.document.Document doc) throws IOException {
        IndexableField path = null;
        for (IndexableField f : doc) {
            if (DocumentMapper.F_FILE_PATH.equals(f.name())) { path = f; break; }
        }
        if (router != null && path != null && path.stringValue() != null) {
            try {
                DocumentMapper.addKeyFields(doc, router.keysOf(path.stringValue()));
            } catch (IllegalArgumentException e) {
                if (single == null) throw e; // a split run can't place it either
                // a [project] document outside the project root: no key fields, filters never match it
            }
        }
        if (single != null) {
            single.addDocument(doc);
            return;
        }
        if (path == null || path.stringValue() == null) {
            throw new IllegalArgumentException("a document without " + DocumentMapper.F_FILE_PATH
                    + " cannot be routed to a shard");
        }
        IndexWriter w = writers.get(router.shardOf(path.stringValue()));
        if (w == null) throw new IllegalStateException("no writer for the shard of " + path.stringValue());
        w.addDocument(doc);
    }

    /** Deletes every document of the file at {@code filePath} (every shard of a split run). */
    void deleteFile(String filePath) throws IOException {
        Term t = new Term(DocumentMapper.F_FILE_PATH, filePath);
        if (single != null) {
            single.deleteDocuments(t);
            return;
        }
        for (IndexWriter w : writers.values()) w.deleteDocuments(t);
    }

    /**
     * Merges away every deleted document of the split run's writers (no deleted document is left: the
     * merge policy's allowance is set to 0 for this call). A reshard calls it on the shards it rewrites:
     * the moved files' documents it deleted from their old shards would otherwise stay in those shards'
     * term statistics (Lucene counts deleted documents in {@code docFreq} and {@code docCount} until a
     * merge drops them), so every moved file would be counted twice in the global BM25 statistics and
     * every score would shift (found by the Phase 6 gate: about 2 % after a 4 to 5 ring change).
     */
    void expungeDeletes(IndexAbort abort) throws IOException {
        if (single != null) throw new IllegalStateException("only a split run expunges deletes");
        for (IndexWriter w : writers.values()) {
            if (w.getConfig().getMergePolicy() instanceof TieredMergePolicy t) t.setForceMergeDeletesPctAllowed(0.0);
            // buffered term deletes are not counted by the merge policy until they are applied: an NRT
            // reader applies them (nothing is committed; a flush alone does not)
            try (org.apache.lucene.index.DirectoryReader applied = org.apache.lucene.index.DirectoryReader.open(w)) {
                // opened only to apply the deletes
            }
            // ruling P6-R18: the merges run in the background and the abort is polled meanwhile; an abort
            // throws, and the caller's rollback() stops the running merges and drops their output
            w.forceMergeDeletes(false);
            while (w.hasPendingMerges() || !w.getMergingSegments().isEmpty()) {
                abort.checkpoint();
                try {
                    Thread.sleep(MERGE_POLL_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while expunging deletes", e);
                }
            }
            abort.checkpoint();
        }
    }

    /** How often {@link #expungeDeletes} checks the abort while merges run. */
    static final long MERGE_POLL_MILLIS = 50;

    /** Commits every writer, in plan order. */
    void commit() throws IOException {
        if (single != null) {
            single.commit();
            return;
        }
        for (IndexWriter w : writers.values()) w.commit();
    }

    /** Rolls every writer back (and closes it): the previous commits stay live. */
    void rollback() throws IOException {
        if (single != null) {
            single.rollback();
            return;
        }
        IOException first = null;
        for (IndexWriter w : writers.values()) {
            try {
                w.rollback();
            } catch (IOException e) {
                if (first == null) first = e; else first.addSuppressed(e);
            }
        }
        if (first != null) throw first;
    }

    /** Closes every writer (none commits on close), the first failure thrown after all are closed. */
    @Override
    public void close() throws IOException {
        if (single != null) {
            single.close();
            return;
        }
        IOException first = null;
        for (IndexWriter w : writers.values()) {
            try {
                w.close();
            } catch (IOException | RuntimeException e) {
                IOException io = e instanceof IOException x ? x : new IOException(e);
                if (first == null) first = io; else first.addSuppressed(io);
            }
        }
        if (first != null) throw first;
    }

    /** Opens one writer per shard; on a failure, closes the ones already open. */
    static Map<ShardId, IndexWriter> openAll(List<ShardId> shards, Opener opener) throws IOException {
        Map<ShardId, IndexWriter> out = new LinkedHashMap<>();
        try {
            for (ShardId s : shards) out.put(s, opener.open(s));
            return out;
        } catch (IOException | RuntimeException e) {
            for (IndexWriter w : new ArrayList<>(out.values())) {
                try { w.close(); } catch (IOException | RuntimeException ignored) { /* the open failure wins */ }
            }
            throw e;
        }
    }

    @FunctionalInterface
    interface Opener {
        IndexWriter open(ShardId shard) throws IOException;
    }
}
