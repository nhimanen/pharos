package com.pharos.cluster;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Writer-only state a new shard owner needs to resume incrementally: the embedding caches
 * ({@code embed-cache*.{bin,meta}}) and {@code file-state.json}. Everything else a writer needs
 * (Lucene files, call graph, refs, the format stamp) is the published generation itself.
 *
 * <p>The publisher backs the state up after each successful publish; a new owner calls
 * {@link #restore} before its first index run. Writer state alone is never a base for an
 * incremental run: {@link #restore} first materialises the shard's {@code LATEST} generation and
 * only then overlays the backed-up state.
 */
public interface WriterStateStore {

    /** No writer state is kept (single node, readers): backup does nothing, restore finds nothing. */
    WriterStateStore NONE = new WriterStateStore() {
        @Override public void backup(ShardId shard, Generation gen, Path indexDir) {}

        @Override public Optional<Restored> restore(ShardId shard, Path indexDir) { return Optional.empty(); }
    };

    /**
     * What {@link #restore} put in the index dir.
     *
     * @param generation      the published generation the Lucene files and the graph are
     *                        ({@code LATEST})
     * @param stateGeneration the generation of the overlaid writer state ({@code file-state.json},
     *                        embedding caches); at most {@code generation}. When older, the file state
     *                        doesn't describe the documents: a file added since and deleted again, or
     *                        changed and reverted, looks unchanged against it. The caller must then run
     *                        a FULL index (the restored embedding cache still serves unchanged texts).
     */
    record Restored(Generation generation, Generation stateGeneration) {
        public Restored {
            java.util.Objects.requireNonNull(generation, "generation");
            java.util.Objects.requireNonNull(stateGeneration, "stateGeneration");
            if (stateGeneration.compareTo(generation) > 0) {
                throw new IllegalArgumentException("writer state " + stateGeneration.value()
                        + " is newer than generation " + generation.value());
            }
        }

        /** True when the file state is that of {@link #generation}: an incremental run can build on it. */
        public boolean stateMatches() {
            return stateGeneration.equals(generation);
        }
    }

    /**
     * Uploads the writer state of {@code indexDir} as the state of the just published generation
     * {@code gen}. The embedding caches are content-addressed and uploaded only when changed.
     *
     * @throws IOException when the backup could not be written (the publish itself has succeeded)
     */
    void backup(ShardId shard, Generation gen, Path indexDir) throws IOException;

    /**
     * Replaces {@code indexDir} with the shard's {@code LATEST} generation (Lucene files, the call
     * graph as a writer graph generation plus its pointer, refs, the format stamp), overlaid with
     * the newest writer-state backup at or below {@code LATEST}. A backup older than {@code LATEST}
     * still applies for its embedding caches, but its file state doesn't describe the restored
     * documents ({@link Restored#stateGeneration}): the next run must be a full index.
     *
     * <p>A missing, corrupt or unusable backup (or an unusable {@code LATEST} generation) is logged
     * as a WARN and returns empty with {@code indexDir} untouched: the caller then runs a full
     * index. It never leaves {@code indexDir} half replaced.
     *
     * <p><b>Precondition:</b> the project must not be open on this node: no cached
     * {@code LuceneIndexer} reader or writer, no open {@code CallGraph} or cached graph view on
     * {@code indexDir}, and no index run of it in progress. Restore swaps the whole directory: a
     * reader opened before keeps serving the replaced (deleted) files, and a writer opened before
     * would commit into them. The caller (the work loop) enforces this; {@code LuceneIndexer} has no
     * per-project close to call from here.
     *
     * @return the generation {@code indexDir} now holds ({@code LATEST}) and that of its writer
     *         state; empty when nothing was restored
     * @throws IOException when the store cannot be read (a transient failure: retry later rather
     *                     than paying for a full re-embed)
     */
    Optional<Restored> restore(ShardId shard, Path indexDir) throws IOException;

    // ---- split projects (ruling P6-R2) ------------------------------------------------------

    /**
     * Uploads the writer state of a split project's index dir once, as the state of its index run
     * {@code projectGeneration} ({@link com.pharos.cluster.objectstore.ShardManifest#projectGeneration}),
     * under {@code projects/<project>/writer-state/}. The default keeps nothing.
     *
     * @throws IOException when the backup could not be written (the publish itself has succeeded)
     */
    default void backupProject(String project, long projectGeneration, Path indexDir) throws IOException {}

    /**
     * {@link #restore} for a split project: replaces {@code indexDir} with every shard's {@code LATEST}
     * generation (each shard's Lucene files in its local shard dir), the project's call graph and refs,
     * and the newest project writer-state backup at or below the restored run. In the result,
     * {@code generation} is the newest {@code projectGeneration} of the restored shards; when the shards
     * are not all at that run (a publish that stopped between two shards), {@code stateGeneration} is
     * below it, so the caller runs a full index. Same failure rules and precondition as {@link #restore};
     * the default restores nothing.
     */
    default Optional<Restored> restoreProject(com.pharos.cluster.sharding.ShardPlan plan, Path indexDir)
            throws IOException {
        return Optional.empty();
    }
}
