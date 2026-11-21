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

        @Override public Optional<Generation> restore(ShardId shard, Path indexDir) { return Optional.empty(); }
    };

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
     * still applies: the files changed since are dirty for the next incremental run.
     *
     * <p>A missing, corrupt or unusable backup (or an unusable {@code LATEST} generation) is logged
     * as a WARN and returns empty with {@code indexDir} untouched: the caller then runs a full
     * index. It never leaves {@code indexDir} half replaced.
     *
     * @return the generation {@code indexDir} now holds ({@code LATEST}); empty when nothing was
     *         restored
     * @throws IOException when the store cannot be read (a transient failure: retry later rather
     *                     than paying for a full re-embed)
     */
    Optional<Generation> restore(ShardId shard, Path indexDir) throws IOException;
}
