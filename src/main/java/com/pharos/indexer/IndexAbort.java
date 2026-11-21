package com.pharos.indexer;

/**
 * Cooperative abort of one index run at a safe point.
 *
 * <p>An index run calls {@link #checkpoint()} between units of work (files parsed, embedding
 * chunks, documents written) and {@link #enterCommit()} right before its Lucene commit. A requested
 * abort makes the next checkpoint throw {@link IndexAbortedException}; the run then rolls its
 * {@code IndexWriter} back (uncommitted segment files are deleted, the previous commit stays the
 * live one), deletes the graph generation it was building, deletes its spool and temporary
 * embedding files, and leaves {@code file-state.json}, the graph pointer and the registry as they
 * were. The project is left exactly at its previous commit, still publishable.
 *
 * <p>Once the run has entered its commit ({@link #enterCommit()} returned), an abort comes too
 * late: {@link #request} returns false and the run finishes normally (commit, graph, file state,
 * publish), so a run is never half committed. The persistent embedding cache may keep vectors the
 * aborted run computed; it is content-addressed, so they are valid for the next run.
 *
 * <p>Used by the writer's graceful stop (a run that cannot finish within {@code cluster.drainSeconds})
 * and by the disk guard. Thread-safe; one instance per run.
 */
public final class IndexAbort {

    private String reason;      // guarded by this
    private boolean committing; // guarded by this

    /** A token nobody aborts (manual runs). Requests on it are ignored. */
    public static IndexAbort none() {
        return new IndexAbort();
    }

    /**
     * Requests an abort at the next safe point.
     *
     * @return true when the run will abort (or already did); false when it has entered its commit
     *         and will finish
     */
    public synchronized boolean request(String why) {
        if (committing) return false;
        if (reason == null) reason = why == null ? "aborted" : why;
        return true;
    }

    public synchronized boolean requested() {
        return reason != null;
    }

    /** True once the run passed its last safe point (it will commit and finish). */
    public synchronized boolean committing() {
        return committing;
    }

    /** @throws IndexAbortedException when an abort was requested */
    public synchronized void checkpoint() throws IndexAbortedException {
        if (reason != null) throw new IndexAbortedException(reason);
    }

    /**
     * The last safe point, right before the Lucene commit: throws when an abort was requested,
     * otherwise marks the run committing (later requests return false).
     */
    public synchronized void enterCommit() throws IndexAbortedException {
        checkpoint();
        committing = true;
    }
}
