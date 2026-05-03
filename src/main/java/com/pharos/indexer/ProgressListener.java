package com.pharos.indexer;

/**
 * Callback for indexing progress updates.
 *
 * Callers receive the current stage name, a count of items processed so far,
 * and the total number of items in the current stage (0 if total is unknown).
 *
 * Implementations should be thread-safe and handle calls from any thread.
 */
@FunctionalInterface
public interface ProgressListener {

    /**
     * Called when progress advances.
     *
     * @param stage   human-readable stage name, e.g. "Parsing", "Building graph", "Indexing"
     * @param current number of items completed in this stage (0-based)
     * @param total   total items in this stage, or 0 if unknown
     */
    void onProgress(String stage, int current, int total);

    /** No-op listener — use when no progress reporting is needed. */
    ProgressListener SILENT = (stage, current, total) -> {};
}
