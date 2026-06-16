package com.pharos.indexer;

import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;

/**
 * Versioning constants for index-time field groups.
 *
 * <p>When the logic that produces a specific Lucene field changes, bump the corresponding
 * constant. The incremental index runner compares stored versions in {@code file-state.json}
 * against these constants and re-indexes every file whose recorded version differs —
 * skipping ONNX embedding calls wherever the persistent embedding cache still holds a
 * valid vector for unchanged text.
 *
 * <h3>How to use</h3>
 * <ol>
 *   <li>Change the chunking or embedding-text logic (e.g. in {@link DefaultChunker}).</li>
 *   <li>Increment {@link #CHUNKING_VERSION}.</li>
 *   <li>Run {@code ./pharos index <project>} — all files will be re-embedded; unchanged
 *       single-chunk methods are served from cache with zero ONNX cost.</li>
 * </ol>
 */
public final class IndexVersions {

    private IndexVersions() {}

    /**
     * Bump whenever {@link DefaultChunker}, {@link Chunker}, or any method that builds
     * per-chunk embedding texts changes in a way that affects the resulting float[].
     *
     * <p>Current changes per version:
     * <ul>
     *   <li>1 — initial tracking (method-style blank-line splitting with trailing overlap)</li>
     * </ul>
     */
    public static final int CHUNKING_VERSION = 1;

    /**
     * Fingerprint for a single embedding provider — includes everything that
     * influences the resulting float[] so two providers can't share a cache by
     * accident. Used both as the {@link PersistentEmbeddingCache} key and as
     * the per-file embed marker in {@link FileStateTracker}.
     */
    public static String modelFingerprint(EmbeddingProviderConfig cfg) {
        if (cfg == null) return "none";
        return cfg.getModelId() + ":" + cfg.getDimensions()
                + ":" + nvl(cfg.getUrl())
                + ":" + nvl(cfg.getModel());
    }

    /**
     * Legacy variant — returns the fingerprint of the first configured provider,
     * or the synthesized legacy provider if the user is mid-upgrade. Existing
     * callers that haven't been refactored to the multi-provider API still get
     * a meaningful per-config value.
     *
     * @deprecated pass an explicit {@link EmbeddingProviderConfig} instead.
     */
    @Deprecated
    public static String modelFingerprint(IndexConfig config) {
        if (config == null) return "none";
        if (!config.getEmbeddingProviders().isEmpty()) {
            return modelFingerprint(config.getEmbeddingProviders().get(0));
        }
        String url = config.getEmbeddingModelUrl() != null ? config.getEmbeddingModelUrl() : "none";
        return url + ":" + config.getEmbeddingDimensions();
    }

    private static String nvl(String s) { return s == null ? "" : s; }
}
