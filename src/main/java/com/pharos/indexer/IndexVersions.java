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
     *   <li>2 — Haskell body extraction now walks past the {@code name :: Type} signature
     *       to the sibling equation clause(s) instead of stopping at the signature line</li>
     *   <li>3 — added Terraform (.tf) parsing via TerraformCodeParser</li>
     *   <li>4 — added Go (.go) parsing via GoCodeParser</li>
     * </ul>
     */
    public static final int CHUNKING_VERSION = 4;

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

    /**
     * Fingerprints that earlier pharos versions wrote for the <em>same</em> model as {@code cfg}.
     *
     * <p>Before multi-provider embeddings (commit 1715a31) the fingerprint was
     * {@code embeddingModelUrl + ":" + dimensions}; it is now
     * {@code modelId:dims:url:model}. Same weights, same dims, different string — so every
     * existing embedding cache and file-state entry looked stale after upgrading, and the
     * next re-index re-embedded the whole project. Caches and trackers accept these aliases
     * and rewrite them in the current format on the next save.
     */
    public static java.util.List<String> legacyFingerprints(EmbeddingProviderConfig cfg) {
        if (cfg == null || cfg.getUrl() == null || cfg.getUrl().isBlank() || cfg.getDimensions() <= 0) {
            return java.util.List.of();
        }
        return java.util.List.of(cfg.getUrl() + ":" + cfg.getDimensions());
    }

    private static String nvl(String s) { return s == null ? "" : s; }
}
