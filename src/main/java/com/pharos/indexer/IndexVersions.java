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
     * On-disk index format, stamped per project in {@code index-format.json}. This build serves
     * format N and N-1 ({@link IndexFormat#MIN_READABLE_FORMAT}); older or newer stamps are
     * refused until re-indexed. A missing stamp means format 0. On a bump, keep the N-1 read path
     * working and add the {@code compat/format-<N>.zip} fixture ({@code IndexCompatTest}). Bump this when
     * a change makes existing indexes unreadable or wrong (ids, schema, graph, cache layout);
     * bump {@link #CHUNKING_VERSION} instead when only embedding text changes and the
     * incremental run can re-embed in place.
     *
     * <p>History:
     * <ul>
     *   <li>1 — #18 Java ids, #23 script/doc ids, #24 call-graph index repair,
     *       #19 per-model embedding caches</li>
     * </ul>
     */
    public static final int INDEX_FORMAT = 1;

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
        String fp = cfg.getModelId() + ":" + cfg.getDimensions()
                + ":" + nvl(cfg.getUrl())
                + ":" + nvl(cfg.getModel());
        // Pooling/prefixes change the vectors, but default configs must keep their exact
        // historic fingerprint (no cache invalidation on upgrade) — so append only when set.
        com.pharos.config.Pooling pooling = cfg.getPooling();
        String qp = nvl(cfg.getQueryPrefix());
        String dp = nvl(cfg.getDocumentPrefix());
        if (pooling != com.pharos.config.Pooling.MEAN || !qp.isEmpty() || !dp.isEmpty()) {
            fp += ":pool=" + pooling.json() + ":qp=" + sha8(qp) + ":dp=" + sha8(dp);
        }
        return fp;
    }

    private static String sha8(String s) {
        try {
            byte[] h = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(h, 0, 4);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
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
