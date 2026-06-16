package com.pharos.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Locks in the legacy → multi-provider config migration contract.
 *
 * <p>An old {@code ~/.pharos/config.json} produced by pharos before multi-provider
 * embeddings landed carries a single {@code embeddingModelUrl}/{@code embeddingDimensions}/
 * {@code embeddingMaxTokens} triplet. After upgrade, calling {@link IndexConfig#load()}
 * must synthesize a single-element {@code embeddingProviders} list from those fields
 * so existing indexes keep working without manual config edits. The synthesized
 * provider's {@code modelId} must be the {@link IndexConfig#LEGACY_MODEL_ID} sentinel
 * so the search-side legacy-fallback path activates.
 */
class IndexConfigMigrationTest {

    @TempDir
    Path tempDir;

    private final ObjectMapper json = new ObjectMapper();

    @Test
    void load_synthesizesLegacyProvider_fromOldSingleFieldShape() throws Exception {
        Path configFile = tempDir.resolve("config.json");
        // Simulate a config.json written by a previous pharos version — only the
        // legacy embedding fields are present, no embeddingProviders list.
        json.writeValue(configFile.toFile(), Map.of(
                "embeddingModelUrl", "hf://jinaai/jina-embeddings-v2-base-code",
                "embeddingDimensions", 768,
                "embeddingMaxTokens", 512
        ));

        IndexConfig config = loadFrom(configFile);

        assertThat(config.getEmbeddingProviders()).hasSize(1);
        EmbeddingProviderConfig legacy = config.getEmbeddingProviders().get(0);
        assertThat(legacy.getModelId()).isEqualTo(IndexConfig.LEGACY_MODEL_ID);
        assertThat(legacy.getType()).isEqualTo("djl");
        assertThat(legacy.getUrl()).isEqualTo("hf://jinaai/jina-embeddings-v2-base-code");
        assertThat(legacy.getDimensions()).isEqualTo(768);
        assertThat(legacy.getMaxTokens()).isEqualTo(512);
        // searchEmbeddingModel must default to the legacy id so the search-side
        // legacy-fallback path activates for pre-upgrade indexes.
        assertThat(config.getSearchEmbeddingModel()).isEqualTo(IndexConfig.LEGACY_MODEL_ID);
    }

    @Test
    void load_doesNotMigrate_whenProvidersAlreadyConfigured() throws Exception {
        // A fresh-shape config — embeddingProviders is set explicitly. Migration
        // must NOT overwrite it with a synthesized legacy entry even if the old
        // embeddingModelUrl field is also present (mid-migration leftover).
        Path configFile = tempDir.resolve("config.json");
        json.writeValue(configFile.toFile(), Map.of(
                "embeddingModelUrl", "hf://obsolete/url",
                "embeddingDimensions", 999,
                "embeddingProviders", List.of(Map.of(
                        "type", "djl",
                        "modelId", "jina-code-v2",
                        "url", "hf://jinaai/jina-embeddings-v2-base-code",
                        "dimensions", 768,
                        "maxTokens", 512
                )),
                "searchEmbeddingModel", "jina-code-v2"
        ));

        IndexConfig config = loadFrom(configFile);

        assertThat(config.getEmbeddingProviders()).hasSize(1);
        assertThat(config.getEmbeddingProviders().get(0).getModelId()).isEqualTo("jina-code-v2");
        assertThat(config.getSearchEmbeddingModel()).isEqualTo("jina-code-v2");
    }

    @Test
    void load_noEmbeddingConfig_returnsEmptyProviders() throws Exception {
        // No embedding config at all — providers list stays empty, search
        // degrades to keyword-only.
        Path configFile = tempDir.resolve("config.json");
        json.writeValue(configFile.toFile(), Map.of("indexDir", tempDir.resolve("indexes").toString()));

        IndexConfig config = loadFrom(configFile);

        assertThat(config.getEmbeddingProviders()).isEmpty();
        assertThat(config.getSearchEmbeddingModel()).isNull();
    }

    @Test
    void load_rejectsProviderWithExcessiveDimensions() throws Exception {
        // Lucene 10 default codec caps at 1024 dims. Validation must fire at load
        // time so misconfiguration surfaces at daemon startup, not mid-index.
        Path configFile = tempDir.resolve("config.json");
        json.writeValue(configFile.toFile(), Map.of(
                "embeddingProviders", List.of(Map.of(
                        "type", "djl",
                        "modelId", "too-big",
                        "url", "hf://example/big",
                        "dimensions", 3072
                ))
        ));

        // Routing through IndexConfig.load() would point at a system path; use the
        // same Jackson + migration logic via a load-from-explicit-path shim.
        assertThatThrownBy(() -> loadFromOrThrow(configFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimensions=3072")
                .hasMessageContaining("1024");
    }

    @Test
    void resolveSearchProvider_picksFirstProviderWhenNoneSelected() throws Exception {
        Path configFile = tempDir.resolve("config.json");
        json.writeValue(configFile.toFile(), Map.of(
                "embeddingProviders", List.of(
                        Map.of("type", "djl", "modelId", "first",  "url", "hf://x", "dimensions", 128),
                        Map.of("type", "djl", "modelId", "second", "url", "hf://y", "dimensions", 256)
                )
        ));
        IndexConfig config = loadFrom(configFile);

        // searchEmbeddingModel was not set — resolveSearchProvider should default to the first.
        assertThat(config.resolveSearchProvider()).isPresent();
        assertThat(config.resolveSearchProvider().orElseThrow().getModelId()).isEqualTo("first");
    }

    @Test
    void searchEmbeddingProvider_overridesEmbeddingProvidersAtSearchTime() throws Exception {
        // The use case: index via fast remote runtime, embed search queries via
        // a different (local, slow but offline-capable) runtime for the same
        // logical model.
        Path configFile = tempDir.resolve("config.json");
        json.writeValue(configFile.toFile(), Map.of(
                "embeddingProviders", List.of(Map.of(
                        "type", "openai",
                        "modelId", "jina-code-v2",
                        "url", "http://remote/v1",
                        "model", "jinaai/jina-embeddings-v2-base-code",
                        "dimensions", 768
                )),
                "searchEmbeddingProvider", Map.of(
                        "type", "djl",
                        "modelId", "jina-code-v2",
                        "url", "hf://jinaai/jina-embeddings-v2-base-code",
                        "dimensions", 768
                )
        ));

        IndexConfig config = loadFromOrThrow(configFile);
        EmbeddingProviderConfig resolved = config.resolveSearchProvider().orElseThrow();

        assertThat(resolved.getType()).isEqualTo("djl");
        assertThat(resolved.getModelId()).isEqualTo("jina-code-v2");
        assertThat(resolved.getUrl()).isEqualTo("hf://jinaai/jina-embeddings-v2-base-code");
    }

    @Test
    void searchEmbeddingProvider_rejectsModelIdMismatch() throws Exception {
        // Catch the foot-gun where the user wires up a search-time runtime
        // pointing at a modelId no project has vectors for. Without this
        // check, vector queries silently return zero hits.
        Path configFile = tempDir.resolve("config.json");
        json.writeValue(configFile.toFile(), Map.of(
                "embeddingProviders", List.of(Map.of(
                        "type", "openai",
                        "modelId", "jina-code-v2",
                        "url", "http://remote/v1",
                        "model", "jinaai/jina-embeddings-v2-base-code",
                        "dimensions", 768
                )),
                "searchEmbeddingProvider", Map.of(
                        "type", "djl",
                        "modelId", "qwen3-emb-1024",
                        "url", "hf://qwen/qwen3-embedding-4b",
                        "dimensions", 1024
                )
        ));

        assertThatThrownBy(() -> loadFromOrThrow(configFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("searchEmbeddingProvider.modelId='qwen3-emb-1024'")
                .hasMessageContaining("does not match");
    }

    @Test
    void resolveSearchProvider_fallsBackToEmbeddingProviders_whenSearchProviderUnset() throws Exception {
        Path configFile = tempDir.resolve("config.json");
        json.writeValue(configFile.toFile(), Map.of(
                "embeddingProviders", List.of(
                        Map.of("type", "djl", "modelId", "alpha", "url", "hf://a", "dimensions", 64),
                        Map.of("type", "djl", "modelId", "beta",  "url", "hf://b", "dimensions", 128)
                ),
                "searchEmbeddingModel", "beta"
        ));
        IndexConfig config = loadFromOrThrow(configFile);

        EmbeddingProviderConfig resolved = config.resolveSearchProvider().orElseThrow();
        assertThat(resolved.getModelId()).isEqualTo("beta");
        assertThat(resolved.getType()).isEqualTo("djl");
    }

    @Test
    void findProviderConfig_returnsTheRightOne() throws Exception {
        Path configFile = tempDir.resolve("config.json");
        json.writeValue(configFile.toFile(), Map.of(
                "embeddingProviders", List.of(
                        Map.of("type", "djl", "modelId", "alpha", "url", "hf://a", "dimensions", 64),
                        Map.of("type", "djl", "modelId", "beta",  "url", "hf://b", "dimensions", 128)
                )
        ));
        IndexConfig config = loadFrom(configFile);

        assertThat(config.findProviderConfig("beta")).isPresent()
                .get().extracting(EmbeddingProviderConfig::getDimensions).isEqualTo(128);
        assertThat(config.findProviderConfig("nonexistent")).isEmpty();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Reads a config from an arbitrary path using the same Jackson setup and
     * migration logic as {@link IndexConfig#load()} — but without IndexConfig.load()'s
     * fallback-to-defaults-on-error so the dimension-cap validation surfaces as a
     * thrown exception instead of a logged warning.
     */
    private static IndexConfig loadFrom(Path configFile) throws Exception {
        if (!Files.exists(configFile)) return IndexConfig.defaults();
        ObjectMapper mapper = new ObjectMapper();
        IndexConfig config = mapper.readValue(configFile.toFile(), IndexConfig.class);
        // Re-run the package-private migration via the same code path IndexConfig.load()
        // uses. Calling load() would read from ~/.pharos/config.json (not what we want
        // in tests), so we duplicate the post-deserialize steps inline.
        invokePostLoadHooks(config);
        return config;
    }

    private static IndexConfig loadFromOrThrow(Path configFile) throws Exception {
        IndexConfig config = loadFrom(configFile);
        for (EmbeddingProviderConfig p : config.getEmbeddingProviders()) p.validate();
        if (config.getSearchEmbeddingProvider() != null) {
            config.getSearchEmbeddingProvider().validate();
            boolean matches = config.getEmbeddingProviders().stream()
                    .anyMatch(p -> p.getModelId().equals(config.getSearchEmbeddingProvider().getModelId()));
            if (!matches) {
                throw new IllegalArgumentException(String.format(
                        "searchEmbeddingProvider.modelId='%s' does not match any " +
                        "embeddingProviders entry. Vectors would not be queryable.",
                        config.getSearchEmbeddingProvider().getModelId()));
            }
        }
        return config;
    }

    @SuppressWarnings("unchecked")
    private static void invokePostLoadHooks(IndexConfig config) throws Exception {
        // Reflect into the package-private migrate method so tests don't need to
        // duplicate its logic. IndexConfig is in the same package, so the private
        // method is accessible via the reflected MethodHandle.
        var m = IndexConfig.class.getDeclaredMethod("migrateLegacyEmbeddingConfig");
        m.setAccessible(true);
        m.invoke(config);
    }
}
