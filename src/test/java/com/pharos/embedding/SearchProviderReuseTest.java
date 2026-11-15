package com.pharos.embedding;

import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The search-time provider must be the same instance as the indexing provider for that model.
 * Main used to build it a second time, loading a second copy of the ONNX model (~1.1 GB for
 * jina-v2-base-code) into every pharos process.
 */
class SearchProviderReuseTest {

    @Test
    void searchModelIsAnIndexingModel_reusesThatInstance() {
        IndexConfig config = configWith("model-a", "model-b");
        config.setSearchEmbeddingModel("model-b");
        EmbeddingProvider a = new Stub("model-a"), b = new Stub("model-b");

        assertThat(EmbeddingProvider.searchProvider(config, List.of(a, b))).isSameAs(b);
    }

    @Test
    void noSearchModelConfigured_reusesFirstIndexingProvider() {
        IndexConfig config = configWith("model-a", "model-b");
        EmbeddingProvider a = new Stub("model-a"), b = new Stub("model-b");

        assertThat(EmbeddingProvider.searchProvider(config, List.of(a, b))).isSameAs(a);
    }

    @Test
    void migratedLegacyConfig_reusesTheLegacyProvider() {
        IndexConfig config = configWith(IndexConfig.LEGACY_MODEL_ID);
        config.setSearchEmbeddingModel(IndexConfig.LEGACY_MODEL_ID);
        EmbeddingProvider legacy = new Stub(IndexConfig.LEGACY_MODEL_ID);

        assertThat(EmbeddingProvider.searchProvider(config, List.of(legacy))).isSameAs(legacy);
    }

    @Test
    void dedicatedSearchRuntime_isNotReplacedByTheIndexingInstance() {
        IndexConfig config = configWith("model-a");
        EmbeddingProviderConfig dedicated = provider("model-a");
        dedicated.setType("openai");
        dedicated.setUrl("http://127.0.0.1:1/v1");   // unreachable → falls back to NoOp, never loads ONNX
        config.setSearchEmbeddingProvider(dedicated);
        EmbeddingProvider indexing = new Stub("model-a");

        assertThat(EmbeddingProvider.searchProvider(config, List.of(indexing))).isNotSameAs(indexing);
    }

    @Test
    void nothingConfigured_isNoOp() {
        assertThat(EmbeddingProvider.searchProvider(IndexConfig.defaults(), List.of()))
                .isInstanceOf(NoOpEmbeddingProvider.class);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static IndexConfig configWith(String... modelIds) {
        IndexConfig config = IndexConfig.defaults();
        for (String id : modelIds) config.getEmbeddingProviders().add(provider(id));
        return config;
    }

    private static EmbeddingProviderConfig provider(String modelId) {
        EmbeddingProviderConfig c = new EmbeddingProviderConfig();
        c.setType("djl");
        c.setModelId(modelId);
        c.setUrl("hf://example/" + modelId);
        c.setDimensions(4);
        return c;
    }

    private record Stub(String modelId) implements EmbeddingProvider {
        @Override public float[] embed(String text) { return new float[4]; }
        @Override public float[][] embedBatch(List<String> texts) { return new float[texts.size()][4]; }
        @Override public int dimensions() { return 4; }
        @Override public boolean isAvailable() { return true; }
    }
}
