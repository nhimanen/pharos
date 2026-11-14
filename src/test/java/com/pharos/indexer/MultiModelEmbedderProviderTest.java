package com.pharos.indexer;

import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.embedding.NoOpEmbeddingProvider;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * `pharos embed --model X` must reuse the process's instance of model X instead of loading the
 * model again — previously once more per command, and once per project with --all.
 */
class MultiModelEmbedderProviderTest {

    private static EmbeddingProviderConfig cfg(String modelId, String type, String url) {
        EmbeddingProviderConfig c = new EmbeddingProviderConfig();
        c.setType(type);
        c.setModelId(modelId);
        c.setUrl(url);
        c.setDimensions(4);
        return c;
    }

    private record Stub(String modelId) implements EmbeddingProvider {
        @Override public float[] embed(String text) { return new float[4]; }
        @Override public int dimensions() { return 4; }
        @Override public boolean isAvailable() { return true; }
    }

    @Test
    void reusesTheAlreadyLoadedInstanceOfTheModel() {
        EmbeddingProvider loaded = new Stub("model-a");
        MultiModelEmbedder e = new MultiModelEmbedder(IndexConfig.defaults(), null, null, List.of(loaded));
        assertThat(e.providerFor(cfg("model-a", "djl", "hf://x/model-a"))).isSameAs(loaded);
    }

    @Test
    void aModelCreatedHereIsCreatedOnceAndReusedAcrossProjects() {
        MultiModelEmbedder e = new MultiModelEmbedder(IndexConfig.defaults(), null, null, List.of());
        // unreachable HTTP runtime → create() falls back to a NoOp without loading anything
        EmbeddingProviderConfig c = cfg("model-b", "openai", "http://127.0.0.1:1/v1");
        EmbeddingProvider first = e.providerFor(c);
        assertThat(e.providerFor(c)).isSameAs(first);
    }

    @Test
    void aLoadedButUnavailableModelIsReusedAndReportsUnavailable() {
        EmbeddingProvider failed = new NoOpEmbeddingProvider("model-c");
        MultiModelEmbedder e = new MultiModelEmbedder(IndexConfig.defaults(), null, null, List.of(failed));
        EmbeddingProvider p = e.providerFor(cfg("model-c", "djl", "hf://x/model-c"));
        assertThat(p).isSameAs(failed);
        assertThat(p.isAvailable()).isFalse();
    }
}
