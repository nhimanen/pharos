package com.pharos.embedding;

/**
 * No-op embedding provider used when DJL/ONNX is not configured, or when a
 * configured provider fails to initialise. Keyword search (BM25) still works;
 * vector and hybrid search degrade gracefully.
 *
 * <p>Carries the model id of the provider it stands in for, so per-model
 * bookkeeping (Lucene field names, {@link com.pharos.config.ProjectMeta} updates)
 * stays consistent even when the real provider is offline.
 */
public class NoOpEmbeddingProvider implements EmbeddingProvider {

    private final String modelId;

    public NoOpEmbeddingProvider() {
        this("noop");
    }

    public NoOpEmbeddingProvider(String modelId) {
        this.modelId = modelId == null ? "noop" : modelId;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public float[] embed(String text) {
        return null;
    }

    @Override
    public int dimensions() {
        return 0;
    }

    @Override
    public boolean isAvailable() {
        return false;
    }
}
