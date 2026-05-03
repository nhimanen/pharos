package com.pharos.embedding;

/**
 * No-op embedding provider used when DJL/ONNX is not configured.
 * Keyword search (BM25) still works; vector and hybrid search degrade gracefully.
 */
public class NoOpEmbeddingProvider implements EmbeddingProvider {

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
