package com.pharos.embedding;

import com.pharos.config.IndexConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for generating vector embeddings from text.
 * Implemented by DjlEmbeddingProvider (DJL + ONNX) or NoOpEmbeddingProvider (disabled).
 */
public interface EmbeddingProvider {

    Logger log = LoggerFactory.getLogger(EmbeddingProvider.class);

    /**
     * Embed text into a float vector.
     *
     * @param text the input text (javadoc + signature + body for code)
     * @return float array of length dimensions(), or null on error
     */
    float[] embed(String text);

    /** Dimensionality of the embedding vectors. */
    int dimensions();

    /** Returns false if this is a NoOp provider (embeddings not configured). */
    boolean isAvailable();

    /** Factory: creates the appropriate provider based on config. */
    static EmbeddingProvider create(IndexConfig config) {
        if (config.getEmbeddingModelUrl() == null || config.getEmbeddingModelUrl().isBlank()) {
            log.info("No embedding model configured — vector search disabled. " +
                    "Set embeddingModelUrl in ~/.pharos/config.json to enable.");
            return new NoOpEmbeddingProvider();
        }
        try {
            return new DjlEmbeddingProvider(config.getEmbeddingModelUrl(), config.getEmbeddingDimensions(), config.getEmbeddingMaxTokens());
        } catch (Exception e) {
            log.warn("DJL embedding provider unavailable ({}), falling back to NoOp. Keyword search will still work.", e.getMessage());
            return new NoOpEmbeddingProvider();
        }
    }
}
