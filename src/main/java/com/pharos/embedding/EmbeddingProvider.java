package com.pharos.embedding;

import com.pharos.config.IndexConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

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

    /**
     * Embed multiple texts in one call, returning one vector per input.
     *
     * <p>Implementations that support batched ONNX inference should override this to
     * amortize the JNI/model-session overhead across the batch.  The default falls back
     * to individual {@link #embed} calls.
     *
     * @param texts list of input texts (null/blank entries yield a null result slot)
     * @return array of float vectors, same length as {@code texts}; individual slots may be null
     */
    default float[][] embedBatch(List<String> texts) {
        float[][] results = new float[texts.size()][];
        for (int i = 0; i < texts.size(); i++) {
            results[i] = embed(texts.get(i));
        }
        return results;
    }

    /**
     * Embeds all texts using length-sorted chunking for optimal ONNX throughput.
     *
     * Sorts by approximate text length (a proxy for token count) so sequences of similar
     * length land in the same chunk, minimising padding waste within each batch.
     * Results are returned in the original input order.
     *
     * @param texts        input texts (null/blank entries yield null result slots)
     * @param chunkSize    maximum number of texts per ONNX forward pass
     * @param onChunkDone  called after each chunk with the number of chunks completed so far;
     *                     null for no callback
     */
    default float[][] embedChunked(List<String> texts, int chunkSize, IntConsumer onChunkDone) {
        if (texts == null || texts.isEmpty()) return new float[0][];

        // Sort indices by text length — groups similar-length sequences into the same chunk
        // so padding within each batch is minimised.
        Integer[] order = IntStream.range(0, texts.size()).boxed().toArray(Integer[]::new);
        Arrays.sort(order, Comparator.comparingInt(i -> {
            String t = texts.get(i);
            return t == null ? 0 : t.length();
        }));

        float[][] results = new float[texts.size()][];
        int chunksDone = 0;
        for (int start = 0; start < order.length; start += chunkSize) {
            int end = Math.min(start + chunkSize, order.length);
            List<String> chunk = new ArrayList<>(end - start);
            for (int j = start; j < end; j++) {
                chunk.add(texts.get(order[j]));
            }
            float[][] chunkResults = embedBatch(chunk);
            for (int j = start; j < end; j++) {
                results[order[j]] = chunkResults[j - start];
            }
            if (onChunkDone != null) onChunkDone.accept(++chunksDone);
        }
        return results;
    }

    /** Embeds all texts using length-sorted chunking. No progress callback. */
    default float[][] embedChunked(List<String> texts, int chunkSize) {
        return embedChunked(texts, chunkSize, null);
    }

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
        } catch (Throwable e) {
            log.warn("DJL embedding provider unavailable ({}: {}), falling back to NoOp. Keyword search will still work.",
                    e.getClass().getSimpleName(), e.getMessage());
            return new NoOpEmbeddingProvider();
        }
    }
}
