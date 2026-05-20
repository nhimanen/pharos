package com.pharos.embedding;

import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

/**
 * Interface for generating vector embeddings from text.
 * Implemented by {@link DjlEmbeddingProvider} (DJL + ONNX, local model) and
 * {@link OpenAiHttpEmbeddingProvider} (HTTP, remote model). {@link NoOpEmbeddingProvider}
 * is the sentinel for "vector search disabled".
 *
 * <p>Each provider carries a stable {@link #modelId()} string. The id is used both
 * to derive the Lucene field name where this provider's vectors are stored
 * ({@code vec.<sanitized-modelId>}) and to look up the right provider at search
 * time via {@link IndexConfig#getSearchEmbeddingModel()}.
 */
public interface EmbeddingProvider {

    Logger log = LoggerFactory.getLogger(EmbeddingProvider.class);

    /**
     * Stable identifier for this provider — chosen by the user in
     * {@code config.json}'s {@code embeddingProviders[].modelId}. Pharos uses
     * it for Lucene field naming, cache keys, and {@link com.pharos.config.ProjectMeta#getEmbeddedModels()}
     * tracking. Must be non-null and stable across restarts.
     */
    String modelId();

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

    /**
     * Build one provider from a single config entry. Errors during construction
     * are logged and converted to a {@link NoOpEmbeddingProvider} so the daemon
     * stays up even when one configured model is temporarily unreachable.
     * Indexing/embedding commands should check {@link #isAvailable()} on each
     * provider and fail loud rather than silently writing no vectors.
     */
    static EmbeddingProvider create(EmbeddingProviderConfig cfg) {
        try {
            switch (cfg.getType()) {
                case "djl":
                    return new DjlEmbeddingProvider(
                            cfg.getModelId(), cfg.getUrl(),
                            cfg.getDimensions(), cfg.getMaxTokens());
                case "openai":
                    return new OpenAiHttpEmbeddingProvider(cfg);
                default:
                    log.warn("Unknown embedding provider type '{}' for modelId='{}', falling back to NoOp.",
                            cfg.getType(), cfg.getModelId());
                    return new NoOpEmbeddingProvider(cfg.getModelId());
            }
        } catch (Throwable e) {
            log.warn("Embedding provider '{}' unavailable ({}: {}), falling back to NoOp. " +
                    "Keyword search will still work.",
                    cfg.getModelId(), e.getClass().getSimpleName(), e.getMessage());
            return new NoOpEmbeddingProvider(cfg.getModelId());
        }
    }

    /**
     * Build all providers declared in {@code config.embeddingProviders}, in order.
     * Returns an empty list when no providers are configured — callers that need
     * a working embedder should test the result and fall back to keyword-only
     * search.
     */
    static List<EmbeddingProvider> createAll(IndexConfig config) {
        List<EmbeddingProviderConfig> cfgs = config.getEmbeddingProviders();
        if (cfgs == null || cfgs.isEmpty()) {
            log.info("No embedding providers configured — vector search disabled. " +
                    "Add an entry to 'embeddingProviders' in ~/.pharos/config.json to enable.");
            return List.of();
        }
        List<EmbeddingProvider> out = new ArrayList<>(cfgs.size());
        for (EmbeddingProviderConfig cfg : cfgs) {
            out.add(create(cfg));
        }
        return out;
    }

    /**
     * Pick the provider for search-time query embedding. Honours
     * {@link IndexConfig#getSearchEmbeddingModel()}; if unset, falls back to the
     * first configured provider. Returns a {@link NoOpEmbeddingProvider} when
     * nothing is configured.
     */
    static EmbeddingProvider searchProvider(IndexConfig config) {
        Optional<EmbeddingProviderConfig> chosen = config.resolveSearchProvider();
        if (chosen.isEmpty()) return new NoOpEmbeddingProvider();
        return create(chosen.get());
    }

    /**
     * Legacy single-provider factory — kept so callers that still read the old
     * {@code embeddingModelUrl} shape from {@link IndexConfig} keep working
     * during the multi-provider migration.
     *
     * @deprecated use {@link #createAll(IndexConfig)} or {@link #searchProvider(IndexConfig)}
     */
    @Deprecated
    static EmbeddingProvider create(IndexConfig config) {
        return searchProvider(config);
    }
}
