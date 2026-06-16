package com.pharos.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Configuration for a single embedding provider. Pharos supports running several
 * providers side-by-side so a project can carry vectors from multiple models in the
 * same Lucene document — search picks one at query time via
 * {@link IndexConfig#getSearchEmbeddingModel()}.
 *
 * <p>JSON shape in {@code ~/.pharos/config.json}:
 * <pre>{@code
 *   {
 *     "embeddingProviders": [
 *       {
 *         "type": "djl",
 *         "modelId": "jina-code-v2",
 *         "url": "hf://jinaai/jina-embeddings-v2-base-code",
 *         "dimensions": 768,
 *         "maxTokens": 512
 *       },
 *       {
 *         "type": "openai",
 *         "modelId": "qwen3-emb-1024",
 *         "url": "http://localhost:8080/v1",
 *         "model": "Qwen/Qwen3-Embedding-4B",
 *         "apiKeyEnv": "OPENAI_API_KEY",
 *         "dimensions": 1024,
 *         "batchSize": 32,
 *         "timeoutMillis": 60000
 *       }
 *     ],
 *     "searchEmbeddingModel": "qwen3-emb-1024"
 *   }
 * }</pre>
 *
 * <p>v1 caps {@code dimensions} at 1024 (Lucene 10 default codec limit). For
 * higher-dim models, use Matryoshka truncation — OpenAI honours the {@code dimensions}
 * request parameter natively; self-hosted MRL-trained models can be trimmed
 * client-side.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmbeddingProviderConfig {

    /** Maximum embedding dimensions supported by the Lucene 10 default KNN codec. */
    public static final int MAX_DIMENSIONS = 1024;

    /** Provider type — {@code "djl"} (local ONNX) or {@code "openai"} (HTTP). */
    private String type;

    /**
     * Stable identifier used to derive Lucene field names and persist per-project
     * embedding metadata. Choose a short, human-readable alias (e.g. {@code "jina-code-v2"},
     * {@code "qwen3-emb-1024"}). Must be unique within a config.
     */
    private String modelId;

    /**
     * For {@code djl}: the model URL ({@code hf://...} or DJL zoo {@code groupId:artifactId}).
     * For {@code openai}: the base URL of the OpenAI-compatible endpoint (ending in
     * {@code /v1} typically), e.g. {@code https://api.openai.com/v1} or
     * {@code http://localhost:8080/v1}.
     */
    private String url;

    /**
     * OpenAI-only: the model name passed in the request body
     * (e.g. {@code "text-embedding-3-large"}, {@code "Qwen/Qwen3-Embedding-4B"}).
     * Ignored for the {@code djl} type.
     */
    private String model;

    /**
     * OpenAI-only: name of the environment variable holding the API key
     * (e.g. {@code "OPENAI_API_KEY"}). If null or the env var is unset, the provider
     * sends no {@code Authorization} header — useful for self-hosted endpoints
     * without auth.
     */
    private String apiKeyEnv;

    /**
     * Embedding vector length. Must be {@code <= MAX_DIMENSIONS} (1024 for v1).
     * For OpenAI providers, this is sent as the {@code dimensions} request parameter
     * (Matryoshka truncation). For DJL providers, this is the native model dim.
     */
    private int dimensions;

    /** DJL-only: tokenizer truncation. Ignored for OpenAI providers. */
    private int maxTokens = 512;

    /**
     * Embedding batch size — number of input texts per call.
     * <ul>
     *   <li>DJL: chunk size for the in-process ONNX session (default 8).</li>
     *   <li>OpenAI: number of inputs per HTTP request (default 64). Hosted OpenAI
     *       caps at 2048; self-hosted servers may have lower limits.</li>
     * </ul>
     */
    private int batchSize;

    /** OpenAI-only: per-request timeout in milliseconds (default 60s). */
    private long timeoutMillis = 60_000L;

    /**
     * Number of concurrent worker threads processing embedding batches.
     * <ul>
     *   <li>DJL local: typically 1 — ONNX Runtime's intra-op parallelism already
     *       uses every CPU core for one inference. Extra threads contend for the
     *       same cores. Bump to 2-3 if you have many cores AND the model is
     *       small (jina-small etc).</li>
     *   <li>OpenAI HTTP: typically 3-8 — concurrent requests keep the remote
     *       GPU's batching window full and overlap network round-trip with
     *       compute. Tune to what the server can absorb without queueing.</li>
     * </ul>
     * Default 0 → resolved to 1 (DJL) or 4 (OpenAI).
     */
    private int embeddingThreads;

    public EmbeddingProviderConfig() {}

    /**
     * Validates the config. Called by {@link IndexConfig#load()} so misconfiguration
     * surfaces at startup instead of mid-index.
     *
     * @throws IllegalArgumentException on any invalid field combination
     */
    public void validate() {
        if (modelId == null || modelId.isBlank()) {
            throw new IllegalArgumentException("embeddingProvider.modelId is required");
        }
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException(
                    "embeddingProvider['" + modelId + "'].type is required (djl|openai)");
        }
        if (!"djl".equals(type) && !"openai".equals(type)) {
            throw new IllegalArgumentException(
                    "embeddingProvider['" + modelId + "'].type='" + type +
                    "' is not supported (use 'djl' or 'openai')");
        }
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException(
                    "embeddingProvider['" + modelId + "'].url is required");
        }
        if ("openai".equals(type) && (model == null || model.isBlank())) {
            throw new IllegalArgumentException(
                    "embeddingProvider['" + modelId + "'].model is required for type=openai " +
                    "(the model name passed in the request body)");
        }
        if (dimensions <= 0) {
            throw new IllegalArgumentException(
                    "embeddingProvider['" + modelId + "'].dimensions must be > 0");
        }
        if (dimensions > MAX_DIMENSIONS) {
            throw new IllegalArgumentException(
                    "embeddingProvider['" + modelId + "'].dimensions=" + dimensions +
                    " exceeds the Lucene 10 default codec limit of " + MAX_DIMENSIONS + ". " +
                    "Use Matryoshka truncation (OpenAI's 'dimensions' request parameter, or " +
                    "client-side trim for MRL-trained models) to bring this down to " +
                    MAX_DIMENSIONS + " or less.");
        }
    }

    // Getters and setters

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getModelId() { return modelId; }
    public void setModelId(String modelId) { this.modelId = modelId; }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }

    public String getApiKeyEnv() { return apiKeyEnv; }
    public void setApiKeyEnv(String apiKeyEnv) { this.apiKeyEnv = apiKeyEnv; }

    public int getDimensions() { return dimensions; }
    public void setDimensions(int dimensions) { this.dimensions = dimensions; }

    public int getMaxTokens() { return maxTokens; }
    public void setMaxTokens(int maxTokens) { this.maxTokens = maxTokens; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    /**
     * Resolved batch size — substitutes a sensible default when {@code batchSize}
     * is zero (unset). Override per provider in {@code config.json} when you
     * want to push throughput: jina on a fast CPU does fine at 32+, an OpenAI-
     * compatible endpoint over localhost can take 128+ depending on the
     * server's batching window.
     */
    public int resolvedBatchSize() {
        if (batchSize > 0) return batchSize;
        return "openai".equals(type) ? 64 : 8;
    }

    public long getTimeoutMillis() { return timeoutMillis; }
    public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }

    public int getEmbeddingThreads() { return embeddingThreads; }
    public void setEmbeddingThreads(int embeddingThreads) { this.embeddingThreads = embeddingThreads; }

    /** Resolved thread count — substitutes a sensible default when unset. */
    public int resolvedEmbeddingThreads() {
        if (embeddingThreads > 0) return embeddingThreads;
        return "openai".equals(type) ? 4 : 1;
    }
}
