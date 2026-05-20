package com.pharos.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.config.EmbeddingProviderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Embedding provider that talks to any OpenAI-compatible {@code /v1/embeddings}
 * endpoint — covers hosted OpenAI, vLLM, llama.cpp server (llama-server),
 * LM Studio, and any other gateway that accepts the standard request/response
 * shape.
 *
 * <p>Request:
 * <pre>{@code
 *   POST <baseUrl>/embeddings
 *   {
 *     "model": "<model>",
 *     "input": ["text1", "text2", ...],
 *     "dimensions": <optional MRL truncation>,
 *     "encoding_format": "float"
 *   }
 * }</pre>
 *
 * <p>Response:
 * <pre>{@code
 *   {"data": [{"embedding": [...], "index": 0}, ...]}
 * }</pre>
 *
 * <p>The provider:
 * <ul>
 *   <li>Sends all texts in one HTTP call up to {@code batchSize}; chunks larger
 *       inputs internally.</li>
 *   <li>Sorts response items by {@code index} defensively (hosted OpenAI preserves
 *       order; some clones don't).</li>
 *   <li>Skips null/blank input slots and weaves null back into the output array
 *       at the same position.</li>
 *   <li>Retries up to 3 times on connect failures, 429 (rate-limit), or 5xx with
 *       exponential backoff.</li>
 *   <li>Sets {@code Authorization: Bearer <env>} only when {@code apiKeyEnv} is
 *       configured AND the env var is non-empty — useful for self-hosted
 *       endpoints without auth.</li>
 *   <li>Fires one test embedding on construction and asserts the response length
 *       matches {@code dimensions}; fails loud if the server doesn't honour the
 *       Matryoshka {@code dimensions} request parameter.</li>
 * </ul>
 */
public class OpenAiHttpEmbeddingProvider implements EmbeddingProvider {

    private static final Logger log = LoggerFactory.getLogger(OpenAiHttpEmbeddingProvider.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_RETRIES = 3;

    private final EmbeddingProviderConfig cfg;
    private final URI endpoint;
    private final HttpClient http;
    private final String authHeader;     // null when no auth configured
    private final int batchSize;
    private final Duration timeout;

    public OpenAiHttpEmbeddingProvider(EmbeddingProviderConfig cfg) {
        this.cfg = cfg;
        this.endpoint = URI.create(joinUrl(cfg.getUrl(), "embeddings"));
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.batchSize = cfg.resolvedBatchSize();
        this.timeout = Duration.ofMillis(cfg.getTimeoutMillis() > 0 ? cfg.getTimeoutMillis() : 60_000L);

        // Resolve auth header from env var, if configured. Missing env var is
        // legal (self-hosted unauthenticated endpoints) — log it but proceed.
        String env = cfg.getApiKeyEnv();
        if (env != null && !env.isBlank()) {
            String key = System.getenv(env);
            if (key != null && !key.isBlank()) {
                this.authHeader = "Bearer " + key.trim();
            } else {
                log.warn("Embedding provider '{}' configured apiKeyEnv='{}' but the env var is not set; " +
                        "sending requests without Authorization header.", cfg.getModelId(), env);
                this.authHeader = null;
            }
        } else {
            this.authHeader = null;
        }

        // Self-test: send one embedding to verify the endpoint works AND honours
        // our dimensions request. This catches the "server ignores MRL truncation"
        // failure mode at construction time rather than mid-index. embed() wraps
        // transport failures in RuntimeException; any failure here aborts
        // construction so the caller (EmbeddingProvider.create) can fall back to
        // NoOp instead of indexing with a half-initialised provider.
        float[] probe;
        try {
            probe = embed("pharos provider self-test");
        } catch (RuntimeException e) {
            throw new IllegalStateException("OpenAI embedding self-test failed for '"
                    + cfg.getModelId() + "': " + e.getMessage(), e);
        }
        if (probe == null) {
            throw new IllegalStateException(
                    "Self-test embed returned null for provider '" + cfg.getModelId() + "'");
        }
        if (probe.length != cfg.getDimensions()) {
            throw new IllegalStateException(String.format(
                    "Server returned %d-dim vectors but config requested dimensions=%d for " +
                    "provider '%s'. Either pick an MRL-capable model (e.g. text-embedding-3-small, " +
                    "Qwen3-Embedding-*) that honours the 'dimensions' request parameter, " +
                    "or set dimensions=%d in this provider's config.",
                    probe.length, cfg.getDimensions(), cfg.getModelId(), probe.length));
        }
        log.info("Embedding provider '{}' ready ({} dims via {} @ {})",
                cfg.getModelId(), probe.length, cfg.getModel(), endpoint);
    }

    @Override
    public String modelId() {
        return cfg.getModelId();
    }

    @Override
    public int dimensions() {
        return cfg.getDimensions();
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public float[] embed(String text) {
        if (text == null || text.isBlank()) return null;
        float[][] r = embedBatch(List.of(text));
        return r[0];
    }

    /**
     * Batch-embed. Null/blank input slots are skipped in the request and woven
     * back into the output array at the same position. Internal chunking
     * respects {@code batchSize} so callers can pass arbitrarily long lists.
     */
    @Override
    public float[][] embedBatch(List<String> texts) {
        if (texts == null || texts.isEmpty()) return new float[0][];
        float[][] out = new float[texts.size()][];

        // Chunk by batchSize. For each chunk, build a request that only contains
        // non-null/blank texts (with a parallel list of original indices to weave
        // results back in).
        for (int start = 0; start < texts.size(); start += batchSize) {
            int end = Math.min(start + batchSize, texts.size());
            List<String> nonBlank = new ArrayList<>(end - start);
            List<Integer> origIdx = new ArrayList<>(end - start);
            for (int i = start; i < end; i++) {
                String t = texts.get(i);
                if (t == null || t.isBlank()) continue;
                nonBlank.add(t);
                origIdx.add(i);
            }
            if (nonBlank.isEmpty()) continue;

            float[][] chunk;
            try {
                chunk = postEmbeddings(nonBlank);
            } catch (Exception e) {
                throw new RuntimeException("Embedding HTTP call failed for provider '"
                        + cfg.getModelId() + "': " + e.getMessage(), e);
            }
            for (int j = 0; j < chunk.length && j < origIdx.size(); j++) {
                out[origIdx.get(j)] = chunk[j];
            }
        }
        return out;
    }

    /**
     * One HTTP call: send {@code texts.size()} inputs, return that many vectors.
     * Retries on transient failures with exponential backoff.
     */
    private float[][] postEmbeddings(List<String> texts) throws IOException, InterruptedException {
        byte[] body = buildRequestBody(texts);
        IOException lastIo = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(endpoint)
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body));
            if (authHeader != null) b.header("Authorization", authHeader);

            HttpResponse<String> resp;
            try {
                resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            } catch (IOException ioe) {
                lastIo = ioe;
                if (attempt == MAX_RETRIES) throw ioe;
                sleepBackoff(attempt);
                continue;
            }

            int status = resp.statusCode();
            if (status >= 200 && status < 300) {
                return parseResponse(resp.body(), texts.size());
            }
            if (status == 429 || status >= 500) {
                if (attempt == MAX_RETRIES) {
                    throw new IOException("Embedding request failed after " + MAX_RETRIES +
                            " attempts: HTTP " + status + " — " + truncate(resp.body(), 500));
                }
                log.warn("Embedding HTTP {} from {} (attempt {}/{}); retrying.",
                        status, endpoint, attempt, MAX_RETRIES);
                sleepBackoff(attempt);
                continue;
            }
            // 4xx other than 429: don't retry, the request is malformed.
            throw new IOException("Embedding request rejected by " + endpoint + ": HTTP "
                    + status + " — " + truncate(resp.body(), 500));
        }
        throw new IOException("Embedding request exhausted retries", lastIo);
    }

    private byte[] buildRequestBody(List<String> texts) {
        ObjectNode root = JSON.createObjectNode();
        root.put("model", cfg.getModel());
        ArrayNode inputs = root.putArray("input");
        for (String t : texts) inputs.add(t);
        if (cfg.getDimensions() > 0) root.put("dimensions", cfg.getDimensions());
        root.put("encoding_format", "float");
        try {
            return JSON.writeValueAsBytes(root);
        } catch (Exception e) {
            // Jackson serialisation of a simple tree can't realistically fail —
            // wrap as unchecked so callers don't need to.
            throw new RuntimeException("Failed to serialise embedding request", e);
        }
    }

    private float[][] parseResponse(String body, int expectedCount) throws IOException {
        JsonNode root = JSON.readTree(body);
        JsonNode data = root.path("data");
        if (!data.isArray()) {
            throw new IOException("Embedding response has no 'data' array: " + truncate(body, 300));
        }
        // Defensive: sort by 'index' since not all OpenAI-compatible servers
        // preserve input order in the response.
        float[][] out = new float[expectedCount][];
        for (JsonNode item : data) {
            int idx = item.path("index").asInt(-1);
            if (idx < 0 || idx >= expectedCount) continue;
            JsonNode emb = item.path("embedding");
            if (!emb.isArray()) continue;
            float[] v = new float[emb.size()];
            for (int i = 0; i < emb.size(); i++) v[i] = (float) emb.get(i).asDouble();
            out[idx] = v;
        }
        return out;
    }

    private static String joinUrl(String base, String suffix) {
        if (base == null) return suffix;
        if (base.endsWith("/")) return base + suffix;
        return base + "/" + suffix;
    }

    private static void sleepBackoff(int attempt) throws InterruptedException {
        // 200ms, 400ms, 800ms — small total wait on a 3-attempt budget.
        Thread.sleep(200L * (1L << (attempt - 1)));
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }
}
