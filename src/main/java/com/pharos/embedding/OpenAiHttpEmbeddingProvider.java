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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

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
    /**
     * Hard ceiling on individual input length. Anything longer is truncated at
     * request-build time (with a WARN log). This is a safety net for upstream
     * bugs in the embedding-text builders — chunker output is normally ≤ 8000
     * chars, but document-kind classes with huge javadocs or preambles have
     * been observed at 300k+ chars, which permanently overflows any server
     * regardless of batch sizing. 24 000 chars ≈ 20 000 tokens for code, which
     * fits a single slot in a 24k-token batch-size server config.
     */
    private static final int MAX_INPUT_CHARS = 24_000;

    private final EmbeddingProviderConfig cfg;
    private final URI endpoint;
    private final HttpClient http;
    private final String authHeader;     // null when no auth configured
    private final int configuredBatchSize;
    /**
     * Adaptive batch size. Starts at {@link #configuredBatchSize} and is halved on
     * detected overflow errors (4xx with token/length/context keywords). Never
     * drops below 1. Successful calls below this ceiling don't grow it back —
     * once the server's true ceiling is known, we stay under it for the
     * lifetime of this provider.
     */
    private final AtomicInteger effectiveBatchSize;
    private final Duration timeout;

    // Observability counters — surfaced via JMX-friendly getters in case we
    // later expose them; for now they back the periodic summary log line.
    private final LongAdder requests       = new LongAdder();
    private final LongAdder requestsOk     = new LongAdder();
    private final LongAdder requestsRetried= new LongAdder();
    private final LongAdder requests4xx    = new LongAdder();
    private final LongAdder requests5xx    = new LongAdder();
    private final LongAdder overflowSplits = new LongAdder();
    private final LongAdder totalLatencyMs = new LongAdder();

    public OpenAiHttpEmbeddingProvider(EmbeddingProviderConfig cfg) {
        this.cfg = cfg;
        this.endpoint = URI.create(joinUrl(cfg.getUrl(), "embeddings"));
        // HTTP/1.1 deliberately, not HTTP/2. With HTTP/2 multiplexing, a single
        // bad stream (e.g. server got into a degraded state after an overflow
        // recovery) can block all subsequent requests on the same connection
        // indefinitely — observed in the wild with llama-server. HTTP/1.1 uses
        // one connection per outstanding request; if a connection goes bad, it
        // dies cleanly and the next request gets a fresh one.
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.configuredBatchSize = cfg.resolvedBatchSize();
        this.effectiveBatchSize  = new AtomicInteger(configuredBatchSize);
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
        log.info("Embedding provider '{}' ready ({} dims via {} @ {}, batchSize={}, timeout={}ms, auth={})",
                cfg.getModelId(), probe.length, cfg.getModel(), endpoint,
                configuredBatchSize, timeout.toMillis(),
                authHeader == null ? "none" : "bearer");
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
     * respects the adaptive batch size; on detected overflow the chunk is split
     * recursively (bisection) so callers can pass arbitrarily long lists
     * without manual sizing.
     */
    @Override
    public float[][] embedBatch(List<String> texts) {
        if (texts == null || texts.isEmpty()) return new float[0][];
        float[][] out = new float[texts.size()][];

        int chunkSize = effectiveBatchSize.get();
        // Chunk by adaptive batch size. For each chunk, build a request that only
        // contains non-null/blank texts (with a parallel list of original indices
        // to weave results back in).
        for (int start = 0; start < texts.size(); start += chunkSize) {
            int end = Math.min(start + chunkSize, texts.size());
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
                chunk = postEmbeddingsAdaptive(nonBlank);
            } catch (Exception e) {
                log.error("Embedding HTTP call permanently failed for provider '{}' " +
                        "(batch={}, endpoint={}): {}",
                        cfg.getModelId(), nonBlank.size(), endpoint, e.getMessage(), e);
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
     * Posts a batch; on detected overflow (server rejects the batch as too
     * large in tokens/context/length) splits and recurses. Permanently lowers
     * {@link #effectiveBatchSize} so subsequent calls in this process avoid
     * the same overflow.
     */
    private float[][] postEmbeddingsAdaptive(List<String> texts) throws IOException, InterruptedException {
        try {
            return postEmbeddings(texts);
        } catch (OverflowException oe) {
            if (texts.size() == 1) {
                // A single text overflows the server's ctx — can't split further.
                // Surface as a hard failure with diagnostic info.
                log.error("Provider '{}': single text ({} chars) overflows server context; " +
                        "skipping this slot. Body excerpt: {}",
                        cfg.getModelId(), texts.get(0).length(), oe.serverBody);
                throw new IOException("Single-input overflow (text=" + texts.get(0).length() +
                        " chars) — server says: " + oe.serverBody, oe);
            }
            overflowSplits.increment();
            int newBatch = Math.max(1, texts.size() / 2);
            int prev = effectiveBatchSize.getAndUpdate(cur -> Math.min(cur, newBatch));
            log.warn("Provider '{}': batch of {} overflowed server (HTTP {}); splitting and " +
                    "downsizing effective batch from {} → {}. Server: {}",
                    cfg.getModelId(), texts.size(), oe.status, prev, newBatch, oe.serverBody);

            int mid = texts.size() / 2;
            float[][] left  = postEmbeddingsAdaptive(texts.subList(0, mid));
            float[][] right = postEmbeddingsAdaptive(texts.subList(mid, texts.size()));
            float[][] merged = new float[texts.size()][];
            System.arraycopy(left,  0, merged, 0,           left.length);
            System.arraycopy(right, 0, merged, left.length, right.length);
            return merged;
        }
    }

    /**
     * One HTTP call: send {@code texts.size()} inputs, return that many vectors.
     * Retries transient failures (connect errors, 429, 5xx) with exponential
     * backoff. Throws {@link OverflowException} on 4xx responses that look like
     * "batch / context too large" so the caller can bisect.
     */
    private float[][] postEmbeddings(List<String> texts) throws IOException, InterruptedException {
        byte[] body = buildRequestBody(texts);
        if (log.isDebugEnabled()) {
            int totalChars = 0;
            for (String t : texts) totalChars += t.length();
            log.debug("→ embed POST [{}] texts={} bytes={} chars={} endpoint={}",
                    cfg.getModelId(), texts.size(), body.length, totalChars, endpoint);
        }

        IOException lastIo = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            HttpRequest.Builder b = HttpRequest.newBuilder()
                    .uri(endpoint)
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body));
            if (authHeader != null) b.header("Authorization", authHeader);

            long startNs = System.nanoTime();
            HttpResponse<String> resp;
            try {
                resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            } catch (IOException ioe) {
                long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;
                lastIo = ioe;
                requests.increment();
                if (attempt == MAX_RETRIES) {
                    log.error("Provider '{}': transport failure after {} attempts ({} ms): {}",
                            cfg.getModelId(), MAX_RETRIES, latencyMs, ioe.toString());
                    throw ioe;
                }
                requestsRetried.increment();
                log.warn("Provider '{}': transport error on attempt {}/{} ({} ms): {} — retrying",
                        cfg.getModelId(), attempt, MAX_RETRIES, latencyMs, ioe.toString());
                sleepBackoff(attempt);
                continue;
            }

            long latencyMs = (System.nanoTime() - startNs) / 1_000_000L;
            int status = resp.statusCode();
            requests.increment();
            totalLatencyMs.add(latencyMs);

            if (status >= 200 && status < 300) {
                requestsOk.increment();
                if (log.isDebugEnabled()) {
                    log.debug("← embed OK   [{}] texts={} status={} {} ms bodyBytes={}",
                            cfg.getModelId(), texts.size(), status, latencyMs,
                            resp.body() == null ? 0 : resp.body().length());
                }
                return parseResponse(resp.body(), texts.size());
            }

            String bodyExcerpt = truncate(resp.body(), 1000);

            // Overflow check first — applies to BOTH 4xx and 5xx. llama-server
            // notably returns HTTP 500 with "is too large to process" when the
            // input exceeds its --batch-size limit, even though semantically
            // this is a client-side sizing issue, not a server failure.
            if (looksLikeOverflow(status, resp.body())) {
                if (status >= 500) requests5xx.increment(); else requests4xx.increment();
                // Don't retry — bubble up so the caller (postEmbeddingsAdaptive)
                // can bisect the batch. Logged there with split decision.
                throw new OverflowException(status, bodyExcerpt);
            }

            if (status >= 400 && status < 500 && status != 429) {
                requests4xx.increment();
                log.error("Provider '{}': HTTP {} rejection (batch={}, {} ms) — {}",
                        cfg.getModelId(), status, texts.size(), latencyMs, bodyExcerpt);
                throw new IOException("Embedding request rejected by " + endpoint +
                        ": HTTP " + status + " — " + bodyExcerpt);
            }
            // 429 or 5xx (non-overflow) — transient, retry with backoff.
            if (status == 429) requests4xx.increment(); else requests5xx.increment();
            if (attempt == MAX_RETRIES) {
                log.error("Provider '{}': HTTP {} after {} attempts (batch={}, {} ms) — {}",
                        cfg.getModelId(), status, MAX_RETRIES, texts.size(), latencyMs, bodyExcerpt);
                throw new IOException("Embedding request failed after " + MAX_RETRIES +
                        " attempts: HTTP " + status + " — " + bodyExcerpt);
            }
            requestsRetried.increment();
            log.warn("Provider '{}': HTTP {} on attempt {}/{} (batch={}, {} ms) — {} — retrying",
                    cfg.getModelId(), status, attempt, MAX_RETRIES, texts.size(),
                    latencyMs, bodyExcerpt);
            sleepBackoff(attempt);
        }
        throw new IOException("Embedding request exhausted retries", lastIo);
    }

    /**
     * Heuristic: does this response indicate the server rejected the request
     * because the batch / context / tokens exceeded its limit?
     *
     * <p>Applies to BOTH 4xx (hosted OpenAI, vLLM) and 5xx (llama-server returns
     * HTTP 500 with "is too large to process" — semantically a client sizing
     * issue but wrapped as a server error).
     *
     * <p>Different OpenAI-compatible servers phrase this differently — match
     * the common keywords. Errs on the side of "treat as overflow → bisect"
     * because:
     * <ul>
     *   <li>If we wrongly bisect a real bug, we waste a few requests and then
     *       fail with the same message at batch=1 — same outcome, slower.</li>
     *   <li>If we wrongly treat overflow as a hard error, we propagate, the
     *       whole indexing run dies, and the user has to manually shrink
     *       batchSize — that's the trap we just fell into.</li>
     * </ul>
     */
    private static boolean looksLikeOverflow(int status, String body) {
        if (status == 413) return true;  // Payload Too Large — unambiguous
        if (body == null) return false;
        String b = body.toLowerCase();
        // Phrasings observed in the wild:
        //   llama-server:     "input (N tokens) is too large to process. increase the physical batch size"
        //   hosted OpenAI:    "maximum context length is X tokens" / "exceeds the model's maximum context"
        //   vLLM:             "input length X exceeds maximum allowed N" / "prompt is too long"
        //   text-generation-inference: "input validation error: ... is too long"
        //   LM Studio:        "context window is full"
        return b.contains("too large to process")
            || b.contains("physical batch")
            || b.contains("batch size")
            || b.contains("context length")
            || b.contains("context_length")
            || b.contains("context size")
            || b.contains("context window")
            || b.contains("maximum context")
            || b.contains("too many tokens")
            || b.contains("token limit")
            || b.contains("too long")
            || b.contains("too large")
            || b.contains("is too long")
            || b.contains("exceeds")
            || b.contains("exceeded")
            || b.contains("n_ctx")
            || b.contains("n_batch")
            || b.contains("ctx exceeded");
    }

    /**
     * Internal signal that the server rejected the batch as too large. Caught
     * by {@link #postEmbeddingsAdaptive} which then bisects.
     */
    private static final class OverflowException extends IOException {
        final int status;
        final String serverBody;
        OverflowException(int status, String serverBody) {
            super("Server rejected batch as too large (HTTP " + status + "): " + serverBody);
            this.status = status;
            this.serverBody = serverBody;
        }
    }

    private byte[] buildRequestBody(List<String> texts) {
        ObjectNode root = JSON.createObjectNode();
        root.put("model", cfg.getModel());
        ArrayNode inputs = root.putArray("input");
        for (String t : texts) {
            if (t != null && t.length() > MAX_INPUT_CHARS) {
                log.warn("Provider '{}': truncating input from {} chars to {} chars " +
                        "(upstream chunker/builder produced over-long text — see DocumentMapper)",
                        cfg.getModelId(), t.length(), MAX_INPUT_CHARS);
                t = t.substring(0, MAX_INPUT_CHARS);
            }
            inputs.add(t);
        }
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
