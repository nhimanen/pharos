package com.pharos.embedding;

import ai.djl.Model;
import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.translate.TranslateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.nio.file.StandardCopyOption;

/**
 * Loads a cross-encoder ONNX model from HuggingFace Hub and scores
 * (query, document) pairs with a single relevance score in [0, 1].
 *
 * <p>Default model: {@code Xenova/ms-marco-MiniLM-L-6-v2} (quantized, ~23 MB).
 * Cached in {@code ~/.djl.ai/pharos/} after first download.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 *   try (CrossEncoderProvider ce = new CrossEncoderProvider()) {
 *       float score = ce.score("skip list", "reads skip lists with multiple levels");
 *   }
 * }</pre>
 */
public class CrossEncoderProvider implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CrossEncoderProvider.class);

    // Pre-configured model profiles
    public static final String MS_MARCO_MODEL_ID   = "Xenova/ms-marco-MiniLM-L-6-v2";
    public static final String MS_MARCO_L12_ID     = "Xenova/ms-marco-MiniLM-L-12-v2";
    public static final String BGE_MODEL_ID        = "Xenova/bge-reranker-base";
    // Note: cross-encoder/stsb-roberta-large uses RoBERTa BPE tokenizer format
    // not supported by DJL tokenizers library; ms-marco-L12 used as substitute.
    public static final String STS_MODEL_ID        = MS_MARCO_L12_ID;
    public static final String QUANTIZED_ONNX      = "onnx/model_quantized.onnx";
    public static final String FULL_ONNX           = "onnx/model.onnx";

    private static final int  MAX_LENGTH  = 512;
    private static final Path MODEL_CACHE =
            Path.of(System.getProperty("user.home"), ".djl.ai", "pharos");

    private final Model     model;
    private final Path      tokenizerFile;
    private final Predictor<String[], Float>       predictor;
    private final Predictor<List<String[]>, float[]> batchPredictor;
    private final String    label;

    /** ms-marco MiniLM-L-6 quantized (default). */
    public CrossEncoderProvider() throws IOException, ModelException {
        this(MS_MARCO_MODEL_ID, QUANTIZED_ONNX, true, "ms-marco-L6");
    }

    /**
     * @param modelId             HuggingFace model id (e.g. "Xenova/bge-reranker-base")
     * @param onnxFilename        relative path to ONNX file within the repo
     * @param includeTokenTypeIds true for BERT-based (ms-marco, BGE); false for RoBERTa (STS)
     * @param label               short display label for logging / test output
     */
    public CrossEncoderProvider(String modelId, String onnxFilename,
                                 boolean includeTokenTypeIds, String label)
            throws IOException, ModelException {
        this.label = label;
        log.info("Loading cross-encoder [{}]: {} ({})", label, modelId, onnxFilename);
        tokenizerFile = ensureFile(modelId, "tokenizer.json", "tokenizer.json");
        Path onnxFile = ensureFile(modelId, onnxFilename, "model.onnx");

        model = Model.newInstance("cross-encoder-" + label, "OnnxRuntime");
        model.load(onnxFile.getParent(), onnxFile.getFileName().toString()
                .replace(".onnx", ""));

        // Auto-detect whether model needs token_type_ids: try a dummy pair,
        // fall back to false if ONNX reports an input mismatch.
        boolean useTokenTypes = probeTokenTypeIds(tokenizerFile, includeTokenTypeIds);
        predictor = model.newPredictor(
                new CrossEncoderTranslator(
                        tokenizerFile.toAbsolutePath(), MAX_LENGTH, useTokenTypes));
        batchPredictor = model.newPredictor(
                new CrossEncoderBatchTranslator(
                        tokenizerFile.toAbsolutePath(), MAX_LENGTH, useTokenTypes));
        log.info("Cross-encoder [{}] loaded (token_type_ids={}).", label, useTokenTypes);
    }

    public String label() { return label; }

    /**
     * Normalizes a compound trigger term to space-separated form before scoring.
     *
     * <p>Handles three patterns common in mined synonyms:
     * <ul>
     *   <li>All-lowercase compound ("skiplist" → "skip list") — uses simple
     *       heuristic of finding known sub-words via common English prefixes
     *   <li>CamelCase decomposition is not applicable (miners lowercase everything)
     *   <li>Falls back to the raw term if no split is found
     * </ul>
     *
     * <p>For production quality, replace with a proper word-segmentation
     * algorithm backed by the index vocabulary.
     */
    public static String normalizeQuery(String term) {
        if (term == null || term.isBlank()) return term;
        // Insert spaces where a lowercase letter sequence ends and a new word likely starts.
        // We use a simple approach: try all split points, pick the one where both halves
        // are valid dictionary words (checked against a small built-in set of common
        // technical prefixes/words).  Fall back to raw term if no valid split found.
        String best = insertWordSpaces(term);
        return best.isBlank() ? term : best;
    }

    // ── Word space insertion ──────────────────────────────────────────────────

    private static final Set<String> COMMON_WORDS = new HashSet<>(java.util.Arrays.asList(
        "skip","list","block","max","weak","and","finite","state","machine","synonym",
        "expansion","compound","word","vector","space","stemmer","kd","tree","small",
        "world","taxonomy","index","facet","function","boost","best","field","ordered",
        "proximity","language","modeling","accent","strip","soft","deleted","hierarchical",
        "navigable","part","of","speech","impact","scoring","delta","pack","memory",
        "mapped","segment","flush","background","merge","char","folding","sparse","bits",
        "top","document","query","boosting","level","off","heap","mmap","per","doc",
        "numeric","column","store","bp","reorder","roaring","set","log","policy","values",
        "write","logistic","search","reader","writer","index","graph","builder","cache",
        "directory","analyzer","filter","collector","scorer","format","codec","provider",
        "manager","handler","iterator","comparator","classifier","parser","encoder",
        "decoder","tokenizer","indexer","searcher","strategy","factory","visitor"
    ));

    private static String insertWordSpaces(String s) {
        // Dynamic programming segmentation using COMMON_WORDS
        int n = s.length();
        int[] dp = new int[n + 1];
        String[] prev = new String[n + 1];
        java.util.Arrays.fill(dp, -1);
        dp[0] = 0;
        for (int i = 0; i < n; i++) {
            if (dp[i] < 0) continue;
            for (int j = i + 2; j <= n; j++) {
                String sub = s.substring(i, j);
                if (COMMON_WORDS.contains(sub) && dp[j] < 0) {
                    dp[j] = i;
                    prev[j] = sub;
                }
            }
        }
        if (dp[n] < 0) return s; // no full segmentation found
        java.util.Deque<String> parts = new java.util.ArrayDeque<>();
        int pos = n;
        while (pos > 0) {
            parts.addFirst(prev[pos]);
            pos = dp[pos];
        }
        return String.join(" ", parts);
    }

    /**
     * Returns a relevance score in [0, 1] for the given (query, document) pair.
     * Higher = more relevant.
     */
    public float score(String query, String document) {
        try {
            return predictor.predict(new String[]{query, document});
        } catch (TranslateException e) {
            log.warn("Cross-encoder scoring failed: {}", e.getMessage());
            return 0f;
        }
    }

    /**
     * Scores all (query, document) pairs in a single batched ONNX forward pass.
     * Substantially faster than calling {@link #score} in a loop.
     *
     * @param query     the search query (same for all documents)
     * @param documents the candidate documents to score
     * @return relevance scores in [0, 1], one per document, same order
     */
    public float[] scoreBatch(String query, List<String> documents) {
        if (documents.isEmpty()) return new float[0];
        try {
            List<String[]> pairs = documents.stream()
                    .map(doc -> new String[]{query, doc})
                    .toList();
            return batchPredictor.predict(pairs);
        } catch (TranslateException e) {
            log.warn("Cross-encoder batch scoring failed: {}", e.getMessage());
            return new float[documents.size()];
        }
    }

    @Override
    public void close() {
        predictor.close();
        batchPredictor.close();
        model.close();
    }

    // ── Token type ID probe ───────────────────────────────────────────────────

    /** Returns true if the model accepts token_type_ids, false if it doesn't. */
    private boolean probeTokenTypeIds(Path tokenizerPath, boolean preferInclude) {
        if (!preferInclude) return false;
        // Try a dummy inference with token_type_ids; if it fails, switch to false.
        try {
            var probe = model.newPredictor(
                    new CrossEncoderTranslator(tokenizerPath.toAbsolutePath(), MAX_LENGTH, true));
            probe.predict(new String[]{"test", "probe"});
            probe.close();
            return true;
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("Input mismatch")) {
                log.debug("Model [{}] does not accept token_type_ids — disabling.", label);
                return false;
            }
            return false;
        }
    }

    // ── Download helpers ──────────────────────────────────────────────────────

    private static Path ensureFile(String modelId, String remoteRelPath,
                                    String localFilename) throws IOException {
        String safeId  = modelId.replace('/', '-');
        Path   dir     = MODEL_CACHE.resolve(safeId);
        Files.createDirectories(dir);

        Path local = dir.resolve(localFilename);
        if (Files.exists(local) && Files.size(local) > 1024) return local;

        String url = "https://huggingface.co/" + modelId
                   + "/resolve/main/" + remoteRelPath;
        log.info("Downloading {} → {}", url, local);
        downloadFollowingRedirects(url, local);
        return local;
    }

    private static void downloadFollowingRedirects(String urlStr, Path dest)
            throws IOException {
        URL current = new URL(urlStr);
        for (int attempt = 0; attempt < 10; attempt++) {
            HttpURLConnection conn = (HttpURLConnection) current.openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestProperty("User-Agent", "pharos/1.0");
            int status = conn.getResponseCode();
            if (status == 200) {
                try (InputStream in = conn.getInputStream()) {
                    Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
                }
                return;
            } else if (status == 301 || status == 302 || status == 307 || status == 308) {
                String loc = conn.getHeaderField("Location");
                conn.disconnect();
                current = new URL(current, loc);
            } else {
                throw new IOException("HTTP " + status + " for " + urlStr);
            }
        }
        throw new IOException("Too many redirects: " + urlStr);
    }
}
