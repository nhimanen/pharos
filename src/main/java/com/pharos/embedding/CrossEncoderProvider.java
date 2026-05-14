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

    public static final String DEFAULT_MODEL_ID   = "Xenova/ms-marco-MiniLM-L-6-v2";
    public static final String DEFAULT_ONNX_FILE  = "onnx/model_quantized.onnx";
    private static final int   MAX_LENGTH         = 512;
    private static final Path  MODEL_CACHE        =
            Path.of(System.getProperty("user.home"), ".djl.ai", "pharos");

    private final Model     model;
    private final Path      tokenizerFile;
    private final Predictor<String[], Float> predictor;

    public CrossEncoderProvider() throws IOException, ModelException {
        this(DEFAULT_MODEL_ID, DEFAULT_ONNX_FILE);
    }

    public CrossEncoderProvider(String modelId, String onnxFilename)
            throws IOException, ModelException {
        log.info("Loading cross-encoder: {} ({})", modelId, onnxFilename);
        tokenizerFile = ensureFile(modelId, "tokenizer.json", "tokenizer.json");
        Path onnxFile = ensureFile(modelId, onnxFilename, "model.onnx");

        model = Model.newInstance("cross-encoder", "OnnxRuntime");
        model.load(onnxFile.getParent(), onnxFile.getFileName().toString()
                .replace(".onnx", ""));

        predictor = model.newPredictor(
                new CrossEncoderTranslator(tokenizerFile.toAbsolutePath(), MAX_LENGTH));
        log.info("Cross-encoder loaded.");
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

    @Override
    public void close() {
        predictor.close();
        model.close();
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
