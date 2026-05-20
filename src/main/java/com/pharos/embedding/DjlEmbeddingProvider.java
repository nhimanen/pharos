package com.pharos.embedding;

import ai.djl.Model;
import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Embedding provider using Deep Java Library (DJL) with ONNX Runtime backend.
 *
 * Supported modelUrl formats:
 *
 *   "hf://jinaai/jina-embeddings-v2-base-code"
 *        — downloads the ONNX model from HuggingFace Hub, caches in ~/.djl.ai/pharos/,
 *          and loads it with a custom SentenceTransformerTranslator (mean pooling).
 *          Set embeddingDimensions: 768.
 *
 *   "ai.djl.huggingface.onnxruntime:sentence-transformers/all-MiniLM-L6-v2"
 *        — groupId:artifactId, looks up the DJL model zoo (limited curated set).
 *          Set embeddingDimensions: 384.
 */
public class DjlEmbeddingProvider implements EmbeddingProvider {

    private static final Logger log = LoggerFactory.getLogger(DjlEmbeddingProvider.class);
    private static final int MAX_INPUT_LENGTH = 32_000;
    /** Recreate the ONNX Runtime predictor after this many embed() calls to release workspace memory. */
    private static final int PREDICTOR_RESET_INTERVAL = 500;

    // Cache dir for downloaded ONNX models
    private static final Path MODEL_CACHE =
            Path.of(System.getProperty("user.home"), ".djl.ai", "pharos");

    /** Stable identifier from the provider config — used for Lucene field naming. */
    private final String modelId;
    /** Non-null for zoo models (has a no-arg newPredictor()); null for HF models. */
    private final ZooModel<String, float[]> zooModel;
    private final Model hfModel;
    private final Path tokenizerFile;
    private final int dimensions;
    private final int maxTokens;

    /**
     * One Predictor per thread — DJL Predictor is not thread-safe.
     * Each thread that calls embed() gets its own ONNX Runtime inference session.
     */
    private final ThreadLocal<Predictor<String, float[]>> threadPredictor =
            ThreadLocal.withInitial(this::newPredictor);

    /**
     * Batch predictor: runs multiple texts through the model in a single forward pass.
     * Only created for HF models (custom translator); null for zoo models.
     */
    private final ThreadLocal<Predictor<List<String>, float[][]>> threadBatchPredictor =
            ThreadLocal.withInitial(this::newBatchPredictor);

    /**
     * Per-thread embed call counter used for periodic predictor reset.
     * int[] so the value can be mutated inside the ThreadLocal lambda.
     */
    private final ThreadLocal<int[]> threadEmbedCount =
            ThreadLocal.withInitial(() -> new int[]{0});

    public DjlEmbeddingProvider(String modelId, String modelUrl, int dimensions, int maxTokens)
            throws ModelException, IOException {
        log.info("Loading embedding model '{}' from {} (maxTokens={})",
                modelId, modelUrl, maxTokens);
        this.modelId = modelId;
        this.maxTokens = maxTokens;
        this.dimensions = dimensions;

        if (modelUrl.startsWith("hf://")) {
            this.tokenizerFile = prepareHuggingFaceFiles(modelUrl.substring("hf://".length()));
            this.hfModel = loadHuggingFaceModel(tokenizerFile.getParent());
            this.zooModel = null;
        } else {
            this.tokenizerFile = null;
            this.hfModel = null;
            this.zooModel = loadZooModel(modelUrl);
        }

        log.info("Embedding model '{}' loaded ({} dimensions)", modelId, dimensions);
    }

    /**
     * Legacy 3-arg constructor — derives a model id from the URL for callers
     * that haven't been migrated to the multi-provider API yet.
     *
     * @deprecated use {@link #DjlEmbeddingProvider(String, String, int, int)} with
     *             an explicit modelId from the provider config.
     */
    @Deprecated
    public DjlEmbeddingProvider(String modelUrl, int dimensions, int maxTokens)
            throws ModelException, IOException {
        this(deriveLegacyModelId(modelUrl), modelUrl, dimensions, maxTokens);
    }

    private static String deriveLegacyModelId(String modelUrl) {
        if (modelUrl == null || modelUrl.isBlank()) return "djl";
        String s = modelUrl;
        if (s.startsWith("hf://")) s = s.substring("hf://".length());
        int slash = s.lastIndexOf('/');
        if (slash >= 0) s = s.substring(slash + 1);
        int colon = s.indexOf(':');
        if (colon >= 0) s = s.substring(0, colon);
        return s.isEmpty() ? "djl" : s;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    /** Creates a fresh Predictor from the already-loaded model. Used on construction and periodic reset. */
    private Predictor<String, float[]> newPredictor() {
        if (zooModel != null) {
            return zooModel.newPredictor();
        }
        SentenceTransformerTranslator translator =
                new SentenceTransformerTranslator(tokenizerFile.toAbsolutePath(), maxTokens);
        return hfModel.newPredictor(translator);
    }

    /**
     * Creates a fresh batch predictor (HF models only).
     * Zoo models don't expose a parameterless newPredictor for custom translators,
     * so we return null and fall back to sequential in {@link #embedBatch}.
     */
    @SuppressWarnings("unchecked")
    private Predictor<List<String>, float[][]> newBatchPredictor() {
        if (hfModel == null) return null;
        BatchSentenceTransformerTranslator translator =
                new BatchSentenceTransformerTranslator(tokenizerFile.toAbsolutePath(), maxTokens);
        return (Predictor<List<String>, float[][]>) hfModel.newPredictor(translator);
    }

    /**
     * Downloads the ONNX model and tokenizer from HuggingFace Hub (cached) and returns
     * the local path to tokenizer.json.
     */
    private Path prepareHuggingFaceFiles(String modelId) throws IOException {
        String base = "https://huggingface.co/" + modelId + "/resolve/main/";

        // Sanitise modelId for use as a directory name (replace / with -)
        Path modelDir = MODEL_CACHE.resolve(modelId.replace('/', '-'));
        Files.createDirectories(modelDir);

        Path onnxFile       = modelDir.resolve("model.onnx");
        Path tokenizerPath  = modelDir.resolve("tokenizer.json");
        Path configJsonFile = modelDir.resolve("config.json");

        if (!Files.exists(onnxFile)) {
            log.info("Downloading ONNX model from {}onnx/model.onnx ...", base);
            downloadWithRedirects(base + "onnx/model.onnx", onnxFile);
        }
        if (!Files.exists(tokenizerPath)) {
            log.info("Downloading tokenizer from {}tokenizer.json ...", base);
            downloadWithRedirects(base + "tokenizer.json", tokenizerPath);
        }
        if (!Files.exists(configJsonFile)) {
            log.info("Downloading model config from {}config.json ...", base);
            downloadWithRedirects(base + "config.json", configJsonFile);
        }
        return tokenizerPath;
    }

    /**
     * Loads an arbitrary HuggingFace ONNX model from a local model directory.
     * The model object is retained so predictors can be recreated on reset.
     */
    private static Model loadHuggingFaceModel(Path modelDir) throws ModelException, IOException {
        Model hfModel = Model.newInstance("model", "OnnxRuntime");
        hfModel.load(modelDir, "model");
        return hfModel;
    }

    /** Downloads a URL to a local file, following HTTP redirects (including relative ones). */
    private static void downloadWithRedirects(String urlStr, Path dest) throws IOException {
        URL current = new URL(urlStr);
        for (int i = 0; i < 10; i++) {
            HttpURLConnection conn = (HttpURLConnection) current.openConnection();
            conn.setInstanceFollowRedirects(false);
            conn.setRequestProperty("User-Agent", "pharos/1.0");
            int status = conn.getResponseCode();
            if (status == HttpURLConnection.HTTP_OK) {
                try (InputStream in = conn.getInputStream()) {
                    Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
                }
                return;
            } else if (status == HttpURLConnection.HTTP_MOVED_TEMP   // 302
                    || status == HttpURLConnection.HTTP_MOVED_PERM   // 301
                    || status == 307                                  // Temporary Redirect
                    || status == 308) {                               // Permanent Redirect
                String location = conn.getHeaderField("Location");
                conn.disconnect();
                // Resolve relative redirects against the current URL
                current = new URL(current, location);
            } else {
                throw new IOException("HTTP " + status + " downloading " + urlStr);
            }
        }
        throw new IOException("Too many redirects downloading " + urlStr);
    }

    /** Loads a model from the DJL model zoo (groupId:artifactId format). */
    @SuppressWarnings("unchecked")
    private static ZooModel<String, float[]> loadZooModel(String modelUrl) throws ModelException, IOException {
        Criteria.Builder<String, float[]> builder = Criteria.builder()
                .setTypes(String.class, float[].class)
                .optEngine("OnnxRuntime");
        if (modelUrl.contains(":") && !modelUrl.startsWith("http") && !modelUrl.startsWith("file")) {
            int sep = modelUrl.indexOf(':');
            builder.optGroupId(modelUrl.substring(0, sep))
                   .optArtifactId(modelUrl.substring(sep + 1));
        } else {
            builder.optModelUrls(modelUrl);
        }
        return (ZooModel<String, float[]>) ModelZoo.loadModel(builder.build());
    }

    @Override
    public float[] embed(String text) {
        if (text == null || text.isBlank()) return null;
        String input = text.length() > MAX_INPUT_LENGTH ? text.substring(0, MAX_INPUT_LENGTH) : text;
        try {
            maybeResetThreadPredictor();
            return threadPredictor.get().predict(input);
        } catch (TranslateException e) {
            log.debug("Embedding failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Closes and recreates the calling thread's ONNX Runtime Predictor every
     * PREDICTOR_RESET_INTERVAL calls.  Each thread tracks its own counter so
     * resets don't interfere across threads.
     */
    private void maybeResetThreadPredictor() {
        int[] count = threadEmbedCount.get();
        count[0]++;
        if (count[0] % PREDICTOR_RESET_INTERVAL == 0) {
            log.debug("Thread {} resetting ONNX predictor after {} embeddings",
                    Thread.currentThread().getName(), count[0]);
            threadPredictor.get().close();
            threadPredictor.set(newPredictor());
        }
    }

    /**
     * Embed a batch of texts in a single ONNX forward pass (HF models only).
     * Sequences are padded to the longest item in the batch; the ONNX model receives
     * a single [batch, seq_len] tensor instead of N separate [1, seq_len] tensors.
     *
     * <p>Falls back to the sequential default for zoo models or on error.
     */
    @Override
    public float[][] embedBatch(List<String> texts) {
        if (texts == null || texts.isEmpty()) return new float[0][];

        Predictor<List<String>, float[][]> batchPred = threadBatchPredictor.get();
        if (batchPred == null) {
            // Zoo model path — sequential fallback
            return EmbeddingProvider.super.embedBatch(texts);
        }

        List<String> sanitised = texts.stream()
                .map(t -> (t == null || t.isBlank()) ? ""
                        : t.length() > MAX_INPUT_LENGTH ? t.substring(0, MAX_INPUT_LENGTH) : t)
                .collect(Collectors.toList());
        try {
            return batchPred.predict(sanitised);
        } catch (TranslateException e) {
            log.debug("Batch embedding failed, falling back to sequential: {}", e.getMessage());
            return EmbeddingProvider.super.embedBatch(texts);
        }
    }

    @Override
    public int dimensions() {
        return dimensions;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }
}
