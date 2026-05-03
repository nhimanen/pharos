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
import java.util.concurrent.atomic.AtomicInteger;

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
     * Per-thread embed call counter used for periodic predictor reset.
     * int[] so the value can be mutated inside the ThreadLocal lambda.
     */
    private final ThreadLocal<int[]> threadEmbedCount =
            ThreadLocal.withInitial(() -> new int[]{0});

    public DjlEmbeddingProvider(String modelUrl, int dimensions, int maxTokens)
            throws ModelException, IOException {
        log.info("Loading embedding model: {} (maxTokens={})", modelUrl, maxTokens);
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

        log.info("Embedding model loaded ({} dimensions)", dimensions);
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

    @Override
    public int dimensions() {
        return dimensions;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }
}
