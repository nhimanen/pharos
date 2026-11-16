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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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

    // Pooling is consumed by the translators (Task 2); prefixes are applied here.
    private final com.pharos.config.Pooling pooling;
    private final String queryPrefix;
    private final String documentPrefix;

    public DjlEmbeddingProvider(String modelId, String modelUrl, int dimensions, int maxTokens)
            throws ModelException, IOException {
        this(modelId, modelUrl, dimensions, maxTokens, com.pharos.config.Pooling.MEAN, "", "");
    }

    public DjlEmbeddingProvider(String modelId, String modelUrl, int dimensions, int maxTokens,
                                com.pharos.config.Pooling pooling, String queryPrefix, String documentPrefix)
            throws ModelException, IOException {
        this.pooling = pooling == null ? com.pharos.config.Pooling.MEAN : pooling;
        this.queryPrefix = queryPrefix == null ? "" : queryPrefix;
        this.documentPrefix = documentPrefix == null ? "" : documentPrefix;
        log.info("Loading embedding model '{}' from {} (maxTokens={})",
                modelId, modelUrl, maxTokens);
        this.modelId = modelId;
        this.maxTokens = maxTokens;
        this.dimensions = dimensions;

        if (modelUrl.startsWith("hf://")) {
            this.tokenizerFile = prepareHuggingFaceFiles(modelUrl.substring("hf://".length()));
            try (var t = NativeInferenceGuard.enter()) {
                try {
                    this.hfModel = loadHuggingFaceModel(tokenizerFile.getParent());
                } catch (ModelException | RuntimeException e) {
                    throw new IOException("Failed to load model from " + tokenizerFile.getParent()
                            + " (missing external data or corrupt files? delete that directory to re-download): "
                            + e.getMessage(), e);
                }
            }
            this.zooModel = null;
        } else {
            this.tokenizerFile = null;
            this.hfModel = null;
            try (var t = NativeInferenceGuard.enter()) {
                this.zooModel = loadZooModel(modelUrl);
            }
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
                new SentenceTransformerTranslator(tokenizerFile.toAbsolutePath(), maxTokens, pooling);
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
                new BatchSentenceTransformerTranslator(tokenizerFile.toAbsolutePath(), maxTokens, pooling);
        return (Predictor<List<String>, float[][]>) hfModel.newPredictor(translator);
    }

    /**
     * Downloads the ONNX model and tokenizer from HuggingFace Hub (cached) and returns
     * the local path to tokenizer.json.
     */
    private Path prepareHuggingFaceFiles(String modelId) throws IOException {
        Path modelDir = resolveModelDir(MODEL_CACHE, modelId);
        ensureModelFiles(hfBaseUrl + modelId + "/resolve/main/", modelDir);
        return modelDir.resolve("tokenizer.json");
    }

    /**
     * Maps a repo id to its cache directory, refusing ids that could escape the cache root
     * (the marker-less path deletes files in this directory).
     */
    static Path resolveModelDir(Path cacheRoot, String repoId) {
        if (!com.pharos.config.EmbeddingProviderConfig.isValidHfRepoId(repoId)) {
            throw new IllegalArgumentException("Invalid HuggingFace repo id '" + repoId
                    + "' (expected owner/name using letters, digits, '.', '_' or '-')");
        }
        Path dir = cacheRoot.resolve(repoId.replace('/', '-')).normalize();
        if (!cacheRoot.normalize().equals(dir.getParent())) {
            throw new IllegalArgumentException("Model directory for '" + repoId
                    + "' would be outside the model cache: " + dir);
        }
        return dir;
    }

    private static final java.util.concurrent.ConcurrentHashMap<Path, Object> DIR_LOCKS =
            new java.util.concurrent.ConcurrentHashMap<>();

    /** Base URL override for tests (package-private). */
    static String hfBaseUrl = "https://huggingface.co/";

    static final String COMPLETE_MARKER = ".complete";

    /**
     * Ensures a complete model directory. Files are downloaded to {@code <file>.part} and moved
     * into place atomically; the {@code .complete} marker is written last. A directory without
     * the marker is re-downloaded, except a legacy one (model.onnx + tokenizer.json, no .part
     * files) from before markers existed, which is accepted once and marked.
     */
    static void ensureModelFiles(String base, Path modelDir) throws IOException {
        Path normalized = modelDir.toAbsolutePath().normalize();
        Files.createDirectories(normalized.getParent());
        // JVM-level monitor (FileChannel.lock cannot be held twice in one JVM) plus an
        // OS-level lock so a daemon and a CLI process cannot race on the same directory.
        synchronized (DIR_LOCKS.computeIfAbsent(normalized, k -> new Object())) {
            Path lockFile = normalized.resolveSibling(normalized.getFileName() + ".lock");
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(lockFile,
                         StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 java.nio.channels.FileLock ignored = ch.lock()) {
                ensureModelFilesLocked(base, normalized);
            }
        }
    }

    private static void ensureModelFilesLocked(String base, Path modelDir) throws IOException {
        Files.createDirectories(modelDir);
        Path marker = modelDir.resolve(COMPLETE_MARKER);
        Path onnxFile = modelDir.resolve("model.onnx");
        Path tokenizerPath = modelDir.resolve("tokenizer.json");
        if (Files.exists(marker)) return;

        boolean hasParts;
        try (var stream = Files.list(modelDir)) {
            hasParts = stream.anyMatch(f -> f.getFileName().toString().endsWith(".part"));
        }
        boolean legacy = !hasParts && Files.exists(onnxFile) && Files.exists(tokenizerPath);
        if (legacy) {
            log.info("Accepting pre-marker model directory {}", modelDir);
            Path dataFile = modelDir.resolve("model.onnx_data");
            if (!Files.exists(dataFile)) {
                try {
                    fetchOptionalOnnxData(base, dataFile);
                } catch (IOException e) {
                    // Offline or server trouble must not break a model that already works:
                    // accept the directory, skip the marker so the probe retries next start.
                    log.warn("Could not probe for external model data in {} ({}); using the cached model",
                            modelDir, e.getMessage());
                    try { ensureConfigJson(base, modelDir); } catch (IOException ignored) { }
                    return;
                }
            }
        } else {
            // Unknown state: discard everything so no half-model can be loaded.
            try (var stream = Files.list(modelDir)) {
                for (Path f : (Iterable<Path>) stream::iterator) Files.deleteIfExists(f);
            }
            log.info("Downloading ONNX model from {}onnx/model.onnx ...", base);
            downloadWithRedirects(base + "onnx/model.onnx", onnxFile);
            // Large models keep weights in a sibling external-data file; absent (404) is normal.
            fetchOptionalOnnxData(base, modelDir.resolve("model.onnx_data"));
            log.info("Downloading tokenizer from {}tokenizer.json ...", base);
            downloadWithRedirects(base + "tokenizer.json", tokenizerPath);
        }
        ensureConfigJson(base, modelDir);
        Files.writeString(marker, "ok\n");
    }

    private static void ensureConfigJson(String base, Path modelDir) throws IOException {
        Path configJsonFile = modelDir.resolve("config.json");
        if (!Files.exists(configJsonFile)) {
            log.info("Downloading model config from {}config.json ...", base);
            downloadWithRedirects(base + "config.json", configJsonFile);
        }
    }

    /** Fetches the optional external-data file; a 404 means the model has none. */
    private static void fetchOptionalOnnxData(String base, Path dataFile) throws IOException {
        try {
            downloadWithRedirects(base + "onnx/model.onnx_data", dataFile);
        } catch (HttpStatusException e) {
            if (e.status != 404) throw e;
        }
    }

    static final class HttpStatusException extends IOException {
        final int status;
        HttpStatusException(int status, String url) {
            super("HTTP " + status + " downloading " + url);
            this.status = status;
        }
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
                Path part = dest.resolveSibling(dest.getFileName() + ".part");
                try (InputStream in = conn.getInputStream()) {
                    Files.copy(in, part, StandardCopyOption.REPLACE_EXISTING);
                    long expected = conn.getContentLengthLong();
                    if (expected >= 0 && Files.size(part) != expected) {
                        throw new IOException("Truncated download of " + urlStr);
                    }
                    Files.move(part, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    Files.deleteIfExists(part);
                    throw e;
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
                try (InputStream err = conn.getErrorStream()) {
                    if (err != null) err.transferTo(OutputStream.nullOutputStream());
                } catch (IOException ignored) { }
                conn.disconnect();
                throw new HttpStatusException(status, urlStr);
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
    public float[] embedQuery(String text) {
        return embed(EmbeddingProvider.withPrefix(queryPrefix, text));
    }

    @Override
    public float[] embedDocument(String text) {
        return embed(EmbeddingProvider.withPrefix(documentPrefix, text));
    }

    @Override
    public float[] embed(String text) {
        if (text == null || text.isBlank()) return null;
        String input = text.length() > MAX_INPUT_LENGTH ? text.substring(0, MAX_INPUT_LENGTH) : text;
        try (var t = NativeInferenceGuard.enter()) {
            maybeResetThreadPredictor();
            return predictOrThrow(modelId, () -> threadPredictor.get().predict(input));
        }
    }

    /**
     * Sequential per-item embedding that tolerates individual failures: a failing item gets a
     * null slot (indexed without a vector), one WARN reports the count, and the failure is
     * rethrown only when every non-blank item failed (the model itself is broken).
     */
    public static final String CANARY_TEXT = "health check";

    public static float[][] embedEachTolerant(String modelId, List<String> texts,
                                       java.util.function.Function<String, float[]> embedOne) {
        float[][] results = new float[texts.size()][];
        int attempted = 0, failed = 0;
        EmbeddingException last = null;
        for (int i = 0; i < texts.size(); i++) {
            String t = texts.get(i);
            if (t == null || t.isBlank()) continue;
            attempted++;
            try {
                results[i] = embedOne.apply(t);
            } catch (EmbeddingException e) {
                failed++;
                last = e;
            }
        }
        if (failed > 0) {
            if (failed == attempted) {
                // Every text failed: tell a poisoned text from a broken model with a canary.
                try {
                    embedOne.apply(CANARY_TEXT);
                } catch (EmbeddingException canaryFailure) {
                    throw last;
                }
            }
            log.warn("Embedding failed for {} of {} texts with model '{}'; they are indexed without a vector",
                    failed, attempted, modelId);
        }
        return results;
    }

    @FunctionalInterface
    interface Prediction { float[] run() throws TranslateException; }

    /** Runs a prediction; a translator failure is logged at WARN and rethrown, never turned into a null vector. */
    static float[] predictOrThrow(String modelId, Prediction p) {
        try {
            return p.run();
        } catch (TranslateException e) {
            log.warn("Embedding failed for model '{}': {}", modelId, e.getMessage());
            throw new EmbeddingException("Embedding failed for model '" + modelId + "': " + e.getMessage(), e);
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
    public float[][] embedBatch(List<String> rawTexts) {
        if (rawTexts == null || rawTexts.isEmpty()) return new float[0][];
        // Batch embedding is document-side.
        List<String> texts = EmbeddingProvider.withPrefix(documentPrefix, rawTexts);

        List<String> sanitised = texts.stream()
                .map(t -> (t == null || t.isBlank()) ? ""
                        : t.length() > MAX_INPUT_LENGTH ? t.substring(0, MAX_INPUT_LENGTH) : t)
                .collect(Collectors.toList());
        try (var ticket = NativeInferenceGuard.enter()) {
            Predictor<List<String>, float[][]> batchPred = threadBatchPredictor.get();
            if (batchPred == null) {
                // Zoo model path — sequential fallback
                return embedEachTolerant(modelId, texts, this::embed);
            }
            return batchPred.predict(sanitised);
        } catch (TranslateException e) {
            log.warn("Batch embedding failed for model '{}', retrying sequentially: {}", modelId, e.getMessage());
            return embedEachTolerant(modelId, texts, this::embed);
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
