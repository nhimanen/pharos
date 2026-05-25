package com.pharos.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Global configuration for the code-search tool.
 * Stored at ~/.pharos/config.json.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexConfig {

    private static final Logger log = LoggerFactory.getLogger(IndexConfig.class);
    public static final Path DEFAULT_BASE = Path.of(System.getProperty("user.home"), ".pharos");

    private Path indexDir;
    private Path synonymsFile;

    /**
     * One or more embedding providers. Pharos can run several in parallel at
     * index time and store one vector field per model in each Lucene document.
     * Search picks one model via {@link #searchEmbeddingModel}.
     */
    private List<EmbeddingProviderConfig> embeddingProviders = new ArrayList<>();

    /**
     * Identifier of the embedding provider to use at search time. Must match the
     * {@code modelId} of one of the configured {@link #embeddingProviders}. When
     * null, the first provider in the list is used. When no providers are
     * configured, vector/hybrid search degrades to keyword-only.
     *
     * <p>Ignored when {@link #searchEmbeddingProvider} is set — the latter is
     * the strict winner.
     */
    private String searchEmbeddingModel;

    /**
     * Dedicated provider config used <b>only at query time</b>. Lets you run a
     * different <i>runtime</i> for the same logical model than was used to
     * build the index — e.g. embed with a fast remote llama-server endpoint at
     * index time, but embed individual search queries with a local DJL model
     * so queries still work offline.
     *
     * <p>The {@code modelId} on this entry must match one of the
     * {@link #embeddingProviders}' modelIds — that's how pharos routes search
     * queries to the right {@code vec.<modelId>} Lucene field. The {@code type}
     * and {@code url} can differ.
     *
     * <p>When null, search uses {@link #searchEmbeddingModel} against
     * {@link #embeddingProviders}.
     *
     * <p>The user is on the hook for ensuring both runtimes produce vectors
     * in the same space — typically by serving the same underlying model
     * (same architecture + weights) under both runtimes.
     */
    private EmbeddingProviderConfig searchEmbeddingProvider;

    // ── Legacy single-provider fields (pre-multi-provider config) ─────────────
    // Read from old config.json shapes via Jackson, then migrated into the
    // embeddingProviders list at load() time. @JsonIgnore on the getters so
    // they are NOT serialized back when save() is called — we want to drop the
    // old shape on the first config write after upgrade.
    private String embeddingModelUrl;
    private int embeddingDimensions;
    private int embeddingMaxTokens;

    private int hnswMaxConnections;
    private int hnswBeamWidth;
    private boolean verbose;

    /**
     * Number of threads for parallel file parsing (JavaCodeParser, GenericFileParser).
     * 0 = auto: max(1, min(8, nCPU / 2)).
     * Higher values speed up parsing but each thread keeps its own type-solver cache in memory.
     */
    private int parseThreads;

    /**
     * Number of threads for parallel embedding + Lucene document writing.
     * 0 = auto: max(1, min(4, nCPU / 4)).
     * Each thread holds its own ONNX Runtime Predictor, so keep this low when embedding is enabled.
     */
    private int indexThreads;

    /**
     * When true, loads a cross-encoder ONNX model to enable HYBRID_RERANKED and
     * HYBRID_CROSS_ENCODER_MERGE search modes. Requires a compatible model on HuggingFace Hub.
     * Degrades gracefully to BordaMerge when the model cannot be loaded.
     */
    private boolean crossEncoderEnabled = true;

    // Jackson requires no-arg constructor
    public IndexConfig() {
        this.indexDir    = DEFAULT_BASE.resolve("indexes");
        this.synonymsFile = DEFAULT_BASE.resolve("synonyms.txt");
        this.embeddingModelUrl = null;
        this.embeddingDimensions = 768;
        this.embeddingMaxTokens = 512;
        this.hnswMaxConnections = 16;
        this.hnswBeamWidth = 100;
        this.verbose = false;
        this.parseThreads = 0;
        this.indexThreads = 0;
    }

    /** Resolved parse thread count — substitutes auto value when parseThreads == 0. */
    public int resolvedParseThreads() {
        if (parseThreads > 0) return parseThreads;
        return Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors() / 2));
    }

    /** Resolved index thread count — substitutes auto value when indexThreads == 0. */
    public int resolvedIndexThreads() {
        if (indexThreads > 0) return indexThreads;
        return Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() / 4));
    }

    /**
     * Number of projects to index concurrently during multi-project discovery.
     * Capped at available processors; each project already uses its own parse/index threads,
     * so a small value (2–4) is enough to keep I/O and parsing pipelines busy.
     */
    public int resolvedProjectThreads() {
        return 1;
    }

    public static IndexConfig defaults() {
        return new IndexConfig();
    }

    public static IndexConfig load() {
        Path configFile = DEFAULT_BASE.resolve("config.json");
        if (!Files.exists(configFile)) {
            return defaults();
        }
        try {
            ObjectMapper mapper = createMapper();
            IndexConfig config = mapper.readValue(configFile.toFile(), IndexConfig.class);
            if (config.indexDir == null) config.indexDir = DEFAULT_BASE.resolve("indexes");
            if (config.embeddingProviders == null) config.embeddingProviders = new ArrayList<>();
            config.migrateLegacyEmbeddingConfig();
            for (EmbeddingProviderConfig p : config.embeddingProviders) p.validate();
            if (config.searchEmbeddingProvider != null) {
                config.searchEmbeddingProvider.validate();
                // Routing safety: the search-time runtime must target a modelId
                // that was actually written to the index. Otherwise vec.<id>
                // queries hit a missing field and return empty.
                boolean matches = config.embeddingProviders.stream()
                        .anyMatch(p -> p.getModelId().equals(config.searchEmbeddingProvider.getModelId()));
                if (!matches) {
                    throw new IllegalArgumentException(String.format(
                            "searchEmbeddingProvider.modelId='%s' does not match any " +
                            "embeddingProviders entry. Vectors would not be queryable. " +
                            "Either fix the modelId or remove the searchEmbeddingProvider entry.",
                            config.searchEmbeddingProvider.getModelId()));
                }
            }
            return config;
        } catch (IOException e) {
            log.warn("Could not read config file {}, using defaults: {}", configFile, e.getMessage());
            return defaults();
        }
    }

    /**
     * Stable, sanitized identifier derived from the legacy {@code embeddingModelUrl}
     * for indexes that were built before multi-provider support landed. Used both
     * during config migration and at search time when an existing index has no
     * recorded {@code embeddedModels} (the search engine reads the legacy
     * {@code vectorEmbedding} field via this model id).
     */
    public static final String LEGACY_MODEL_ID = "legacy";

    /**
     * If the new {@link #embeddingProviders} list is empty but the old
     * {@link #embeddingModelUrl} is set, synthesize a single legacy provider so
     * downstream code (which only reads the new shape) keeps working. The legacy
     * fields are NOT written back on save — see the {@code @JsonIgnore}-marked
     * getters below.
     */
    private void migrateLegacyEmbeddingConfig() {
        if (!embeddingProviders.isEmpty()) return;
        if (embeddingModelUrl == null || embeddingModelUrl.isBlank()) return;

        EmbeddingProviderConfig legacy = new EmbeddingProviderConfig();
        legacy.setType("djl");
        legacy.setModelId(LEGACY_MODEL_ID);
        legacy.setUrl(embeddingModelUrl);
        legacy.setDimensions(embeddingDimensions > 0 ? embeddingDimensions : 768);
        legacy.setMaxTokens(embeddingMaxTokens > 0 ? embeddingMaxTokens : 512);
        embeddingProviders.add(legacy);
        if (searchEmbeddingModel == null || searchEmbeddingModel.isBlank()) {
            searchEmbeddingModel = LEGACY_MODEL_ID;
        }
        log.info("Migrated legacy embeddingModelUrl='{}' into provider '{}'.",
                embeddingModelUrl, LEGACY_MODEL_ID);
    }

    public void save() throws IOException {
        Path configFile = DEFAULT_BASE.resolve("config.json");
        Files.createDirectories(configFile.getParent());
        createMapper().writerWithDefaultPrettyPrinter()
                .writeValue(configFile.toFile(), this);
    }

    private static ObjectMapper createMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // Getters and setters
    public Path getIndexDir() { return indexDir; }
    public void setIndexDir(Path indexDir) { this.indexDir = indexDir; }

    public Path getSynonymsFile() { return synonymsFile; }
    public void setSynonymsFile(Path synonymsFile) { this.synonymsFile = synonymsFile; }

    // Legacy single-provider getters — kept for callers that haven't been
    // refactored to the multi-provider API yet (EmbeddingProvider.create,
    // EmbeddingCacheBackfiller, IndexVersions.modelFingerprint, etc.).
    // @JsonIgnore so they don't get serialized back; setters keep their
    // Jackson-default deserialization so legacy config.json shapes load.
    /** @deprecated use {@link #getEmbeddingProviders()} */
    @JsonIgnore
    @Deprecated
    public String getEmbeddingModelUrl() {
        if (embeddingModelUrl != null) return embeddingModelUrl;
        return embeddingProviders.isEmpty() ? null : embeddingProviders.get(0).getUrl();
    }
    @JsonSetter("embeddingModelUrl")
    public void setEmbeddingModelUrl(String embeddingModelUrl) { this.embeddingModelUrl = embeddingModelUrl; }

    /** @deprecated use {@link #getEmbeddingProviders()} */
    @JsonIgnore
    @Deprecated
    public int getEmbeddingDimensions() {
        if (embeddingDimensions > 0) return embeddingDimensions;
        return embeddingProviders.isEmpty() ? 768 : embeddingProviders.get(0).getDimensions();
    }
    @JsonSetter("embeddingDimensions")
    public void setEmbeddingDimensions(int embeddingDimensions) { this.embeddingDimensions = embeddingDimensions; }

    /** @deprecated use {@link #getEmbeddingProviders()} */
    @JsonIgnore
    @Deprecated
    public int getEmbeddingMaxTokens() {
        if (embeddingMaxTokens > 0) return embeddingMaxTokens;
        return embeddingProviders.isEmpty() ? 512 : embeddingProviders.get(0).getMaxTokens();
    }
    @JsonSetter("embeddingMaxTokens")
    public void setEmbeddingMaxTokens(int embeddingMaxTokens) { this.embeddingMaxTokens = embeddingMaxTokens; }

    public List<EmbeddingProviderConfig> getEmbeddingProviders() { return embeddingProviders; }
    public void setEmbeddingProviders(List<EmbeddingProviderConfig> embeddingProviders) {
        this.embeddingProviders = embeddingProviders == null ? new ArrayList<>() : embeddingProviders;
    }

    public String getSearchEmbeddingModel() { return searchEmbeddingModel; }
    public void setSearchEmbeddingModel(String searchEmbeddingModel) {
        this.searchEmbeddingModel = searchEmbeddingModel;
    }

    /**
     * Resolves the search-time embedding provider.
     * <ol>
     *   <li>If a dedicated {@link #searchEmbeddingProvider} is set, return it.</li>
     *   <li>Otherwise, if {@link #searchEmbeddingModel} matches an entry in
     *       {@link #embeddingProviders}, return that.</li>
     *   <li>Otherwise return the first entry in {@link #embeddingProviders}.</li>
     *   <li>If there are no providers, return empty.</li>
     * </ol>
     */
    public Optional<EmbeddingProviderConfig> resolveSearchProvider() {
        if (searchEmbeddingProvider != null) return Optional.of(searchEmbeddingProvider);
        if (embeddingProviders.isEmpty()) return Optional.empty();
        if (searchEmbeddingModel == null || searchEmbeddingModel.isBlank()) {
            return Optional.of(embeddingProviders.get(0));
        }
        return findProviderConfig(searchEmbeddingModel);
    }

    public EmbeddingProviderConfig getSearchEmbeddingProvider() { return searchEmbeddingProvider; }
    public void setSearchEmbeddingProvider(EmbeddingProviderConfig searchEmbeddingProvider) {
        this.searchEmbeddingProvider = searchEmbeddingProvider;
    }

    /** Look up a provider config by its {@code modelId}. */
    public Optional<EmbeddingProviderConfig> findProviderConfig(String modelId) {
        if (modelId == null) return Optional.empty();
        return embeddingProviders.stream()
                .filter(p -> modelId.equals(p.getModelId()))
                .findFirst();
    }

    public int getHnswMaxConnections() { return hnswMaxConnections; }
    public void setHnswMaxConnections(int hnswMaxConnections) { this.hnswMaxConnections = hnswMaxConnections; }

    public int getHnswBeamWidth() { return hnswBeamWidth; }
    public void setHnswBeamWidth(int hnswBeamWidth) { this.hnswBeamWidth = hnswBeamWidth; }

    public boolean isVerbose() { return verbose; }
    public void setVerbose(boolean verbose) { this.verbose = verbose; }

    public int getParseThreads() { return parseThreads; }
    public void setParseThreads(int parseThreads) { this.parseThreads = parseThreads; }

    public int getIndexThreads() { return indexThreads; }
    public void setIndexThreads(int indexThreads) { this.indexThreads = indexThreads; }

    public boolean isCrossEncoderEnabled() { return crossEncoderEnabled; }
    public void setCrossEncoderEnabled(boolean crossEncoderEnabled) { this.crossEncoderEnabled = crossEncoderEnabled; }
}
