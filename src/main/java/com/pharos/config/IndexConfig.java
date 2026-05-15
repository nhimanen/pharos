package com.pharos.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
    private String embeddingModelUrl;
    private int embeddingDimensions;
    private int hnswMaxConnections;
    private int hnswBeamWidth;
    private int embeddingMaxTokens;
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
            return config;
        } catch (IOException e) {
            log.warn("Could not read config file {}, using defaults: {}", configFile, e.getMessage());
            return defaults();
        }
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

    public String getEmbeddingModelUrl() { return embeddingModelUrl; }
    public void setEmbeddingModelUrl(String embeddingModelUrl) { this.embeddingModelUrl = embeddingModelUrl; }

    public int getEmbeddingDimensions() { return embeddingDimensions; }
    public void setEmbeddingDimensions(int embeddingDimensions) { this.embeddingDimensions = embeddingDimensions; }

    public int getEmbeddingMaxTokens() { return embeddingMaxTokens; }
    public void setEmbeddingMaxTokens(int embeddingMaxTokens) { this.embeddingMaxTokens = embeddingMaxTokens; }

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
