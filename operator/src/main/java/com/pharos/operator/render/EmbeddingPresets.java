package com.pharos.operator.render;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The embedding models {@code spec.embeddings.model} may name: the presets of
 * {@code pharos embeddings enable --model <id>} (pharos 1.5.0, {@code IndexConfig.presetProvider}).
 * {@link #block} is exactly the top-level part of config.json that command writes, so a node the
 * operator configures embeds exactly as one configured by hand.
 *
 * <p>The list is kept here because the presets live in the pharos application, which the operator
 * must not depend on. {@code EmbeddingPresetsTest} compares each block with the config.json that
 * {@code pharos embeddings enable} wrote (test resources {@code embeddings/<id>.json}).
 */
public final class EmbeddingPresets {

    /** No embeddings: keyword search only; nothing is rendered. */
    public static final String NONE = "none";
    public static final String GRANITE_R2 = "granite-r2";
    public static final String JINA_V2_CODE = "jina-v2-code";

    /** Pinned revision of {@code onnx-community/granite-embedding-english-r2-ONNX}, as pharos pins it. */
    static final String GRANITE_URL =
            "hf://onnx-community/granite-embedding-english-r2-ONNX@2a49b9c076aa627b14bc528b36b67462808ccc23";
    static final String JINA_URL = "hf://jinaai/jina-embeddings-v2-base-code";

    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    private EmbeddingPresets() {}

    /** The accepted values of {@code spec.embeddings.model}. */
    public static Set<String> names() {
        return Set.of(NONE, GRANITE_R2, JINA_V2_CODE);
    }

    /**
     * The top-level config.json keys for {@code model} ({@code embeddingProviders},
     * {@code searchEmbeddingModel}), in the order pharos writes them; empty for {@code none} or null.
     *
     * @throws IllegalArgumentException for an unknown model (the validator rejects it first)
     */
    public static Map<String, com.fasterxml.jackson.databind.JsonNode> block(String model) {
        Map<String, com.fasterxml.jackson.databind.JsonNode> out = new LinkedHashMap<>();
        if (model == null || NONE.equals(model)) return out;
        ObjectNode p = F.objectNode();
        p.put("type", "djl");
        switch (model) {
            case GRANITE_R2 -> { p.put("modelId", GRANITE_R2); p.put("url", GRANITE_URL); }
            case JINA_V2_CODE -> { p.put("modelId", JINA_V2_CODE); p.put("url", JINA_URL); }
            default -> throw new IllegalArgumentException("unknown embedding model '" + model + "'");
        }
        p.putNull("model");
        p.putNull("apiKeyEnv");
        p.put("dimensions", 768);
        p.put("maxTokens", 512);
        p.put("batchSize", 0);
        p.put("timeoutMillis", 60000);
        p.put("embeddingThreads", 0);
        p.putArray("fallbacks");
        if (GRANITE_R2.equals(model)) p.put("pooling", "cls"); // mean, the default, is left out
        out.put("embeddingProviders", F.arrayNode().add(p));
        out.put("searchEmbeddingModel", F.textNode(model));
        return out;
    }
}
