package com.pharos.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingProviderConfigTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private static EmbeddingProviderConfig base(String type) {
        EmbeddingProviderConfig c = new EmbeddingProviderConfig();
        c.setType(type);
        c.setModelId("my-model");
        c.setUrl("hf://x/y");
        c.setModel("m");
        c.setDimensions(768);
        return c;
    }

    @Test
    void jsonRoundTrip_poolingAndPrefixes() throws Exception {
        EmbeddingProviderConfig c = base("djl");
        c.setPooling(Pooling.CLS);
        c.setQueryPrefix("query: ");
        c.setDocumentPrefix("passage: ");
        String json = mapper.writeValueAsString(c);
        assertThat(json).contains("\"pooling\":\"cls\"");
        EmbeddingProviderConfig back = mapper.readValue(json, EmbeddingProviderConfig.class);
        assertThat(back.getPooling()).isEqualTo(Pooling.CLS);
        assertThat(back.getQueryPrefix()).isEqualTo("query: ");
        assertThat(back.getDocumentPrefix()).isEqualTo("passage: ");
    }

    @Test
    void defaults_areMeanAndEmpty_andOmittedFromJson() throws Exception {
        EmbeddingProviderConfig c = mapper.readValue(
                "{\"type\":\"djl\",\"modelId\":\"a\",\"url\":\"u\",\"dimensions\":4}",
                EmbeddingProviderConfig.class);
        assertThat(c.getPooling()).isEqualTo(Pooling.MEAN);
        assertThat(c.getQueryPrefix()).isEmpty();
        assertThat(c.getDocumentPrefix()).isEmpty();
        assertThat(mapper.writeValueAsString(c))
                .doesNotContain("pooling").doesNotContain("Prefix");
    }

    @Test
    void openAiWithCls_failsValidationNamingTheModel() {
        EmbeddingProviderConfig c = base("openai");
        c.setPooling(Pooling.CLS);
        assertThatThrownBy(c::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-model")
                .hasMessageContaining("pooling");
    }

    @Test
    void djlNonHfUrlWithCls_failsValidation() {
        EmbeddingProviderConfig c = base("djl");
        c.setUrl("djl://ai.djl.huggingface.pytorch/some-model");
        c.setPooling(Pooling.CLS);
        assertThatThrownBy(c::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-model")
                .hasMessageContaining("pooling")
                .hasMessageContaining("hf://");
    }

    @Test
    void djlNonHfFallback_underClsParent_isRejected() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getProperty("pharos.home") != null);
        EmbeddingProviderConfig parent = base("djl");
        parent.setPooling(Pooling.CLS);
        EmbeddingProviderConfig fb = base("djl");
        fb.setUrl("djl://ai.djl.huggingface.pytorch/some-model");
        parent.getFallbacks().add(fb);
        assertThatThrownBy(() -> loadWith(parent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-model").hasMessageContaining("hf://");
    }

    @Test
    void openAiWithPrefixes_andDjlWithCls_validate() {
        EmbeddingProviderConfig o = base("openai");
        o.setQueryPrefix("q: ");
        o.validate();
        EmbeddingProviderConfig d = base("djl");
        d.setPooling(Pooling.CLS);
        d.validate();
    }

    /** Drives the real IndexConfig.load() from config.json under the surefire-isolated pharos.home. */
    private IndexConfig loadWith(EmbeddingProviderConfig parent) throws Exception {
        // Never write config.json unless surefire isolated the pharos home (e.g. IDE runs).
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getProperty("pharos.home") != null);
        java.nio.file.Path file = IndexConfig.DEFAULT_BASE.resolve("config.json");
        java.nio.file.Files.createDirectories(file.getParent());
        IndexConfig cfg = IndexConfig.defaults();
        cfg.setEmbeddingProviders(new java.util.ArrayList<>(java.util.List.of(parent)));
        try {
            mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
            mapper.writeValue(file.toFile(), cfg);
            return IndexConfig.load();
        } finally {
            java.nio.file.Files.deleteIfExists(file);
        }
    }

    @Test
    void fallback_inheritsParentPoolingAndPrefixes() throws Exception {
        EmbeddingProviderConfig parent = base("djl");
        parent.setPooling(Pooling.CLS);
        parent.setQueryPrefix("q: ");
        parent.setDocumentPrefix("d: ");
        EmbeddingProviderConfig fb = base("djl");
        parent.getFallbacks().add(fb);
        IndexConfig loaded = loadWith(parent);
        EmbeddingProviderConfig loadedFb = loaded.getEmbeddingProviders().get(0).getFallbacks().get(0);
        assertThat(loadedFb.getPooling()).isEqualTo(Pooling.CLS);
        assertThat(loadedFb.getQueryPrefix()).isEqualTo("q: ");
        assertThat(loadedFb.getDocumentPrefix()).isEqualTo("d: ");
    }

    @Test
    void fallback_conflictingExplicitSetting_isRejectedNamingTheModel() {
        EmbeddingProviderConfig parent = base("djl");
        parent.setQueryPrefix("q: ");
        EmbeddingProviderConfig fb = base("djl");
        fb.setQueryPrefix("other: ");
        parent.getFallbacks().add(fb);
        assertThatThrownBy(() -> loadWith(parent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-model").hasMessageContaining("queryPrefix");
    }

    @Test
    void openAiFallback_underClsParent_isRejected() {
        EmbeddingProviderConfig parent = base("djl");
        parent.setPooling(Pooling.CLS);
        parent.getFallbacks().add(base("openai"));
        assertThatThrownBy(() -> loadWith(parent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("my-model").hasMessageContaining("pooling");
    }

    @Test
    void validate_rejectsTraversalAndMalformedHfIds() {
        for (String bad : new String[]{"hf://..", "hf://.", "hf://", "hf://a\\b", "hf://../x", "hf://a/.."}) {
            EmbeddingProviderConfig c = base("djl");
            c.setUrl(bad);
            assertThatThrownBy(c::validate)
                    .as(bad)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("my-model");
        }
        EmbeddingProviderConfig ok = base("djl");
        ok.setUrl("hf://jinaai/jina-embeddings-v2-base-code");
        ok.validate();
    }
}
