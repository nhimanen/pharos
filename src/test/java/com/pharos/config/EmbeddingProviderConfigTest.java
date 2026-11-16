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
    void openAiWithPrefixes_andDjlWithCls_validate() {
        EmbeddingProviderConfig o = base("openai");
        o.setQueryPrefix("q: ");
        o.validate();
        EmbeddingProviderConfig d = base("djl");
        d.setPooling(Pooling.CLS);
        d.validate();
    }

    private static IndexConfig loadWith(EmbeddingProviderConfig parent) throws Exception {
        java.nio.file.Path home = java.nio.file.Files.createTempDirectory("pharos-fb");
        IndexConfig c = IndexConfig.defaults();
        c.setEmbeddingProviders(new java.util.ArrayList<>(java.util.List.of(parent)));
        // validateFallbacks is private; exercise it through the public inherit/validate path
        for (EmbeddingProviderConfig p : c.getEmbeddingProviders()) {
            p.validate();
            for (EmbeddingProviderConfig fb : p.getFallbacks()) fb.inheritRuntimeSettings(p);
            for (EmbeddingProviderConfig fb : p.getFallbacks()) fb.validate();
        }
        return c;
    }

    @Test
    void fallback_inheritsParentPoolingAndPrefixes() throws Exception {
        EmbeddingProviderConfig parent = base("djl");
        parent.setPooling(Pooling.CLS);
        parent.setQueryPrefix("q: ");
        parent.setDocumentPrefix("d: ");
        EmbeddingProviderConfig fb = base("djl");
        parent.getFallbacks().add(fb);
        loadWith(parent);
        assertThat(fb.getPooling()).isEqualTo(Pooling.CLS);
        assertThat(fb.getQueryPrefix()).isEqualTo("q: ");
        assertThat(fb.getDocumentPrefix()).isEqualTo("d: ");
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
}
