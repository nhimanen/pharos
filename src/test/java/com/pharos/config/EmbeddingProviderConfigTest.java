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
}
