package com.pharos.config;

import com.pharos.indexer.IndexVersions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Built-in granite-r2 default applies to new installs only; existing configs keep their model. */
class BuiltInDefaultEmbeddingTest {

    @TempDir Path home;

    @Test
    void freshInstallGetsPinnedGraniteWithClsFingerprint() {
        IndexConfig c = IndexConfig.loadFrom(home);
        assertThat(c.getEmbeddingProviders()).hasSize(1);
        EmbeddingProviderConfig p = c.getEmbeddingProviders().get(0);
        assertThat(p.getModelId()).isEqualTo("granite-r2");
        assertThat(p.getType()).isEqualTo("djl");
        assertThat(p.getUrl()).matches("hf://onnx-community/granite-embedding-english-r2-ONNX@[0-9a-f]{40}");
        assertThat(p.getDimensions()).isEqualTo(768);
        assertThat(p.getMaxTokens()).isEqualTo(512);
        assertThat(p.getPooling()).isEqualTo(Pooling.CLS);
        p.validate();
        assertThat(c.getSearchEmbeddingModel()).isEqualTo("granite-r2");
        assertThat(IndexVersions.modelFingerprint(p)).contains(":pool=cls:");
    }

    @Test
    void freshInstallChoiceIsPersistedAndSurvivesTheHomeFillingUp() throws Exception {
        IndexConfig.loadFrom(home);
        assertThat(home.resolve("config.json")).exists();
        Files.writeString(home.resolve("registry.json"), "{}");   // first project got indexed
        Files.createDirectories(home.resolve("indexes"));
        IndexConfig again = IndexConfig.loadFrom(home);
        assertThat(again.getEmbeddingProviders()).hasSize(1);
        assertThat(again.getEmbeddingProviders().get(0).getModelId()).isEqualTo("granite-r2");
        assertThat(again.getEmbeddingProviders().get(0).getPooling()).isEqualTo(Pooling.CLS);
        assertThat(again.getSearchEmbeddingModel()).isEqualTo("granite-r2");
    }

    @Test
    void existingInstallWithoutConfigJsonStaysKeywordOnly() throws Exception {
        Files.writeString(home.resolve("registry.json"), "{}");
        assertThat(IndexConfig.loadFrom(home).getEmbeddingProviders()).isEmpty();
        assertThat(home.resolve("config.json")).doesNotExist();
    }

    @Test
    void existingInstallWithIndexesDirStaysKeywordOnly() throws Exception {
        Files.createDirectories(home.resolve("indexes"));
        assertThat(IndexConfig.loadFrom(home).getEmbeddingProviders()).isEmpty();
    }

    @Test
    void configWithoutEmbeddingSettingsStaysKeywordOnly() throws Exception {
        Files.writeString(home.resolve("config.json"), "{}");
        IndexConfig c = IndexConfig.loadFrom(home);
        assertThat(c.getEmbeddingProviders()).isEmpty();
        assertThat(c.getSearchEmbeddingModel()).isNull();
    }

    @Test
    void oldStyleJinaConfigStillLoadsAsLegacyWithTheSameFingerprint() throws Exception {
        Files.writeString(home.resolve("config.json"), """
                {"embeddingModelUrl":"hf://jinaai/jina-embeddings-v2-base-code",
                 "embeddingDimensions":768,"embeddingMaxTokens":512}""");
        IndexConfig c = IndexConfig.loadFrom(home);
        assertThat(c.getEmbeddingProviders()).hasSize(1);
        EmbeddingProviderConfig p = c.getEmbeddingProviders().get(0);
        assertThat(p.getModelId()).isEqualTo("legacy");
        assertThat(p.getPooling()).isEqualTo(Pooling.MEAN);
        assertThat(c.getSearchEmbeddingModel()).isEqualTo("legacy");
        assertThat(IndexVersions.modelFingerprint(p))
                .isEqualTo("legacy:768:hf://jinaai/jina-embeddings-v2-base-code:");
    }

    @Test
    void explicitJinaProviderConfigIsUntouched() throws Exception {
        Files.writeString(home.resolve("config.json"), """
                {"embeddingProviders":[{"type":"djl","modelId":"jina-code-v2",
                  "url":"hf://jinaai/jina-embeddings-v2-base-code","dimensions":768}],
                 "searchEmbeddingModel":"jina-code-v2"}""");
        IndexConfig c = IndexConfig.loadFrom(home);
        assertThat(c.getEmbeddingProviders()).hasSize(1);
        assertThat(IndexVersions.modelFingerprint(c.getEmbeddingProviders().get(0)))
                .isEqualTo("jina-code-v2:768:hf://jinaai/jina-embeddings-v2-base-code:");
        assertThat(c.getSearchEmbeddingModel()).isEqualTo("jina-code-v2");
    }
}
