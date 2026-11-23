package com.pharos.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingsCommandTest {
    static final String GRANITE_URL =
            "hf://onnx-community/granite-embedding-english-r2-ONNX@2a49b9c076aa627b14bc528b36b67462808ccc23";

    @TempDir Path home;
    private final PrintStream origOut = System.out;
    private final PrintStream origErr = System.err;
    private ByteArrayOutputStream out;
    private ByteArrayOutputStream err;
    private ProjectRegistry registry;
    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void setUp() {
        out = new ByteArrayOutputStream();
        err = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out, true));
        System.setErr(new PrintStream(err, true));
        registry = ProjectRegistry.at(home.resolve("registry.json"));
    }

    @AfterEach
    void restore() { System.setOut(origOut); System.setErr(origErr); }

    private int run(String... args) {
        return new CommandLine(new EmbeddingsCommand(home, registry)).execute(args);
    }

    private JsonNode config() throws Exception { return json.readTree(home.resolve("config.json").toFile()); }

    @Test
    void enableWritesPinnedGraniteConfigAndPrintsCost() throws Exception {
        assertThat(run("enable")).isZero();
        JsonNode p = config().get("embeddingProviders").get(0);
        assertThat(p.get("modelId").asText()).isEqualTo("granite-r2");
        assertThat(p.get("type").asText()).isEqualTo("djl");
        assertThat(p.get("url").asText()).isEqualTo(GRANITE_URL);
        assertThat(p.get("dimensions").asInt()).isEqualTo(768);
        assertThat(p.get("maxTokens").asInt()).isEqualTo(512);
        assertThat(p.get("pooling").asText()).isEqualTo("cls");
        assertThat(config().get("searchEmbeddingModel").asText()).isEqualTo("granite-r2");
        assertThat(out.toString()).contains("600 MB").contains("pharos index --full <root path>");
        // loads back through the real loader
        assertThat(IndexConfig.loadFrom(home).getEmbeddingProviders().get(0).getModelId()).isEqualTo("granite-r2");
    }

    @Test
    void enableJinaWritesJinaConfig() throws Exception {
        assertThat(run("enable", "--model", "jina-v2-code")).isZero();
        JsonNode p = config().get("embeddingProviders").get(0);
        assertThat(p.get("modelId").asText()).isEqualTo("jina-v2-code");
        assertThat(p.get("url").asText()).isEqualTo("hf://jinaai/jina-embeddings-v2-base-code");
        assertThat(p.has("pooling")).isFalse();
    }

    @Test
    void enableKeepsOtherConfigKeys() throws Exception {
        Files.writeString(home.resolve("config.json"), "{\"verbose\":true}");
        assertThat(run("enable")).isZero();
        assertThat(config().get("verbose").asBoolean()).isTrue();
    }

    @Test
    void unknownModelIsRejectedAndWritesNothing() {
        assertThat(run("enable", "--model", "nope")).isNotZero();
        assertThat(home.resolve("config.json")).doesNotExist();
    }

    @Test
    void enableRefusesToOverwriteExistingJinaWithoutForce() throws Exception {
        String jina = "{\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"jina-code-v2\","
                + "\"url\":\"hf://jinaai/jina-embeddings-v2-base-code\",\"dimensions\":768}],"
                + "\"searchEmbeddingModel\":\"jina-code-v2\"}";
        Files.writeString(home.resolve("config.json"), jina);
        assertThat(run("enable")).isNotZero();
        assertThat(err.toString()).contains("--force");
        assertThat(Files.readString(home.resolve("config.json"))).isEqualTo(jina);

        assertThat(run("enable", "--force")).isZero();
        assertThat(config().get("embeddingProviders")).hasSize(1);
        assertThat(config().get("embeddingProviders").get(0).get("modelId").asText()).isEqualTo("granite-r2");
        assertThat(config().get("searchEmbeddingModel").asText()).isEqualTo("granite-r2");
    }

    @Test
    void enableRefusesLegacyOldStyleConfigWithoutForceAndForceDropsLegacyKeys() throws Exception {
        Files.writeString(home.resolve("config.json"),
                "{\"embeddingModelUrl\":\"hf://jinaai/jina-embeddings-v2-base-code\",\"embeddingDimensions\":768}");
        assertThat(run("enable")).isNotZero();
        assertThat(run("enable", "--force")).isZero();
        assertThat(config().has("embeddingModelUrl")).isFalse();
        assertThat(IndexConfig.loadFrom(home).getEmbeddingProviders()).hasSize(1);
        assertThat(IndexConfig.loadFrom(home).getEmbeddingProviders().get(0).getModelId()).isEqualTo("granite-r2");
    }

    @Test
    void enableIsIdempotentForTheSameModel() throws Exception {
        assertThat(run("enable")).isZero();
        assertThat(run("enable")).isZero();
        assertThat(config().get("embeddingProviders")).hasSize(1);
    }

    @Test
    void disableRemovesProvidersButKeepsOtherKeys() throws Exception {
        Files.writeString(home.resolve("config.json"),
                "{\"verbose\":true,\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"x\",\"url\":\"hf://a/b\","
                        + "\"dimensions\":8}],\"searchEmbeddingModel\":\"x\",\"embeddingModelUrl\":\"hf://a/b\"}");
        assertThat(run("disable")).isZero();
        JsonNode c = config();
        assertThat(c.has("embeddingProviders")).isFalse();
        assertThat(c.has("searchEmbeddingModel")).isFalse();
        assertThat(c.has("embeddingModelUrl")).isFalse();
        assertThat(c.get("verbose").asBoolean()).isTrue();
        assertThat(IndexConfig.loadFrom(home).getEmbeddingProviders()).isEmpty();
    }

    @Test
    void disableWithoutConfigIsANoOp() {
        assertThat(run("disable")).isZero();
        assertThat(home.resolve("config.json")).doesNotExist();
    }

    @Test
    void statusOffWhenNothingConfigured() {
        assertThat(run("status")).isZero();
        assertThat(out.toString()).contains("off");
        assertThat(home.resolve("config.json")).doesNotExist();
    }

    @Test
    void statusShowsModelFingerprintAndWhichProjectsHaveVectors() throws Exception {
        run("enable");
        out.reset();
        VectorIndexFixture.register(registry, home, "alpha", "vec.granite-r2");
        ProjectMeta b = VectorIndexFixture.register(registry, home, "beta");
        // embeddedModels is bookkeeping, not evidence: the index has no vectors for it
        b.setEmbeddedModels(List.of("granite-r2"));
        registry.register(b);
        assertThat(run("status")).isZero();
        String o = out.toString();
        assertThat(o).contains("granite-r2").contains("pool=cls").contains(GRANITE_URL);
        assertThat(o).containsPattern("alpha\\s+yes").containsPattern("beta\\s+no");
        assertThat(o).contains("pharos index --full --single --project beta " + home.resolve("src-beta"));
    }

    @Test
    void failedWriteLeavesOriginalConfigByteIdentical() throws Exception {
        String orig = "{\"verbose\":true,\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"jina-code-v2\","
                + "\"url\":\"hf://jinaai/jina-embeddings-v2-base-code\",\"dimensions\":768}]}";
        Files.writeString(home.resolve("config.json"), orig);
        EmbeddingsCommand.beforeMove = tmp -> { throw new UncheckedIOException(new IOException("boom")); };
        try {
            assertThatThrownBy(() -> run("enable", "--force")).isNotNull();
        } catch (AssertionError ignored) {
            // execute() returns nonzero instead of throwing; either way the file must be intact
        } finally {
            EmbeddingsCommand.beforeMove = p -> { };
        }
        assertThat(Files.readString(home.resolve("config.json"))).isEqualTo(orig);
        assertThat(home.resolve("config.json.tmp")).doesNotExist();
    }

    @Test
    void emptyOrNonObjectConfigIsReportedNotCrashed() throws Exception {
        for (String bad : new String[]{"", "[]", "\"x\"", "42"}) {
            Files.writeString(home.resolve("config.json"), bad);
            err.reset();
            assertThat(run("enable")).isNotZero();
            assertThat(err.toString()).contains("config.json is not a JSON object");
            assertThat(run("disable")).isNotZero();
            assertThat(Files.readString(home.resolve("config.json"))).isEqualTo(bad);
        }
    }

    @Test
    void forceListsTheProvidersItRemoves() throws Exception {
        Files.writeString(home.resolve("config.json"),
                "{\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"a\",\"url\":\"hf://x/a\",\"dimensions\":8},"
                        + "{\"type\":\"djl\",\"modelId\":\"b\",\"url\":\"hf://x/b\",\"dimensions\":8}]}");
        assertThat(run("enable", "--force")).isZero();
        assertThat(out.toString()).contains("a  hf://x/a").contains("b  hf://x/b");
        assertThat(config().get("embeddingProviders")).hasSize(1);
    }

    @Test
    void nestedUnknownKeysSurviveEnableAndDisable() throws Exception {
        Files.writeString(home.resolve("config.json"), "{\"cluster\":{\"token\":\"s3cret\",\"nodes\":[1,2]}}");
        assertThat(run("enable")).isZero();
        assertThat(config().get("cluster").get("token").asText()).isEqualTo("s3cret");
        assertThat(run("disable")).isZero();
        assertThat(config().get("cluster").get("nodes")).hasSize(2);
    }

    @Test
    void statusWithLegacyConfigReadsVectorsFromTheIndex() throws Exception {
        Files.writeString(home.resolve("config.json"),
                "{\"embeddingModelUrl\":\"hf://jinaai/jina-embeddings-v2-base-code\",\"embeddingDimensions\":768}");
        VectorIndexFixture.register(registry, home, "old");                        // keyword-only
        VectorIndexFixture.register(registry, home, "oldvec", "vectorEmbedding");  // legacy field, searched
        VectorIndexFixture.register(registry, home, "misnamed", "vec.legacy");     // 1.4.0-1.4.1 field, not searched
        assertThat(run("status")).isZero();
        String o = out.toString();
        assertThat(o).contains("legacy").contains("hf://jinaai/jina-embeddings-v2-base-code");
        assertThat(o).containsPattern("old\\s+no").containsPattern("oldvec\\s+yes").containsPattern("misnamed\\s+no");
    }

    @Test
    void statusOnALazyReaderGenerationSaysUnknownInsteadOfProbingLocalFiles() throws Exception {
        run("enable");
        out.reset();
        ProjectMeta m = VectorIndexFixture.register(registry, home, "lazyshard", "vec.granite-r2");
        // A lazy reader generation dir: the marker says most Lucene files live on the bucket.
        Files.createFile(Path.of(m.getIndexPath()).resolve(com.pharos.cluster.warm.LazyGenerations.MARKER));
        assertThat(run("status")).isZero();
        String o = out.toString();
        assertThat(o).containsPattern("lazyshard\\s+unknown \\(lazy reader");
        assertThat(o).doesNotContain("pharos index --full --single --project lazyshard");
        assertThat(com.pharos.indexer.LegacyVectorFields.hasLiveVectors(
                Path.of(m.getIndexPath()).resolve("lucene"), "vec.granite-r2")).isFalse();
    }

    @Test
    void enableJinaOnEquivalentJinaSetupIsAlreadyConfigured() throws Exception {
        String[] equivalent = {
                // old single-key config (the user's own form)
                "{\"embeddingModelUrl\":\"hf://jinaai/jina-embeddings-v2-base-code\",\"embeddingDimensions\":768}",
                // README (pre-1.5.0) provider block
                "{\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"jina-v2-code\","
                        + "\"url\":\"hf://jinaai/jina-embeddings-v2-base-code\",\"dimensions\":768}]}",
                // other id, pinned revision
                "{\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"jina-code-v2\","
                        + "\"url\":\"hf://jinaai/jina-embeddings-v2-base-code@abcdef1\",\"dimensions\":768}]}"};
        for (String cfg : equivalent) {
            Files.writeString(home.resolve("config.json"), cfg);
            out.reset();
            err.reset();
            assertThat(run("enable", "--model", "jina-v2-code")).as(cfg).isZero();
            assertThat(out.toString()).contains("already configured");
            assertThat(err.toString()).doesNotContain("different model");
            assertThat(Files.readString(home.resolve("config.json"))).isEqualTo(cfg);
        }
    }

    @Test
    void enableGraniteOnUnpinnedGraniteIsAlreadyConfigured() throws Exception {
        String cfg = "{\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"granite-r2\","
                + "\"url\":\"hf://onnx-community/granite-embedding-english-r2-ONNX\",\"dimensions\":768,"
                + "\"pooling\":\"cls\"}]}";
        Files.writeString(home.resolve("config.json"), cfg);
        assertThat(run("enable")).isZero();
        assertThat(out.toString()).contains("already configured");
    }

    @Test
    void differentModelHintNamesIndexFullWithRootPath() throws Exception {
        Files.writeString(home.resolve("config.json"),
                "{\"embeddingModelUrl\":\"hf://jinaai/jina-embeddings-v2-base-code\",\"embeddingDimensions\":768}");
        assertThat(run("enable")).isNotZero();
        assertThat(err.toString()).contains("pharos index --full <root path>").doesNotContain("<project>");
    }

    @Test
    void statusAndDisableWorkWithAnInvalidEmbeddingConfig() throws Exception {
        // valid JSON, invalid provider: IndexConfig.load() rejects it
        String bad = "{\"verbose\":true,\"embeddingProviders\":[{\"type\":\"djl\",\"modelId\":\"x\","
                + "\"url\":\"hf://a/b\",\"dimensions\":-1}]}";
        Files.writeString(home.resolve("config.json"), bad);
        assertThat(run("status")).isNotZero();
        assertThat(err.toString()).contains(home.resolve("config.json").toString())
                .contains("pharos embeddings disable");
        assertThat(run("disable")).isZero();
        assertThat(IndexConfig.loadFrom(home).getEmbeddingProviders()).isEmpty();
        assertThat(config().get("verbose").asBoolean()).isTrue();
    }
}
