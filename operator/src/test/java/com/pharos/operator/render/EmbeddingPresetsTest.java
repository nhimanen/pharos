package com.pharos.operator.render;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.operator.TestClusters;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.render.ConfigRenderer.Role;
import com.pharos.operator.validate.SpecValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The embedding block matches what pharos itself writes. The fixtures {@code embeddings/<id>.json} are
 * the config.json that {@code pharos embeddings enable --model <id>} (pharos 1.5.0, feat/embedding-pooling
 * at b27db60) wrote into an empty {@code -Dpharos.home}; regenerate them the same way when the presets change.
 */
class EmbeddingPresetsTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static JsonNode fixture(String model) throws Exception {
        try (InputStream in = EmbeddingPresetsTest.class.getResourceAsStream("/embeddings/" + model + ".json")) {
            return JSON.readTree(in);
        }
    }

    private static PharosCluster withModel(String model) {
        PharosCluster base = TestClusters.minio();
        PharosClusterSpec s = base.getSpec();
        return TestClusters.withSpec(base, new PharosClusterSpec(s.version(), s.storage(),
                model == null ? null : new PharosClusterSpec.Embeddings(model), s.projects(), s.writers(), s.readers(),
                s.router(), s.autoscaling(), s.pharosConfig()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"granite-r2", "jina-v2-code"})
    void rendersTheBlockPharosEmbeddingsEnableWrites(String model) throws Exception {
        ObjectNode block = JSON.createObjectNode();
        EmbeddingPresets.block(model).forEach(block::set);
        assertThat(block).isEqualTo(fixture(model));

        ConfigRenderer.Result r = ConfigRenderer.render(withModel(model));
        assertThat(r.errors()).isEmpty();
        for (Role role : new Role[]{Role.WRITER, Role.READER}) {
            JsonNode file = JSON.readTree(r.get(role).json());
            assertThat(file.get("embeddingProviders")).isEqualTo(fixture(model).get("embeddingProviders"));
            assertThat(file.get("searchEmbeddingModel")).isEqualTo(fixture(model).get("searchEmbeddingModel"));
            assertThat(file.has("cluster")).isTrue();
        }
        JsonNode router = JSON.readTree(r.get(Role.ROUTER).json());
        assertThat(router.has("embeddingProviders")).isFalse();
        assertThat(router.has("searchEmbeddingModel")).isFalse();
    }

    @Test
    void noneOrAbsentRendersNothing() throws Exception {
        for (String model : new String[]{"none", null}) {
            for (ConfigRenderer.RoleConfig rc : ConfigRenderer.render(withModel(model)).roles().values()) {
                JsonNode file = JSON.readTree(rc.json());
                assertThat(file.has("embeddingProviders")).isFalse();
                assertThat(file.has("searchEmbeddingModel")).isFalse();
            }
        }
    }

    @Test
    void unknownModelIsRejected() {
        assertThat(SpecValidator.validate(withModel("granite-r3").getSpec())).singleElement().asString()
                .isEqualTo("spec.embeddings.model: must be one of [granite-r2, jina-v2-code, none]"
                        + " (the models of `pharos embeddings enable`), got 'granite-r3'");
        assertThat(SpecValidator.validate(withModel("none").getSpec())).isEmpty();
        assertThat(SpecValidator.validate(withModel("jina-v2-code").getSpec())).isEmpty();
    }
}
