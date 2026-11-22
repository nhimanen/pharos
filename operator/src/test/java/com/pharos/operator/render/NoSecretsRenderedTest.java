package com.pharos.operator.render;

import com.pharos.operator.OperatorSettings;
import com.pharos.operator.TestClusters;
import com.pharos.operator.api.PharosCluster;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review Focus 4: the token and the object-store keys never appear in any object the operator renders
 * other than their own Secret: not in a ConfigMap, a pod template, a Job, or any label or annotation.
 * (The reconcile test scans the status, Events and the operator's logs.)
 */
class NoSecretsRenderedTest {

    private static final OperatorSettings SETTINGS = OperatorSettings.defaults();

    /** Every rendered object of {@code cluster}, the generated Secrets included. */
    static List<HasMetadata> renderAll(PharosCluster cluster, Random random) {
        ConfigRenderer.Result configs = ConfigRenderer.render(cluster);
        assertThat(configs.errors()).isEmpty();
        List<HasMetadata> all = new ArrayList<>(DesiredState.render(cluster, configs, SETTINGS, 2, 3).objects().values());
        // the project Jobs the reconciler creates on the side
        for (var p : cluster.getSpec().projects()) {
            all.add(ProjectJobRenderer.registration(cluster, p, SETTINGS));
            all.add(ProjectJobRenderer.removal(cluster, p.name(), SETTINGS));
        }
        all.add(DesiredState.secret(DesiredState.TOKEN, cluster, random));
        if (StorageRenderer.minioMode(cluster.getSpec())) {
            all.add(DesiredState.secret(DesiredState.MINIO_CREDENTIALS, cluster, random));
        }
        return all;
    }

    /** Each secret value in its plain and base64 forms. */
    static List<String> secretForms(List<HasMetadata> objects) {
        List<String> forms = new ArrayList<>();
        for (HasMetadata o : objects) {
            if (o instanceof Secret s) {
                s.getData().values().forEach(b64 -> {
                    forms.add(b64);
                    forms.add(new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8));
                });
            }
        }
        return forms;
    }

    @ParameterizedTest
    @ValueSource(strings = {"minio.yaml", "s3.yaml"})
    void noSecretAppearsInAnyRenderedObject(String example) {
        PharosCluster cluster = TestClusters.load(example);
        List<HasMetadata> all = renderAll(cluster, new Random(42));
        List<String> secrets = secretForms(all);
        assertThat(secrets).hasSize(StorageRenderer.minioMode(cluster.getSpec()) ? 6 : 2);
        assertThat(secrets).allSatisfy(s -> assertThat(s).hasSizeGreaterThanOrEqualTo(20));

        for (HasMetadata o : all) {
            if (o instanceof Secret s) {
                // a Secret holds only its own values, and never in its metadata
                List<String> own = secretForms(List.of(s));
                String meta = Serialization.asYaml(s.getMetadata());
                secrets.forEach(v -> assertThat(meta).doesNotContain(v));
                secrets.stream().filter(v -> !own.contains(v))
                        .forEach(v -> assertThat(Serialization.asYaml(s)).doesNotContain(v));
                continue;
            }
            String yaml = Serialization.asYaml(o);
            for (String v : secrets) {
                assertThat(yaml).as("%s %s", o.getKind(), o.getMetadata().getName()).doesNotContain(v);
            }
        }
        // The config files themselves name the env vars only.
        for (ConfigRenderer.RoleConfig rc : ConfigRenderer.render(cluster).roles().values()) {
            assertThat(rc.json()).contains("\"tokenEnv\" : \"PHAROS_TOKEN\"").doesNotContain("\"token\" :");
            assertThat(rc.json()).contains("\"accessKeyEnv\" : \"PHAROS_S3_ACCESS_KEY\"");
            secrets.forEach(v -> assertThat(rc.json()).doesNotContain(v));
        }
    }
}
