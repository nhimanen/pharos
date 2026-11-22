package com.pharos.operator.render;

import com.pharos.operator.OperatorSettings;
import com.pharos.operator.TestClusters;
import com.pharos.operator.api.PharosCluster;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class StorageRendererTest {

    private static final OperatorSettings SETTINGS = new OperatorSettings("pharos", "minio:pinned", "mc:pinned");

    private static String decode(String b64) {
        return new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
    }

    private static EnvVar env(Container c, String name) {
        return c.getEnv().stream().filter(e -> e.getName().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void minioModeCreatesBucketJobAndCredentials() {
        PharosCluster c = TestClusters.minio();
        Map<String, HasMetadata> objects = StorageRenderer.render(c, SETTINGS);
        assertThat(objects).containsOnlyKeys(DesiredState.MINIO, DesiredState.MINIO_SERVICE, DesiredState.MINIO_PVC,
                DesiredState.MINIO_BUCKET);

        StatefulSet sts = (StatefulSet) objects.get(DesiredState.MINIO);
        assertThat(sts.getMetadata().getName()).isEqualTo("demo-minio");
        assertThat(sts.getSpec().getReplicas()).isEqualTo(1);
        Container minio = sts.getSpec().getTemplate().getSpec().getContainers().getFirst();
        assertThat(minio.getImage()).isEqualTo("minio:pinned");
        assertThat(env(minio, "MINIO_ROOT_USER").getValueFrom().getSecretKeyRef().getName()).isEqualTo("demo-minio");
        assertThat(env(minio, "MINIO_ROOT_PASSWORD").getValueFrom().getSecretKeyRef().getKey()).isEqualTo("secretKey");
        assertThat(sts.getSpec().getTemplate().getSpec().getVolumes())
                .anySatisfy(v -> assertThat(v.getPersistentVolumeClaim().getClaimName()).isEqualTo("demo-minio"));
        assertThat(sts.getSpec().getTemplate().getSpec().getSecurityContext().getFsGroup()).isEqualTo(10001L);

        PersistentVolumeClaim pvc = (PersistentVolumeClaim) objects.get(DesiredState.MINIO_PVC);
        assertThat(pvc.getMetadata().getName()).isEqualTo("demo-minio");
        assertThat(pvc.getSpec().getResources().getRequests().get("storage").toString()).isEqualTo("20Gi");
        Service svc = (Service) objects.get(DesiredState.MINIO_SERVICE);
        assertThat(svc.getSpec().getPorts().getFirst().getPort()).isEqualTo(9000);

        Job job = (Job) objects.get(DesiredState.MINIO_BUCKET);
        assertThat(job.getMetadata().getName()).isEqualTo("demo-minio-bucket");
        Container mc = job.getSpec().getTemplate().getSpec().getContainers().getFirst();
        assertThat(mc.getImage()).isEqualTo("mc:pinned");
        assertThat(String.join(" ", mc.getCommand())).contains("mc mb --ignore-existing pharos/pharos"); // idempotent
        assertThat(env(mc, "MINIO_ACCESS_KEY").getValueFrom().getSecretKeyRef().getName()).isEqualTo("demo-minio");
        assertThat(env(mc, "MC_HOST_pharos").getValue())
                .isEqualTo("http://$(MINIO_ACCESS_KEY):$(MINIO_SECRET_KEY)@demo-minio.pharos.svc:9000");

        // The credentials: generated, alphanumeric (URL-safe for MC_HOST_*), different every time they are created.
        Secret creds = StorageRenderer.minioCredentials(c, new Random(7));
        assertThat(creds.getMetadata().getName()).isEqualTo("demo-minio");
        String access = decode(creds.getData().get(StorageRenderer.ACCESS_KEY));
        String secret = decode(creds.getData().get(StorageRenderer.SECRET_KEY));
        assertThat(access).matches("[A-Za-z0-9]{20}");
        assertThat(secret).matches("[A-Za-z0-9]{40}");
        assertThat(decode(StorageRenderer.minioCredentials(c, new Random(8)).getData().get(StorageRenderer.SECRET_KEY)))
                .isNotEqualTo(secret);
        assertThat(StorageRenderer.objectStore(c).getEndpoint()).isEqualTo("http://demo-minio.pharos.svc:9000");
    }

    @Test
    void s3ModeReferencesSecretOnly() {
        PharosCluster c = TestClusters.s3();
        assertThat(StorageRenderer.render(c, SETTINGS)).isEmpty(); // no MinIO, no Secret of the operator's
        assertThat(StorageRenderer.credentialsSecret(c)).isEqualTo("pharos-s3");
        List<EnvVar> env = StorageRenderer.credentialsEnv(c);
        assertThat(env).extracting(EnvVar::getName).containsExactly("PHAROS_S3_ACCESS_KEY", "PHAROS_S3_SECRET_KEY");
        assertThat(env).allSatisfy(e -> {
            assertThat(e.getValue()).isNull();
            assertThat(e.getValueFrom().getSecretKeyRef().getName()).isEqualTo("pharos-s3");
        });
        assertThat(env).extracting(e -> e.getValueFrom().getSecretKeyRef().getKey()).containsExactly("accessKey", "secretKey");

        // pharos is told only the names of those env vars
        var os = StorageRenderer.objectStore(c);
        assertThat(os.getAccessKeyEnv()).isEqualTo("PHAROS_S3_ACCESS_KEY");
        assertThat(os.getSecretKeyEnv()).isEqualTo("PHAROS_S3_SECRET_KEY");

        // and every pod gets them
        ConfigRenderer.Result configs = ConfigRenderer.render(c);
        DesiredState s = DesiredState.render(c, configs, SETTINGS, 1, 2);
        assertThat(s.objects().values()).noneMatch(o -> o instanceof Secret);
        StatefulSet writers = s.get(DesiredState.WRITERS, StatefulSet.class);
        assertThat(writers.getSpec().getTemplate().getSpec().getContainers().getFirst().getEnv())
                .anySatisfy(e -> {
                    assertThat(e.getName()).isEqualTo("PHAROS_S3_SECRET_KEY");
                    assertThat(e.getValueFrom().getSecretKeyRef().getName()).isEqualTo("pharos-s3");
                });
    }
}
