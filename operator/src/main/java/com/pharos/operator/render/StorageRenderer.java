package com.pharos.operator.render;

import com.pharos.config.ClusterConfig;
import com.pharos.operator.Defaults;
import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.random.RandomGenerator;

/**
 * The bucket. In minio mode the operator runs MinIO itself: a one-replica StatefulSet on a PVC of
 * {@code storage.minio.storage}, its Service, a generated credentials Secret and a Job that creates
 * the bucket (idempotent: {@code mc mb --ignore-existing}). In s3 mode it creates nothing: pods
 * reference the user's {@code credentialsSecret} (keys {@code accessKey}, {@code secretKey}) as env.
 * Either way the keys reach pharos only as env named by {@code accessKeyEnv}/{@code secretKeyEnv}.
 */
public final class StorageRenderer {

    /** The keys of a credentials Secret: the generated MinIO one and the user's S3 one alike. */
    public static final String ACCESS_KEY = "accessKey";
    public static final String SECRET_KEY = "secretKey";
    /** The bucket in minio mode. */
    public static final String MINIO_BUCKET = "pharos";
    public static final int MINIO_PORT = 9000;
    static final String MINIO_REGION = "us-east-1";

    private static final char[] ALNUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private StorageRenderer() {}

    public static boolean minioMode(PharosClusterSpec spec) {
        return spec.storage() != null && spec.storage().minio() != null;
    }

    /** {@code cluster.objectStore}: endpoint, bucket and the names of the key env vars (never the keys). */
    public static ClusterConfig.ObjectStoreConfig objectStore(PharosCluster cluster) {
        PharosClusterSpec spec = cluster.getSpec();
        ClusterConfig.ObjectStoreConfig o = new ClusterConfig.ObjectStoreConfig();
        if (minioMode(spec)) {
            o.setEndpoint("http://" + Names.serviceHost(Names.minio(cluster.getMetadata().getName()),
                    cluster.getMetadata().getNamespace()) + ":" + MINIO_PORT);
            o.setBucket(MINIO_BUCKET);
            o.setRegion(MINIO_REGION);
            o.setPathStyle(true);
        } else {
            PharosClusterSpec.S3 s3 = spec.storage().s3();
            o.setEndpoint(s3.endpoint());
            o.setBucket(s3.bucket());
            if (s3.region() != null) o.setRegion(s3.region());
            if (s3.pathStyle() != null) o.setPathStyle(s3.pathStyle());
        }
        o.setAccessKeyEnv(ConfigRenderer.ACCESS_KEY_ENV);
        o.setSecretKeyEnv(ConfigRenderer.SECRET_KEY_ENV);
        return o;
    }

    /** The Secret holding the object-store keys: the generated MinIO one, or the user's S3 one. */
    public static String credentialsSecret(PharosCluster cluster) {
        PharosClusterSpec spec = cluster.getSpec();
        return minioMode(spec) ? Names.minio(cluster.getMetadata().getName()) : spec.storage().s3().credentialsSecret();
    }

    /** The two env vars pharos reads the keys from, as references into {@link #credentialsSecret}. */
    public static List<EnvVar> credentialsEnv(PharosCluster cluster) {
        String secret = credentialsSecret(cluster);
        return List.of(secretEnv(ConfigRenderer.ACCESS_KEY_ENV, secret, ACCESS_KEY),
                secretEnv(ConfigRenderer.SECRET_KEY_ENV, secret, SECRET_KEY));
    }

    static EnvVar secretEnv(String name, String secret, String key) {
        return new EnvVarBuilder().withName(name)
                .withNewValueFrom().withNewSecretKeyRef().withName(secret).withKey(key).endSecretKeyRef().endValueFrom()
                .build();
    }

    /**
     * The minio-mode objects (spec sizes) other than the credentials Secret, by dependent name: StatefulSet, Service,
     * PVC and the bucket Job. Empty in s3 mode.
     */
    public static Map<String, HasMetadata> render(PharosCluster cluster, OperatorSettings settings) {
        return render(cluster, settings, Defaults.minioStorage(cluster.getSpec()));
    }

    /** As {@link #render(PharosCluster, OperatorSettings)}, with the MinIO PVC at {@code minioStorage}. */
    public static Map<String, HasMetadata> render(PharosCluster cluster, OperatorSettings settings, String minioStorage) {
        Map<String, HasMetadata> out = new LinkedHashMap<>();
        if (!minioMode(cluster.getSpec())) return out;
        out.put(DesiredState.MINIO, minioStatefulSet(cluster, settings));
        out.put(DesiredState.MINIO_SERVICE, minioService(cluster));
        out.put(DesiredState.MINIO_PVC, minioPvc(cluster, minioStorage));
        out.put(DesiredState.MINIO_BUCKET, bucketJob(cluster, settings));
        return out;
    }

    /**
     * The generated MinIO credentials: created once, never updated (the dependent only creates), so a
     * reconcile never rotates them. Alphanumeric, so they fit an {@code MC_HOST_*} URL.
     */
    public static Secret minioCredentials(PharosCluster cluster, RandomGenerator random) {
        String name = Names.minio(cluster.getMetadata().getName());
        return new SecretBuilder()
                .withNewMetadata().withName(name).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(cluster.getMetadata().getName(), Labels.MINIO)).endMetadata()
                .withType("Opaque")
                .addToData(ACCESS_KEY, base64(randomString(random, 20)))
                .addToData(SECRET_KEY, base64(randomString(random, 40)))
                .build();
    }

    /** Secret values: {@link SecureRandom} unless a test supplies its own. */
    static String randomString(RandomGenerator random, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) sb.append(ALNUM[random.nextInt(ALNUM.length)]);
        return sb.toString();
    }

    static String base64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    static StatefulSet minioStatefulSet(PharosCluster cluster, OperatorSettings settings) {
        String name = cluster.getMetadata().getName();
        String minio = Names.minio(name);
        String secret = minio;
        return new StatefulSetBuilder()
                .withNewMetadata().withName(minio).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.MINIO)).endMetadata()
                .withNewSpec()
                .withReplicas(1)
                .withServiceName(minio)
                .withNewSelector().withMatchLabels(Labels.selector(name, Labels.MINIO)).endSelector()
                .withNewTemplate()
                .withNewMetadata().withLabels(Labels.of(name, Labels.MINIO)).endMetadata()
                .withNewSpec()
                .withAutomountServiceAccountToken(false)
                .withSecurityContext(WorkloadRenderer.podSecurity())
                .addNewContainer()
                .withName("minio")
                .withImage(settings.minioImage())
                .withArgs("server", "/data")
                .withEnv(secretEnv("MINIO_ROOT_USER", secret, ACCESS_KEY),
                        secretEnv("MINIO_ROOT_PASSWORD", secret, SECRET_KEY),
                        // uid 10001 has no home directory; MinIO keeps its certs dir under $HOME
                        new EnvVarBuilder().withName("HOME").withValue("/tmp").build())
                .addNewPort().withName("s3").withContainerPort(MINIO_PORT).withProtocol("TCP").endPort()
                .withNewReadinessProbe().withNewHttpGet().withPath("/minio/health/ready")
                .withPort(new IntOrString("s3")).endHttpGet()
                .withPeriodSeconds(10).withTimeoutSeconds(5).withFailureThreshold(3).endReadinessProbe()
                .withNewLivenessProbe().withNewHttpGet().withPath("/minio/health/live")
                .withPort(new IntOrString("s3")).endHttpGet()
                .withPeriodSeconds(20).withTimeoutSeconds(10).withFailureThreshold(6).endLivenessProbe()
                .withSecurityContext(WorkloadRenderer.containerSecurity())
                .addNewVolumeMount().withName("data").withMountPath("/data").endVolumeMount()
                .addNewVolumeMount().withName("tmp").withMountPath("/tmp").endVolumeMount()
                .endContainer()
                .addNewVolume().withName("data").withNewPersistentVolumeClaim().withClaimName(minio)
                .endPersistentVolumeClaim().endVolume()
                .addNewVolume().withName("tmp").withNewEmptyDir().endEmptyDir().endVolume()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }

    static Service minioService(PharosCluster cluster) {
        String name = cluster.getMetadata().getName();
        return new ServiceBuilder()
                .withNewMetadata().withName(Names.minio(name)).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.MINIO)).endMetadata()
                .withNewSpec()
                .withType("ClusterIP")
                .withSelector(Labels.selector(name, Labels.MINIO))
                .addNewPort().withName("s3").withPort(MINIO_PORT).withTargetPort(new IntOrString("s3"))
                .withProtocol("TCP").endPort()
                .endSpec()
                .build();
    }

    static PersistentVolumeClaim minioPvc(PharosCluster cluster, String size) {
        String name = cluster.getMetadata().getName();
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata().withName(Names.minio(name)).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.MINIO)).endMetadata()
                .withNewSpec()
                .withAccessModes("ReadWriteOnce")
                .withNewResources().addToRequests("storage", new Quantity(size))
                .endResources()
                .endSpec()
                .build();
    }

    /**
     * Creates the bucket once MinIO answers. Idempotent ({@code --ignore-existing}), so a re-run or a
     * re-created Job is harmless. The keys reach {@code mc} only through env: {@code MC_HOST_pharos} is
     * composed by Kubernetes from the two secret-backed variables, never written into this object.
     */
    static Job bucketJob(PharosCluster cluster, OperatorSettings settings) {
        String name = cluster.getMetadata().getName();
        String minio = Names.minio(name);
        String host = Names.serviceHost(minio, cluster.getMetadata().getNamespace()) + ":" + MINIO_PORT;
        return new JobBuilder()
                .withNewMetadata().withName(Names.minioBucketJob(name)).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.MINIO_BUCKET)).endMetadata()
                .withNewSpec()
                .withBackoffLimit(6)
                .withNewTemplate()
                .withNewMetadata().withLabels(Labels.of(name, Labels.MINIO_BUCKET)).endMetadata()
                .withNewSpec()
                .withRestartPolicy("OnFailure")
                .withAutomountServiceAccountToken(false)
                .withSecurityContext(WorkloadRenderer.podSecurity())
                .addNewContainer()
                .withName("mc")
                .withImage(settings.mcImage())
                .withCommand("/bin/sh", "-c",
                        "until mc mb --ignore-existing pharos/" + MINIO_BUCKET
                                + "; do echo 'waiting for MinIO'; sleep 5; done")
                .withEnv(secretEnv("MINIO_ACCESS_KEY", minio, ACCESS_KEY),
                        secretEnv("MINIO_SECRET_KEY", minio, SECRET_KEY),
                        // $(VAR) is expanded by Kubernetes from the variables above, inside the pod only.
                        new EnvVarBuilder().withName("MC_HOST_pharos")
                                .withValue("http://$(MINIO_ACCESS_KEY):$(MINIO_SECRET_KEY)@" + host).build(),
                        new EnvVarBuilder().withName("HOME").withValue("/tmp").build(),
                        new EnvVarBuilder().withName("MC_CONFIG_DIR").withValue("/tmp/.mc").build())
                .withSecurityContext(WorkloadRenderer.containerSecurity())
                .addNewVolumeMount().withName("tmp").withMountPath("/tmp").endVolumeMount()
                .endContainer()
                .addNewVolume().withName("tmp").withNewEmptyDir().endEmptyDir().endVolume()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }
}
