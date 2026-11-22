package com.pharos.operator.render;

import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.render.ConfigRenderer.Role;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A project's one-shot Jobs, run with the pharos image and the writer config (bucket and token) on a
 * pod shaped like a writer's: same security context, the token and object-store keys only as env from
 * Secrets, the writer ConfigMap filled in by the same init container. They only change the shard
 * catalog, so they need no writer running and no git credentials.
 * <ul>
 *   <li>registration, {@code <name>-register-<k8sName>-<specHash>}: {@code pharos index --project <name>
 *       --source-git <url> --ref <ref> [--no-embed]} records the project's source in the catalog; the
 *       writers' work queue then indexes it. The hash covers url, ref and embed, so a changed source
 *       gets a new Job and registers again (Jobs are create-only).</li>
 *   <li>removal, {@code <name>-remove-<k8sName>}: {@code pharos remove-index --from-catalog <name>} drops
 *       the catalog entry; writers release the shard and readers stop serving it. Its data in the
 *       bucket is kept.</li>
 * </ul>
 * The raw project name is in the {@link Labels#PROJECT_ANNOTATION} annotation; object names carry only
 * {@link Names#k8sName}.
 */
public final class ProjectJobRenderer {

    /** Pod attempts within one Job before it fails (the operator then retries the Job with a backoff). */
    public static final int BACKOFF_LIMIT = 3;
    /** A Job still running after this long fails, so a hung store call is retried rather than waited on. */
    public static final long ACTIVE_DEADLINE_SECONDS = 600;

    private ProjectJobRenderer() {}

    /** {@link Names#SPEC_HASH_LENGTH} hex characters of url, ref and embed: what the catalog records. */
    public static String specHash(PharosClusterSpec.Project project) {
        String text = project.git().url() + "\n" + project.git().ref() + "\n" + embed(project);
        return WorkloadRenderer.sha256(text).substring(0, Names.SPEC_HASH_LENGTH);
    }

    private static boolean embed(PharosClusterSpec.Project project) {
        return project.embed() == null || project.embed();
    }

    /** The registration Job of {@code project}'s current source, with the image of {@code spec.version}. */
    public static Job registration(PharosCluster cluster, PharosClusterSpec.Project project, OperatorSettings settings) {
        return registration(cluster, project, settings, cluster.getSpec().version());
    }

    /**
     * The registration Job with the image of {@code version}: the writers' (during an upgrade, the
     * version the writers run until their turn comes).
     */
    public static Job registration(PharosCluster cluster, PharosClusterSpec.Project project, OperatorSettings settings,
                                   String version) {
        String name = cluster.getMetadata().getName();
        List<String> args = new ArrayList<>(List.of("index", "--project", project.name(),
                "--source-git", project.git().url(), "--ref", project.git().ref()));
        if (!embed(project)) args.add("--no-embed");
        return job(cluster, settings, version, Names.registerJob(name, project.name(), specHash(project)),
                Labels.REGISTER, Map.of(Labels.PROJECT_ANNOTATION, project.name()), args);
    }

    /** The removal Job of {@code project} (a name no longer in the spec). */
    public static Job removal(PharosCluster cluster, String project, OperatorSettings settings) {
        return removal(cluster, project, settings, cluster.getSpec().version());
    }

    /** The removal Job with the image of {@code version} (the writers'). */
    public static Job removal(PharosCluster cluster, String project, OperatorSettings settings, String version) {
        return job(cluster, settings, version, Names.removeJob(cluster.getMetadata().getName(), project),
                Labels.REMOVE, Map.of(Labels.PROJECT_ANNOTATION, project),
                List.of("remove-index", "--from-catalog", project));
    }

    /**
     * A one-shot pharos Job on a writer-shaped pod (see the class comment) running {@code args} with the
     * image of {@code version}; {@code annotations} go on the Job and its pod.
     */
    static Job job(PharosCluster cluster, OperatorSettings settings, String version, String jobName,
                   String component, Map<String, String> annotations, List<String> args) {
        String name = cluster.getMetadata().getName();
        String image = settings.pharosImage(version);
        List<EnvVar> env = new ArrayList<>();
        env.add(plain("PHAROS_ROLE", Role.WRITER.id()));
        env.add(plain("PHAROS_HOME", ConfigRenderer.PHAROS_HOME));
        env.add(plain("PHAROS_CONFIG", WorkloadRenderer.CONFIG_MOUNT + "/" + ConfigRenderer.CONFIG_KEY));
        env.add(StorageRenderer.secretEnv(ConfigRenderer.TOKEN_ENV, Names.token(name), WorkloadRenderer.TOKEN_KEY));
        env.addAll(StorageRenderer.credentialsEnv(cluster));
        return new JobBuilder()
                .withNewMetadata().withName(jobName).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, component))
                .withAnnotations(annotations).endMetadata()
                .withNewSpec()
                .withBackoffLimit(BACKOFF_LIMIT)
                .withActiveDeadlineSeconds(ACTIVE_DEADLINE_SECONDS)
                .withNewTemplate()
                .withNewMetadata().withLabels(Labels.of(name, component))
                .withAnnotations(annotations).endMetadata()
                .withNewSpec()
                .withRestartPolicy("Never")
                .withAutomountServiceAccountToken(false)
                .withSecurityContext(WorkloadRenderer.podSecurity())
                .withInitContainers(WorkloadRenderer.podConfigInit(image))
                .addNewContainer()
                .withName("pharos")
                .withImage(image)
                .withArgs(args)
                .withEnv(env)
                .withNewResources()
                .addToRequests("cpu", new Quantity("100m"))
                .addToRequests("memory", new Quantity("512Mi"))
                .addToLimits("memory", new Quantity("1Gi"))
                .endResources()
                .withSecurityContext(WorkloadRenderer.containerSecurity())
                .addNewVolumeMount().withName("config").withMountPath(WorkloadRenderer.CONFIG_MOUNT).withReadOnly(true)
                .endVolumeMount()
                .addNewVolumeMount().withName("data").withMountPath("/data").endVolumeMount()
                .addNewVolumeMount().withName("tmp").withMountPath("/tmp").endVolumeMount()
                .endContainer()
                .addNewVolume().withName("config-template").withNewConfigMap()
                .withName(Names.config(name, Role.WRITER.id())).endConfigMap().endVolume()
                .addNewVolume().withName("config").withNewEmptyDir().endEmptyDir().endVolume()
                .addNewVolume().withName("data").withNewEmptyDir().endEmptyDir().endVolume()
                .addNewVolume().withName("tmp").withNewEmptyDir().endEmptyDir().endVolume()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }

    private static EnvVar plain(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }
}
