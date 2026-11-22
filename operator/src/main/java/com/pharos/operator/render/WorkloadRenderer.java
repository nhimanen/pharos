package com.pharos.operator.render;

import com.pharos.operator.Defaults;
import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.render.ConfigRenderer.Role;
import com.pharos.operator.render.ConfigRenderer.RoleConfig;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.random.RandomGenerator;

/**
 * The pharos workloads: the role ConfigMaps, the writer and reader StatefulSets with their headless
 * Services, the router Deployment and Service, the PodDisruptionBudgets, the NetworkPolicy and the
 * token Secret.
 *
 * <p>Every pharos pod runs as uid/gid 10001 with {@code fsGroup} 10001 (the image's contract), gets
 * the token and the object-store keys only as env from Secrets, and has
 * {@code terminationGracePeriodSeconds} = {@code drainSeconds} + 30 so pharos's SIGTERM drain finishes.
 * Each pod's own name (its node id, and the host of its advertised URL) is filled into its config by
 * an init container, since a ConfigMap is shared by the role.
 *
 * <p>Every writer mounts each project's {@code git.credentialsSecret} read-only at
 * {@code /etc/pharos/git/<k8sName>} ({@link ConfigRenderer#gitCredentialsDir}); the writer config's
 * {@code writer.gitCredentials} points that project's git calls at it.
 */
public final class WorkloadRenderer {

    /** The image's uid, gid and volume group (docker/pharos/Dockerfile). */
    public static final long UID = 10001;
    /** Added to {@code cluster.drainSeconds} for the pod's termination grace period. */
    public static final int GRACE_MARGIN_SECONDS = 30;
    /** Pod-template annotation: a hash of the role's config, so a config change rolls the pods. */
    public static final String CONFIG_HASH = "pharos.dev/config-hash";
    /** The token's key in the {@code <name>-token} Secret. */
    public static final String TOKEN_KEY = "token";

    static final String CONFIG_MOUNT = "/etc/pharos";
    static final String CONFIG_TEMPLATE_MOUNT = "/etc/pharos-template";
    /** Git credentials files: readable by the owner and the pod's group (fsGroup), as ssh requires of a key. */
    static final int GIT_CREDENTIALS_MODE = 0440;

    private WorkloadRenderer() {}

    /** The cluster's workload objects by dependent name (see {@link DesiredState}), all on {@code spec.version}. */
    public static Map<String, HasMetadata> render(PharosCluster cluster, ConfigRenderer.Result configs,
                                                  OperatorSettings settings, DesiredState.Sizing sizing) {
        return render(cluster, configs, settings, sizing, DesiredState.Versions.of(cluster));
    }

    /** The cluster's workload objects by dependent name, each workload on its version of {@code versions}. */
    public static Map<String, HasMetadata> render(PharosCluster cluster, ConfigRenderer.Result configs,
                                                  OperatorSettings settings, DesiredState.Sizing sizing,
                                                  DesiredState.Versions versions) {
        Map<String, HasMetadata> out = new LinkedHashMap<>();
        out.put(DesiredState.CONFIG_WRITER, configMap(cluster, configs.get(Role.WRITER)));
        out.put(DesiredState.CONFIG_READER, configMap(cluster, configs.get(Role.READER)));
        out.put(DesiredState.CONFIG_ROUTER, configMap(cluster, configs.get(Role.ROUTER)));
        StatefulSet writers = statefulSet(cluster, configs.get(Role.WRITER), settings, versions.writers(),
                sizing.writerReplicas(), sizing.workVolume());
        annotate(writers.getMetadata(), Labels.LAST_SCALE, sizing.writerLastScale());
        annotate(writers.getMetadata(), Labels.VERSION_SINCE, versions.writersSince());
        out.put(DesiredState.WRITERS, writers);
        StatefulSet readers = statefulSet(cluster, configs.get(Role.READER), settings, versions.readers(),
                sizing.readerReplicas(), sizing.cacheVolume());
        annotate(readers.getMetadata(), Labels.LAST_SCALE, sizing.readerLastScale());
        annotate(readers.getMetadata(), Labels.VERSION_SINCE, versions.readersSince());
        out.put(DesiredState.READERS, readers);
        Deployment router = router(cluster, configs.get(Role.ROUTER), settings, versions.router());
        annotate(router.getMetadata(), Labels.VERSION_SINCE, versions.routerSince());
        out.put(DesiredState.ROUTER, router);
        out.put(DesiredState.WRITERS_HEADLESS, headless(cluster, Role.WRITER));
        out.put(DesiredState.READERS_HEADLESS, headless(cluster, Role.READER));
        out.put(DesiredState.ROUTER_SERVICE, routerService(cluster));
        out.put(DesiredState.PDB_WRITERS, pdb(cluster, Role.WRITER));
        out.put(DesiredState.PDB_READERS, pdb(cluster, Role.READER));
        out.put(DesiredState.NETWORK_POLICY, networkPolicy(cluster));
        return out;
    }

    /**
     * The cluster token: 32 random bytes as hex (one printable word, as pharos requires). Created once
     * and never updated (the dependent only creates), so no reconcile regenerates or rotates it.
     */
    public static Secret tokenSecret(PharosCluster cluster, RandomGenerator random) {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        String name = cluster.getMetadata().getName();
        return new SecretBuilder()
                .withNewMetadata().withName(Names.token(name)).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.TOKEN)).endMetadata()
                .withType("Opaque")
                .addToData(TOKEN_KEY, StorageRenderer.base64(HexFormat.of().formatHex(bytes)))
                .build();
    }

    static ConfigMap configMap(PharosCluster cluster, RoleConfig rc) {
        String name = cluster.getMetadata().getName();
        return new ConfigMapBuilder()
                .withNewMetadata().withName(Names.config(name, rc.role().id()))
                .withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.CONFIG)).endMetadata()
                .addToData(ConfigRenderer.CONFIG_KEY, rc.json())
                .build();
    }

    /**
     * An annotation on a workload's own metadata (not its pod template, so it rolls nothing), applied
     * together with what it explains: the autoscaler's last application with {@code replicas} (ruling
     * OP-R5), the version's start with the image. Null adds nothing.
     */
    static void annotate(io.fabric8.kubernetes.api.model.ObjectMeta meta, String key, String value) {
        if (value == null) return;
        Map<String, String> a = new java.util.TreeMap<>(meta.getAnnotations() == null ? Map.of() : meta.getAnnotations());
        a.put(key, value);
        meta.setAnnotations(a);
    }

    static StatefulSet statefulSet(PharosCluster cluster, RoleConfig rc, OperatorSettings settings, int replicas,
                                   String size) {
        return statefulSet(cluster, rc, settings, cluster.getSpec().version(), replicas, size);
    }

    static StatefulSet statefulSet(PharosCluster cluster, RoleConfig rc, OperatorSettings settings, String version,
                                   int replicas, String size) {
        String name = cluster.getMetadata().getName();
        boolean writer = rc.role() == Role.WRITER;
        String component = writer ? Labels.WRITER : Labels.READER;
        String claim = writer ? "work" : "cache";
        PharosClusterSpec.RoleSpec role = writer ? cluster.getSpec().writers() : cluster.getSpec().readers();
        PersistentVolumeClaim template = new PersistentVolumeClaimBuilder()
                .withNewMetadata().withName(claim).withLabels(Labels.of(name, component)).endMetadata()
                .withNewSpec()
                .withAccessModes("ReadWriteOnce")
                .withNewResources().addToRequests("storage", new Quantity(size)).endResources()
                .endSpec()
                .build();
        return new StatefulSetBuilder()
                .withNewMetadata().withName(writer ? Names.writers(name) : Names.readers(name))
                .withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, component)).endMetadata()
                .withNewSpec()
                .withReplicas(replicas)
                .withServiceName(Names.headless(name, rc.role().id()))
                .withPodManagementPolicy("Parallel")
                .withNewUpdateStrategy().withType("RollingUpdate").endUpdateStrategy()
                .withNewSelector().withMatchLabels(Labels.selector(name, component)).endSelector()
                .withTemplate(podTemplate(cluster, rc, settings, version, component, claim,
                        role == null ? null : role.resources()))
                .withVolumeClaimTemplates(template)
                .endSpec()
                .build();
    }

    static Deployment router(PharosCluster cluster, RoleConfig rc, OperatorSettings settings) {
        return router(cluster, rc, settings, cluster.getSpec().version());
    }

    static Deployment router(PharosCluster cluster, RoleConfig rc, OperatorSettings settings, String version) {
        String name = cluster.getMetadata().getName();
        return new DeploymentBuilder()
                .withNewMetadata().withName(Names.router(name)).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.ROUTER)).endMetadata()
                .withNewSpec()
                .withReplicas(Defaults.routerReplicas(cluster.getSpec()))
                .withNewSelector().withMatchLabels(Labels.selector(name, Labels.ROUTER)).endSelector()
                .withTemplate(podTemplate(cluster, rc, settings, version, Labels.ROUTER, null, null))
                .endSpec()
                .build();
    }

    /**
     * A pharos pod. {@code dataClaim}: the StatefulSet's PVC template mounted at {@code /data} (it holds
     * {@code PHAROS_HOME} and the work dir or cache), or null for an emptyDir (the router).
     */
    static PodTemplateSpec podTemplate(PharosCluster cluster, RoleConfig rc, OperatorSettings settings,
                                       String version, String component, String dataClaim,
                                       ResourceRequirements resources) {
        String name = cluster.getMetadata().getName();
        Role role = rc.role();
        int port = role == Role.ROUTER ? ConfigRenderer.ROUTER_PORT : ConfigRenderer.WEB_PORT;
        String image = settings.pharosImage(version);
        String configMap = Names.config(name, role.id());

        List<EnvVar> env = new ArrayList<>();
        env.add(plainEnv("PHAROS_ROLE", role.id()));
        env.add(plainEnv("PHAROS_HOME", ConfigRenderer.PHAROS_HOME));
        env.add(plainEnv("PHAROS_PORT", String.valueOf(port)));
        env.add(plainEnv("PHAROS_CONFIG", CONFIG_MOUNT + "/" + ConfigRenderer.CONFIG_KEY));
        env.add(StorageRenderer.secretEnv(ConfigRenderer.TOKEN_ENV, Names.token(name), TOKEN_KEY));
        env.addAll(StorageRenderer.credentialsEnv(cluster));

        ContainerBuilder main = new ContainerBuilder()
                .withName("pharos")
                .withImage(image)
                .withEnv(env)
                .addNewPort().withName("http").withContainerPort(port).withProtocol("TCP").endPort()
                .withReadinessProbe(new ProbeBuilder()
                        .withNewHttpGet().withPath("/health").withPort(new IntOrString("http")).endHttpGet()
                        .withPeriodSeconds(5).withTimeoutSeconds(3).withFailureThreshold(3).build())
                .withStartupProbe(healthAnswers(port, 10, 60))
                .withLivenessProbe(healthAnswers(port, 20, 6))
                .withSecurityContext(containerSecurity())
                .addNewVolumeMount().withName("config").withMountPath(CONFIG_MOUNT).withReadOnly(true).endVolumeMount()
                .addNewVolumeMount().withName(dataClaim == null ? "data" : dataClaim).withMountPath("/data").endVolumeMount()
                .addNewVolumeMount().withName("tmp").withMountPath("/tmp").endVolumeMount();
        if (resources != null) main.withResources(resources);

        List<Volume> volumes = new ArrayList<>();
        List<Container> init = new ArrayList<>();
        List<String> gitDirs = new ArrayList<>();
        if (role == Role.WRITER) {
            for (PharosClusterSpec.Project p : credentialProjects(cluster)) {
                String volume = Names.gitCredentialsVolume(p.name());
                String dir = ConfigRenderer.gitCredentialsDir(p.name());
                gitDirs.add(dir);
                // optional: a missing Secret fails only that project's git calls, not every writer's start
                volumes.add(new VolumeBuilder().withName(volume).withNewSecret()
                        .withSecretName(p.git().credentialsSecret()).withDefaultMode(GIT_CREDENTIALS_MODE)
                        .withOptional(true).endSecret().build());
                main.addNewVolumeMount().withName(volume).withMountPath(dir).withReadOnly(true).endVolumeMount();
            }
        }
        if (role == Role.ROUTER) {
            // no per-pod values in the router's config: mount the ConfigMap as is
            volumes.add(new VolumeBuilder().withName("config").withNewConfigMap().withName(configMap).endConfigMap().build());
        } else {
            volumes.add(new VolumeBuilder().withName("config-template").withNewConfigMap().withName(configMap)
                    .endConfigMap().build());
            volumes.add(new VolumeBuilder().withName("config").withNewEmptyDir().endEmptyDir().build());
            init.add(podConfigInit(image, gitDirs));
        }
        if (dataClaim == null) volumes.add(new VolumeBuilder().withName("data").withNewEmptyDir().endEmptyDir().build());
        volumes.add(new VolumeBuilder().withName("tmp").withNewEmptyDir().endEmptyDir().build());

        return new PodTemplateSpecBuilder()
                .withNewMetadata().withLabels(Labels.of(name, component))
                .addToAnnotations(CONFIG_HASH, sha256(rc.json()).substring(0, 16)).endMetadata()
                .withNewSpec()
                .withAutomountServiceAccountToken(false)
                .withSecurityContext(podSecurity())
                .withTerminationGracePeriodSeconds((long) rc.drainSeconds() + GRACE_MARGIN_SECONDS)
                .withInitContainers(init)
                .withContainers(main.build())
                .withVolumes(volumes)
                .endSpec()
                .build();
    }

    /** The projects with a {@code git.credentialsSecret}, in spec order. */
    static List<PharosClusterSpec.Project> credentialProjects(PharosCluster cluster) {
        List<PharosClusterSpec.Project> out = new ArrayList<>();
        if (cluster.getSpec().projects() == null) return out;
        for (PharosClusterSpec.Project p : cluster.getSpec().projects()) {
            if (p != null && p.git() != null && p.git().credentialsSecret() != null) out.add(p);
        }
        return out;
    }

    /** {@link #podConfigInit(String, List)} with no git credentials directories. */
    static Container podConfigInit(String image) {
        return podConfigInit(image, List.of());
    }

    /**
     * Copies the role's config into the pod, with {@link ConfigRenderer#POD_NAME_PLACEHOLDER} replaced by
     * the pod's name. The script avoids the {@code $(} sequence, which Kubernetes would expand itself.
     * It also creates the mount points of the git credentials ({@code gitDirs}, under the config
     * directory), so the main container's read-only config mount never needs a directory made in it.
     */
    static Container podConfigInit(String image, List<String> gitDirs) {
        String from = CONFIG_TEMPLATE_MOUNT + "/" + ConfigRenderer.CONFIG_KEY;
        String to = CONFIG_MOUNT + "/" + ConfigRenderer.CONFIG_KEY;
        String script = "sed \"s/[$][(]POD_NAME[)]/${POD_NAME}/g\" " + from + " > " + to
                + (gitDirs.isEmpty() ? "" : " && mkdir -p " + String.join(" ", gitDirs));
        return new ContainerBuilder()
                .withName("pod-config")
                .withImage(image)
                .withCommand("sh", "-c", script)
                .withEnv(new EnvVarBuilder().withName("POD_NAME")
                        .withNewValueFrom().withNewFieldRef().withFieldPath("metadata.name").endFieldRef().endValueFrom()
                        .build())
                .withSecurityContext(containerSecurity())
                .addNewVolumeMount().withName("config-template").withMountPath(CONFIG_TEMPLATE_MOUNT).withReadOnly(true)
                .endVolumeMount()
                .addNewVolumeMount().withName("config").withMountPath(CONFIG_MOUNT).endVolumeMount()
                .build();
    }

    /**
     * Liveness (and startup): {@code /health} answers at all. Its 503 while warming or draining is
     * still an answer, which an {@code httpGet} probe would count as a failure, so the probe sends the
     * request itself and only checks that an HTTP status line comes back.
     */
    static Probe healthAnswers(int port, int periodSeconds, int failureThreshold) {
        String script = "exec 3<>/dev/tcp/127.0.0.1/" + port
                + " && printf 'GET /health HTTP/1.0\\r\\n\\r\\n' >&3 && head -c 5 <&3 | grep -q 'HTTP/'";
        return new ProbeBuilder()
                .withNewExec().withCommand("bash", "-c", script).endExec()
                .withPeriodSeconds(periodSeconds).withTimeoutSeconds(10).withFailureThreshold(failureThreshold)
                .build();
    }

    static Service headless(PharosCluster cluster, Role role) {
        String name = cluster.getMetadata().getName();
        String component = role == Role.WRITER ? Labels.WRITER : Labels.READER;
        return new ServiceBuilder()
                .withNewMetadata().withName(Names.headless(name, role.id()))
                .withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, component)).endMetadata()
                .withNewSpec()
                .withClusterIP("None")
                // pod DNS (the advertised URLs) must resolve while a pod is warming: pharos itself decides
                // from heartbeats which nodes to use, not the readiness of the pod
                .withPublishNotReadyAddresses(true)
                .withSelector(Labels.selector(name, component))
                .addNewPort().withName("http").withPort(ConfigRenderer.WEB_PORT)
                .withTargetPort(new IntOrString("http")).withProtocol("TCP").endPort()
                .endSpec()
                .build();
    }

    static Service routerService(PharosCluster cluster) {
        String name = cluster.getMetadata().getName();
        return new ServiceBuilder()
                .withNewMetadata().withName(Names.router(name)).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.ROUTER)).endMetadata()
                .withNewSpec()
                .withType(Defaults.routerServiceType(cluster.getSpec()))
                .withSelector(Labels.selector(name, Labels.ROUTER))
                .addNewPort().withName("http").withPort(ConfigRenderer.ROUTER_PORT)
                .withTargetPort(new IntOrString("http")).withProtocol("TCP").endPort()
                .endSpec()
                .build();
    }

    /** {@code maxUnavailable: 1} for writers and readers alike: a node drain is never blocked. */
    static PodDisruptionBudget pdb(PharosCluster cluster, Role role) {
        String name = cluster.getMetadata().getName();
        String component = role == Role.WRITER ? Labels.WRITER : Labels.READER;
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata().withName(role == Role.WRITER ? Names.writers(name) : Names.readers(name))
                .withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, component)).endMetadata()
                .withNewSpec()
                .withMaxUnavailable(new IntOrString(1))
                .withNewSelector().withMatchLabels(Labels.selector(name, component)).endSelector()
                .endSpec()
                .build();
    }

    /**
     * The writers' and readers' port serves {@code /internal/*} and {@code /replication/*} next to the
     * API, so it only admits pods of the same {@code PharosCluster} (by {@link Labels#CLUSTER}; the router
     * among them) and the operator (which reads {@code /cluster/status}). The router's pods are not
     * selected, so its port stays open.
     */
    static NetworkPolicy networkPolicy(PharosCluster cluster) {
        String name = cluster.getMetadata().getName();
        return new NetworkPolicyBuilder()
                .withNewMetadata().withName(Names.networkPolicy(name)).withNamespace(cluster.getMetadata().getNamespace())
                .withLabels(Labels.of(name, Labels.NETWORK)).endMetadata()
                .withNewSpec()
                .withNewPodSelector()
                .addToMatchLabels(Labels.NAME, Labels.APP)
                .addToMatchLabels(Labels.INSTANCE, name)
                .addNewMatchExpression().withKey(Labels.COMPONENT).withOperator("In")
                .withValues(Labels.WRITER, Labels.READER).endMatchExpression()
                .endPodSelector()
                .withPolicyTypes("Ingress")
                .addNewIngress()
                .withFrom(new NetworkPolicyPeerBuilder().withNewPodSelector()
                                .addToMatchLabels(Labels.CLUSTER, name).endPodSelector().build(),
                        new NetworkPolicyPeerBuilder().withNewPodSelector()
                                .addToMatchLabels(Labels.NAME, Labels.OPERATOR_POD_NAME).endPodSelector().build())
                .addNewPort().withProtocol("TCP").withPort(new IntOrString(ConfigRenderer.WEB_PORT)).endPort()
                .endIngress()
                .endSpec()
                .build();
    }

    static PodSecurityContext podSecurity() {
        return new PodSecurityContextBuilder()
                .withRunAsUser(UID).withRunAsGroup(UID).withFsGroup(UID).withRunAsNonRoot(true)
                .withFsGroupChangePolicy("OnRootMismatch")
                .withNewSeccompProfile().withType("RuntimeDefault").endSeccompProfile()
                .build();
    }

    static SecurityContext containerSecurity() {
        return new SecurityContextBuilder()
                .withAllowPrivilegeEscalation(false)
                .withNewCapabilities().withDrop("ALL").endCapabilities()
                .build();
    }

    private static EnvVar plainEnv(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
