package com.pharos.operator.reconcile;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.TestClusters;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.render.Labels;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.javaoperatorsdk.operator.Operator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reconciler running in a real JOSDK {@link Operator} against fabric8's in-memory API server.
 *
 * <p>The mock server does not implement server-side apply, so this operator is configured to use
 * JOSDK's create/update path instead (production keeps SSA). What is under test is the same: every
 * desired object is created once, and a further reconcile of an unchanged cluster changes nothing.
 */
@EnableKubernetesMockClient(crud = true)
class ReconcilerMockServerTest {

    private static final String NS = "pharos";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final OperatorSettings SETTINGS = new OperatorSettings("pharos", "minio:test", "mc:test");
    private static final List<Class<? extends HasMetadata>> KINDS = List.of(Secret.class, ConfigMap.class,
            Service.class, StatefulSet.class, Deployment.class, PersistentVolumeClaim.class, Job.class,
            PodDisruptionBudget.class, NetworkPolicy.class);

    KubernetesClient client;

    private Operator operator;
    private PharosClusterReconciler reconciler;
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final Map<String, Level> savedLevels = new TreeMap<>();
    /** The console appender, detached while DEBUG is on so the build output stays readable. */
    private ch.qos.logback.core.Appender<ILoggingEvent> console;

    @BeforeEach
    void setUp() throws Exception {
        Path crd = Path.of(System.getProperty("pharos.crdCommitted", "../deploy/crd"), "pharosclusters.pharos.dev-v1.yml");
        try (var in = Files.newInputStream(crd)) {
            client.apiextensions().v1().customResourceDefinitions().load(in).create();
        }
        // Capture everything the operator logs, at DEBUG for the operator and JOSDK.
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logs.start();
        root.addAppender(logs);
        console = root.getAppender("STDOUT");
        if (console != null) root.detachAppender(console);
        for (String name : List.of("com.pharos", "io.javaoperatorsdk")) {
            Logger l = (Logger) LoggerFactory.getLogger(name);
            savedLevels.put(name, l.getLevel());
            l.setLevel(Level.DEBUG);
        }
    }

    @AfterEach
    void tearDown() {
        if (operator != null) operator.stop();
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.detachAppender(logs);
        if (console != null) root.addAppender(console);
        savedLevels.forEach((name, level) -> ((Logger) LoggerFactory.getLogger(name)).setLevel(level));
    }

    private void startOperator() {
        reconciler = new PharosClusterReconciler(SETTINGS);
        operator = new Operator(o -> o.withKubernetesClient(client)
                .withCloseClientOnStop(false)
                .withSSABasedCreateUpdateMatchForDependentResources(false)
                .withUseSSAToPatchPrimaryResource(false));
        operator.register(reconciler, c -> c.settingNamespace(NS));
        operator.start();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(50);
        }
    }

    private PharosCluster current(String name) {
        return client.resources(PharosCluster.class).inNamespace(NS).withName(name).get();
    }

    /** kind/name → resourceVersion of everything the operator manages. */
    private Map<String, String> managedVersions() {
        Map<String, String> m = new TreeMap<>();
        for (Class<? extends HasMetadata> kind : KINDS) {
            for (HasMetadata o : client.resources(kind).inNamespace(NS).withLabel(Labels.MANAGED_BY, Labels.OPERATOR)
                    .list().getItems()) {
                m.put(kind.getSimpleName() + "/" + o.getMetadata().getName(), o.getMetadata().getResourceVersion());
            }
        }
        return m;
    }

    private List<HasMetadata> allManaged() {
        List<HasMetadata> all = new ArrayList<>();
        for (Class<? extends HasMetadata> kind : KINDS) {
            all.addAll(client.resources(kind).inNamespace(NS).list().getItems());
        }
        return all;
    }

    private String secretValue(String secret, String key) {
        String b64 = client.secrets().inNamespace(NS).withName(secret).get().getData().get(key);
        return new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
    }

    @Test
    void reconcileCreatesEverythingIdempotently() throws Exception {
        startOperator();
        client.resource(TestClusters.minio()).create();

        List<String> expected = List.of("Secret/demo-token", "Secret/demo-minio",
                "ConfigMap/demo-config-writer", "ConfigMap/demo-config-reader", "ConfigMap/demo-config-router",
                "Service/demo-writers-hl", "Service/demo-readers-hl", "Service/demo-router", "Service/demo-minio",
                "StatefulSet/demo-writers", "StatefulSet/demo-readers", "StatefulSet/demo-minio",
                "Deployment/demo-router", "PersistentVolumeClaim/demo-minio", "Job/demo-minio-bucket",
                "PodDisruptionBudget/demo-writers", "PodDisruptionBudget/demo-readers", "NetworkPolicy/demo-internal");
        await("every object", () -> managedVersions().keySet().containsAll(expected));
        await("status Provisioning", () -> {
            PharosCluster c = current("demo");
            return c.getStatus() != null && "Provisioning".equals(c.getStatus().phase());
        });
        assertThat(managedVersions()).containsOnlyKeys(expected);
        PharosClusterStatus status = current("demo").getStatus();
        assertThat(status.writers()).isEqualTo(new PharosClusterStatus.Replicas(1, 0));
        assertThat(status.readers()).isEqualTo(new PharosClusterStatus.Replicas(2, 0));
        assertThat(status.storage()).isEqualTo(new PharosClusterStatus.StorageStatus("minio", false));
        assertThat(client.apps().statefulSets().inNamespace(NS).withName("demo-writers").get()
                .getMetadata().getOwnerReferences()).singleElement()
                .satisfies(o -> assertThat(o.getName()).isEqualTo("demo"));

        // Let any reconcile triggered by our own writes finish, then: nothing changes any more.
        long seen = reconciler.reconciles.get();
        await("a quiet reconcile loop", () -> {
            long now = reconciler.reconciles.get();
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return reconciler.reconciles.get() == now;
        });
        assertThat(reconciler.reconciles.get()).isGreaterThanOrEqualTo(seen);
        Map<String, String> before = managedVersions();
        String token = secretValue("demo-token", "token");
        String minioSecret = secretValue("demo-minio", "secretKey");

        // A spec change that renders nothing differently reconciles again (new generation): no object changes.
        PharosCluster c = current("demo");
        PharosClusterSpec s = c.getSpec();
        long reconciles = reconciler.reconciles.get();
        c.setSpec(new PharosClusterSpec(s.version(), s.storage(), s.embeddings(), s.projects(), s.writers(), s.readers(),
                s.router(), new PharosClusterSpec.Autoscaling(true, 600, null, null, s.autoscaling().triggers(),
                s.autoscaling().schedule()), s.pharosConfig()));
        client.resource(c).update();
        await("the reconcile of the new generation", () -> {
            PharosCluster now = current("demo");
            return reconciler.reconciles.get() > reconciles && now.getStatus().observedGeneration()
                    .equals(now.getMetadata().getGeneration());
        });
        assertThat(managedVersions()).isEqualTo(before);

        // An operator restart reconciles everything again: still no change, and no Secret is regenerated.
        operator.stop();
        startOperator();
        await("the first reconcile after the restart", () -> reconciler.reconciles.get() >= 1);
        assertThat(managedVersions()).isEqualTo(before);
        assertThat(secretValue("demo-token", "token")).isEqualTo(token);
        assertThat(secretValue("demo-minio", "secretKey")).isEqualTo(minioSecret);

        assertNoSecretAnywhere("demo", List.of(token, secretValue("demo-minio", "accessKey"), minioSecret));
    }

    /**
     * Review Focus 4, on a live reconcile: the generated secrets appear only in their own Secrets, never in
     * another object, the resource's status, an Event or the operator's logs.
     */
    private void assertNoSecretAnywhere(String cluster, List<String> secrets) {
        List<String> forms = new ArrayList<>();
        for (String s : secrets) {
            assertThat(s).hasSizeGreaterThanOrEqualTo(20);
            forms.add(s);
            forms.add(Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8)));
        }
        for (HasMetadata o : allManaged()) {
            if (o instanceof Secret && (o.getMetadata().getName().equals(cluster + "-token")
                    || o.getMetadata().getName().equals(cluster + "-minio"))) {
                continue;
            }
            String yaml = Serialization.asYaml(o);
            forms.forEach(f -> assertThat(yaml).as("%s/%s", o.getKind(), o.getMetadata().getName()).doesNotContain(f));
        }
        String resource = Serialization.asYaml(current(cluster));
        forms.forEach(f -> assertThat(resource).as("the PharosCluster and its status").doesNotContain(f));
        Stream.concat(client.v1().events().inNamespace(NS).list().getItems().stream().map(Serialization::asYaml),
                        client.events().v1().events().inNamespace(NS).list().getItems().stream().map(Serialization::asYaml))
                .forEach(e -> forms.forEach(f -> assertThat(e).as("an Event").doesNotContain(f)));
        List<ILoggingEvent> operatorLogs = logs.list.stream()
                .filter(e -> !e.getLoggerName().startsWith("io.fabric8.mockwebserver")
                        && !e.getLoggerName().startsWith("io.fabric8.kubernetes.client.server.mock"))
                .toList();
        assertThat(operatorLogs).isNotEmpty();
        for (ILoggingEvent e : operatorLogs) {
            String line = e.getFormattedMessage() + (e.getThrowableProxy() == null ? "" : e.getThrowableProxy().getMessage());
            forms.forEach(f -> assertThat(line).as("log line of %s", e.getLoggerName()).doesNotContain(f));
        }
    }

    @Test
    void invalidSpecCreatesNothingAndListsOwnedKeys() throws Exception {
        startOperator();
        PharosCluster c = TestClusters.minio();
        c.setSpec(TestClusters.withPharosConfig(c.getSpec(), Map.of(
                "tokenEnv", JsonNodeFactory.instance.textNode("X"),
                "writer.workDir", JsonNodeFactory.instance.textNode("/tmp"))));
        client.resource(c).create();
        await("status SpecInvalid", () -> {
            PharosCluster now = current("demo");
            return now.getStatus() != null && now.getStatus().conditions() != null;
        });
        PharosClusterStatus status = current("demo").getStatus();
        assertThat(status.phase()).isEqualTo("Pending");
        assertThat(status.conditions()).anySatisfy(cond -> {
            assertThat(cond.getType()).isEqualTo("SpecInvalid");
            assertThat(cond.getStatus()).isEqualTo("True");
            assertThat(cond.getMessage())
                    .isEqualTo("spec.pharosConfig: keys owned by the operator: tokenEnv, writer.workDir");
        });
        assertThat(managedVersions()).isEmpty();
    }

    @Test
    void s3ModeCreatesNoStorageObjects() throws Exception {
        startOperator();
        client.resource(TestClusters.s3()).create();
        await("status Provisioning", () -> {
            PharosCluster c = current("prod");
            return c.getStatus() != null && "Provisioning".equals(c.getStatus().phase());
        });
        Map<String, String> objects = managedVersions();
        assertThat(objects).containsKeys("Secret/prod-token", "StatefulSet/prod-writers", "Deployment/prod-router");
        assertThat(objects.keySet()).noneMatch(k -> k.contains("minio"));
        assertThat(objects.keySet()).filteredOn(k -> k.startsWith("Secret/")).containsExactly("Secret/prod-token");
        assertThat(current("prod").getStatus().storage()).isEqualTo(new PharosClusterStatus.StorageStatus("s3", true));
    }
}
