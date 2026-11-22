package com.pharos.operator.reconcile;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.pharos.cluster.status.ClusterStatus;
import com.pharos.cluster.status.StatusState;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.TestClusters;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.render.Labels;
import com.pharos.operator.status.ClusterStatusEventSource;
import com.pharos.operator.status.StatusReading;
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
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
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
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
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
    KubernetesMockServer server;

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

    private final MutableClock clock = new MutableClock(java.time.Instant.parse("2026-11-22T10:00:00Z"));

    /** A clock the test moves by hand (the bucket Job's backoff). */
    static final class MutableClock extends java.time.Clock {
        private volatile java.time.Instant now;

        MutableClock(java.time.Instant now) { this.now = now; }

        void advance(Duration d) { now = now.plus(d); }

        @Override public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }

        @Override public java.time.Clock withZone(java.time.ZoneId zone) { return this; }

        @Override public java.time.Instant instant() { return now; }
    }

    private void startOperator() {
        startOperator(SETTINGS);
    }

    private void startOperator(OperatorSettings settings) {
        startOperator(new PharosClusterReconciler(clock, settings));
    }

    private void startOperator(PharosClusterReconciler r) {
        reconciler = r;
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
        assertThat(status.storage()).isEqualTo(new PharosClusterStatus.StorageStatus("minio", false, null));
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
        assertThat(current("prod").getStatus().storage()).isEqualTo(new PharosClusterStatus.StorageStatus("s3", true, null));
    }

    private PharosCluster provisionedMinio() throws InterruptedException {
        startOperator();
        client.resource(TestClusters.minio()).create();
        await("status Provisioning", () -> {
            PharosCluster c = current("demo");
            return c.getStatus() != null && "Provisioning".equals(c.getStatus().phase());
        });
        await("every minio object", () -> managedVersions().keySet().containsAll(List.of("PersistentVolumeClaim/demo-minio",
                "Secret/demo-minio", "Secret/demo-token", "Job/demo-minio-bucket", "StatefulSet/demo-minio")));
        return current("demo");
    }

    private io.fabric8.kubernetes.api.model.Condition condition(String cluster, String type) {
        PharosClusterStatus st = current(cluster).getStatus();
        return st == null || st.conditions() == null ? null
                : st.conditions().stream().filter(c -> c.getType().equals(type)).findFirst().orElse(null);
    }

    private boolean conditionTrue(String cluster, String type) {
        var c = condition(cluster, type);
        return c != null && "True".equals(c.getStatus());
    }

    private static PharosClusterSpec spec(PharosClusterSpec s, PharosClusterSpec.Storage storage, String version,
                                          PharosClusterSpec.RoleSpec writers) {
        return new PharosClusterSpec(version, storage, s.embeddings(), s.projects(), writers, s.readers(), s.router(),
                s.autoscaling(), s.pharosConfig());
    }

    @Test
    void storageKindIsImmutableAndNothingIsDeleted() throws Exception {
        PharosCluster c = provisionedMinio();
        Map<String, String> uids = new TreeMap<>();
        for (HasMetadata o : allManaged()) uids.put(o.getKind() + "/" + o.getMetadata().getName(), o.getMetadata().getUid());

        PharosClusterSpec s = c.getSpec();
        c.setSpec(spec(s, TestClusters.s3().getSpec().storage(), s.version(), s.writers()));
        client.resource(c).update();
        await("SpecInvalid", () -> conditionTrue("demo", "SpecInvalid"));
        assertThat(condition("demo", "SpecInvalid").getMessage())
                .contains("spec.storage: the storage kind is immutable (it is minio; the spec asks for s3)");
        assertThat(current("demo").getStatus().storage().kind()).isEqualTo("minio");

        // The data-bearing objects (and everything else) are still there, the very same objects.
        Map<String, String> after = new TreeMap<>();
        for (HasMetadata o : allManaged()) after.put(o.getKind() + "/" + o.getMetadata().getName(), o.getMetadata().getUid());
        assertThat(after).isEqualTo(uids);
        assertThat(after).containsKeys("PersistentVolumeClaim/demo-minio", "Secret/demo-minio", "Secret/demo-token");
    }

    @Test
    void volumeChangeIsNotAppliedButReplicasAndVersionAre() throws Exception {
        PharosCluster c = provisionedMinio();
        PharosClusterSpec s = c.getSpec();
        c.setSpec(spec(s, new PharosClusterSpec.Storage(new PharosClusterSpec.Minio("10Gi"), null), "1.6.1",
                new PharosClusterSpec.RoleSpec(2, 4, s.writers().resources(), "80Gi", null, null)));
        client.resource(c).update();
        await("VolumeChangeNotApplied", () -> conditionTrue("demo", "VolumeChangeNotApplied"));

        StatefulSet writers = client.apps().statefulSets().inNamespace(NS).withName("demo-writers").get();
        assertThat(writers.getSpec().getReplicas()).isEqualTo(2);
        assertThat(writers.getSpec().getTemplate().getSpec().getContainers().getFirst().getImage()).isEqualTo("pharos:1.6.1");
        assertThat(writers.getSpec().getVolumeClaimTemplates().getFirst().getSpec().getResources().getRequests()
                .get("storage").toString()).isEqualTo("50Gi");
        assertThat(client.persistentVolumeClaims().inNamespace(NS).withName("demo-minio").get()
                .getSpec().getResources().getRequests().get("storage").toString()).isEqualTo("20Gi");
        assertThat(condition("demo", "VolumeChangeNotApplied").getMessage())
                .contains("spec.writers.workVolume: 50Gi -> 80Gi is not applied")
                .contains("spec.storage.minio.storage: 20Gi -> 10Gi is not applied");
        assertThat(conditionTrue("demo", "ApplyFailed")).isFalse();

        // Growing the MinIO volume is applied.
        PharosCluster again = current("demo");
        again.setSpec(spec(again.getSpec(), new PharosClusterSpec.Storage(new PharosClusterSpec.Minio("30Gi"), null),
                "1.6.1", again.getSpec().writers()));
        client.resource(again).update();
        await("the MinIO PVC grows", () -> "30Gi".equals(client.persistentVolumeClaims().inNamespace(NS)
                .withName("demo-minio").get().getSpec().getResources().getRequests().get("storage").toString()));
    }

    @Test
    void bumpedMcImageChangesNoJobAndFailsNothing() throws Exception {
        provisionedMinio();
        Map<String, String> before = managedVersions();
        operator.stop();
        startOperator(new OperatorSettings("pharos", "minio:test", "mc:newer"));
        await("the first reconcile after the restart", () -> reconciler.reconciles.get() >= 1);
        assertThat(managedVersions()).isEqualTo(before);
        assertThat(client.batch().v1().jobs().inNamespace(NS).withName("demo-minio-bucket").get()
                .getSpec().getTemplate().getSpec().getContainers().getFirst().getImage()).isEqualTo("mc:test");
        assertThat(conditionTrue("demo", "ApplyFailed")).isFalse();
    }

    @Test
    void applyFailureIsReportedAsACondition() throws Exception {
        server.expect().post().withPath("/apis/networking.k8s.io/v1/namespaces/pharos/networkpolicies")
                .andReturn(500, "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"status\":\"Failure\","
                        + "\"message\":\"injected failure\",\"code\":500}")
                .always();
        startOperator();
        client.resource(TestClusters.minio()).create();
        await("ApplyFailed", () -> conditionTrue("demo", "ApplyFailed"));
        assertThat(condition("demo", "ApplyFailed").getReason()).isEqualTo("ApplyFailed");
        assertThat(condition("demo", "ApplyFailed").getMessage()).contains("network-policy");
        // everything else was still applied
        assertThat(managedVersions()).containsKeys("StatefulSet/demo-writers", "Secret/demo-token");
    }

    @Test
    void failedBucketJobIsReportedThenRetried() throws Exception {
        provisionedMinio();
        Job job = client.batch().v1().jobs().inNamespace(NS).withName("demo-minio-bucket").get();
        String firstUid = job.getMetadata().getUid();
        job.setStatus(new io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder().withFailed(7)
                .addNewCondition().withType("Failed").withStatus("True").withReason("BackoffLimitExceeded")
                .withLastTransitionTime(clock.instant().toString()).endCondition().build());
        client.batch().v1().jobs().resource(job).updateStatus();

        await("BucketJobFailed", () -> {
            var c = condition("demo", "StorageReady");
            return c != null && "BucketJobFailed".equals(c.getReason());
        });
        assertThat(condition("demo", "StorageReady").getMessage()).contains("BackoffLimitExceeded")
                .contains("re-created after 60 s");
        assertThat(client.batch().v1().jobs().inNamespace(NS).withName("demo-minio-bucket").get().getMetadata().getUid())
                .isEqualTo(firstUid); // within the backoff: not retried yet

        clock.advance(Duration.ofSeconds(61));
        Job touch = client.batch().v1().jobs().inNamespace(NS).withName("demo-minio-bucket").get();
        touch.getMetadata().getAnnotations().put("test/touch", "1"); // any event: reconcile now
        client.batch().v1().jobs().resource(touch).update();
        await("a new bucket Job", () -> {
            Job now = client.batch().v1().jobs().inNamespace(NS).withName("demo-minio-bucket").get();
            return now != null && !now.getMetadata().getUid().equals(firstUid);
        });
        await("retries counted", () -> Integer.valueOf(1).equals(current("demo").getStatus().storage().bucketRetries()));
    }

    @Test
    void proposalsScaleTheStatefulSetsAndAreRecorded() throws Exception {
        AtomicReference<StatusReading> reading = new AtomicReference<>();
        AtomicReference<String> token = new AtomicReference<>();
        AtomicReference<Runnable> update = new AtomicReference<>();
        ClusterStatusEventSource source = new ClusterStatusEventSource((name, namespace, onUpdate) -> {
            update.set(onUpdate);
            return new ClusterStatusEventSource.Watch() {
                @Override public void token(String t) { token.set(t); }

                @Override public Optional<StatusReading> latest() { return Optional.ofNullable(reading.get()); }

                @Override public String problem() { return reading.get() == null ? "nobody home" : null; }

                @Override public void close() { }
            };
        });
        startOperator(new PharosClusterReconciler(clock, SETTINGS, source));
        client.resource(TestClusters.minio()).create();

        // no node answers: the floors (readers min 2) apply and the status says why nothing else does
        await("readers at the floor", () -> replicas("demo-readers") == 2);
        await("ClusterStatusAvailable=False", () -> {
            var c = current("demo") == null ? null : condition("demo", "ClusterStatusAvailable");
            return c != null && "False".equals(c.getStatus()) && c.getMessage().startsWith("nobody home");
        });
        await("the token handed to the watch", () -> token.get() != null);
        assertThat(token.get()).isEqualTo(secretValue("demo-token", "token"));

        var add = new ClusterStatus.Proposal("add-r", "add-readers", "readers", "p95 above target", Map.of(),
                "start 3 readers", 3, "2026-11-22T09:50:00Z");
        var reshard = new ClusterStatus.Proposal("rs", "reshard", "lucene", "big", Map.of("sizeBytes", 5),
                "split lucene", null, "2026-11-22T09:40:00Z");
        reading.set(new StatusReading(new ClusterStatus("cluster", "2026-11-22T10:00:00Z",
                new ClusterStatus.Layout(new StatusState.Summary(1, 1, 2), List.of(), List.of(), List.of()),
                new ClusterStatus.BacklogSummary(0, null), List.of(), List.of(add, reshard)), clock.instant(), "test"));
        update.get().run();

        // maxStepUp 2 (the example): 2 -> 4, once
        await("readers scaled", () -> replicas("demo-readers") == 4);
        await("the application recorded", () -> {
            PharosClusterStatus st = current("demo").getStatus();
            return st.proposals() != null && st.reshardProposals() != null;
        });
        PharosClusterStatus st = current("demo").getStatus();
        assertThat(st.proposals()).containsExactly(new PharosClusterStatus.ProposalStatus("add-r", "add-readers",
                "readers", "2026-11-22T09:50:00Z", true, "2026-11-22T10:00:00Z", 2, 4));
        assertThat(st.reshardProposals()).singleElement().satisfies(r -> assertThat(r.target()).isEqualTo("lucene"));
        assertThat(st.readers().desired()).isEqualTo(4);
        assertThat(condition("demo", "ClusterStatusAvailable").getStatus()).isEqualTo("True");
        assertThat(replicas("demo-writers")).isEqualTo(1);
        await("the Events", () -> client.v1().events().inNamespace(NS).list().getItems().stream()
                .map(e -> e.getReason()).toList().containsAll(List.of("Scaled", "ReshardProposed")));

        // still in force, inside the cooldown: further reads change nothing
        long reconciles = reconciler.reconciles.get();
        update.get().run();
        await("another reconcile", () -> reconciler.reconciles.get() > reconciles);
        assertThat(replicas("demo-readers")).isEqualTo(4);
        // after the cooldown (300 s), the next step
        clock.advance(Duration.ofSeconds(300));
        StatusReading old = reading.get();
        reading.set(new StatusReading(old.status(), clock.instant(), old.from())); // a fresh read
        update.get().run();
        await("readers ramped", () -> replicas("demo-readers") == 6);
    }

    private int replicas(String sts) {
        StatefulSet s = client.apps().statefulSets().inNamespace(NS).withName(sts).get();
        return s == null ? -1 : s.getSpec().getReplicas();
    }
}
