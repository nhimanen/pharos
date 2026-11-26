package com.pharos.operator.reconcile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.TestClusters;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.ProjectStatus;
import com.pharos.operator.render.ConfigRenderer;
import com.pharos.operator.render.DesiredState;
import com.pharos.operator.render.Labels;
import com.pharos.operator.render.ProjectJobRenderer;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Projects and their git sources: registration and removal Jobs, failures, and the writers' git
 * credentials. The Job lifecycle runs on a real JOSDK {@link Operator} against fabric8's mock API
 * server (s3 mode: the storage is ready at once, so Jobs are created); the planning edge cases are
 * unit tests of {@link ProjectReconciler#plan}.
 */
@EnableKubernetesMockClient(crud = true)
class ProjectReconcilerTest {

    private static final String NS = "pharos";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final OperatorSettings SETTINGS = new OperatorSettings("pharos", "minio:test", "mc:test");
    private static final Instant T0 = Instant.parse("2026-11-22T10:00:00Z");

    KubernetesClient client;

    private Operator operator;
    private PharosClusterReconciler reconciler;
    private final ReconcilerMockServerTest.MutableClock clock = new ReconcilerMockServerTest.MutableClock(T0);

    @BeforeEach
    void setUp() throws Exception {
        Path crd = Path.of(System.getProperty("pharos.crdCommitted", "../deploy/crd"), "pharosclusters.pharos.dev-v1.yml");
        try (var in = Files.newInputStream(crd)) {
            client.apiextensions().v1().customResourceDefinitions().load(in).create();
        }
    }

    @AfterEach
    void tearDown() {
        if (operator != null) operator.stop();
    }

    private void startOperator() {
        reconciler = new PharosClusterReconciler(clock, SETTINGS);
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

    private PharosCluster current() {
        return client.resources(PharosCluster.class).inNamespace(NS).withName("prod").get();
    }

    private Job job(String name) {
        return client.batch().v1().jobs().inNamespace(NS).withName(name).get();
    }

    private List<String> jobNames(String component) {
        return client.batch().v1().jobs().inNamespace(NS).withLabel(Labels.COMPONENT, component).list().getItems()
                .stream().map(j -> j.getMetadata().getName()).sorted().toList();
    }

    private Condition condition(String type) {
        PharosClusterStatus st = current().getStatus();
        return st == null || st.conditions() == null ? null
                : st.conditions().stream().filter(c -> c.getType().equals(type)).findFirst().orElse(null);
    }

    /** Marks a Job finished as the Job controller would. */
    private void finish(String name, String type, String reason) {
        Job j = job(name);
        j.setStatus(new JobStatusBuilder().withSucceeded("Complete".equals(type) ? 1 : 0)
                .withFailed("Failed".equals(type) ? 4 : 0)
                .addNewCondition().withType(type).withStatus("True").withReason(reason)
                .withLastTransitionTime(clock.instant().toString()).endCondition().build());
        client.batch().v1().jobs().resource(j).updateStatus();
    }

    /** An event on an owned object (the create-only token Secret): a reconcile now. */
    private void touch(String value) {
        var secret = client.secrets().inNamespace(NS).withName("prod-token").get();
        if (secret.getMetadata().getAnnotations() == null) secret.getMetadata().setAnnotations(new java.util.HashMap<>());
        secret.getMetadata().getAnnotations().put("test/touch", value);
        client.resource(secret).update();
    }

    private static PharosClusterSpec withProjects(PharosClusterSpec s, List<PharosClusterSpec.Project> projects) {
        return new PharosClusterSpec(s.version(), s.storage(), s.embeddings(), projects, s.writers(), s.readers(),
                s.router(), s.autoscaling(), s.pharosConfig());
    }

    private static PharosClusterSpec.Project lucene(String ref) {
        return new PharosClusterSpec.Project("lucene",
                new PharosClusterSpec.Git("https://github.com/apache/lucene.git", ref, null), false);
    }

    private static String registerJob(String ref) {
        return Names.registerJob("prod", "lucene", ProjectJobRenderer.specHash(lucene(ref)));
    }

    /** The s3 example (project lucene, embed false, ref main) created and its registration Job succeeded. */
    private void registeredLucene() throws InterruptedException {
        startOperator();
        PharosCluster c = TestClusters.s3();
        client.resource(c).create();
        await("the registration Job", () -> job(registerJob("main")) != null);
        finish(registerJob("main"), "Complete", null);
        await("lucene registered", () -> current().getStatus() != null
                && List.of(new ProjectStatus("lucene", true)).equals(current().getStatus().projects()));
    }

    @Test
    void newProjectRunsRegistrationJobOnce() throws Exception {
        startOperator();
        PharosCluster c = TestClusters.s3();
        client.resource(c).create();
        String name = registerJob("main");
        await("the registration Job", () -> job(name) != null);
        await("lucene listed, not yet registered", () -> current().getStatus() != null
                && List.of(new ProjectStatus("lucene", false)).equals(current().getStatus().projects()));

        Job j = job(name);
        assertThat(name).startsWith("prod-register-lucene-").hasSizeLessThanOrEqualTo(Names.MAX_NAME);
        assertThat(j.getMetadata().getAnnotations()).containsEntry(Labels.PROJECT_ANNOTATION, "lucene");
        assertThat(j.getMetadata().getOwnerReferences()).singleElement()
                .satisfies(o -> assertThat(o.getName()).isEqualTo("prod"));
        PodSpec pod = j.getSpec().getTemplate().getSpec();
        Container main = pod.getContainers().getFirst();
        assertThat(main.getImage()).isEqualTo("pharos:1.6.0");
        assertThat(main.getArgs()).containsExactly("index", "--project", "lucene",
                "--source-git", "https://github.com/apache/lucene.git", "--ref", "main", "--no-embed");
        // a writer's pod: same security, secrets only as env from Secrets, the writer config
        assertThat(pod.getSecurityContext().getRunAsUser()).isEqualTo(10001L);
        assertThat(pod.getSecurityContext().getRunAsNonRoot()).isTrue();
        assertThat(pod.getAutomountServiceAccountToken()).isFalse();
        assertThat(main.getSecurityContext().getAllowPrivilegeEscalation()).isFalse();
        assertThat(main.getEnv()).filteredOn(e -> e.getValueFrom() != null).extracting(EnvVar::getName)
                .containsExactlyInAnyOrder(ConfigRenderer.TOKEN_ENV, ConfigRenderer.ACCESS_KEY_ENV,
                        ConfigRenderer.SECRET_KEY_ENV);
        assertThat(main.getEnv()).filteredOn(e -> e.getValueFrom() == null).allSatisfy(e ->
                assertThat(e.getName()).isIn("PHAROS_ROLE", "PHAROS_HOME", "PHAROS_CONFIG", "PHAROS_ARITHMETIC"));
        assertThat(main.getEnv()).anySatisfy(e -> {
            assertThat(e.getName()).isEqualTo("PHAROS_ROLE");
            assertThat(e.getValue()).isEqualTo("writer");
        });
        assertThat(pod.getVolumes()).anySatisfy(v -> {
            assertThat(v.getName()).isEqualTo("config-template");
            assertThat(v.getConfigMap().getName()).isEqualTo("prod-config-writer");
        });
        assertThat(pod.getInitContainers()).singleElement().satisfies(i -> assertThat(i.getName()).isEqualTo("pod-config"));

        // further reconciles create nothing more and leave the Job alone
        String uid = j.getMetadata().getUid();
        long seen = reconciler.reconciles.get();
        touch("1");
        await("another reconcile", () -> reconciler.reconciles.get() > seen);
        Thread.sleep(500); // and any it triggers
        assertThat(jobNames(Labels.REGISTER)).containsExactly(name);
        assertThat(job(name).getMetadata().getUid()).isEqualTo(uid);

        finish(name, "Complete", null);
        await("lucene registered", () ->
                List.of(new ProjectStatus("lucene", true)).equals(current().getStatus().projects()));
        assertThat(jobNames(Labels.REGISTER)).containsExactly(name);
        assertThat(current().getStatus().phase()).isNotEqualTo("Degraded");
    }

    @Test
    void refChangeReRegisters() throws Exception {
        registeredLucene();
        PharosCluster c = current();
        c.setSpec(withProjects(c.getSpec(), List.of(lucene("branch_10x"))));
        client.resource(c).update();

        String next = registerJob("branch_10x");
        assertThat(next).isNotEqualTo(registerJob("main"));
        await("a new registration Job", () -> job(next) != null);
        assertThat(job(next).getSpec().getTemplate().getSpec().getContainers().getFirst().getArgs())
                .containsSequence("--ref", "branch_10x");
        // still registered (with the old source) while the new Job runs
        assertThat(current().getStatus().projects()).containsExactly(new ProjectStatus("lucene", true));

        finish(next, "Complete", null);
        await("the old source's Job is cleaned up", () -> jobNames(Labels.REGISTER).equals(List.of(next)));
        assertThat(current().getStatus().projects()).containsExactly(new ProjectStatus("lucene", true));
    }

    @Test
    void removedProjectRunsRemovalJob() throws Exception {
        registeredLucene();
        PharosCluster c = current();
        c.setSpec(withProjects(c.getSpec(), List.of()));
        client.resource(c).update();

        String removal = Names.removeJob("prod", "lucene");
        await("the removal Job", () -> job(removal) != null);
        Job j = job(removal);
        assertThat(j.getSpec().getTemplate().getSpec().getContainers().getFirst().getArgs())
                .containsExactly("remove-index", "--from-catalog", "lucene");
        assertThat(j.getMetadata().getAnnotations()).containsEntry(Labels.PROJECT_ANNOTATION, "lucene");
        // listed until the catalog no longer holds it
        assertThat(current().getStatus().projects()).containsExactly(new ProjectStatus("lucene", true));

        finish(removal, "Complete", null);
        await("every project Job gone", () -> jobNames(Labels.REGISTER).isEmpty() && jobNames(Labels.REMOVE).isEmpty());
        await("lucene no longer listed", () -> current().getStatus().projects() == null);

        // a quiet loop: the removal is not run again
        long seen = reconciler.reconciles.get();
        touch("2");
        await("another reconcile", () -> reconciler.reconciles.get() > seen);
        Thread.sleep(500); // and any it triggers
        assertThat(jobNames(Labels.REMOVE)).isEmpty();
        assertThat(jobNames(Labels.REGISTER)).isEmpty();
    }

    @Test
    void failedJobSetsDegradedWithReason() throws Exception {
        startOperator();
        PharosCluster c = TestClusters.s3();
        client.resource(c).create();
        String name = registerJob("main");
        await("the registration Job", () -> job(name) != null);
        String firstUid = job(name).getMetadata().getUid();
        finish(name, "Failed", "BackoffLimitExceeded");

        await("Degraded", () -> current().getStatus() != null && "Degraded".equals(current().getStatus().phase()));
        Condition degraded = condition("Degraded");
        assertThat(degraded.getStatus()).isEqualTo("True");
        assertThat(degraded.getReason()).isEqualTo("RegistrationJobFailed");
        assertThat(degraded.getMessage()).contains("project lucene").contains(name).contains("BackoffLimitExceeded")
                .contains("re-created after 60 s");
        assertThat(condition("Ready").getStatus()).isEqualTo("False");
        assertThat(job(name).getMetadata().getUid()).as("not retried within the backoff").isEqualTo(firstUid);

        clock.advance(Duration.ofSeconds(61));
        touch("1");
        await("a new registration Job", () -> {
            Job now = job(name);
            return now != null && !now.getMetadata().getUid().equals(firstUid);
        });
        await("retries counted", () -> current().getStatus().projects() != null
                && Integer.valueOf(1).equals(current().getStatus().projects().getFirst().retries()));
        await("no longer Degraded", () -> "False".equals(condition("Degraded").getStatus()));

        finish(name, "Complete", null);
        await("registered, retries cleared", () ->
                List.of(new ProjectStatus("lucene", true)).equals(current().getStatus().projects()));
    }

    /** Minio mode: the Jobs write to the bucket, so they wait for it; the bucket Job shares their informer. */
    @Test
    void minioModeRegistersOnceTheBucketExists() throws Exception {
        startOperator();
        client.resource(TestClusters.minio()).create();
        await("the bucket Job", () -> client.batch().v1().jobs().inNamespace(NS).withName("demo-minio-bucket").get() != null);
        Thread.sleep(500);
        assertThat(jobNames(Labels.REGISTER)).as("no bucket yet").isEmpty();

        StatefulSet minio = client.apps().statefulSets().inNamespace(NS).withName("demo-minio").get();
        minio.setStatus(new io.fabric8.kubernetes.api.model.apps.StatefulSetStatusBuilder().withReplicas(1)
                .withReadyReplicas(1).build());
        client.apps().statefulSets().resource(minio).updateStatus();
        Job bucket = client.batch().v1().jobs().inNamespace(NS).withName("demo-minio-bucket").get();
        String bucketUid = bucket.getMetadata().getUid();
        bucket.setStatus(new JobStatusBuilder().withSucceeded(1).addNewCondition().withType("Complete")
                .withStatus("True").endCondition().build());
        client.batch().v1().jobs().resource(bucket).updateStatus();

        await("both registration Jobs", () -> jobNames(Labels.REGISTER).size() == 2);
        PharosCluster demo = client.resources(PharosCluster.class).inNamespace(NS).withName("demo").get();
        assertThat(jobNames(Labels.REGISTER)).allSatisfy(n -> assertThat(n).startsWith("demo-register-"));
        for (PharosClusterSpec.Project p : demo.getSpec().projects()) {
            assertThat(jobNames(Labels.REGISTER)).contains(
                    Names.registerJob("demo", p.name(), ProjectJobRenderer.specHash(p)));
        }
        assertThat(client.batch().v1().jobs().inNamespace(NS).withName("demo-minio-bucket").get().getMetadata().getUid())
                .isEqualTo(bucketUid);
        await("both listed", () -> {
            PharosClusterStatus st = client.resources(PharosCluster.class).inNamespace(NS).withName("demo").get().getStatus();
            return st.projects() != null && st.projects().equals(List.of(new ProjectStatus("lucene", false),
                    new ProjectStatus("mypal", false)));
        });
        PharosClusterStatus st = client.resources(PharosCluster.class).inNamespace(NS).withName("demo").get().getStatus();
        assertThat(st.conditions()).anySatisfy(c -> {
            assertThat(c.getType()).isEqualTo("ApplyFailed");
            assertThat(c.getStatus()).isEqualTo("False");
        });
    }

    @Test
    void credentialsMountedOnEveryWriter() throws Exception {
        PharosCluster cluster = TestClusters.minio(); // mypal: credentialsSecret mypal-deploy-key; lucene: none
        ConfigRenderer.Result configs = ConfigRenderer.render(cluster);
        assertThat(configs.errors()).isEmpty();
        DesiredState state = DesiredState.render(cluster, configs, SETTINGS, 2, 2);
        String dir = "/etc/pharos/git/" + Names.k8sName("mypal");
        String volume = "git-" + Names.k8sName("mypal");

        StatefulSet writers = state.get(DesiredState.WRITERS, StatefulSet.class);
        PodSpec pod = writers.getSpec().getTemplate().getSpec();
        assertThat(pod.getVolumes()).filteredOn(v -> v.getSecret() != null).singleElement().satisfies(v -> {
            assertThat(v.getName()).isEqualTo(volume);
            assertThat(v.getSecret().getSecretName()).isEqualTo("mypal-deploy-key");
            assertThat(v.getSecret().getDefaultMode()).isEqualTo(0440);
            assertThat(v.getSecret().getOptional()).isTrue();
        });
        assertThat(pod.getContainers().getFirst().getVolumeMounts()).anySatisfy(m -> {
            assertThat(m.getName()).isEqualTo(volume);
            assertThat(m.getMountPath()).isEqualTo(dir);
            assertThat(m.getReadOnly()).isTrue();
        });
        // the mount point exists before the main container mounts it under its read-only config directory
        assertThat(String.join(" ", pod.getInitContainers().getFirst().getCommand())).endsWith("&& mkdir -p " + dir);

        // the writer config points mypal's git calls there: a path, never the Secret's content
        JsonNode writerConfig = new ObjectMapper().readTree(configs.get(ConfigRenderer.Role.WRITER).json());
        assertThat(writerConfig.at("/cluster/writer/gitCredentials")).hasSize(1);
        assertThat(writerConfig.at("/cluster/writer/gitCredentials/mypal").asText()).isEqualTo(dir);
        assertThat(new ObjectMapper().readTree(configs.get(ConfigRenderer.Role.READER).json())
                .at("/cluster/writer/gitCredentials").isMissingNode()).isTrue();

        // every writer: the StatefulSet's one template; readers, the router and project Jobs get none
        assertThat(writers.getSpec().getReplicas()).isEqualTo(2);
        List<PodSpec> others = new ArrayList<>(List.of(
                state.get(DesiredState.READERS, StatefulSet.class).getSpec().getTemplate().getSpec(),
                state.get(DesiredState.ROUTER, Deployment.class).getSpec().getTemplate().getSpec(),
                ProjectJobRenderer.registration(cluster, cluster.getSpec().projects().get(1), SETTINGS)
                        .getSpec().getTemplate().getSpec()));
        for (PodSpec other : others) {
            assertThat(other.getVolumes()).extracting(Volume::getName).noneMatch(n -> n.startsWith("git-"));
        }

        // a pharosConfig cannot point git calls elsewhere
        PharosCluster overridden = TestClusters.withSpec(cluster, TestClusters.withPharosConfig(cluster.getSpec(),
                java.util.Map.of("writer.gitCredentials", new ObjectMapper().readTree("{\"mypal\":\"/tmp\"}"))));
        assertThat(ConfigRenderer.render(overridden).errors()).singleElement().asString()
                .contains("keys owned by the operator: writer.gitCredentials");
    }

    // ---------------------------------------------------------------- plan

    private static PharosCluster planCluster(List<PharosClusterSpec.Project> projects, List<ProjectStatus> listed) {
        PharosCluster c = TestClusters.s3();
        c.setSpec(withProjects(c.getSpec(), projects));
        if (listed != null) {
            c.setStatus(new PharosClusterStatus("Ready", 1L, null, null, null, listed, null, null, null));
        }
        return c;
    }

    private static Job state(Job j, String condition, String deletionTimestamp) {
        JobBuilder b = new JobBuilder(j).editMetadata().withUid(j.getMetadata().getName())
                .withDeletionTimestamp(deletionTimestamp).endMetadata();
        if (condition != null) {
            b.withStatus(new JobStatusBuilder().addNewCondition().withType(condition).withStatus("True")
                    .withReason(condition.equals("Failed") ? "DeadlineExceeded" : null)
                    .withLastTransitionTime(T0.toString()).endCondition().build());
        }
        return b.build();
    }

    private static List<String> names(List<Job> jobs) {
        return jobs.stream().map(j -> j.getMetadata().getName()).toList();
    }

    @Test
    void anOlderRunningRegistrationIsDeletedBeforeTheNewOneStarts() {
        PharosCluster c = planCluster(List.of(lucene("v2")), List.of(new ProjectStatus("lucene", true)));
        Job old = state(ProjectJobRenderer.registration(c, lucene("main"), SETTINGS), null, null);

        var plan = ProjectReconciler.plan(c, SETTINGS, List.of(old), true, T0);
        assertThat(plan.create()).isEmpty();
        assertThat(plan.delete()).containsExactly(new ProjectReconciler.Deletion(registerJob("main"),
                DeletionPropagation.FOREGROUND));

        // while it is being deleted, still nothing new
        Job going = state(ProjectJobRenderer.registration(c, lucene("main"), SETTINGS), null, T0.toString());
        plan = ProjectReconciler.plan(c, SETTINGS, List.of(going), true, T0);
        assertThat(plan.create()).isEmpty();
        assertThat(plan.delete()).isEmpty();

        plan = ProjectReconciler.plan(c, SETTINGS, List.of(), true, T0);
        assertThat(names(plan.create())).containsExactly(registerJob("v2"));
        assertThat(plan.projects()).containsExactly(new ProjectStatus("lucene", true));
    }

    @Test
    void nothingIsCreatedBeforeTheStorageIsReady() {
        PharosCluster c = planCluster(List.of(lucene("main")), null);
        var plan = ProjectReconciler.plan(c, SETTINGS, List.of(), false, T0);
        assertThat(plan.create()).isEmpty();
        assertThat(plan.projects()).containsExactly(new ProjectStatus("lucene", false));
        assertThat(names(ProjectReconciler.plan(c, SETTINGS, List.of(), true, T0).create()))
                .containsExactly(registerJob("main"));
    }

    @Test
    void aProjectReAddedDuringItsRemovalWaitsThenRegistersAfresh() {
        PharosCluster c = planCluster(List.of(lucene("main")), List.of(new ProjectStatus("lucene", true)));
        Job registered = state(ProjectJobRenderer.registration(c, lucene("main"), SETTINGS), "Complete", null);
        Job removing = state(ProjectJobRenderer.removal(c, "lucene", SETTINGS), null, null);

        var plan = ProjectReconciler.plan(c, SETTINGS, List.of(registered, removing), true, T0);
        assertThat(plan.create()).as("no registration while the removal runs").isEmpty();
        assertThat(plan.delete()).isEmpty();

        Job removed = state(ProjectJobRenderer.removal(c, "lucene", SETTINGS), "Complete", null);
        plan = ProjectReconciler.plan(c, SETTINGS, List.of(registered, removed), true, T0);
        assertThat(plan.create()).isEmpty();
        assertThat(plan.delete()).extracting(ProjectReconciler.Deletion::job)
                .containsExactlyInAnyOrder(Names.removeJob("prod", "lucene"), registerJob("main"));
        assertThat(plan.projects()).as("the catalog no longer holds it").containsExactly(new ProjectStatus("lucene", false));

        // once both are gone, the same source registers again
        plan = ProjectReconciler.plan(c, SETTINGS, List.of(), true, T0);
        assertThat(names(plan.create())).containsExactly(registerJob("main"));
    }

    @Test
    void aRemovedProjectsRunningRegistrationIsStoppedFirstAndAFailedRemovalIsRetried() {
        PharosCluster c = planCluster(List.of(), null); // listed nowhere but in its Jobs
        Job running = state(ProjectJobRenderer.registration(c, lucene("main"), SETTINGS), null, null);
        var plan = ProjectReconciler.plan(c, SETTINGS, List.of(running), true, T0);
        assertThat(plan.create()).isEmpty();
        assertThat(plan.delete()).containsExactly(new ProjectReconciler.Deletion(registerJob("main"),
                DeletionPropagation.FOREGROUND));
        assertThat(plan.projects()).containsExactly(new ProjectStatus("lucene", false));

        Job failed = state(ProjectJobRenderer.removal(c, "lucene", SETTINGS), "Failed", null);
        plan = ProjectReconciler.plan(planCluster(List.of(), List.of(new ProjectStatus("lucene", true))), SETTINGS,
                List.of(failed), true, T0.plusSeconds(10));
        assertThat(plan.degradedReason()).isEqualTo("RemovalJobFailed");
        assertThat(plan.failures()).singleElement().asString().contains("removal Job " + Names.removeJob("prod", "lucene"))
                .contains("DeadlineExceeded");
        assertThat(plan.reschedule()).isEqualTo(Duration.ofSeconds(50));
        assertThat(plan.delete()).isEmpty();

        plan = ProjectReconciler.plan(planCluster(List.of(), List.of(new ProjectStatus("lucene", true))), SETTINGS,
                List.of(failed), true, T0.plusSeconds(60));
        assertThat(plan.delete()).containsExactly(new ProjectReconciler.Deletion(Names.removeJob("prod", "lucene"),
                DeletionPropagation.BACKGROUND));
        assertThat(plan.projects()).containsExactly(new ProjectStatus("lucene", true, null, null, null, 1));
    }

    private static Job created(Job j, Instant at) {
        return new JobBuilder(j).editMetadata().withCreationTimestamp(at.toString()).endMetadata().build();
    }

    /** main -> v2 -> main: main's old succeeded Job ran before v2's, so it must run again. */
    @Test
    void aSourceRevertedToAnEarlierOneRegistersItAgain() {
        PharosCluster c = planCluster(List.of(lucene("main")), List.of(new ProjectStatus("lucene", true)));
        Job mainJob = created(state(ProjectJobRenderer.registration(c, lucene("main"), SETTINGS), "Complete", null), T0);
        Job v2Job = created(state(ProjectJobRenderer.registration(c, lucene("v2"), SETTINGS), "Complete", null),
                T0.plusSeconds(600));

        var plan = ProjectReconciler.plan(c, SETTINGS, List.of(mainJob, v2Job), true, T0.plusSeconds(1200));
        assertThat(plan.create()).isEmpty();
        assertThat(plan.delete()).as("the catalog may hold v2: main's Job runs again")
                .containsExactly(new ProjectReconciler.Deletion(registerJob("main"), DeletionPropagation.BACKGROUND));

        // the next reconcile re-creates main's Job; once it succeeded (now the newest), v2's is cleaned up
        plan = ProjectReconciler.plan(c, SETTINGS, List.of(v2Job), true, T0.plusSeconds(1201));
        assertThat(names(plan.create())).containsExactly(registerJob("main"));
        Job again = created(state(ProjectJobRenderer.registration(c, lucene("main"), SETTINGS), "Complete", null),
                T0.plusSeconds(1201));
        plan = ProjectReconciler.plan(c, SETTINGS, List.of(again, v2Job), true, T0.plusSeconds(1300));
        assertThat(plan.create()).isEmpty();
        assertThat(plan.delete()).containsExactly(new ProjectReconciler.Deletion(registerJob("v2"),
                DeletionPropagation.BACKGROUND));
        assertThat(plan.projects()).containsExactly(new ProjectStatus("lucene", true));

        // a v2 Job still running when the spec goes back to main: both go, then main runs again
        Job v2Running = created(state(ProjectJobRenderer.registration(c, lucene("v2"), SETTINGS), null, null),
                T0.plusSeconds(600));
        plan = ProjectReconciler.plan(c, SETTINGS, List.of(mainJob, v2Running), true, T0.plusSeconds(700));
        assertThat(plan.create()).isEmpty();
        assertThat(plan.delete()).containsExactlyInAnyOrder(
                new ProjectReconciler.Deletion(registerJob("v2"), DeletionPropagation.FOREGROUND),
                new ProjectReconciler.Deletion(registerJob("main"), DeletionPropagation.BACKGROUND));
    }

    @Test
    void retryBackoffDoublesUpToFifteenMinutes() {
        assertThat(ProjectReconciler.backoffSeconds(null)).isEqualTo(60);
        assertThat(ProjectReconciler.backoffSeconds(1)).isEqualTo(120);
        assertThat(ProjectReconciler.backoffSeconds(3)).isEqualTo(480);
        assertThat(ProjectReconciler.backoffSeconds(4)).isEqualTo(900);
        assertThat(ProjectReconciler.backoffSeconds(40)).isEqualTo(900);
    }
}
