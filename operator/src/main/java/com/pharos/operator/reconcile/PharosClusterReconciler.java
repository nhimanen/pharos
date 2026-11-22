package com.pharos.operator.reconcile;

import com.pharos.operator.Defaults;
import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.Phase;
import com.pharos.operator.render.ConfigRenderer;
import com.pharos.operator.render.DesiredState;
import com.pharos.operator.render.StorageRenderer;
import com.pharos.operator.validate.SpecValidator;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reconciles a {@link PharosCluster}: validates the spec (and the rendered pharos config), applies the
 * desired objects through the JOSDK dependent resources below, and reports the result in
 * {@code .status}. An invalid spec gives {@code phase: Pending} with a {@code SpecInvalid} condition
 * listing every error, and nothing is created or changed.
 *
 * <p>The workflow is invoked explicitly, only for a valid spec. Each dependent applies its object
 * from the {@link DesiredState} rendered here; a second reconcile of an unchanged cluster changes
 * nothing. This reconciler is the only writer of {@code .status} (ruling OP-R3).
 */
@Workflow(explicitInvocation = true, dependents = {
        @Dependent(name = DesiredState.TOKEN, type = Dependents.Secrets.class),
        @Dependent(name = DesiredState.MINIO_CREDENTIALS, type = Dependents.Secrets.class,
                useEventSourceWithName = DesiredState.TOKEN, reconcilePrecondition = Dependents.MinioMode.class),
        @Dependent(name = DesiredState.CONFIG_WRITER, type = Dependents.ConfigMaps.class),
        @Dependent(name = DesiredState.CONFIG_READER, type = Dependents.ConfigMaps.class,
                useEventSourceWithName = DesiredState.CONFIG_WRITER),
        @Dependent(name = DesiredState.CONFIG_ROUTER, type = Dependents.ConfigMaps.class,
                useEventSourceWithName = DesiredState.CONFIG_WRITER),
        @Dependent(name = DesiredState.WRITERS_HEADLESS, type = Dependents.Services.class),
        @Dependent(name = DesiredState.READERS_HEADLESS, type = Dependents.Services.class,
                useEventSourceWithName = DesiredState.WRITERS_HEADLESS),
        @Dependent(name = DesiredState.ROUTER_SERVICE, type = Dependents.Services.class,
                useEventSourceWithName = DesiredState.WRITERS_HEADLESS),
        @Dependent(name = DesiredState.MINIO_SERVICE, type = Dependents.Services.class,
                useEventSourceWithName = DesiredState.WRITERS_HEADLESS, reconcilePrecondition = Dependents.MinioMode.class),
        @Dependent(name = DesiredState.WRITERS, type = Dependents.StatefulSets.class),
        @Dependent(name = DesiredState.READERS, type = Dependents.StatefulSets.class,
                useEventSourceWithName = DesiredState.WRITERS),
        @Dependent(name = DesiredState.MINIO, type = Dependents.StatefulSets.class,
                useEventSourceWithName = DesiredState.WRITERS, reconcilePrecondition = Dependents.MinioMode.class),
        @Dependent(name = DesiredState.ROUTER, type = Dependents.Deployments.class),
        @Dependent(name = DesiredState.MINIO_PVC, type = Dependents.Pvcs.class,
                reconcilePrecondition = Dependents.MinioMode.class),
        @Dependent(name = DesiredState.MINIO_BUCKET, type = Dependents.Jobs.class,
                reconcilePrecondition = Dependents.MinioMode.class),
        @Dependent(name = DesiredState.PDB_WRITERS, type = Dependents.Pdbs.class),
        @Dependent(name = DesiredState.PDB_READERS, type = Dependents.Pdbs.class,
                useEventSourceWithName = DesiredState.PDB_WRITERS),
        @Dependent(name = DesiredState.NETWORK_POLICY, type = Dependents.NetworkPolicies.class),
})
@ControllerConfiguration(name = PharosClusterReconciler.NAME)
public class PharosClusterReconciler implements Reconciler<PharosCluster> {

    public static final String NAME = "pharoscluster";

    /** Reasons. */
    static final String REASON_SPEC_INVALID = "SpecInvalid";
    static final String REASON_SPEC_VALID = "SpecValid";
    static final String REASON_PROVISIONED = "Provisioned";
    static final String REASON_WAITING = "Waiting";
    static final String REASON_READY = "Ready";

    private static final Logger LOG = LoggerFactory.getLogger(PharosClusterReconciler.class);

    private final Clock clock;
    private final OperatorSettings settings;
    /** Completed reconciles; lets tests wait for one. */
    final AtomicLong reconciles = new AtomicLong();

    public PharosClusterReconciler(OperatorSettings settings) {
        this(Clock.systemUTC(), settings);
    }

    PharosClusterReconciler(Clock clock) {
        this(clock, OperatorSettings.defaults());
    }

    PharosClusterReconciler(Clock clock, OperatorSettings settings) {
        this.clock = clock;
        this.settings = settings;
    }

    @Override
    public UpdateControl<PharosCluster> reconcile(PharosCluster resource, Context<PharosCluster> context) {
        try {
            List<String> errors = new ArrayList<>(SpecValidator.validate(resource));
            ConfigRenderer.Result configs = null;
            if (errors.isEmpty()) {
                configs = ConfigRenderer.render(resource);
                errors.addAll(configs.errors());
            }
            PharosClusterStatus desired;
            if (!errors.isEmpty()) {
                desired = invalidStatus(resource, errors);
            } else {
                Observed before = Observed.of(resource, context);
                int writers = replicas(resource.getSpec().writers(), before.writersReplicas());
                int readers = replicas(resource.getSpec().readers(), before.readersReplicas());
                DesiredState state = DesiredState.render(resource, configs, settings, writers, readers);
                var workflow = context.managedWorkflowAndDependentResourceContext();
                workflow.put(DesiredState.KEY, state);
                workflow.reconcileManagedWorkflow();
                desired = provisionedStatus(resource, writers, readers, Observed.of(resource, context));
            }
            if (Objects.equals(desired, resource.getStatus())) {
                return UpdateControl.noUpdate();
            }
            return UpdateControl.patchStatus(statusPatch(resource, desired));
        } finally {
            reconciles.incrementAndGet();
        }
    }

    /**
     * A role's replica count: the current one kept within {@code [min, max]} (the autoscaler moves it),
     * or {@code min} when the StatefulSet does not exist yet.
     */
    static int replicas(PharosClusterSpec.RoleSpec role, Integer current) {
        int min = Defaults.min(role);
        int max = Defaults.max(role);
        int n = current == null ? min : current;
        return Math.max(min, Math.min(max, n));
    }

    /** The status of an invalid spec: {@code Pending} with every error in {@code SpecInvalid}. */
    PharosClusterStatus invalidStatus(PharosCluster resource, List<String> errors) {
        PharosClusterStatus previous = resource.getStatus();
        LOG.info("PharosCluster {}/{}: spec is invalid ({} errors): {}", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName(), errors.size(), errors);
        var conditions = conditions(resource)
                .set(Phase.Pending.name(), true, REASON_SPEC_INVALID,
                        "The spec is invalid; nothing is created or changed until it is fixed.")
                .set(PharosClusterStatus.CONDITION_SPEC_INVALID, true, REASON_SPEC_INVALID, String.join("; ", errors));
        return new PharosClusterStatus(Phase.Pending.name(), generation(resource),
                previous == null ? null : previous.writers(), previous == null ? null : previous.readers(),
                previous == null ? null : previous.storage(), previous == null ? null : previous.projects(),
                previous == null ? null : previous.proposals(), previous == null ? null : previous.reshardProposals(),
                conditions.build());
    }

    /**
     * The status once the objects are applied: {@code Ready} when the storage is ready and at least
     * {@code min} writers and readers and one router are ready, else {@code Provisioning}.
     */
    PharosClusterStatus provisionedStatus(PharosCluster resource, int writers, int readers, Observed observed) {
        PharosClusterSpec spec = resource.getSpec();
        PharosClusterStatus previous = resource.getStatus();
        boolean minio = StorageRenderer.minioMode(spec);
        // s3: an existing bucket the operator does not probe; reachability is reported by the nodes
        boolean storageReady = !minio || (observed.minioReady() >= 1 && observed.bucketCreated());
        List<String> waiting = new ArrayList<>();
        if (!storageReady) waiting.add(observed.minioReady() < 1 ? "MinIO" : "the bucket");
        if (observed.writersReady() < Defaults.min(spec.writers())) {
            waiting.add("writers (" + observed.writersReady() + "/" + Defaults.min(spec.writers()) + " ready)");
        }
        if (observed.readersReady() < Defaults.min(spec.readers())) {
            waiting.add("readers (" + observed.readersReady() + "/" + Defaults.min(spec.readers()) + " ready)");
        }
        if (observed.routerReady() < 1) waiting.add("the router");
        boolean ready = waiting.isEmpty();
        String waitingFor = "Waiting for " + String.join(", ", waiting) + ".";
        var conditions = conditions(resource)
                .set(Phase.Pending.name(), false, REASON_PROVISIONED, "The objects are created.")
                .set(Phase.Provisioning.name(), !ready, ready ? REASON_READY : REASON_WAITING,
                        ready ? "" : waitingFor)
                .set(Phase.Ready.name(), ready, ready ? REASON_READY : REASON_WAITING,
                        ready ? "Storage, writers, readers and the router are ready." : waitingFor)
                .set(PharosClusterStatus.CONDITION_SPEC_INVALID, false, REASON_SPEC_VALID, "");
        return new PharosClusterStatus((ready ? Phase.Ready : Phase.Provisioning).name(), generation(resource),
                new PharosClusterStatus.Replicas(writers, observed.writersReady()),
                new PharosClusterStatus.Replicas(readers, observed.readersReady()),
                new PharosClusterStatus.StorageStatus(minio ? "minio" : "s3", storageReady),
                previous == null ? null : previous.projects(), previous == null ? null : previous.proposals(),
                previous == null ? null : previous.reshardProposals(), conditions.build());
    }

    private StatusConditions conditions(PharosCluster resource) {
        PharosClusterStatus previous = resource.getStatus();
        return new StatusConditions(previous == null ? null : previous.conditions(), generation(resource),
                clock.instant());
    }

    private static long generation(PharosCluster resource) {
        return resource.getMetadata().getGeneration() == null ? 0 : resource.getMetadata().getGeneration();
    }

    /** What the cluster's workloads report, from the informer caches. */
    record Observed(Integer writersReplicas, Integer readersReplicas, int writersReady, int readersReady,
                    int routerReady, int minioReady, boolean bucketCreated) {

        static Observed of(PharosCluster resource, Context<PharosCluster> context) {
            String name = resource.getMetadata().getName();
            Optional<StatefulSet> writers = context.getSecondaryResource(StatefulSet.class, DesiredState.WRITERS,
                    Names.writers(name));
            Optional<StatefulSet> readers = context.getSecondaryResource(StatefulSet.class, DesiredState.WRITERS,
                    Names.readers(name));
            Optional<StatefulSet> minio = context.getSecondaryResource(StatefulSet.class, DesiredState.WRITERS,
                    Names.minio(name));
            Optional<Deployment> router = context.getSecondaryResource(Deployment.class, DesiredState.ROUTER,
                    Names.router(name));
            Optional<Job> bucket = context.getSecondaryResource(Job.class, DesiredState.MINIO_BUCKET,
                    Names.minioBucketJob(name));
            return new Observed(
                    writers.map(s -> s.getSpec().getReplicas()).orElse(null),
                    readers.map(s -> s.getSpec().getReplicas()).orElse(null),
                    ready(writers), ready(readers),
                    router.map(d -> d.getStatus() == null ? null : d.getStatus().getReadyReplicas()).orElse(0),
                    ready(minio),
                    bucket.map(j -> j.getStatus() != null && j.getStatus().getSucceeded() != null
                            && j.getStatus().getSucceeded() >= 1).orElse(false));
        }

        private static int ready(Optional<StatefulSet> s) {
            return s.map(x -> x.getStatus() == null || x.getStatus().getReadyReplicas() == null
                    ? 0 : x.getStatus().getReadyReplicas()).orElse(0);
        }
    }

    /**
     * A fresh resource carrying only identity and status: the status patch is a server-side apply,
     * and a copy of the whole resource would make the operator a field manager of the spec too.
     */
    private static PharosCluster statusPatch(PharosCluster resource, PharosClusterStatus status) {
        PharosCluster patch = new PharosCluster();
        patch.setMetadata(new ObjectMetaBuilder()
                .withName(resource.getMetadata().getName())
                .withNamespace(resource.getMetadata().getNamespace())
                .build());
        patch.setStatus(status);
        return patch;
    }
}
