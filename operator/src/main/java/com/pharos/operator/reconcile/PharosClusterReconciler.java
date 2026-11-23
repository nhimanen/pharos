package com.pharos.operator.reconcile;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.operator.Defaults;
import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.Phase;
import com.pharos.operator.render.ConfigRenderer;
import com.pharos.operator.render.DesiredState;
import com.pharos.operator.render.Labels;
import com.pharos.operator.render.PreflightJobRenderer;
import com.pharos.operator.render.StorageRenderer;
import com.pharos.operator.render.WorkloadRenderer;
import com.pharos.operator.scale.Autoscaler;
import com.pharos.operator.scale.LastScale;
import com.pharos.operator.status.ClusterStatusClient;
import com.pharos.operator.status.ClusterStatusEventSource;
import com.pharos.operator.status.PhaseEvaluator;
import com.pharos.operator.status.StatusReading;
import com.pharos.operator.upgrade.UpgradeCoordinator;
import com.pharos.operator.validate.SpecValidator;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.javaoperatorsdk.operator.AggregatedOperatorException;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * nothing. No dependent ever deletes (see {@link Dependents}). What an existing object cannot take is
 * not rendered: StatefulSet volumes keep their size and the MinIO PVC never shrinks
 * ({@code VolumeChangeNotApplied}), the storage kind never changes ({@code SpecInvalid}), and a failed
 * apply is reported as {@code ApplyFailed}. This reconciler is the only writer of {@code .status}
 * (ruling OP-R3).
 *
 * <p>Replicas come from the {@link Autoscaler}: pharos's proposals, read from {@code /cluster/status}
 * by the {@link ClusterStatusEventSource} on its own threads, applied within the spec's bounds and
 * schedule floors. Its records ({@code status.proposals}, {@code status.reshardProposals}) and the
 * {@code ClusterStatusAvailable} and {@code WriterLowDisk} conditions are merged into the one status
 * patch of each reconcile.
 *
 * <p>Each workload's version comes from the {@link UpgradeCoordinator}: a changed {@code spec.version}
 * first runs the pre-flight Job, then rolls readers, writers and the router in that order; project Jobs
 * use the writers' version. The phase and its conditions come from the {@link PhaseEvaluator}.
 */
@Workflow(explicitInvocation = true, dependents = {
        @Dependent(name = DesiredState.TOKEN, type = Dependents.Secrets.class),
        @Dependent(name = DesiredState.MINIO_CREDENTIALS, type = Dependents.Secrets.class,
                useEventSourceWithName = DesiredState.TOKEN),
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
                useEventSourceWithName = DesiredState.WRITERS_HEADLESS),
        @Dependent(name = DesiredState.WRITERS, type = Dependents.StatefulSets.class),
        @Dependent(name = DesiredState.READERS, type = Dependents.StatefulSets.class,
                useEventSourceWithName = DesiredState.WRITERS),
        @Dependent(name = DesiredState.MINIO, type = Dependents.StatefulSets.class,
                useEventSourceWithName = DesiredState.WRITERS),
        @Dependent(name = DesiredState.ROUTER, type = Dependents.Deployments.class),
        @Dependent(name = DesiredState.MINIO_PVC, type = Dependents.Pvcs.class),
        @Dependent(name = DesiredState.MINIO_BUCKET, type = Dependents.Jobs.class),
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
    static final String REASON_MINIO_NOT_READY = "MinioNotReady";
    static final String REASON_BUCKET_PENDING = "BucketPending";
    static final String REASON_BUCKET_JOB_FAILED = "BucketJobFailed";
    static final String REASON_EXTERNAL = "External";
    static final String REASON_IMMUTABLE_VOLUME = "ImmutableVolume";
    static final String REASON_NONE = "None";
    static final String REASON_APPLY_FAILED = "ApplyFailed";
    static final String REASON_APPLIED = "Applied";
    static final String REASON_ANSWERED = "Answered";
    static final String REASON_NO_ANSWER = "NoNodeAnswered";
    static final String REASON_LOW_DISK = "LowDisk";
    static final String REASON_UNKNOWN = "Unknown";

    /** A status read older than this counts as no answer (the client re-reads every 60 s). */
    static final Duration STATUS_STALE_AFTER = ClusterStatusClient.REFRESH.multipliedBy(2).plusSeconds(30);

    /** Backoff of the failed bucket Job: 60 s, doubling per retry, at most 15 min. */
    static final long BUCKET_RETRY_FIRST_SECONDS = 60;
    static final long BUCKET_RETRY_MAX_SECONDS = 900;
    /** Longest text of one failure in the {@code ApplyFailed} message. */
    static final int MAX_ERROR_TEXT = 400;

    private static final Logger LOG = LoggerFactory.getLogger(PharosClusterReconciler.class);

    private final Clock clock;
    private final OperatorSettings settings;
    private final ClusterStatusEventSource statusSource;
    /** Completed reconciles; lets tests wait for one. */
    final AtomicLong reconciles = new AtomicLong();

    /** Production: status read from the pods over HTTP. */
    public PharosClusterReconciler(OperatorSettings settings) {
        this(Clock.systemUTC(), settings,
                new ClusterStatusEventSource(ClusterStatusEventSource.http(Clock.systemUTC())));
    }

    /** Tests without pharos nodes: no node ever answers. */
    PharosClusterReconciler(Clock clock) {
        this(clock, OperatorSettings.defaults());
    }

    PharosClusterReconciler(Clock clock, OperatorSettings settings) {
        this(clock, settings, new ClusterStatusEventSource(ClusterStatusEventSource.NEVER_ANSWERS));
    }

    PharosClusterReconciler(Clock clock, OperatorSettings settings, ClusterStatusEventSource statusSource) {
        this.clock = clock;
        this.settings = settings;
        this.statusSource = statusSource;
    }

    @Override
    public List<EventSource<?, PharosCluster>> prepareEventSources(EventSourceContext<PharosCluster> context) {
        return List.of(statusSource);
    }

    @Override
    public UpdateControl<PharosCluster> reconcile(PharosCluster resource, Context<PharosCluster> context) {
        try {
            List<String> errors = new ArrayList<>(SpecValidator.validate(resource));
            String kindChange = storageKindChange(resource);
            if (kindChange != null) errors.add(kindChange);
            ConfigRenderer.Result configs = null;
            if (errors.isEmpty()) {
                configs = ConfigRenderer.render(resource);
                errors.addAll(configs.errors());
            }
            PharosClusterStatus desired;
            Duration reschedule = null;
            if (!errors.isEmpty()) {
                desired = invalidStatus(resource, errors);
            } else {
                PharosClusterSpec spec = resource.getSpec();
                Observed before = Observed.of(resource, context);
                ClusterStatusEventSource.Watch watch = statusSource.watch(resource);
                watch.token(token(resource, context));
                Instant now = clock.instant();
                StatusReading reading = watch.latest()
                        .filter(r -> !now.isAfter(r.fetchedAt().plus(STATUS_STALE_AFTER))).orElse(null);
                Autoscaler.Plan scale = Autoscaler.plan(resource, before.writersReplicas(), before.readersReplicas(),
                        LastScale.parse(before.writersLastScale()), LastScale.parse(before.readersLastScale()),
                        reading, now);
                int writers = scale.writers().target();
                int readers = scale.readers().target();
                logScaling(resource, "writers", before.writersReplicas(), scale.writers());
                logScaling(resource, "readers", before.readersReplicas(), scale.readers());
                List<String> volumeNotes = new ArrayList<>();
                String work = keepSize(before.workVolume(), Defaults.workVolume(spec), "spec.writers.workVolume",
                        volumeNotes);
                String cache = keepSize(before.cacheVolume(), Defaults.cacheVolume(spec), "spec.readers.cacheVolume",
                        volumeNotes);
                String minioSize = noShrink(before.minioStorage(), Defaults.minioStorage(spec), volumeNotes);

                // every Job of this cluster: the informer is shared by name with the bucket Job's dependent
                List<Job> jobsBefore = context.getSecondaryResourcesAsStream(Job.class, DesiredState.MINIO_BUCKET).toList();
                UpgradeCoordinator.Plan upgrade = UpgradeCoordinator.plan(upgradeInput(resource, before, jobsBefore,
                        storageReady(spec, before), reading == null ? null : reading.status(), now));

                Integer retries = previousRetries(resource);
                if (StorageRenderer.minioMode(spec) && before.bucketFailedAt() != null && !before.bucketDeleting()) {
                    Instant due = before.bucketFailedAt().plusSeconds(bucketBackoffSeconds(retries));
                    if (now.isBefore(due)) {
                        reschedule = Duration.between(now, due);
                    } else {
                        retries = retries == null ? 1 : retries + 1;
                        LOG.info("PharosCluster {}/{}: the bucket Job failed; re-creating it (retry {})",
                                resource.getMetadata().getNamespace(), resource.getMetadata().getName(), retries);
                        context.getClient().batch().v1().jobs().inNamespace(resource.getMetadata().getNamespace())
                                .withName(Names.minioBucketJob(resource.getMetadata().getName()))
                                .withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
                    }
                }

                DesiredState state = DesiredState.render(resource, configs, settings,
                        new DesiredState.Sizing(writers, readers, work, cache, minioSize,
                                json(scale.writersLastScale()), json(scale.readersLastScale())), upgrade.versions());
                var workflow = context.managedWorkflowAndDependentResourceContext();
                workflow.put(DesiredState.KEY, state);
                workflow.reconcileManagedWorkflow();
                Observed after = Observed.of(resource, context);
                watch.token(token(resource, context)); // the first reconcile has just created the Secret
                applyUpgrade(upgrade, resource, context);
                List<Job> jobs = context.getSecondaryResourcesAsStream(Job.class, DesiredState.MINIO_BUCKET).toList();
                ProjectReconciler.Plan projects = ProjectReconciler.plan(resource, settings, jobs,
                        storageReady(spec, after), clock.instant(), upgrade.versions().writers());
                ProjectReconciler.apply(projects, resource, context);
                if (upgrade.recheck() != null && (reschedule == null || upgrade.recheck().compareTo(reschedule) < 0)) {
                    reschedule = upgrade.recheck();
                }
                if (projects.reschedule() != null && (reschedule == null || projects.reschedule().compareTo(reschedule) < 0)) {
                    reschedule = projects.reschedule();
                }
                if (scale.recheck() != null && (reschedule == null || scale.recheck().compareTo(reschedule) < 0)) {
                    reschedule = scale.recheck();
                }
                raise(resource, context, scale.notices());
                desired = provisionedStatus(resource, writers, readers, after, volumeNotes, retries, projects,
                        scale, reading == null ? null : reading.status(),
                        reading == null ? (watch.problem() == null ? "no pharos node answered" : watch.problem()) : null,
                        upgrade);
                Duration phaseRecheck = phaseRecheck(desired, clock.instant());
                if (phaseRecheck != null && (reschedule == null || phaseRecheck.compareTo(reschedule) < 0)) {
                    reschedule = phaseRecheck;
                }
            }
            UpdateControl<PharosCluster> control = Objects.equals(desired, resource.getStatus())
                    ? UpdateControl.noUpdate() : UpdateControl.patchStatus(statusPatch(resource, desired));
            return reschedule == null ? control : control.rescheduleAfter(reschedule);
        } finally {
            reconciles.incrementAndGet();
        }
    }

    /**
     * Any failure to apply the desired objects becomes the {@code ApplyFailed} condition (the rest of
     * the status is kept); JOSDK retries the reconcile.
     */
    @Override
    public ErrorStatusUpdateControl<PharosCluster> updateErrorStatus(PharosCluster resource,
                                                                    Context<PharosCluster> context, Exception e) {
        PharosClusterStatus previous = resource.getStatus();
        List<Condition> conditions = new ArrayList<>(previous == null || previous.conditions() == null
                ? List.of() : previous.conditions());
        Condition failed = conditions(resource)
                .set(PharosClusterStatus.CONDITION_APPLY_FAILED, true, REASON_APPLY_FAILED, applyError(e))
                .build().getFirst();
        conditions.removeIf(c -> PharosClusterStatus.CONDITION_APPLY_FAILED.equals(c.getType()));
        conditions.add(failed);
        PharosClusterStatus status = previous == null
                ? new PharosClusterStatus(Phase.Pending.name(), generation(resource), null, null, null, null, null,
                        null, List.copyOf(conditions))
                : new PharosClusterStatus(previous.phase(), previous.observedGeneration(), previous.writers(),
                        previous.readers(), previous.storage(), previous.projects(), previous.proposals(),
                        previous.reshardProposals(), List.copyOf(conditions), previous.upgrade());
        LOG.warn("PharosCluster {}/{}: applying the desired state failed: {}", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName(), failed.getMessage());
        return ErrorStatusUpdateControl.patchStatus(statusPatch(resource, status));
    }

    /**
     * Which objects failed and why, one short line each. Secret dependents already reduce their errors
     * to the name and HTTP code, so no secret value can reach this text.
     */
    static String applyError(Exception e) {
        List<String> parts = new ArrayList<>();
        if (e instanceof AggregatedOperatorException agg && !agg.getAggregatedExceptions().isEmpty()) {
            for (Map.Entry<String, Exception> x : new java.util.TreeMap<>(agg.getAggregatedExceptions()).entrySet()) {
                parts.add(x.getKey() + ": " + text(x.getValue()));
            }
        } else {
            parts.add(text(e));
        }
        return String.join("; ", parts);
    }

    private static String text(Throwable t) {
        String m = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
        m = m.lines().findFirst().orElse("").strip();
        return m.length() > MAX_ERROR_TEXT ? m.substring(0, MAX_ERROR_TEXT) + "..." : m;
    }

    /** A changed storage kind: the indexes live in the old bucket, so the kind is immutable. */
    static String storageKindChange(PharosCluster resource) {
        PharosClusterStatus status = resource.getStatus();
        String was = status == null || status.storage() == null ? null : status.storage().kind();
        PharosClusterSpec.Storage s = resource.getSpec() == null ? null : resource.getSpec().storage();
        if (was == null || s == null || (s.minio() == null) == (s.s3() == null)) return null;
        String now = s.minio() != null ? "minio" : "s3";
        return was.equals(now) ? null
                : "spec.storage: the storage kind is immutable (it is " + was + "; the spec asks for " + now
                + "): the indexes live in the existing bucket. Restore spec.storage." + was
                + ", or create a new PharosCluster";
    }

    /**
     * A StatefulSet's volume size: the existing claim template's (immutable; v1 does not resize),
     * with a note when the spec asks for another, or the spec's for a new StatefulSet.
     */
    static String keepSize(String current, String wanted, String field, List<String> notes) {
        if (current == null) return wanted;
        if (bytes(current).compareTo(bytes(wanted)) != 0) {
            notes.add(field + ": " + current + " -> " + wanted + " is not applied (StatefulSet volumes cannot be"
                    + " resized in v1); the volumes stay " + current);
        }
        return current;
    }

    /** The MinIO PVC size: the spec's, but never below the existing one (a PVC cannot shrink). */
    static String noShrink(String current, String wanted, List<String> notes) {
        if (current == null || bytes(wanted).compareTo(bytes(current)) >= 0) return wanted;
        notes.add("spec.storage.minio.storage: " + current + " -> " + wanted + " is not applied (a volume never"
                + " shrinks); it stays " + current);
        return current;
    }

    private static java.math.BigDecimal bytes(String q) {
        return Quantity.getAmountInBytes(new Quantity(q));
    }

    static long bucketBackoffSeconds(Integer retries) {
        int n = retries == null ? 0 : Math.min(retries, 10);
        return Math.min(BUCKET_RETRY_MAX_SECONDS, BUCKET_RETRY_FIRST_SECONDS << n);
    }

    private static Integer previousRetries(PharosCluster resource) {
        PharosClusterStatus s = resource.getStatus();
        return s == null || s.storage() == null ? null : s.storage().bucketRetries();
    }

    private static String json(LastScale s) {
        return s == null ? null : s.toJson();
    }

    /** The cluster token from its Secret (the informer's cache; held in memory only), or null. */
    private static String token(PharosCluster resource, Context<PharosCluster> context) {
        Optional<Secret> secret = context.getSecondaryResource(Secret.class, DesiredState.TOKEN,
                Names.token(resource.getMetadata().getName()));
        String b64 = secret.map(Secret::getData).map(d -> d.get(WorkloadRenderer.TOKEN_KEY)).orElse(null);
        if (b64 == null) return null;
        try {
            return new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8).strip();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static void logScaling(PharosCluster resource, String role, Integer before, Autoscaler.Decision d) {
        if (before != null && before != d.target()) {
            LOG.info("PharosCluster {}/{}: {} {} -> {}: {}", resource.getMetadata().getNamespace(),
                    resource.getMetadata().getName(), role, before, d.target(), d.reason());
        } else if (before != null) {
            LOG.debug("PharosCluster {}/{}: {} stay at {}: {}", resource.getMetadata().getNamespace(),
                    resource.getMetadata().getName(), role, before, d.reason());
        }
    }

    /**
     * Raises the autoscaler's Events on the {@code PharosCluster}. Best effort: a failure is logged and
     * changes nothing else.
     */
    private void raise(PharosCluster resource, Context<PharosCluster> context, List<Autoscaler.Notice> notices) {
        if (notices.isEmpty() || context == null) return;
        String ns = resource.getMetadata().getNamespace();
        String now = clock.instant().truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString();
        for (Autoscaler.Notice n : notices) {
            try {
                context.getClient().v1().events().inNamespace(ns).resource(new EventBuilder()
                        .withNewMetadata().withGenerateName(resource.getMetadata().getName() + "-")
                        .withNamespace(ns).endMetadata()
                        .withNewInvolvedObject().withApiVersion(resource.getApiVersion()).withKind(resource.getKind())
                        .withName(resource.getMetadata().getName()).withNamespace(ns)
                        .withUid(resource.getMetadata().getUid()).endInvolvedObject()
                        .withReason(n.reason()).withType(n.type()).withMessage(n.message())
                        .withFirstTimestamp(now).withLastTimestamp(now).withCount(1)
                        .withNewSource().withComponent(Labels.OPERATOR).endSource()
                        .build()).create();
            } catch (RuntimeException e) {
                LOG.warn("PharosCluster {}/{}: raising Event {} failed: {}", ns, resource.getMetadata().getName(),
                        n.reason(), e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage().lines()
                                .findFirst().orElse(""));
            }
        }
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
                conditions.build(), previous == null ? null : previous.upgrade());
    }

    /** {@link #provisionedStatus(PharosCluster, int, int, Observed, List, Integer, ProjectReconciler.Plan, Autoscaler.Plan, ClusterStatus, String, UpgradeCoordinator.Plan)} with nothing to note. */
    PharosClusterStatus provisionedStatus(PharosCluster resource, int writers, int readers, Observed observed) {
        return provisionedStatus(resource, writers, readers, observed, List.of(), null, null, null, null, null, null);
    }

    /** The storage is usable: minio is ready with its bucket created, or an (unprobed) s3 bucket. */
    static boolean storageReady(PharosClusterSpec spec, Observed observed) {
        return !StorageRenderer.minioMode(spec) || (observed.minioReady() >= 1 && observed.bucketCreated());
    }

    /**
     * The status once the objects are applied, with the phase and its conditions from the
     * {@link PhaseEvaluator}. {@code projects}: the project plan of this reconcile, or null to keep the
     * listed projects. {@code scale}: the autoscaler's plan, or null to keep its records;
     * {@code status}: the {@code /cluster/status} read, or null with {@code noAnswer} saying why;
     * {@code upgrade}: the upgrade's plan, or null when there is none to report.
     */
    PharosClusterStatus provisionedStatus(PharosCluster resource, int writers, int readers, Observed observed,
                                          List<String> volumeNotes, Integer bucketRetries,
                                          ProjectReconciler.Plan projects, Autoscaler.Plan scale,
                                          ClusterStatus status, String noAnswer, UpgradeCoordinator.Plan upgrade) {
        PharosClusterSpec spec = resource.getSpec();
        PharosClusterStatus previous = resource.getStatus();
        boolean minio = StorageRenderer.minioMode(spec);
        // s3: an existing bucket the operator does not probe; reachability is reported by the nodes
        boolean storageReady = storageReady(spec, observed);
        String storageReason;
        String storageMessage;
        if (!minio) {
            storageReason = REASON_EXTERNAL;
            storageMessage = "An existing S3 bucket; the operator does not probe it.";
        } else if (observed.bucketFailedAt() != null) { // reported first: it needs a retry, not just time
            storageReason = REASON_BUCKET_JOB_FAILED;
            storageMessage = "The bucket Job " + Names.minioBucketJob(resource.getMetadata().getName()) + " failed"
                    + (observed.bucketFailure() == null ? "" : " (" + observed.bucketFailure() + ")")
                    + "; it is re-created after " + bucketBackoffSeconds(bucketRetries) + " s"
                    + (bucketRetries == null ? "" : " (retries so far: " + bucketRetries + ")") + ".";
        } else if (observed.minioReady() < 1) {
            storageReason = REASON_MINIO_NOT_READY;
            storageMessage = "Waiting for MinIO to become ready.";
        } else if (!observed.bucketCreated()) {
            storageReason = REASON_BUCKET_PENDING;
            storageMessage = "Waiting for the bucket Job to create the bucket.";
        } else {
            storageReason = REASON_READY;
            storageMessage = "MinIO is ready and the bucket exists.";
        }
        List<PharosClusterStatus.ProjectStatus> projectList = projects != null
                ? (projects.projects().isEmpty() ? null : projects.projects())
                : previous == null ? null : previous.projects();
        projectList = withClusterStatus(projectList, status, previous);
        Map<String, Long> published = new java.util.TreeMap<>();
        if (spec.projects() != null) {
            Map<String, Long> generations = new HashMap<>();
            if (projectList != null) for (var p : projectList) if (p.generation() != null) generations.put(p.name(), p.generation());
            for (PharosClusterSpec.Project p : spec.projects()) {
                if (p != null && p.name() != null) published.put(p.name(), generations.get(p.name()));
            }
        }
        Instant now = clock.instant();
        boolean blocked = upgrade != null && upgrade.blocked();
        String upgradeReason = upgrade != null ? upgrade.reason() : UpgradeCoordinator.REASON_UP_TO_DATE;
        String upgradeMessage = upgrade != null ? upgrade.message() : "Every workload runs " + spec.version() + ".";
        PhaseEvaluator.Result phase = PhaseEvaluator.evaluate(new PhaseEvaluator.Input(storageReady, storageReason,
                storageMessage, minio && observed.bucketFailedAt() != null, minio, Defaults.min(spec.writers()),
                observed.writersReady(), Defaults.min(spec.readers()), observed.readersReady(), observed.routerReady(),
                status, published, projects == null ? List.of() : projects.failures(),
                projects == null ? null : projects.degradedReason(), blocked, upgradeReason, upgradeMessage,
                previous == null ? null : previous.conditions(), now));
        var conditions = conditions(resource);
        for (PhaseEvaluator.Cond c : phase.conditions()) conditions.set(c.type(), c.status(), c.reason(), c.message());
        conditions
                .set(PharosClusterStatus.CONDITION_VOLUME_CHANGE_NOT_APPLIED, !volumeNotes.isEmpty(),
                        volumeNotes.isEmpty() ? REASON_NONE : REASON_IMMUTABLE_VOLUME, String.join("; ", volumeNotes))
                .set(PharosClusterStatus.CONDITION_APPLY_FAILED, false, REASON_APPLIED, "")
                .set(PharosClusterStatus.CONDITION_SPEC_INVALID, false, REASON_SPEC_VALID, "");
        if (status != null) {
            conditions.set(PharosClusterStatus.CONDITION_STATUS_AVAILABLE, true, REASON_ANSWERED,
                    "A pharos node answers /cluster/status.");
        } else {
            conditions.set(PharosClusterStatus.CONDITION_STATUS_AVAILABLE, false, REASON_NO_ANSWER,
                    (noAnswer == null ? "no pharos node answered" : noAnswer)
                            + "; the min floors still apply, no proposal is applied.");
        }
        List<String> lowDisk = scale == null ? null : scale.lowDisk();
        if (lowDisk != null) {
            conditions.set(PharosClusterStatus.CONDITION_WRITER_LOW_DISK, !lowDisk.isEmpty(),
                    lowDisk.isEmpty() ? REASON_NONE : REASON_LOW_DISK,
                    lowDisk.isEmpty() ? "" : String.join("; ", lowDisk) + " (v1 does not resize volumes)");
        } else {
            Condition was = previousCondition(previous, PharosClusterStatus.CONDITION_WRITER_LOW_DISK);
            if (was != null) {
                conditions.set(was.getType(), "True".equals(was.getStatus()), was.getReason(), was.getMessage());
            } else {
                conditions.set(PharosClusterStatus.CONDITION_WRITER_LOW_DISK, false, REASON_UNKNOWN,
                        "No pharos node has answered yet.");
            }
        }
        return new PharosClusterStatus(phase.phase().name(), generation(resource),
                new PharosClusterStatus.Replicas(writers, observed.writersReady()),
                new PharosClusterStatus.Replicas(readers, observed.readersReady()),
                new PharosClusterStatus.StorageStatus(minio ? "minio" : "s3", storageReady,
                        observed.bucketCreated() ? null : bucketRetries),
                projectList,
                scale != null ? scale.proposals() : previous == null ? null : previous.proposals(),
                scale != null ? scale.reshardProposals() : previous == null ? null : previous.reshardProposals(),
                conditions.build(),
                upgrade != null ? upgrade.upgrade() : previous == null ? null : previous.upgrade());
    }

    /**
     * When the readers' grace runs out: readers below {@code min} since a transition time turn the
     * phase Degraded {@link PhaseEvaluator#READERS_GRACE} later, with no event of their own.
     */
    static Duration phaseRecheck(PharosClusterStatus status, Instant now) {
        Condition readers = previousCondition(status, PharosClusterStatus.CONDITION_READERS_READY);
        if (readers == null || !"False".equals(readers.getStatus())
                || !PhaseEvaluator.REASON_BELOW_MIN.equals(readers.getReason())
                || readers.getLastTransitionTime() == null) {
            return null;
        }
        Instant due = Instant.parse(readers.getLastTransitionTime()).plus(PhaseEvaluator.READERS_GRACE).plusSeconds(1);
        return now.isBefore(due) ? Duration.between(now, due) : null;
    }

    /** What the {@link UpgradeCoordinator} needs, from the informer caches. */
    UpgradeCoordinator.Input upgradeInput(PharosCluster resource, Observed observed, List<Job> jobs,
                                          boolean storageReady, ClusterStatus status, Instant now) {
        String name = resource.getMetadata().getName();
        String target = resource.getSpec().version();
        String targetJob = Names.preflightJob(name, target);
        String postJob = Names.preflightPostJob(name, target);
        UpgradeCoordinator.Preflight preflight = UpgradeCoordinator.Preflight.ABSENT;
        UpgradeCoordinator.Preflight postCheck = UpgradeCoordinator.Preflight.ABSENT;
        List<String> others = new ArrayList<>();
        for (Job j : jobs) {
            Map<String, String> labels = j.getMetadata().getLabels();
            if (labels == null || !name.equals(labels.get(Labels.CLUSTER))
                    || !Labels.PREFLIGHT.equals(labels.get(Labels.COMPONENT))) {
                continue;
            }
            ProjectReconciler.ProjectJob pj = new ProjectReconciler.ProjectJob(j);
            if (targetJob.equals(pj.name())) {
                preflight = checkState(pj);
            } else if (postJob.equals(pj.name())) {
                postCheck = checkState(pj);
            } else if (!pj.deleting()) {
                others.add(pj.name());
            }
        }
        java.util.Collections.sort(others);
        PharosClusterStatus previous = resource.getStatus();
        return new UpgradeCoordinator.Input(name, target, observed.readersWorkload(), observed.writersWorkload(),
                observed.routerWorkload(), preflight, postCheck, List.copyOf(others), storageReady, status,
                previous == null ? null : previous.upgrade(), now);
    }

    /** A check Job's state; failed with reason {@code PodFailurePolicy} (exit 3) is a refusal. */
    private static UpgradeCoordinator.Preflight checkState(ProjectReconciler.ProjectJob pj) {
        if (pj.deleting()) {
            return new UpgradeCoordinator.Preflight(UpgradeCoordinator.PreflightState.DELETING, null, null);
        } else if (pj.succeeded()) {
            return new UpgradeCoordinator.Preflight(UpgradeCoordinator.PreflightState.SUCCEEDED, null, null);
        } else if (pj.failed()) {
            boolean refused = PreflightJobRenderer.REASON_REFUSED.equals(pj.failure());
            return new UpgradeCoordinator.Preflight(refused ? UpgradeCoordinator.PreflightState.REFUSED
                    : UpgradeCoordinator.PreflightState.FAILED, pj.failedAt(), pj.failure());
        }
        return new UpgradeCoordinator.Preflight(UpgradeCoordinator.PreflightState.RUNNING, null, null);
    }

    /**
     * Applies the upgrade's Job changes: deletes pre-flight Jobs (Background), then creates the spec's
     * version's (owned by the cluster; one that exists already is left alone). Names only are logged.
     */
    private void applyUpgrade(UpgradeCoordinator.Plan plan, PharosCluster resource, Context<PharosCluster> context) {
        String ns = resource.getMetadata().getNamespace();
        String name = resource.getMetadata().getName();
        for (String job : plan.deleteJobs()) {
            LOG.info("PharosCluster {}/{}: deleting pre-flight Job {}", ns, name, job);
            context.getClient().batch().v1().jobs().inNamespace(ns).withName(job)
                    .withPropagationPolicy(DeletionPropagation.BACKGROUND).delete();
        }
        if (plan.createPreflight() || plan.createPostCheck()) {
            Job job = plan.createPreflight() ? PreflightJobRenderer.render(resource, settings, resource.getSpec().version())
                    : PreflightJobRenderer.renderPostCheck(resource, settings, resource.getSpec().version());
            job.addOwnerReference(resource);
            LOG.info("PharosCluster {}/{}: creating pre-flight Job {} ({})", ns, name, job.getMetadata().getName(),
                    plan.message());
            try {
                context.resourceOperations().create(job);
            } catch (io.fabric8.kubernetes.client.KubernetesClientException e) {
                if (e.getCode() != 409) {
                    throw new IllegalStateException("creating Job " + job.getMetadata().getName() + " failed (HTTP "
                            + e.getCode() + ")", e);
                }
            }
        }
    }

    private static Condition previousCondition(PharosClusterStatus previous, String type) {
        if (previous == null || previous.conditions() == null) return null;
        return previous.conditions().stream().filter(c -> type.equals(c.getType())).findFirst().orElse(null);
    }

    /**
     * The projects with what {@code /cluster/status} reports for them: {@code generation} (null until
     * published), {@code docCount} and {@code pending} (a change not yet published). These come from the
     * status's {@code projects[]} (ruling P6-R15: a split project is one entry over its active shards,
     * never a staged or retired one, so a project mid-reshard stays published with its summed docCount).
     * A status without {@code projects[]} (a pharos before Phase 6, e.g. during a rolling upgrade) is read
     * per shard, by its {@code project} key. Without a status, the values of the previous status stand; a
     * project without a shard yet has none.
     */
    static List<PharosClusterStatus.ProjectStatus> withClusterStatus(List<PharosClusterStatus.ProjectStatus> projects,
                                                                     ClusterStatus status,
                                                                     PharosClusterStatus previous) {
        if (projects == null) return null;
        Map<String, PharosClusterStatus.ProjectStatus> before = new HashMap<>();
        if (previous != null && previous.projects() != null) {
            for (var p : previous.projects()) if (p != null && p.name() != null) before.put(p.name(), p);
        }
        Map<String, ClusterStatus.Project> fromProjects = new HashMap<>();
        if (status != null && status.projects() != null) {
            for (ClusterStatus.Project p : status.projects()) if (p != null && p.project() != null) fromProjects.put(p.project(), p);
        }
        Map<String, ClusterStatus.Shard> shards = new HashMap<>();
        if (fromProjects.isEmpty() && status != null && status.layout() != null && status.layout().shards() != null) {
            for (ClusterStatus.Shard s : status.layout().shards()) {
                if (s == null) continue;
                String project = s.keys() != null && s.keys().get("project") != null ? s.keys().get("project") : s.shard();
                if (project != null) shards.put(project, s);
            }
        }
        List<PharosClusterStatus.ProjectStatus> out = new ArrayList<>();
        for (var p : projects) {
            Long generation = null;
            Long docCount = null;
            Boolean pending = null;
            if (status != null && !fromProjects.isEmpty()) {
                ClusterStatus.Project cp = fromProjects.get(p.name());
                if (cp != null) {
                    generation = cp.published() ? cp.generation() : null;
                    docCount = cp.docCount();
                    pending = cp.pending();
                }
            } else if (status != null) {
                ClusterStatus.Shard s = shards.get(p.name());
                if (s != null) {
                    generation = s.generation();
                    docCount = s.docCount();
                    pending = s.backlog() != null ? s.backlog().pending() : "lagging".equals(s.state());
                }
            } else if (before.get(p.name()) != null) {
                var b = before.get(p.name());
                generation = b.generation();
                docCount = b.docCount();
                pending = b.pending();
            }
            out.add(new PharosClusterStatus.ProjectStatus(p.name(), p.registered(), generation, docCount, pending,
                    p.retries()));
        }
        return out;
    }

    private StatusConditions conditions(PharosCluster resource) {
        PharosClusterStatus previous = resource.getStatus();
        return new StatusConditions(previous == null ? null : previous.conditions(), generation(resource),
                clock.instant());
    }

    private static long generation(PharosCluster resource) {
        return resource.getMetadata().getGeneration() == null ? 0 : resource.getMetadata().getGeneration();
    }

    /**
     * What the cluster's workloads report, from the informer caches. {@code bucketFailedAt}: when the
     * bucket Job's {@code Failed} condition became true, or null; {@code bucketDeleting}: it is being
     * deleted (a retry is under way). The {@code *Workload}s: each workload's version and rollout for
     * the {@link UpgradeCoordinator}.
     */
    record Observed(Integer writersReplicas, Integer readersReplicas, int writersReady, int readersReady,
                    int routerReady, int minioReady, boolean bucketCreated, String workVolume, String cacheVolume,
                    String minioStorage, Instant bucketFailedAt, String bucketFailure, boolean bucketDeleting,
                    String writersLastScale, String readersLastScale, UpgradeCoordinator.Workload writersWorkload,
                    UpgradeCoordinator.Workload readersWorkload, UpgradeCoordinator.Workload routerWorkload) {

        /** Readiness only, for status tests. */
        Observed(Integer writersReplicas, Integer readersReplicas, int writersReady, int readersReady,
                 int routerReady, int minioReady, boolean bucketCreated) {
            this(writersReplicas, readersReplicas, writersReady, readersReady, routerReady, minioReady, bucketCreated,
                    null, null, null, null, null, false, null, null, UpgradeCoordinator.Workload.ABSENT,
                    UpgradeCoordinator.Workload.ABSENT, UpgradeCoordinator.Workload.ABSENT);
        }

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
            Optional<PersistentVolumeClaim> pvc = context.getSecondaryResource(PersistentVolumeClaim.class,
                    DesiredState.MINIO_PVC, Names.minio(name));
            Optional<JobCondition> failed = bucket.flatMap(j -> j.getStatus() == null || j.getStatus().getConditions() == null
                    ? Optional.empty()
                    : j.getStatus().getConditions().stream()
                            .filter(c -> "Failed".equals(c.getType()) && "True".equals(c.getStatus())).findFirst());
            return new Observed(
                    writers.map(s -> s.getSpec().getReplicas()).orElse(null),
                    readers.map(s -> s.getSpec().getReplicas()).orElse(null),
                    ready(writers), ready(readers),
                    router.map(d -> d.getStatus() == null ? null : d.getStatus().getReadyReplicas()).orElse(0),
                    ready(minio),
                    bucket.map(j -> j.getStatus() != null && j.getStatus().getSucceeded() != null
                            && j.getStatus().getSucceeded() >= 1).orElse(false),
                    claimSize(writers), claimSize(readers),
                    pvc.map(p -> p.getSpec() == null || p.getSpec().getResources() == null
                                    || p.getSpec().getResources().getRequests() == null ? null
                            : p.getSpec().getResources().getRequests().get("storage"))
                            .map(Quantity::toString).orElse(null),
                    failed.map(c -> c.getLastTransitionTime() == null ? Instant.EPOCH
                            : Instant.parse(c.getLastTransitionTime())).orElse(null),
                    failed.map(c -> c.getReason()).orElse(null),
                    bucket.map(j -> j.getMetadata().getDeletionTimestamp() != null).orElse(false),
                    annotation(writers, Labels.LAST_SCALE), annotation(readers, Labels.LAST_SCALE),
                    writers.map(Observed::workload).orElse(UpgradeCoordinator.Workload.ABSENT),
                    readers.map(Observed::workload).orElse(UpgradeCoordinator.Workload.ABSENT),
                    router.map(Observed::workload).orElse(UpgradeCoordinator.Workload.ABSENT));
        }

        /**
         * A StatefulSet as the upgrade sees it: rolled out when the controller has seen its current
         * generation, every replica is updated to the current template and ready, and the current
         * revision is the update revision.
         */
        static UpgradeCoordinator.Workload workload(StatefulSet s) {
            int replicas = s.getSpec().getReplicas() == null ? 1 : s.getSpec().getReplicas();
            var st = s.getStatus();
            boolean rolledOut = st != null && seen(st.getObservedGeneration(), s.getMetadata().getGeneration())
                    && nz(st.getUpdatedReplicas()) >= replicas && nz(st.getReadyReplicas()) >= replicas
                    && (st.getUpdateRevision() == null || st.getUpdateRevision().equals(st.getCurrentRevision()));
            return new UpgradeCoordinator.Workload(version(s.getSpec().getTemplate()), rolledOut, replicas,
                    s.getMetadata().getAnnotations() == null ? null
                            : s.getMetadata().getAnnotations().get(Labels.VERSION_SINCE));
        }

        /** The router Deployment: rolled out when every replica is updated and available, and no old one is left. */
        static UpgradeCoordinator.Workload workload(Deployment d) {
            int replicas = d.getSpec().getReplicas() == null ? 1 : d.getSpec().getReplicas();
            var st = d.getStatus();
            boolean rolledOut = st != null && seen(st.getObservedGeneration(), d.getMetadata().getGeneration())
                    && nz(st.getUpdatedReplicas()) >= replicas && nz(st.getAvailableReplicas()) >= replicas
                    && nz(st.getReplicas()) <= replicas;
            return new UpgradeCoordinator.Workload(version(d.getSpec().getTemplate()), rolledOut, replicas,
                    d.getMetadata().getAnnotations() == null ? null
                            : d.getMetadata().getAnnotations().get(Labels.VERSION_SINCE));
        }

        private static boolean seen(Long observed, Long generation) {
            return observed != null && observed >= (generation == null ? 0 : generation);
        }

        private static int nz(Integer i) {
            return i == null ? 0 : i;
        }

        /** The tag of the pod template's {@code pharos} container image ({@code repo[:port]/name:tag}), or "". */
        static String version(io.fabric8.kubernetes.api.model.PodTemplateSpec template) {
            if (template == null || template.getSpec() == null || template.getSpec().getContainers() == null) return "";
            String image = template.getSpec().getContainers().stream().filter(c -> "pharos".equals(c.getName()))
                    .map(io.fabric8.kubernetes.api.model.Container::getImage).findFirst().orElse(null);
            if (image == null) return "";
            int colon = image.lastIndexOf(':');
            return colon > image.lastIndexOf('/') ? image.substring(colon + 1) : "";
        }

        private static String annotation(Optional<StatefulSet> s, String key) {
            return s.map(x -> x.getMetadata().getAnnotations()).map(a -> a.get(key)).orElse(null);
        }

        private static int ready(Optional<StatefulSet> s) {
            return s.map(x -> x.getStatus() == null || x.getStatus().getReadyReplicas() == null
                    ? 0 : x.getStatus().getReadyReplicas()).orElse(0);
        }

        /** The size of a StatefulSet's (only) volume claim template, or null. */
        private static String claimSize(Optional<StatefulSet> s) {
            return s.map(x -> x.getSpec().getVolumeClaimTemplates())
                    .filter(v -> !v.isEmpty())
                    .map(v -> v.getFirst().getSpec().getResources().getRequests().get("storage"))
                    .map(Quantity::toString).orElse(null);
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
