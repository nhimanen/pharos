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
import com.pharos.operator.render.StorageRenderer;
import com.pharos.operator.render.WorkloadRenderer;
import com.pharos.operator.scale.Autoscaler;
import com.pharos.operator.status.ClusterStatusClient;
import com.pharos.operator.status.ClusterStatusEventSource;
import com.pharos.operator.status.StatusReading;
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
                        new DesiredState.Sizing(writers, readers, work, cache, minioSize));
                var workflow = context.managedWorkflowAndDependentResourceContext();
                workflow.put(DesiredState.KEY, state);
                workflow.reconcileManagedWorkflow();
                Observed after = Observed.of(resource, context);
                watch.token(token(resource, context)); // the first reconcile has just created the Secret
                // every Job of this cluster: the informer is shared by name with the bucket Job's dependent
                List<Job> jobs = context.getSecondaryResourcesAsStream(Job.class, DesiredState.MINIO_BUCKET).toList();
                ProjectReconciler.Plan projects = ProjectReconciler.plan(resource, settings, jobs,
                        storageReady(spec, after), clock.instant());
                ProjectReconciler.apply(projects, resource, context);
                if (projects.reschedule() != null && (reschedule == null || projects.reschedule().compareTo(reschedule) < 0)) {
                    reschedule = projects.reschedule();
                }
                if (scale.recheck() != null && (reschedule == null || scale.recheck().compareTo(reschedule) < 0)) {
                    reschedule = scale.recheck();
                }
                raise(resource, context, scale.notices());
                desired = provisionedStatus(resource, writers, readers, after, volumeNotes, retries, projects,
                        scale, reading == null ? null : reading.status(),
                        reading == null ? (watch.problem() == null ? "no pharos node answered" : watch.problem()) : null);
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
                        previous.reshardProposals(), List.copyOf(conditions));
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
                conditions.build());
    }

    /** {@link #provisionedStatus(PharosCluster, int, int, Observed, List, Integer, ProjectReconciler.Plan, Autoscaler.Plan, ClusterStatus, String)} with nothing to note. */
    PharosClusterStatus provisionedStatus(PharosCluster resource, int writers, int readers, Observed observed) {
        return provisionedStatus(resource, writers, readers, observed, List.of(), null, null, null, null, null);
    }

    /** The storage is usable: minio is ready with its bucket created, or an (unprobed) s3 bucket. */
    static boolean storageReady(PharosClusterSpec spec, Observed observed) {
        return !StorageRenderer.minioMode(spec) || (observed.minioReady() >= 1 && observed.bucketCreated());
    }

    /**
     * The status once the objects are applied: {@code Ready} when the storage is ready and at least
     * {@code min} writers and readers and one router are ready, else {@code Provisioning};
     * {@code Degraded} when a project Job failed. {@code projects}: the project plan of this reconcile,
     * or null to keep the listed projects. {@code scale}: the autoscaler's plan, or null to keep its
     * records; {@code status}: the {@code /cluster/status} read, or null with {@code noAnswer} saying why.
     */
    PharosClusterStatus provisionedStatus(PharosCluster resource, int writers, int readers, Observed observed,
                                          List<String> volumeNotes, Integer bucketRetries,
                                          ProjectReconciler.Plan projects, Autoscaler.Plan scale,
                                          ClusterStatus status, String noAnswer) {
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
        boolean degraded = projects != null && !projects.failures().isEmpty();
        String degradedMessage = degraded ? String.join("; ", projects.failures()) : "";
        var conditions = conditions(resource)
                .set(Phase.Pending.name(), false, REASON_PROVISIONED, "The objects are created.")
                .set(Phase.Provisioning.name(), !ready, ready ? REASON_READY : REASON_WAITING,
                        ready ? "" : waitingFor)
                .set(Phase.Ready.name(), ready && !degraded,
                        degraded ? Phase.Degraded.name() : ready ? REASON_READY : REASON_WAITING,
                        degraded ? degradedMessage
                                : ready ? "Storage, writers, readers and the router are ready." : waitingFor)
                .set(Phase.Degraded.name(), degraded, degraded ? projects.degradedReason() : REASON_NONE,
                        degradedMessage)
                .set(PharosClusterStatus.CONDITION_STORAGE_READY, storageReady, storageReason, storageMessage)
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
        List<PharosClusterStatus.ProjectStatus> projectList = projects != null
                ? (projects.projects().isEmpty() ? null : projects.projects())
                : previous == null ? null : previous.projects();
        projectList = withClusterStatus(projectList, status, previous);
        Phase phase = degraded ? Phase.Degraded : ready ? Phase.Ready : Phase.Provisioning;
        return new PharosClusterStatus(phase.name(), generation(resource),
                new PharosClusterStatus.Replicas(writers, observed.writersReady()),
                new PharosClusterStatus.Replicas(readers, observed.readersReady()),
                new PharosClusterStatus.StorageStatus(minio ? "minio" : "s3", storageReady,
                        observed.bucketCreated() ? null : bucketRetries),
                projectList,
                scale != null ? scale.proposals() : previous == null ? null : previous.proposals(),
                scale != null ? scale.reshardProposals() : previous == null ? null : previous.reshardProposals(),
                conditions.build());
    }

    private static Condition previousCondition(PharosClusterStatus previous, String type) {
        if (previous == null || previous.conditions() == null) return null;
        return previous.conditions().stream().filter(c -> type.equals(c.getType())).findFirst().orElse(null);
    }

    /**
     * The projects with what {@code /cluster/status} reports for their shard: {@code generation},
     * {@code docCount} and {@code pending} (a change not yet published). Without a status, the values
     * of the previous status stand; a project without a shard yet has none.
     */
    static List<PharosClusterStatus.ProjectStatus> withClusterStatus(List<PharosClusterStatus.ProjectStatus> projects,
                                                                     ClusterStatus status,
                                                                     PharosClusterStatus previous) {
        if (projects == null) return null;
        Map<String, PharosClusterStatus.ProjectStatus> before = new HashMap<>();
        if (previous != null && previous.projects() != null) {
            for (var p : previous.projects()) if (p != null && p.name() != null) before.put(p.name(), p);
        }
        Map<String, ClusterStatus.Shard> shards = new HashMap<>();
        if (status != null && status.layout() != null && status.layout().shards() != null) {
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
            if (status != null) {
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
     * deleted (a retry is under way).
     */
    record Observed(Integer writersReplicas, Integer readersReplicas, int writersReady, int readersReady,
                    int routerReady, int minioReady, boolean bucketCreated, String workVolume, String cacheVolume,
                    String minioStorage, Instant bucketFailedAt, String bucketFailure, boolean bucketDeleting) {

        /** Readiness only, for status tests. */
        Observed(Integer writersReplicas, Integer readersReplicas, int writersReady, int readersReady,
                 int routerReady, int minioReady, boolean bucketCreated) {
            this(writersReplicas, readersReplicas, writersReady, readersReady, routerReady, minioReady, bucketCreated,
                    null, null, null, null, null, false);
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
                    bucket.map(j -> j.getMetadata().getDeletionTimestamp() != null).orElse(false));
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
