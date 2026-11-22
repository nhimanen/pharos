package com.pharos.operator.reconcile;

import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.ProjectStatus;
import com.pharos.operator.render.Labels;
import com.pharos.operator.render.ProjectJobRenderer;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Keeps the shard catalog in line with {@code spec.projects} through one-shot Jobs
 * ({@link ProjectJobRenderer}), and reports each project in {@code status.projects}. It plans from
 * what the informer shows ({@link #plan}, pure) and the reconciler applies the plan ({@link #apply}).
 *
 * <p>Jobs are create-only and named after what they do, so the Jobs themselves record progress:
 * <ul>
 *   <li><b>Registration.</b> A project in the spec gets {@code <name>-register-<k8sName>-<specHash>};
 *       a new url, ref or embed is a new hash and so a new Job. A succeeded Job of the current source
 *       counts only while no other registration Job of the project is newer (a source reverted
 *       A → B → A would otherwise leave the catalog on B): else it is deleted and runs again.
 *       Two registrations of one project never run at once (the catalog would end with whichever wrote last): an unfinished Job of an
 *       older source is deleted, with its pods, before the new one is created. Once the current Job
 *       succeeded, finished Jobs of older sources are deleted.</li>
 *   <li><b>Removal.</b> A project that left the spec (still listed in the status, or with
 *       registration Jobs left) gets {@code <name>-remove-<k8sName>}, after its unfinished
 *       registrations are deleted. Once it succeeded, the project's registration Jobs are deleted,
 *       then the removal Job, and the project leaves the status.</li>
 *   <li><b>Re-added while removing.</b> No registration while the removal Job runs; once it finished,
 *       the removal Job and the project's registration Jobs are deleted, so the next reconcile
 *       registers the project afresh.</li>
 *   <li><b>Failure.</b> A Job whose {@code Failed} condition is true makes the cluster Degraded with the
 *       project, the Job and its reason. After a backoff (60 s doubling to 15 min, from the condition's
 *       {@code lastTransitionTime}; the count is the project's {@code retries}) it is deleted, and the
 *       next reconcile creates it again.</li>
 * </ul>
 * Nothing is created before the storage is ready: the Jobs write to the bucket.
 */
public final class ProjectReconciler {

    /** Backoff of a failed project Job: 60 s, doubling per retry, at most 15 min (as the bucket Job's). */
    static final long RETRY_FIRST_SECONDS = 60;
    static final long RETRY_MAX_SECONDS = 900;

    /** Degraded reasons. */
    static final String REASON_REGISTRATION_FAILED = "RegistrationJobFailed";
    static final String REASON_REMOVAL_FAILED = "RemovalJobFailed";

    private static final Logger LOG = LoggerFactory.getLogger(ProjectReconciler.class);

    private ProjectReconciler() {}

    /** A Job to delete, and how its pods go. */
    record Deletion(String job, DeletionPropagation propagation) {}

    /**
     * What to do and report. {@code failures}: one line per failed Job; {@code degradedReason}: the
     * reason for the Degraded condition, or null; {@code reschedule}: when the next retry is due, or null.
     */
    record Plan(List<Job> create, List<Deletion> delete, List<ProjectStatus> projects, List<String> failures,
                String degradedReason, Duration reschedule) {}

    /** One project Job as the informer shows it. */
    record ProjectJob(Job job) {

        String name() { return job.getMetadata().getName(); }

        String project() { return job.getMetadata().getAnnotations().get(Labels.PROJECT_ANNOTATION); }

        boolean registration() { return Labels.REGISTER.equals(job.getMetadata().getLabels().get(Labels.COMPONENT)); }

        boolean deleting() { return job.getMetadata().getDeletionTimestamp() != null; }

        /** When the Job was created, or null when unknown. */
        Instant created() {
            String t = job.getMetadata().getCreationTimestamp();
            return t == null ? null : Instant.parse(t);
        }

        /** Created after {@code other} (false when either time is unknown). */
        boolean newerThan(ProjectJob other) {
            Instant a = created();
            Instant b = other.created();
            return a != null && b != null && a.isAfter(b);
        }

        Optional<JobCondition> condition(String type) {
            if (job.getStatus() == null || job.getStatus().getConditions() == null) return Optional.empty();
            return job.getStatus().getConditions().stream()
                    .filter(c -> type.equals(c.getType()) && "True".equals(c.getStatus())).findFirst();
        }

        boolean succeeded() {
            return condition("Complete").isPresent()
                    || (job.getStatus() != null && job.getStatus().getSucceeded() != null && job.getStatus().getSucceeded() >= 1);
        }

        boolean failed() { return !succeeded() && condition("Failed").isPresent(); }

        boolean finished() { return succeeded() || failed(); }

        /** Still running, or being deleted with pods that may still run. */
        boolean active() { return !finished() || deleting(); }

        Instant failedAt() {
            return condition("Failed").map(JobCondition::getLastTransitionTime).map(Instant::parse).orElse(Instant.EPOCH);
        }

        String failure() {
            return condition("Failed").map(JobCondition::getReason).orElse(null);
        }
    }

    /** The project Jobs of {@code cluster} among {@code jobs}. */
    static List<ProjectJob> projectJobs(PharosCluster cluster, Collection<Job> jobs) {
        String name = cluster.getMetadata().getName();
        List<ProjectJob> out = new ArrayList<>();
        for (Job j : jobs) {
            Map<String, String> labels = j.getMetadata().getLabels();
            Map<String, String> annotations = j.getMetadata().getAnnotations();
            if (labels == null || annotations == null || !name.equals(labels.get(Labels.CLUSTER))) continue;
            String component = labels.get(Labels.COMPONENT);
            if (!(Labels.REGISTER.equals(component) || Labels.REMOVE.equals(component))) continue;
            if (annotations.get(Labels.PROJECT_ANNOTATION) == null) continue;
            out.add(new ProjectJob(j));
        }
        return out;
    }

    static long backoffSeconds(Integer retries) {
        int n = retries == null ? 0 : Math.min(retries, 10);
        return Math.min(RETRY_MAX_SECONDS, RETRY_FIRST_SECONDS << n);
    }

    /** Plans the project Jobs of a valid spec from the Jobs the informer shows. */
    static Plan plan(PharosCluster cluster, OperatorSettings settings, Collection<Job> jobs, boolean storageReady,
                     Instant now) {
        String name = cluster.getMetadata().getName();
        PharosClusterSpec spec = cluster.getSpec();
        PharosClusterStatus status = cluster.getStatus();
        Map<String, ProjectStatus> previous = new LinkedHashMap<>();
        if (status != null && status.projects() != null) {
            for (ProjectStatus p : status.projects()) if (p != null && p.name() != null) previous.put(p.name(), p);
        }
        Map<String, PharosClusterSpec.Project> wanted = new LinkedHashMap<>();
        if (spec.projects() != null) for (PharosClusterSpec.Project p : spec.projects()) wanted.put(p.name(), p);
        Map<String, List<ProjectJob>> registrations = new LinkedHashMap<>();
        Map<String, ProjectJob> removals = new LinkedHashMap<>();
        for (ProjectJob j : projectJobs(cluster, jobs)) {
            if (j.registration()) {
                registrations.computeIfAbsent(j.project(), k -> new ArrayList<>()).add(j);
            } else if (j.name().equals(Names.removeJob(name, j.project()))) {
                removals.put(j.project(), j);
            }
        }

        Builder b = new Builder(cluster, settings, storageReady, now);
        for (PharosClusterSpec.Project p : wanted.values()) {
            b.wanted(p, previous.get(p.name()), registrations.getOrDefault(p.name(), List.of()),
                    removals.get(p.name()));
        }
        // left the spec: listed in the status, or with registration or removal Jobs that are not going away
        Set<String> removing = new TreeSet<>(previous.keySet());
        registrations.forEach((project, js) -> {
            if (js.stream().anyMatch(j -> !j.deleting())) removing.add(project);
        });
        removals.forEach((project, j) -> {
            if (!j.deleting()) removing.add(project);
        });
        removing.removeAll(wanted.keySet());
        for (String project : removing) {
            b.removed(project, previous.get(project), registrations.getOrDefault(project, List.of()),
                    removals.get(project));
        }
        return b.build();
    }

    /** Collects one plan. */
    private static final class Builder {
        private final PharosCluster cluster;
        private final OperatorSettings settings;
        private final boolean storageReady;
        private final Instant now;
        private final List<Job> create = new ArrayList<>();
        private final Map<String, Deletion> delete = new LinkedHashMap<>();
        private final List<ProjectStatus> projects = new ArrayList<>();
        private final List<String> failures = new ArrayList<>();
        private final Set<String> reasons = new LinkedHashSet<>();
        private Duration reschedule;

        Builder(PharosCluster cluster, OperatorSettings settings, boolean storageReady, Instant now) {
            this.cluster = cluster;
            this.settings = settings;
            this.storageReady = storageReady;
            this.now = now;
        }

        private void delete(ProjectJob j, DeletionPropagation propagation) {
            if (!j.deleting()) delete.putIfAbsent(j.name(), new Deletion(j.name(), propagation));
        }

        private void create(Job job) {
            if (storageReady) create.add(job);
        }

        /** {@code previous} with {@code registered} and {@code retries} replaced; the cluster-status fields kept. */
        private static ProjectStatus entry(String project, ProjectStatus previous, boolean registered, Integer retries) {
            return previous == null ? new ProjectStatus(project, registered, null, null, null, retries)
                    : new ProjectStatus(project, registered, previous.generation(), previous.docCount(),
                            previous.pending(), retries);
        }

        private static boolean registered(ProjectStatus previous) {
            return previous != null && Boolean.TRUE.equals(previous.registered());
        }

        /**
         * A failed Job: reported, then deleted for a retry once its backoff is over.
         *
         * @return the project's retry count after this reconcile
         */
        private Integer failed(String project, ProjectJob j, ProjectStatus previous, String what, String reason) {
            Integer retries = previous == null ? null : previous.retries();
            Instant due = j.failedAt().plusSeconds(backoffSeconds(retries));
            reasons.add(reason);
            if (!j.deleting() && !now.isBefore(due)) {
                retries = retries == null ? 1 : retries + 1;
                LOG.info("PharosCluster {}/{}: the {} Job {} of project {} failed; re-creating it (retry {})",
                        cluster.getMetadata().getNamespace(), cluster.getMetadata().getName(), what, j.name(),
                        project, retries);
                delete(j, DeletionPropagation.BACKGROUND);
                failures.add("project " + project + ": the " + what + " Job " + j.name() + " failed"
                        + (j.failure() == null ? "" : " (" + j.failure() + ")") + "; re-creating it (retry " + retries
                        + ")");
                return retries;
            }
            if (!j.deleting()) {
                Duration left = Duration.between(now, due);
                reschedule = reschedule == null || left.compareTo(reschedule) < 0 ? left : reschedule;
            }
            failures.add("project " + project + ": the " + what + " Job " + j.name() + " failed"
                    + (j.failure() == null ? "" : " (" + j.failure() + ")") + "; it is re-created after "
                    + backoffSeconds(retries) + " s" + (retries == null ? "" : " (retries so far: " + retries + ")"));
            return retries;
        }

        void wanted(PharosClusterSpec.Project p, ProjectStatus previous, List<ProjectJob> regs, ProjectJob removal) {
            String project = p.name();
            if (removal != null) {
                // re-added while (or after) being removed: let the removal finish, then start over
                boolean removed = removal.succeeded();
                if (removal.finished() && !removal.deleting()) {
                    delete(removal, DeletionPropagation.BACKGROUND);
                    regs.forEach(j -> delete(j, DeletionPropagation.BACKGROUND));
                }
                projects.add(entry(project, previous, !removed && registered(previous), null));
                return;
            }
            String current = Names.registerJob(cluster.getMetadata().getName(), project, ProjectJobRenderer.specHash(p));
            ProjectJob job = null;
            boolean olderActive = false;
            for (ProjectJob j : regs) {
                if (j.name().equals(current)) {
                    job = j;
                } else if (j.active()) {
                    // an older source's registration must not finish after the current one
                    olderActive = true;
                    delete(j, DeletionPropagation.FOREGROUND);
                }
            }
            Integer retries = previous == null ? null : previous.retries();
            boolean registered = registered(previous);
            final ProjectJob mine = job;
            boolean superseded = mine != null && regs.stream().anyMatch(j -> j != mine && j.newerThan(mine));
            if (job == null) {
                if (!olderActive) create(ProjectJobRenderer.registration(cluster, p, settings));
            } else if (job.succeeded() && superseded) {
                // A -> B -> A: B's Job ran after this one, so the catalog may hold B; run this one again
                delete(job, DeletionPropagation.BACKGROUND);
            } else if (job.succeeded()) {
                registered = true;
                retries = null;
                for (ProjectJob j : regs) if (j != job && j.finished()) delete(j, DeletionPropagation.BACKGROUND);
            } else if (job.failed()) {
                retries = failed(project, job, previous, "registration", REASON_REGISTRATION_FAILED);
            }
            projects.add(entry(project, previous, registered, retries));
        }

        void removed(String project, ProjectStatus previous, List<ProjectJob> regs, ProjectJob removal) {
            boolean registered = registered(previous) || regs.stream().anyMatch(ProjectJob::succeeded);
            Integer retries = previous == null ? null : previous.retries();
            boolean registering = false;
            for (ProjectJob j : regs) {
                if (j.active()) {
                    registering = true;
                    delete(j, DeletionPropagation.FOREGROUND);
                }
            }
            if (removal == null) {
                if (!registering) create(ProjectJobRenderer.removal(cluster, project, settings));
            } else if (removal.succeeded()) {
                boolean left = false;
                for (ProjectJob j : regs) {
                    if (!j.deleting()) {
                        left = true;
                        delete(j, DeletionPropagation.BACKGROUND);
                    }
                }
                // the removal Job goes last: while it exists, the project is still known to be removed
                if (!left) delete(removal, DeletionPropagation.BACKGROUND);
                return; // removed from the catalog: no longer listed
            } else if (removal.failed()) {
                retries = failed(project, removal, previous, "removal", REASON_REMOVAL_FAILED);
            }
            projects.add(entry(project, previous, registered, retries));
        }

        Plan build() {
            String reason = reasons.isEmpty() ? null
                    : reasons.contains(REASON_REGISTRATION_FAILED) ? REASON_REGISTRATION_FAILED : REASON_REMOVAL_FAILED;
            return new Plan(List.copyOf(create), List.copyOf(delete.values()), List.copyOf(projects),
                    List.copyOf(failures), reason, reschedule);
        }
    }

    /**
     * Applies {@code plan}: deletes, then creates (owned by the cluster, create-only: a Job that already
     * exists is left alone). Names only are logged, never a Job's spec.
     */
    static void apply(Plan plan, PharosCluster cluster, Context<PharosCluster> context) {
        String ns = cluster.getMetadata().getNamespace();
        for (Deletion d : plan.delete()) {
            LOG.info("PharosCluster {}/{}: deleting Job {}", ns, cluster.getMetadata().getName(), d.job());
            context.getClient().batch().v1().jobs().inNamespace(ns).withName(d.job())
                    .withPropagationPolicy(d.propagation()).delete();
        }
        for (Job job : plan.create()) {
            job.addOwnerReference(cluster);
            LOG.info("PharosCluster {}/{}: creating Job {}", ns, cluster.getMetadata().getName(),
                    job.getMetadata().getName());
            try {
                context.resourceOperations().create(job);
            } catch (KubernetesClientException e) {
                if (e.getCode() != 409) {
                    throw new IllegalStateException("creating Job " + job.getMetadata().getName() + " failed (HTTP "
                            + e.getCode() + ")", e);
                }
            }
        }
    }
}
