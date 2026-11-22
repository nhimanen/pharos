package com.pharos.operator.upgrade;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.operator.Names;
import com.pharos.operator.api.PharosClusterStatus.UpgradeStatus;
import com.pharos.operator.render.DesiredState;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Plans a change of {@code spec.version} (spec, "Upgrades"; resolved detail 8). Pure: the reconciler
 * passes what it observes and applies the {@link Plan}.
 *
 * <p>An upgrade is in progress while any existing workload runs another version than the spec's. It
 * goes in steps, and each step's workloads move only once the previous step is done:
 * <ol>
 *   <li><b>Preflight:</b> the Job {@code <name>-preflight-<version>} runs the new image with
 *       {@code pharos upgrade-indexes --check} against the bucket (once the storage is ready). Until it
 *       succeeds, every workload keeps the version it runs: nothing is applied. When it fails, the
 *       upgrade is blocked ({@code UpgradeBlocked}): {@code PreflightRefused} when a shard would be
 *       refused (exit 3; older than N-1, or newer), {@code PreflightFailed} otherwise. The Job is
 *       re-created after a backoff (60 s doubling to 15 min, {@code status.upgrade.preflightRetries}),
 *       since the indexes may have been re-indexed meanwhile; the upgrade stays blocked until a run
 *       succeeds.</li>
 *   <li><b>Readers</b> move to the new version first (they read format N and N-1, so they accept the
 *       new format before any writer writes it: UPGRADING.md's N-1 rule);</li>
 *   <li>then <b>writers</b>, once every reader pod runs the new version: Kubernetes reports the
 *       StatefulSet rolled out and ready, and {@code /cluster/status} lists a live heartbeat for every
 *       reader pod, started after the new version was applied (heartbeats carry no version; the pod
 *       name and its start time do), in state {@code ready} or {@code degraded};</li>
 *   <li>then the <b>router</b>, once the writers are rolled the same way (a live, not stopping
 *       heartbeat per writer pod).</li>
 * </ol>
 * Each workload carries {@code pharos.dev/version-since} ({@link VersionSince}): the version and the
 * instant it was applied, set in the same apply as the image, so the gate survives an operator restart
 * or a failed status patch. A version changed again mid-upgrade starts over with its own pre-flight
 * Job (pre-flight Jobs of other versions are deleted); once every workload runs the spec's version,
 * every pre-flight Job is deleted.
 */
public final class UpgradeCoordinator {

    /** The steps of an upgrade ({@code status.upgrade.step}). */
    public enum Step { Preflight, Readers, Writers, Router }

    /** {@code UpgradeBlocked} reasons. */
    public static final String REASON_UP_TO_DATE = "UpToDate";
    public static final String REASON_WAITING_FOR_STORAGE = "WaitingForStorage";
    public static final String REASON_PREFLIGHT_RUNNING = "PreflightRunning";
    public static final String REASON_PREFLIGHT_RETRYING = "PreflightRetrying";
    public static final String REASON_PREFLIGHT_REFUSED = "PreflightRefused";
    public static final String REASON_PREFLIGHT_FAILED = "PreflightFailed";
    public static final String REASON_UPGRADING = "Upgrading";

    /** Backoff of a failed pre-flight Job: 60 s, doubling per retry, at most 15 min. */
    static final long RETRY_FIRST_SECONDS = 60;
    static final long RETRY_MAX_SECONDS = 900;
    /** A heartbeat's start may precede the version's apply by this much (node and operator clocks differ). */
    static final Duration CLOCK_SKEW = Duration.ofSeconds(60);

    private UpgradeCoordinator() {}

    /**
     * A workload as the informer shows it. {@code version}: the tag of its pharos image, null when the
     * workload does not exist, empty when its image has no readable tag (it then counts as running the
     * version being replaced, or the spec's when no other is); {@code rolledOut}: Kubernetes reports every replica updated to the current
     * template and ready; {@code replicas}: its desired replicas; {@code since}: its
     * {@code pharos.dev/version-since} annotation, or null.
     */
    public record Workload(String version, boolean rolledOut, int replicas, String since) {

        public static final Workload ABSENT = new Workload(null, false, 0, null);

        boolean exists() { return version != null; }
    }

    /** What the pre-flight Job of the spec's version shows. */
    public enum PreflightState { ABSENT, RUNNING, SUCCEEDED, REFUSED, FAILED, DELETING }

    /** The pre-flight Job of the spec's version: its state, and when and why it failed (null otherwise). */
    public record Preflight(PreflightState state, Instant failedAt, String failure) {

        public static final Preflight ABSENT = new Preflight(PreflightState.ABSENT, null, null);
    }

    /**
     * Everything observed. {@code otherPreflightJobs}: the names of this cluster's pre-flight Jobs of
     * other versions that are not being deleted yet; {@code status}: the {@code /cluster/status} read,
     * or null; {@code previous}: {@code status.upgrade} of the last reconcile, or null.
     */
    public record Input(String cluster, String target, Workload readers, Workload writers, Workload router,
                        Preflight preflight, List<String> otherPreflightJobs, boolean storageReady,
                        ClusterStatus status, UpgradeStatus previous, Instant now) {}

    /**
     * What to apply. {@code versions}: each workload's version and annotation; {@code createPreflight}:
     * create the spec's version's pre-flight Job; {@code deleteJobs}: pre-flight Jobs to delete;
     * {@code upgrade}: the new {@code status.upgrade} (null: none in progress); {@code blocked},
     * {@code reason} and {@code message}: the {@code UpgradeBlocked} condition; {@code recheck}: when to
     * look again (a retry's backoff), or null.
     */
    public record Plan(DesiredState.Versions versions, boolean createPreflight, List<String> deleteJobs,
                       UpgradeStatus upgrade, boolean blocked, String reason, String message, Duration recheck) {}

    public static Plan plan(Input in) {
        String target = in.target();
        List<String> delete = new ArrayList<>();
        List<Workload> present = new ArrayList<>();
        for (Workload w : List.of(in.writers(), in.readers(), in.router())) if (w.exists()) present.add(w);
        String from = present.stream().map(Workload::version).filter(v -> !v.isEmpty() && !v.equals(target))
                .findFirst().orElse(null);
        if (from == null) { // every existing workload runs the spec's version (or nothing exists yet)
            delete.addAll(in.otherPreflightJobs());
            if (in.preflight().state() != PreflightState.ABSENT && in.preflight().state() != PreflightState.DELETING) {
                delete.add(Names.preflightJob(in.cluster(), target));
            }
            return new Plan(new DesiredState.Versions(target, target, target, in.writers().since(),
                    in.readers().since(), in.router().since()), false, List.copyOf(delete), null, false,
                    REASON_UP_TO_DATE, "Every workload runs " + target + ".", null);
        }
        delete.addAll(in.otherPreflightJobs()); // a version changed again mid-upgrade: only the spec's counts
        String job = Names.preflightJob(in.cluster(), target);
        Integer retries = in.previous() != null && target.equals(in.previous().to())
                ? in.previous().preflightRetries() : null;
        String upgrade = "Upgrade " + from + " -> " + target + ": ";

        // the pre-flight check
        Preflight pf = in.preflight();
        boolean create = false;
        boolean blocked = false;
        String reason;
        String message;
        Duration recheck = null;
        switch (pf.state()) {
            case SUCCEEDED -> {
                retries = null;
                reason = REASON_UPGRADING;
                message = null; // set by the roll below
            }
            case ABSENT -> {
                if (!in.storageReady()) {
                    reason = REASON_WAITING_FOR_STORAGE;
                    message = upgrade + "waiting for the storage before the pre-flight check; nothing is rolled yet.";
                } else {
                    create = true;
                    reason = retries == null ? REASON_PREFLIGHT_RUNNING : REASON_PREFLIGHT_RETRYING;
                    message = preflightRunning(upgrade, job, target, retries);
                }
                blocked = retries != null;
            }
            case RUNNING, DELETING -> {
                reason = retries == null ? REASON_PREFLIGHT_RUNNING : REASON_PREFLIGHT_RETRYING;
                message = preflightRunning(upgrade, job, target, retries);
                blocked = retries != null;
            }
            case REFUSED, FAILED -> {
                blocked = true;
                long backoff = backoffSeconds(retries);
                Instant due = (pf.failedAt() == null ? in.now() : pf.failedAt()).plusSeconds(backoff);
                boolean refused = pf.state() == PreflightState.REFUSED;
                reason = refused ? REASON_PREFLIGHT_REFUSED : REASON_PREFLIGHT_FAILED;
                message = upgrade + (refused
                        ? "the pre-flight Job " + job + " found shards that pharos " + target + " would refuse (index"
                          + " format older than N-1, or newer; see its log). No workload is changed. Re-index them on "
                          + from + " (UPGRADING.md), or set spec.version back to " + from + "."
                        : "the pre-flight Job " + job + " failed" + (pf.failure() == null ? "" : " (" + pf.failure() + ")")
                          + " before it could check the bucket's indexes (see its log: the bucket unreachable, or an"
                          + " image without 'pharos upgrade-indexes --check'). No workload is changed.")
                        + " The check runs again after " + backoff + " s"
                        + (retries == null ? "" : " (retries so far: " + retries + ")") + ".";
                if (in.now().isBefore(due)) {
                    recheck = Duration.between(in.now(), due);
                } else {
                    delete.add(job); // re-created on the next reconcile
                    retries = retries == null ? 1 : retries + 1;
                }
            }
            default -> throw new IllegalStateException(pf.state().name());
        }

        Workload readers = in.readers();
        Workload writers = in.writers();
        Workload router = in.router();
        if (pf.state() != PreflightState.SUCCEEDED) {
            // nothing moves: every workload keeps the version it runs (one that does not exist yet: from's)
            return new Plan(new DesiredState.Versions(keep(writers, from), keep(readers, from), keep(router, from),
                    writers.since(), readers.since(), router.since()), create, List.copyOf(delete),
                    new UpgradeStatus(from, target, Step.Preflight.name(), retries), blocked, reason, message, recheck);
        }

        // the roll: readers, then writers, then the router
        List<String> waiting = new ArrayList<>();
        String readersV = target;
        String writersV = keep(writers, from);
        String routerV = keep(router, from);
        Step step = Step.Readers;
        if (rolled(in, readers, Names.readers(in.cluster()), true, waiting)) {
            writersV = target;
            step = Step.Writers;
            if (rolled(in, writers, Names.writers(in.cluster()), false, waiting)) {
                routerV = target;
                step = Step.Router;
            }
        }
        message = upgrade + "the pre-flight check passed; rolling " + switch (step) {
            case Readers -> "the readers";
            case Writers -> "the writers (the readers run " + target + ")";
            default -> "the router (readers and writers run " + target + ")";
        } + (waiting.isEmpty() ? "." : "; waiting for " + String.join(", ", waiting) + ".");
        return new Plan(new DesiredState.Versions(writersV, readersV, routerV,
                since(writers, writersV, in.now()), since(readers, readersV, in.now()), since(router, routerV, in.now())),
                false, List.copyOf(delete), new UpgradeStatus(from, target, step.name(), null), false, REASON_UPGRADING,
                message, null);
    }

    private static String preflightRunning(String upgrade, String job, String target, Integer retries) {
        return upgrade + "the pre-flight Job " + job + " checks the bucket's indexes with " + target
                + (retries == null ? "" : " again (retries so far: " + retries + ")")
                + "; nothing is rolled until it passes.";
    }

    /**
     * The version a workload keeps: the one it runs, or {@code from} for one that does not exist yet or
     * whose version is unreadable.
     */
    private static String keep(Workload w, String from) {
        return w.exists() && !w.version().isEmpty() ? w.version() : from;
    }

    /** The annotation to render: carried while the version stays, a new one when it changes now. */
    private static String since(Workload w, String version, Instant now) {
        if (Objects.equals(w.version(), version)) return w.since();
        return w.exists() ? new VersionSince(version, now.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)).value()
                : null;
    }

    /**
     * Whether a step's workload runs {@code in.target()} everywhere: Kubernetes reports it rolled out and,
     * for readers and writers, {@code /cluster/status} lists a live heartbeat for each of its pods
     * {@code <sts>-<i>}, started after the version was applied. What is missing goes to {@code waiting}.
     */
    static boolean rolled(Input in, Workload w, String sts, boolean readers, List<String> waiting) {
        String role = readers ? "readers" : "writers";
        if (!w.exists() || !in.target().equals(w.version())) {
            waiting.add("the " + role + " to take " + in.target());
            return false;
        }
        if (!w.rolledOut()) {
            waiting.add("the " + role + "' rollout (every pod updated and ready)");
            return false;
        }
        ClusterStatus status = in.status();
        if (status == null || status.layout() == null) {
            waiting.add("a /cluster/status answer listing the new " + role);
            return false;
        }
        VersionSince vs = VersionSince.parse(w.since());
        long after = vs != null && in.target().equals(vs.version())
                ? vs.since().minus(CLOCK_SKEW).toEpochMilli() : Long.MIN_VALUE;
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < w.replicas(); i++) {
            String pod = sts + "-" + i;
            boolean ok;
            if (readers) {
                ok = status.layout().readers() != null && status.layout().readers().stream().anyMatch(r -> r != null
                        && pod.equals(r.node()) && r.startedAt() >= after
                        && ("ready".equals(r.state()) || "degraded".equals(r.state())));
            } else {
                ok = status.layout().writers() != null && status.layout().writers().stream().anyMatch(x -> x != null
                        && pod.equals(x.node()) && x.startedAt() >= after && !x.stopping());
            }
            if (!ok) missing.add(pod);
        }
        if (missing.isEmpty()) return true;
        waiting.add("heartbeats of " + String.join(", ", missing) + " on " + in.target());
        return false;
    }

    static long backoffSeconds(Integer retries) {
        int n = retries == null ? 0 : Math.min(retries, 10);
        return Math.min(RETRY_MAX_SECONDS, RETRY_FIRST_SECONDS << n);
    }

    /**
     * The {@code pharos.dev/version-since} annotation: {@code <version>@<instant>} (a tag has no
     * {@code @}).
     */
    public record VersionSince(String version, Instant since) {

        public String value() {
            return version + "@" + since;
        }

        /** The annotation read back, or null when absent or unreadable. */
        public static VersionSince parse(String value) {
            if (value == null) return null;
            int at = value.lastIndexOf('@');
            if (at <= 0) return null;
            try {
                return new VersionSince(value.substring(0, at), Instant.parse(value.substring(at + 1)));
            } catch (DateTimeParseException e) {
                return null;
            }
        }
    }
}
