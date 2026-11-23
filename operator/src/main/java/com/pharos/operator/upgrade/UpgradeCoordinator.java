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
    public enum Step { Preflight, Readers, Writers, PostCheck, Router }

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
                        Preflight preflight, Preflight postCheck, List<String> otherPreflightJobs,
                        boolean storageReady, ClusterStatus status, UpgradeStatus previous, Instant now) {

        /** Without a post-check Job (none exists). */
        public Input(String cluster, String target, Workload readers, Workload writers, Workload router,
                     Preflight preflight, List<String> otherPreflightJobs, boolean storageReady, ClusterStatus status,
                     UpgradeStatus previous, Instant now) {
            this(cluster, target, readers, writers, router, preflight, Preflight.ABSENT, otherPreflightJobs,
                    storageReady, status, previous, now);
        }
    }

    /**
     * What to apply. {@code versions}: each workload's version and annotation; {@code createPreflight}:
     * create the spec's version's pre-flight Job; {@code createPostCheck}: create its post-check Job (a
     * downgrade, once the writers run the target); {@code deleteJobs}: pre-flight Jobs to delete;
     * {@code upgrade}: the new {@code status.upgrade} (null: none in progress); {@code blocked},
     * {@code reason} and {@code message}: the {@code UpgradeBlocked} condition; {@code recheck}: when to
     * look again (a retry's backoff), or null.
     */
    public record Plan(DesiredState.Versions versions, boolean createPreflight, List<String> deleteJobs,
                       UpgradeStatus upgrade, boolean blocked, String reason, String message, Duration recheck,
                       boolean createPostCheck) {}

    /** A check Job's verdict for this reconcile. */
    private record Check(boolean passed, boolean create, boolean blocked, String reason, String message,
                         Duration recheck, Integer retries) {}

    public static Plan plan(Input in) {
        String target = in.target();
        List<String> delete = new ArrayList<>();
        List<Workload> present = new ArrayList<>();
        for (Workload w : List.of(in.writers(), in.readers(), in.router())) if (w.exists()) present.add(w);
        String from = present.stream().map(Workload::version).filter(v -> !v.isEmpty() && !v.equals(target))
                .findFirst().orElse(null);
        String job = Names.preflightJob(in.cluster(), target);
        String postJob = Names.preflightPostJob(in.cluster(), target);
        delete.addAll(in.otherPreflightJobs()); // a version changed again mid-upgrade: only the spec's counts
        if (from == null) { // every existing workload runs the spec's version (or nothing exists yet)
            if (live(in.preflight())) delete.add(job);
            if (live(in.postCheck())) delete.add(postJob);
            return new Plan(new DesiredState.Versions(target, target, target, in.writers().since(),
                    in.readers().since(), in.router().since()), false, List.copyOf(delete), null, false,
                    REASON_UP_TO_DATE, "Every workload runs " + target + ".", null, false);
        }
        Integer retries = in.previous() != null && target.equals(in.previous().to())
                ? in.previous().preflightRetries() : null;
        boolean writersFirst = writersFirst(from, target, in.previous());
        String upgrade = (writersFirst ? "Downgrade " : "Upgrade ") + from + " -> " + target + ": ";
        Workload readers = in.readers();
        Workload writers = in.writers();
        Workload router = in.router();

        Check pre = check(in, in.preflight(), job, false, upgrade, from, retries, delete);
        if (!pre.passed()) {
            // nothing moves: every workload keeps the version it runs (one that does not exist yet: from's)
            return new Plan(new DesiredState.Versions(keep(writers, from), keep(readers, from), keep(router, from),
                    writers.since(), readers.since(), router.since()), pre.create(), List.copyOf(delete),
                    new UpgradeStatus(from, target, Step.Preflight.name(), pre.retries(), writersFirst), pre.blocked(),
                    pre.reason(), pre.message(), pre.recheck(), false);
        }

        List<String> waiting = new ArrayList<>();
        String readersV = keep(readers, from);
        String writersV = keep(writers, from);
        String routerV = keep(router, from);
        Step step;
        Check post = null;
        if (!writersFirst) {
            // an upgrade: readers, then writers, then the router (readers read N and N-1 before writers write N)
            readersV = target;
            step = Step.Readers;
            if (rolled(in, readers, Names.readers(in.cluster()), true, waiting)) {
                writersV = target;
                step = Step.Writers;
                if (rolled(in, writers, Names.writers(in.cluster()), false, waiting)) {
                    routerV = target;
                    step = Step.Router;
                }
            }
        } else {
            // a downgrade: writers first, so no writer of the newer version is left to publish a format the
            // target's readers refuse; then the bucket is checked again before the readers move
            writersV = target;
            step = Step.Writers;
            if (rolled(in, writers, Names.writers(in.cluster()), false, waiting)) {
                step = Step.PostCheck;
                Integer postRetries = in.previous() != null && target.equals(in.previous().to())
                        && Step.PostCheck.name().equals(in.previous().step()) ? in.previous().preflightRetries() : null;
                post = check(in, in.postCheck(), postJob, true, upgrade, from, postRetries, delete);
                if (post.passed()) {
                    readersV = target;
                    step = Step.Readers;
                    if (rolled(in, readers, Names.readers(in.cluster()), true, waiting)) {
                        routerV = target;
                        step = Step.Router;
                    }
                }
            }
        }
        DesiredState.Versions versions = new DesiredState.Versions(writersV, readersV, routerV,
                since(writers, writersV, in.now()), since(readers, readersV, in.now()), since(router, routerV, in.now()));
        if (step == Step.PostCheck) {
            return new Plan(versions, false, List.copyOf(delete),
                    new UpgradeStatus(from, target, step.name(), post.retries(), true), post.blocked(), post.reason(),
                    post.message(), post.recheck(), post.create());
        }
        String message = upgrade + (writersFirst ? "the checks passed; rolling " : "the pre-flight check passed; rolling ")
                + switch (step) {
                    case Readers -> writersFirst ? "the readers (the writers run " + target + ")" : "the readers";
                    case Writers -> writersFirst ? "the writers first (a downgrade)"
                            : "the writers (the readers run " + target + ")";
                    default -> "the router (readers and writers run " + target + ")";
                } + (waiting.isEmpty() ? "." : "; waiting for " + String.join(", ", waiting) + ".");
        return new Plan(versions, false, List.copyOf(delete),
                new UpgradeStatus(from, target, step.name(), null, writersFirst), false, REASON_UPGRADING, message,
                null, false);
    }

    private static boolean live(Preflight p) {
        return p.state() != PreflightState.ABSENT && p.state() != PreflightState.DELETING;
    }

    /**
     * Whether this version change rolls writers first (ruling OP-R6): a downgrade, where writers of the
     * version being left may still publish an index format the target's readers refuse. Kept from
     * {@code status.upgrade} for the same target; otherwise release tags are compared ({@code target}
     * older than {@code from}); tags that are no releases count as a downgrade when the upgrade in
     * progress was heading to {@code from} (a mid-roll revert).
     */
    static boolean writersFirst(String from, String target, UpgradeStatus previous) {
        if (previous != null && target.equals(previous.to()) && previous.writersFirst() != null) {
            return previous.writersFirst();
        }
        int c = compareReleases(target, from);
        if (c != Integer.MIN_VALUE) return c < 0;
        return previous != null && from.equals(previous.to());
    }

    private static final java.util.regex.Pattern RELEASE = java.util.regex.Pattern.compile("v?(\\d{1,9})\\.(\\d{1,9})\\.(\\d{1,9})(.*)");

    /** Sign of {@code a - b} by major.minor.patch, or {@code Integer.MIN_VALUE} when either is no release tag. */
    static int compareReleases(String a, String b) {
        var ma = RELEASE.matcher(a);
        var mb = RELEASE.matcher(b);
        if (!ma.matches() || !mb.matches()) return Integer.MIN_VALUE;
        for (int i = 1; i <= 3; i++) {
            int c = Long.compare(Long.parseLong(ma.group(i)), Long.parseLong(mb.group(i)));
            if (c != 0) return c;
        }
        return 0;
    }

    /**
     * The verdict of a check Job: the pre-flight ({@code post} false) or, in a downgrade, the post-check
     * after the writers moved. A failed one blocks, and is deleted (re-created next time) after its
     * backoff, counted in {@code retries}.
     */
    private static Check check(Input in, Preflight pf, String job, boolean post, String upgrade, String from,
                               Integer retries, List<String> delete) {
        String target = in.target();
        String stays = post ? " The readers stay on " + from + "." : " No workload is changed.";
        switch (pf.state()) {
            case SUCCEEDED -> {
                return new Check(true, false, false, REASON_UPGRADING, null, null, null);
            }
            case ABSENT, RUNNING, DELETING -> {
                boolean create = pf.state() == PreflightState.ABSENT && in.storageReady();
                if (pf.state() == PreflightState.ABSENT && !in.storageReady()) {
                    return new Check(false, false, retries != null, REASON_WAITING_FOR_STORAGE, upgrade
                            + "waiting for the storage before the " + (post ? "post-check" : "pre-flight check") + ";"
                            + stays, null, retries);
                }
                String message = upgrade + (post
                        ? "the writers run " + target + "; the post-check Job " + job + " checks the bucket's indexes"
                          + " with " + target + (retries == null ? "" : " again (retries so far: " + retries + ")")
                          + " before the readers move."
                        : preflightRunning(job, target, retries));
                return new Check(false, create, retries != null,
                        retries == null ? REASON_PREFLIGHT_RUNNING : REASON_PREFLIGHT_RETRYING, message, null, retries);
            }
            case REFUSED, FAILED -> {
                long backoff = backoffSeconds(retries);
                Instant due = (pf.failedAt() == null ? in.now() : pf.failedAt()).plusSeconds(backoff);
                boolean refused = pf.state() == PreflightState.REFUSED;
                String what = post ? "post-check" : "pre-flight";
                String message = upgrade + (refused
                        ? (post
                          ? "the post-check Job " + job + " found that the bucket holds index formats pharos " + target
                            + " cannot read (published by the " + from + " writers before they stopped; see its"
                            + " log)." + stays + " Roll forward (set spec.version back to " + from + "), or re-index"
                            + " those shards on " + target + "."
                          : "the pre-flight Job " + job + " found shards that pharos " + target + " would refuse (index"
                            + " format older than N-1, or newer; see its log)." + stays + " Re-index them on " + from
                            + " (UPGRADING.md), or set spec.version back to " + from + ".")
                        : "the " + what + " Job " + job + " failed" + (pf.failure() == null ? "" : " (" + pf.failure() + ")")
                          + " before it could check the bucket's indexes (see its log: the bucket unreachable, or an"
                          + " image without 'pharos upgrade-indexes --check')." + stays)
                        + " The check runs again after " + backoff + " s"
                        + (retries == null ? "" : " (retries so far: " + retries + ")") + ".";
                Duration recheck = null;
                Integer next = retries;
                if (in.now().isBefore(due)) {
                    recheck = Duration.between(in.now(), due);
                } else {
                    delete.add(job); // re-created on the next reconcile
                    next = retries == null ? 1 : retries + 1;
                }
                return new Check(false, false, true, refused ? REASON_PREFLIGHT_REFUSED : REASON_PREFLIGHT_FAILED,
                        message, recheck, next);
            }
            default -> throw new IllegalStateException(pf.state().name());
        }
    }

    private static String preflightRunning(String job, String target, Integer retries) {
        return "the pre-flight Job " + job + " checks the bucket's indexes with " + target
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
