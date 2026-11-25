package com.pharos.operator.status;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.Phase;
import io.fabric8.kubernetes.api.model.Condition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The phase and its conditions for a valid spec whose objects are applied (spec, resolved detail 9).
 * Pure: the reconciler passes what it observes and merges the result into its one status patch
 * (ruling OP-R3). An invalid spec is {@code Pending} with {@code SpecInvalid}, set by the reconciler.
 * <ul>
 *   <li><b>Ready:</b> the storage is ready, at least {@code min} readers are ready ({@code /health}
 *       200), at least {@code min} writers are ready and report heartbeats in {@code /cluster/status},
 *       every project of the spec has a {@code LATEST}, and the router is ready.</li>
 *   <li><b>Degraded</b> (before Ready), any of: the storage is unreachable (the bucket Job failed, or
 *       MinIO was ready and no longer is), fewer than {@code min} readers ready for longer than
 *       {@value #READERS_GRACE_MINUTES} min after they had been ready, a registration or removal Job
 *       failed, a reader reports degraded shards, {@code UpgradeBlocked}.</li>
 *   <li><b>Provisioning:</b> otherwise (objects created, storage or nodes not ready yet).</li>
 * </ul>
 * Conditions: one per phase, plus {@code StorageReady}, {@code ReadersReady}, {@code WritersReady},
 * {@code ProjectsPublished} and {@code UpgradeBlocked}, each with a reason. Messages hold no time or
 * counter that changes by itself, so an unchanged cluster patches nothing.
 */
public final class PhaseEvaluator {

    static final long READERS_GRACE_MINUTES = 5;
    public static final Duration READERS_GRACE = Duration.ofMinutes(READERS_GRACE_MINUTES);

    /** Reasons. */
    public static final String REASON_READY = "Ready";
    public static final String REASON_WAITING = "Waiting";
    public static final String REASON_PROVISIONED = "Provisioned";
    public static final String REASON_NONE = "None";
    public static final String REASON_BELOW_MIN = "BelowMin";
    public static final String REASON_UNREACHABLE = "Unreachable";
    public static final String REASON_BUCKET_JOB_FAILED = "BucketJobFailed";
    public static final String REASON_STORAGE_UNREACHABLE = "StorageUnreachable";
    public static final String REASON_READERS_BELOW_MIN = "ReadersBelowMin";
    public static final String REASON_READER_DEGRADED_SHARDS = "ReaderDegradedShards";
    public static final String REASON_UPGRADE_BLOCKED = "UpgradeBlocked";
    public static final String REASON_NO_HEARTBEATS = "NoHeartbeats";
    public static final String REASON_NO_STATUS = "NoClusterStatus";
    public static final String REASON_PUBLISHED = "Published";
    public static final String REASON_UNPUBLISHED = "Unpublished";
    public static final String REASON_NO_PROJECTS = "NoProjects";

    private PhaseEvaluator() {}

    /**
     * What the reconciler observed.
     * <ul>
     *   <li>{@code storageReady}, {@code storageReason}, {@code storageMessage}: the storage as the
     *       reconciler judges it (minio: MinIO ready and the bucket created; s3: not probed);
     *       {@code bucketJobFailed}: the minio bucket Job has failed;</li>
     *   <li>pod counts from the workloads' status (readiness is {@code /health} 200);</li>
     *   <li>{@code status}: the {@code /cluster/status} read, or null when no node answers;</li>
     *   <li>{@code projects}: the spec's project names with their published generation (null: none);</li>
     *   <li>{@code projectFailures}, {@code projectFailureReason}: failed registration or removal Jobs;</li>
     *   <li>{@code upgradeBlocked}, {@code upgradeReason}, {@code upgradeMessage}: the upgrade's
     *       {@code UpgradeBlocked} condition;</li>
     *   <li>{@code previous}: the conditions of the last status (transition times, earlier readiness).</li>
     * </ul>
     */
    public record Input(boolean storageReady, String storageReason, String storageMessage, boolean bucketJobFailed,
                        boolean minio, int writersMin, int writersReady, int readersMin, int readersReady,
                        int routerReady, ClusterStatus status, Map<String, Long> projects,
                        List<String> projectFailures, String projectFailureReason, boolean upgradeBlocked,
                        String upgradeReason, String upgradeMessage, List<Condition> previous, Instant now) {}

    /** One condition to set. */
    public record Cond(String type, boolean status, String reason, String message) {}

    /** The phase, the conditions in a stable order, and when to look again (the readers' grace), or null. */
    public record Result(Phase phase, List<Cond> conditions, Duration recheck) {}

    public static Result evaluate(Input in) {
        List<String> waiting = new ArrayList<>();
        List<String> degraded = new ArrayList<>();
        List<String> degradedReasons = new ArrayList<>();
        Duration recheck = null;

        // storage
        boolean storageLost = !in.storageReady() && !in.bucketJobFailed()
                && (wasTrue(in.previous(), PharosClusterStatus.CONDITION_STORAGE_READY)
                    || hadReason(in.previous(), PharosClusterStatus.CONDITION_STORAGE_READY, REASON_UNREACHABLE));
        String storageReason = storageLost ? REASON_UNREACHABLE : in.storageReason();
        if (!in.storageReady()) {
            waiting.add(in.minio() ? (REASON_BUCKET_JOB_FAILED.equals(in.storageReason())
                    || "BucketPending".equals(in.storageReason()) ? "the bucket" : "MinIO") : "the storage");
        }
        if (in.bucketJobFailed()) {
            degradedReasons.add(REASON_BUCKET_JOB_FAILED);
            degraded.add(in.storageMessage());
        } else if (storageLost) {
            degradedReasons.add(REASON_STORAGE_UNREACHABLE);
            degraded.add("The storage was ready and is unreachable: " + in.storageMessage());
        }

        // writers: pods ready, then heartbeats
        boolean writerPods = in.writersReady() >= in.writersMin();
        if (!writerPods) waiting.add("writers (" + in.writersReady() + "/" + in.writersMin() + " ready)");
        // readers
        boolean readersReady = in.readersReady() >= in.readersMin();
        if (!readersReady) waiting.add("readers (" + in.readersReady() + "/" + in.readersMin() + " ready)");
        String readersReason;
        String readersMessage = in.readersReady() + "/" + in.readersMin() + " readers ready (/health 200).";
        if (readersReady) {
            readersReason = REASON_READY;
        } else if (wasTrue(in.previous(), PharosClusterStatus.CONDITION_READERS_READY)
                || hadReason(in.previous(), PharosClusterStatus.CONDITION_READERS_READY, REASON_BELOW_MIN)) {
            readersReason = REASON_BELOW_MIN;
            Instant since = falseSince(in.previous(), PharosClusterStatus.CONDITION_READERS_READY, in.now());
            Instant due = since.plus(READERS_GRACE);
            if (!in.now().isBefore(due)) {
                degradedReasons.add(REASON_READERS_BELOW_MIN);
                degraded.add("Fewer than min readers ready for more than " + READERS_GRACE_MINUTES + " min: "
                        + readersMessage);
            } else {
                recheck = Duration.between(in.now(), due);
            }
        } else {
            readersReason = REASON_WAITING;
        }

        if (in.routerReady() < 1) waiting.add("the router");

        int heartbeats = in.status() == null || in.status().layout() == null || in.status().layout().writers() == null
                ? 0 : (int) in.status().layout().writers().stream().filter(w -> w != null).count();
        boolean writersReady;
        String writersReason;
        String writersMessage;
        if (!writerPods) {
            writersReady = false;
            writersReason = REASON_WAITING;
            writersMessage = in.writersReady() + "/" + in.writersMin() + " writers ready.";
        } else if (in.status() == null) {
            writersReady = false;
            writersReason = REASON_NO_STATUS;
            writersMessage = "No pharos node answers /cluster/status, so no writer heartbeat is known.";
            waiting.add("writer heartbeats (no /cluster/status answer)");
        } else if (heartbeats < in.writersMin()) {
            writersReady = false;
            writersReason = REASON_NO_HEARTBEATS;
            writersMessage = heartbeats + "/" + in.writersMin() + " writers report heartbeats.";
            waiting.add("writer heartbeats (" + heartbeats + "/" + in.writersMin() + ")");
        } else {
            writersReady = true;
            writersReason = REASON_READY;
            writersMessage = in.writersReady() + " writers ready, " + heartbeats + " reporting heartbeats.";
        }

        // projects
        List<String> unpublished = new ArrayList<>();
        for (Map.Entry<String, Long> p : in.projects().entrySet()) if (p.getValue() == null) unpublished.add(p.getKey());
        boolean published = unpublished.isEmpty();
        if (!published) waiting.add("projects to publish (" + String.join(", ", unpublished) + ")");
        String projectsReason = in.projects().isEmpty() ? REASON_NO_PROJECTS : published ? REASON_PUBLISHED
                : REASON_UNPUBLISHED;
        String projectsMessage = in.projects().isEmpty() ? "The spec lists no project."
                : published ? "Every project has a published generation."
                : "Not published yet (no LATEST): " + String.join(", ", unpublished) + ".";

        if (!in.projectFailures().isEmpty()) {
            degradedReasons.add(in.projectFailureReason());
            degraded.addAll(in.projectFailures());
        }
        if (in.status() != null && in.status().layout() != null && in.status().layout().readers() != null) {
            for (ClusterStatus.Reader r : in.status().layout().readers()) {
                if (r != null && r.degradedShards() != null && !r.degradedShards().isEmpty()) {
                    if (!degradedReasons.contains(REASON_READER_DEGRADED_SHARDS)) {
                        degradedReasons.add(REASON_READER_DEGRADED_SHARDS);
                    }
                    degraded.add("Reader " + r.node() + " reports degraded shards: "
                            + String.join(", ", r.degradedShards()));
                }
            }
        }
        if (in.upgradeBlocked()) {
            degradedReasons.addFirst(REASON_UPGRADE_BLOCKED);
            degraded.addFirst(in.upgradeMessage());
        }

        boolean ready = waiting.isEmpty();
        boolean isDegraded = !degradedReasons.isEmpty();
        String waitingFor = "Waiting for " + String.join(", ", waiting) + ".";
        String degradedMessage = String.join("; ", degraded);
        Phase phase = isDegraded ? Phase.Degraded : ready ? Phase.Ready : Phase.Provisioning;
        List<Cond> c = new ArrayList<>();
        c.add(new Cond(Phase.Pending.name(), false, REASON_PROVISIONED, "The objects are created."));
        c.add(new Cond(Phase.Provisioning.name(), !ready, ready ? REASON_READY : REASON_WAITING, ready ? "" : waitingFor));
        c.add(new Cond(Phase.Ready.name(), ready && !isDegraded,
                isDegraded ? Phase.Degraded.name() : ready ? REASON_READY : REASON_WAITING,
                isDegraded ? degradedMessage : ready
                        ? "Storage, readers, writers (with heartbeats), projects and the router are ready." : waitingFor));
        c.add(new Cond(Phase.Degraded.name(), isDegraded, isDegraded ? degradedReasons.getFirst() : REASON_NONE,
                degradedMessage));
        c.add(new Cond(PharosClusterStatus.CONDITION_STORAGE_READY, in.storageReady(), storageReason,
                in.storageMessage()));
        c.add(new Cond(PharosClusterStatus.CONDITION_READERS_READY, readersReady, readersReason, readersMessage));
        c.add(new Cond(PharosClusterStatus.CONDITION_WRITERS_READY, writersReady, writersReason, writersMessage));
        c.add(new Cond(PharosClusterStatus.CONDITION_PROJECTS_PUBLISHED, published, projectsReason, projectsMessage));
        c.add(new Cond(PharosClusterStatus.CONDITION_UPGRADE_BLOCKED, in.upgradeBlocked(), in.upgradeReason(),
                in.upgradeMessage()));
        return new Result(phase, List.copyOf(c), recheck);
    }

    private static Condition previous(List<Condition> previous, String type) {
        if (previous == null) return null;
        return previous.stream().filter(c -> c != null && type.equals(c.getType())).findFirst().orElse(null);
    }

    private static boolean wasTrue(List<Condition> previous, String type) {
        Condition c = previous(previous, type);
        return c != null && "True".equals(c.getStatus());
    }

    private static boolean hadReason(List<Condition> previous, String type, String reason) {
        Condition c = previous(previous, type);
        return c != null && "False".equals(c.getStatus()) && reason.equals(c.getReason());
    }

    /** Since when the condition is False: its last transition when it was False already, else now. */
    private static Instant falseSince(List<Condition> previous, String type, Instant now) {
        Condition c = previous(previous, type);
        if (c == null || !"False".equals(c.getStatus()) || c.getLastTransitionTime() == null) return now;
        try {
            return Instant.parse(c.getLastTransitionTime());
        } catch (java.time.format.DateTimeParseException e) {
            return now;
        }
    }
}
