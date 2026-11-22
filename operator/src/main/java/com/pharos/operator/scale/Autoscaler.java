package com.pharos.operator.scale;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.status.ClusterStatus;
import com.pharos.cluster.status.ClusterStatus.Proposal;
import com.pharos.operator.Defaults;
import com.pharos.operator.Names;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.ProposalStatus;
import com.pharos.operator.api.PharosClusterStatus.ReshardProposal;
import com.pharos.operator.status.StatusReading;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Applies pharos's scaling proposals within the user's bounds. Pharos decides when a role needs more
 * or fewer nodes (its proposals, raised under the triggers the operator rendered); the operator only
 * steps the StatefulSet {@code replicas}:
 * <ul>
 *   <li>the step is the proposal's {@code delta} (1 without one, from an older pharos), capped at
 *       {@code maxStepUp} / {@code maxStepDown}, then clamped to {@code [min, max]};</li>
 *   <li>at most one change per role per {@code cooldownSeconds}, and an in-force proposal is applied
 *       again once per cooldown, so the role ramps towards {@code max};</li>
 *   <li>the floor ({@code min}, raised by a schedule entry) is enforced at once, even when no pharos
 *       node answers, and is not a proposal application (it starts no cooldown);</li>
 *   <li>each application is also returned as the role's {@link LastScale}, which the reconciler
 *       writes onto the StatefulSet in the same apply as {@code replicas} (ruling OP-R5);</li>
 *   <li>writers go down one at a time, each step only once the previously removed writers have
 *       drained ({@link ScaleDownGate}); readers above {@code max} are cut to it at once.</li>
 * </ul>
 * Everything it remembers lives in {@code status.proposals} ({@link AppliedHistory}); the result is
 * merged into the reconciler's single status patch (ruling OP-R3). Pure: no I/O, the clock is a
 * parameter.
 */
public final class Autoscaler {

    public static final String RESHARD = "reshard";
    public static final String WRITER_DISK = "writer-disk";

    /** Event reasons. */
    public static final String EVENT_SCALED = "Scaled";
    public static final String EVENT_RESHARD = "ReshardProposed";
    public static final String EVENT_WRITER_DISK = "WriterLowDisk";

    private static final ObjectMapper JSON = new ObjectMapper();

    private Autoscaler() {}

    /**
     * A role's new replica count and why. {@code proposalId} is set only when a proposal was applied
     * (the count changed because of it).
     */
    public record Decision(int target, String reason, String proposalId) {}

    /** A Kubernetes Event to raise on the {@code PharosCluster}: {@code type} Normal or Warning. */
    public record Notice(String reason, String type, String message) {}

    /**
     * The autoscaler's share of one reconcile.
     *
     * @param proposals        the new {@code status.proposals} (null when empty)
     * @param reshardProposals the new {@code status.reshardProposals} (null when empty)
     * @param lowDisk          one line per writer-disk proposal in force; null when unknown (no node
     *                         answered: the previous condition stands)
     * @param recheck          when to reconcile again for a proposal waiting out the cooldown, or null
     */
    public record Plan(Decision writers, Decision readers, List<ProposalStatus> proposals,
                       List<ReshardProposal> reshardProposals, List<String> lowDisk, List<Notice> notices,
                       Duration recheck, LastScale writersLastScale, LastScale readersLastScale) {}

    /**
     * One role's decision.
     *
     * @param current   the StatefulSet's {@code replicas}
     * @param proposals the proposals in force, or null when none may be applied (no pharos node answered)
     * @param history   what was applied before ({@code status.proposals})
     */
    public static Decision decide(Role role, int current, Bounds bounds, List<Proposal> proposals,
                                  AppliedHistory history, Instant now) {
        if (current < bounds.min()) {
            return new Decision(bounds.min(), role.target() + " below the floor " + bounds.min(), null);
        }
        if (current > bounds.max()) {
            int target = role == Role.WRITERS ? current - 1 : bounds.max();
            return new Decision(target, role.target() + " above max " + bounds.max(), null);
        }
        if (proposals == null) {
            return new Decision(current, "no pharos node answered; no proposal applied", null);
        }
        Proposal p = inForce(role, proposals);
        if (p == null) return new Decision(current, "no " + role.target() + " proposal in force", null);
        Instant last = history.lastChange(role);
        if (last != null) {
            Instant until = last.plusSeconds(bounds.cooldownSeconds());
            if (now.isBefore(until)) {
                return new Decision(current, p.kind() + " waits for the cooldown until " + until, null);
            }
        }
        int step = step(role, p, bounds);
        int target = bounds.clamp(current + step);
        if (target == current) {
            return new Decision(current, p.kind() + ": already at " + (step > 0 ? "max " + bounds.max()
                    : "the floor " + bounds.min()), null);
        }
        ProposalStatus rec = history.record(p.id());
        String episode = rec != null && Objects.equals(rec.since(), p.since()) && Boolean.TRUE.equals(rec.applied())
                ? "still in force" : "new";
        return new Decision(target, p.kind() + " (" + episode + ", since " + p.since() + ", delta "
                + (p.delta() == null ? "none" : p.delta()) + "): " + current + " -> " + target, p.id());
    }

    /**
     * The signed step of a proposal: its {@code delta}, or one node its kind's way without one (or
     * with a delta pointing the other way); capped at {@code maxStepUp} / {@code maxStepDown}, and
     * at one for writers going down.
     */
    static int step(Role role, Proposal p, Bounds bounds) {
        boolean add = role.addKind().equals(p.kind());
        int raw = p.delta() == null ? (add ? 1 : -1) : p.delta();
        if (add ? raw <= 0 : raw >= 0) raw = add ? 1 : -1;
        int step = Math.max(-bounds.maxStepDown(), Math.min(bounds.maxStepUp(), raw));
        if (role == Role.WRITERS && step < -1) step = -1;
        return step;
    }

    /** The role's proposal in force: an add before a remove (pharos never raises both). */
    static Proposal inForce(Role role, List<Proposal> proposals) {
        Proposal remove = null;
        for (Proposal p : proposals) {
            // without an id it could not be recorded, and so not held to the cooldown
            if (p == null || p.id() == null || p.target() != null && !role.target().equals(p.target())) continue;
            if (role.addKind().equals(p.kind())) return p;
            if (role.removeKind().equals(p.kind()) && remove == null) remove = p;
        }
        return remove;
    }

    /**
     * Plans a reconcile: both roles' replicas, the new {@code status.proposals} and
     * {@code status.reshardProposals}, the writer-disk lines and the Events to raise.
     *
     * @param writersNow the writer StatefulSet's {@code replicas}, or null when it does not exist yet
     * @param readersNow the same for readers
     * @param reading    the latest {@code /cluster/status}, or null when no pharos node answered
     */
    public static Plan plan(PharosCluster cluster, Integer writersNow, Integer readersNow, StatusReading reading,
                            Instant now) {
        return plan(cluster, writersNow, readersNow, null, null, reading, now);
    }

    /**
     * {@link #plan(PharosCluster, Integer, Integer, StatusReading, Instant)} with the roles' last
     * applications read from their StatefulSets ({@link LastScale}, null when absent), which take
     * precedence over {@code status.proposals}.
     */
    public static Plan plan(PharosCluster cluster, Integer writersNow, Integer readersNow, LastScale writersLast,
                            LastScale readersLast, StatusReading reading, Instant now) {
        PharosClusterSpec spec = cluster.getSpec();
        PharosClusterStatus previous = cluster.getStatus();
        AppliedHistory history = AppliedHistory.of(previous, writersLast, readersLast);
        ClusterStatus status = reading == null ? null : reading.status();
        List<Proposal> inForce = status == null ? null
                : status.proposals() == null ? List.of() : status.proposals().stream().filter(Objects::nonNull).toList();
        boolean enabled = Defaults.autoscalingEnabled(spec.autoscaling());
        List<Proposal> applicable = enabled ? inForce : null;

        Bounds wb = Bounds.of(spec, Role.WRITERS, now);
        Bounds rb = Bounds.of(spec, Role.READERS, now);
        Decision writers = role(Role.WRITERS, writersNow, wb, applicable, history, now, enabled, status,
                Names.writers(cluster.getMetadata().getName()));
        Decision readers = role(Role.READERS, readersNow, rb, applicable, history, now, enabled, status, null);

        List<Notice> notices = new ArrayList<>();
        Duration recheck = null;
        for (var d : List.of(Map.entry(Role.WRITERS, writers), Map.entry(Role.READERS, readers))) {
            Role role = d.getKey();
            Integer current = role == Role.WRITERS ? writersNow : readersNow;
            if (d.getValue().proposalId() != null) {
                notices.add(new Notice(EVENT_SCALED, "Normal", role.target() + " scaled " + current + " -> "
                        + d.getValue().target() + ": " + d.getValue().reason()));
            }
            Instant last = history.lastChange(role);
            Bounds b = role == Role.WRITERS ? wb : rb;
            if (applicable != null && last != null && inForce(role, applicable) != null
                    && d.getValue().proposalId() == null) {
                Instant until = last.plusSeconds(b.cooldownSeconds());
                if (now.isBefore(until)) {
                    Duration wait = Duration.between(now, until).plusSeconds(1);
                    if (recheck == null || wait.compareTo(recheck) < 0) recheck = wait;
                }
            }
        }

        String appliedAt = now.truncatedTo(ChronoUnit.SECONDS).toString();
        LastScale wl = lastScale(writers, writersNow, applicable, appliedAt, writersLast, Role.WRITERS);
        LastScale rl = lastScale(readers, readersNow, applicable, appliedAt, readersLast, Role.READERS);
        if (status == null) {
            return new Plan(writers, readers, previous == null ? null : previous.proposals(),
                    previous == null ? null : previous.reshardProposals(), null, notices, recheck, wl, rl);
        }
        List<ProposalStatus> records = records(inForce, history, Map.of(Role.WRITERS, writers, Role.READERS, readers),
                writersNow, readersNow, wb, rb, now, notices);
        List<ReshardProposal> reshards = reshards(inForce, previous, notices);
        List<String> lowDisk = inForce.stream().filter(p -> WRITER_DISK.equals(p.kind()))
                .map(p -> "writer " + p.target() + ": " + (p.reason() == null ? "low on disk" : p.reason()))
                .sorted().toList();
        return new Plan(writers, readers, records.isEmpty() ? null : records, reshards.isEmpty() ? null : reshards,
                lowDisk, notices, recheck, wl, rl);
    }

    /** The role's annotation to apply: this reconcile's application, else the one already there. */
    private static LastScale lastScale(Decision d, Integer current, List<Proposal> applicable, String appliedAt,
                                       LastScale observed, Role role) {
        if (d.proposalId() == null || applicable == null) return observed;
        Proposal p = applicable.stream().filter(x -> d.proposalId().equals(x.id())).findFirst().orElseThrow();
        return new LastScale(p.id(), p.kind(), p.target() == null ? role.target() : p.target(), p.since(), appliedAt,
                current, d.target());
    }

    private static Decision role(Role role, Integer current, Bounds bounds, List<Proposal> applicable,
                                 AppliedHistory history, Instant now, boolean enabled, ClusterStatus status,
                                 String writersSts) {
        if (current == null) {
            return new Decision(bounds.min(), role.target() + " start at the floor " + bounds.min(), null);
        }
        Decision d = decide(role, current, bounds, applicable, history, now);
        if (!enabled && applicable == null && d.target() == current) {
            d = new Decision(current, "autoscaling is disabled", null);
        }
        if (role == Role.WRITERS && d.target() < current) {
            String blocker = ScaleDownGate.writerBlocker(writersSts, current, status);
            if (blocker != null) {
                return new Decision(current, d.reason() + "; the step down waits: " + blocker, null);
            }
        }
        return d;
    }

    /**
     * The new {@code status.proposals}: one record per scaling or writer-disk proposal in force (its
     * episode carried over when {@code since} is unchanged; a new {@code since} starts a new, not yet
     * applied, episode that keeps the id's last application), plus records of cleared proposals still
     * inside their role's cooldown (so the cooldown survives a restart). Sorted by kind and id.
     */
    private static List<ProposalStatus> records(List<Proposal> inForce, AppliedHistory history,
                                                Map<Role, Decision> decisions, Integer writersNow, Integer readersNow,
                                                Bounds wb, Bounds rb, Instant now, List<Notice> notices) {
        Map<String, ProposalStatus> out = new LinkedHashMap<>();
        for (Proposal p : inForce) {
            Role role = Role.of(p.kind());
            boolean disk = WRITER_DISK.equals(p.kind());
            if ((role == null && !disk) || p.id() == null) continue;
            ProposalStatus prev = history.record(p.id());
            boolean sameEpisode = prev != null && Objects.equals(prev.since(), p.since());
            Boolean applied = disk ? null : sameEpisode && Boolean.TRUE.equals(prev.applied());
            out.put(p.id(), new ProposalStatus(p.id(), p.kind(), p.target(), p.since(), applied,
                    prev == null ? null : prev.appliedAt(), prev == null ? null : prev.fromReplicas(),
                    prev == null ? null : prev.toReplicas()));
            if (disk && !sameEpisode) {
                notices.add(new Notice(EVENT_WRITER_DISK, "Warning", "writer " + p.target() + " is low on disk"
                        + (p.reason() == null ? "" : ": " + p.reason())
                        + (p.action() == null ? "" : " (" + p.action() + ")")
                        + "; v1 does not resize volumes"));
            }
        }
        for (ProposalStatus r : history.records()) {
            Role role = Role.of(r.kind());
            if (out.containsKey(r.id()) || role == null) continue;
            Instant at = AppliedHistory.instant(r.appliedAt());
            int cooldown = (role == Role.WRITERS ? wb : rb).cooldownSeconds();
            if (at != null && now.isBefore(at.plusSeconds(cooldown))) out.put(r.id(), r);
        }
        String appliedAt = now.truncatedTo(ChronoUnit.SECONDS).toString();
        decisions.forEach((role, d) -> {
            if (d.proposalId() == null) return;
            ProposalStatus r = out.get(d.proposalId());
            out.put(d.proposalId(), new ProposalStatus(r.id(), r.kind(), r.target(), r.since(), true, appliedAt,
                    role == Role.WRITERS ? writersNow : readersNow, d.target()));
        });
        return out.values().stream()
                .sorted(Comparator.comparing((ProposalStatus r) -> String.valueOf(r.kind()))
                        .thenComparing(r -> String.valueOf(r.id())))
                .toList();
    }

    /** The reshard proposals in force, copied as pharos reports them; an Event for each new one. */
    private static List<ReshardProposal> reshards(List<Proposal> inForce, PharosClusterStatus previous,
                                                  List<Notice> notices) {
        Set<String> seen = previous == null || previous.reshardProposals() == null ? Set.of()
                : previous.reshardProposals().stream().filter(Objects::nonNull)
                        .map(r -> r.id() + "@" + r.since()).collect(Collectors.toSet());
        List<ReshardProposal> out = new ArrayList<>();
        for (Proposal p : inForce) {
            if (!RESHARD.equals(p.kind())) continue;
            out.add(new ReshardProposal(p.id(), p.kind(), p.target(), p.reason(), metrics(p.metrics()), p.action(),
                    p.since()));
            if (!seen.contains(p.id() + "@" + p.since())) {
                notices.add(new Notice(EVENT_RESHARD, "Normal", "pharos proposes to reshard " + p.target()
                        + (p.reason() == null ? "" : ": " + p.reason())
                        + (p.action() == null ? "" : " (" + p.action() + ")")
                        + "; surfaced in status.reshardProposals, never applied in v1"));
            }
        }
        return out;
    }

    private static Map<String, JsonNode> metrics(Map<String, Object> m) {
        if (m == null) return null;
        Map<String, JsonNode> out = new TreeMap<>();
        m.forEach((k, v) -> out.put(k, JSON.valueToTree(v)));
        return out;
    }
}
