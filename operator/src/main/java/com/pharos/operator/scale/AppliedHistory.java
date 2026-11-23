package com.pharos.operator.scale;

import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.ProposalStatus;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What the operator has applied: the roles' StatefulSet annotations ({@link LastScale}, the source of
 * truth, written in the same apply as {@code replicas}) over {@code status.proposals} (the
 * informational copy, which also remembers cleared proposals), so the cooldown and the episodes
 * survive an operator restart and a reconcile whose status patch never happened. Each record is one proposal id: its current episode
 * ({@code since}), whether that episode was applied, and the last application of the id
 * ({@code appliedAt}, {@code fromReplicas}, {@code toReplicas}), which a new episode carries over
 * so the role's cooldown is never forgotten.
 */
public final class AppliedHistory {

    public static final AppliedHistory EMPTY = new AppliedHistory(List.of());

    private final Map<String, ProposalStatus> byId = new LinkedHashMap<>();

    public AppliedHistory(List<ProposalStatus> records) {
        if (records != null) for (ProposalStatus r : records) if (r != null && r.id() != null) byId.put(r.id(), r);
    }

    public static AppliedHistory of(PharosClusterStatus status) {
        return status == null ? EMPTY : new AppliedHistory(status.proposals());
    }

    /**
     * The status records with the StatefulSets' last applications laid over them: an annotation newer
     * than the record of its id (or with no record at all) replaces it as an applied episode.
     */
    public static AppliedHistory of(PharosClusterStatus status, LastScale... lastScales) {
        AppliedHistory h = of(status);
        if (lastScales.length == 0) return h;
        AppliedHistory out = new AppliedHistory(h.records());
        for (LastScale s : lastScales) {
            if (s == null) continue;
            ProposalStatus r = out.byId.get(s.proposalId());
            Instant recorded = r == null ? null : instant(r.appliedAt());
            Instant annotated = instant(s.appliedAt());
            if (recorded == null || annotated.isAfter(recorded)) out.byId.put(s.proposalId(), s.record());
        }
        return out;
    }

    public ProposalStatus record(String id) {
        return byId.get(id);
    }

    public List<ProposalStatus> records() {
        return List.copyOf(byId.values());
    }

    /** The role's last applied change (any of its proposal ids, any episode), or null. */
    public Instant lastChange(Role role) {
        Instant last = null;
        for (ProposalStatus r : byId.values()) {
            if (!role.owns(r.kind())) continue;
            Instant at = instant(r.appliedAt());
            if (at != null && (last == null || at.isAfter(last))) last = at;
        }
        return last;
    }

    /** The instant of an {@code appliedAt}, or null when absent or unreadable. */
    static Instant instant(String s) {
        if (s == null) return null;
        try {
            return Instant.parse(s);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
