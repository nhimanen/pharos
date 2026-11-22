package com.pharos.operator.scale;

import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.ProposalStatus;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What the operator has applied, read from {@code status.proposals} only, so the cooldown and the
 * episodes survive an operator restart. Each record is one proposal id: its current episode
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
