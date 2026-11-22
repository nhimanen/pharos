package com.pharos.cluster.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One {@code /cluster/events} (and webhook) event: {@code {type: appeared|changed|cleared, proposal}}
 * when a proposal comes into force, changes its reason or action (with it the delta), or clears, and
 * {@code {type: layout, summary}} when the scale (shard, writer or reader count) changes.
 */
@JsonPropertyOrder({"type", "proposal", "summary"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StatusEvent(String type, ClusterStatus.Proposal proposal, StatusState.Summary summary) {

    public static final String APPEARED = "appeared";
    public static final String CHANGED = "changed";
    public static final String CLEARED = "cleared";
    public static final String LAYOUT = "layout";

    public String toJson() {
        try {
            return ClusterStatus.MAPPER.writeValueAsString(this);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The events between two states (layout first, then proposals by id), proposals carrying the
     * metrics of {@code cands}. Equal states give none; the first layout ever recorded is no change.
     */
    public static List<StatusEvent> diff(StatusState before, StatusState after, Map<String, Proposals.Candidate> cands) {
        StatusState a = before == null ? StatusState.EMPTY : before;
        StatusState b = after == null ? StatusState.EMPTY : after;
        List<StatusEvent> out = new ArrayList<>();
        if (a.layout() != null && b.layout() != null && !Objects.equals(a.layout(), b.layout())) { // not the first record
            out.add(new StatusEvent(LAYOUT, null, b.layout()));
        }
        TreeSet<String> ids = new TreeSet<>(a.rules().keySet());
        ids.addAll(b.rules().keySet());
        for (String id : ids) {
            StatusState.Rule was = a.rules().get(id);
            StatusState.Rule is = b.rules().get(id);
            boolean wasOn = was != null && was.active();
            boolean isOn = is != null && is.active();
            if (!wasOn && isOn) {
                out.add(new StatusEvent(APPEARED, proposal(id, is, cands), null));
            } else if (wasOn && !isOn) {
                out.add(new StatusEvent(CLEARED, proposal(id, was, cands), null));
            } else if (wasOn && (!Objects.equals(was.reason(), is.reason()) || !Objects.equals(was.action(), is.action()))) {
                // the delta follows the action's count, so it needs no comparison of its own (and a rule an
                // older node rewrote without it is no change)
                out.add(new StatusEvent(CHANGED, proposal(id, is, cands), null));
            }
        }
        return out;
    }

    private static ClusterStatus.Proposal proposal(String id, StatusState.Rule r, Map<String, Proposals.Candidate> cands) {
        return Proposals.proposal(id, r, cands);
    }
}
