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
 * when a proposal comes into force, changes its reason or action, or clears,
 * {@code {type: layout, summary}} when the scale (shard, writer or reader count) changes, and
 * {@code {type: reshard, reshard}} when a project's reshard changes phase ({@code requested},
 * {@code building}, {@code pinned}, {@code flipped}, {@code abandoned}; {@code ended} once it is no longer
 * shown) or reaches the next quarter of its shards or files (never one event per file or shard).
 */
@JsonPropertyOrder({"type", "proposal", "summary", "reshard"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StatusEvent(String type, ClusterStatus.Proposal proposal, StatusState.Summary summary,
                          ReshardChange reshard) {

    public static final String APPEARED = "appeared";
    public static final String CHANGED = "changed";
    public static final String CLEARED = "cleared";
    public static final String LAYOUT = "layout";
    public static final String RESHARD = "reshard";
    /** The phase of a {@code reshard} event once the project's reshard is no longer shown. */
    public static final String ENDED = "ended";

    public StatusEvent(String type, ClusterStatus.Proposal proposal, StatusState.Summary summary) {
        this(type, proposal, summary, null);
    }

    /** A {@code reshard} event's payload: the project, the spec version, its phase and progress. */
    @JsonPropertyOrder({"project", "specVersion", "phase", "shardsDone", "shardsTotal", "filesDone", "filesTotal"})
    public record ReshardChange(String project, int specVersion, String phase, Integer shardsDone,
                                Integer shardsTotal, Integer filesDone, Integer filesTotal) {}

    public String toJson() {
        try {
            return ClusterStatus.MAPPER.writeValueAsString(this);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The events between two states (layout first, then proposals by id, then reshards by project),
     * proposals carrying the metrics of {@code cands}. Equal states give none; the first layout ever
     * recorded is no change.
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
                out.add(new StatusEvent(CHANGED, proposal(id, is, cands), null));
            }
        }
        TreeSet<String> projects = new TreeSet<>(a.reshards().keySet());
        projects.addAll(b.reshards().keySet());
        for (String p : projects) {
            StatusState.ReshardMark was = a.reshards().get(p);
            StatusState.ReshardMark is = b.reshards().get(p);
            if (is != null && !is.sameStep(was)) {
                out.add(new StatusEvent(RESHARD, null, null, new ReshardChange(p, is.specVersion(), is.phase(),
                        is.shardsDone(), is.shardsTotal(), is.filesDone(), is.filesTotal())));
            } else if (is == null && was != null) {
                out.add(new StatusEvent(RESHARD, null, null, new ReshardChange(p, was.specVersion(), ENDED,
                        null, null, null, null)));
            }
        }
        return out;
    }

    private static ClusterStatus.Proposal proposal(String id, StatusState.Rule r, Map<String, Proposals.Candidate> cands) {
        Proposals.Candidate c = cands == null ? null : cands.get(id);
        return new ClusterStatus.Proposal(id, r.kind(), r.target(), r.reason(),
                c == null ? new TreeMap<>() : c.metrics(), r.action(), Instant.ofEpochSecond(r.since()).toString());
    }
}
