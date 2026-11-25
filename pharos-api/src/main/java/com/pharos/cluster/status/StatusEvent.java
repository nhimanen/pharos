package com.pharos.cluster.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.IOException;

/**
 * One {@code /cluster/events} (and webhook) event: {@code {type: appeared|changed|cleared, proposal}}
 * when a proposal comes into force, changes its reason or action (with it the delta), or clears,
 * {@code {type: layout, summary}} when the scale (shard, writer or reader count) changes, and
 * {@code {type: reshard, reshard}} when a project's reshard changes phase ({@code requested},
 * {@code building}, {@code pinned}, {@code flipped}, {@code abandoned}; {@code ended} once it is no longer
 * shown) or reaches the next quarter of its shards or files (never one event per file or shard). The
 * events themselves come from {@code Proposals.events}.
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
}
