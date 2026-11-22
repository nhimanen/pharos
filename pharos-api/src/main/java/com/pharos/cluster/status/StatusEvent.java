package com.pharos.cluster.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.IOException;

/**
 * One {@code /cluster/events} (and webhook) event: {@code {type: appeared|changed|cleared, proposal}}
 * when a proposal comes into force, changes its reason or action (with it the delta), or clears, and
 * {@code {type: layout, summary}} when the scale (shard, writer or reader count) changes. The events
 * themselves come from {@code Proposals.events}.
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
}
