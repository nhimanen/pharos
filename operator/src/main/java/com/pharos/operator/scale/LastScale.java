package com.pharos.operator.scale;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.operator.api.PharosClusterStatus.ProposalStatus;

/**
 * A role's last proposal application, kept in its StatefulSet's {@value #ANNOTATION} annotation
 * (ruling OP-R5). It is written in the same apply as the {@code replicas} it explains, so it cannot be
 * lost while the step stands: not when the operator dies before the status patch, and not when
 * another dependent fails and the status keeps its old records. The autoscaler reads cooldown and
 * episode state from it first; {@code status.proposals} is the informational copy.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"proposalId", "kind", "target", "since", "appliedAt", "fromReplicas", "toReplicas"})
public record LastScale(String proposalId, String kind, String target, String since, String appliedAt,
                        Integer fromReplicas, Integer toReplicas) {

    public static final String ANNOTATION = com.pharos.operator.render.Labels.LAST_SCALE;

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The annotation's value (deterministic JSON). */
    public String toJson() {
        try {
            return JSON.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The annotation read back, or null when absent or unreadable (then only the status counts). */
    public static LastScale parse(String json) {
        if (json == null || json.isBlank()) return null;
        try {
            LastScale s = JSON.readValue(json, LastScale.class);
            return s.proposalId() == null || AppliedHistory.instant(s.appliedAt()) == null ? null : s;
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    /** As a status record: this episode applied. */
    public ProposalStatus record() {
        return new ProposalStatus(proposalId, kind, target, since, true, appliedAt, fromReplicas, toReplicas);
    }
}
