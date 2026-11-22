package com.pharos.operator.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.Condition;

import java.util.List;
import java.util.Map;

/**
 * Observed state, written only by the reconciler (ruling OP-R3). Never holds a secret.
 *
 * <p>{@code phase} is one of {@link Phase}; each phase is also a condition of the same type with a
 * reason, next to {@code SpecInvalid} and {@code UpgradeBlocked}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PharosClusterStatus(
        String phase,
        Long observedGeneration,
        Replicas writers,
        Replicas readers,
        StorageStatus storage,
        List<ProjectStatus> projects,
        List<ProposalStatus> proposals,
        List<ReshardProposal> reshardProposals,
        List<Condition> conditions) {

    /** The status phases (spec, resolved detail 9). */
    public enum Phase { Pending, Provisioning, Ready, Degraded }

    /** Condition types besides the phases. */
    public static final String CONDITION_SPEC_INVALID = "SpecInvalid";
    public static final String CONDITION_UPGRADE_BLOCKED = "UpgradeBlocked";
    public static final String CONDITION_STORAGE_READY = "StorageReady";
    /** A spec change to a volume that the existing object cannot take (v1 neither resizes nor shrinks). */
    public static final String CONDITION_VOLUME_CHANGE_NOT_APPLIED = "VolumeChangeNotApplied";
    /** Applying the desired objects failed; the message says which and why. */
    public static final String CONDITION_APPLY_FAILED = "ApplyFailed";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Replicas(Integer desired, Integer ready) {}

    /**
     * {@code kind}: {@code minio} | {@code s3}; immutable once set (switching would orphan the indexes).
     * {@code bucketRetries}: how often the failed minio bucket Job was re-created (drives its backoff).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record StorageStatus(String kind, Boolean ready, Integer bucketRetries) {}

    /**
     * A project: whether the catalog holds it ({@code registered}: a registration Job succeeded, and no
     * removal has since), plus what {@code /cluster/status} reports. A project removed from the spec is
     * listed until its removal Job succeeds. {@code retries}: how often its failed registration or
     * removal Job was re-created (drives the backoff); cleared on success.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ProjectStatus(String name, Boolean registered, Long generation, Long docCount, Boolean pending,
                                Integer retries) {

        /** Name and registration only (the rest comes from {@code /cluster/status}). */
        public ProjectStatus(String name, Boolean registered) {
            this(name, registered, null, null, null, null);
        }
    }

    /**
     * A scaling proposal the operator has seen: pharos's {@code id}, {@code kind}, {@code target} and
     * {@code since} (the episode), and the operator's last application of it.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ProposalStatus(String id, String kind, String target, String since, Boolean applied,
                                 String appliedAt, Integer fromReplicas, Integer toReplicas) {}

    /**
     * A reshard proposal, copied as pharos reports it ({@code ClusterStatus.Proposal}); surfaced only,
     * never applied in v1. {@code metrics} values are free-form numbers or strings.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ReshardProposal(String id, String kind, String target, String reason, Map<String, JsonNode> metrics,
                                  String action, String since) {}
}
