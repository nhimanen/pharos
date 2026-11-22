package com.pharos.operator.reconcile;

import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.Phase;
import com.pharos.operator.validate.SpecValidator;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

/**
 * Reconciles a {@link PharosCluster}. So far it only validates the spec and reports the result in
 * {@code .status}; the dependent resources come later. An invalid spec gives {@code phase: Pending}
 * with a {@code SpecInvalid} condition listing every error, and nothing is created.
 *
 * <p>This reconciler is the only writer of {@code .status} (ruling OP-R3).
 */
@ControllerConfiguration(name = PharosClusterReconciler.NAME)
public class PharosClusterReconciler implements Reconciler<PharosCluster> {

    public static final String NAME = "pharoscluster";

    /** Reasons. */
    static final String REASON_SPEC_INVALID = "SpecInvalid";
    static final String REASON_SPEC_VALID = "SpecValid";
    static final String REASON_NOT_PROVISIONED = "NotProvisioned";

    private static final Logger LOG = LoggerFactory.getLogger(PharosClusterReconciler.class);

    private final Clock clock;

    public PharosClusterReconciler() {
        this(Clock.systemUTC());
    }

    PharosClusterReconciler(Clock clock) {
        this.clock = clock;
    }

    @Override
    public UpdateControl<PharosCluster> reconcile(PharosCluster resource, Context<PharosCluster> context) {
        PharosClusterStatus desired = desiredStatus(resource);
        if (Objects.equals(desired, resource.getStatus())) {
            return UpdateControl.noUpdate();
        }
        return UpdateControl.patchStatus(statusPatch(resource, desired));
    }

    /** The status for the resource as it is now: validation result and phase. */
    PharosClusterStatus desiredStatus(PharosCluster resource) {
        long generation = resource.getMetadata().getGeneration() == null ? 0 : resource.getMetadata().getGeneration();
        PharosClusterStatus previous = resource.getStatus();
        List<String> errors = SpecValidator.validate(resource.getSpec());
        var conditions = new StatusConditions(previous == null ? null : previous.conditions(), generation,
                clock.instant());
        if (!errors.isEmpty()) {
            LOG.info("PharosCluster {}/{}: spec is invalid ({} errors): {}", resource.getMetadata().getNamespace(),
                    resource.getMetadata().getName(), errors.size(), errors);
            conditions.set(Phase.Pending.name(), true, REASON_SPEC_INVALID,
                    "The spec is invalid; nothing is created until it is fixed.")
                    .set(PharosClusterStatus.CONDITION_SPEC_INVALID, true, REASON_SPEC_INVALID,
                            String.join("; ", errors));
        } else {
            conditions.set(Phase.Pending.name(), true, REASON_NOT_PROVISIONED,
                    "The spec is valid; no resources are created yet.")
                    .set(PharosClusterStatus.CONDITION_SPEC_INVALID, false, REASON_SPEC_VALID, "");
        }
        return new PharosClusterStatus(Phase.Pending.name(), generation, null, null, null, null, null, null,
                conditions.build());
    }

    /**
     * A fresh resource carrying only identity and status: the status patch is a server-side apply,
     * and a copy of the whole resource would make the operator a field manager of the spec too.
     */
    private static PharosCluster statusPatch(PharosCluster resource, PharosClusterStatus status) {
        PharosCluster patch = new PharosCluster();
        patch.setMetadata(new ObjectMetaBuilder()
                .withName(resource.getMetadata().getName())
                .withNamespace(resource.getMetadata().getNamespace())
                .build());
        patch.setStatus(status);
        return patch;
    }
}
