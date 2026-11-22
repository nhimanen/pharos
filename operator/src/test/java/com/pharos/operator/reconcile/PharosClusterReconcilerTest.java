package com.pharos.operator.reconcile;

import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterStatus;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PharosClusterReconcilerTest {

    private static final Instant T0 = Instant.parse("2026-11-22T10:00:00Z");

    private static PharosCluster cluster(PharosClusterSpec spec) {
        PharosCluster c = new PharosCluster();
        c.setMetadata(new ObjectMetaBuilder().withName("demo").withNamespace("pharos").withGeneration(3L).build());
        c.setSpec(spec);
        return c;
    }

    private static PharosClusterSpec spec(PharosClusterSpec.Storage storage, int writersMin, int writersMax) {
        return new PharosClusterSpec("1.6.0", storage, null, List.of(),
                new PharosClusterSpec.RoleSpec(writersMin, writersMax, null, null, null, null),
                new PharosClusterSpec.RoleSpec(1, 2, null, null, null, null), null, null, null);
    }

    private static PharosClusterReconciler at(Instant now) {
        return new PharosClusterReconciler(Clock.fixed(now, ZoneOffset.UTC));
    }

    private static Condition condition(PharosClusterStatus status, String type) {
        return status.conditions().stream().filter(c -> c.getType().equals(type)).findFirst().orElseThrow();
    }

    @Test
    void invalidSpecSetsPendingWithSpecInvalidListingErrors() {
        PharosCluster c = cluster(spec(new PharosClusterSpec.Storage(null, null), 3, 1));
        UpdateControl<PharosCluster> control = at(T0).reconcile(c, null);

        // Only a status patch: no spec/metadata update, nothing else touched (the reconciler has no client).
        assertThat(control.isPatchStatus()).isTrue();
        assertThat(control.isPatchResource()).isFalse();
        PharosCluster patch = control.getResource().orElseThrow();
        assertThat(patch.getSpec()).isNull();
        assertThat(patch.getMetadata().getName()).isEqualTo("demo");
        assertThat(patch.getMetadata().getNamespace()).isEqualTo("pharos");

        PharosClusterStatus status = patch.getStatus();
        assertThat(status.phase()).isEqualTo("Pending");
        assertThat(status.observedGeneration()).isEqualTo(3L);
        Condition invalid = condition(status, "SpecInvalid");
        assertThat(invalid.getStatus()).isEqualTo("True");
        assertThat(invalid.getReason()).isEqualTo("SpecInvalid");
        assertThat(invalid.getMessage()).isEqualTo(
                "spec.storage: set exactly one of minio or s3; spec.writers: min (3) is above max (1)");
        assertThat(condition(status, "Pending").getReason()).isEqualTo("SpecInvalid");
    }

    @Test
    void validSpecIsPendingWithoutSpecInvalid() {
        PharosCluster c = cluster(spec(new PharosClusterSpec.Storage(new PharosClusterSpec.Minio("20Gi"), null), 1, 2));
        PharosClusterStatus status = at(T0).reconcile(c, null).getResource().orElseThrow().getStatus();
        assertThat(status.phase()).isEqualTo("Pending");
        assertThat(condition(status, "SpecInvalid").getStatus()).isEqualTo("False");
        assertThat(condition(status, "Pending").getReason()).isEqualTo("NotProvisioned");
    }

    @Test
    void unchangedStatusIsNotPatchedAndKeepsTransitionTime() {
        PharosCluster c = cluster(spec(new PharosClusterSpec.Storage(null, null), 1, 1));
        c.setStatus(at(T0).reconcile(c, null).getResource().orElseThrow().getStatus());

        UpdateControl<PharosCluster> again = at(T0.plusSeconds(600)).reconcile(c, null);
        assertThat(again.isNoUpdate()).isTrue();

        // A changed error list re-patches, keeping SpecInvalid's lastTransitionTime (still True).
        c.setSpec(spec(new PharosClusterSpec.Storage(null, null), 2, 1));
        PharosClusterStatus next = at(T0.plusSeconds(900)).reconcile(c, null).getResource().orElseThrow().getStatus();
        assertThat(condition(next, "SpecInvalid").getLastTransitionTime()).isEqualTo(T0.toString());
        assertThat(condition(next, "SpecInvalid").getMessage()).contains("min (2) is above max (1)");
    }
}
