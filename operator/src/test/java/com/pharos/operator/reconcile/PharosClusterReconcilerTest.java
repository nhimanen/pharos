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
    void validSpecIsProvisioningUntilStorageAndMinReplicasAreReady() {
        PharosCluster c = cluster(spec(new PharosClusterSpec.Storage(new PharosClusterSpec.Minio("20Gi"), null), 1, 2));
        PharosClusterStatus status = at(T0).provisionedStatus(c, 1, 1,
                new PharosClusterReconciler.Observed(null, null, 0, 0, 0, 0, false));
        assertThat(status.phase()).isEqualTo("Provisioning");
        assertThat(condition(status, "SpecInvalid").getStatus()).isEqualTo("False");
        assertThat(condition(status, "Pending").getStatus()).isEqualTo("False");
        assertThat(condition(status, "Provisioning").getStatus()).isEqualTo("True");
        assertThat(condition(status, "Provisioning").getMessage())
                .isEqualTo("Waiting for MinIO, writers (0/1 ready), readers (0/1 ready), the router.");
        assertThat(condition(status, "Ready").getStatus()).isEqualTo("False");
        assertThat(status.storage()).isEqualTo(new PharosClusterStatus.StorageStatus("minio", false, null));

        // MinIO up but the bucket Job not done yet: storage is still not ready
        assertThat(at(T0).provisionedStatus(c, 1, 1, new PharosClusterReconciler.Observed(1, 1, 1, 1, 1, 1, false))
                .phase()).isEqualTo("Provisioning");

        // every pod ready, but no node answers /cluster/status: no writer heartbeat is known yet
        PharosClusterStatus noHeartbeats = at(T0).provisionedStatus(c, 2, 1,
                new PharosClusterReconciler.Observed(2, 1, 2, 1, 1, 1, true));
        assertThat(noHeartbeats.phase()).isEqualTo("Provisioning");
        assertThat(condition(noHeartbeats, "WritersReady").getReason()).isEqualTo("NoClusterStatus");
        assertThat(condition(noHeartbeats, "Provisioning").getMessage())
                .isEqualTo("Waiting for writer heartbeats (no /cluster/status answer).");

        var heartbeat = new com.pharos.cluster.status.ClusterStatus.Writer("demo-writers-0", true, List.of(), List.of(),
                null, 0, false, null, 3);
        var answer = new com.pharos.cluster.status.ClusterStatus("cluster", "2026-11-22T10:00:00Z",
                new com.pharos.cluster.status.ClusterStatus.Layout(null, List.of(), List.of(heartbeat), List.of()),
                null, List.of(), List.of());
        PharosClusterStatus ready = at(T0).provisionedStatus(c, 2, 1,
                new PharosClusterReconciler.Observed(2, 1, 2, 1, 1, 1, true), List.of(), null, null, null, answer, null,
                null);
        assertThat(ready.phase()).isEqualTo("Ready");
        assertThat(condition(ready, "WritersReady").getStatus()).isEqualTo("True");
        assertThat(condition(ready, "ReadersReady").getStatus()).isEqualTo("True");
        assertThat(condition(ready, "ProjectsPublished").getReason()).isEqualTo("NoProjects");
        assertThat(condition(ready, "UpgradeBlocked").getStatus()).isEqualTo("False");
        assertThat(condition(ready, "Ready").getStatus()).isEqualTo("True");
        assertThat(condition(ready, "Provisioning").getStatus()).isEqualTo("False");
        assertThat(ready.writers()).isEqualTo(new PharosClusterStatus.Replicas(2, 2));
        assertThat(ready.storage()).isEqualTo(new PharosClusterStatus.StorageStatus("minio", true, null));
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

    @Test
    void noDependentCanDeleteAnything() {
        // JOSDK deletes a Deleter whose precondition stops holding; data-bearing objects must never go that way
        assertThat(Dependents.ALL).hasSize(9).allSatisfy(type -> {
            assertThat(io.javaoperatorsdk.operator.api.reconciler.dependent.Deleter.class.isAssignableFrom(type))
                    .as(type.getSimpleName()).isFalse();
        });
        assertThat(io.javaoperatorsdk.operator.processing.dependent.Updater.class.isAssignableFrom(Dependents.Secrets.class))
                .isFalse();
        assertThat(io.javaoperatorsdk.operator.processing.dependent.Updater.class.isAssignableFrom(Dependents.Jobs.class))
                .isFalse();
    }

    @Test
    void storageKindChangeIsRefused() {
        PharosCluster c = cluster(spec(new PharosClusterSpec.Storage(null, new PharosClusterSpec.S3(
                "https://s3.example", "b", null, null, "creds")), 1, 1));
        assertThat(PharosClusterReconciler.storageKindChange(c)).isNull(); // no status yet
        c.setStatus(new PharosClusterStatus("Ready", 1L, null, null,
                new PharosClusterStatus.StorageStatus("minio", true, null), null, null, null, List.of()));
        assertThat(PharosClusterReconciler.storageKindChange(c))
                .startsWith("spec.storage: the storage kind is immutable (it is minio; the spec asks for s3)");
        UpdateControl<PharosCluster> control = at(T0).reconcile(c, null); // invalid: the context is never used
        PharosClusterStatus status = control.getResource().orElseThrow().getStatus();
        assertThat(status.phase()).isEqualTo("Pending");
        assertThat(status.storage().kind()).isEqualTo("minio");
        assertThat(condition(status, "SpecInvalid").getMessage()).contains("the storage kind is immutable");
    }

    @Test
    void volumesKeepTheirSizeAndNeverShrink() {
        List<String> notes = new java.util.ArrayList<>();
        assertThat(PharosClusterReconciler.keepSize(null, "80Gi", "f", notes)).isEqualTo("80Gi");
        assertThat(PharosClusterReconciler.keepSize("50Gi", "51200Mi", "f", notes)).isEqualTo("50Gi");
        assertThat(notes).isEmpty(); // the same size, written differently
        assertThat(PharosClusterReconciler.keepSize("50Gi", "80Gi", "spec.writers.workVolume", notes)).isEqualTo("50Gi");
        assertThat(PharosClusterReconciler.noShrink("20Gi", "30Gi", notes)).isEqualTo("30Gi");
        assertThat(PharosClusterReconciler.noShrink("20Gi", "10Gi", notes)).isEqualTo("20Gi");
        assertThat(notes).containsExactly(
                "spec.writers.workVolume: 50Gi -> 80Gi is not applied (StatefulSet volumes cannot be resized in v1);"
                        + " the volumes stay 50Gi",
                "spec.storage.minio.storage: 20Gi -> 10Gi is not applied (a volume never shrinks); it stays 20Gi");
    }

    @Test
    void bucketBackoffDoublesUpToFifteenMinutes() {
        assertThat(PharosClusterReconciler.bucketBackoffSeconds(null)).isEqualTo(60);
        assertThat(PharosClusterReconciler.bucketBackoffSeconds(1)).isEqualTo(120);
        assertThat(PharosClusterReconciler.bucketBackoffSeconds(3)).isEqualTo(480);
        assertThat(PharosClusterReconciler.bucketBackoffSeconds(4)).isEqualTo(900);
        assertThat(PharosClusterReconciler.bucketBackoffSeconds(60)).isEqualTo(900);
    }

    @Test
    void applyErrorsBecomeOneConditionKeepingTheStatus() {
        PharosCluster c = cluster(spec(new PharosClusterSpec.Storage(new PharosClusterSpec.Minio("20Gi"), null), 1, 2));
        c.setStatus(new PharosClusterStatus("Provisioning", 3L, new PharosClusterStatus.Replicas(1, 0), null,
                new PharosClusterStatus.StorageStatus("minio", false, null), null, null, null, List.of()));
        var e = new io.javaoperatorsdk.operator.AggregatedOperatorException("workflow failed", java.util.Map.of(
                "writers", new IllegalStateException("StatefulSet is invalid: spec: Forbidden\nmore lines"),
                "token", new IllegalStateException("creating Secret demo-token failed (HTTP 500)")));
        var control = at(T0).updateErrorStatus(c, null, e);
        PharosClusterStatus status = control.getResource().orElseThrow().getStatus();
        assertThat(status.phase()).isEqualTo("Provisioning");
        assertThat(status.writers()).isEqualTo(new PharosClusterStatus.Replicas(1, 0));
        Condition failed = condition(status, "ApplyFailed");
        assertThat(failed.getStatus()).isEqualTo("True");
        assertThat(failed.getMessage()).isEqualTo("token: creating Secret demo-token failed (HTTP 500);"
                + " writers: StatefulSet is invalid: spec: Forbidden");
        assertThat(PharosClusterReconciler.applyError(new RuntimeException("x".repeat(1000))))
                .hasSize(PharosClusterReconciler.MAX_ERROR_TEXT + 3);
    }
}
