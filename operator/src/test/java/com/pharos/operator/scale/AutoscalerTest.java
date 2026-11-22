package com.pharos.operator.scale;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.cluster.status.ClusterStatus.Proposal;
import com.pharos.cluster.status.StatusState;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Autoscaling;
import com.pharos.operator.api.PharosClusterSpec.Floor;
import com.pharos.operator.api.PharosClusterSpec.RoleSpec;
import com.pharos.operator.api.PharosClusterSpec.ScheduleEntry;
import com.pharos.operator.api.PharosClusterStatus;
import com.pharos.operator.api.PharosClusterStatus.ProposalStatus;
import com.pharos.operator.status.StatusReading;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AutoscalerTest {

    static final Instant T0 = Instant.parse("2026-11-23T10:00:00Z");
    static final String S1 = "2026-11-23T09:55:00Z";
    static final String S2 = "2026-11-23T10:03:00Z";

    static final String ADD_READERS_ID = "id-add-readers";
    static final String REMOVE_READERS_ID = "id-remove-readers";

    static PharosCluster cluster(int writersMin, int writersMax, int readersMin, int readersMax,
                                 Autoscaling autoscaling) {
        PharosCluster c = new PharosCluster();
        c.setMetadata(new ObjectMetaBuilder().withName("demo").withNamespace("pharos").withGeneration(1L).build());
        c.setSpec(new PharosClusterSpec("1.6.0", new PharosClusterSpec.Storage(new PharosClusterSpec.Minio(null), null),
                null, List.of(), new RoleSpec(writersMin, writersMax, null, null, null, null),
                new RoleSpec(readersMin, readersMax, null, null, null, null), null, autoscaling, null));
        return c;
    }

    static PharosCluster readers(int min, int max) {
        return cluster(1, 1, min, max, null);
    }

    static Proposal proposal(String id, String kind, String target, Integer delta, String since) {
        return new Proposal(id, kind, target, kind + " because", Map.of("qpsPerReader", 80), "start " + delta,
                delta, since);
    }

    static Proposal addReaders(Integer delta, String since) {
        return proposal(ADD_READERS_ID, "add-readers", "readers", delta, since);
    }

    static ClusterStatus status(List<Proposal> proposals, List<ClusterStatus.Writer> writers,
                                List<ClusterStatus.Shard> shards) {
        return new ClusterStatus("cluster", "2026-11-23T10:00:00Z",
                new ClusterStatus.Layout(new StatusState.Summary(shards.size(), writers.size(), 1), shards, writers,
                        List.of()),
                new ClusterStatus.BacklogSummary(0, null), List.of(), proposals);
    }

    static StatusReading reading(Instant at, Proposal... proposals) {
        return new StatusReading(status(List.of(proposals), List.of(), List.of()), at, "10.0.0.1:7070");
    }

    /** A plan, then the reconciler's status patch of its records (what the next reconcile reads). */
    static Autoscaler.Plan step(PharosCluster c, Integer writers, Integer readers, StatusReading r, Instant now) {
        Autoscaler.Plan plan = Autoscaler.plan(c, writers, readers, r, now);
        c.setStatus(new PharosClusterStatus("Ready", 1L, null, null, null, null, plan.proposals(),
                plan.reshardProposals(), List.of()));
        return plan;
    }

    static ProposalStatus record(PharosCluster c, String id) {
        return c.getStatus().proposals() == null ? null
                : c.getStatus().proposals().stream().filter(r -> r.id().equals(id)).findFirst().orElse(null);
    }

    @Test
    void rampsOncePerCooldownWhileProposalInForce() {
        PharosCluster c = readers(1, 5);
        int n = 1;
        Autoscaler.Plan p = step(c, 1, n, reading(T0, addReaders(1, S1)), T0);
        assertThat(p.readers().target()).isEqualTo(2);
        assertThat(p.readers().proposalId()).isEqualTo(ADD_READERS_ID);
        assertThat(record(c, ADD_READERS_ID)).isEqualTo(new ProposalStatus(ADD_READERS_ID, "add-readers", "readers",
                S1, true, "2026-11-23T10:00:00Z", 1, 2));
        n = 2;

        // still in force: nothing inside the cooldown, however often the status is re-read
        for (int s : new int[]{1, 60, 120, 299}) {
            p = step(c, 1, n, reading(T0.plusSeconds(s), addReaders(1, S1)), T0.plusSeconds(s));
            assertThat(p.readers().target()).as("at +%ds", s).isEqualTo(2);
            assertThat(p.readers().proposalId()).isNull();
        }
        assertThat(p.recheck()).isEqualTo(Duration.ofSeconds(2)); // until T0+300, plus one second

        // then once per cooldown, towards max
        for (int k = 1; k <= 3; k++) {
            Instant at = T0.plusSeconds(300L * k);
            p = step(c, 1, n, reading(at, addReaders(1, S1)), at);
            assertThat(p.readers().target()).as("cooldown %d", k).isEqualTo(n + 1);
            n++;
            Instant half = at.plusSeconds(150);
            assertThat(step(c, 1, n, reading(half, addReaders(1, S1)), half).readers().target()).isEqualTo(n);
        }
        assertThat(n).isEqualTo(5);
        Instant later = T0.plusSeconds(1500);
        p = step(c, 1, n, reading(later, addReaders(1, S1)), later);
        assertThat(p.readers().target()).isEqualTo(5);
        assertThat(p.readers().reason()).contains("already at max 5");
        assertThat(record(c, ADD_READERS_ID)).isEqualTo(new ProposalStatus(ADD_READERS_ID, "add-readers", "readers",
                S1, true, "2026-11-23T10:15:00Z", 4, 5));
    }

    @Test
    void newSinceIsNewEpisode() {
        PharosCluster c = readers(1, 5);
        assertThat(step(c, 1, 1, reading(T0, addReaders(1, S1)), T0).readers().target()).isEqualTo(2);

        // cleared: its record stays while the cooldown runs, so the cooldown is remembered
        step(c, 1, 2, reading(T0.plusSeconds(100)), T0.plusSeconds(100));
        assertThat(record(c, ADD_READERS_ID)).isNotNull();

        // reappears with a new since: a new episode, not yet applied, still held to the role's cooldown
        Autoscaler.Plan p = step(c, 1, 2, reading(T0.plusSeconds(200), addReaders(1, S2)), T0.plusSeconds(200));
        assertThat(p.readers().target()).isEqualTo(2);
        assertThat(record(c, ADD_READERS_ID)).isEqualTo(new ProposalStatus(ADD_READERS_ID, "add-readers", "readers",
                S2, false, "2026-11-23T10:00:00Z", 1, 2));

        p = step(c, 1, 2, reading(T0.plusSeconds(300), addReaders(1, S2)), T0.plusSeconds(300));
        assertThat(p.readers().target()).isEqualTo(3);
        assertThat(p.readers().reason()).contains("new, since " + S2);
        assertThat(record(c, ADD_READERS_ID)).isEqualTo(new ProposalStatus(ADD_READERS_ID, "add-readers", "readers",
                S2, true, "2026-11-23T10:05:00Z", 2, 3));

        // cleared for good: the record goes once the cooldown has passed
        step(c, 1, 3, reading(T0.plusSeconds(400)), T0.plusSeconds(400));
        assertThat(record(c, ADD_READERS_ID)).isNotNull();
        step(c, 1, 3, reading(T0.plusSeconds(600)), T0.plusSeconds(600));
        assertThat(c.getStatus().proposals()).isNull();
    }

    @Test
    void noDoubleApplyAcrossOperatorRestart() {
        PharosCluster c = readers(1, 8);
        assertThat(step(c, 1, 2, reading(T0, addReaders(2, S1)), T0).readers().target()).isEqualTo(4);

        // a restart: nothing survives but the resource itself, read back from the API server
        PharosCluster restarted = Serialization.unmarshal(Serialization.asJson(c), PharosCluster.class);
        assertThat(restarted.getStatus().proposals()).isEqualTo(c.getStatus().proposals());

        Instant at = T0.plusSeconds(10);
        Autoscaler.Plan p = Autoscaler.plan(restarted, 1, 4, reading(at, addReaders(2, S1)), at);
        assertThat(p.readers().target()).isEqualTo(4);
        assertThat(p.readers().reason()).contains("cooldown until 2026-11-23T10:05:00Z");
        // nor through a new episode right after the restart
        p = Autoscaler.plan(restarted, 1, 4, reading(at, addReaders(2, S2)), at);
        assertThat(p.readers().target()).isEqualTo(4);

        at = T0.plusSeconds(300);
        assertThat(Autoscaler.plan(restarted, 1, 4, reading(at, addReaders(2, S1)), at).readers().target())
                .isEqualTo(6);
    }

    @Test
    void stepClampedByMaxStepAndBounds() {
        Bounds b = new Bounds(1, 10, 2, 1, 300);
        assertThat(decide(Role.READERS, 3, b, addReaders(5, S1)).target()).isEqualTo(5);        // maxStepUp 2
        assertThat(decide(Role.READERS, 3, new Bounds(1, 4, 2, 1, 300), addReaders(5, S1)).target())
                .isEqualTo(4);                                                                   // max
        assertThat(decide(Role.READERS, 5, b, removeReaders(-3)).target()).isEqualTo(4);        // maxStepDown 1
        assertThat(decide(Role.READERS, 5, new Bounds(1, 10, 2, 3, 300), removeReaders(-3)).target()).isEqualTo(2);
        assertThat(decide(Role.READERS, 5, new Bounds(4, 10, 2, 3, 300), removeReaders(-3)).target())
                .isEqualTo(4);                                                                   // min
        // writers leave one at a time, whatever maxStepDown says
        Proposal removeWriters = proposal("w", "remove-writers", "writers", -3, S1);
        assertThat(decide(Role.WRITERS, 5, new Bounds(1, 10, 2, 3, 300), removeWriters).target()).isEqualTo(4);
        Proposal addWriters = proposal("w+", "add-writers", "writers", 3, S1);
        assertThat(decide(Role.WRITERS, 2, new Bounds(1, 10, 3, 1, 300), addWriters).target()).isEqualTo(5);
        // at a bound the proposal changes nothing and is not recorded as applied
        Autoscaler.Decision atMax = decide(Role.READERS, 10, b, addReaders(1, S1));
        assertThat(atMax.target()).isEqualTo(10);
        assertThat(atMax.proposalId()).isNull();
        // outside the range (the spec changed): back into it, no proposal needed
        assertThat(Autoscaler.decide(Role.READERS, 12, b, null, AppliedHistory.EMPTY, T0).target()).isEqualTo(10);
        assertThat(Autoscaler.decide(Role.READERS, 0, b, null, AppliedHistory.EMPTY, T0).target()).isEqualTo(1);
    }

    @Test
    void missingDeltaStepsByOne() {
        Bounds b = new Bounds(1, 10, 2, 2, 300);
        assertThat(decide(Role.READERS, 3, b, addReaders(null, S1)).target()).isEqualTo(4);
        assertThat(decide(Role.READERS, 3, b, removeReaders(null)).target()).isEqualTo(2);
        assertThat(decide(Role.WRITERS, 3, b, proposal("w", "add-writers", "writers", null, S1)).target()).isEqualTo(4);
        // a delta pointing against its kind counts as none
        assertThat(decide(Role.READERS, 3, b, addReaders(-2, S1)).target()).isEqualTo(4);
        assertThat(decide(Role.READERS, 3, b, addReaders(0, S1)).target()).isEqualTo(4);
    }

    @Test
    void minFloorsEnforcedWhenNoNodeAnswers() {
        Autoscaling schedule = new Autoscaling(true, null, null, null, null,
                List.of(new ScheduleEntry("0 8 * * *", "UTC", new Floor(4), null)));
        PharosCluster c = cluster(2, 4, 3, 6, schedule);
        ProposalStatus seen = new ProposalStatus(ADD_READERS_ID, "add-readers", "readers", S1, false, null, null, null);
        var reshard = new PharosClusterStatus.ReshardProposal("r", "reshard", "big", "too big", null, "split", S1);
        c.setStatus(new PharosClusterStatus("Ready", 1L, null, null, null, null, List.of(seen), List.of(reshard),
                List.of()));

        // cold start: the StatefulSets are created at the floors (the schedule's 4 readers since 08:00)
        Autoscaler.Plan p = Autoscaler.plan(c, null, null, null, T0);
        assertThat(p.writers().target()).isEqualTo(2);
        assertThat(p.readers().target()).isEqualTo(4);
        // below the floor (pods lost, a rollout): raised at once, no status needed
        p = Autoscaler.plan(c, 1, 1, null, T0);
        assertThat(p.writers().target()).isEqualTo(2);
        assertThat(p.readers().target()).isEqualTo(4);
        assertThat(p.writers().proposalId()).isNull();
        // within the range: no proposal is applied, the records and the reshard list stand
        p = Autoscaler.plan(c, 3, 5, null, T0);
        assertThat(p.writers().target()).isEqualTo(3);
        assertThat(p.readers().target()).isEqualTo(5);
        assertThat(p.readers().reason()).isEqualTo("no pharos node answered; no proposal applied");
        assertThat(p.proposals()).containsExactly(seen);
        assertThat(p.reshardProposals()).containsExactly(reshard);
        assertThat(p.lowDisk()).isNull();
        assertThat(p.notices()).isEmpty();
        // above max: readers are cut to it; a writer is only removed once its drain can be checked
        p = Autoscaler.plan(c, 6, 9, null, T0);
        assertThat(p.readers().target()).isEqualTo(6);
        assertThat(p.writers().target()).isEqualTo(6);
        assertThat(p.writers().reason()).contains("no pharos node answered");
    }

    @Test
    void readerScaleDownOneStepPerCooldown() {
        PharosCluster c = readers(1, 8);
        Proposal remove = removeReaders(-3);
        assertThat(step(c, 1, 4, reading(T0, remove), T0).readers().target()).isEqualTo(3);
        assertThat(step(c, 1, 3, reading(T0.plusSeconds(100), remove), T0.plusSeconds(100)).readers().target())
                .isEqualTo(3);
        assertThat(step(c, 1, 3, reading(T0.plusSeconds(300), remove), T0.plusSeconds(300)).readers().target())
                .isEqualTo(2);
        // a remove right after an add waits too: one change per role per cooldown
        PharosCluster d = readers(1, 8);
        assertThat(step(d, 1, 2, reading(T0, addReaders(1, S1)), T0).readers().target()).isEqualTo(3);
        assertThat(step(d, 1, 3, reading(T0.plusSeconds(60), remove), T0.plusSeconds(60)).readers().target())
                .isEqualTo(3);
    }

    @Test
    void reshardProposalSurfacedNeverApplied() {
        PharosCluster c = cluster(1, 4, 1, 4, null);
        Proposal reshard = new Proposal("rs", "reshard", "lucene", "size above half the cache cap",
                Map.of("sizeBytes", 123456789L, "ratio", 0.62), "split lucene into 2 shards", null, S1);
        Autoscaler.Plan p = step(c, 2, 2, reading(T0, reshard), T0);
        assertThat(p.writers().target()).isEqualTo(2);
        assertThat(p.readers().target()).isEqualTo(2);
        assertThat(p.proposals()).isNull();
        assertThat(p.reshardProposals()).singleElement().satisfies(r -> {
            assertThat(r.id()).isEqualTo("rs");
            assertThat(r.target()).isEqualTo("lucene");
            assertThat(r.reason()).isEqualTo("size above half the cache cap");
            assertThat(r.action()).isEqualTo("split lucene into 2 shards");
            assertThat(r.since()).isEqualTo(S1);
            assertThat(r.metrics().get("sizeBytes").asLong()).isEqualTo(123456789L);
            assertThat(r.metrics().get("ratio").asDouble()).isEqualTo(0.62);
        });
        assertThat(p.notices()).singleElement().satisfies(n -> {
            assertThat(n.reason()).isEqualTo(Autoscaler.EVENT_RESHARD);
            assertThat(n.message()).contains("lucene", "never applied");
        });
        // seen before: no second Event; cleared: the list empties
        assertThat(step(c, 2, 2, reading(T0.plusSeconds(60), reshard), T0.plusSeconds(60)).notices()).isEmpty();
        assertThat(step(c, 2, 2, reading(T0.plusSeconds(120)), T0.plusSeconds(120)).reshardProposals()).isNull();
    }

    @Test
    void writerDiskRaisesConditionOnly() {
        PharosCluster c = cluster(1, 4, 1, 4, null);
        Proposal disk = new Proposal("wd", "writer-disk", "demo-writers-1", "12% free", Map.of(),
                "free disk on demo-writers-1", null, S1);
        Autoscaler.Plan p = step(c, 2, 2, reading(T0, disk), T0);
        assertThat(p.writers().target()).isEqualTo(2);
        assertThat(p.readers().target()).isEqualTo(2);
        assertThat(p.lowDisk()).containsExactly("writer demo-writers-1: 12% free");
        assertThat(p.proposals()).containsExactly(
                new ProposalStatus("wd", "writer-disk", "demo-writers-1", S1, null, null, null, null));
        assertThat(p.notices()).singleElement().satisfies(n -> {
            assertThat(n.reason()).isEqualTo(Autoscaler.EVENT_WRITER_DISK);
            assertThat(n.type()).isEqualTo("Warning");
        });
        assertThat(step(c, 2, 2, reading(T0.plusSeconds(60), disk), T0.plusSeconds(60)).notices()).isEmpty();
        p = step(c, 2, 2, reading(T0.plusSeconds(120)), T0.plusSeconds(120));
        assertThat(p.lowDisk()).isEmpty();
        assertThat(p.proposals()).isNull();
    }

    @Test
    void disabledAutoscalingAppliesNoProposalButKeepsTheRange() {
        PharosCluster c = cluster(1, 4, 2, 6, new Autoscaling(false, null, null, null, null,
                List.of(new ScheduleEntry("0 8 * * *", null, new Floor(5), null))));
        Autoscaler.Plan p = Autoscaler.plan(c, 2, 3, reading(T0, addReaders(1, S1)), T0);
        assertThat(p.readers().target()).isEqualTo(3); // no proposal, no schedule floor
        assertThat(p.readers().reason()).isEqualTo("autoscaling is disabled");
        assertThat(Autoscaler.plan(c, 2, 1, null, T0).readers().target()).isEqualTo(2);
    }

    private static Proposal removeReaders(Integer delta) {
        return proposal(REMOVE_READERS_ID, "remove-readers", "readers", delta, S1);
    }

    private static Autoscaler.Decision decide(Role role, int current, Bounds b, Proposal p) {
        return Autoscaler.decide(role, current, b, List.of(p), AppliedHistory.EMPTY, T0);
    }
}
