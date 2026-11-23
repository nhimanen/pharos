package com.pharos.operator.scale;

import com.pharos.cluster.status.ClusterStatus;
import com.pharos.cluster.status.ClusterStatus.Proposal;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.status.StatusReading;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.pharos.operator.scale.AutoscalerTest.S1;
import static com.pharos.operator.scale.AutoscalerTest.T0;
import static com.pharos.operator.scale.AutoscalerTest.cluster;
import static com.pharos.operator.scale.AutoscalerTest.status;
import static com.pharos.operator.scale.AutoscalerTest.step;
import static org.assertj.core.api.Assertions.assertThat;

class ScaleDownGateTest {

    static final Proposal REMOVE_WRITERS = new Proposal("rw", "remove-writers", "writers", "idle", Map.of(),
            "stop 2 writers", -2, S1);

    static ClusterStatus.Writer writer(String node, boolean stopping, String... leases) {
        return new ClusterStatus.Writer(node, true, List.of(leases), List.of(), null, 0L, stopping, null, 3L);
    }

    static ClusterStatus.Shard shard(String name, String holder, boolean expired, boolean released) {
        return new ClusterStatus.Shard(name, 1, Map.of("project", name), 10L, 100L, 3L, 5L, holder,
                holder == null ? null : new ClusterStatus.Lease(holder, 5, expired, released), List.of(), null,
                "current");
    }

    static StatusReading reading(Instant at, List<ClusterStatus.Writer> writers, List<ClusterStatus.Shard> shards) {
        return new StatusReading(status(List.of(REMOVE_WRITERS), writers, shards), at, "10.0.0.1:7070");
    }

    @Test
    void writerScaleDownWaitsForHeartbeatAndLeaseRelease() {
        PharosCluster c = cluster(1, 4, 1, 1, null);
        var w0 = writer("demo-writers-0", false, "a");
        var w1 = writer("demo-writers-1", false, "b");

        // first step: one writer, although pharos asks for two (nothing removed earlier is pending)
        Autoscaler.Plan p = step(c, 3, 1, reading(T0, List.of(w0, w1, writer("demo-writers-2", false, "p")),
                List.of(shard("p", "demo-writers-2", false, false))), T0);
        assertThat(p.writers().target()).isEqualTo(2);
        assertThat(p.writers().proposalId()).isEqualTo("rw");

        // after the cooldown, still draining: its heartbeat (stopping, holding p) is there
        Instant t1 = T0.plusSeconds(300);
        p = step(c, 2, 1, reading(t1, List.of(w0, w1, writer("demo-writers-2", true, "p")),
                List.of(shard("p", "demo-writers-2", false, false))), t1);
        assertThat(p.writers().target()).isEqualTo(2);
        assertThat(p.writers().proposalId()).isNull();
        assertThat(p.writers().reason()).contains("demo-writers-2 still has a heartbeat (leases [p])");

        // heartbeat gone, but the lease on p is neither released nor expired
        Instant t2 = T0.plusSeconds(360);
        p = step(c, 2, 1, reading(t2, List.of(w0, w1), List.of(shard("p", "demo-writers-2", false, false))), t2);
        assertThat(p.writers().target()).isEqualTo(2);
        assertThat(p.writers().reason()).contains("demo-writers-2 still holds the lease of shard p");

        // released: the next writer may go
        Instant t3 = T0.plusSeconds(420);
        p = step(c, 2, 1, reading(t3, List.of(w0, w1), List.of(shard("p", "demo-writers-2", false, true))), t3);
        assertThat(p.writers().target()).isEqualTo(1);
        assertThat(p.writers().proposalId()).isEqualTo("rw");
    }

    @Test
    void gateChecksOnlyRemovedOrdinals() {
        ClusterStatus s = status(List.of(), List.of(writer("demo-writers-0", false, "a"),
                writer("demo-writers-10", false)), List.of(shard("x", "demo-writers-1", false, false),
                shard("y", "demo-writers-2", true, false), shard("z", "other-writers-5", false, false)));
        // replicas 2: ordinals >= 2 are removed; writers-10 has a heartbeat, writers-2's lease is expired (free)
        assertThat(ScaleDownGate.writerBlocker("demo-writers", 2, s)).isEqualTo("writer demo-writers-10 still has a heartbeat");
        assertThat(ScaleDownGate.writerBlocker("demo-writers", 11, s)).isNull();
        assertThat(ScaleDownGate.writerBlocker("demo-writers", 1, s))
                .contains("demo-writers-1 still holds the lease of shard x");
        assertThat(ScaleDownGate.writerBlocker("demo-writers", 2, null)).contains("no pharos node answered");
        assertThat(ScaleDownGate.removed("demo-writers", 2, "demo-writers-x")).isFalse();
        assertThat(ScaleDownGate.removed("demo-writers", 2, "demo-writers-2")).isTrue();
    }
}
