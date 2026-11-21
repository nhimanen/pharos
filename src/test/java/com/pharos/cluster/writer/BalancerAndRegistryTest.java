package com.pharos.cluster.writer;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class BalancerAndRegistryTest {

    @Test
    void shareIsShardsOverLiveWritersRoundedUp() {
        assertThat(Balancer.share(4, 2)).isEqualTo(2);
        assertThat(Balancer.share(5, 2)).isEqualTo(3);
        assertThat(Balancer.share(1, 3)).isEqualTo(1);
        assertThat(Balancer.share(0, 3)).isZero();
        assertThat(Balancer.share(3, 0)).isEqualTo(3);
        assertThat(Balancer.wanted(2, 3)).isZero();
    }

    @Test
    void releaseNeedsSurplusMinHoldAndNoPin() {
        Balancer b = new Balancer(Duration.ofSeconds(120));
        Instant t0 = Instant.parse("2026-11-21T10:00:00Z");
        assertThat(b.mayRelease(3, 2, false, t0, t0.plusSeconds(119))).isFalse();
        assertThat(b.mayRelease(3, 2, false, t0, t0.plusSeconds(120))).isTrue();
        assertThat(b.mayRelease(2, 2, false, t0, t0.plusSeconds(500))).isFalse();
        assertThat(b.mayRelease(3, 2, true, t0, t0.plusSeconds(500))).isFalse();
    }

    @Test
    void acquireOrderDiffersPerWriterAndSkipsHeld() {
        List<ShardId> all = List.of(new ShardId("a"), new ShardId("b"), new ShardId("c"), new ShardId("d"));
        List<ShardId> x = Balancer.acquireOrder("node-x", all, Set.of(new ShardId("a")));
        assertThat(x).hasSize(3).doesNotContain(new ShardId("a"));
        assertThat(Balancer.acquireOrder("node-x", all, Set.of())).isEqualTo(Balancer.acquireOrder("node-x", all, Set.of()));
        boolean differs = false;
        for (int i = 0; i < 10 && !differs; i++) {
            differs = !Balancer.acquireOrder("node-" + i, all, Set.of()).equals(Balancer.acquireOrder("node-x", all, Set.of()));
        }
        assertThat(differs).isTrue();
    }

    @Test
    void registryLivenessUsesTheBucketClockAndCloseDeletes() throws Exception {
        WorkLoopTest.TestClock clock = new WorkLoopTest.TestClock();
        InMemoryObjectStore store = new InMemoryObjectStore(clock);
        ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
        Duration ttl = Duration.ofSeconds(30);
        WriterRegistry a = new WriterRegistry(store, layout, "a", ttl, Duration.ofSeconds(10), () -> hb("a"));
        WriterRegistry b = new WriterRegistry(store, layout, "b", ttl, Duration.ofSeconds(10), () -> hb("b"));
        b.beat();
        a.beat(); // a's list is refreshed by a's own beat
        assertThat(a.live()).extracting(l -> l.heartbeat().node()).containsExactly("a", "b");
        assertThat(a.bucketNow()).contains(clock.instant());
        clock.advance(31);
        a.beat(); // b's heartbeat is now more than the TTL older than ours
        assertThat(a.live()).extracting(l -> l.heartbeat().node()).containsExactly("a");
        b.beat();
        a.beat();
        assertThat(a.live()).hasSize(2);
        b.close();
        a.beat();
        assertThat(a.live()).extracting(l -> l.heartbeat().node()).containsExactly("a");
        assertThat(store.get(layout.writerKey("b"))).isEmpty();
        a.close();
    }

    private static WriterHeartbeat hb(String node) {
        return new WriterHeartbeat(node, true, List.of(), List.of(), Map.of(), null, 0L, false);
    }
}
