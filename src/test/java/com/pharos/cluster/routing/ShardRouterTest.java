package com.pharos.cluster.routing;

import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.ReaderHeartbeat;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class ShardRouterTest {

    private final AtomicLong nanos = new AtomicLong(1_000_000_000L);
    private final ReaderHealth health = new ReaderHealth(30, nanos::get);
    private List<ReaderHeartbeat> live = new ArrayList<>();
    private List<ShardId> catalogShards = List.of(new ShardId("a"), new ShardId("b"));
    private final ShardCatalog catalog = () -> catalogShards;

    private static ReaderHeartbeat hb(String node, boolean draining, int inFlight, String... shards) {
        return new ReaderHeartbeat(node, "http://" + node, List.of(shards), draining, Map.of(), inFlight, 0, 0L);
    }

    private ShardRouter router() {
        return new ShardRouter(() -> live, catalog, health, s -> s.value().equals("a"));
    }

    private static List<String> nodes(ShardRouter.Route r) {
        return r.candidates().stream().map(ReaderHeartbeat::node).toList();
    }

    @Test
    void routesOnlyToReadersServingTheShard() {
        live = List.of(hb("r1", false, 0, "a"), hb("r2", false, 0, "b"), hb("r3", false, 0, "a", "b"));
        List<ShardRouter.Route> routes = router().route(List.of());
        assertThat(routes).extracting(r -> r.shard().value()).containsExactly("a", "b");
        assertThat(nodes(routes.get(0))).containsExactlyInAnyOrder("r1", "r3");
        assertThat(nodes(routes.get(1))).containsExactlyInAnyOrder("r2", "r3");
        assertThat(router().route(List.of("b"))).hasSize(1);
    }

    @Test
    void skipsDrainingAndSuspect() {
        live = List.of(hb("r1", true, 0, "a"), hb("r2", false, 0, "a"), hb("r3", false, 0, "a"));
        health.markSuspect("r3");
        assertThat(nodes(router().route(List.of("a")).get(0))).containsExactly("r2");
    }

    @Test
    void successClearsSuspect() {
        live = List.of(hb("r1", false, 0, "a"));
        health.markSuspect("r1");
        assertThat(router().route(List.of("a")).get(0).candidates()).isEmpty();
        health.markHealthy("r1");
        assertThat(router().route(List.of("a")).get(0).candidates()).hasSize(1);
    }

    @Test
    void spreadsAcrossReplicas() {
        live = List.of(hb("r1", false, 0, "a"), hb("r2", false, 0, "a"), hb("r3", false, 0, "a"));
        ShardRouter router = router();
        Map<String, Integer> first = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            first.merge(router.route(List.of("a")).get(0).candidates().get(0).node(), 1, Integer::sum);
        }
        assertThat(first).hasSize(3);
        assertThat(first.values()).allSatisfy(n -> assertThat(n).isBetween(300, 370));
    }

    @Test
    void leastInFlightFirst() {
        live = List.of(hb("r1", false, 5, "a"), hb("r2", false, 1, "a"), hb("r3", false, 1, "a"));
        for (int i = 0; i < 10; i++) {
            List<String> n = nodes(router().route(List.of("a")).get(0));
            assertThat(n.get(2)).isEqualTo("r1");
        }
    }

    @Test
    void suspectClearsAfterTtl() {
        live = List.of(hb("r1", false, 0, "a"));
        health.markSuspect("r1");
        nanos.addAndGet(29_000_000_000L);
        assertThat(router().route(List.of("a")).get(0).candidates()).isEmpty();
        nanos.addAndGet(2_000_000_000L);
        assertThat(router().route(List.of("a")).get(0).candidates()).hasSize(1);
    }

    @Test
    void noLiveReaderForShardYieldsEmptyCandidates() {
        live = List.of(hb("r1", false, 0, "b"));
        List<ShardRouter.Route> routes = router().route(List.of("a"));
        assertThat(routes).hasSize(1);
        assertThat(routes.get(0).candidates()).isEmpty();
    }

    @Test
    void servesLocallyDelegates() {
        assertThat(router().servesLocally(new ShardId("a"))).isTrue();
        assertThat(router().servesLocally(new ShardId("b"))).isFalse();
    }
}
