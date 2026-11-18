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

    @Test
    void invalidProjectIsTyped() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> router().route(List.of("../x")))
                .isInstanceOf(InvalidProjectException.class);
    }

    @Test
    void unreadableCatalogIsTyped() {
        ShardCatalog broken = () -> { throw new java.io.IOException("boom"); };
        ShardRouter r = new ShardRouter(() -> live, broken, health, s -> true);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> r.route(List.of()))
                .isInstanceOf(CatalogUnavailableException.class);
    }

    @Test
    void coordinatorsOwnInFlightCountsInTheOrdering() {
        live = List.of(hb("r1", false, 1, "a"), hb("r2", false, 1, "a"));
        ShardRouter r = new ShardRouter(() -> live, catalog, health, s -> true, node -> node.equals("r1") ? 3 : 0);
        for (int i = 0; i < 6; i++) assertThat(nodes(r.route(List.of("a")).get(0))).containsExactly("r2", "r1");
    }

    /** The coordinator's in-flight counts change while a route is computed: one snapshot per route, never a broken grouping. */
    @Test
    void changingInFlightCountsDuringRoutingNeverBreakIt() {
        live = new ArrayList<>();
        for (int i = 0; i < 12; i++) live.add(hb("r" + i, false, 0, "b"));
        java.util.concurrent.atomic.AtomicInteger ticks = new java.util.concurrent.atomic.AtomicInteger();
        ShardRouter r = new ShardRouter(() -> live, catalog, health, s -> false,
                node -> (ticks.incrementAndGet() * 7 + node.hashCode()) % 3); // a different answer on every call
        for (int i = 0; i < 2_000; i++) {
            assertThat(r.route(List.of("b")).get(0).candidates()).hasSize(12);
        }
    }

    private static ReaderHeartbeat degraded(String node, List<String> degradedShards, String... shards) {
        return new ReaderHeartbeat(node, "http://" + node, List.of(shards), false, Map.of(), 0, 0, 0L,
                ReaderHeartbeat.DEGRADED, degradedShards);
    }

    /**
     * Phase 2 follow-up M3: one degraded shard on every reader (a rolling upgrade whose GC took the
     * generation they serve) leaves every node routable for its other shards; only the degraded
     * shard loses them as candidates, and its route names why.
     */
    @Test
    void aDegradedShardExcludesReadersForThatShardOnly() {
        live = List.of(degraded("r1", List.of("a"), "a", "b"), degraded("r2", List.of("a"), "a", "b"),
                hb("r3", false, 0, "b"));
        List<ShardRouter.Route> routes = router().route(List.of());
        assertThat(routes.get(0).shard().value()).isEqualTo("a");
        assertThat(routes.get(0).candidates()).as("every reader of a is degraded for it").isEmpty();
        assertThat(routes.get(0).degraded()).containsExactly("r1", "r2");
        assertThat(nodes(routes.get(1))).as("still routable for b").containsExactlyInAnyOrder("r1", "r2", "r3");
        assertThat(routes.get(1).degraded()).isEmpty();

        live = List.of(degraded("r1", List.of("a"), "a", "b"), hb("r2", false, 0, "a", "b"));
        assertThat(nodes(router().route(List.of("a")).get(0))).as("a healthy replica takes it").containsExactly("r2");
    }

    /** A heartbeat written by an older reader (no state, no degradedShards) still parses and routes. */
    @Test
    void anOlderHeartbeatWithoutStateParses() throws Exception {
        String json = "{\"node\":\"old\",\"advertiseUrl\":\"http://old\",\"shards\":[\"a\"],\"draining\":false,"
                + "\"inFlight\":0,\"qps\":0.0,\"startedAt\":1}";
        ReaderHeartbeat hb = new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, ReaderHeartbeat.class);
        assertThat(hb.state()).isEqualTo(ReaderHeartbeat.READY);
        assertThat(hb.degradedShards()).isEmpty();
        live = List.of(hb);
        assertThat(nodes(router().route(List.of("a")).get(0))).containsExactly("old");
        ReaderHeartbeat draining = new ReaderHeartbeat("d", "http://d", List.of("a"), true, null, 0, 0, 0L);
        assertThat(draining.state()).isEqualTo(ReaderHeartbeat.DRAINING);
    }

    /** Queries for a shard every reader is degraded for name it, and why, instead of dropping it silently. */
    @Test
    void aShardDegradedEverywhereIsNamedInTheQueryWarnings() {
        live = List.of(degraded("r1", List.of("b"), "a", "b"), degraded("r2", List.of("b"), "a", "b"));
        ShardRouter remoteOnly = new ShardRouter(() -> live, catalog, health, s -> false);
        com.pharos.search.distributed.DistributedSearch ds = new com.pharos.search.distributed.DistributedSearch(
                remoteOnly, new com.pharos.cluster.internal.ShardClient(health, "t", java.time.Duration.ofMillis(100),
                java.time.Duration.ofMillis(100)), null, null, null, java.time.Duration.ofSeconds(5), () -> -1L);
        try {
            try (var q = ds.open(List.of("b"))) {
                assertThat(q.warnings()).containsExactly(
                        "shard b unavailable: every live reader serving it is degraded (r1, r2); results exclude it");
            }
            try (var q = ds.open(List.of("a"))) {
                assertThat(q.fansOut()).isTrue();
                assertThat(q.warnings()).isEmpty();
            }
        } finally {
            ds.close();
        }
    }
}
