package com.pharos.cluster.membership;

import com.pharos.cluster.NodeRole;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.config.ClusterConfig;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ReaderRegistryTest {

    /** The bucket's clock: only the test moves it. */
    static final class TestClock extends Clock {
        final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-17T10:00:00Z"));
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId z) { return this; }
        @Override public Instant instant() { return now.get(); }
        void advance(long seconds) { now.updateAndGet(i -> i.plusSeconds(seconds)); }
    }

    private final TestClock bucketClock = new TestClock();
    private final InMemoryObjectStore store = new InMemoryObjectStore(bucketClock);
    private final ObjectStoreLayout layout = new ObjectStoreLayout("pharos");

    private ClusterConfig cfg(String node, List<String> pinned) {
        ClusterConfig c = new ClusterConfig();
        c.setRole(NodeRole.READER);
        c.setNodeId(node);
        c.setAdvertiseUrl("http://" + node + ":7171");
        c.setToken("secret");
        c.setHeartbeatSeconds(10);
        c.setReaderTtlSeconds(30);
        c.getReader().setShards(pinned);
        return c;
    }

    private ReaderRegistry registry(String node, List<String> shards, boolean[] draining) {
        ClusterConfig c = cfg(node, List.of());
        return new ReaderRegistry(store, layout, c, () -> new ReaderHeartbeat(node, c.getAdvertiseUrl(),
                shards, draining[0], Map.of("mode", "lazy"), 2, 1.5, 0));
    }

    private ReaderRegistry registry(String node, List<String> shards) {
        return registry(node, shards, new boolean[]{false});
    }

    @Test
    void heartbeatAppearsAndRefreshes() throws Exception {
        ReaderRegistry a = registry("a", List.of("p1"));
        a.tick();
        assertThat(store.get("pharos/catalog/readers/a.json")).isPresent();
        assertThat(a.live()).extracting(ReaderHeartbeat::node).containsExactly("a");
        assertThat(a.live().get(0).inFlight()).isEqualTo(2);

        bucketClock.advance(20);
        a.tick(); // rewrites, so LastModified moves
        assertThat(a.live()).hasSize(1);
        assertThat(a.live().get(0).ageSeconds()).isZero();
        a.close();
    }

    @Test
    void expiredByStoreTimeNotNodeClock() throws Exception {
        ReaderRegistry a = registry("a", List.of("p1"));
        ReaderRegistry b = registry("b", List.of("p1"));
        a.tick();
        b.tick();
        a.tick();
        assertThat(a.live()).hasSize(2);

        // The bucket's clock moves on and only a keeps beating. b's own payload still looks fresh
        // (nothing in it is consulted): only the object's LastModified counts.
        bucketClock.advance(60);
        a.tick();
        assertThat(a.live()).extracting(ReaderHeartbeat::node).containsExactly("a");
        assertThat(store.get("pharos/catalog/readers/b.json")).isPresent();
    }

    @Test
    void drainingIsVisible() throws Exception {
        boolean[] draining = {false};
        ReaderRegistry a = registry("a", List.of("p1"), draining);
        ReaderRegistry watcher = registry("w", List.of("p1"));
        a.tick();
        watcher.tick();
        assertThat(watcher.live()).filteredOn(h -> h.node().equals("a")).allMatch(h -> !h.draining());

        draining[0] = true;
        a.drain(); // writes at once
        watcher.tick();
        assertThat(watcher.live()).filteredOn(h -> h.node().equals("a")).allMatch(ReaderHeartbeat::draining)
                .hasSize(1);
    }

    @Test
    void closeRemovesHeartbeat() throws Exception {
        ReaderRegistry a = registry("a", List.of("p1"));
        a.start();
        assertThat(store.get("pharos/catalog/readers/a.json")).isPresent();
        a.close();
        assertThat(store.get("pharos/catalog/readers/a.json")).isEmpty();
    }

    @Test
    void pinnedShardsAdvertised() throws Exception {
        ReaderRegistry a = registry("a", List.of("p1", "p2"));
        a.tick();
        assertThat(a.live().get(0).shards()).containsExactly("p1", "p2");
    }
}
