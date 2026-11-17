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
    private final InMemoryObjectStore mem = new InMemoryObjectStore(bucketClock);
    private final java.util.concurrent.atomic.AtomicBoolean failPuts = new java.util.concurrent.atomic.AtomicBoolean();
    /** The in-memory bucket, whose {@code put} can be made to fail. */
    private final com.pharos.cluster.objectstore.ObjectStore store = (com.pharos.cluster.objectstore.ObjectStore)
            java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{com.pharos.cluster.objectstore.ObjectStore.class}, (proxy, m, args) -> {
                        if (m.getName().equals("put") && failPuts.get()) throw new java.io.IOException("injected put failure");
                        try {
                            return m.invoke(mem, args);
                        } catch (java.lang.reflect.InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
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
                shards, draining[0], Map.of("mode", "lazy"), 2, 1.5, 0L));
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
        assertThat(a.liveWithAge().get(0).ageSeconds()).isZero();
        assertThat(a.live().get(0).startedAt()).isPositive();
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
        for (int i = 0; i < 200 && store.get("pharos/catalog/readers/a.json").isEmpty(); i++) Thread.sleep(25);
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

    @Test
    void ownWriteFailureDoesNotFreezeStalePeers() throws Exception {
        ReaderRegistry a = registry("a", List.of("p1"));
        ReaderRegistry b = registry("b", List.of("p1"));
        a.tick();
        b.tick();
        a.tick();
        assertThat(a.live()).hasSize(2);

        bucketClock.advance(60); // b is dead
        ReaderRegistry c = registry("c", List.of("p1"));
        c.tick(); // a fresh peer makes the bucket's newest object far newer than our own
        failPuts.set(true);
        a.tick(); // cannot tell dead peers from an unreachable bucket: previous list kept
        assertThat(a.live()).extracting(ReaderHeartbeat::node).containsExactly("a", "b");

        failPuts.set(false);
        a.tick(); // recovered: judged against our own fresh write
        assertThat(a.live()).extracting(ReaderHeartbeat::node).containsExactly("a", "c");
    }

    @Test
    void allPeersDeadExpireRelativeToOwnWrite() throws Exception {
        ReaderRegistry a = registry("a", List.of("p1"));
        ReaderRegistry b = registry("b", List.of("p1"));
        a.tick();
        bucketClock.advance(40);
        b.tick(); // newer than our own write
        a.refresh(); // no write: "now" is our own LastModified, not the newest object in the bucket
        assertThat(a.live()).extracting(ReaderHeartbeat::node).containsExactly("a", "b");

        bucketClock.advance(100);
        a.tick();
        assertThat(a.live()).extracting(ReaderHeartbeat::node).containsExactly("a");
    }

    @Test
    void tickRacingCloseLeavesNoObject() throws Exception {
        for (int i = 0; i < 200; i++) {
            ReaderRegistry a = registry("a", List.of("p1"));
            java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
            Thread t = new Thread(() -> {
                try { go.await(); } catch (InterruptedException ignored) { }
                a.tick();
            });
            t.start();
            go.countDown();
            a.close();
            t.join();
            assertThat(store.get("pharos/catalog/readers/a.json")).as("iteration " + i).isEmpty();
        }
    }

    @Test
    void storedHeartbeatHasNoAgeButHasStartedAt() throws Exception {
        ReaderRegistry a = registry("a", List.of("p1"));
        a.tick();
        String json = new String(store.get("pharos/catalog/readers/a.json").orElseThrow().data());
        assertThat(json).contains("\"startedAt\"").doesNotContain("ageSeconds");
    }
}
