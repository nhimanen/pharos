package com.pharos.cluster.membership;

import com.pharos.cluster.BackendKind;
import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.ReplicationBackend;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.cluster.peer.PeerCatalog;
import com.pharos.cluster.peer.PeerHttp;
import com.pharos.cluster.peer.PeerMembership;
import com.pharos.cluster.peer.PeerWriterFixture;
import com.pharos.cluster.routing.ShardRouter;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static com.pharos.cluster.peer.PeerWriterFixture.heartbeat;
import static org.assertj.core.api.Assertions.assertThat;

/** Phase 5 Task 2b: the peer membership lists what the bucket registry lists, and the bucket path is unchanged. */
class PeerMembershipParityTest {

    @TempDir Path tmp;
    private final List<AutoCloseable> open = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : open.reversed()) {
            try {
                c.close();
            } catch (Exception e) {
                // best effort
            }
        }
    }

    private static ClusterConfig cfg(String node) {
        ClusterConfig c = PeerWriterFixture.cfg(10, 30);
        c.setRole(NodeRole.READER);
        c.setNodeId(node);
        return c;
    }

    /** Heartbeats as compared: the process start each membership stamps is its own. */
    private static List<ReaderHeartbeat> normalized(List<ReaderHeartbeat> live) {
        return live.stream().map(h -> h.with(h.draining(), 0L)).toList();
    }

    private static List<List<String>> routes(MembershipSource m) {
        ShardRouter r = new ShardRouter(m, () -> List.of(new ShardId("p1"), new ShardId("p2"), new ShardId("p3")),
                new ReaderHealth(30), id -> false, n -> 0);
        List<List<String>> out = new ArrayList<>();
        for (int round = 0; round < 3; round++) { // rotation among ties included
            for (ShardRouter.Route route : r.route(List.of())) {
                out.add(route.candidates().stream().map(ReaderHeartbeat::node).toList());
                out.add(route.degraded());
            }
        }
        return out;
    }

    @Test
    void peerMembershipMatchesBucketMembership() throws Exception {
        Map<String, ReaderHeartbeat> beats = new LinkedHashMap<>();
        beats.put("a", heartbeat("a", List.of("p1"), false, null, null, 2));
        beats.put("b", heartbeat("b", List.of("p1", "p2"), false, ReaderHeartbeat.WARMING, null, 0));
        beats.put("c", heartbeat("c", List.of("p2"), true, null, null, 0));
        beats.put("d", heartbeat("d", List.of("p1", "p2", "p3"), false, ReaderHeartbeat.DEGRADED, List.of("p2"), 0));
        beats.put("e", heartbeat("e", List.of("p1", "p3"), false, null, null, 0));

        // the bucket: heartbeat objects, liveness by the bucket's clock
        ReaderRegistryTest.TestClock bucketClock = new ReaderRegistryTest.TestClock();
        InMemoryObjectStore mem = new InMemoryObjectStore(bucketClock);
        ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
        Map<String, ReaderRegistry> bucket = new LinkedHashMap<>();
        for (String n : beats.keySet()) bucket.put(n, new ReaderRegistry(mem, layout, cfg(n), () -> beats.get(n)));
        ReaderRegistry bucketView = ReaderRegistry.observer(mem, layout, cfg("router"));

        // no bucket: the same heartbeats POSTed to two writers, liveness by each writer's clock
        PeerWriterFixture w1 = new PeerWriterFixture("w1", tmp.resolve("w1"), 30, 1_000L).start();
        PeerWriterFixture w2 = new PeerWriterFixture("w2", tmp.resolve("w2"), 30, 9_000_000L).start();
        open.add(w1);
        open.add(w2);
        PeerHttp http = PeerWriterFixture.http();
        open.add(http);
        Map<String, PeerMembership> peer = new LinkedHashMap<>();
        for (String n : beats.keySet()) {
            PeerMembership m = new PeerMembership(PeerWriterFixture.catalog(http, w1, w2), http, cfg(n),
                    () -> beats.get(n), System::nanoTime);
            open.add(m);
            peer.put(n, m);
        }
        PeerCatalog routerCatalog = PeerWriterFixture.catalog(http, w1, w2);
        PeerMembership peerView = PeerMembership.observer(routerCatalog, http, cfg("router"));
        open.add(peerView);

        bucket.values().forEach(ReaderRegistry::tick);
        peer.values().forEach(PeerMembership::tick);
        bucketView.refresh();
        peerView.tick();
        assertThat(normalized(bucketView.live())).hasSize(5);
        assertThat(normalized(peerView.live())).isEqualTo(normalized(bucketView.live()));
        assertThat(routes(peerView)).isEqualTo(routes(bucketView));

        // a stops beating: one window later it is gone from both (the others beat again)
        bucketClock.advance(31);
        w1.advance(31);
        w2.advance(31);
        for (String n : List.of("b", "c", "d", "e")) {
            bucket.get(n).tick();
            peer.get(n).tick();
        }
        bucketView.refresh();
        peerView.tick();
        assertThat(normalized(bucketView.live())).extracting(ReaderHeartbeat::node).containsExactly("b", "c", "d", "e");
        assertThat(normalized(peerView.live())).isEqualTo(normalized(bucketView.live()));
        assertThat(routes(peerView)).isEqualTo(routes(bucketView));

        // e closes: it leaves both at once
        bucket.get("e").close();
        peer.get("e").close();
        bucketView.refresh();
        peerView.tick();
        assertThat(normalized(bucketView.live())).extracting(ReaderHeartbeat::node).containsExactly("b", "c", "d");
        assertThat(normalized(peerView.live())).isEqualTo(normalized(bucketView.live()));
        assertThat(routes(peerView)).isEqualTo(routes(bucketView));
    }

    @Test
    void objectStoreMembershipUnchanged() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        IndexConfig config = ClusterFixture.config(tmp.resolve("reader"), NodeRole.READER, "bucket");
        ReplicationBackend backend = TestBackends.objectStore(config, store);
        ProjectRegistry registry = ClusterRuntime.registryFor(config);
        LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene);
        open.add(() -> {
            runtime.close();
            lucene.close();
        });
        assertThat(backend.kind()).isEqualTo(BackendKind.OBJECT_STORE);
        assertThat(runtime.replicationEndpoint()).isEmpty();

        runtime.beginServing(java.util.concurrent.CompletableFuture.completedFuture(null), true);
        String key = "pharos/catalog/readers/reader-1.json";
        await("the heartbeat object in the bucket", () -> exists(store, key));
        await("the reader in its own membership", () -> !runtime.liveReaders().isEmpty());
        assertThat(runtime.membership().live()).extracting(ReaderHeartbeat::node).containsExactly("reader-1");

        // status: from the bucket, its state in catalog/status/state.json
        assertThat(runtime.statusService().refresh().mode()).isEqualTo("cluster");
        assertThat(exists(store, "pharos/catalog/status/state.json")).isTrue();

        runtime.drain();
        assertThat(new String(store.get(key).orElseThrow().data())).contains("\"draining\":true");
        runtime.close();
        assertThat(exists(store, key)).isFalse();
    }

    private static boolean exists(InMemoryObjectStore store, String key) {
        return store.get(key).isPresent();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(50);
        }
    }
}
