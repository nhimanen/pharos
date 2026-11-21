package com.pharos.cluster.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.writer.WriterHeartbeat;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static com.pharos.cluster.status.StatusFixtures.T0;
import static org.assertj.core.api.Assertions.assertThat;

/** Task 5: status from the bucket, identical across nodes; CAS races; single node. */
class StatusServiceTest {

    static final ObjectMapper JSON = new ObjectMapper();

    final StatusFixtures.MutableClock clock = new StatusFixtures.MutableClock(T0);
    final InMemoryObjectStore store = new InMemoryObjectStore(clock);
    final ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
    final List<AutoCloseable> open = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = open.size() - 1; i >= 0; i--) open.get(i).close();
    }

    /** A bucket with shards a (published, held by w1) and b (unpublished), one writer and two readers. */
    void seedBucket() throws Exception {
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        catalog.register(new ShardId("a"), Optional.of(ShardSource.path("/src/a")));
        catalog.register(new ShardId("b"), Optional.of(ShardSource.path("/src/b")));
        publish("a", 4, 1234, 5678, "w1", "rev-1");
        clock.advance(Duration.ofSeconds(5));
        lease("a", "w1");
        lease("b", "w1");
        WriterHeartbeat w1 = new WriterHeartbeat("w1", true, List.of("a", "b"), List.of(),
                Map.of("a", new WriterHeartbeat.Backlog("rev-2", "rev-1", true, 42L),
                        "b", new WriterHeartbeat.Backlog("rev-9", null, true, 7L)),
                new WriterHeartbeat.Indexing("a", WriterHeartbeat.EMBED, 0.25, 1L), 1L, false,
                new WriterHeartbeat.Disk(50L << 30, 100L << 30, false));
        store.put(layout.writerKey("w1"), JSON.writeValueAsBytes(w1));
        store.put(layout.readerKey("r1"), JSON.writeValueAsBytes(StatusFixtures.reader("r1", 12.5, 40L, List.of("a"))));
        clock.advance(Duration.ofSeconds(2));
        store.put(layout.readerKey("r2"), JSON.writeValueAsBytes(StatusFixtures.reader("r2", 3, null, List.of("a"))));
    }

    void publish(String shard, long gen, long docs, long size, String writer, String rev) throws Exception {
        ShardId id = new ShardId(shard);
        ProjectMeta meta = new ProjectMeta();
        meta.setName(shard);
        meta.setSourceRevision(rev);
        ShardManifest m = new ShardManifest(ShardManifest.FORMAT_VERSION, shard, gen, gen - 1, clock.instant(), writer,
                docs, size, "fp", List.of(new ManifestFile(ManifestFile.LUCENE, "segments_1",
                        layout.shardPrefix(id) + "lucene/segments_1@00", 10, "00")), meta, null);
        store.put(layout.manifestKey(id, new Generation(gen)), m.toJson());
        store.put(layout.latestKey(id), ("{\"generation\":" + gen + ",\"manifest\":\"gen-" + gen + ".json\"}")
                .getBytes(StandardCharsets.UTF_8));
    }

    void lease(String shard, String writer) throws Exception {
        store.put(layout.leaseKey(new ShardId(shard)), ("{\"writer\":\"" + writer + "\",\"expires\":\"x\"}")
                .getBytes(StandardCharsets.UTF_8));
    }

    StatusService node(StatusEvents events) {
        StatusService s = new StatusService(
                new BucketStatusSource(store, layout, new ObjectStoreCatalog(store, layout),
                        new ObjectStoreReplicaSource(store, layout), Duration.ofSeconds(30), Duration.ofSeconds(30),
                        clock),
                StatusStateStore.bucket(store, layout), StatusSettings.defaults(), events, Duration.ofHours(1),
                Duration.ZERO, System::nanoTime);
        open.add(s);
        return s;
    }

    @Test
    void statusIsIdenticalAcrossNodes() throws Exception {
        seedBucket();
        StatusService a = node(null);
        StatusService b = node(null);
        clock.advance(Duration.ofSeconds(3)); // node clocks differ from the bucket's "now": irrelevant
        byte[] fromA = a.currentJson();
        clock.advance(Duration.ofSeconds(4));
        byte[] fromB = b.currentJson();
        assertThat(new String(fromB, StandardCharsets.UTF_8)).isEqualTo(new String(fromA, StandardCharsets.UTF_8));

        ClusterStatus st = a.current();
        assertThat(st.mode()).isEqualTo("cluster");
        assertThat(st.asOf()).as("the newest LastModified read: r2's heartbeat").isEqualTo(T0.plusSeconds(7).toString());
        assertThat(st.layout().summary()).isEqualTo(new StatusState.Summary(2, 1, 2));
        ClusterStatus.Shard sa = st.layout().shards().get(0);
        assertThat(sa.shard()).isEqualTo("a");
        assertThat(sa.specVersion()).isEqualTo(1);
        assertThat(sa.keys()).isEqualTo(Map.of("project", "a"));
        assertThat(sa.docCount()).isEqualTo(1234L);
        assertThat(sa.sizeBytes()).isEqualTo(5678L);
        assertThat(sa.generation()).isEqualTo(4L);
        assertThat(sa.writer()).isEqualTo("w1");
        assertThat(sa.ageSeconds()).isEqualTo(7L);
        assertThat(sa.readers()).containsExactly("r1", "r2");
        assertThat(sa.state()).isEqualTo("lagging");
        // w1's heartbeat was written at T0+5 with an age of 42 s: first seen at T0-37, 44 s before asOf
        assertThat(sa.backlog()).isEqualTo(new ClusterStatus.Backlog("rev-2", "rev-1", true, 44L));
        ClusterStatus.Shard sb = st.layout().shards().get(1);
        assertThat(sb.generation()).isNull();
        assertThat(sb.readers()).isEmpty();
        assertThat(st.backlogSummary()).isEqualTo(new ClusterStatus.BacklogSummary(2, 44L));
        assertThat(st.layout().writers()).singleElement().satisfies(w -> {
            assertThat(w.indexing().phase()).isEqualTo("embed");
            assertThat(w.heartbeatAgeSeconds()).isEqualTo(2L);
        });
        assertThat(st.layout().readers()).extracting(ClusterStatus.Reader::node).containsExactly("r1", "r2");
        assertThat(st.layout().readers().get(0).warmP95Millis()).isEqualTo(40L);
        assertThat(st.layout().readers().get(0).catalogAgeSeconds()).isEqualTo(3L);
        assertThat(st.layout().readers().get(0).readerCache()).containsEntry("mode", "lazy");

        // the state both nodes agree on is in the bucket
        assertThat(store.get(layout.statusStateKey())).isPresent();
        // refreshes with nothing new rewrite nothing and keep the bytes
        String etag = store.get(layout.statusStateKey()).orElseThrow().etag();
        assertThat(a.refresh().toJson()).isEqualTo(fromA);
        assertThat(store.get(layout.statusStateKey()).orElseThrow().etag()).isEqualTo(etag);
    }

    @Test
    void racingNodesAgreeOnTheSharedState() throws Exception {
        seedBucket();
        List<StatusService> nodes = new ArrayList<>();
        for (int i = 0; i < 6; i++) nodes.add(node(null));
        // a hot reader tier: every node races to record the add-readers condition
        store.put(layout.readerKey("r1"), JSON.writeValueAsBytes(StatusFixtures.reader("r1", 90, 40L, List.of("a"))));
        for (int round = 0; round < 40; round++) {
            clock.advance(Duration.ofSeconds(10));
            store.put(layout.readerKey("r1"), JSON.writeValueAsBytes(StatusFixtures.reader("r1", 90, 40L, List.of("a"))));
            store.put(layout.readerKey("r2"), JSON.writeValueAsBytes(StatusFixtures.reader("r2", 90, null, List.of("a"))));
            store.put(layout.writerKey("w1"), store.get(layout.writerKey("w1")).orElseThrow().data());
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> ts = new ArrayList<>();
            for (StatusService n : nodes) {
                ts.add(Thread.ofVirtual().start(() -> {
                    try {
                        go.await();
                        n.refresh();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            go.countDown();
            for (Thread t : ts) t.join();
        }
        byte[] first = nodes.get(0).refresh().toJson();
        for (StatusService n : nodes) assertThat(n.refresh().toJson()).isEqualTo(first);
        ClusterStatus st = nodes.get(0).current();
        assertThat(st.proposals()).extracting(ClusterStatus.Proposal::kind).contains(Proposals.ADD_READERS);
        ClusterStatus.Proposal p = st.proposals().stream().filter(x -> x.kind().equals(Proposals.ADD_READERS))
                .findFirst().orElseThrow();
        assertThat(Instant.parse(p.since())).as("the first round's sighting, not reset by the races")
                .isEqualTo(T0.plusSeconds(7 + 10));
    }

    @Test
    void singleNodeStatusShowsOneNode(@TempDir Path tmp) throws Exception {
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        for (String name : List.of("beta", "alpha")) {
            ProjectMeta m = new ProjectMeta();
            m.setName(name);
            m.setRootPath(tmp.resolve(name).toString());
            m.setIndexPath(tmp.resolve("idx").resolve(name).toString());
            m.setLastIndexed(T0.minusSeconds(30));
            registry.register(m);
        }
        com.pharos.cluster.membership.LoadCounter load = new com.pharos.cluster.membership.LoadCounter();
        StatusService s = new StatusService(new LocalStatusSource("local", registry, null, () -> load, clock),
                StatusStateStore.memory(), StatusSettings.defaults(), null, Duration.ofHours(1), Duration.ZERO,
                System::nanoTime);
        open.add(s);
        ClusterStatus st = s.refresh();
        assertThat(st.mode()).isEqualTo("single-node");
        assertThat(st.layout().summary()).isEqualTo(new StatusState.Summary(2, 1, 1));
        assertThat(st.layout().shards()).extracting(ClusterStatus.Shard::shard).containsExactly("alpha", "beta");
        assertThat(st.layout().shards()).allSatisfy(sh -> {
            assertThat(sh.readers()).containsExactly("local");
            assertThat(sh.writer()).isEqualTo("local");
            assertThat(sh.state()).isEqualTo("current");
            assertThat(sh.ageSeconds()).isEqualTo(30L);
        });
        assertThat(st.layout().writers()).singleElement().satisfies(w -> {
            assertThat(w.node()).isEqualTo("local");
            assertThat(w.leases()).containsExactly("alpha", "beta");
        });
        assertThat(st.layout().readers()).singleElement().satisfies(r -> assertThat(r.shards())
                .containsExactly("alpha", "beta"));
        assertThat(st.health()).isEmpty();
        assertThat(st.proposals()).isEmpty();
        assertThat(store.list("")).as("nothing written to any bucket").isEmpty();
    }

    @Test
    void failedRefreshServesThePreviousStatus() throws Exception {
        seedBucket();
        StatusService a = node(null);
        byte[] before = a.currentJson();
        store.failGetOn("catalog/shards.json");
        assertThat(a.currentJson()).isEqualTo(before);
    }
}
