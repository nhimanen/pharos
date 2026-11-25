package com.pharos.cluster.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.cluster.writer.WriterHeartbeat;
import com.pharos.config.ProjectMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.pharos.cluster.status.StatusFixtures.T0;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6 Task 5: a reshard in {@code /cluster/status} and on {@code /cluster/events}, read from the bucket
 * exactly as a node reads it (catalog, job record, reshard lease): spec versions active, staged and retired,
 * progress in shards and files, the lease holder, staged shards marked, projects instead of shard ids.
 */
class ReshardStatusTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ShardId S01 = new ShardId("lucene~ring=s01");
    private static final ShardId S02 = new ShardId("lucene~ring=s02");
    private static final ShardId S03 = new ShardId("lucene~ring=s03");

    final StatusFixtures.MutableClock clock = new StatusFixtures.MutableClock(T0);
    final InMemoryObjectStore store = new InMemoryObjectStore(clock);
    final ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
    final ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
    final List<AutoCloseable> open = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = open.size() - 1; i >= 0; i--) open.get(i).close();
    }

    /** SSE events as a client gets them: type and JSON. */
    static final class Recorder implements StatusEvents.Sink {
        final List<JsonNode> got = new CopyOnWriteArrayList<>();

        @Override public void event(String type, String data) {
            try {
                got.add(JSON.readTree(data));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override public void comment(String text) { }

        @Override public void onClose(Runnable r) { }

        @Override public void close() { }

        List<JsonNode> reshards() {
            return got.stream().filter(n -> StatusEvent.RESHARD.equals(n.path("type").asText()))
                    .map(n -> n.path("reshard")).toList();
        }
    }

    void publish(ShardId id, long gen, long size) throws Exception {
        ProjectMeta meta = new ProjectMeta();
        meta.setName(id.value());
        meta.setSourceRevision("rev-1");
        ShardManifest m = new ShardManifest(ShardManifest.FORMAT_VERSION, id.value(), gen, gen - 1, clock.instant(),
                "w1", 100, size, "fp", List.of(new ManifestFile(ManifestFile.LUCENE, "segments_1",
                layout.shardPrefix(id) + "lucene/segments_1@00", 10, "00")), meta, null);
        store.put(layout.manifestKey(id, new Generation(gen)), m.toJson());
        store.put(layout.latestKey(id), ("{\"generation\":" + gen + ",\"manifest\":\"gen-" + gen + ".json\"}")
                .getBytes(StandardCharsets.UTF_8));
    }

    void heartbeats() throws Exception {
        Map<String, WriterHeartbeat.Backlog> backlog = new LinkedHashMap<>();
        for (String s : List.of(S01.value(), S02.value(), "docs")) backlog.put(s, new WriterHeartbeat.Backlog("rev-1",
                "rev-1", false, null));
        store.put(layout.writerKey("w1"), JSON.writeValueAsBytes(new WriterHeartbeat("w1", true,
                List.of(S01.value(), S02.value(), "docs"), List.of(), backlog, null, 1L, false, null)));
        store.put(layout.readerKey("r1"), JSON.writeValueAsBytes(StatusFixtures.reader("r1", 1, 10L,
                List.of(S01.value(), S02.value(), "docs"))));
        for (ShardId s : List.of(S01, S02, new ShardId("docs"))) {
            store.put(layout.leaseKey(s), "{\"writer\":\"w1\"}".getBytes(StandardCharsets.UTF_8));
        }
    }

    /** The job record as {@code ReshardJob} writes it. */
    void job(int spec, String phase, List<ShardId> rewritten, List<ShardId> kept, int filesDone, int filesTotal)
            throws Exception {
        ObjectNode n = JSON.createObjectNode();
        n.put("spec", spec);
        n.put("from", 1);
        n.put("phase", phase);
        n.put("writer", "w1");
        n.putArray("shards").add(S01.value()).add(S02.value()).add(S03.value());
        n.putObject("generations");
        rewritten.forEach(s -> n.withArray("affected").add(s.value()));
        n.withArray("affected");
        kept.forEach(s -> n.withArray("unaffected").add(s.value()));
        n.withArray("unaffected");
        n.putArray("removed");
        n.put("filesTotal", filesTotal);
        n.put("filesDone", filesDone);
        n.put("updatedAt", clock.instant().toString());
        store.put(layout.reshardJobKey("lucene"), JSON.writeValueAsBytes(n));
    }

    StatusService node(StatusEvents events) {
        StatusService s = new StatusService(
                new BucketStatusSource(store, layout, catalog, new ObjectStoreReplicaSource(store, layout),
                        Duration.ofSeconds(30), Duration.ofSeconds(30), clock),
                StatusStateStore.bucket(store, layout), StatusSettings.defaults(), events, Duration.ofHours(1),
                Duration.ZERO, System::nanoTime);
        open.add(s);
        return s;
    }

    private static ClusterStatus.Project project(ClusterStatus st, String name) {
        return st.projects().stream().filter(p -> p.project().equals(name)).findFirst().orElseThrow();
    }

    private static ClusterStatus.Shard shard(ClusterStatus st, ShardId id) {
        return st.layout().shards().stream().filter(s -> s.shard().equals(id.value())).findFirst().orElseThrow();
    }

    private static List<String> specStates(ClusterStatus st) {
        return st.specs().stream().map(v -> v.version() + ":" + v.state()).toList();
    }

    private ClusterStatus step(StatusService node) throws Exception {
        clock.advance(Duration.ofSeconds(5));
        store.put(layout.readerKey("r1"), JSON.writeValueAsBytes(StatusFixtures.reader("r1", 1, 10L,
                List.of(S01.value(), S02.value(), "docs"))));
        return node.refresh();
    }

    private static void awaitEvents(Recorder r, int n) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (r.reshards().size() < n && System.nanoTime() < deadline) Thread.sleep(5);
    }

    /** Ruling P6-R15: the project view of the active shards: published, min generation, summed docs. */
    private static void assertActiveView(ClusterStatus.Project p, long generation, long docs) {
        assertThat(p.published()).as(p.project() + " published").isTrue();
        assertThat(p.generation()).isEqualTo(generation);
        assertThat(p.docCount()).isEqualTo(docs);
        assertThat(p.pending()).isFalse();
    }

    /**
     * Ruling P6-R15 (a per-project consumer, like the operator, must not pick up a staged shard): mid-reshard,
     * the project is published at the lowest generation of its active shards, with their docs summed, and
     * pending when one of them is; the staged-only shard, unpublished and with its own docs, counts for none.
     */
    @Test
    void projectViewCountsTheActiveShardsOnly() {
        String a = S01.value(), b = S02.value(), c = S03.value();
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        ShardSpec ring3 = ShardSpec.parse("[project, ring(path, shards=3)]");
        StatusSnapshot.ProjectObs lucene = new StatusSnapshot.ProjectObs("lucene", 1, ring2, List.of(a, b),
                new StatusSnapshot.StagedObs(2, ring3, List.of(a, b, c), Map.of(), null), null,
                new StatusSnapshot.RequestObs(2, ring3, T0), null, null, null);
        java.util.function.Function<Boolean, StatusSnapshot> snap = pending -> new StatusSnapshot(
                StatusSnapshot.CLUSTER, T0,
                List.of(new StatusSnapshot.ShardObs(a, true, 4L, 100L, 10L, "w1", T0, "r1", null),
                        new StatusSnapshot.ShardObs(b, true, 2L, 50L, 10L, "w1", T0, "r1", null),
                        new StatusSnapshot.ShardObs(c, true, null, 999L, 10L, null, null, null, null)),
                List.of(new StatusSnapshot.WriterObs(StatusFixtures.writer("w1", List.of(a, b),
                        Map.of(a, pending ? StatusFixtures.pending(10) : new WriterHeartbeat.Backlog("r1", "r1", false,
                                null)), null, null), T0)),
                List.of(), Duration.ofSeconds(30), false, List.of(lucene), null, null);
        StatusSettings cfg = StatusSettings.defaults();
        ClusterStatus.Project p = StatusBuilder.build(snap.apply(false), null, cfg).projects().get(0);
        assertThat(p.published()).isTrue();
        assertThat(p.generation()).isEqualTo(2L);
        assertThat(p.docCount()).isEqualTo(150L);
        assertThat(p.pending()).isFalse();
        assertThat(StatusBuilder.build(snap.apply(true), null, cfg).projects().get(0).pending()).isTrue();

        // an active shard without a generation: not published
        StatusSnapshot unpublished = new StatusSnapshot(StatusSnapshot.CLUSTER, T0,
                List.of(new StatusSnapshot.ShardObs(a, true, 4L, 100L, 10L, "w1", T0, "r1", null),
                        new StatusSnapshot.ShardObs(b, true, null, null, null, null, null, null, null)),
                List.of(), List.of(), Duration.ofSeconds(30), false, List.of(lucene), null, null);
        ClusterStatus.Project u = StatusBuilder.build(unpublished, null, cfg).projects().get(0);
        assertThat(u.published()).isFalse();
        assertThat(u.generation()).isEqualTo(4L);
    }

    @Test
    void statusShowsReshardProgress() throws Exception {
        // lucene on [project, ring(path, shards=2)] (spec version 1), docs on the implicit [project]
        int v1 = catalog.addSpec(Optional.of("lucene"), ShardSpec.parse("[project, ring(path, shards=2)]"));
        catalog.putProject("lucene", v1, List.of(S01, S02));
        for (ShardId s : List.of(S01, S02, new ShardId("docs"))) {
            catalog.register(s, Optional.of(ShardSource.path("/src/" + s.value())));
        }
        publish(S01, 4, 600);
        publish(S02, 2, 400);
        publish(new ShardId("docs"), 7, 50);
        heartbeats();
        StatusEvents events = new StatusEvents(Duration.ofHours(1), null);
        StatusService node = node(events);
        Recorder sse = new Recorder();
        assertThat(events.tryReserve()).isTrue();
        events.add(sse);

        ClusterStatus st = step(node);
        // projects, not shard ids: the split project is one entry over its shards
        assertThat(st.projects()).extracting(ClusterStatus.Project::project).containsExactly("docs", "lucene");
        ClusterStatus.Project lucene = project(st, "lucene");
        assertThat(lucene.specVersion()).isEqualTo(1);
        assertThat(lucene.spec()).isEqualTo("[project, ring(path, shards=2, vnodes=512)]");
        assertThat(lucene.shards()).containsExactly(S01.value(), S02.value());
        assertThat(lucene.sizeBytes()).isEqualTo(1000L);
        assertThat(lucene.docCount()).isEqualTo(200L);
        assertThat(lucene.reshard()).isNull();
        assertThat(project(st, "docs").specVersion()).isNull();
        assertThat(project(st, "docs").spec()).isEqualTo("[project]");
        assertThat(shard(st, S01).keys()).isEqualTo(Map.of("project", "lucene", "ring", "s01"));
        assertThat(shard(st, S01).project()).isEqualTo("lucene");
        assertThat(shard(st, S01).role()).isEqualTo("active");
        assertThat(shard(st, S01).specVersion()).isEqualTo(1);
        assertThat(shard(st, new ShardId("docs")).keys()).isEqualTo(Map.of("project", "docs"));
        assertThat(shard(st, new ShardId("docs")).specVersion()).isEqualTo(StatusBuilder.SPEC_VERSION);
        assertThat(specStates(st)).containsExactly("1:active");

        // requested: a new spec version, nothing staged yet
        int v2 = catalog.requestReshard("lucene", ShardSpec.parse("[project, ring(path, shards=3)]"), T0);
        st = step(node);
        ClusterStatus.Reshard r = project(st, "lucene").reshard();
        assertThat(r.phase()).isEqualTo("requested");
        assertThat(r.specVersion()).isEqualTo(v2);
        assertThat(r.from()).isEqualTo(v1);
        assertThat(r.spec()).isEqualTo("[project, ring(path, shards=3, vnodes=512)]");
        assertThat(r.requestedAt()).isEqualTo(T0.toString());
        assertThat(r.shardsDone()).isZero();
        assertThat(r.shardsTotal()).isNull();
        assertThat(r.filesTotal()).isNull();
        assertThat(specStates(st)).containsExactly("1:active", "2:requested");

        // building: staged, the job record counts files, w1 holds the reshard lease
        catalog.stageReshard("lucene", v2, OptionalInt.of(v1), List.of(S01, S02, S03), T0);
        catalog.register(S03, Optional.of(ShardSource.path("/src/lucene")));
        job(v2, "built", List.of(), List.of(), 10, 40);
        store.put(layout.reshardLeaseKey("lucene"), "{\"writer\":\"w1\"}".getBytes(StandardCharsets.UTF_8));
        st = step(node);
        r = project(st, "lucene").reshard();
        assertThat(r.phase()).isEqualTo("building");
        assertThat(r.shardsDone()).isZero();
        assertThat(r.shardsTotal()).isEqualTo(3);
        assertThat(r.filesDone()).isEqualTo(10);
        assertThat(r.filesTotal()).isEqualTo(40);
        assertThat(r.holder()).isEqualTo("w1");
        assertThat(r.lease().writer()).isEqualTo("w1");
        assertThat(r.lease().expired()).isFalse();
        assertThat(r.jobPhase()).isEqualTo("built");
        assertThat(project(st, "lucene").staged())
                .isEqualTo(new ClusterStatus.StagedVersion(v2, r.spec(), List.of(S01.value(), S02.value(), S03.value()),
                        "building"));
        assertThat(specStates(st)).containsExactly("1:active", "2:staged");
        // the staged-only shard is marked, and the shards both versions share stay active (and staged)
        ClusterStatus.Shard s03 = shard(st, S03);
        assertThat(s03.role()).isEqualTo("staged");
        assertThat(s03.state()).isEqualTo("staged");
        assertThat(s03.staged()).isTrue();
        assertThat(s03.specVersion()).isEqualTo(v2);
        assertThat(s03.project()).isEqualTo("lucene");
        assertThat(shard(st, S01).role()).isEqualTo("active");
        assertThat(shard(st, S01).staged()).isTrue();
        assertThat(st.health()).as("a staged shard is neither unassigned nor unserved")
                .noneMatch(h -> h.target().equals(S03.value()));
        assertActiveView(project(st, "lucene"), 2L, 200L); // P6-R15: s03 (staged, unpublished) not counted

        // progress inside the same quarter: no event; the next quarter of shards and files: one
        job(v2, "built", List.of(), List.of(), 19, 40);
        st = step(node);
        assertThat(project(st, "lucene").reshard().filesDone()).isEqualTo(19);
        job(v2, "built", List.of(S01), List.of(S02), 40, 40);
        st = step(node);
        assertThat(project(st, "lucene").reshard().shardsDone()).isEqualTo(2);
        assertThat(project(st, "lucene").reshard().shardsRewritten()).isEqualTo(1);
        assertThat(project(st, "lucene").reshard().shardsKept()).isEqualTo(1);

        // pinned: every shard published; s01 is rewritten at a generation the active version doesn't serve
        catalog.pinStaged("lucene", v2, Map.of(S01, 5L, S02, 2L, S03, 1L), Map.of(S01, 4L, S02, 2L));
        job(v2, "waiting", List.of(S01, S03), List.of(S02), 40, 40);
        st = step(node);
        r = project(st, "lucene").reshard();
        assertThat(r.phase()).isEqualTo("pinned");
        assertThat(r.shardsDone()).isEqualTo(3);
        assertThat(r.filesDone()).isEqualTo(40);
        assertThat(shard(st, S01).stagedGeneration()).isEqualTo(5L);
        assertThat(shard(st, S01).generation()).isEqualTo(4L);
        assertThat(shard(st, S02).stagedGeneration()).isEqualTo(2L);
        assertActiveView(project(st, "lucene"), 2L, 200L);

        // flipped: version 2 active, version 1 retired and kept
        catalog.flipReshard("lucene", v2, Map.of(S01, 4L, S02, 2L), T0.plusSeconds(60));
        publish(S03, 1, 300);
        publish(S01, 5, 300);
        store.put(layout.reshardLeaseKey("lucene"), "{\"writer\":\"w1\",\"released\":true}"
                .getBytes(StandardCharsets.UTF_8));
        st = step(node);
        r = project(st, "lucene").reshard();
        assertThat(r.phase()).isEqualTo("flipped");
        assertThat(r.from()).isEqualTo(v1);
        assertThat(r.specVersion()).isEqualTo(v2);
        assertThat(r.holder()).as("the lease is released").isNull();
        assertThat(project(st, "lucene").specVersion()).isEqualTo(v2);
        assertThat(project(st, "lucene").shards()).containsExactly(S01.value(), S02.value(), S03.value());
        assertThat(project(st, "lucene").previous().specVersion()).isEqualTo(v1);
        assertThat(project(st, "lucene").previous().retiredAt()).isEqualTo(T0.plusSeconds(60).toString());
        assertThat(specStates(st)).containsExactly("1:retired", "2:active");
        assertThat(shard(st, S03).role()).isEqualTo("active");
        assertThat(shard(st, S03).specVersion()).isEqualTo(v2);

        // a cancelled reshard of docs is abandoned
        int v3 = catalog.requestReshard("docs", ShardSpec.parse("[project, ring(path, shards=2)]"), T0);
        catalog.stageReshard("docs", v3, OptionalInt.empty(),
                List.of(new ShardId("docs~ring=s01"), new ShardId("docs~ring=s02")), T0);
        st = step(node);
        assertThat(project(st, "docs").reshard().phase()).isEqualTo("building");
        assertThat(project(st, "docs").reshard().from()).isNull();
        catalog.cancelReshard("docs", T0.plusSeconds(90));
        st = step(node);
        assertThat(project(st, "docs").reshard().phase()).isEqualTo("abandoned");
        assertThat(project(st, "docs").staged().state()).isEqualTo("abandoned");
        assertThat(specStates(st)).containsExactly("1:retired", "2:active", "3:abandoned");

        // the public JSON carries it all, and the events came at the phase transitions and coarse steps only
        JsonNode pub = JSON.readTree(node.currentJson(false));
        assertThat(pub.path("projects").get(1).path("reshard").path("phase").asText()).isEqualTo("flipped");
        assertThat(pub.path("specs")).hasSize(3);
        awaitEvents(sse, 7);
        List<String> seen = sse.reshards().stream().map(n -> n.path("project").asText() + ":"
                + n.path("phase").asText() + ":" + n.path("shardsDone").asText("-") + ":"
                + n.path("filesDone").asText("-")).toList();
        assertThat(seen).containsExactly(
                "lucene:requested:0:-",
                "lucene:building:0:10",  // files 10/40: quarter 1
                "lucene:building:2:40",  // shards 2/3 and files 40/40 (19/40 stayed in quarter 1: no event)
                "lucene:pinned:3:40",
                "lucene:flipped:3:40",
                "docs:building:0:-",
                "docs:abandoned:0:-");
        assertThat(sse.reshards().get(3).path("specVersion").asInt()).isEqualTo(v2);
        assertThat(sse.reshards().get(3).path("shardsTotal").asInt()).isEqualTo(3);
    }
}
