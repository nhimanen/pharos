package com.pharos.search.distributed;

import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.sharding.DocKeys;
import com.pharos.cluster.sharding.ReshardJob;
import com.pharos.cluster.sharding.ShardPlan;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.indexer.IndexAbort;
import com.pharos.search.SearchResponse;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6 Task 4 through real reader nodes and the coordinator (ruling 1 carried from Task 3): a ring
 * change moves a file into a new shard while its old shard is rewritten under the same id. A query
 * reads one spec version and finds the file exactly once before the reshard, while it is staged, right
 * after the flip while the reader still serves the previous version (the router falls back to it), and
 * once the reader has activated the new version.
 */
class ReshardQueryTest {

    private static final String BUCKET = "test-bucket";
    private static final ParityCorpus.HashEmbeddings EMBEDDER = new ParityCorpus.HashEmbeddings();

    @Test
    void aMovedFileIsReadOnceBeforeDuringAndAfterTheFlip() throws Exception {
        Path tmp = Files.createTempDirectory("reshard-query");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String project = "mover";
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        ShardSpec ring3 = ShardSpec.parse("[project, ring(path, shards=3)]");
        List<ShardId> two = List.of(new ShardId(project + "~ring=s01"), new ShardId(project + "~ring=s02"));
        ShardId s03 = new ShardId(project + "~ring=s03");
        String moving = null;
        for (int i = 0; i < 500 && moving == null; i++) {
            String rel = "src/main/java/com/example/z" + i + "/Zebrafinch.java";
            if (ring3.assign(DocKeys.of(project, rel)).equals(s03)) moving = rel;
        }
        assertThat(moving).isNotNull();
        ShardId from = ring2.assign(DocKeys.of(project, moving));
        Path src = tmp.resolve("src");
        for (int i = 0; i < 8; i++) SplitProjectSearchTest.writeClass(src, "src/main/java/com/example/p/Plain" + i + ".java",
                "com.example.p", "Plain" + i, "plain words " + i);
        String pkg = "com.example." + Path.of(moving).getParent().getFileName();
        SplitProjectSearchTest.writeClass(src, moving, pkg, "Zebrafinch", "zebrafinch song");
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, null)) {
            fx.indexSplit(src, project, ring2, two, false);
            fx.index(JAVA, false);
            try (TestReaderNode x = new TestReaderNode(tmp.resolve("x"), store, BUCKET, "x",
                    List.of(ShardId.wildcardOf(project)), EMBEDDER, null);
                 TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(JAVA), EMBEDDER, null)) {
                k.awaitShard(two.get(0).value(), 1);
                k.awaitShard(two.get(1).value(), 1);
                assertOnce(k, project, "before");

                ReshardJob job = fx.reshardJob(() -> k.parts.runtime().liveReaders().stream()
                        .map(l -> l.heartbeat()).toList(), Clock.systemUTC(), Duration.ofMinutes(10));
                ShardPlan active = new ShardPlan(project, ring2, two);
                fx.writerCatalog().requestReshard(project, ring3, java.time.Instant.now());
                assertThat(job.step(project, active, ShardSource.path(src.toString()), src, false, IndexAbort.none()))
                        .as("staged and published; x lags").isEqualTo(ReshardJob.Step.WAITING);
                assertOnce(k, project, "staged, x not synced");
                x.parts.runtime().syncNow(); // materialises the rewritten shard, activates s03; serves N
                assertOnce(k, project, "staged, x synced");
                assertThat(x.parts.runtime().served().get(from).value())
                        .as("the rewritten shard is not served before the flip")
                        .isEqualTo(fx.writerCatalog().projectShards().pinsOf(project).get(from));

                ReshardJob.Step step = ReshardJob.Step.WAITING;
                long deadline = System.nanoTime() + 20_000_000_000L;
                while (step == ReshardJob.Step.WAITING && System.nanoTime() < deadline) { // x's heartbeat: prepared
                    Thread.sleep(200);
                    step = job.step(project, active, ShardSource.path(src.toString()), src, false, IndexAbort.none());
                }
                assertThat(step).isEqualTo(ReshardJob.Step.FLIPPED);

                // flipped; x has not synced it: queries read the previous version until x activates it
                boolean fellBack = false;
                deadline = System.nanoTime() + 20_000_000_000L;
                while (!fellBack && System.nanoTime() < deadline) {
                    SearchResponse r = assertOnce(k, project, "flipped, x not synced");
                    fellBack = r.trace().notes().stream().anyMatch(n -> n.contains("previous version"));
                    if (!fellBack) Thread.sleep(500);
                }
                assertThat(fellBack).as("the coordinator saw the flip and read the previous version").isTrue();

                // x activates the new version (keeping the previous generations answerable, P6-R19)
                deadline = System.nanoTime() + 45_000_000_000L;
                boolean onNew = false;
                while (!onNew && System.nanoTime() < deadline) {
                    x.parts.runtime().syncNow();
                    SearchResponse r = assertOnce(k, project, "flipped, x syncing");
                    onNew = r.trace().notes().stream().noneMatch(n -> n.contains("previous version"));
                    if (!onNew) Thread.sleep(500);
                }
                assertThat(onNew).as("reads the new version once x serves it").isTrue();
                assertThat(x.parts.runtime().served()).containsKey(s03);
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    /**
     * Ruling P6-R19, found by the Phase 6 gate (2c): right after a flip the readers activate the rewritten
     * shards each at its own moment, while a coordinator still routes the version before the flip (its
     * catalog read predates it, and pins that version's generations). Every reader that switched still
     * answers the previous generation it retains, so the coordinator reads that version complete, with no
     * warning and no re-plan (re-plans are off here). Without the retention, a reader that switched refuses
     * the pinned generation and the shard is left out.
     */
    @Test
    void aCoordinatorRoutingThePreFlipVersionIsAnsweredByReadersThatSwitchedAtDifferentTimes() throws Exception {
        Path tmp = Files.createTempDirectory("reshard-retained");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String project = "mover";
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        ShardSpec ring3 = ShardSpec.parse("[project, ring(path, shards=3)]");
        List<ShardId> two = List.of(new ShardId(project + "~ring=s01"), new ShardId(project + "~ring=s02"));
        ShardId s03 = new ShardId(project + "~ring=s03");
        String moving = null;
        for (int i = 0; i < 500 && moving == null; i++) {
            String rel = "src/main/java/com/example/z" + i + "/Zebrafinch.java";
            if (ring3.assign(DocKeys.of(project, rel)).equals(s03)) moving = rel;
        }
        assertThat(moving).isNotNull();
        Path src = tmp.resolve("src");
        for (int i = 0; i < 8; i++) SplitProjectSearchTest.writeClass(src, "src/main/java/com/example/p/Plain" + i + ".java",
                "com.example.p", "Plain" + i, "plain words " + i);
        String pkg = "com.example." + Path.of(moving).getParent().getFileName();
        SplitProjectSearchTest.writeClass(src, moving, pkg, "Zebrafinch", "zebrafinch song");
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, null)) {
            fx.indexSplit(src, project, ring2, two, false);
            fx.index(JAVA, false);
            java.util.function.Consumer<com.pharos.config.ClusterConfig> grace =
                    c -> c.getReader().setPreviousGenerationGraceSeconds(300);
            try (TestReaderNode x = new TestReaderNode(tmp.resolve("x"), store, BUCKET, "x",
                    List.of(ShardId.wildcardOf(project)), EMBEDDER, null, grace);
                 TestReaderNode y = new TestReaderNode(tmp.resolve("y"), store, BUCKET, "y",
                         List.of(ShardId.wildcardOf(project)), EMBEDDER, null, grace);
                 TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(JAVA), EMBEDDER, null)) {
                k.awaitShard(two.get(0).value(), 2);
                k.awaitShard(two.get(1).value(), 2);
                k.ds().retries = false;
                assertOnce(k, project, "before");

                ReshardJob job = fx.reshardJob(() -> k.parts.runtime().liveReaders().stream()
                        .map(l -> l.heartbeat()).toList(), Clock.systemUTC(), Duration.ofMinutes(10));
                ShardPlan active = new ShardPlan(project, ring2, two);
                fx.writerCatalog().requestReshard(project, ring3, java.time.Instant.now());
                assertThat(job.step(project, active, ShardSource.path(src.toString()), src, false, IndexAbort.none()))
                        .isEqualTo(ReshardJob.Step.WAITING);
                x.parts.runtime().syncNow();
                y.parts.runtime().syncNow();
                com.pharos.cluster.sharding.ProjectShards preFlip = fx.writerCatalog().projectShards();
                java.util.Map<ShardId, Long> preFlipPins = preFlip.pinsOf(project);
                assertThat(preFlipPins).as("the active version is pinned before the flip").isNotEmpty();
                // as the flip's minimum pinned age ensures: every coordinator has read the pins before the flip
                long seen = System.nanoTime() + 20_000_000_000L;
                while (k.ds().catalogProjects().map(m -> m.pinsOf(project).isEmpty()).orElse(true)
                        && System.nanoTime() < seen) {
                    assertOnce(k, project, "staged, pinned");
                    Thread.sleep(200);
                }
                assertThat(k.ds().catalogProjects().orElseThrow().pinsOf(project)).as("k read the pins").isNotEmpty();
                ReshardJob.Step step = ReshardJob.Step.WAITING;
                long deadline = System.nanoTime() + 30_000_000_000L;
                while (step == ReshardJob.Step.WAITING && System.nanoTime() < deadline) {
                    Thread.sleep(200);
                    step = job.step(project, active, ShardSource.path(src.toString()), src, false, IndexAbort.none());
                }
                assertThat(step).isEqualTo(ReshardJob.Step.FLIPPED);

                // A coordinator still routing the pre-flip catalog: its own router, client and shard service.
                var cfg = k.parts.config().getCluster();
                var health = new com.pharos.cluster.membership.ReaderHealth(cfg.getReaderTtlSeconds());
                var stale = new DistributedSearch(new com.pharos.cluster.routing.ShardRouter(
                        k.parts.runtime().membership(), frozen(preFlip), health, id -> false, n -> 0),
                        new com.pharos.cluster.internal.ShardClient(health, cfg.getToken(),
                                Duration.ofMillis(cfg.getConnectTimeoutMillis()), Duration.ofMillis(cfg.getFanoutTimeoutMillis())),
                        k.engine.shardService(id -> k.parts.runtime().servesShard(id), null), EMBEDDER,
                        k.engine.vectorStrategy(), Duration.ofSeconds(10), () -> 0L);
                stale.retries = false;
                DistributedSearch fresh = k.ds();
                try {
                    k.engine.setDistributed(stale);
                    x.parts.runtime().syncNow(); // x switches to the new version; y has not yet
                    for (ShardId s : two) {
                        if (!preFlipPins.get(s).equals(fx.writerCatalog().projectShards().pinsOf(project).get(s))) {
                            assertThat(x.parts.runtime().served().get(s).value()).as(s + " switched on x")
                                    .isNotEqualTo(preFlipPins.get(s));
                        }
                    }
                    for (int i = 0; i < 6; i++) assertNoReplan(assertOnce(k, project, "pre-flip routing, x switched"));
                    y.parts.runtime().syncNow(); // now y too
                    for (int i = 0; i < 6; i++) assertNoReplan(assertOnce(k, project, "pre-flip routing, both switched"));
                } finally {
                    k.engine.setDistributed(fresh);
                    stale.close();
                }
                for (int i = 0; i < 3; i++) assertNoReplan(assertOnce(k, project, "fresh routing, both switched"));
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    private static void assertNoReplan(SearchResponse r) {
        assertThat(r.trace().notes()).as("no re-plan").noneMatch(n -> n.startsWith("re-planned"));
    }

    static com.pharos.cluster.ShardCatalog frozen(com.pharos.cluster.sharding.ProjectShards snapshot) {
        return new com.pharos.cluster.ShardCatalog() {
            @Override public List<ShardId> shards() { return snapshot.listed(); }
            @Override public com.pharos.cluster.sharding.ProjectShards projectShards() { return snapshot; }
        };
    }

    /** The moved class's method is found exactly once, with no warning (no shard left out). */
    private static SearchResponse assertOnce(TestReaderNode k, String project, String when) throws Exception {
        SearchResponse r = k.engine.searchWithTrace(new com.pharos.search.SearchRequest("zebrafinch",
                com.pharos.search.SearchRequest.SearchType.KEYWORD, project, null, 10, "text", null, null, 0), false);
        assertThat(r.warnings()).as(when + ": no warning").isEmpty();
        assertThat(r.results().stream().filter(res -> res.id().contains("Zebrafinch#")).count())
                .as(when + ": the moved method, once (notes " + r.trace().notes() + ", results "
                        + r.results().stream().map(res -> res.id()).toList() + ")").isEqualTo(1);
        return r;
    }
}
