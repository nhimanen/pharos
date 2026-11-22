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

                x.parts.runtime().syncNow();
                deadline = System.nanoTime() + 20_000_000_000L;
                boolean onNew = false;
                while (!onNew && System.nanoTime() < deadline) {
                    SearchResponse r = assertOnce(k, project, "flipped, x synced");
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

    /** The moved class's method is found exactly once, with no warning (no shard left out). */
    private static SearchResponse assertOnce(TestReaderNode k, String project, String when) throws Exception {
        SearchResponse r = k.engine.searchWithTrace(new com.pharos.search.SearchRequest("zebrafinch",
                com.pharos.search.SearchRequest.SearchType.KEYWORD, project, null, 10, "text", null, null, 0), false);
        assertThat(r.warnings()).as(when + ": no warning").isEmpty();
        assertThat(r.results().stream().filter(res -> res.id().contains("Zebrafinch#")).count())
                .as(when + ": the moved method, once").isEqualTo(1);
        return r;
    }
}
