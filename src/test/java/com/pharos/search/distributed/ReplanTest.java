package com.pharos.search.distributed;

import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.sharding.DocKeys;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchRequest.SearchType;
import com.pharos.search.SearchResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static com.pharos.cluster.ClusterFixture.PY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Ruling P6-R16, found by the Phase 6 gate: a query that loses a shard because a reader swapped the
 * shard's generation between the query's statistics round and its search round ("its generation changed
 * during the query"), or because a reader analyses with other synonyms than the coordinator, is planned
 * once more on the state the readers have moved to, and answers complete, without a warning. Each case
 * runs first with the re-plan off (the control): the shard is left out, with its warning.
 */
class ReplanTest {

    private static final String BUCKET = "test-bucket";
    private static final ParityCorpus.HashEmbeddings EMBEDDER = new ParityCorpus.HashEmbeddings();

    /** A reader that keeps no previous generation (P6-R19 off): the re-plan is what mends a swap then. */
    private static final java.util.function.Consumer<com.pharos.config.ClusterConfig> NO_RETENTION =
            c -> c.getReader().setPreviousGenerationGraceSeconds(0);

    @AfterEach
    void clearHooks() {
        DistributedSearch.afterStatsRound = null;
        DistributedSearch.beforeReplan = null;
    }

    /**
     * A split project's reader activates the project's next index run during a query (as readers do
     * after a flip, each at its own moment): the query pinned the previous generations in its statistics
     * round, the search round is refused, and the re-plan reads the new run.
     */
    @Test
    void aReaderSwappingAShardDuringTheQueryIsFollowedByOneReplan() throws Exception {
        Path tmp = Files.createTempDirectory("replan-gen");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String project = "swap";
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        List<ShardId> shards = List.of(new ShardId(project + "~ring=s01"), new ShardId(project + "~ring=s02"));
        Path src = tmp.resolve("src");
        for (int i = 0; i < 12; i++) {
            SplitProjectSearchTest.writeClass(src, "src/main/java/com/example/p" + i + "/Kestrel" + i + ".java",
                    "com.example.p" + i, "Kestrel" + i, "kestrel hover " + i);
        }
        for (ShardId s : shards) {
            boolean has = false;
            for (int i = 0; i < 12; i++) {
                has |= ring2.assign(DocKeys.of(project, "src/main/java/com/example/p" + i + "/Kestrel" + i + ".java"))
                        .equals(s);
            }
            assertThat(has).as("the fixture puts files in " + s).isTrue();
        }
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, null)) {
            fx.indexSplit(src, project, ring2, shards, false);
            fx.index(JAVA, false);
            try (TestReaderNode x = new TestReaderNode(tmp.resolve("x"), store, BUCKET, "x",
                    List.of(ShardId.wildcardOf(project)), EMBEDDER, null, NO_RETENTION);
                 TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(JAVA), EMBEDDER, null)) {
                for (ShardId s : shards) k.awaitShard(s.value(), 1);
                assertThat(search(k, project).warnings()).as("before").isEmpty();

                // control: the re-plan off; x activates the next run between the two rounds
                k.ds().retries = false;
                nextRun(fx, src, project, ring2, shards, 1);
                swapOnceAfterStats(x);
                // both shards are lost: a query scoped to the project then fails (nothing answered)
                assertThatThrownBy(() -> search(k, project)).as("without the re-plan")
                        .hasMessageContaining("generation changed during the query");

                k.ds().retries = true;
                nextRun(fx, src, project, ring2, shards, 2);
                swapOnceAfterStats(x);
                SearchResponse on = search(k, project);
                assertThat(on.warnings()).as("with the re-plan").isEmpty();
                assertThat(on.results().stream().map(res -> res.className()).distinct().count())
                        .as("every Kestrel class, from both shards").isEqualTo(12);
                assertThat(on.trace().notes()).anyMatch(n -> n.startsWith("re-planned once"));
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    /**
     * Ruling P6-R19: the same swap on a reader that retains the previous generation. The query's search round
     * asks for the generations its statistics round pinned, and the reader answers them from the retained
     * ones: complete, no warning, and no re-plan needed (re-plans are off).
     */
    @Test
    void aReaderSwappingAShardDuringTheQueryAnswersFromItsRetainedGeneration() throws Exception {
        Path tmp = Files.createTempDirectory("replan-retained");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String project = "swap";
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        List<ShardId> shards = List.of(new ShardId(project + "~ring=s01"), new ShardId(project + "~ring=s02"));
        Path src = tmp.resolve("src");
        for (int i = 0; i < 12; i++) {
            SplitProjectSearchTest.writeClass(src, "src/main/java/com/example/p" + i + "/Kestrel" + i + ".java",
                    "com.example.p" + i, "Kestrel" + i, "kestrel hover " + i);
        }
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, null)) {
            fx.indexSplit(src, project, ring2, shards, false);
            fx.index(JAVA, false);
            try (TestReaderNode x = new TestReaderNode(tmp.resolve("x"), store, BUCKET, "x",
                    List.of(ShardId.wildcardOf(project)), EMBEDDER, null,
                    c -> c.getReader().setPreviousGenerationGraceSeconds(300));
                 TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(JAVA), EMBEDDER, null)) {
                for (ShardId s : shards) k.awaitShard(s.value(), 1);
                k.ds().retries = false;
                for (int n = 1; n <= 2; n++) {
                    nextRun(fx, src, project, ring2, shards, n);
                    swapOnceAfterStats(x);
                    SearchResponse r = search(k, project);
                    assertThat(r.warnings()).as("run " + n + ": no shard left out").isEmpty();
                    assertThat(r.results().stream().map(res -> res.className()).distinct().count())
                            .as("run " + n + ": every Kestrel class, from both shards").isEqualTo(12);
                    assertThat(r.trace().notes()).noneMatch(note -> note.startsWith("re-planned"));
                }
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    /**
     * A synonyms publish reaches the readers at different moments: the reader holding a shard analyses
     * with other synonyms than the coordinator and refuses; by the re-plan the coordinator has the same
     * synonyms, and the query answers complete.
     */
    @Test
    void aSynonymsPublishSeenAtDifferentMomentsIsFollowedByOneReplan() throws Exception {
        Path tmp = Files.createTempDirectory("replan-syn");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String published = "calculator, adder\n";
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER)) {
            fx.indexBoth();
            try (TestReaderNode c = new TestReaderNode(tmp.resolve("c"), store, BUCKET, "c", List.of(PY), EMBEDDER,
                    "old, rule\n");
                 TestReaderNode r = new TestReaderNode(tmp.resolve("r"), store, BUCKET, "r", List.of(JAVA), EMBEDDER,
                         published)) {
                c.awaitShard(JAVA, 1);
                Path coordinatorSynonyms = c.configBase.resolve("synonyms.txt");

                c.ds().retries = false;
                SearchResponse off = search(c, null, "multiply Calculator");
                assertThat(off.warnings()).as("without the re-plan").anyMatch(w -> w.contains("synonyms"));

                c.ds().retries = true;
                AtomicBoolean caughtUp = new AtomicBoolean();
                DistributedSearch.beforeReplan = () -> { // the publish reaches the coordinator meanwhile
                    try {
                        Files.writeString(coordinatorSynonyms, published);
                        Files.setLastModifiedTime(coordinatorSynonyms,
                                FileTime.fromMillis(System.currentTimeMillis() + 5000));
                        caughtUp.set(true);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                };
                SearchResponse on = search(c, null, "multiply Calculator");
                assertThat(caughtUp).as("the query was re-planned").isTrue();
                assertThat(on.warnings()).as("with the re-plan").isEmpty();
                assertThat(on.results()).as("the sample-app shard answered")
                        .anyMatch(x -> x.project().equals(JAVA));
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    /**
     * Ruling P6-R19: a synonyms publish reaches one node late. Both nodes start on the same synonyms; the
     * publish reaches the coordinator first (the reader still has the old ones: the coordinator analyses the
     * query with its previous synonyms instead), then, in the other order, the reader first (it answers the
     * coordinator's old synonyms from its previous ones). Complete results either way, no warning, re-plans off.
     */
    @Test
    void aLateSynonymsPublishOnOneNodeStillAnswersComplete() throws Exception {
        Path tmp = Files.createTempDirectory("syn-late");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String before = "old, rule\n";
        String published = "calculator, adder\n";
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER)) {
            fx.indexBoth();
            try (TestReaderNode c = new TestReaderNode(tmp.resolve("c"), store, BUCKET, "c", List.of(PY), EMBEDDER, before);
                 TestReaderNode r = new TestReaderNode(tmp.resolve("r"), store, BUCKET, "r", List.of(JAVA), EMBEDDER,
                         before)) {
                c.awaitShard(JAVA, 1);
                c.ds().retries = false;
                assertComplete(search(c, null, "multiply Calculator"), "same synonyms");

                publish(c.configBase.resolve("synonyms.txt"), published, 5000); // the coordinator first
                SearchResponse coordinatorFirst = search(c, null, "multiply Calculator");
                assertComplete(coordinatorFirst, "the reader is late");
                assertThat(coordinatorFirst.trace().notes()).anyMatch(n -> n.contains("previous synonyms"));

                publish(r.configBase.resolve("synonyms.txt"), published, 5000);
                assertComplete(search(c, null, "multiply Calculator"), "both on the new synonyms");

                publish(r.configBase.resolve("synonyms.txt"), before, 10_000); // now the reader first
                assertComplete(search(c, null, "multiply Calculator"), "the coordinator is late");
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    private static void publish(Path synonyms, String text, long aheadMillis) throws Exception {
        Files.writeString(synonyms, text);
        Files.setLastModifiedTime(synonyms, FileTime.fromMillis(System.currentTimeMillis() + aheadMillis));
    }

    private static void assertComplete(SearchResponse r, String when) {
        assertThat(r.warnings()).as(when + ": no warning").isEmpty();
        assertThat(r.results()).as(when + ": the sample-app shard answered").anyMatch(x -> x.project().equals(JAVA));
        assertThat(r.trace().notes()).as(when + ": no re-plan").noneMatch(n -> n.startsWith("re-planned"));
    }

    /**
     * The re-plan itself loses every shard (here: both readers die before it runs). The first attempt's
     * answer, partial with its warning, is returned, not the re-plan's failure.
     */
    @Test
    void aReplanThatLosesEveryShardKeepsTheFirstPartialAnswer() throws Exception {
        Path tmp = Files.createTempDirectory("replan-keep");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String project = "swap";
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        List<ShardId> shards = List.of(new ShardId(project + "~ring=s01"), new ShardId(project + "~ring=s02"));
        Path src = tmp.resolve("src");
        for (int i = 0; i < 12; i++) {
            SplitProjectSearchTest.writeClass(src, "src/main/java/com/example/p" + i + "/Kestrel" + i + ".java",
                    "com.example.p" + i, "Kestrel" + i, "kestrel calculator " + i);
        }
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, null)) {
            fx.indexSplit(src, project, ring2, shards, false);
            fx.indexBoth();
            try (TestReaderNode x = new TestReaderNode(tmp.resolve("x"), store, BUCKET, "x",
                    List.of(ShardId.wildcardOf(project)), EMBEDDER, null, NO_RETENTION);
                 TestReaderNode r = new TestReaderNode(tmp.resolve("r"), store, BUCKET, "r", List.of(JAVA), EMBEDDER, null);
                 TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(PY), EMBEDDER, null)) {
                for (ShardId s : shards) k.awaitShard(s.value(), 1);
                k.awaitShard(JAVA, 1);
                nextRun(fx, src, project, ring2, shards, 1);
                swapOnceAfterStats(x);
                DistributedSearch.beforeReplan = () -> { // nothing answers the re-plan
                    x.kill();
                    r.kill();
                };
                SearchResponse resp = k.engine.searchWithTrace(new SearchRequest("Calculator kestrel", SearchType.KEYWORD,
                        null, List.of(JAVA, project), 20, "text", null, null, 0), false);
                assertThat(resp.results()).as("the first attempt's sample-app results")
                        .isNotEmpty().allSatisfy(res -> assertThat(res.project()).isEqualTo(JAVA));
                assertThat(resp.warnings()).as("its warning").anyMatch(w -> w.contains("generation changed"));
                assertThat(resp.trace().notes()).anyMatch(n -> n.startsWith("the re-plan failed"));
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    /**
     * A re-plan shares the original query's budget: it waits at most half of what is left (2 s at most)
     * and the re-planned query keeps the original deadline, so the whole query stays within its timeout
     * even when nothing has changed by the re-plan (here a coordinator whose synonyms never catch up).
     */
    @Test
    void aReplanNeverExceedsTheOriginalQueryTimeout() throws Exception {
        Path tmp = Files.createTempDirectory("replan-budget");
        InMemoryObjectStore store = new InMemoryObjectStore();
        int timeoutMillis = 1500;
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER)) {
            fx.indexBoth();
            try (TestReaderNode c = new TestReaderNode(tmp.resolve("c"), store, BUCKET, "c", List.of(PY), EMBEDDER,
                    "old, rule\n", cfg -> {
                        cfg.setFanoutTimeoutMillis(1000);
                        cfg.setQueryTimeoutMillis(timeoutMillis);
                    });
                 TestReaderNode r = new TestReaderNode(tmp.resolve("r"), store, BUCKET, "r", List.of(JAVA), EMBEDDER,
                         "calculator, adder\n")) {
                c.awaitShard(JAVA, 1);
                search(c, null, "multiply Calculator"); // warm-up
                for (int i = 0; i < 3; i++) {
                    long t0 = System.nanoTime();
                    SearchResponse resp = search(c, null, "multiply Calculator");
                    long tookMillis = (System.nanoTime() - t0) / 1_000_000;
                    assertThat(resp.trace().notes()).anyMatch(n -> n.startsWith("re-planned once"));
                    assertThat(resp.warnings()).as("still refused: the partial answer").anyMatch(w -> w.contains("synonyms"));
                    assertThat(tookMillis).as("within the query timeout of " + timeoutMillis + " ms (plus scheduling)")
                            .isLessThan(timeoutMillis + 400);
                }
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    /**
     * A hybrid query (its vector stage overlapped with the keyword rounds) re-plans as a keyword query does:
     * the swap between the statistics and search rounds loses both shards of the split project in the
     * control, and one re-plan answers complete.
     */
    @Test
    void aHybridQuerySwappingAShardIsFollowedByOneReplan() throws Exception {
        Path tmp = Files.createTempDirectory("replan-hybrid");
        InMemoryObjectStore store = new InMemoryObjectStore();
        String project = "swap";
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        List<ShardId> shards = List.of(new ShardId(project + "~ring=s01"), new ShardId(project + "~ring=s02"));
        Path src = tmp.resolve("src");
        for (int i = 0; i < 12; i++) {
            SplitProjectSearchTest.writeClass(src, "src/main/java/com/example/p" + i + "/Kestrel" + i + ".java",
                    "com.example.p" + i, "Kestrel" + i, "kestrel hover " + i);
        }
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER)) {
            fx.indexSplit(src, project, ring2, shards, false);
            fx.index(JAVA, false);
            try (TestReaderNode x = new TestReaderNode(tmp.resolve("x"), store, BUCKET, "x",
                    List.of(ShardId.wildcardOf(project)), EMBEDDER, null, NO_RETENTION);
                 TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(JAVA), EMBEDDER, null)) {
                for (ShardId s : shards) k.awaitShard(s.value(), 1);
                assertThat(search(k, project, "kestrel", SearchType.HYBRID).warnings()).as("before").isEmpty();

                k.ds().retries = false;
                nextRun(fx, src, project, ring2, shards, 1);
                swapOnceAfterStats(x);
                assertThatThrownBy(() -> search(k, project, "kestrel", SearchType.HYBRID)).as("without the re-plan")
                        .hasMessageContaining("generation changed during the query");

                k.ds().retries = true;
                nextRun(fx, src, project, ring2, shards, 2);
                swapOnceAfterStats(x);
                SearchResponse on = search(k, project, "kestrel", SearchType.HYBRID);
                assertThat(on.warnings()).as("with the re-plan").isEmpty();
                assertThat(on.results().stream().map(res -> res.className()).distinct().count())
                        .as("every Kestrel class, from both shards").isEqualTo(12);
                assertThat(on.trace().notes()).anyMatch(n -> n.startsWith("re-planned once"));
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    /**
     * The re-plan's budget is unchanged for a hybrid query whose embedding runs alongside its keyword
     * rounds: within the original timeout, with the same answer as the sequential coordinator.
     */
    @Test
    void aHybridReplanNeverExceedsTheOriginalQueryTimeout() throws Exception {
        Path tmp = Files.createTempDirectory("replan-hybrid-budget");
        InMemoryObjectStore store = new InMemoryObjectStore();
        int timeoutMillis = 1500;
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER)) {
            fx.indexBoth();
            try (TestReaderNode c = new TestReaderNode(tmp.resolve("c"), store, BUCKET, "c", List.of(PY), EMBEDDER,
                    "old, rule\n", cfg -> {
                        cfg.setFanoutTimeoutMillis(1000);
                        cfg.setQueryTimeoutMillis(timeoutMillis);
                    });
                 TestReaderNode r = new TestReaderNode(tmp.resolve("r"), store, BUCKET, "r", List.of(JAVA), EMBEDDER,
                         "calculator, adder\n")) {
                c.awaitShard(JAVA, 1);
                search(c, null, "multiply Calculator", SearchType.HYBRID); // warm-up
                c.ds().overlapEmbedding = false;
                SearchResponse control = search(c, null, "multiply Calculator", SearchType.HYBRID);
                c.ds().overlapEmbedding = true;
                for (int i = 0; i < 3; i++) {
                    long t0 = System.nanoTime();
                    SearchResponse resp = search(c, null, "multiply Calculator", SearchType.HYBRID);
                    long tookMillis = (System.nanoTime() - t0) / 1_000_000;
                    assertThat(resp.trace().notes()).anyMatch(n -> n.startsWith("re-planned once"));
                    assertThat(resp.warnings()).as("still refused: the partial answer").anyMatch(w -> w.contains("synonyms"))
                            .isEqualTo(control.warnings());
                    assertThat(DistributedSearchTest.bits(resp.results())).isEqualTo(DistributedSearchTest.bits(control.results()));
                    assertThat(tookMillis).as("within the query timeout of " + timeoutMillis + " ms (plus scheduling)")
                            .isLessThan(timeoutMillis + 400);
                }
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }

    /** Publishes the project's next index run (a changed file), which reader x has not activated yet. */
    private static void nextRun(ClusterFixture fx, Path src, String project, ShardSpec spec, List<ShardId> shards,
                                int n) throws Exception {
        Path f = src.resolve("src/main/java/com/example/p0/Kestrel0.java");
        Files.writeString(f, Files.readString(f).replace("kestrel hover 0", "kestrel hover 0 run " + n));
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 5000L * n));
        fx.indexSplit(src, project, spec, shards, true);
    }

    /** Right after the next query's statistics round, x activates what is published (once). */
    private static void swapOnceAfterStats(TestReaderNode x) {
        AtomicBoolean done = new AtomicBoolean();
        DistributedSearch.afterStatsRound = q -> {
            if (!done.compareAndSet(false, true)) return;
            try {
                x.parts.runtime().syncNow();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        };
    }

    private static SearchResponse search(TestReaderNode node, String project) throws Exception {
        return search(node, project, "kestrel");
    }

    private static SearchResponse search(TestReaderNode node, String project, String q) throws Exception {
        return node.engine.searchWithTrace(new SearchRequest(q, SearchType.KEYWORD, project, null, 50, "text", null,
                null, 0), false);
    }

    private static SearchResponse search(TestReaderNode node, String project, String q, SearchType type) throws Exception {
        return node.engine.searchWithTrace(new SearchRequest(q, type, project, null, 50, "text", null, null, 0), false);
    }
}
