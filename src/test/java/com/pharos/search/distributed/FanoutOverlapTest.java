package com.pharos.search.distributed;

import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.embedding.EmbeddingException;
import com.pharos.search.SearchRequest.SearchType;
import com.pharos.search.SearchResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static com.pharos.cluster.ClusterFixture.PY;
import static com.pharos.search.distributed.DistributedSearchTest.bits;
import static com.pharos.search.distributed.DistributedSearchTest.search;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The coordinator embeds a hybrid or unified query while the shard rounds that do not need the vector
 * run: the embedding starts on {@code pharos-query-embed-*} before the keyword rounds (hybrid) or the
 * statistics round (unified) are answered. Results, warnings, the query deadline and the re-plan are
 * those of the sequential coordinator (the control: {@link DistributedSearch#overlapEmbedding} off).
 */
class FanoutOverlapTest {

    private static final String BUCKET = "test-bucket";
    private static final ParityCorpus.HashEmbeddings EMBEDDER = new ParityCorpus.HashEmbeddings();
    private static final String POOL = "pharos-query-embed-";

    @TempDir Path tmp;

    /** Hash embeddings recording, per call, the thread and what {@link #probe} said when the call began. */
    static final class ProbeEmbeddings extends ParityCorpus.HashEmbeddings {
        final List<String> threads = Collections.synchronizedList(new ArrayList<>());
        final List<Boolean> probes = Collections.synchronizedList(new ArrayList<>());
        volatile BooleanSupplier probe = () -> false;
        volatile long sleepMillis;
        volatile boolean fail;

        void reset(BooleanSupplier p) {
            threads.clear();
            probes.clear();
            probe = p;
        }

        @Override
        public float[] embed(String text) {
            threads.add(Thread.currentThread().getName());
            probes.add(probe.getAsBoolean());
            if (sleepMillis > 0) {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (fail) throw new EmbeddingException("probe failure", null);
            return super.embed(text);
        }
    }

    private record Small(ClusterFixture fixture, TestReaderNode coordinator, TestReaderNode replica)
            implements AutoCloseable {
        @Override
        public void close() throws Exception {
            replica.close();
            coordinator.close();
            fixture.close();
        }
    }

    private int clusters;

    /** sample-python-app held by the coordinator, sample-app by one replica. */
    private Small small(ProbeEmbeddings coordinatorEmbedder,
                        java.util.function.Consumer<com.pharos.config.ClusterConfig> config) throws Exception {
        Path home = tmp.resolve("small-" + clusters++);
        InMemoryObjectStore store = new InMemoryObjectStore();
        ClusterFixture f = new ClusterFixture(home.resolve("fixture"), store, store, BUCKET, EMBEDDER);
        f.indexBoth();
        TestReaderNode c = new TestReaderNode(home.resolve("c"), store, BUCKET, "c", List.of(PY), coordinatorEmbedder,
                null, config);
        TestReaderNode r = new TestReaderNode(home.resolve("r1"), store, BUCKET, "r1", List.of(JAVA), EMBEDDER, null);
        c.awaitShard(JAVA, 1);
        return new Small(f, c, r);
    }

    /** True once {@code node} has answered a call ending in {@code path} (of {@code retriever}, when set). */
    static boolean answered(TestReaderNode node, String path, String retriever) {
        synchronized (node.calls) {
            return node.calls.stream().anyMatch(c -> c.path().endsWith(path) && c.status() == 200
                    && (retriever == null || retriever.equals(c.requestJson().path("retriever").asText())));
        }
    }

    @Test
    void hybridEmbedsWhileTheKeywordRoundsRun() throws Exception {
        ProbeEmbeddings probe = new ProbeEmbeddings();
        try (Small s = small(probe, c -> { })) {
            String q = "GreetingService farewell";
            TestReaderNode r = s.replica();
            DistributedSearch ds = s.coordinator().ds();

            ds.overlapEmbedding = false; // control: embedded by the vector stage, after the keyword rounds
            r.drainCalls();
            probe.reset(() -> answered(r, "/shard-search", "keyword"));
            SearchResponse off = search(s.coordinator(), q, SearchType.HYBRID, null, 10);
            assertThat(probe.probes).as("control: the keyword round had answered").first().isEqualTo(true);
            assertThat(probe.threads).noneMatch(t -> t.startsWith(POOL));

            ds.overlapEmbedding = true;
            r.stallPath = "/internal/term-stats";
            r.stallMillis = 300;
            r.drainCalls();
            probe.reset(() -> answered(r, "/term-stats", null) || answered(r, "/shard-search", "keyword"));
            SearchResponse on;
            try {
                on = search(s.coordinator(), q, SearchType.HYBRID, null, 10);
            } finally {
                r.stallPath = null;
            }
            assertThat(probe.probes).as("embedded before the statistics round answered").containsExactly(false);
            assertThat(probe.threads).singleElement().asString().startsWith(POOL);
            assertThat(bits(on.results())).isNotEmpty().isEqualTo(bits(off.results()));
            assertThat(on.results()).anyMatch(x -> x.project().equals(JAVA)).anyMatch(x -> x.project().equals(PY));
            assertThat(on.warnings()).isEqualTo(off.warnings());
        } finally {
            probe.reset(() -> false);
        }
    }

    @Test
    void unifiedEmbedsWhileTheStatisticsRoundRuns() throws Exception {
        ProbeEmbeddings probe = new ProbeEmbeddings();
        try (Small s = small(probe, c -> { })) {
            String q = "multiply two numbers";
            TestReaderNode r = s.replica();
            DistributedSearch ds = s.coordinator().ds();

            ds.overlapEmbedding = false; // control: the statistics round first, then the embedding
            r.drainCalls();
            probe.reset(() -> answered(r, "/term-stats", null));
            SearchResponse off = search(s.coordinator(), q, SearchType.UNIFIED, null, 10);
            assertThat(probe.probes).first().isEqualTo(true);

            ds.overlapEmbedding = true;
            r.stallPath = "/internal/term-stats";
            r.stallMillis = 300;
            r.drainCalls();
            probe.reset(() -> answered(r, "/term-stats", null));
            SearchResponse on;
            try {
                on = search(s.coordinator(), q, SearchType.UNIFIED, null, 10);
            } finally {
                r.stallPath = null;
            }
            assertThat(probe.probes).containsExactly(false);
            assertThat(probe.threads).singleElement().asString().startsWith(POOL);
            assertThat(bits(on.results())).isNotEmpty().isEqualTo(bits(off.results()));
            assertThat(on.warnings()).isEqualTo(off.warnings());
        }
    }

    @Test
    void vectorOnlyEmbedsOnTheQueryThread() throws Exception {
        ProbeEmbeddings probe = new ProbeEmbeddings();
        try (Small s = small(probe, c -> { })) {
            SearchResponse v = search(s.coordinator(), "greeting message", SearchType.VECTOR, null, 10);
            assertThat(v.results()).isNotEmpty();
            assertThat(probe.threads).isNotEmpty().noneMatch(t -> t.startsWith(POOL));
        }
    }

    /** A failing embedding warns and leaves hybrid keyword-only, exactly as when embedded on the query thread. */
    @Test
    void aFailedPrefetchedEmbeddingWarnsAsBefore() throws Exception {
        ProbeEmbeddings probe = new ProbeEmbeddings();
        probe.fail = true;
        try (Small s = small(probe, c -> { })) {
            String q = "GreetingService farewell";
            DistributedSearch ds = s.coordinator().ds();
            ds.overlapEmbedding = false;
            SearchResponse off = search(s.coordinator(), q, SearchType.HYBRID, null, 10);
            ds.overlapEmbedding = true;
            probe.reset(() -> false);
            SearchResponse on = search(s.coordinator(), q, SearchType.HYBRID, null, 10);
            assertThat(probe.threads).singleElement().asString().startsWith(POOL);
            assertThat(on.warnings()).isEqualTo(off.warnings())
                    .anyMatch(w -> w.startsWith("vector embedding failed for model ") && w.endsWith("results are keyword-only"));
            assertThat(bits(on.results())).isNotEmpty().isEqualTo(bits(off.results()));
        }
    }

    /**
     * The query deadline still covers every round, and only the rounds: with a slow embedding overlapped
     * with a stalled keyword round, the query ends at its deadline (the control pays the embedding after
     * it), with the same partial answer and warning.
     */
    @Test
    void theDeadlineStillCoversEveryRound() throws Exception {
        ProbeEmbeddings probe = new ProbeEmbeddings();
        probe.sleepMillis = 1000;
        try (Small s = small(probe, cfg -> {
            cfg.setFanoutTimeoutMillis(1400);
            cfg.setQueryTimeoutMillis(1500);
        })) {
            TestReaderNode r = s.replica();
            DistributedSearch ds = s.coordinator().ds();
            String q = "GreetingService multiply";
            r.stallPath = "/internal/shard-search";
            r.stallMillis = 10_000;
            try {
                ds.overlapEmbedding = false;
                long t0 = System.nanoTime();
                SearchResponse off = search(s.coordinator(), q, SearchType.HYBRID, null, 10);
                long offMs = (System.nanoTime() - t0) / 1_000_000;
                ds.overlapEmbedding = true;
                t0 = System.nanoTime();
                SearchResponse on = search(s.coordinator(), q, SearchType.HYBRID, null, 10);
                long onMs = (System.nanoTime() - t0) / 1_000_000;
                assertThat(offMs).as("control: the stalled round's 1500 ms, then the 1000 ms embedding")
                        .isGreaterThanOrEqualTo(2_400);
                assertThat(onMs).as("overlapped: the 1500 ms query timeout").isLessThan(2_200);
                assertThat(on.results()).isNotEmpty().allSatisfy(x -> assertThat(x.project()).isEqualTo(PY));
                assertThat(bits(on.results())).isEqualTo(bits(off.results()));
                assertThat(on.warnings()).isEqualTo(off.warnings()).singleElement().asString()
                        .startsWith("shard " + JAVA + " unavailable: ");
            } finally {
                r.stallPath = null;
            }
        }
    }
}
