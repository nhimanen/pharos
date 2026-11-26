package com.pharos.search.distributed;

import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.search.SearchRequest.SearchType;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static com.pharos.cluster.ClusterFixture.PY;
import static com.pharos.search.distributed.DistributedSearchTest.search;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Vector snippet hints over the fan-out: a remote shard without vectors of the model is not asked (it has
 * no hints, as on a single node), and the calls of several remote shards run in parallel. The decorated
 * results are those of the sequential coordinator asking every shard (the control) and of a single node.
 */
class SnippetHintsTest {

    private static final String BUCKET = "test-bucket";
    private static final ParityCorpus.HashEmbeddings EMBEDDER = new ParityCorpus.HashEmbeddings();
    private static final String Q = "GreetingService greeting message";

    @TempDir Path tmp;

    private static List<SearchResult> decorated(TestReaderNode node, SearchType type) throws Exception {
        SearchResponse resp = search(node, Q, type, null, 10);
        return node.engine.newSnippetDecorator(15, resp).decorate(resp.results(), Q);
    }

    private static long snippetCalls(TestReaderNode n) {
        return n.drainCalls().stream().filter(c -> c.path().endsWith("/internal/snippets")).count();
    }

    @Test
    void aShardWithoutTheModelsVectorsIsNotAskedForHints() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture f = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER)) {
            f.index(PY, false, true);
            f.index(JAVA, false, false); // --no-embed
            try (TestReaderNode c = new TestReaderNode(tmp.resolve("c"), store, BUCKET, "c", List.of(PY), EMBEDDER, null);
                 TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(JAVA), EMBEDDER, null);
                 TestReaderNode one = new TestReaderNode(tmp.resolve("one"), store, BUCKET, "one", List.of(), EMBEDDER, null)) {
                for (TestReaderNode n : List.of(c, k, one)) {
                    n.awaitShard(PY, 2);
                    n.awaitShard(JAVA, 2);
                }
                DistributedSearch ds = c.ds();
                ds.snippetShortcuts = false;
                k.drainCalls();
                List<SearchResult> control = decorated(c, SearchType.HYBRID);
                assertThat(control).anyMatch(r -> r.project().equals(JAVA));
                assertThat(snippetCalls(k)).as("control: the shard is asked").isEqualTo(1);
                ds.snippetShortcuts = true;
                List<SearchResult> on = decorated(c, SearchType.HYBRID);
                assertThat(snippetCalls(k)).as("no vectors of the model: not asked").isZero();
                assertThat(on).isEqualTo(control);
                java.util.Map<String, Object> single = new java.util.HashMap<>();
                for (SearchResult r : decorated(one, SearchType.HYBRID)) single.put(r.id(), r.snippet());
                List<SearchResult> shared = on.stream().filter(r -> r.project().equals(JAVA) && single.containsKey(r.id())).toList();
                assertThat(shared).as("results of the shard the single node returns too").isNotEmpty()
                        .allSatisfy(r -> assertThat(r.snippet()).as(r.id()).isEqualTo(single.get(r.id())));
            }
        }
    }

    @Test
    void severalRemoteShardsAreAskedInParallel() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture f = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER)) {
            f.indexBoth();
            f.indexSource(DistributedSearchTest.skewSource(tmp.resolve("skew-c"), 2, 2, 1), "skew-c");
            try (TestReaderNode c = new TestReaderNode(tmp.resolve("c"), store, BUCKET, "c", List.of("skew-c"), EMBEDDER, null);
                 TestReaderNode r1 = new TestReaderNode(tmp.resolve("r1"), store, BUCKET, "r1", List.of(JAVA), EMBEDDER, null);
                 TestReaderNode r2 = new TestReaderNode(tmp.resolve("r2"), store, BUCKET, "r2", List.of(PY), EMBEDDER, null)) {
                c.awaitShard(JAVA, 1);
                c.awaitShard(PY, 1);
                DistributedSearch ds = c.ds();
                for (TestReaderNode r : List.of(r1, r2)) {
                    r.stallPath = "/internal/snippets";
                    r.stallMillis = 500;
                }
                try {
                    ds.parallelSnippets = false;
                    long t0 = System.nanoTime();
                    List<SearchResult> control = decorated(c, SearchType.HYBRID);
                    long controlMs = (System.nanoTime() - t0) / 1_000_000;
                    assertThat(control).anyMatch(r -> r.project().equals(JAVA)).anyMatch(r -> r.project().equals(PY));
                    assertThat(snippetCalls(r1) + snippetCalls(r2)).isEqualTo(2);
                    ds.parallelSnippets = true;
                    t0 = System.nanoTime();
                    List<SearchResult> on = decorated(c, SearchType.HYBRID);
                    long onMs = (System.nanoTime() - t0) / 1_000_000;
                    assertThat(snippetCalls(r1) + snippetCalls(r2)).isEqualTo(2);
                    assertThat(on).isEqualTo(control);
                    assertThat(controlMs).as("control: two stalled calls in a row").isGreaterThanOrEqualTo(1000);
                    assertThat(onMs).as("in parallel").isLessThan(900);
                } finally {
                    r1.stallPath = null;
                    r2.stallPath = null;
                }
            }
        }
    }
}
