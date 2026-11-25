package com.pharos.search.distributed;

import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchRequest.SearchType;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static com.pharos.cluster.ClusterFixture.PY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ruling P6-R20: the trace carries each result's raw (pre-graph-boost) score per retrieval stage, for a query
 * searched in process (a shard held here) and for one fanned out to another reader; the final score is that
 * raw score shaped (graph boost and the other multipliers), so the two differ for some result.
 */
class RawScoreTraceTest {

    private static final String BUCKET = "test-bucket";
    private static final ParityCorpus.HashEmbeddings EMBEDDER = new ParityCorpus.HashEmbeddings();

    @Test
    void theTraceCarriesEveryResultsRawScore() throws Exception {
        Path tmp = Files.createTempDirectory("raw-scores");
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture fx = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER)) {
            fx.indexBoth();
            try (TestReaderNode k = new TestReaderNode(tmp.resolve("k"), store, BUCKET, "k", List.of(JAVA), EMBEDDER, null);
                 TestReaderNode r = new TestReaderNode(tmp.resolve("r"), store, BUCKET, "r", List.of(PY), EMBEDDER, null)) {
                k.awaitShard(PY, 1);
                k.awaitShard(JAVA, 1);
                for (String project : List.of(JAVA, PY)) {
                    SearchResponse resp = k.engine.searchWithTrace(new SearchRequest("calculator add", SearchType.KEYWORD,
                            project, null, 10, "text", null, null, 0), false);
                    assertThat(resp.results()).as(project).isNotEmpty();
                    Map<String, Float> raw = resp.trace().rawScores().get("keyword");
                    assertThat(raw).as(project + ": keyword raw scores").isNotNull();
                    for (SearchResult res : resp.results()) {
                        assertThat(raw).as(project + ": raw score of " + res.id()).containsKey(res.id());
                    }
                    assertThat(resp.results()).as(project + ": shaped scores differ from raw ones")
                            .anyMatch(res -> raw.get(res.id()) != res.score());
                }
            }
        } finally {
            SplitProjectSearchTest.deleteTree(tmp);
        }
    }
}
