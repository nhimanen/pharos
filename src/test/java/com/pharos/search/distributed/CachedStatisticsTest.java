package com.pharos.search.distributed;

import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.search.SearchRequest.SearchType;
import com.pharos.search.SearchResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static com.pharos.cluster.ClusterFixture.PY;
import static com.pharos.search.distributed.DistributedSearchTest.bits;
import static com.pharos.search.distributed.DistributedSearchTest.search;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The keyword round with cached statistics ({@code DistributedSearch.Query#keywordRoundWithCachedStats}):
 * one round trip when every remote shard confirms the cached statistics, and the statistics round after
 * all when one does not (a wrong entry, a new generation): never another score.
 */
class CachedStatisticsTest {

    private static final String BUCKET = "test-bucket";
    private static final ParityCorpus.HashEmbeddings EMBEDDER = new ParityCorpus.HashEmbeddings();
    private static final String CACHED = "(cached, confirmed by the search round)";

    @TempDir Path tmp;

    private record Small(ClusterFixture fixture, TestReaderNode coordinator, TestReaderNode replica)
            implements AutoCloseable {
        @Override
        public void close() throws Exception {
            replica.close();
            coordinator.close();
            fixture.close();
        }
    }

    private Small small() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        ClusterFixture f = new ClusterFixture(tmp.resolve("fixture"), store, store, BUCKET, EMBEDDER);
        f.indexBoth();
        TestReaderNode c = new TestReaderNode(tmp.resolve("c"), store, BUCKET, "c", List.of(PY), EMBEDDER, null);
        TestReaderNode r = new TestReaderNode(tmp.resolve("r1"), store, BUCKET, "r1", List.of(JAVA), EMBEDDER, null);
        c.awaitShard(JAVA, 1);
        return new Small(f, c, r);
    }

    private static long count(List<TestReaderNode.Call> calls, String path) {
        return calls.stream().filter(c -> c.path().endsWith(path) && c.status() == 200).count();
    }

    @Test
    void aRepeatedQueryNeedsOneRoundTrip() throws Exception {
        try (Small s = small()) {
            String q = "GreetingService farewell";
            TestReaderNode r = s.replica();
            r.drainCalls();
            SearchResponse first = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            List<TestReaderNode.Call> firstCalls = r.drainCalls();
            assertThat(count(firstCalls, "/internal/term-stats")).isEqualTo(1);
            assertThat(count(firstCalls, "/internal/shard-search")).isEqualTo(1);
            assertThat(first.trace().statistics()).doesNotEndWith(CACHED);

            SearchResponse second = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            List<TestReaderNode.Call> calls = r.drainCalls();
            assertThat(count(calls, "/internal/term-stats")).as("no statistics round").isZero();
            assertThat(calls).singleElement().satisfies(c -> {
                assertThat(c.path()).endsWith("/internal/shard-search");
                assertThat(c.requestJson().path("statsCheck").path("terms").isEmpty()).isFalse();
                assertThat(c.responseJson().path("shardStats").path("fields").isEmpty()).isFalse();
            });
            assertThat(second.trace().statistics()).endsWith(CACHED);
            assertThat(second.results()).anyMatch(x -> x.project().equals(JAVA)).anyMatch(x -> x.project().equals(PY));
            assertThat(bits(second.results())).isEqualTo(bits(first.results()));
            assertThat(second.warnings()).isEqualTo(first.warnings()).isEmpty();

            // hybrid's keyword stage takes the same path, with the same answer as the statistics round
            SearchResponse hybrid = search(s.coordinator(), q, SearchType.HYBRID, null, 10);
            assertThat(hybrid.trace().statistics()).endsWith(CACHED);
            s.coordinator().ds().cachedStatistics = false;
            SearchResponse control = search(s.coordinator(), q, SearchType.HYBRID, null, 10);
            s.coordinator().ds().cachedStatistics = true;
            assertThat(control.trace().statistics()).doesNotEndWith(CACHED);
            assertThat(bits(hybrid.results())).isEqualTo(bits(control.results()));
        }
    }

    /** A wrong cache entry is caught by the shard's own statistics: one more round, the same scores. */
    @Test
    void aWrongEntryCostsARoundNotAScore() throws Exception {
        try (Small s = small()) {
            String q = "GreetingService farewell";
            TestReaderNode r = s.replica();
            SearchResponse first = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            s.coordinator().ds().statsCache.corruptForTest();
            r.drainCalls();
            SearchResponse again = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            List<TestReaderNode.Call> calls = r.drainCalls();
            assertThat(count(calls, "/internal/shard-search")).as("the unconfirmed round, then the real one").isEqualTo(2);
            assertThat(count(calls, "/internal/term-stats")).isEqualTo(1);
            assertThat(again.trace().statistics()).doesNotEndWith(CACHED);
            assertThat(bits(again.results())).isEqualTo(bits(first.results()));
            assertThat(again.warnings()).isEmpty();
            // the shards' statistics replaced the wrong entries: the next one is a single round trip again
            r.drainCalls();
            SearchResponse third = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            assertThat(count(r.drainCalls(), "/internal/term-stats")).isZero();
            assertThat(third.trace().statistics()).endsWith(CACHED);
            assertThat(bits(third.results())).isEqualTo(bits(first.results()));
        }
    }

    /**
     * The replica activates a new generation of its shard between two queries: the cached statistics of
     * the old one are not used for it (its own statistics disagree, or the advertised generation is new),
     * and the query scores as the statistics round of the new generation does.
     */
    @Test
    void aNewGenerationIsScoredWithItsOwnStatistics() throws Exception {
        try (Small s = small()) {
            String q = "tripleTheValue Calculator multiply";
            search(s.coordinator(), q, SearchType.KEYWORD, null, 10); // caches generation 1's statistics
            Path calc = s.fixture().source.resolve(JAVA).resolve("src/main/java/com/example/Calculator.java");
            Files.writeString(calc, Files.readString(calc).replace("class Calculator {",
                    "class Calculator {\n    public int tripleTheValue(int x) { return 3 * x; }\n"));
            Files.setLastModifiedTime(calc, FileTime.fromMillis(System.currentTimeMillis() + 5000));
            s.fixture().index(JAVA, true);
            s.replica().parts.runtime().syncNow();
            SearchResponse after = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            assertThat(after.results()).as("generation 2 searched").anyMatch(x -> x.id().contains("tripleTheValue"));
            assertThat(after.trace().statistics()).doesNotEndWith(CACHED);
            s.coordinator().ds().cachedStatistics = false;
            SearchResponse control = search(s.coordinator(), q, SearchType.KEYWORD, null, 10);
            s.coordinator().ds().cachedStatistics = true;
            assertThat(bits(after.results())).isEqualTo(bits(control.results()));
        }
    }
}
