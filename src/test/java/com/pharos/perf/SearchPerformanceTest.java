package com.pharos.perf;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.MavenPomReader;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.apache.lucene.index.IndexReader;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Search throughput benchmark against a Lucene-indexed codebase.
 *
 * Measures queries-per-second for keyword search by:
 *   1. Indexing the Apache Lucene source tree (same corpus as CrawlerPerformanceTest).
 *   2. Running a JIT warm-up phase.
 *   3. Running a timed measurement phase and reporting QPS + latency percentiles.
 *   4. Running a concurrent (multi-threaded) phase to find peak parallel throughput.
 *
 * Run with:
 *   mvn test -Dtest=SearchPerformanceTest -Dgroups=perf
 *
 * QPS floor assertions are intentionally generous — they exist only to catch
 * catastrophic regressions (e.g., accidental I/O on every query).
 */
@Tag("perf")
class SearchPerformanceTest {

    private static final Path LUCENE_ROOT =
            Path.of("/home/nhimanen/projects/lucene/lucene/lucene");

    /** Seconds to spend on warm-up (queries not measured). */
    private static final int WARMUP_SECONDS = 3;

    /** Seconds to spend on the timed measurement pass. */
    private static final int MEASURE_SECONDS = 10;

    /** Thread counts for the concurrency sweep. */
    private static final int[] THREAD_COUNTS = {1, 2, 4, 8};

    /** Minimum acceptable single-threaded QPS — fails on catastrophic regressions only. */
    private static final double MIN_QPS_SINGLE = 100.0;

    /**
     * Representative mix of queries that exercise different Lucene fields:
     * method names, javadoc tokens, body tokens, class names, and multi-token phrases.
     */
    private static final List<String> QUERIES = List.of(
            // Method / class name matches (methodName field, 3× boost)
            "search",
            "index",
            "query",
            "parse",
            "merge",
            "flush",
            "commit",
            "delete",
            "close",
            "open",
            "read",
            "write",
            "score",
            "sort",
            "filter",
            // Multi-token — exercises BooleanQuery path
            "boolean query",
            "term query",
            "index writer",
            "directory reader",
            "field boost",
            "document field",
            "segment merge",
            "token stream",
            "query parser",
            "similarity score",
            // Longer phrases — more realistic MCP / chat-driven queries
            "find all methods that throw exceptions",
            "calculate relevance score for documents",
            "open index for reading and searching",
            "flush pending changes to disk",
            "merge small segments for performance"
    );

    @TempDir
    Path tempDir;

    // ----------------------------------------------------------------------------------
    // Single-threaded QPS + latency percentiles
    // ----------------------------------------------------------------------------------

    /**
     * Indexes the Lucene source tree, then runs repeated keyword searches and
     * reports queries-per-second and p50/p95/p99 latencies.
     */
    @Test
    void benchmark_searchQps_singleThread() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(LUCENE_ROOT),
                "Lucene source repo not found at " + LUCENE_ROOT + " — skipping");

        SearchHarness harness = new SearchHarness(tempDir);
        harness.indexCorpus();

        System.out.println("\n[search-qps] Warming up for " + WARMUP_SECONDS + "s …");
        long warmupDeadline = System.currentTimeMillis() + WARMUP_SECONDS * 1_000L;
        int wi = 0;
        while (System.currentTimeMillis() < warmupDeadline) {
            harness.runQuery(QUERIES.get(wi++ % QUERIES.size()));
        }

        System.out.println("[search-qps] Measuring for " + MEASURE_SECONDS + "s …");
        List<Long> latenciesNs = new ArrayList<>(4096);
        long measureDeadline = System.currentTimeMillis() + MEASURE_SECONDS * 1_000L;
        int qi = 0;
        while (System.currentTimeMillis() < measureDeadline) {
            long t0 = System.nanoTime();
            harness.runQuery(QUERIES.get(qi++ % QUERIES.size()));
            latenciesNs.add(System.nanoTime() - t0);
        }

        printReport("Single-threaded keyword search QPS", 1, latenciesNs, MEASURE_SECONDS);

        double qps = (double) latenciesNs.size() / MEASURE_SECONDS;
        org.assertj.core.api.Assertions.assertThat(qps)
                .as("single-threaded QPS >= %s", MIN_QPS_SINGLE)
                .isGreaterThanOrEqualTo(MIN_QPS_SINGLE);
    }

    // ----------------------------------------------------------------------------------
    // Concurrent QPS sweep
    // ----------------------------------------------------------------------------------

    /**
     * Sweeps over several thread counts and reports peak QPS and latency percentiles
     * for each level of concurrency.  Uses the same index built for the single-threaded
     * test (if re-run in the same JVM via surefire) or re-indexes.
     */
    @Test
    void benchmark_searchQps_concurrencySweep() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(LUCENE_ROOT),
                "Lucene source repo not found at " + LUCENE_ROOT + " — skipping");

        SearchHarness harness = new SearchHarness(tempDir);
        harness.indexCorpus();

        // Brief warm-up
        long wuDeadline = System.currentTimeMillis() + WARMUP_SECONDS * 1_000L;
        int wi = 0;
        while (System.currentTimeMillis() < wuDeadline) {
            harness.runQuery(QUERIES.get(wi++ % QUERIES.size()));
        }

        System.out.println();
        System.out.println("=".repeat(72));
        System.out.println("  Keyword search — concurrency sweep");
        System.out.println("=".repeat(72));
        System.out.printf("  %-8s  %10s  %10s  %10s  %10s%n",
                "THREADS", "QPS", "p50 (ms)", "p95 (ms)", "p99 (ms)");
        System.out.println("  " + "-".repeat(56));

        for (int threads : THREAD_COUNTS) {
            List<Long> latenciesNs = runConcurrent(harness, threads, MEASURE_SECONDS);
            double qps = (double) latenciesNs.size() / MEASURE_SECONDS;
            double p50 = percentileMs(latenciesNs, 50);
            double p95 = percentileMs(latenciesNs, 95);
            double p99 = percentileMs(latenciesNs, 99);
            System.out.printf("  %-8d  %10.0f  %10.2f  %10.2f  %10.2f%n",
                    threads, qps, p50, p95, p99);
        }
        System.out.println("=".repeat(72));
        System.out.println();
    }

    // ----------------------------------------------------------------------------------
    // Mixed-query breakdown
    // ----------------------------------------------------------------------------------

    /**
     * Groups queries into short (≤2 tokens) and long (≥3 tokens) and reports
     * QPS separately, so we can see how query complexity affects throughput.
     */
    @Test
    void benchmark_searchQps_shortVsLongQueries() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(LUCENE_ROOT),
                "Lucene source repo not found at " + LUCENE_ROOT + " — skipping");

        SearchHarness harness = new SearchHarness(tempDir);
        harness.indexCorpus();

        List<String> shortQueries = QUERIES.stream()
                .filter(q -> q.split("\\s+").length <= 2)
                .toList();
        List<String> longQueries = QUERIES.stream()
                .filter(q -> q.split("\\s+").length > 2)
                .toList();

        // Warm-up using all queries
        long wuDeadline = System.currentTimeMillis() + WARMUP_SECONDS * 1_000L;
        int wi = 0;
        while (System.currentTimeMillis() < wuDeadline) {
            harness.runQuery(QUERIES.get(wi++ % QUERIES.size()));
        }

        List<Long> shortLatencies = measureQueries(harness, shortQueries, MEASURE_SECONDS);
        List<Long> longLatencies  = measureQueries(harness, longQueries,  MEASURE_SECONDS);

        printReport("Short queries (≤2 tokens)", 1, shortLatencies, MEASURE_SECONDS);
        printReport("Long queries  (≥3 tokens)", 1, longLatencies,  MEASURE_SECONDS);
    }

    // ----------------------------------------------------------------------------------
    // Internal helpers
    // ----------------------------------------------------------------------------------

    private List<Long> measureQueries(SearchHarness harness, List<String> queries, int seconds) throws IOException {
        List<Long> latencies = new ArrayList<>(2048);
        long deadline = System.currentTimeMillis() + seconds * 1_000L;
        int i = 0;
        while (System.currentTimeMillis() < deadline) {
            long t0 = System.nanoTime();
            harness.runQuery(queries.get(i++ % queries.size()));
            latencies.add(System.nanoTime() - t0);
        }
        return latencies;
    }

    private List<Long> runConcurrent(SearchHarness harness, int threads, int seconds)
            throws InterruptedException {
        List<Long> combined = Collections.synchronizedList(new ArrayList<>(8192));
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threads);
        AtomicLong errors = new AtomicLong();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            pool.submit(() -> {
                try {
                    start.await();
                    long deadline = System.currentTimeMillis() + seconds * 1_000L;
                    int qi = threadIdx;
                    while (System.currentTimeMillis() < deadline) {
                        long t0 = System.nanoTime();
                        harness.runQuery(QUERIES.get(qi % QUERIES.size()));
                        combined.add(System.nanoTime() - t0);
                        qi++;
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        done.await();
        pool.shutdown();

        if (errors.get() > 0) {
            System.err.println("[WARN] " + errors.get() + " query errors during concurrent run");
        }
        return combined;
    }

    private static void printReport(String title, int threads,
                                    List<Long> latenciesNs, int measureSeconds) {
        int n       = latenciesNs.size();
        double qps  = (double) n / measureSeconds;
        double mean = latenciesNs.stream().mapToLong(l -> l).average().orElse(0) / 1e6;
        double p50  = percentileMs(latenciesNs, 50);
        double p95  = percentileMs(latenciesNs, 95);
        double p99  = percentileMs(latenciesNs, 99);
        double max  = latenciesNs.stream().mapToLong(l -> l).max().orElse(0) / 1e6;

        System.out.println();
        System.out.println("=".repeat(68));
        System.out.println("  " + title);
        System.out.println("=".repeat(68));
        System.out.printf("  Threads        : %d%n",   threads);
        System.out.printf("  Queries run    : %,d%n",  n);
        System.out.printf("  Measure window : %d s%n", measureSeconds);
        System.out.printf("  QPS            : %.1f%n", qps);
        System.out.println("  ---");
        System.out.printf("  Mean latency   : %.2f ms%n", mean);
        System.out.printf("  p50            : %.2f ms%n", p50);
        System.out.printf("  p95            : %.2f ms%n", p95);
        System.out.printf("  p99            : %.2f ms%n", p99);
        System.out.printf("  Max            : %.2f ms%n", max);
        System.out.println("=".repeat(68));
    }

    private static double percentileMs(List<Long> latenciesNs, int pct) {
        if (latenciesNs.isEmpty()) return 0;
        List<Long> sorted = new ArrayList<>(latenciesNs);
        Collections.sort(sorted);
        int idx = (int) Math.ceil(pct / 100.0 * sorted.size()) - 1;
        idx = Math.max(0, Math.min(idx, sorted.size() - 1));
        return sorted.get(idx) / 1e6;
    }

    // ----------------------------------------------------------------------------------
    // Search harness — indexes the corpus and exposes a single-query method
    // ----------------------------------------------------------------------------------

    /**
     * Wires up the same components as {@code pharos index} / {@code pharos search},
     * indexes the Lucene source tree into a temp dir, then exposes
     * {@link #runQuery(String)} for the benchmark loops.
     *
     * {@link SearchEngine} uses cached {@link IndexReader}s internally, so the
     * reader open cost is paid once at index time, not per query.
     */
    private static class SearchHarness {

        private static final String PROJECT = "lucene-search-bench";

        private final IndexConfig config;
        private final LuceneIndexer luceneIndexer;
        private final SearchEngine searchEngine;
        private final TestRegistry registry;
        private boolean indexed = false;

        SearchHarness(Path tempDir) {
            config = IndexConfig.defaults();
            config.setIndexDir(tempDir.resolve("indexes"));

            registry      = new TestRegistry();
            EmbeddingProvider embedder = EmbeddingProvider.create(config); // NoOp — no model URL
            luceneIndexer = new LuceneIndexer(config);
            searchEngine  = new SearchEngine(luceneIndexer, embedder, registry);
        }

        /** Index the corpus once. Subsequent calls are no-ops. */
        synchronized void indexCorpus() throws IOException {
            if (indexed) return;

            ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
                @Override
                public synchronized List<String> incorporate(
                        Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                    return List.of();
                }
            };

            ProjectIndexManager indexManager = new ProjectIndexManager(
                    config, luceneIndexer, registry, EmbeddingProvider.create(config), noopModules);

            System.out.print("[search-qps] Indexing corpus … ");
            long t0 = System.currentTimeMillis();
            ProjectMeta meta = indexManager.index(LUCENE_ROOT, PROJECT, false, false);
            System.out.printf("done in %.1f s  (%,d methods, %,d classes)%n",
                    (System.currentTimeMillis() - t0) / 1e3,
                    meta.getMethodCount(), meta.getClassCount());
            indexed = true;
        }

        /** Execute a single keyword search and return the results (result ignored by callers). */
        List<SearchResult> runQuery(String query) throws IOException {
            return searchEngine.search(
                    SearchRequest.keyword(query, PROJECT, 10));
        }
    }

    // ----------------------------------------------------------------------------------
    // In-memory registry — no writes to ~/.pharos/registry.json
    // ----------------------------------------------------------------------------------

    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() { super(IndexConfig.defaults()); }

        @Override public synchronized void register(ProjectMeta m)    { store.put(m.getName(), m); }
        @Override public synchronized Optional<ProjectMeta> find(String n) { return Optional.ofNullable(store.get(n)); }
        @Override public synchronized List<ProjectMeta> listAll()     { return new ArrayList<>(store.values()); }
        @Override public synchronized void link(String a, String b)   {}
        @Override public synchronized void unregister(String n)       { store.remove(n); }
    }
}
