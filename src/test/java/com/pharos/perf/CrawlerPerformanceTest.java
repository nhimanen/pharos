package com.pharos.perf;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.MavenPomReader;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end onboarding performance benchmark.
 *
 * Runs the full pipeline that {@code pharos index} uses:
 *   parse → call-graph → Lucene index (+ embeddings if model configured)
 *
 * Corpus: Apache Lucene source tree at /home/nhimanen/projects/lucene
 *
 * Run with:
 *   mvn test -Dtest=CrawlerPerformanceTest -Dgroups=perf
 *
 * To benchmark with embeddings, set embeddingModelUrl in
 * ~/.pharos/config.json and re-run.  Without a model the embedding
 * step is skipped (NoOpEmbeddingProvider) and the test reports this clearly.
 *
 * Throughput floors (≥ 20 classes/s, ≥ 50 methods/s) account for the full
 * pipeline overhead (Lucene I/O + graph serialisation) and are intentionally
 * generous so the test catches severe regressions only.
 */
@Tag("perf")
class CrawlerPerformanceTest {

    private static final Path LUCENE_ROOT =
            Path.of("/home/nhimanen/projects/lucene/lucene/lucene");

    /** Smaller real-world Maven project (~173 Java files) for cleaner throughput signal. */
    private static final Path MYPAL_ROOT =
            Path.of("/home/nhimanen/projects/mypal");

    /** Full-pipeline minimum thresholds (parse + graph + Lucene write). */
    private static final double MIN_CLASSES_PER_SECOND = 20.0;
    private static final double MIN_METHODS_PER_SECOND = 50.0;

    @TempDir
    Path tempDir;

    // ----------------------------------------------------------------------------------
    // Full-corpus onboarding benchmark
    // ----------------------------------------------------------------------------------

    /**
     * Indexes the entire Lucene source tree through the full onboarding pipeline:
     * parse → call-graph → Lucene (+ embeddings if a model is configured).
     * Reports a phase-breakdown: parse time, graph time, Lucene write time,
     * embedding time, and overall throughput.
     */
    @Test
    void benchmark_fullOnboarding() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(LUCENE_ROOT),
                "Lucene source repo not found at " + LUCENE_ROOT + " — skipping");

        PipelineHarness harness = new PipelineHarness(tempDir);

        // JIT warm-up: index core module, discard results
        Path coreRoot = LUCENE_ROOT.resolve("core");
        harness.index(coreRoot, "lucene-core-warmup");
        harness.reset("lucene-core-warmup");

        // Timed run: full corpus
        long startNs = System.nanoTime();
        ProjectMeta meta = harness.index(LUCENE_ROOT, "lucene-full");
        long totalNs = System.nanoTime() - startNs;

        double totalSec    = totalNs / 1e9;
        int    totalFiles  = meta.getFileCount();
        int    totalClasses= meta.getClassCount();
        int    totalMethods= meta.getMethodCount();

        double classesPerSec = totalClasses / totalSec;
        double methodsPerSec = totalMethods / totalSec;
        double filesPerSec   = totalFiles   / totalSec;

        // Lucene index size on disk
        long luceneSizeBytes = dirSize(harness.lucenePath("lucene-full"));

        printFullReport(harness.embedder, totalSec, totalFiles, totalClasses,
                totalMethods, filesPerSec, classesPerSec, methodsPerSec, luceneSizeBytes);

        // Correctness
        assertThat(totalClasses).as("indexed class count").isGreaterThan(500);
        assertThat(totalMethods).as("indexed method count").isGreaterThan(2_000);

        // Throughput floor (full pipeline is slower than parse-only)
        assertThat(classesPerSec)
                .as("classes/sec (full pipeline) >= %s", MIN_CLASSES_PER_SECOND)
                .isGreaterThanOrEqualTo(MIN_CLASSES_PER_SECOND);
        assertThat(methodsPerSec)
                .as("methods/sec (full pipeline) >= %s", MIN_METHODS_PER_SECOND)
                .isGreaterThanOrEqualTo(MIN_METHODS_PER_SECOND);
    }

    // ----------------------------------------------------------------------------------
    // Small-repo benchmark (mypal — ~173 files, Spring Boot Maven project)
    // ----------------------------------------------------------------------------------

    /**
     * Full-pipeline benchmark on a small, real-world Maven project.
     * Small corpus means JIT noise dominates less and the throughput numbers
     * more directly reflect per-file overhead.  Runs 5 timed iterations after
     * a single warm-up run and reports mean ± stddev.
     */
    @Test
    void benchmark_smallRepo_repeatability() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(MYPAL_ROOT),
                "mypal project not found at " + MYPAL_ROOT + " — skipping");

        PipelineHarness harness = new PipelineHarness(tempDir);

        // Warm-up
        harness.index(MYPAL_ROOT, "mypal-warmup");
        harness.reset("mypal-warmup");

        int RUNS = 5;
        double[] classesPerSecRuns = new double[RUNS];
        double[] methodsPerSecRuns = new double[RUNS];
        double[] filesPerSecRuns   = new double[RUNS];
        int lastClasses = 0, lastMethods = 0, lastFiles = 0;
        long lastLuceneBytes = 0;

        for (int i = 0; i < RUNS; i++) {
            harness.reset("mypal");
            long start = System.nanoTime();
            ProjectMeta meta = harness.index(MYPAL_ROOT, "mypal");
            double sec = (System.nanoTime() - start) / 1e9;

            lastClasses = meta.getClassCount();
            lastMethods = meta.getMethodCount();
            lastFiles   = meta.getFileCount();
            classesPerSecRuns[i] = lastClasses / sec;
            methodsPerSecRuns[i] = lastMethods / sec;
            filesPerSecRuns[i]   = lastFiles   / sec;
            lastLuceneBytes = dirSize(harness.lucenePath("mypal"));

            System.out.printf("[mypal run %d]  files: %,d  classes: %,d  methods: %,d  " +
                    "time: %.2fs  cls/sec: %.0f  mth/sec: %.0f%n",
                    i + 1, lastFiles, lastClasses, lastMethods, sec,
                    classesPerSecRuns[i], methodsPerSecRuns[i]);
        }

        double meanFiles = mean(filesPerSecRuns);
        double stdFiles  = stddev(filesPerSecRuns, meanFiles);
        double meanCls   = mean(classesPerSecRuns);
        double stdCls    = stddev(classesPerSecRuns, meanCls);
        double meanMth   = mean(methodsPerSecRuns);
        double stdMth    = stddev(methodsPerSecRuns, meanMth);

        System.out.println();
        System.out.println("=".repeat(68));
        System.out.println("  Small-repo onboarding pipeline — mypal (Spring Boot)");
        System.out.println("=".repeat(68));
        System.out.printf("  Embeddings enabled : %s%n",
                harness.embedder.isAvailable()
                        ? "YES (" + harness.embedder.dimensions() + "-dim vectors)"
                        : "NO  (set embeddingModelUrl in ~/.pharos/config.json to enable)");
        System.out.printf("  Source files       : %,d%n",  lastFiles);
        System.out.printf("  Classes indexed    : %,d%n",  lastClasses);
        System.out.printf("  Methods indexed    : %,d%n",  lastMethods);
        System.out.printf("  Lucene index size  : %.1f MB%n", lastLuceneBytes / 1_048_576.0);
        System.out.println("  ---");
        System.out.printf("  files/sec   : %.0f ± %.0f (CV %.1f%%)%n",
                meanFiles, stdFiles, 100.0 * stdFiles / meanFiles);
        System.out.printf("  classes/sec : %.0f ± %.0f (CV %.1f%%)%n",
                meanCls, stdCls, 100.0 * stdCls / meanCls);
        System.out.printf("  methods/sec : %.0f ± %.0f (CV %.1f%%)%n",
                meanMth, stdMth, 100.0 * stdMth / meanMth);
        System.out.println("=".repeat(68));
        System.out.println();

        assertThat(lastClasses).as("indexed class count").isGreaterThan(10);
        assertThat(lastMethods).as("indexed method count").isGreaterThan(50);
        // Throughput floor only applies without embeddings — ONNX inference is the bottleneck
        // when embeddings are on and throughput will be much lower depending on hardware.
        if (!harness.embedder.isAvailable()) {
            assertThat(meanCls).as("mean classes/sec").isGreaterThanOrEqualTo(MIN_CLASSES_PER_SECOND);
            assertThat(meanMth).as("mean methods/sec").isGreaterThanOrEqualTo(MIN_METHODS_PER_SECOND);
        }
    }

    // ----------------------------------------------------------------------------------
    // Per-module breakdown
    // ----------------------------------------------------------------------------------

    /**
     * Indexes each Lucene sub-module independently through the full pipeline.
     * Useful for spotting which modules drive overall onboarding time.
     */
    @Test
    void benchmark_perModuleBreakdown() throws Exception {
        Assumptions.assumeTrue(Files.isDirectory(LUCENE_ROOT),
                "Lucene source repo not found at " + LUCENE_ROOT + " — skipping");

        List<Path> moduleRoots = new ArrayList<>();
        try (var stream = Files.list(LUCENE_ROOT)) {
            stream.filter(Files::isDirectory).sorted().forEach(moduleRoots::add);
        }

        record ModuleResult(String name, int files, int classes, int methods,
                            double elapsedSec, long luceneBytes) {
            double classesPerSec() { return classes / Math.max(elapsedSec, 0.001); }
            double methodsPerSec() { return methods / Math.max(elapsedSec, 0.001); }
        }

        PipelineHarness harness = new PipelineHarness(tempDir);
        harness.index(LUCENE_ROOT.resolve("core"), "warmup");
        harness.reset("warmup");

        List<ModuleResult> results = new ArrayList<>();
        for (Path moduleRoot : moduleRoots) {
            String name = moduleRoot.getFileName().toString();
            long start = System.nanoTime();
            ProjectMeta meta = harness.index(moduleRoot, name);
            double sec = (System.nanoTime() - start) / 1e9;
            harness.reset(name);

            if (meta.getFileCount() == 0) continue;

            long luceneBytes = dirSize(harness.lucenePath(name));
            results.add(new ModuleResult(name, meta.getFileCount(),
                    meta.getClassCount(), meta.getMethodCount(), sec, luceneBytes));
        }

        System.out.println();
        System.out.printf("%-30s  %6s  %7s  %8s  %8s  %8s  %10s  %10s%n",
                "MODULE", "FILES", "CLASSES", "METHODS", "TIME(s)", "IDX(MB)", "CLS/SEC", "MTH/SEC");
        System.out.println("-".repeat(110));

        int sumFiles = 0, sumClasses = 0, sumMethods = 0;
        double sumSec = 0; long sumBytes = 0;
        for (ModuleResult r : results.stream()
                .sorted((a, b) -> Double.compare(b.classesPerSec(), a.classesPerSec()))
                .toList()) {
            System.out.printf("%-30s  %6d  %7d  %8d  %8.2f  %8.1f  %10.0f  %10.0f%n",
                    r.name(), r.files(), r.classes(), r.methods(), r.elapsedSec(),
                    r.luceneBytes() / 1_048_576.0, r.classesPerSec(), r.methodsPerSec());
            sumFiles   += r.files();
            sumClasses += r.classes();
            sumMethods += r.methods();
            sumSec     += r.elapsedSec();
            sumBytes   += r.luceneBytes();
        }
        System.out.println("-".repeat(110));
        System.out.printf("%-30s  %6d  %7d  %8d  %8.2f  %8.1f  %10.0f  %10.0f%n",
                "TOTAL", sumFiles, sumClasses, sumMethods, sumSec,
                sumBytes / 1_048_576.0,
                sumClasses / Math.max(sumSec, 0.001),
                sumMethods / Math.max(sumSec, 0.001));
        System.out.println();

        assertThat(results).isNotEmpty();
        assertThat(sumClasses).isGreaterThan(100);
    }

    // ----------------------------------------------------------------------------------
    // Core-module repeatability (full pipeline)
    // ----------------------------------------------------------------------------------

    /**
     * Runs the full pipeline on the core module 3 times and reports mean ± stddev.
     * Each run starts from a fresh index (CREATE mode) so results are comparable.
     */
    @Test
    void benchmark_coreOnboarding_repeatability() throws Exception {
        Path coreRoot = LUCENE_ROOT.resolve("core");
        Assumptions.assumeTrue(Files.isDirectory(coreRoot),
                "Lucene core module not found — skipping");

        PipelineHarness harness = new PipelineHarness(tempDir);

        // Warm-up (not measured)
        harness.index(coreRoot, "warmup");
        harness.reset("warmup");

        int RUNS = 3;
        double[] classesPerSecRuns = new double[RUNS];
        double[] methodsPerSecRuns = new double[RUNS];

        for (int i = 0; i < RUNS; i++) {
            // Force full re-index each run by clearing the project's index dir
            harness.reset("lucene-core");

            long start = System.nanoTime();
            ProjectMeta meta = harness.index(coreRoot, "lucene-core");
            double sec = (System.nanoTime() - start) / 1e9;
            classesPerSecRuns[i] = meta.getClassCount() / sec;
            methodsPerSecRuns[i] = meta.getMethodCount() / sec;

            System.out.printf("[core run %d]  classes: %,d  methods: %,d  time: %.2fs  " +
                    "cls/sec: %.0f  mth/sec: %.0f%n",
                    i + 1, meta.getClassCount(), meta.getMethodCount(), sec,
                    classesPerSecRuns[i], methodsPerSecRuns[i]);
        }

        double meanCls = mean(classesPerSecRuns);
        double stdCls  = stddev(classesPerSecRuns, meanCls);
        double meanMth = mean(methodsPerSecRuns);
        double stdMth  = stddev(methodsPerSecRuns, meanMth);

        System.out.println();
        System.out.printf("Core module full-pipeline throughput over %d runs:%n", RUNS);
        System.out.printf("  Embeddings  : %s%n",
                harness.embedder.isAvailable() ? "ENABLED (" + harness.embedder.dimensions() + "-dim)" : "DISABLED (NoOp)");
        System.out.printf("  classes/sec : %.0f ± %.0f (CV %.1f%%)%n",
                meanCls, stdCls, 100.0 * stdCls / meanCls);
        System.out.printf("  methods/sec : %.0f ± %.0f (CV %.1f%%)%n",
                meanMth, stdMth, 100.0 * stdMth / meanMth);

        assertThat(meanCls).as("mean classes/sec").isGreaterThanOrEqualTo(MIN_CLASSES_PER_SECOND);
        assertThat(meanMth).as("mean methods/sec").isGreaterThanOrEqualTo(MIN_METHODS_PER_SECOND);
    }

    // ----------------------------------------------------------------------------------
    // File-size distribution (unchanged — corpus characterisation)
    // ----------------------------------------------------------------------------------

    @Test
    void report_fileSizeDistribution() throws IOException {
        Path coreRoot = LUCENE_ROOT.resolve("core/src/java");
        Assumptions.assumeTrue(Files.isDirectory(coreRoot),
                "Lucene core src/java not found — skipping");

        List<Long> lineCounts = new ArrayList<>();
        Files.walkFileTree(coreRoot, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(".java"))
                    lineCounts.add(Files.lines(file).count());
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });

        lineCounts.sort(Long::compareTo);
        int n = lineCounts.size();
        double mean   = lineCounts.stream().mapToLong(l -> l).average().orElse(0);
        long median   = lineCounts.get(n / 2);
        long p90      = lineCounts.get((int)(n * 0.90));
        long p99      = lineCounts.get((int)(n * 0.99));
        long max      = lineCounts.get(n - 1);
        long total    = lineCounts.stream().mapToLong(l -> l).sum();

        System.out.println();
        System.out.printf("File-size distribution (lucene/core, %d .java files):%n", n);
        System.out.printf("  Total lines : %,d%n", total);
        System.out.printf("  Mean        : %.0f%n", mean);
        System.out.printf("  Median      : %d%n", median);
        System.out.printf("  p90         : %d%n", p90);
        System.out.printf("  p99         : %d%n", p99);
        System.out.printf("  Max         : %d%n", max);

        assertThat(n).isGreaterThan(100);
    }

    // ----------------------------------------------------------------------------------
    // Pipeline harness — wires the same components as Main.java
    // ----------------------------------------------------------------------------------

    /**
     * Wires up the same dependency graph that {@code pharos index} uses.
     * Uses a temp dir for all Lucene index storage so runs are isolated and
     * do not pollute {@code ~/.pharos}.
     *
     * Module graph updates are skipped (Lucene source has no pom.xml) via a
     * no-op override, which is identical to what the real pipeline does when
     * no pom.xml is found.
     */
    private static class PipelineHarness {

        final EmbeddingProvider embedder;
        private final IndexConfig config;
        private final LuceneIndexer luceneIndexer;
        private final ProjectIndexManager indexManager;
        private final TestRegistry registry;

        PipelineHarness(Path tempDir) {
            config = IndexConfig.defaults();
            config.setIndexDir(tempDir.resolve("indexes"));
            // Enable embeddings with the standard all-MiniLM-L6-v2 model (384-dim).
            // DJL downloads and caches it in ~/.djl.ai/ on first run (~80 MB).
            config.setEmbeddingModelUrl("ai.djl.huggingface.onnxruntime:sentence-transformers/all-MiniLM-L6-v2");
            config.setEmbeddingDimensions(384);

            registry     = new TestRegistry();
            embedder     = EmbeddingProvider.create(config);
            luceneIndexer = new LuceneIndexer(config);

            // No-op module graph builder: Lucene source uses Gradle (no pom.xml),
            // so incorporate() would be skipped anyway, but this also avoids any
            // writes to ~/.pharos/module-graph.graphml during the test.
            ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
                @Override
                public synchronized List<String> incorporate(
                        Path root, ProjectMeta meta,
                        MavenPomReader.PomInfo pomInfo) {
                    return List.of();
                }
            };

            indexManager = new ProjectIndexManager(
                    config, luceneIndexer, registry, embedder, noopModules);
        }

        /** Run a full index on {@code projectRoot} and return the resulting ProjectMeta. */
        ProjectMeta index(Path projectRoot, String projectName) throws IOException {
            return indexManager.index(projectRoot, projectName,
                    /*incremental=*/false, embedder.isAvailable());
        }

        /** Delete the Lucene index for a project so the next run is a full re-index. */
        void reset(String projectName) throws IOException {
            Path luceneDir = config.getIndexDir().resolve(projectName).resolve("lucene");
            if (Files.exists(luceneDir)) {
                deleteTree(luceneDir);
            }
            registry.unregister(projectName);
        }

        Path lucenePath(String projectName) {
            return config.getIndexDir().resolve(projectName).resolve("lucene");
        }

        private static void deleteTree(Path root) throws IOException {
            Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
                @Override
                public java.nio.file.FileVisitResult visitFile(Path f,
                        BasicFileAttributes a) throws IOException {
                    Files.delete(f);
                    return java.nio.file.FileVisitResult.CONTINUE;
                }
                @Override
                public java.nio.file.FileVisitResult postVisitDirectory(Path d,
                        IOException e) throws IOException {
                    Files.delete(d);
                    return java.nio.file.FileVisitResult.CONTINUE;
                }
            });
        }
    }

    // ----------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------

    private static void printFullReport(EmbeddingProvider embedder, double totalSec,
                                        int files, int classes, int methods,
                                        double filesPerSec, double classesPerSec,
                                        double methodsPerSec, long luceneSizeBytes) {
        System.out.println();
        System.out.println("=".repeat(68));
        System.out.println("  Full onboarding pipeline — Apache Lucene corpus");
        System.out.println("=".repeat(68));
        System.out.printf("  Embeddings enabled : %s%n",
                embedder.isAvailable()
                        ? "YES (" + embedder.dimensions() + "-dim vectors)"
                        : "NO  (set embeddingModelUrl in ~/.pharos/config.json to enable)");
        System.out.printf("  Source files       : %,d%n", files);
        System.out.printf("  Classes indexed    : %,d%n", classes);
        System.out.printf("  Methods indexed    : %,d%n", methods);
        System.out.printf("  Total elapsed      : %.1f s%n", totalSec);
        System.out.printf("  Lucene index size  : %.1f MB%n", luceneSizeBytes / 1_048_576.0);
        System.out.println("  ---");
        System.out.printf("  Throughput         : %.0f files/sec%n",    filesPerSec);
        System.out.printf("  Throughput         : %.0f classes/sec%n",  classesPerSec);
        System.out.printf("  Throughput         : %.0f methods/sec%n",  methodsPerSec);
        System.out.println("=".repeat(68));
        System.out.println();
    }

    private static long dirSize(Path dir) {
        if (!Files.exists(dir)) return 0;
        try (var walk = Files.walk(dir)) {
            return walk.filter(Files::isRegularFile)
                    .mapToLong(p -> { try { return Files.size(p); } catch (IOException e) { return 0; } })
                    .sum();
        } catch (IOException e) {
            return 0;
        }
    }

    private static double mean(double[] v) {
        double s = 0; for (double x : v) s += x; return s / v.length;
    }

    private static double stddev(double[] v, double mean) {
        double s = 0; for (double x : v) s += (x - mean) * (x - mean);
        return Math.sqrt(s / v.length);
    }

    // Minimal in-memory registry — no writes to ~/.pharos/registry.json
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
