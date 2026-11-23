package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class UpgradeIndexesCommandTest {

    @TempDir Path tmp;

    private final PrintStream origOut = System.out;
    private final PrintStream origErr = System.err;
    private ByteArrayOutputStream out;
    private ByteArrayOutputStream err;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private ProjectRegistry registry;
    private ProjectIndexManager manager;

    @BeforeEach
    void setUp() throws Exception {
        out = new ByteArrayOutputStream();
        err = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out, true));
        System.setErr(new PrintStream(err, true));
        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        luceneIndexer = new LuceneIndexer(config);
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        manager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopGraph);
    }

    @AfterEach
    void restore() {
        System.setOut(origOut);
        System.setErr(origErr);
    }

    private Path dir(String name) { return luceneIndexer.getProjectIndexDir(name); }

    private Path copySample(String name) throws Exception {
        Path src = Paths.get("src/test/resources/test-projects/sample-app");
        Path root = tmp.resolve("src-" + name);
        try (var walk = Files.walk(src)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path t = root.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        return root;
    }

    /** Index a project and optionally restamp it with the previous format (N-1: upgrade available). */
    private Path indexProject(String name, boolean outdated) throws Exception {
        Path root = copySample(name);
        manager.index(root, name, false, false, false, false, ProgressListener.SILENT);
        if (outdated) {
            IndexFormat.write(dir(name), new IndexFormat.Stamp(IndexFormat.MIN_READABLE_FORMAT, "0.0.0",
                    java.time.Instant.parse("2020-01-01T00:00:00Z")));
        }
        return root;
    }

    private int format(String name) {
        return IndexFormat.read(dir(name)).format();
    }

    private UpgradeIndexesCommand cmd() {
        return new UpgradeIndexesCommand(manager, registry, config, p -> "n", false);
    }

    private int run(UpgradeIndexesCommand c, String... args) {
        return new CommandLine(c).execute(args);
    }

    @Test
    void dryRunChangesNothing() throws Exception {
        indexProject("a", false);
        indexProject("b", true);
        int rc = run(cmd(), "--dry-run");
        assertThat(rc).isZero();
        assertThat(format("b")).isEqualTo(IndexFormat.MIN_READABLE_FORMAT);
        assertThat(out.toString()).contains("b").doesNotContain("Re-index");
    }

    @Test
    void upgradesOnlyOutdated() throws Exception {
        indexProject("a", false);
        indexProject("b", true);
        var aStamp = IndexFormat.read(dir("a")).writtenAt();
        int rc = run(cmd(), "--yes");
        assertThat(rc).isZero();
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.OK);
        assertThat(IndexFormat.read(dir("a")).writtenAt()).isEqualTo(aStamp);
        assertThat(out.toString()).contains("upgraded");
    }

    @Test
    void missingRootIsSkippedAndNonZero() throws Exception {
        Path rootB = indexProject("b", true);
        indexProject("c", true);
        deleteRecursively(rootB);
        int rc = run(cmd(), "--yes");
        assertThat(rc).isEqualTo(1);
        assertThat(out.toString()).contains("re-index manually: pharos index --full b");
        assertThat(IndexFormat.status(dir("c"))).isEqualTo(IndexFormat.Status.OK);
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
    }

    @Test
    void nonInteractiveWithoutYesExits2() throws Exception {
        indexProject("b", true);
        int rc = run(cmd());
        assertThat(rc).isEqualTo(2);
        assertThat(err.toString() + out.toString()).contains("use --yes to run non-interactively");
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
    }

    @Test
    void interactiveNoAnswersAborts() throws Exception {
        indexProject("b", true);
        AtomicInteger asked = new AtomicInteger();
        Function<String, String> no = p -> { asked.incrementAndGet(); assertThat(p).contains("Re-index 1 projects? [y/N]"); return "n"; };
        int rc = run(new UpgradeIndexesCommand(manager, registry, config, no, true));
        assertThat(rc).isZero();
        assertThat(asked.get()).isEqualTo(1);
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
    }

    @Test
    void eofOnPromptExits2() throws Exception {
        indexProject("b", true);
        int rc = run(new UpgradeIndexesCommand(manager, registry, config, p -> null, true));
        assertThat(rc).isEqualTo(2);
        assertThat(err.toString()).contains("use --yes to run non-interactively");
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
    }

    @Test
    void defaultInteractivityIsFalseWithoutTerminal() {
        assertThat(UpgradeIndexesCommand.interactiveConsole()).isFalse();
    }

    @Test
    void interactiveYesProceeds() throws Exception {
        indexProject("b", true);
        int rc = run(new UpgradeIndexesCommand(manager, registry, config, p -> "y", true));
        assertThat(rc).isZero();
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.OK);
    }

    @Test
    void namedProjectsFilterSelection() throws Exception {
        indexProject("b", true);
        indexProject("c", true);
        assertThat(run(cmd(), "--yes", "c")).isZero();
        assertThat(IndexFormat.status(dir("c"))).isEqualTo(IndexFormat.Status.OK);
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
    }

    @Test
    void newerFormatIsUpgradedToo() throws Exception {
        indexProject("n", false);
        Files.writeString(dir("n").resolve("index-format.json"),
                "{\"format\":99,\"pharosVersion\":\"9.9.9\",\"writtenAt\":\"2030-01-01T00:00:00Z\"}");
        assertThat(IndexFormat.status(dir("n"))).isEqualTo(IndexFormat.Status.NEWER);
        assertThat(run(cmd(), "--yes")).isZero();
        assertThat(IndexFormat.status(dir("n"))).isEqualTo(IndexFormat.Status.OK);
    }

    @Test
    void failedGraphBuildCountsAsFailedAndLeavesTheProjectUnstamped() throws Exception {
        indexProject("b", true);
        ProjectIndexManager failing = new ProjectIndexManager(config, luceneIndexer, registry,
                List.of(new NoOpEmbeddingProvider()), noopModuleGraph()) {
            @Override
            protected void populateGraph(com.pharos.graph.CallGraph graph,
                    com.pharos.parser.model.ParsedProject project,
                    com.pharos.parser.model.ParsedRelationships relationships) {
                throw new OutOfMemoryError("simulated heap exhaustion");
            }
        };
        int rc = run(new UpgradeIndexesCommand(failing, registry, config, p -> "n", false), "--yes");
        assertThat(rc).isEqualTo(1);
        assertThat(out.toString()).contains("0 upgraded").contains("1 failed");
        assertThat(err.toString()).contains("FAILED 'b'").contains("Call graph build failed")
                .contains("simulated heap exhaustion");
        assertThat(format("b")).as("not stamped with this format").isNotEqualTo(com.pharos.indexer.IndexVersions.INDEX_FORMAT);
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
    }

    @Test
    void entryWithoutIndexPathIsReportedMissing() throws Exception {
        registry.register(new ProjectMeta("nopath", tmp.toString(), null));
        int rc = run(cmd(), "--yes");
        assertThat(rc).isEqualTo(1);
        assertThat(out.toString()).contains("nopath").contains("missing index dir");
    }

    private ModuleGraphBuilder noopModuleGraph() {
        return new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
    }

    private static void deleteRecursively(Path p) throws Exception {
        try (var w = Files.walk(p)) {
            for (Path x : (Iterable<Path>) w.sorted(java.util.Comparator.reverseOrder())::iterator) {
                Files.delete(x);
            }
        }
    }

    @Test
    void invalidRegistryNameIsSkippedAndCountedNotCrashed() throws Exception {
        Files.createDirectories(config.getIndexDir());
        registry.register(new ProjectMeta(".", tmp.toString(),
                config.getIndexDir().resolve(".").toString()));
        int rc = run(cmd(), "--yes");
        assertThat(rc).isZero();
        assertThat(out.toString()).contains("invalid project name '.'").contains("registry.json");
        assertThat(config.getIndexDir()).isDirectory();
    }

    @Test
    void invalidRegistrationsAreWarningOnlyExitZero() throws Exception {
        Files.createDirectories(config.getIndexDir());
        registry.register(new ProjectMeta(".", tmp.toString(),
                config.getIndexDir().resolve(".").toString()));
        int rc = run(cmd(), "--yes");
        assertThat(rc).isZero();
        assertThat(out.toString()).contains("1 invalid registration ignored")
                .contains(registry.getPath().toString());
    }

    @Test
    void invalidRegistrationPlusRealSkipStillExitsOne() throws Exception {
        Files.createDirectories(config.getIndexDir());
        registry.register(new ProjectMeta(".", tmp.toString(), config.getIndexDir().toString()));
        registry.register(new ProjectMeta("ghost", tmp.toString(),
                config.getIndexDir().resolve("ghost").toString()));
        assertThat(run(cmd(), "--yes")).isEqualTo(1);
    }

    // ── Adaptation 7: readers receive upgrades by syncing ───────────────────────────────────

    @Test
    void readerRefusesToUpgradeAndTouchesNothing() throws Exception {
        indexProject("b", true);
        com.pharos.config.ClusterConfig cluster = new com.pharos.config.ClusterConfig();
        cluster.setRole(com.pharos.cluster.NodeRole.READER);
        cluster.setBackend(com.pharos.cluster.BackendKind.OBJECT_STORE);
        config.setCluster(cluster);

        int rc = run(cmd(), "--yes");

        assertThat(rc).isEqualTo(5); // its own code: 2 means "needs --yes"
        assertThat(err.toString()).contains("readers receive upgrades by syncing; upgrade the writer");
        assertThat(out.toString()).doesNotContain("Re-indexing");
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
    }

    @Test
    void helpListsEveryExitCode() {
        String usage = new CommandLine(cmd()).getUsageMessage();
        assertThat(usage).contains("Exit codes:").contains("0").contains("1").contains("2")
                .contains("5").contains("reader node");
    }

    // ── --check: read-only, run by the Kubernetes operator's pre-flight Job with the new image ──

    /** A bucket view: shard → LATEST manifest's index format (null: no LATEST yet; MIN_VALUE: unreadable manifest). */
    private static int check(java.util.Map<String, Integer> formats) {
        java.util.List<com.pharos.cluster.ShardId> shards = formats.keySet().stream().sorted()
                .map(com.pharos.cluster.ShardId::ofProject).toList();
        com.pharos.cluster.ReplicaSource source = new com.pharos.cluster.ReplicaSource() {
            @Override public java.util.Optional<com.pharos.cluster.Generation> latest(com.pharos.cluster.ShardId s) {
                return formats.get(s.value()) == null ? java.util.Optional.empty()
                        : java.util.Optional.of(new com.pharos.cluster.Generation(7));
            }

            @Override public com.pharos.cluster.objectstore.ShardManifest manifest(com.pharos.cluster.ShardId s,
                    com.pharos.cluster.Generation g) throws java.io.IOException {
                int f = formats.get(s.value());
                if (f == Integer.MIN_VALUE) throw new java.io.IOException("missing manifest");
                return new com.pharos.cluster.objectstore.ShardManifest(1, s.value(), g.value(), null,
                        java.time.Instant.EPOCH, "w-0", 1, 1, null, List.of(), null,
                        new IndexFormat.Stamp(f, "x", java.time.Instant.EPOCH));
            }

            @Override public void download(com.pharos.cluster.objectstore.ManifestFile f, Path t) { throw new AssertionError(); }

            @Override public void downloadRange(com.pharos.cluster.objectstore.ManifestFile f, long o, long l, Path t) {
                throw new AssertionError();
            }

            @Override public java.io.InputStream openRange(com.pharos.cluster.objectstore.ManifestFile f, long o, long l) {
                throw new AssertionError();
            }

            @Override public java.util.Optional<byte[]> synonyms() { throw new AssertionError(); }
        };
        return UpgradeIndexesCommand.checkShards(() -> shards, source);
    }

    @Test
    void checkRefusesBucketShardsThisBuildWouldNotServe() {
        int n = com.pharos.indexer.IndexVersions.INDEX_FORMAT;
        java.util.Map<String, Integer> servable = new java.util.HashMap<>();
        servable.put("current", n);
        servable.put("previous", n - 1);
        servable.put("fresh", null);
        assertThat(check(servable)).isZero();
        assertThat(out.toString()).contains("current").contains("upgrade-available").contains("not published yet")
                .contains("0 refused");

        java.util.Map<String, Integer> older = new java.util.HashMap<>(servable);
        older.put("ancient", n - 2); // N-2: a reader of this build refuses it
        assertThat(check(older)).isEqualTo(3);
        java.util.Map<String, Integer> newer = new java.util.HashMap<>(servable);
        newer.put("future", n + 1); // a downgrade
        assertThat(check(newer)).isEqualTo(3);
        assertThat(out.toString()).contains("outdated").contains("newer").contains("would refuse 1 shard");

        java.util.Map<String, Integer> broken = new java.util.HashMap<>(servable);
        broken.put("broken", Integer.MIN_VALUE);
        assertThat(check(broken)).isEqualTo(1);
        assertThat(UpgradeIndexesCommand.checkShards(() -> { throw new java.io.IOException("no bucket"); }, null))
                .isEqualTo(1);
    }

    /** Final review I2: a catalog entry this build cannot parse (Phase 6's keyed id) is refused, by name. */
    @Test
    void checkRefusesCatalogEntriesThisBuildCannotParse() {
        com.pharos.cluster.ShardCatalog catalog = new com.pharos.cluster.ShardCatalog() {
            @Override public List<com.pharos.cluster.ShardId> shards() { return List.of(); }

            @Override public List<String> unparseableEntries() { return List.of("proj~k=v"); }
        };
        assertThat(UpgradeIndexesCommand.checkShards(catalog, null)).isEqualTo(3);
        assertThat(out.toString()).contains("proj~k=v").contains("cannot read").contains("1 refused");
    }

    @Test
    void checkRunsOnAWorkQueueWriterAndChangesNothing() throws Exception {
        indexProject("ok", false);
        indexProject("n", false);
        Files.writeString(dir("n").resolve("index-format.json"),
                "{\"format\":99,\"pharosVersion\":\"9.9.9\",\"writtenAt\":\"2030-01-01T00:00:00Z\"}");
        com.pharos.config.ClusterConfig cluster = new com.pharos.config.ClusterConfig();
        cluster.setRole(com.pharos.cluster.NodeRole.WRITER);
        cluster.getWriter().setWorkQueue(true);
        config.setCluster(cluster);

        assertThat(run(cmd(), "--check")).isEqualTo(3);
        assertThat(err.toString()).doesNotContain("work-queue mode");
        assertThat(out.toString()).contains("newer").doesNotContain("Re-indexing");
        assertThat(IndexFormat.status(dir("n"))).isEqualTo(IndexFormat.Status.NEWER);

        Files.delete(dir("n").resolve("index-format.json")); // format 0: N-1 or older
        int expected = IndexFormat.servable(IndexFormat.statusOf(0)) ? 0 : 3;
        assertThat(run(cmd(), "--check")).isEqualTo(expected);
    }
}
