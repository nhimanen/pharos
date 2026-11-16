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

    /** Index a project and optionally strip its stamp so it reads as format 0 (outdated). */
    private Path indexProject(String name, boolean outdated) throws Exception {
        Path root = copySample(name);
        manager.index(root, name, false, false, false, false, ProgressListener.SILENT);
        if (outdated) Files.delete(dir(name).resolve("index-format.json"));
        return root;
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
        assertThat(Files.exists(dir("b").resolve("index-format.json"))).isFalse();
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
        assertThat(Files.exists(dir("b").resolve("index-format.json"))).as("not stamped").isFalse();
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

        assertThat(rc).isEqualTo(2);
        assertThat(err.toString()).contains("readers receive upgrades by syncing; upgrade the writer");
        assertThat(out.toString()).doesNotContain("Re-indexing");
        assertThat(IndexFormat.status(dir("b"))).isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);
    }
}
