package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The safe-point abort primitive ({@link IndexAbort}): an aborted run leaves the project exactly at
 * its previous commit (Lucene files, graph generation and pointer, file state, registry), and an
 * abort that comes after the run entered its commit is refused, so a run is never half committed.
 */
class IndexAbortTest {

    private static final String PROJECT = "sample-app";

    @TempDir Path tmp;
    private Path source;
    private LuceneIndexer lucene;
    private ProjectRegistry registry;
    private ProjectIndexManager manager;
    private final CountingEmbedder embedder = new CountingEmbedder();

    @BeforeEach
    void setUp() throws IOException {
        source = tmp.resolve("checkout");
        Path from = Path.of("src/test/resources/test-projects/sample-app");
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path dest = source.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(dest);
                else Files.copy(p, dest);
            }
        }
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        lucene = new LuceneIndexer(config);
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta, MavenPomReader.PomInfo pom) {
                return List.of();
            }
        };
        manager = new ProjectIndexManager(config, lucene, registry, List.of(embedder), noopModules,
                List.of(new JavaCodeParser()));
    }

    @AfterEach
    void tearDown() {
        lucene.close();
    }

    private Path dir() { return lucene.getProjectIndexDir(PROJECT); }

    private ProjectMeta run(boolean incremental, IndexAbort abort, ProgressListener progress) throws IOException {
        return manager.index(source, PROJECT, incremental, false, true, false, progress, abort, null);
    }

    /** Everything an aborted run must leave untouched. */
    private record Snapshot(Set<String> files, byte[] fileState, String pointer, int docs) {}

    private Snapshot snapshot() throws IOException {
        Set<String> files = new TreeSet<>();
        try (Stream<Path> s = Files.walk(dir())) {
            s.filter(Files::isRegularFile).map(p -> dir().relativize(p).toString())
                    .filter(n -> !n.endsWith("write.lock"))
                    .filter(n -> !n.contains(".arcadedb/")) // ArcadeDB rewrites its own files when opened
                    .forEach(files::add);
        }
        try (Stream<Path> s = Files.list(dir())) {
            s.filter(Files::isDirectory).map(p -> p.getFileName().toString() + "/").forEach(files::add);
        }
        int docs;
        try (DirectoryReader r = DirectoryReader.open(FSDirectory.open(dir().resolve("lucene")))) {
            docs = r.numDocs();
        }
        Path pointer = dir().resolve("callgraph.current");
        return new Snapshot(files, Files.readAllBytes(dir().resolve("file-state.json")),
                Files.isRegularFile(pointer) ? Files.readString(pointer) : null, docs);
    }

    private void changeCalculator() throws IOException {
        Path f = source.resolve("src/main/java/com/example/Calculator.java");
        String src = Files.readString(f);
        int end = src.lastIndexOf('}');
        Files.writeString(f, src.substring(0, end) + "\n    public int aborted(int x) { return x + 7; }\n}\n");
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 5_000));
    }

    /** Requests an abort the first time a stage starting with {@code prefix} reports progress. */
    private static ProgressListener abortOn(String prefix, IndexAbort abort) {
        return (stage, cur, total) -> { if (stage.startsWith(prefix)) abort.request("test"); };
    }

    @Test
    void abortedIncrementalRunLeavesThePreviousCommit() throws IOException {
        run(false, IndexAbort.none(), ProgressListener.SILENT);
        Snapshot before = snapshot();
        ProjectMeta metaBefore = registry.find(PROJECT).orElseThrow();
        changeCalculator();

        IndexAbort abort = new IndexAbort();
        embedder.onBatch = n -> abort.request("embedding started");
        assertThatThrownBy(() -> run(true, abort, ProgressListener.SILENT))
                .isInstanceOf(IndexAbortedException.class);

        assertThat(snapshot()).usingRecursiveComparison().isEqualTo(before);
        assertThat(registry.find(PROJECT).orElseThrow().getLastIndexed()).isEqualTo(metaBefore.getLastIndexed());
        assertThat(CallGraph.pointerProblem(dir())).isNull();
        assertThat(IndexFormat.status(dir())).isEqualTo(IndexFormat.Status.OK);

        // the next run picks the change up as if the aborted one never happened
        embedder.onBatch = n -> {};
        run(true, IndexAbort.none(), ProgressListener.SILENT);
        assertThat(snapshot().docs()).isGreaterThan(before.docs());
    }

    @Test
    void abortedFullRunDeletesItsGraphGenerationAndSegments() throws IOException {
        run(false, IndexAbort.none(), ProgressListener.SILENT);
        Snapshot before = snapshot();
        changeCalculator();

        IndexAbort abort = new IndexAbort();
        // after the graph generation was built, while documents are being written
        assertThatThrownBy(() -> run(false, abort, abortOn("Embedding", abort)))
                .isInstanceOf(IndexAbortedException.class);

        Snapshot after = snapshot();
        assertThat(after).usingRecursiveComparison().isEqualTo(before);
        try (Stream<Path> s = Files.list(dir())) {
            assertThat(s.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("callgraph-"))
                    .map(n -> n.substring(0, n.indexOf(".arcadedb")))
                    .collect(Collectors.toSet())).hasSize(1); // only the live generation (and its sidecars)
        }
        assertThat(lucene.indexExists(PROJECT)).isTrue();
    }

    @Test
    void abortAfterTheCommitPointIsRefusedAndTheRunFinishes() throws IOException {
        IndexAbort abort = new IndexAbort();
        boolean[] refused = {false};
        // "Mining synonyms"/"Done" come after the commit; "Done" is the last stage of every run
        ProgressListener late = (stage, cur, total) -> {
            if (stage.equals("Done")) refused[0] = !abort.request("too late");
        };
        ProjectMeta meta = run(false, abort, late);
        assertThat(refused[0]).isTrue();
        assertThat(abort.committing()).isTrue();
        assertThat(meta.getMethodCount()).isPositive();
        assertThat(IndexFormat.status(dir())).isEqualTo(IndexFormat.Status.OK);
    }

    @Test
    void abortBeforeTheRunStartsTouchesNothing() {
        IndexAbort abort = new IndexAbort();
        abort.request("stopping");
        assertThatThrownBy(() -> run(false, abort, ProgressListener.SILENT)).isInstanceOf(IndexAbortedException.class);
        assertThat(Files.exists(dir())).isFalse();
    }
}
