package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.parser.model.ParsedProject;
import com.pharos.parser.model.ParsedRelationships;
import com.pharos.search.SearchEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A run that fails part-way must leave the previous index live and unstamped-as-new: no partial
 * Lucene commit, no stamp over a stale graph, and no empty legacy graph behind a broken pointer.
 */
class IndexFailurePathsTest {

    private static final String PROJECT = "sample-app";
    private static final String GET_NAME = "com.example.model.User#getName()";

    @TempDir Path tmp;

    private Path projectRoot;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private ProjectRegistry registry;
    private ModuleGraphBuilder noopGraph;
    private final AtomicBoolean embedFails = new AtomicBoolean();
    private final AtomicBoolean graphFails = new AtomicBoolean();
    private ProjectIndexManager manager;

    /** Available 4-dim embedder that throws while {@link #embedFails} is set. */
    private final EmbeddingProvider embedder = new EmbeddingProvider() {
        @Override public String modelId() { return "boom"; }
        @Override public float[] embed(String text) {
            if (embedFails.get()) throw new IllegalStateException("simulated embedder failure");
            return new float[]{1, 0, 0, 0};
        }
        @Override public int dimensions() { return 4; }
        @Override public boolean isAvailable() { return true; }
    };

    @BeforeEach
    void setUp() throws Exception {
        projectRoot = copySample();
        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        luceneIndexer = new LuceneIndexer(config);
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        manager = new ProjectIndexManager(config, luceneIndexer, registry, List.of(embedder), noopGraph) {
            @Override
            protected void populateGraph(CallGraph graph, ParsedProject project, ParsedRelationships rel) {
                if (graphFails.get()) throw new OutOfMemoryError("simulated heap exhaustion");
                super.populateGraph(graph, project, rel);
            }
        };
    }

    private Path copySample() throws Exception {
        Path src = Paths.get("src/test/resources/test-projects/sample-app");
        Path dst = tmp.resolve("sample-app");
        try (var walk = Files.walk(src)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path t = dst.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        return dst;
    }

    private Path dir() { return luceneIndexer.getProjectIndexDir(PROJECT); }
    private Path stamp() { return dir().resolve("index-format.json"); }
    private Path pointer() { return dir().resolve(CallGraph.POINTER_FILE); }

    private void index(boolean incremental, boolean embed) throws Exception {
        manager.index(projectRoot, PROJECT, incremental, false, embed, false, ProgressListener.SILENT);
    }

    private SearchEngine engine() {
        return new SearchEngine(luceneIndexer, new NoOpEmbeddingProvider(), registry);
    }

    private int docCount() throws IOException {
        return luceneIndexer.openReader(PROJECT).numDocs();
    }

    private void touchUser() throws IOException {
        Path f = projectRoot.resolve("src/main/java/com/example/model/User.java");
        Files.writeString(f, Files.readString(f).replace("getName()", "getName( )"));
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 10_000));
    }

    // --- graph build failure (finding 1) ---------------------------------------------------

    @Test
    void failedGraphBuildThrowsAndLeavesStampAndGraphUnchanged() throws Exception {
        index(false, false);
        byte[] stampBefore = Files.readAllBytes(stamp());
        String pointerBefore = Files.readString(pointer());

        graphFails.set(true);
        assertThatThrownBy(() -> index(false, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Call graph build failed")
                .hasMessageContaining("pharos index --full " + PROJECT)
                .hasRootCauseInstanceOf(OutOfMemoryError.class);

        assertThat(Files.readAllBytes(stamp())).as("stamp untouched").isEqualTo(stampBefore);
        assertThat(Files.readString(pointer())).as("previous graph still live").isEqualTo(pointerBefore);
        assertThat(engine().findUsages(GET_NAME, "callers", PROJECT).callers()).isNotEmpty();
    }

    @Test
    void failedGraphBuildOnUpgradeKeepsTheProjectNotCurrent() throws Exception {
        index(false, false);
        Files.delete(stamp());  // reads as format 0: the upgrade case

        graphFails.set(true);
        assertThatThrownBy(() -> index(true, false)).isInstanceOf(IOException.class);
        assertThat(stamp()).as("still unstamped").doesNotExist();
        assertThat(IndexFormat.status(dir())).isNotEqualTo(IndexFormat.Status.OK);

        // The next run, with the cause fixed, upgrades it.
        graphFails.set(false);
        index(true, false);
        assertThat(IndexFormat.status(dir())).isEqualTo(IndexFormat.Status.OK);
    }

    // --- partial Lucene commits (finding 2) ------------------------------------------------

    @Test
    void fullReindexFailingInEmbeddingKeepsThePreviousIndex() throws Exception {
        index(false, true);
        int before = docCount();
        byte[] stampBefore = Files.readAllBytes(stamp());
        assertThat(engine().findMethodsByPartialFqn("com.example.model.User#getName")).isNotEmpty();

        embedFails.set(true);
        touchUser();  // a cache miss, so the embedder is really called
        assertThatThrownBy(() -> index(false, true)).hasMessageContaining("simulated embedder failure");

        assertThat(docCount()).as("previous commit still live").isEqualTo(before);
        assertThat(engine().findMethodsByPartialFqn("com.example.model.User#getName")).isNotEmpty();
        assertThat(Files.readAllBytes(stamp())).isEqualTo(stampBefore);
    }

    @Test
    void incrementalRunFailingInEmbeddingCommitsNothing() throws Exception {
        index(false, true);
        int before = docCount();

        embedFails.set(true);
        touchUser();
        assertThatThrownBy(() -> index(true, true)).hasMessageContaining("simulated embedder failure");

        // The run had already deleted User.java's documents; without a commit they are back.
        assertThat(docCount()).isEqualTo(before);
        assertThat(engine().findMethodsByPartialFqn("com.example.model.User#getName")).isNotEmpty();
    }

    // --- broken pointer (findings 4 and 5) -------------------------------------------------

    @Test
    void writerGraphDirRejectsMalformedAndDanglingPointers() throws Exception {
        index(false, false);
        assertThat(CallGraph.writerGraphDir(dir())).isEqualTo(dir().resolve(Files.readString(pointer()).trim()));

        Files.writeString(pointer(), "not-a-generation\n");
        assertThatThrownBy(() -> CallGraph.writerGraphDir(dir()))
                .isInstanceOf(IOException.class).hasMessageContaining("malformed");

        Files.writeString(pointer(), "callgraph-123.arcadedb\n");
        assertThatThrownBy(() -> CallGraph.writerGraphDir(dir()))
                .isInstanceOf(IOException.class).hasMessageContaining("does not exist");
        assertThat(dir().resolve(CallGraph.LEGACY_DIR)).doesNotExist();
    }

    @Test
    void incrementalRunWithMalformedPointerRebuildsInsteadOfPatchingAnEmptyLegacyGraph() throws Exception {
        index(false, false);
        Files.writeString(pointer(), "garbage\n");
        assertIncrementalRunRebuildsTheGraph();
    }

    @Test
    void incrementalRunWithPointerToMissingGenerationRebuilds() throws Exception {
        index(false, false);
        Files.writeString(pointer(), "callgraph-123.arcadedb\n");
        assertIncrementalRunRebuildsTheGraph();
    }

    private void assertIncrementalRunRebuildsTheGraph() throws Exception {
        touchUser();
        List<String> messages = new ArrayList<>();
        manager.index(projectRoot, PROJECT, true, false, false, false,
                (phase, done, total) -> messages.add(phase));

        assertThat(messages).anyMatch(m -> m.contains("Call graph pointer") && m.contains("full re-index"));
        assertThat(dir().resolve(CallGraph.LEGACY_DIR)).as("no empty legacy graph").doesNotExist();
        assertThat(CallGraph.pointerProblem(dir())).isNull();
        try (CallGraph g = CallGraph.openExisting(CallGraph.projectGraphDir(dir()))) {
            assertThat(g.callers(GET_NAME)).isNotEmpty();
        }
    }

    @Test
    void hugeGenerationNumberDoesNotBreakAFullRun() throws Exception {
        index(false, false);
        Files.createDirectories(dir().resolve("callgraph-99999999999999999999999.arcadedb"));
        index(false, false);
        assertThat(IndexFormat.status(dir())).isEqualTo(IndexFormat.Status.OK);
        assertThat(CallGraph.pointerProblem(dir())).isNull();
    }
}
