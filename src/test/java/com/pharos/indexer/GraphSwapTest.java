package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleGraphBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A full re-index builds the call graph beside the live one and swaps it in at the end, so a
 * reader never sees a cleared or half-built graph.
 */
class GraphSwapTest {

    private static final String PROJECT = "sample-app";
    private static final String GET_NAME = "com.example.model.User#getName()";

    @TempDir Path tmp;

    private Path projectRoot;
    private LuceneIndexer luceneIndexer;
    private ProjectIndexManager manager;

    @BeforeEach
    void setUp() throws Exception {
        Path src = Paths.get("src/test/resources/test-projects/sample-app");
        projectRoot = tmp.resolve("sample-app");
        try (var walk = Files.walk(src)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path t = projectRoot.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        luceneIndexer = new LuceneIndexer(config);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
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

    private Path dbDir() {
        return luceneIndexer.getProjectIndexDir(PROJECT).resolve("callgraph.arcadedb");
    }

    private void indexFull() throws Exception {
        manager.index(projectRoot, PROJECT, false, false, false, false, ProgressListener.SILENT);
    }

    private void addCallerOfGetName() throws Exception {
        Path f = projectRoot.resolve("src/main/java/com/example/service/Badge.java");
        Files.writeString(f, """
                package com.example.service;

                import com.example.model.User;

                public class Badge {
                    public String label(User user) {
                        return "[" + user.getName() + "]";
                    }
                }
                """);
    }

    private void assertNoLeftovers() throws Exception {
        try (var s = Files.list(luceneIndexer.getProjectIndexDir(PROJECT))) {
            assertThat(s.map(p -> p.getFileName().toString()))
                    .noneMatch(n -> n.startsWith("callgraph.arcadedb.new") || n.startsWith("callgraph.arcadedb.old"));
        }
    }

    @Test
    void readersSeeOldGraphUntilFullReindexSwaps() throws Exception {
        indexFull();
        addCallerOfGetName();

        AtomicReference<Throwable> indexFailure = new AtomicReference<>();
        int polls = 0;
        try (CallGraph reader = CallGraph.open(dbDir())) {
            assertThat(reader.callers(GET_NAME)).isNotEmpty();

            Thread indexer = new Thread(() -> {
                try { indexFull(); } catch (Throwable t) { indexFailure.set(t); }
            }, "full-reindex");
            indexer.start();
            while (indexer.isAlive()) {
                Set<String> callers = reader.callers(GET_NAME);
                assertThat(callers).as("callers seen by a reader during the full re-index").isNotEmpty();
                polls++;
            }
            indexer.join();
            assertThat(reader.callers(GET_NAME)).isNotEmpty();
        }
        assertThat(indexFailure.get()).isNull();
        assertThat(polls).isPositive();

        // After the swap, a fresh reader sees the rebuilt graph, including the new caller.
        try (CallGraph fresh = CallGraph.open(dbDir())) {
            assertThat(fresh.callers(GET_NAME)).anyMatch(c -> c.startsWith("com.example.service.Badge#label("));
        }
        assertNoLeftovers();
    }

    @Test
    void runKilledBetweenRenamesGetsOldGraphBack() throws Exception {
        indexFull();
        Path idx = luceneIndexer.getProjectIndexDir(PROJECT);
        Files.move(dbDir(), idx.resolve("callgraph.arcadedb.old"));

        // An incremental run with nothing changed: it patches the graph in place, so it needs one.
        manager.index(projectRoot, PROJECT, true, false, false, false, ProgressListener.SILENT);

        assertNoLeftovers();
        try (CallGraph g = CallGraph.open(dbDir())) {
            assertThat(g.callers(GET_NAME)).isNotEmpty();
        }
    }

    @Test
    void leftoverNewDirIsCleaned() throws Exception {
        indexFull();
        Path idx = luceneIndexer.getProjectIndexDir(PROJECT);
        // An interrupted earlier full run: a half-built .new, a stale .old and their sidecars.
        Files.createDirectories(idx.resolve("callgraph.arcadedb.new"));
        Files.writeString(idx.resolve("callgraph.arcadedb.new/junk"), "x");
        Files.writeString(idx.resolve("callgraph.arcadedb.new.index-v2"), "rebuilt\n");
        Files.createDirectories(idx.resolve("callgraph.arcadedb.old"));
        Files.writeString(idx.resolve("callgraph.arcadedb.old/junk"), "x");

        indexFull();

        assertNoLeftovers();
        try (CallGraph g = CallGraph.open(dbDir())) {
            assertThat(g.callers(GET_NAME)).isNotEmpty();
        }
    }
}
