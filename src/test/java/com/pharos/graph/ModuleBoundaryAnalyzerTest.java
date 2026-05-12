package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.search.SearchEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class ModuleBoundaryAnalyzerTest {

    @TempDir
    Path tempDir;

    private TestRegistry registry;
    private ModuleBoundaryAnalyzer analyzer;


    @BeforeEach
    void setUp() {
        registry = new TestRegistry();
        // SearchEngine is now stored — pass a real (but non-functional) instance
        LuceneIndexerStub luceneStub = new LuceneIndexerStub();
        SearchEngine searchEngine = new SearchEngine(
                luceneStub, new NoOpEmbeddingProvider(), registry);
        analyzer = new ModuleBoundaryAnalyzer(registry, searchEngine);
    }

    @Test
    void analyze_returnsProjectName() throws Exception {
        Path indexPath = tempDir.resolve("proj-a");
        createGraph(indexPath, "com.example.A#doWork()");

        registry.register(projectMeta("proj-a", indexPath));

        ModuleBoundaryAnalyzer.BoundaryResult result = analyzer.analyze("proj-a");

        assertThat(result.projectName()).isEqualTo("proj-a");
    }

    @Test
    void analyze_unknownProject_throwsIllegalArgument() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> analyzer.analyze("no-such-project"))
                .withMessageContaining("no-such-project");
    }

    @Test
    void analyze_noLinkedProjects_entryPointsIsEmpty() throws Exception {
        Path indexPath = tempDir.resolve("proj-alone");
        createGraph(indexPath, "com.example.Alone#work()");

        registry.register(projectMeta("proj-alone", indexPath));

        ModuleBoundaryAnalyzer.BoundaryResult result = analyzer.analyze("proj-alone");

        assertThat(result.entryPoints()).isEmpty();
    }

    @Test
    void analyze_linkedProjectCallsOurMethod_returnsEntryPoint() throws Exception {
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        // proj-a has doWork()
        createGraph(indexA, "com.example.A#doWork()");

        // proj-b's call graph has an edge caller() → doWork()
        Files.createDirectories(indexB);
        try (CallGraph g = CallGraph.open(indexB.resolve("callgraph.arcadedb"))) {
            g.addCall("com.example.B#caller()", "com.example.A#doWork()");
            g.flush();
        }

        ProjectMeta metaA = projectMeta("proj-a", indexA);
        metaA.getLinkedProjects().add("proj-b");
        registry.register(metaA);
        registry.register(projectMeta("proj-b", indexB));

        ModuleBoundaryAnalyzer.BoundaryResult result = analyzer.analyze("proj-a");

        assertThat(result.entryPoints()).contains("com.example.A#doWork()");
    }

    @Test
    void analyze_unresolvedRefsBecomesExitPoints() throws Exception {
        Path indexPath = tempDir.resolve("proj-exit");
        createGraph(indexPath, "com.example.Exit#run()");

        ProjectMeta meta = projectMeta("proj-exit", indexPath);
        meta.getUnresolvedRefs().add(
                new ProjectMeta.UnresolvedRef("com.example.Exit#run()", "externalService", 42));
        registry.register(meta);

        ModuleBoundaryAnalyzer.BoundaryResult result = analyzer.analyze("proj-exit");

        assertThat(result.exitPoints()).hasSize(1);
        ModuleBoundaryAnalyzer.ExitPoint ep = result.exitPoints().get(0);
        assertThat(ep.callerFqn()).isEqualTo("com.example.Exit#run()");
        assertThat(ep.calleeSimpleName()).isEqualTo("externalService");
        assertThat(ep.line()).isEqualTo(42);
    }

    @Test
    void analyze_entryPointsSorted() throws Exception {
        Path indexA = tempDir.resolve("proj-a");
        createGraph(indexA, "com.example.A#beta()", "com.example.A#alpha()");

        Path indexB = tempDir.resolve("proj-b");
        Files.createDirectories(indexB);
        try (CallGraph g = CallGraph.open(indexB.resolve("callgraph.arcadedb"))) {
            g.addCall("com.example.B#x()", "com.example.A#beta()");
            g.addCall("com.example.B#y()", "com.example.A#alpha()");
            g.flush();
        }

        ProjectMeta metaA = projectMeta("proj-a", indexA);
        metaA.getLinkedProjects().add("proj-b");
        registry.register(metaA);
        registry.register(projectMeta("proj-b", indexB));

        ModuleBoundaryAnalyzer.BoundaryResult result = analyzer.analyze("proj-a");

        // alpha before beta (natural String sort)
        assertThat(result.entryPoints()).containsExactly(
                "com.example.A#alpha()", "com.example.A#beta()");
    }

    @Test
    void analyze_missingLinkedGraphFile_skippedGracefully() throws Exception {
        Path indexA = tempDir.resolve("proj-a");
        createGraph(indexA, "com.example.A#work()");

        ProjectMeta metaA = projectMeta("proj-a", indexA);
        // Link to a project whose callgraph.arcadedb directory doesn't exist
        metaA.getLinkedProjects().add("ghost-project");
        registry.register(metaA);
        registry.register(projectMeta("ghost-project", tempDir.resolve("nonexistent")));

        // Should not throw — missing graph is logged at debug and skipped
        ModuleBoundaryAnalyzer.BoundaryResult result = analyzer.analyze("proj-a");

        assertThat(result.entryPoints()).isEmpty();
    }

    // --- helpers ---

    /** Create an ArcadeDB call graph at {@code indexPath/callgraph.arcadedb} with the given FQNs. */
    private static void createGraph(Path indexPath, String... fqns) throws IOException {
        Files.createDirectories(indexPath);
        try (CallGraph g = CallGraph.open(indexPath.resolve("callgraph.arcadedb"))) {
            for (String fqn : fqns) {
                g.addMethod(fqn, classOf(fqn));
            }
            g.flush();
        }
    }

    /** Extract class prefix from a FQN like "com.example.Foo#bar()" → "com.example.Foo". */
    private static String classOf(String fqn) {
        int h = fqn.indexOf('#');
        return h > 0 ? fqn.substring(0, h) : fqn;
    }

    private static ProjectMeta projectMeta(String name, Path indexPath) {
        ProjectMeta meta = new ProjectMeta(name, "/src/" + name, indexPath.toAbsolutePath().toString());
        return meta;
    }

    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() { super(IndexConfig.defaults()); }

        @Override public synchronized void register(ProjectMeta m) { store.put(m.getName(), m); }
        @Override public synchronized Optional<ProjectMeta> find(String n) { return Optional.ofNullable(store.get(n)); }
        @Override public synchronized List<ProjectMeta> listAll() { return new ArrayList<>(store.values()); }
        @Override public synchronized void link(String a, String b) {}
        @Override public synchronized void unregister(String n) { store.remove(n); }
    }

    /** Minimal LuceneIndexer stub that doesn't touch real disk paths. */
    static class LuceneIndexerStub extends com.pharos.indexer.LuceneIndexer {
        LuceneIndexerStub() { super(IndexConfig.defaults()); }

        @Override
        public boolean indexExists(String projectName) { return false; }
    }
}
