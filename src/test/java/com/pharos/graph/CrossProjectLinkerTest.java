package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
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

class CrossProjectLinkerTest {

    @TempDir
    Path tempDir;

    private TestRegistry registry;
    private CrossProjectLinker linker;
    private IndexConfig config;


    @BeforeEach
    void setUp() {
        registry = new TestRegistry();
        config   = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));
        linker   = new CrossProjectLinker(config, registry);
    }

    @Test
    void buildCrossProjectGraph_mergesBothGraphs() throws IOException {
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        createGraph(indexA, "com.example.A#doWork()");
        createGraph(indexB, "com.example.B#handle()");

        registry.register(projectMeta("proj-a", indexA));
        registry.register(projectMeta("proj-b", indexB));

        linker.buildCrossProjectGraph("proj-a", "proj-b");

        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            assertThat(fqnExists(merged, "com.example.A#doWork()")).isTrue();
            assertThat(fqnExists(merged, "com.example.B#handle()")).isTrue();
        }
    }

    @Test
    void buildCrossProjectGraph_resolvesUnresolvedRefAcrossProjects() throws IOException {
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        // proj-a calls "handle" but didn't resolve it during indexing
        createGraph(indexA, "com.example.A#caller()");

        // proj-b has handle()
        createGraph(indexB, "com.example.B#handle()");

        ProjectMeta metaA = projectMeta("proj-a", indexA);
        metaA.getUnresolvedRefs().add(
                new ProjectMeta.UnresolvedRef("com.example.A#caller()", "handle", 5));
        ProjectMeta metaB = projectMeta("proj-b", indexB);
        registry.register(metaA);
        registry.register(metaB);

        linker.buildCrossProjectGraph("proj-a", "proj-b");

        // The cross-project edge A#caller() → B#handle() should have been added
        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            assertThat(merged.callees("com.example.A#caller()"))
                    .contains("com.example.B#handle()");
        }
    }

    @Test
    void buildCrossProjectGraph_bothDirectionsCrossLinked() throws IOException {
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        createGraph(indexA, "com.example.A#callsB()");
        createGraph(indexB, "com.example.B#callsA()", "com.example.B#targetOfA()");

        ProjectMeta metaA = projectMeta("proj-a", indexA);
        metaA.getUnresolvedRefs().add(
                new ProjectMeta.UnresolvedRef("com.example.A#callsB()", "targetOfA", 10));
        ProjectMeta metaB = projectMeta("proj-b", indexB);
        metaB.getUnresolvedRefs().add(
                new ProjectMeta.UnresolvedRef("com.example.B#callsA()", "callsB", 3));
        registry.register(metaA);
        registry.register(metaB);

        linker.buildCrossProjectGraph("proj-a", "proj-b");

        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            // A → B direction
            assertThat(merged.callees("com.example.A#callsB()"))
                    .contains("com.example.B#targetOfA()");
            // B → A direction
            assertThat(merged.callees("com.example.B#callsA()"))
                    .contains("com.example.A#callsB()");
        }
    }

    // -----------------------------------------------------------------------
    // Limitation 1 & 5: scored candidate selection
    // -----------------------------------------------------------------------

    @Test
    void receiverTypeName_prefersMatchingClass_overArbitraryFirstMatch() throws IOException {
        // Two classes in proj-b both have a method called "write()"
        // The unresolved ref carries receiverTypeName="FileWriter" → should pick FileWriter#write
        Path indexA = tempDir.resolve("scored-a");
        Path indexB = tempDir.resolve("scored-b");

        createGraph(indexA, "com.app.Sender#send()");
        createGraph(indexB,
                "com.lib.BufferedWriter#write(String)",
                "com.lib.FileWriter#write(String)");   // correct target

        ProjectMeta metaA = projectMeta("scored-a", indexA);
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Sender#send()", "write", "FileWriter", 1, null, 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("scored-b", indexB));

        linker.buildCrossProjectGraph("scored-a", "scored-b");

        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            assertThat(merged.callees("com.app.Sender#send()"))
                    .contains("com.lib.FileWriter#write(String)")
                    .doesNotContain("com.lib.BufferedWriter#write(String)");
        }
    }

    @Test
    void paramCount_breaksNameTie_whenNoReceiverTypeKnown() throws IOException {
        // Two overloads of process() — one with 1 param, one with 2
        // ref has paramCount=2, no receiverTypeName → param count breaks the tie
        Path indexA = tempDir.resolve("param-a");
        Path indexB = tempDir.resolve("param-b");

        createGraph(indexA, "com.app.Client#run()");
        createGraph(indexB,
                "com.lib.Processor#process(String)",          // wrong arity
                "com.lib.Processor#process(String,int)");     // correct arity

        ProjectMeta metaA = projectMeta("param-a", indexA);
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Client#run()", "process", null, 2, null, 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("param-b", indexB));

        linker.buildCrossProjectGraph("param-a", "param-b");

        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            assertThat(merged.callees("com.app.Client#run()"))
                    .contains("com.lib.Processor#process(String,int)")
                    .doesNotContain("com.lib.Processor#process(String)");
        }
    }

    @Test
    void packageHint_hardFiltersToCorrectPackage() throws IOException {
        // Same method name "connect()" exists in two packages; packageHint narrows to the right one
        Path indexA = tempDir.resolve("pkg-a");
        Path indexB = tempDir.resolve("pkg-b");

        createGraph(indexA, "com.app.Main#start()");
        createGraph(indexB,
                "com.db.jdbc.Connection#connect()",    // correct package
                "com.net.http.Connection#connect()");  // wrong package

        ProjectMeta metaA = projectMeta("pkg-a", indexA);
        // packageHint inferred from "import com.db.jdbc.Connection"
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Main#start()", "connect", "Connection", 0, "com.db.jdbc", 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("pkg-b", indexB));

        linker.buildCrossProjectGraph("pkg-a", "pkg-b");

        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            assertThat(merged.callees("com.app.Main#start()"))
                    .contains("com.db.jdbc.Connection#connect()")
                    .doesNotContain("com.net.http.Connection#connect()");
        }
    }

    @Test
    void packageHint_fallsBackToAllCandidates_whenNoMatchInHintedPackage() throws IOException {
        // packageHint points to a package not present in proj-b → fall back to full list
        Path indexA = tempDir.resolve("fallback-a");
        Path indexB = tempDir.resolve("fallback-b");

        createGraph(indexA, "com.app.Worker#doWork()");
        createGraph(indexB, "com.lib.Engine#execute()");

        ProjectMeta metaA = projectMeta("fallback-a", indexA);
        // packageHint points to nonexistent package — should fall back and still resolve
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Worker#doWork()", "execute", null, 0, "com.nonexistent.pkg", 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("fallback-b", indexB));

        linker.buildCrossProjectGraph("fallback-a", "fallback-b");

        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            // Falls back to name-only match — still resolves
            assertThat(merged.callees("com.app.Worker#doWork()"))
                    .contains("com.lib.Engine#execute()");
        }
    }

    @Test
    void receiverType_outweighs_paramCount_in_scoring() throws IOException {
        // Three candidates for "save(X)":
        //   com.lib.WrongStore#save(String)   — param count match (+1), wrong class
        //   com.lib.DataStore#save(String,int) — class match (+2), wrong arity
        //   → DataStore wins (score 2 > score 1)
        Path indexA = tempDir.resolve("score-weight-a");
        Path indexB = tempDir.resolve("score-weight-b");

        createGraph(indexA, "com.app.Service#run()");
        createGraph(indexB,
                "com.lib.WrongStore#save(String)",      // param match only (+1)
                "com.lib.DataStore#save(String,int)");  // class match only (+2)

        ProjectMeta metaA = projectMeta("score-weight-a", indexA);
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Service#run()", "save", "DataStore", 1, null, 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("score-weight-b", indexB));

        linker.buildCrossProjectGraph("score-weight-a", "score-weight-b");

        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            assertThat(merged.callees("com.app.Service#run()"))
                    .contains("com.lib.DataStore#save(String,int)")
                    .doesNotContain("com.lib.WrongStore#save(String)");
        }
    }

    // -----------------------------------------------------------------------
    // Limitation 2: stale-link refresh — graph reflects re-index
    // -----------------------------------------------------------------------

    @Test
    void rebuildCrossProjectGraph_picksUpNewGraphAfterReindex() throws IOException {
        Path indexA = tempDir.resolve("refresh-a");
        Path indexB = tempDir.resolve("refresh-b");

        createGraph(indexA, "com.app.A#oldMethod()");
        createGraph(indexB, "com.lib.B#helper()");

        ProjectMeta metaA = projectMeta("refresh-a", indexA);
        registry.register(metaA);
        registry.register(projectMeta("refresh-b", indexB));

        // Initial build
        linker.buildCrossProjectGraph("refresh-a", "refresh-b");
        try (CallGraph merged1 = linker.loadCrossProjectGraph()) {
            assertThat(fqnExists(merged1, "com.app.A#oldMethod()")).isTrue();
            assertThat(fqnExists(merged1, "com.app.A#newMethod()")).isFalse();
        }

        // Simulate re-index: overwrite proj-a's ArcadeDB with a new graph
        try (CallGraph g = CallGraph.open(indexA.resolve("callgraph.arcadedb"))) {
            g.clear();
            g.addMethod("com.app.A#newMethod()", "com.app.A");
            g.flush();
        }
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.A#newMethod()", "helper", null, 0, null, 1);
        metaA.setUnresolvedRefs(List.of(ref));
        registry.register(metaA);

        // Rebuild — should reflect the fresh graph
        linker.buildCrossProjectGraph("refresh-a", "refresh-b");
        try (CallGraph merged2 = linker.loadCrossProjectGraph()) {
            assertThat(fqnExists(merged2, "com.app.A#newMethod()")).isTrue();
            assertThat(fqnExists(merged2, "com.app.A#oldMethod()")).isFalse();
            assertThat(merged2.callees("com.app.A#newMethod()"))
                    .contains("com.lib.B#helper()");
        }
    }

    @Test
    void buildCrossProjectGraph_throwsForUnknownProject() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> linker.buildCrossProjectGraph("no-such", "also-no"))
                .withMessageContaining("no-such");
    }

    @Test
    void loadCrossProjectGraph_returnsEmptyWhenFileDoesNotExist() {
        // loadCrossProjectGraph() creates an empty ArcadeDB if none exists — must not throw
        assertThatCode(() -> {
            try (CallGraph g = linker.loadCrossProjectGraph()) {
                // just verify it opened successfully
            }
        }).doesNotThrowAnyException();
    }

    @Test
    void buildCrossProjectGraph_handlesAbsentGraphFileGracefully() throws IOException {
        // Project registered but callgraph.arcadedb was never written
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        // Only create base directories, no callgraph.arcadedb subdirectory
        Files.createDirectories(indexA);
        Files.createDirectories(indexB);

        registry.register(projectMeta("proj-a", indexA));
        registry.register(projectMeta("proj-b", indexB));

        // Empty graphs merge to empty graph — no exception
        linker.buildCrossProjectGraph("proj-a", "proj-b");

        try (CallGraph merged = linker.loadCrossProjectGraph()) {
            assertThat(merged.methodCount()).isEqualTo(0);
        }
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

    /** Returns true when the FQN appears in the graph (as a project-owned method). */
    private static boolean fqnExists(CallGraph graph, String fqn) {
        return graph.allFqns().anyMatch(fqn::equals);
    }

    /** Extract class prefix from a FQN like "com.example.Foo#bar()" → "com.example.Foo". */
    private static String classOf(String fqn) {
        int h = fqn.indexOf('#');
        return h > 0 ? fqn.substring(0, h) : fqn;
    }

    private static ProjectMeta projectMeta(String name, Path indexPath) {
        return new ProjectMeta(name, "/src/" + name, indexPath.toAbsolutePath().toString());
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
}
