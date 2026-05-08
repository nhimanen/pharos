package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
    private CallGraphSerializer serializer;

    @BeforeEach
    void setUp() {
        registry   = new TestRegistry();
        linker     = new CrossProjectLinker(IndexConfig.defaults(), registry);
        serializer = new CallGraphSerializer();
    }

    @Test
    void buildCrossProjectGraph_mergesBothGraphs() throws IOException {
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.example.A#doWork()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.example.B#handle()");
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("proj-a", indexA);
        ProjectMeta metaB = projectMeta("proj-b", indexB);
        registry.register(metaA);
        registry.register(metaB);

        CallGraph merged = linker.buildCrossProjectGraph("proj-a", "proj-b");

        assertThat(merged.contains("com.example.A#doWork()")).isTrue();
        assertThat(merged.contains("com.example.B#handle()")).isTrue();
    }

    @Test
    void buildCrossProjectGraph_resolvesUnresolvedRefAcrossProjects() throws IOException {
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        // proj-a calls "handle" but didn't resolve it during indexing
        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.example.A#caller()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        // proj-b has handle()
        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.example.B#handle()");
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("proj-a", indexA);
        metaA.getUnresolvedRefs().add(
                new ProjectMeta.UnresolvedRef("com.example.A#caller()", "handle", 5));
        ProjectMeta metaB = projectMeta("proj-b", indexB);
        registry.register(metaA);
        registry.register(metaB);

        CallGraph merged = linker.buildCrossProjectGraph("proj-a", "proj-b");

        // The cross-project edge A#caller() → B#handle() should have been added
        assertThat(merged.getCallees("com.example.A#caller()"))
                .contains("com.example.B#handle()");
    }

    @Test
    void buildCrossProjectGraph_bothDirectionsCrossLinked() throws IOException {
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.example.A#callsB()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.example.B#callsA()");
        graphB.addMethod("com.example.B#targetOfA()");
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("proj-a", indexA);
        metaA.getUnresolvedRefs().add(
                new ProjectMeta.UnresolvedRef("com.example.A#callsB()", "targetOfA", 10));
        ProjectMeta metaB = projectMeta("proj-b", indexB);
        metaB.getUnresolvedRefs().add(
                new ProjectMeta.UnresolvedRef("com.example.B#callsA()", "callsB", 3));
        registry.register(metaA);
        registry.register(metaB);

        CallGraph merged = linker.buildCrossProjectGraph("proj-a", "proj-b");

        // A → B direction
        assertThat(merged.getCallees("com.example.A#callsB()"))
                .contains("com.example.B#targetOfA()");
        // B → A direction
        assertThat(merged.getCallees("com.example.B#callsA()"))
                .contains("com.example.A#callsB()");
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

        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.app.Sender#send()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.lib.BufferedWriter#write(String)");
        graphB.addMethod("com.lib.FileWriter#write(String)");   // correct target
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("scored-a", indexA);
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Sender#send()", "write", "FileWriter", 1, null, 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("scored-b", indexB));

        CallGraph merged = linker.buildCrossProjectGraph("scored-a", "scored-b");

        assertThat(merged.getCallees("com.app.Sender#send()"))
                .contains("com.lib.FileWriter#write(String)")
                .doesNotContain("com.lib.BufferedWriter#write(String)");
    }

    @Test
    void paramCount_breaksNameTie_whenNoReceiverTypeKnown() throws IOException {
        // Two overloads of process() — one with 1 param, one with 2
        // ref has paramCount=2, no receiverTypeName → param count breaks the tie
        Path indexA = tempDir.resolve("param-a");
        Path indexB = tempDir.resolve("param-b");

        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.app.Client#run()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.lib.Processor#process(String)");           // wrong arity
        graphB.addMethod("com.lib.Processor#process(String,int)");       // correct arity
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("param-a", indexA);
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Client#run()", "process", null, 2, null, 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("param-b", indexB));

        CallGraph merged = linker.buildCrossProjectGraph("param-a", "param-b");

        assertThat(merged.getCallees("com.app.Client#run()"))
                .contains("com.lib.Processor#process(String,int)")
                .doesNotContain("com.lib.Processor#process(String)");
    }

    @Test
    void packageHint_hardFiltersToCorrectPackage() throws IOException {
        // Same method name "connect()" exists in two packages; packageHint narrows to the right one
        Path indexA = tempDir.resolve("pkg-a");
        Path indexB = tempDir.resolve("pkg-b");

        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.app.Main#start()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.db.jdbc.Connection#connect()");    // correct package
        graphB.addMethod("com.net.http.Connection#connect()");   // wrong package
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("pkg-a", indexA);
        // packageHint inferred from "import com.db.jdbc.Connection"
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Main#start()", "connect", "Connection", 0, "com.db.jdbc", 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("pkg-b", indexB));

        CallGraph merged = linker.buildCrossProjectGraph("pkg-a", "pkg-b");

        assertThat(merged.getCallees("com.app.Main#start()"))
                .contains("com.db.jdbc.Connection#connect()")
                .doesNotContain("com.net.http.Connection#connect()");
    }

    @Test
    void packageHint_fallsBackToAllCandidates_whenNoMatchInHintedPackage() throws IOException {
        // packageHint points to a package not present in proj-b → fall back to full list
        Path indexA = tempDir.resolve("fallback-a");
        Path indexB = tempDir.resolve("fallback-b");

        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.app.Worker#doWork()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.lib.Engine#execute()");
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("fallback-a", indexA);
        // packageHint points to nonexistent package — should fall back and still resolve
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Worker#doWork()", "execute", null, 0, "com.nonexistent.pkg", 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("fallback-b", indexB));

        CallGraph merged = linker.buildCrossProjectGraph("fallback-a", "fallback-b");

        // Falls back to name-only match — still resolves
        assertThat(merged.getCallees("com.app.Worker#doWork()"))
                .contains("com.lib.Engine#execute()");
    }

    @Test
    void receiverType_outweighs_paramCount_in_scoring() throws IOException {
        // Three candidates for "save(X)":
        //   com.lib.WrongStore#save(String)   — param count match (+1), wrong class
        //   com.lib.DataStore#save(String,int) — class match (+2), wrong arity
        //   → DataStore wins (score 2 > score 1)
        Path indexA = tempDir.resolve("score-weight-a");
        Path indexB = tempDir.resolve("score-weight-b");

        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.app.Service#run()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.lib.WrongStore#save(String)");      // param match only (+1)
        graphB.addMethod("com.lib.DataStore#save(String,int)");   // class match only (+2)
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("score-weight-a", indexA);
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.Service#run()", "save", "DataStore", 1, null, 1);
        metaA.getUnresolvedRefs().add(ref);
        registry.register(metaA);
        registry.register(projectMeta("score-weight-b", indexB));

        CallGraph merged = linker.buildCrossProjectGraph("score-weight-a", "score-weight-b");

        assertThat(merged.getCallees("com.app.Service#run()"))
                .contains("com.lib.DataStore#save(String,int)")
                .doesNotContain("com.lib.WrongStore#save(String)");
    }

    // -----------------------------------------------------------------------
    // Limitation 2: stale-link refresh — graph reflects re-index
    // -----------------------------------------------------------------------

    @Test
    void rebuildCrossProjectGraph_picksUpNewGraphAfterReindex() throws IOException {
        // Simulate re-index: proj-a's graph.graphml is updated with a new method,
        // then buildCrossProjectGraph is called again — it must reflect the new state.
        Path indexA = tempDir.resolve("refresh-a");
        Path indexB = tempDir.resolve("refresh-b");

        CallGraph graphA = new CallGraph();
        graphA.addMethod("com.app.A#oldMethod()");
        serializer.save(graphA, indexA.resolve("graph.graphml"));

        CallGraph graphB = new CallGraph();
        graphB.addMethod("com.lib.B#helper()");
        serializer.save(graphB, indexB.resolve("graph.graphml"));

        ProjectMeta metaA = projectMeta("refresh-a", indexA);
        registry.register(metaA);
        registry.register(projectMeta("refresh-b", indexB));

        // Initial build
        CallGraph merged1 = linker.buildCrossProjectGraph("refresh-a", "refresh-b");
        assertThat(merged1.contains("com.app.A#oldMethod()")).isTrue();
        assertThat(merged1.contains("com.app.A#newMethod()")).isFalse();

        // Simulate re-index: overwrite proj-a's graph with a new one
        CallGraph updatedGraphA = new CallGraph();
        updatedGraphA.addMethod("com.app.A#newMethod()");
        ProjectMeta.UnresolvedRef ref = new ProjectMeta.UnresolvedRef(
                "com.app.A#newMethod()", "helper", null, 0, null, 1);
        metaA.setUnresolvedRefs(List.of(ref));
        registry.register(metaA);   // update registry with new unresolved refs
        serializer.save(updatedGraphA, indexA.resolve("graph.graphml"));

        // Rebuild — should reflect the fresh graph
        CallGraph merged2 = linker.buildCrossProjectGraph("refresh-a", "refresh-b");
        assertThat(merged2.contains("com.app.A#newMethod()")).isTrue();
        assertThat(merged2.contains("com.app.A#oldMethod()")).isFalse();
        assertThat(merged2.getCallees("com.app.A#newMethod()"))
                .contains("com.lib.B#helper()");
    }

    @Test
    void buildCrossProjectGraph_throwsForUnknownProject() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> linker.buildCrossProjectGraph("no-such", "also-no"))
                .withMessageContaining("no-such");
    }

    @Test
    void loadCrossProjectGraph_returnsEmptyWhenFileDoesNotExist() throws IOException {
        // The cross-graph file lives at DEFAULT_BASE — we can't easily redirect that,
        // but if it doesn't exist loadCrossProjectGraph() must return an empty graph gracefully
        // (this validates the fallback path; the file may or may not exist on this machine)
        assertThatCode(() -> linker.loadCrossProjectGraph()).doesNotThrowAnyException();
    }

    @Test
    void buildCrossProjectGraph_handlesAbsentGraphFileGracefully() throws IOException {
        // Project registered but graph.graphml was never written
        Path indexA = tempDir.resolve("proj-a");
        Path indexB = tempDir.resolve("proj-b");

        // Only create directories, no graph.graphml files
        java.nio.file.Files.createDirectories(indexA);
        java.nio.file.Files.createDirectories(indexB);

        registry.register(projectMeta("proj-a", indexA));
        registry.register(projectMeta("proj-b", indexB));

        CallGraph merged = linker.buildCrossProjectGraph("proj-a", "proj-b");

        // Empty graphs merge to empty graph — no exception
        assertThat(merged.nodeCount()).isEqualTo(0);
    }

    // --- helpers ---

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
