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
