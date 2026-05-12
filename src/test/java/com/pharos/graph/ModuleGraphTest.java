package com.pharos.graph;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

class ModuleGraphTest {

    @TempDir
    Path tempDir;

    private ModuleGraph graph;


    @BeforeEach
    void setUp() {
        graph = ModuleGraph.open(tempDir.resolve("test-module.arcadedb"));
    }

    @AfterEach
    void tearDown() {
        graph.close();
    }

    // --- addOrUpdate ---

    @Test
    void addOrUpdate_addsNewNode() {
        graph.addOrUpdate("com.example:lib-a", "com.example", "lib-a", "1.0", "EXTERNAL", null);

        assertThat(graph.moduleCount()).isEqualTo(1);
        assertThat(graph.findByKey("com.example:lib-a")).isPresent();
    }

    @Test
    void addOrUpdate_deduplicatesByModuleKey() {
        graph.addOrUpdate("com.example:lib", "com.example", "lib", "1.0", "EXTERNAL", null);
        graph.addOrUpdate("com.example:lib", "com.example", "lib", "2.0", "EXTERNAL", null);

        assertThat(graph.moduleCount()).isEqualTo(1);
    }

    @Test
    void addOrUpdate_upgradesExternalToIndexedWhenNewNodeIsIndexed() {
        graph.addOrUpdate("com.example:lib", "com.example", "lib", "1.0", "EXTERNAL", null);
        graph.addOrUpdate("com.example:lib", "com.example", "lib", "1.1", "INDEXED", "my-project");

        ModuleNodeData data = graph.findByKey("com.example:lib").orElseThrow();
        assertThat(data.isIndexed()).isTrue();
        assertThat(data.projectName()).isEqualTo("my-project");
        assertThat(data.version()).isEqualTo("1.1");
        assertThat(graph.moduleCount()).isEqualTo(1);
    }

    @Test
    void addOrUpdate_doesNotDowngradeIndexedToExternal() {
        graph.addOrUpdate("com.example:lib", "com.example", "lib", "1.0", "INDEXED", "project");
        graph.addOrUpdate("com.example:lib", "com.example", "lib", "2.0", "EXTERNAL", null);

        ModuleNodeData data = graph.findByKey("com.example:lib").orElseThrow();
        assertThat(data.isIndexed()).isTrue();
        assertThat(graph.moduleCount()).isEqualTo(1);
    }

    // --- addDependency ---

    @Test
    void addDependency_addsEdgeAndUpdatesTraversals() {
        graph.addOrUpdate("g:app", "g", "app", "1.0", "EXTERNAL", null);
        graph.addOrUpdate("g:lib", "g", "lib", "1.0", "EXTERNAL", null);

        graph.addDependency("g:app", "g:lib", "compile", "1.0");

        assertThat(graph.dependencyCount()).isEqualTo(1);
        assertThat(graph.dependencies("g:app"))
                .extracting(ModuleNodeData::moduleKey)
                .containsExactly("g:lib");
        assertThat(graph.dependents("g:lib"))
                .extracting(ModuleNodeData::moduleKey)
                .containsExactly("g:app");
    }

    @Test
    void addDependency_deduplicatesByScopeForSamePair() {
        graph.addOrUpdate("g:app", "g", "app", "1", "EXTERNAL", null);
        graph.addOrUpdate("g:lib", "g", "lib", "1", "EXTERNAL", null);

        graph.addDependency("g:app", "g:lib", "compile", "1.0");
        graph.addDependency("g:app", "g:lib", "compile", "1.0"); // duplicate

        assertThat(graph.dependencyCount()).isEqualTo(1);
    }

    @Test
    void addDependency_allowsDifferentScopesForSamePair() {
        graph.addOrUpdate("g:app", "g", "app", "1", "EXTERNAL", null);
        graph.addOrUpdate("g:lib", "g", "lib", "1", "EXTERNAL", null);

        graph.addDependency("g:app", "g:lib", "compile", "1.0");
        graph.addDependency("g:app", "g:lib", "test", "1.0");

        assertThat(graph.dependencyCount()).isEqualTo(2);
    }

    @Test
    void addDependency_ignoresAbsentTargetVertex() {
        graph.addOrUpdate("g:a", "g", "a", "1", "EXTERNAL", null);

        // "g:ghost" doesn't exist — edge silently ignored
        assertThatCode(() -> graph.addDependency("g:a", "g:ghost", "compile", null))
                .doesNotThrowAnyException();
        assertThat(graph.dependencyCount()).isEqualTo(0);
    }

    // --- findByKey / findByProjectName ---

    @Test
    void findByKey_returnsCorrectNode() {
        graph.addOrUpdate("com.example:lib", "com.example", "lib", "1.0", "EXTERNAL", null);

        assertThat(graph.findByKey("com.example:lib"))
                .map(ModuleNodeData::moduleKey)
                .hasValue("com.example:lib");
    }

    @Test
    void findByKey_returnsEmptyForAbsent() {
        assertThat(graph.findByKey("com.example:nonexistent")).isEmpty();
    }

    @Test
    void findByProjectName_findsIndexedNode() {
        graph.addOrUpdate("com.example:lib", "com.example", "lib", "1.0", "INDEXED", "my-project");

        assertThat(graph.findByProjectName("my-project"))
                .map(ModuleNodeData::moduleKey)
                .hasValue("com.example:lib");
    }

    @Test
    void findByProjectName_returnsEmptyForMissingProject() {
        assertThat(graph.findByProjectName("no-such-project")).isEmpty();
    }

    // --- shortestPath (was findPath) ---

    @Test
    void shortestPath_returnsDirectDependencyPath() {
        graph.addOrUpdate("g:a", "g", "a", "1", "EXTERNAL", null);
        graph.addOrUpdate("g:b", "g", "b", "1", "EXTERNAL", null);
        graph.addDependency("g:a", "g:b", "compile", null);

        assertThat(graph.shortestPath("g:a", "g:b"))
                .extracting(ModuleNodeData::moduleKey)
                .containsExactly("g:a", "g:b");
    }

    @Test
    void shortestPath_returnsTransitivePath() {
        graph.addOrUpdate("g:a", "g", "a", "1", "EXTERNAL", null);
        graph.addOrUpdate("g:b", "g", "b", "1", "EXTERNAL", null);
        graph.addOrUpdate("g:c", "g", "c", "1", "EXTERNAL", null);
        graph.addDependency("g:a", "g:b", "compile", null);
        graph.addDependency("g:b", "g:c", "compile", null);

        assertThat(graph.shortestPath("g:a", "g:c"))
                .extracting(ModuleNodeData::moduleKey)
                .containsExactly("g:a", "g:b", "g:c");
    }

    @Test
    void shortestPath_returnsEmptyWhenNoPath() {
        graph.addOrUpdate("g:a", "g", "a", "1", "EXTERNAL", null);
        graph.addOrUpdate("g:b", "g", "b", "1", "EXTERNAL", null);
        // no edges

        assertThat(graph.shortestPath("g:a", "g:b")).isEmpty();
    }

    @Test
    void shortestPath_returnsEmptyWhenNodeMissing() {
        graph.addOrUpdate("g:a", "g", "a", "1", "EXTERNAL", null);

        assertThat(graph.shortestPath("g:a", "g:nonexistent")).isEmpty();
    }

    // --- dependencies / dependents ---

    @Test
    void dependencies_returnsEmptySetForNodeWithNoDeps() {
        graph.addOrUpdate("g:a", "g", "a", "1", "EXTERNAL", null);

        assertThat(graph.dependencies("g:a")).isEmpty();
    }

    @Test
    void dependencies_returnsEmptySetForAbsentNode() {
        assertThat(graph.dependencies("g:ghost")).isEmpty();
    }

    @Test
    void dependents_returnsEmptySetForAbsentNode() {
        assertThat(graph.dependents("g:ghost")).isEmpty();
    }

    @Test
    void dependencies_returnsMultipleTargets() {
        graph.addOrUpdate("g:app",  "g", "app",  "1", "EXTERNAL", null);
        graph.addOrUpdate("g:lib1", "g", "lib1", "1", "EXTERNAL", null);
        graph.addOrUpdate("g:lib2", "g", "lib2", "1", "EXTERNAL", null);
        graph.addDependency("g:app", "g:lib1", "compile", null);
        graph.addDependency("g:app", "g:lib2", "compile", null);

        assertThat(graph.dependencies("g:app"))
                .extracting(ModuleNodeData::moduleKey)
                .containsExactlyInAnyOrder("g:lib1", "g:lib2");
    }

    // --- allModules / moduleCount / dependencyCount ---

    @Test
    void allModules_includesAllAddedNodes() {
        graph.addOrUpdate("g:a", "g", "a", "1", "EXTERNAL", null);
        graph.addOrUpdate("g:b", "g", "b", "1", "EXTERNAL", null);

        assertThat(graph.allModules()
                .map(ModuleNodeData::moduleKey)
                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("g:a", "g:b");
    }

    @Test
    void emptyGraph_hasZeroNodesAndEdges() {
        assertThat(graph.moduleCount()).isEqualTo(0);
        assertThat(graph.dependencyCount()).isEqualTo(0);
    }
}
