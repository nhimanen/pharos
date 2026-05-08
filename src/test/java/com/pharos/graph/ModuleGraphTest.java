package com.pharos.graph;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class ModuleGraphTest {

    private ModuleGraph graph;

    @BeforeEach
    void setUp() {
        graph = new ModuleGraph();
    }

    // --- addOrUpdate ---

    @Test
    void addOrUpdate_addsNewNode_returnsIt() {
        ModuleNode node = ModuleNode.external("com.example", "lib-a", "1.0");

        ModuleNode canonical = graph.addOrUpdate(node);

        assertThat(canonical).isSameAs(node);
        assertThat(graph.nodeCount()).isEqualTo(1);
    }

    @Test
    void addOrUpdate_deduplicatesByModuleKey() {
        ModuleNode a = ModuleNode.external("com.example", "lib", "1.0");
        ModuleNode b = ModuleNode.external("com.example", "lib", "2.0");

        ModuleNode c1 = graph.addOrUpdate(a);
        ModuleNode c2 = graph.addOrUpdate(b);

        assertThat(c1).isSameAs(c2);
        assertThat(graph.nodeCount()).isEqualTo(1);
    }

    @Test
    void addOrUpdate_upgradesExternalToIndexedWhenNewNodeIsIndexed() {
        ModuleNode external = ModuleNode.external("com.example", "lib", "1.0");
        graph.addOrUpdate(external);

        ModuleNode indexed = ModuleNode.indexed("com.example", "lib", "1.1", "my-project");
        ModuleNode canonical = graph.addOrUpdate(indexed);

        assertThat(canonical.isIndexed()).isTrue();
        assertThat(canonical.getProjectName()).isEqualTo("my-project");
        assertThat(canonical.getVersion()).isEqualTo("1.1");
        assertThat(graph.nodeCount()).isEqualTo(1);
    }

    @Test
    void addOrUpdate_doesNotDowngradeIndexedToExternal() {
        graph.addOrUpdate(ModuleNode.indexed("com.example", "lib", "1.0", "project"));
        ModuleNode canonical = graph.addOrUpdate(ModuleNode.external("com.example", "lib", "2.0"));

        assertThat(canonical.isIndexed()).isTrue();
        assertThat(graph.nodeCount()).isEqualTo(1);
    }

    // --- addDependency ---

    @Test
    void addDependency_addsEdgeAndUpdatesTraversals() {
        ModuleNode a = graph.addOrUpdate(ModuleNode.external("com.example", "app", "1.0"));
        ModuleNode b = graph.addOrUpdate(ModuleNode.external("com.example", "lib", "1.0"));

        graph.addDependency(a, b, ModuleDep.compile("1.0"));

        assertThat(graph.edgeCount()).isEqualTo(1);
        assertThat(graph.getDependencies(a)).containsExactly(b);
        assertThat(graph.getDependents(b)).containsExactly(a);
    }

    @Test
    void addDependency_deduplicatesByScopeForSamePair() {
        ModuleNode a = graph.addOrUpdate(ModuleNode.external("g", "app", "1"));
        ModuleNode b = graph.addOrUpdate(ModuleNode.external("g", "lib", "1"));

        graph.addDependency(a, b, ModuleDep.compile("1.0"));
        graph.addDependency(a, b, ModuleDep.compile("1.0")); // duplicate

        assertThat(graph.edgeCount()).isEqualTo(1);
    }

    @Test
    void addDependency_allowsDifferentScopesForSamePair() {
        ModuleNode a = graph.addOrUpdate(ModuleNode.external("g", "app", "1"));
        ModuleNode b = graph.addOrUpdate(ModuleNode.external("g", "lib", "1"));

        graph.addDependency(a, b, ModuleDep.compile("1.0"));
        graph.addDependency(a, b, ModuleDep.test("1.0"));

        assertThat(graph.edgeCount()).isEqualTo(2);
    }

    @Test
    void addDependency_ignoresAbsentVertices() {
        ModuleNode notInGraph = ModuleNode.external("g", "ghost", "1");
        ModuleNode a = graph.addOrUpdate(ModuleNode.external("g", "a", "1"));

        // Should not throw; edge silently ignored
        assertThatCode(() -> graph.addDependency(a, notInGraph, ModuleDep.compile(null)))
                .doesNotThrowAnyException();
        assertThat(graph.edgeCount()).isEqualTo(0);
    }

    // --- findByKey / findByProjectName ---

    @Test
    void findByKey_returnsCorrectNode() {
        ModuleNode node = graph.addOrUpdate(ModuleNode.external("com.example", "lib", "1.0"));

        assertThat(graph.findByKey("com.example:lib")).isSameAs(node);
    }

    @Test
    void findByKey_returnsNullForAbsent() {
        assertThat(graph.findByKey("com.example:nonexistent")).isNull();
    }

    @Test
    void findByProjectName_findsIndexedNode() {
        ModuleNode indexed = graph.addOrUpdate(
                ModuleNode.indexed("com.example", "lib", "1.0", "my-project"));

        assertThat(graph.findByProjectName("my-project")).contains(indexed);
    }

    @Test
    void findByProjectName_returnsEmptyForMissingProject() {
        assertThat(graph.findByProjectName("no-such-project")).isEmpty();
    }

    // --- findPath ---

    @Test
    void findPath_returnsDirectDependencyPath() {
        ModuleNode a = graph.addOrUpdate(ModuleNode.external("g", "a", "1"));
        ModuleNode b = graph.addOrUpdate(ModuleNode.external("g", "b", "1"));
        graph.addDependency(a, b, ModuleDep.compile(null));

        List<ModuleNode> path = graph.findPath("g:a", "g:b");

        assertThat(path).containsExactly(a, b);
    }

    @Test
    void findPath_returnsTransitivePath() {
        ModuleNode a = graph.addOrUpdate(ModuleNode.external("g", "a", "1"));
        ModuleNode b = graph.addOrUpdate(ModuleNode.external("g", "b", "1"));
        ModuleNode c = graph.addOrUpdate(ModuleNode.external("g", "c", "1"));
        graph.addDependency(a, b, ModuleDep.compile(null));
        graph.addDependency(b, c, ModuleDep.compile(null));

        List<ModuleNode> path = graph.findPath("g:a", "g:c");

        assertThat(path).containsExactly(a, b, c);
    }

    @Test
    void findPath_returnsEmptyWhenNoPath() {
        graph.addOrUpdate(ModuleNode.external("g", "a", "1"));
        graph.addOrUpdate(ModuleNode.external("g", "b", "1"));
        // no edges

        assertThat(graph.findPath("g:a", "g:b")).isEmpty();
    }

    @Test
    void findPath_returnsEmptyWhenNodeMissing() {
        graph.addOrUpdate(ModuleNode.external("g", "a", "1"));

        assertThat(graph.findPath("g:a", "g:nonexistent")).isEmpty();
    }

    // --- getDependencies / getDependents ---

    @Test
    void getDependencies_returnsEmptySetForNodeWithNoDeps() {
        ModuleNode a = graph.addOrUpdate(ModuleNode.external("g", "a", "1"));

        assertThat(graph.getDependencies(a)).isEmpty();
    }

    @Test
    void getDependencies_returnsEmptySetForAbsentNode() {
        ModuleNode absent = ModuleNode.external("g", "ghost", "1");

        assertThat(graph.getDependencies(absent)).isEmpty();
    }

    @Test
    void getDependents_returnsEmptySetForAbsentNode() {
        ModuleNode absent = ModuleNode.external("g", "ghost", "1");

        assertThat(graph.getDependents(absent)).isEmpty();
    }

    @Test
    void getDependencies_returnsMultipleTargets() {
        ModuleNode app = graph.addOrUpdate(ModuleNode.external("g", "app", "1"));
        ModuleNode lib1 = graph.addOrUpdate(ModuleNode.external("g", "lib1", "1"));
        ModuleNode lib2 = graph.addOrUpdate(ModuleNode.external("g", "lib2", "1"));
        graph.addDependency(app, lib1, ModuleDep.compile(null));
        graph.addDependency(app, lib2, ModuleDep.compile(null));

        assertThat(graph.getDependencies(app)).containsExactlyInAnyOrder(lib1, lib2);
    }

    // --- allNodes / nodeCount / edgeCount ---

    @Test
    void allNodes_includesAllAddedNodes() {
        ModuleNode a = graph.addOrUpdate(ModuleNode.external("g", "a", "1"));
        ModuleNode b = graph.addOrUpdate(ModuleNode.external("g", "b", "1"));

        assertThat(graph.allNodes()).containsExactlyInAnyOrder(a, b);
    }

    @Test
    void emptyGraph_hasZeroNodesAndEdges() {
        assertThat(graph.nodeCount()).isEqualTo(0);
        assertThat(graph.edgeCount()).isEqualTo(0);
    }
}
