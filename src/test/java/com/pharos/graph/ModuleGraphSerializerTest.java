package com.pharos.graph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

class ModuleGraphSerializerTest {

    @TempDir
    Path tempDir;

    private final ModuleGraphSerializer serializer = new ModuleGraphSerializer();

    @Test
    void saveAndLoad_roundTrip_preservesNodes() throws Exception {
        ModuleGraph original = new ModuleGraph();
        original.addOrUpdate(ModuleNode.indexed("com.example", "app", "1.0", "my-app"));
        original.addOrUpdate(ModuleNode.external("org.third", "common-lib", "3.2.1"));

        Path file = tempDir.resolve("graph.graphml");
        serializer.save(original, file);
        ModuleGraph loaded = serializer.load(file);

        assertThat(loaded.nodeCount()).isEqualTo(2);

        ModuleNode loadedApp = loaded.findByKey("com.example:app");
        assertThat(loadedApp).isNotNull();
        assertThat(loadedApp.getStatus()).isEqualTo(ModuleNode.Status.INDEXED);
        assertThat(loadedApp.getProjectName()).isEqualTo("my-app");
        assertThat(loadedApp.getVersion()).isEqualTo("1.0");

        ModuleNode loadedLib = loaded.findByKey("org.third:common-lib");
        assertThat(loadedLib).isNotNull();
        assertThat(loadedLib.getStatus()).isEqualTo(ModuleNode.Status.EXTERNAL);
        assertThat(loadedLib.getVersion()).isEqualTo("3.2.1");
    }

    @Test
    void saveAndLoad_roundTrip_preservesEdge() throws Exception {
        ModuleGraph original = new ModuleGraph();
        ModuleNode app = original.addOrUpdate(ModuleNode.indexed("com.example", "app", "1.0", "my-app"));
        ModuleNode lib = original.addOrUpdate(ModuleNode.external("org.third", "lib", "1.0"));
        original.addDependency(app, lib, ModuleDep.compile("1.0"));

        Path file = tempDir.resolve("graph.graphml");
        serializer.save(original, file);
        ModuleGraph loaded = serializer.load(file);

        assertThat(loaded.edgeCount()).isEqualTo(1);
        ModuleNode la = loaded.findByKey("com.example:app");
        ModuleNode ll = loaded.findByKey("org.third:lib");
        assertThat(loaded.getDependencies(la)).containsExactly(ll);
        assertThat(loaded.getDependents(ll)).containsExactly(la);
    }

    @Test
    void saveAndLoad_multipleEdgesBetweenSamePair() throws Exception {
        ModuleGraph original = new ModuleGraph();
        ModuleNode a = original.addOrUpdate(ModuleNode.external("g", "a", "1"));
        ModuleNode b = original.addOrUpdate(ModuleNode.external("g", "b", "1"));
        original.addDependency(a, b, ModuleDep.compile("1.0"));
        original.addDependency(a, b, ModuleDep.test("1.0"));

        Path file = tempDir.resolve("multi-edge.graphml");
        serializer.save(original, file);
        ModuleGraph loaded = serializer.load(file);

        assertThat(loaded.edgeCount()).isEqualTo(2);
        Set<ModuleDep> edges = loaded.getEdges(loaded.findByKey("g:a"), loaded.findByKey("g:b"));
        assertThat(edges).hasSize(2);
        assertThat(edges).anyMatch(e -> "compile".equals(e.getScope()));
        assertThat(edges).anyMatch(e -> "test".equals(e.getScope()));
    }

    @Test
    void load_nonExistentFile_returnsEmptyGraph() throws Exception {
        Path file = tempDir.resolve("does-not-exist.graphml");

        ModuleGraph graph = serializer.load(file);

        assertThat(graph.nodeCount()).isEqualTo(0);
        assertThat(graph.edgeCount()).isEqualTo(0);
    }

    @Test
    void saveAndLoad_emptyGraph_roundTrips() throws Exception {
        Path file = tempDir.resolve("empty.graphml");
        serializer.save(new ModuleGraph(), file);
        ModuleGraph loaded = serializer.load(file);

        assertThat(loaded.nodeCount()).isEqualTo(0);
        assertThat(loaded.edgeCount()).isEqualTo(0);
    }

    @Test
    void saveAndLoad_specialCharsInModuleKey() throws Exception {
        // Module keys contain colons and dots which require URL encoding in GraphML node IDs
        ModuleGraph original = new ModuleGraph();
        original.addOrUpdate(ModuleNode.external("org.apache.commons", "commons-lang3", "3.12.0"));

        Path file = tempDir.resolve("special.graphml");
        serializer.save(original, file);
        ModuleGraph loaded = serializer.load(file);

        assertThat(loaded.findByKey("org.apache.commons:commons-lang3")).isNotNull();
    }

    @Test
    void saveAndLoad_nullDeclaredVersion_roundTrips() throws Exception {
        ModuleGraph original = new ModuleGraph();
        ModuleNode a = original.addOrUpdate(ModuleNode.external("g", "a", "1"));
        ModuleNode b = original.addOrUpdate(ModuleNode.external("g", "b", "1"));
        original.addDependency(a, b, ModuleDep.compile(null)); // null declaredVersion (BOM-managed)

        Path file = tempDir.resolve("null-version.graphml");
        serializer.save(original, file);
        ModuleGraph loaded = serializer.load(file);

        Set<ModuleDep> edges = loaded.getEdges(loaded.findByKey("g:a"), loaded.findByKey("g:b"));
        assertThat(edges).hasSize(1);
        assertThat(edges.iterator().next().getDeclaredVersion()).isNull();
    }

    @Test
    void save_writesAtomically_viaReplaceExisting() throws Exception {
        Path file = tempDir.resolve("atomic.graphml");

        // First save
        ModuleGraph g1 = new ModuleGraph();
        g1.addOrUpdate(ModuleNode.external("g", "first", "1"));
        serializer.save(g1, file);

        // Second save overwrites
        ModuleGraph g2 = new ModuleGraph();
        g2.addOrUpdate(ModuleNode.external("g", "second", "2"));
        serializer.save(g2, file);

        ModuleGraph loaded = serializer.load(file);
        assertThat(loaded.nodeCount()).isEqualTo(1);
        assertThat(loaded.findByKey("g:second")).isNotNull();
        assertThat(loaded.findByKey("g:first")).isNull();
    }
}
