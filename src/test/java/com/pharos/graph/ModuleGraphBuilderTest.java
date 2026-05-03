package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class ModuleGraphBuilderTest {

    @TempDir
    Path tempDir;

    private TestRegistry registry;
    private ModuleGraphBuilder builder;

    @BeforeEach
    void setUp() {
        registry = new TestRegistry();
        builder = new ModuleGraphBuilder(registry);
    }

    // --- autoLink ---

    @Test
    void autoLink_upgradesExternalNodeWhenRegistryProjectMatches() {
        ModuleGraph graph = new ModuleGraph();
        ModuleNode externalNode = graph.addOrUpdate(
                ModuleNode.external("com.example", "my-lib", "1.0"));

        ProjectMeta meta = new ProjectMeta("my-lib-project", "/path", "/index");
        meta.setGroupId("com.example");
        meta.setArtifactId("my-lib");
        meta.setMavenVersion("1.5");
        registry.register(meta);

        List<String> linked = builder.autoLink(graph);

        assertThat(linked).containsExactly("my-lib-project");
        assertThat(externalNode.isIndexed()).isTrue();
        assertThat(externalNode.getProjectName()).isEqualTo("my-lib-project");
        assertThat(externalNode.getVersion()).isEqualTo("1.5");
    }

    @Test
    void autoLink_doesNotUpgradeAlreadyIndexedNode() {
        ModuleGraph graph = new ModuleGraph();
        graph.addOrUpdate(ModuleNode.indexed("com.example", "lib", "1.0", "existing-project"));

        ProjectMeta meta = new ProjectMeta("another-project", "/path", "/index");
        meta.setGroupId("com.example");
        meta.setArtifactId("lib");
        registry.register(meta);

        List<String> linked = builder.autoLink(graph);

        assertThat(linked).isEmpty();
    }

    @Test
    void autoLink_skipsProjectsWithoutMavenCoordinates() {
        ModuleGraph graph = new ModuleGraph();
        graph.addOrUpdate(ModuleNode.external("com.example", "lib", "1.0"));

        ProjectMeta meta = new ProjectMeta("some-project", "/path", "/index");
        // groupId / artifactId intentionally not set
        registry.register(meta);

        List<String> linked = builder.autoLink(graph);

        assertThat(linked).isEmpty();
    }

    @Test
    void autoLink_upgradesMultipleNodes() {
        ModuleGraph graph = new ModuleGraph();
        graph.addOrUpdate(ModuleNode.external("com.example", "lib-a", "1.0"));
        graph.addOrUpdate(ModuleNode.external("com.example", "lib-b", "1.0"));

        ProjectMeta metaA = new ProjectMeta("proj-a", "/path", "/index");
        metaA.setGroupId("com.example");
        metaA.setArtifactId("lib-a");
        metaA.setMavenVersion("1.0");
        registry.register(metaA);

        ProjectMeta metaB = new ProjectMeta("proj-b", "/path", "/index");
        metaB.setGroupId("com.example");
        metaB.setArtifactId("lib-b");
        metaB.setMavenVersion("1.0");
        registry.register(metaB);

        List<String> linked = builder.autoLink(graph);

        assertThat(linked).containsExactlyInAnyOrder("proj-a", "proj-b");
    }

    @Test
    void autoLink_returnsEmptyWhenGraphIsEmpty() {
        ModuleGraph graph = new ModuleGraph();
        ProjectMeta meta = new ProjectMeta("proj", "/path", "/index");
        meta.setGroupId("com.example");
        meta.setArtifactId("lib");
        registry.register(meta);

        List<String> linked = builder.autoLink(graph);

        assertThat(linked).isEmpty();
    }

    @Test
    void autoLink_returnsEmptyWhenRegistryIsEmpty() {
        ModuleGraph graph = new ModuleGraph();
        graph.addOrUpdate(ModuleNode.external("com.example", "lib", "1.0"));

        List<String> linked = builder.autoLink(graph);

        assertThat(linked).isEmpty();
    }

    // --- In-memory test registry that avoids writing to ~/.pharos/registry.json ---

    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() {
            super(IndexConfig.defaults());
        }

        @Override
        public synchronized void register(ProjectMeta meta) {
            store.put(meta.getName(), meta);
        }

        @Override
        public synchronized Optional<ProjectMeta> find(String name) {
            return Optional.ofNullable(store.get(name));
        }

        @Override
        public synchronized List<ProjectMeta> listAll() {
            return new ArrayList<>(store.values());
        }

        @Override
        public synchronized void link(String p1, String p2) {
            // no-op for tests
        }

        @Override
        public synchronized void unregister(String name) {
            store.remove(name);
        }
    }
}
