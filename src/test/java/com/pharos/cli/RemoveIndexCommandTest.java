package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNode;
import com.pharos.indexer.LuceneIndexer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for RemoveIndexCommand.
 *
 * Uses a real temp-dir for file-system I/O and in-memory stubs for registry
 * and module graph to avoid touching ~/.pharos.
 */
class RemoveIndexCommandTest {

    @TempDir
    Path tempDir;

    private IndexConfig config;
    private TestRegistry registry;
    private LuceneIndexer luceneIndexer;
    private TestModuleGraphBuilder moduleGraphBuilder;

    @BeforeEach
    void setUp() {
        config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));
        registry = new TestRegistry();
        luceneIndexer = new LuceneIndexer(config);
        moduleGraphBuilder = new TestModuleGraphBuilder(registry);
    }

    // --- exit codes ---

    @Test
    void unknownProject_returnsExitCode1() {
        assertThat(run("no-such-project")).isEqualTo(1);
    }

    @Test
    void knownProject_returnsExitCode0() {
        registerProject("my-project");

        assertThat(run("my-project")).isEqualTo(0);
    }

    // --- registry cleanup ---

    @Test
    void knownProject_isRemovedFromRegistry() {
        registerProject("my-project");

        run("my-project");

        assertThat(registry.find("my-project")).isEmpty();
    }

    @Test
    void otherProjects_remainInRegistry() {
        registerProject("proj-a");
        registerProject("proj-b");

        run("proj-a");

        assertThat(registry.find("proj-b")).isPresent();
    }

    @Test
    void removedProject_isUnlinkedFromOtherProjects() {
        registerProject("proj-a");
        registerProject("proj-b");
        registry.linkProjects("proj-a", "proj-b");

        run("proj-b");

        assertThat(registry.find("proj-a").orElseThrow().getLinkedProjects())
                .doesNotContain("proj-b");
    }

    @Test
    void removedProject_isUnlinkedFromMultipleProjects() {
        registerProject("core");
        registerProject("lib-1");
        registerProject("lib-2");
        registry.linkProjects("core", "lib-1");
        registry.linkProjects("core", "lib-2");

        run("core");

        assertThat(registry.find("lib-1").orElseThrow().getLinkedProjects()).doesNotContain("core");
        assertThat(registry.find("lib-2").orElseThrow().getLinkedProjects()).doesNotContain("core");
    }

    // --- filesystem cleanup ---

    @Test
    void projectIndexDirectory_isDeleted() throws IOException {
        registerProject("my-project");
        Path projectDir = createProjectDir("my-project");

        run("my-project");

        assertThat(projectDir).doesNotExist();
    }

    @Test
    void projectIndexDirectory_andContents_areDeleted() throws IOException {
        registerProject("my-project");
        Path projectDir = createProjectDir("my-project");
        Files.writeString(projectDir.resolve("graph.graphml"), "<graphml/>");
        Files.writeString(projectDir.resolve("file-state.json"), "{}");
        Files.writeString(projectDir.resolve("lucene/segments_1"), "data");

        run("my-project");

        assertThat(projectDir).doesNotExist();
    }

    @Test
    void noProjectDirectory_doesNotThrowAndSucceeds() {
        registerProject("my-project");
        // No directory created — removal must still succeed cleanly

        assertThat(run("my-project")).isEqualTo(0);
        assertThat(registry.find("my-project")).isEmpty();
    }

    // --- module graph cleanup ---

    @Test
    void indexedModuleGraphNode_isDowngradedToExternal() {
        registerProject("my-project");
        moduleGraphBuilder.addIndexedNode("com.example", "my-lib", "1.0", "my-project");

        run("my-project");

        ModuleNode node = moduleGraphBuilder.graph.findByKey("com.example:my-lib");
        assertThat(node).isNotNull();
        assertThat(node.isIndexed()).isFalse();
        assertThat(node.getProjectName()).isNull();
    }

    @Test
    void noModuleGraphNode_doesNotThrow() {
        registerProject("my-project");
        // No module node — must silently succeed

        assertThat(run("my-project")).isEqualTo(0);
    }

    @Test
    void moduleGraphIsSaved_afterNodeDowngrade() {
        registerProject("my-project");
        moduleGraphBuilder.addIndexedNode("com.example", "my-lib", "1.0", "my-project");

        run("my-project");

        assertThat(moduleGraphBuilder.saved).isTrue();
    }

    @Test
    void otherProjectModuleNodes_areUntouched() {
        registerProject("proj-a");
        registerProject("proj-b");
        moduleGraphBuilder.addIndexedNode("com.example", "lib-a", "1.0", "proj-a");
        moduleGraphBuilder.addIndexedNode("com.example", "lib-b", "1.0", "proj-b");

        run("proj-a");

        ModuleNode libB = moduleGraphBuilder.graph.findByKey("com.example:lib-b");
        assertThat(libB.isIndexed()).isTrue();
        assertThat(libB.getProjectName()).isEqualTo("proj-b");
    }

    // --- helpers ---

    private int run(String projectName) {
        RemoveIndexCommand cmd = new RemoveIndexCommand(registry, luceneIndexer, moduleGraphBuilder);
        return new CommandLine(cmd).execute(projectName);
    }

    private void registerProject(String name) {
        registry.register(new ProjectMeta(
                name,
                tempDir.resolve(name).toString(),
                config.getIndexDir().resolve(name).toString()));
    }

    private Path createProjectDir(String name) throws IOException {
        Path dir = config.getIndexDir().resolve(name);
        Files.createDirectories(dir.resolve("lucene"));
        return dir;
    }

    // --- in-memory registry (avoids ~/.pharos) ---

    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() { super(IndexConfig.defaults()); }

        @Override public synchronized void register(ProjectMeta meta) { store.put(meta.getName(), meta); }
        @Override public synchronized Optional<ProjectMeta> find(String name) { return Optional.ofNullable(store.get(name)); }
        @Override public synchronized List<ProjectMeta> listAll() { return new ArrayList<>(store.values()); }
        @Override public synchronized void unregister(String name) { store.remove(name); }
        @Override public synchronized void link(String p1, String p2) {}

        @Override
        public synchronized void unlinkAll(String name) {
            for (ProjectMeta meta : store.values()) {
                meta.getLinkedProjects().remove(name);
            }
        }

        void linkProjects(String p1, String p2) {
            store.get(p1).getLinkedProjects().add(p2);
            store.get(p2).getLinkedProjects().add(p1);
        }
    }

    // --- in-memory module graph builder (avoids ~/.pharos) ---

    static class TestModuleGraphBuilder extends ModuleGraphBuilder {
        final ModuleGraph graph = new ModuleGraph();
        boolean saved = false;

        TestModuleGraphBuilder(ProjectRegistry registry) { super(registry); }

        @Override public ModuleGraph load() { return graph; }

        @Override
        public void save(ModuleGraph g) { saved = true; }

        void addIndexedNode(String groupId, String artifactId, String version, String projectName) {
            graph.addOrUpdate(ModuleNode.indexed(groupId, artifactId, version, projectName));
        }
    }
}
