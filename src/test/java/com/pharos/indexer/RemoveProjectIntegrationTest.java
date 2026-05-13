package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Full-pipeline integration test for {@link ProjectIndexManager#removeProject}.
 *
 * Indexes a real project into a temp dir, then removes it and asserts that
 * every artifact — Lucene index, call-graph directory, file-state, and
 * registry entry — has been cleaned up.
 */
class RemoveProjectIntegrationTest {

    @TempDir
    Path tempDir;

    private static final String PROJECT = "sample-project";

    private Path projectRoot;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private TestRegistry registry;
    private TestModuleGraphBuilder moduleGraphBuilder;
    private ProjectIndexManager indexManager;

    @BeforeEach
    void setUp() throws IOException {
        projectRoot = tempDir.resolve("src");
        Files.createDirectories(projectRoot.resolve("src/main/java/com/example"));
        writeJava("Calculator.java",
                "package com.example;\n" +
                "public class Calculator {\n" +
                "  /** Adds two numbers */ public int add(int a, int b) { return a + b; }\n" +
                "  public int subtract(int a, int b) { return a - b; }\n" +
                "}");
        writeJava("Greeter.java",
                "package com.example;\n" +
                "public class Greeter {\n" +
                "  public String hello(String name) { return \"Hello \" + name; }\n" +
                "}");

        config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));

        registry = new TestRegistry();
        luceneIndexer = new LuceneIndexer(config);
        moduleGraphBuilder = new TestModuleGraphBuilder(registry, tempDir);
        indexManager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), moduleGraphBuilder);
    }

    @Test
    void afterRemove_luceneIndexDirectoryIsDeleted() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false);
        Path luceneDir = luceneIndexer.getLucenePath(PROJECT);
        assertThat(luceneDir).exists();

        indexManager.removeProject(PROJECT);

        assertThat(luceneDir).doesNotExist();
    }

    @Test
    void afterRemove_fullProjectIndexDirectoryIsDeleted() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false);
        Path projectIndexDir = luceneIndexer.getProjectIndexDir(PROJECT);
        assertThat(projectIndexDir).exists();

        indexManager.removeProject(PROJECT);

        assertThat(projectIndexDir).doesNotExist();
    }

    @Test
    void afterRemove_projectIsUnregistered() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false);
        assertThat(registry.find(PROJECT)).isPresent();

        indexManager.removeProject(PROJECT);

        assertThat(registry.find(PROJECT)).isEmpty();
    }

    @Test
    void afterRemove_linkedProjectReferencesAreCleaned() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false);
        registry.register(new ProjectMeta("other", "/other", "/other/index"));
        registry.find("other").orElseThrow().getLinkedProjects().add(PROJECT);

        indexManager.removeProject(PROJECT);

        assertThat(registry.find("other").orElseThrow().getLinkedProjects())
                .doesNotContain(PROJECT);
    }

    @Test
    void afterRemove_moduleGraphNodeIsDowngradedToExternal() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false);
        moduleGraphBuilder.addIndexedNode("com.example", "sample", "1.0", PROJECT);

        indexManager.removeProject(PROJECT);

        com.pharos.graph.ModuleNodeData node = moduleGraphBuilder.findByKey("com.example:sample");
        assertThat(node).isNotNull();
        assertThat(node.isIndexed()).isFalse();
    }

    @Test
    void afterRemove_luceneReaderCacheIsEvicted() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false);
        // Open a reader to populate the cached reader inside LuceneIndexer
        luceneIndexer.openMultiReader(List.of(PROJECT));

        indexManager.removeProject(PROJECT);

        // The cached reader must have been closed and the index directory gone
        assertThat(luceneIndexer.indexExists(PROJECT)).isFalse();
    }

    @Test
    void forceReindex_wipesAllArtifactsBeforeReindexing() throws Exception {
        // First full index
        indexManager.index(projectRoot, PROJECT, false, false);
        Path projectIndexDir = luceneIndexer.getProjectIndexDir(PROJECT);
        // Plant a stale sentinel file that should be gone after force re-index
        Path staleFile = projectIndexDir.resolve("stale-callgraph.bin");
        Files.writeString(staleFile, "old data");

        // Force re-index (wipes everything, then indexes fresh)
        indexManager.index(projectRoot, PROJECT, false, true, false, ProgressListener.SILENT);

        // Stale artifact from the old index must be gone
        assertThat(staleFile).doesNotExist();
        // But the project must be indexed and searchable again
        assertThat(luceneIndexer.indexExists(PROJECT)).isTrue();
        assertThat(registry.find(PROJECT)).isPresent();
    }

    @Test
    void removeProject_onNonExistentProject_doesNotThrow() {
        assertThatCode(() -> indexManager.removeProject("no-such-project"))
                .doesNotThrowAnyException();
    }

    // --- helpers ---

    private void writeJava(String filename, String content) throws IOException {
        Path file = projectRoot.resolve("src/main/java/com/example").resolve(filename);
        Files.writeString(file, content);
    }

    // --- in-memory registry ---

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
    }

    // --- module graph builder backed by temp dir ---

    static class TestModuleGraphBuilder extends ModuleGraphBuilder {
        private final Path dbPath;

        TestModuleGraphBuilder(ProjectRegistry registry, Path tempDir) {
            super(registry);
            this.dbPath = tempDir.resolve("test-module.arcadedb");
        }

        @Override
        public ModuleGraph open() {
            return ModuleGraph.open(dbPath);
        }

        @Override
        public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
            return List.of();
        }

        void addIndexedNode(String groupId, String artifactId, String version, String projectName) {
            String key = groupId + ":" + artifactId;
            try (ModuleGraph g = ModuleGraph.open(dbPath)) {
                g.addOrUpdate(key, groupId, artifactId, version, "INDEXED", projectName);
            }
        }

        com.pharos.graph.ModuleNodeData findByKey(String key) {
            try (ModuleGraph g = ModuleGraph.open(dbPath)) {
                return g.findByKey(key).orElse(null);
            }
        }
    }
}
