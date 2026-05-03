package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.apache.lucene.index.DirectoryReader;
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

/**
 * Tests the incremental indexing pipeline — especially deleted-file cleanup.
 * Uses real filesystem and real Lucene NIOFSDirectory inside a temp dir.
 */
class IncrementalIndexingTest {

    @TempDir
    Path tempDir;

    private Path projectRoot;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private ProjectIndexManager indexManager;
    private KeywordSearchStrategy strategy;

    @BeforeEach
    void setUp() throws IOException {
        projectRoot = tempDir.resolve("project");
        Files.createDirectories(projectRoot.resolve("src/main/java/com/example"));

        // Point index storage to a subdir of tempDir (not ~/.pharos)
        config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));

        TestRegistry registry = new TestRegistry();
        luceneIndexer = new LuceneIndexer(config);

        // ModuleGraphBuilder writes to ~/.pharos/module-graph.graphml; use a no-op wrapper
        ModuleGraphBuilder noopModuleBuilder = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of(); // skip actual graph I/O in these tests
            }
        };

        indexManager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopModuleBuilder);
        strategy = new KeywordSearchStrategy();
    }

    // --- incremental: changed file is re-indexed ---

    @Test
    void incrementalIndex_changedFile_updatesLuceneDocument() throws Exception {
        Path source = writeJava("Greeter.java",
                "package com.example;\npublic class Greeter {\n" +
                "  /** Says hello */ public String hello() { return \"hello\"; }\n}");

        // Full index
        indexManager.index(projectRoot, "test-project", false, false);

        // Verify initial content is indexed
        assertThat(searchMethods("hello")).anyMatch(r -> "hello".equals(r.methodName()));

        // Modify file
        Thread.sleep(50);
        Files.writeString(source,
                "package com.example;\npublic class Greeter {\n" +
                "  /** Says hi and hello */ public String hello() { return \"hi there\"; }\n" +
                "  /** Says goodbye */ public String goodbye() { return \"bye\"; }\n}");

        // Incremental re-index
        indexManager.index(projectRoot, "test-project", true, false);

        assertThat(searchMethods("goodbye")).anyMatch(r -> "goodbye".equals(r.methodName()));
    }

    // --- incremental: deleted file removes Lucene documents ---

    @Test
    void incrementalIndex_deletedFile_removesDocumentsFromIndex() throws Exception {
        writeJava("Alpha.java",
                "package com.example;\npublic class Alpha {\n" +
                "  public void alphaMethod() {}\n}");
        writeJava("Beta.java",
                "package com.example;\npublic class Beta {\n" +
                "  public void betaMethod() {}\n}");

        indexManager.index(projectRoot, "test-project", false, false);

        // Both methods should be findable
        assertThat(searchMethods("alphaMethod")).anyMatch(r -> "alphaMethod".equals(r.methodName()));
        assertThat(searchMethods("betaMethod")).anyMatch(r -> "betaMethod".equals(r.methodName()));

        // Delete Alpha.java
        Files.delete(projectRoot.resolve("src/main/java/com/example/Alpha.java"));

        // Incremental re-index
        indexManager.index(projectRoot, "test-project", true, false);

        // alphaMethod docs must be gone; betaMethod still present
        assertThat(searchMethods("alphaMethod")).noneMatch(r -> "alphaMethod".equals(r.methodName()));
        assertThat(searchMethods("betaMethod")).anyMatch(r -> "betaMethod".equals(r.methodName()));
    }

    @Test
    void incrementalIndex_deletedFile_removesFromFileStateTracker() throws Exception {
        Path source = writeJava("Temp.java",
                "package com.example;\npublic class Temp {\n" +
                "  public void tempMethod() {}\n}");

        indexManager.index(projectRoot, "test-project", false, false);

        Path stateFile = config.getIndexDir().resolve("test-project").resolve("file-state.json");
        assertThat(stateFile).exists();
        String stateContent = Files.readString(stateFile);
        assertThat(stateContent).contains("Temp.java");

        // Delete source file and run incremental index
        Files.delete(source);
        indexManager.index(projectRoot, "test-project", true, false);

        // The deleted file should no longer be tracked
        String updatedState = Files.readString(stateFile);
        assertThat(updatedState).doesNotContain("Temp.java");
    }

    // --- incremental: no-change short-circuit ---

    @Test
    void incrementalIndex_noChanges_returnsCurrentMeta() throws Exception {
        writeJava("Stable.java",
                "package com.example;\npublic class Stable {\n" +
                "  public void run() {}\n}");

        indexManager.index(projectRoot, "test-project", false, false);

        // Immediately do incremental — nothing changed
        ProjectMeta meta = indexManager.index(projectRoot, "test-project", true, false);

        assertThat(meta).isNotNull();
        assertThat(meta.getName()).isEqualTo("test-project");
    }

    // --- helpers ---

    private Path writeJava(String filename, String content) throws IOException {
        Path file = projectRoot.resolve("src/main/java/com/example").resolve(filename);
        return Files.writeString(file, content);
    }

    private List<SearchResult> searchMethods(String query) throws IOException {
        List<String> projects = List.of("test-project");
        var reader = luceneIndexer.openMultiReader(projects);
        SearchRequest req = new SearchRequest(query, SearchRequest.SearchType.KEYWORD,
                "test-project", null, 20, "text", "method");
        return strategy.search(reader, req);
    }

    // Minimal in-memory registry that avoids writing to ~/.pharos/
    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() { super(IndexConfig.defaults()); }

        @Override public synchronized void register(ProjectMeta meta) { store.put(meta.getName(), meta); }
        @Override public synchronized Optional<ProjectMeta> find(String name) { return Optional.ofNullable(store.get(name)); }
        @Override public synchronized List<ProjectMeta> listAll() { return new ArrayList<>(store.values()); }
        @Override public synchronized void link(String p1, String p2) {}
        @Override public synchronized void unregister(String name) { store.remove(name); }
    }
}
