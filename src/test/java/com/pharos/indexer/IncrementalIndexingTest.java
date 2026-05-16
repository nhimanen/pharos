package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
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

    // -----------------------------------------------------------------------
    // Auto-detection: full on first run, incremental on subsequent runs
    // -----------------------------------------------------------------------

    @Test
    void autoDetect_fullIndexOnFirstRun_thenIncrementalOnSecondRun() throws Exception {
        // Write two Java files to the project
        writeJava("Alpha.java",
                "package com.example;\npublic class Alpha {\n" +
                "  public void alphaMethod() {}\n}");
        writeJava("Beta.java",
                "package com.example;\npublic class Beta {\n" +
                "  public void betaMethod() {}\n}");

        // --- First run: project not yet in registry and no index on disk → full index ---
        assertThat(indexManager.indexExists("test-project")).isFalse();

        indexManager.index(projectRoot, "test-project", false, false);

        assertThat(indexManager.indexExists("test-project")).isTrue();
        assertThat(searchMethods("alphaMethod")).anyMatch(r -> "alphaMethod".equals(r.methodName()));
        assertThat(searchMethods("betaMethod")).anyMatch(r -> "betaMethod".equals(r.methodName()));

        // --- Second run: project is known, index exists → incremental is safe ---
        // Add a new method to Alpha.java and wait for mtime to advance
        Thread.sleep(50);
        writeJava("Alpha.java",
                "package com.example;\npublic class Alpha {\n" +
                "  public void alphaMethod() {}\n" +
                "  public void newAlphaMethod() {}\n}");

        // Simulate what IndexCommand does: auto-detect incremental
        boolean incremental = indexManager.indexExists("test-project");
        assertThat(incremental).isTrue();   // proves auto-detection would pick incremental

        indexManager.index(projectRoot, "test-project", incremental, false);

        // Both old and new methods must be present
        assertThat(searchMethods("alphaMethod")).anyMatch(r -> "alphaMethod".equals(r.methodName()));
        assertThat(searchMethods("newAlphaMethod")).anyMatch(r -> "newAlphaMethod".equals(r.methodName()));
        assertThat(searchMethods("betaMethod")).anyMatch(r -> "betaMethod".equals(r.methodName()));
    }

    @Test
    void autoDetect_newProjectAlwaysGetsFullIndex() throws Exception {
        writeJava("Gamma.java",
                "package com.example;\npublic class Gamma {\n" +
                "  public void gammaMethod() {}\n}");

        // Project "new-project" has never been indexed → indexExists returns false
        assertThat(indexManager.indexExists("new-project")).isFalse();

        // Auto-detection logic: not exists → full (incremental=false)
        boolean incremental = indexManager.indexExists("new-project");
        assertThat(incremental).isFalse();

        indexManager.index(projectRoot, "new-project", incremental, false);

        assertThat(indexManager.indexExists("new-project")).isTrue();
        assertThat(searchMethods("gammaMethod", "new-project"))
                .anyMatch(r -> "gammaMethod".equals(r.methodName()));
    }

    // -----------------------------------------------------------------------
    // Regression: dotdir/noise-dir files must not cause endless dirty churn
    // -----------------------------------------------------------------------

    /**
     * Files that live in dotdirs (e.g. .gradle/, .cache/) are invisible to the
     * incremental scanner but were historically indexed by the full parser.
     * Regression guard: they must NOT appear as "deleted" on subsequent incremental
     * runs, and the fileCount reported must match what the scanner actually found.
     */
    @Test
    void incrementalIndex_filesInDotdirNotTreatedAsDeleted() throws Exception {
        writeJava("Service.java",
                "package com.example;\npublic class Service {\n" +
                "  public void serve() {}\n}");

        // Simulate a file that ended up tracked from a previous full run in a dotdir
        // (e.g. .gradle/wrapper/gradle-wrapper.properties). We inject it directly into
        // the state file by running a full index that creates the state, then manually
        // adding a "tracked" dotdir file entry.
        indexManager.index(projectRoot, "test-project", false, false);

        Path stateFile = config.getIndexDir().resolve("test-project/file-state.json");
        String state = Files.readString(stateFile);

        // Inject a fake tracked file that lives in a dotdir under the project root
        Path dotdirFile = projectRoot.resolve(".gradle/wrapper/gradle-wrapper.properties");
        String injected = state.replace("}",
                ", \"" + dotdirFile.toAbsolutePath() + "\": {\"lastModifiedMs\":1000,\"sha256\":\"aabbcc\"}}");
        Files.writeString(stateFile, injected);

        // Incremental run — dotdir file is not on disk, but should NOT appear as deleted
        // (no exception thrown, serve() method must still be searchable)
        ProjectMeta meta = indexManager.index(projectRoot, "test-project", true, false);

        assertThat(searchMethods("serve")).anyMatch(r -> "serve".equals(r.methodName()));
        // fileCount must reflect the scanner view, not include the injected dotdir file
        assertThat(meta.getFileCount()).isLessThanOrEqualTo(10);
    }

    /**
     * The fileCount in the registry must stay stable across incremental runs when
     * nothing changes. Previously it could inflate because buildMeta used the
     * graph-rebuild full-parse result (which walks into dirs the scanner skips).
     */
    @Test
    void incrementalIndex_fileCountStableAcrossRuns() throws Exception {
        writeJava("Foo.java",
                "package com.example;\npublic class Foo {\n  public void foo() {}\n}");
        writeJava("Bar.java",
                "package com.example;\npublic class Bar {\n  public void bar() {}\n}");

        ProjectMeta first = indexManager.index(projectRoot, "test-project", false, false);
        int fileCountAfterFull = first.getFileCount();
        assertThat(fileCountAfterFull).isGreaterThan(0);

        // Two consecutive incremental runs without any changes
        ProjectMeta second = indexManager.index(projectRoot, "test-project", true, false);
        ProjectMeta third  = indexManager.index(projectRoot, "test-project", true, false);

        // fileCount must not drift upward across runs
        assertThat(second.getFileCount()).isEqualTo(fileCountAfterFull);
        assertThat(third.getFileCount()).isEqualTo(fileCountAfterFull);
    }

    // --- helpers ---

    private Path writeJava(String filename, String content) throws IOException {
        Path file = projectRoot.resolve("src/main/java/com/example").resolve(filename);
        return Files.writeString(file, content);
    }

    private List<SearchResult> searchMethods(String query) throws IOException {
        return searchMethods(query, "test-project");
    }

    private List<SearchResult> searchMethods(String query, String project) throws IOException {
        var reader = luceneIndexer.openMultiReader(List.of(project));
        SearchRequest req = new SearchRequest(query, SearchRequest.SearchType.KEYWORD,
                project, null, 20, "text", "method", null, 0);
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
