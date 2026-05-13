package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.parser.CodeParser;
import com.pharos.parser.GenericFileParser;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedProject;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.apache.lucene.index.IndexReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests that parallel parsing and parallel indexing produce correct, consistent
 * results — same document set as the sequential path, no duplicates, no races.
 */
class ParallelIndexingTest {

    @TempDir
    Path tempDir;

    private Path projectRoot;
    private Path sourceDir;

    @BeforeEach
    void setUp() throws IOException {
        projectRoot = tempDir.resolve("project");
        sourceDir = projectRoot.resolve("src/main/java/com/example");
        Files.createDirectories(sourceDir);
    }

    // -------------------------------------------------------------------------
    // Parallel parsing — JavaCodeParser
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "parseThreads={0}")
    @ValueSource(ints = {1, 2, 4})
    void parallelParse_producesAllFiles(int threads) throws Exception {
        int fileCount = 20;
        Set<String> expectedMethods = new HashSet<>();
        for (int i = 0; i < fileCount; i++) {
            String method = "method" + i;
            expectedMethods.add(method);
            writeJava("Class" + i + ".java",
                    "package com.example;\npublic class Class" + i + " {\n" +
                    "  public void " + method + "() {}\n}");
        }

        JavaCodeParser parser = new JavaCodeParser(List.of(), List.of(), threads);
        ParsedProject project = parser.parseProject(projectRoot, "test");

        assertThat(project.files()).hasSize(fileCount);

        Set<String> parsedMethods = project.allMethods().stream()
                .map(m -> m.methodName())
                .collect(Collectors.toSet());
        assertThat(parsedMethods).containsAll(expectedMethods);
    }

    @Test
    void parallelParse_noDuplicateFiles() throws Exception {
        for (int i = 0; i < 15; i++) {
            writeJava("Dup" + i + ".java",
                    "package com.example;\npublic class Dup" + i + " {\n" +
                    "  public void run" + i + "() {}\n}");
        }

        JavaCodeParser parser = new JavaCodeParser(List.of(), List.of(), 4);
        ParsedProject project = parser.parseProject(projectRoot, "test");

        // Each file path must appear exactly once
        List<String> filePaths = project.files().stream()
                .map(ParsedFile::filePath)
                .collect(Collectors.toList());
        Set<String> unique = new HashSet<>(filePaths);
        assertThat(filePaths).hasSize(unique.size());
    }

    @Test
    void parallelParse_resultsMatchSequential() throws Exception {
        for (int i = 0; i < 12; i++) {
            writeJava("Seq" + i + ".java",
                    "package com.example;\npublic class Seq" + i + " {\n" +
                    "  public int compute" + i + "(int x) { return x * " + i + "; }\n}");
        }

        JavaCodeParser sequential = new JavaCodeParser(List.of(), List.of(), 1);
        JavaCodeParser parallel = new JavaCodeParser(List.of(), List.of(), 4);

        ParsedProject seqResult = sequential.parseProject(projectRoot, "test");
        ParsedProject parResult = parallel.parseProject(projectRoot, "test");

        assertThat(parResult.files()).hasSize(seqResult.files().size());

        Set<String> seqMethods = seqResult.allMethods().stream()
                .map(m -> m.methodName()).collect(Collectors.toSet());
        Set<String> parMethods = parResult.allMethods().stream()
                .map(m -> m.methodName()).collect(Collectors.toSet());
        assertThat(parMethods).isEqualTo(seqMethods);

        Set<String> seqClasses = seqResult.allClasses().stream()
                .map(c -> c.qualifiedClassName()).collect(Collectors.toSet());
        Set<String> parClasses = parResult.allClasses().stream()
                .map(c -> c.qualifiedClassName()).collect(Collectors.toSet());
        assertThat(parClasses).isEqualTo(seqClasses);
    }

    // -------------------------------------------------------------------------
    // Parallel parsing — GenericFileParser
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "parseThreads={0}")
    @ValueSource(ints = {1, 2, 4})
    void parallelGenericParse_producesAllChunks(int threads) throws Exception {
        Path docDir = projectRoot.resolve("docs");
        Files.createDirectories(docDir);
        for (int i = 0; i < 10; i++) {
            Files.writeString(docDir.resolve("doc" + i + ".md"),
                    "# Section " + i + "\n\nContent for document " + i + ".\n");
        }

        GenericFileParser parser = new GenericFileParser(threads);
        ParsedProject project = parser.parseProject(projectRoot, "test");

        assertThat(project.files()).hasSize(10);
    }

    @Test
    void parallelGenericParse_resultsMatchSequential() throws Exception {
        Path docDir = projectRoot.resolve("docs");
        Files.createDirectories(docDir);
        for (int i = 0; i < 10; i++) {
            Files.writeString(docDir.resolve("page" + i + ".md"),
                    "# Topic " + i + "\n\nDetails about topic " + i + ".\n\n## Subsection\n\nMore text.\n");
        }

        GenericFileParser sequential = new GenericFileParser(1);
        GenericFileParser parallel = new GenericFileParser(4);

        ParsedProject seqResult = sequential.parseProject(projectRoot, "test");
        ParsedProject parResult = parallel.parseProject(projectRoot, "test");

        assertThat(parResult.files()).hasSize(seqResult.files().size());
        // Chunk count must also match
        assertThat(parResult.allMethods()).hasSize(seqResult.allMethods().size());
    }

    // -------------------------------------------------------------------------
    // Parallel indexing — ProjectIndexManager
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "indexThreads={0}")
    @ValueSource(ints = {1, 2, 4})
    void parallelIndex_allMethodsSearchable(int indexThreads) throws Exception {
        int fileCount = 10;
        Set<String> expectedMethods = new HashSet<>();
        for (int i = 0; i < fileCount; i++) {
            String method = "uniqueMethod" + i;
            expectedMethods.add(method);
            writeJava("Worker" + i + ".java",
                    "package com.example;\npublic class Worker" + i + " {\n" +
                    "  /** Unique javadoc for " + method + " */ public void " + method + "() {}\n}");
        }

        IndexConfig config = makeConfig(indexThreads);
        ProjectIndexManager mgr = makeIndexManager(config);
        mgr.index(projectRoot, "test", false, false);

        KeywordSearchStrategy strategy = new KeywordSearchStrategy();
        try (IndexReader reader = makeLuceneIndexer(config).openMultiReader(List.of("test"))) {
            for (String method : expectedMethods) {
                SearchRequest req = new SearchRequest(method, SearchRequest.SearchType.KEYWORD,
                        "test", null, 10, "text", "method", null);
                List<SearchResult> results = strategy.search(reader, req);
                assertThat(results)
                        .as("Method '%s' not found with indexThreads=%d", method, indexThreads)
                        .anyMatch(r -> method.equals(r.methodName()));
            }
        }
    }

    @Test
    void parallelIndex_documentCountMatchesSequential() throws Exception {
        for (int i = 0; i < 15; i++) {
            writeJava("Entity" + i + ".java",
                    "package com.example;\npublic class Entity" + i + " {\n" +
                    "  public void doWork" + i + "() {}\n" +
                    "  public String getName" + i + "() { return \"entity" + i + "\"; }\n}");
        }

        // Sequential index
        IndexConfig seqConfig = makeConfig(1);
        seqConfig.setIndexDir(tempDir.resolve("seq-index"));
        ProjectIndexManager seqMgr = makeIndexManager(seqConfig);
        ProjectMeta seqMeta = seqMgr.index(projectRoot, "test", false, false);

        // Parallel index
        IndexConfig parConfig = makeConfig(4);
        parConfig.setIndexDir(tempDir.resolve("par-index"));
        ProjectIndexManager parMgr = makeIndexManager(parConfig);
        ProjectMeta parMeta = parMgr.index(projectRoot, "test", false, false);

        assertThat(parMeta.getMethodCount()).isEqualTo(seqMeta.getMethodCount());
        assertThat(parMeta.getClassCount()).isEqualTo(seqMeta.getClassCount());
        assertThat(parMeta.getFileCount()).isEqualTo(seqMeta.getFileCount());
    }

    @Test
    void parallelIndex_noDuplicateDocuments() throws Exception {
        for (int i = 0; i < 10; i++) {
            writeJava("Unique" + i + ".java",
                    "package com.example;\npublic class Unique" + i + " {\n" +
                    "  public void onlyOnce" + i + "() {}\n}");
        }

        IndexConfig config = makeConfig(4);
        ProjectIndexManager mgr = makeIndexManager(config);
        mgr.index(projectRoot, "test", false, false);

        // Each method should appear exactly once in search results
        KeywordSearchStrategy strategy = new KeywordSearchStrategy();
        LuceneIndexer luceneIndexer = makeLuceneIndexer(config);
        try (IndexReader reader = luceneIndexer.openMultiReader(List.of("test"))) {
            for (int i = 0; i < 10; i++) {
                String method = "onlyOnce" + i;
                SearchRequest req = new SearchRequest(method, SearchRequest.SearchType.KEYWORD,
                        "test", null, 50, "text", "method", null);
                List<SearchResult> results = strategy.search(reader, req);
                long matchCount = results.stream()
                        .filter(r -> method.equals(r.methodName()))
                        .count();
                assertThat(matchCount)
                        .as("Method '%s' appears %d times (expected exactly 1)", method, matchCount)
                        .isEqualTo(1);
            }
        }
    }

    @Test
    void parallelIndex_fileStateTrackerConsistent() throws Exception {
        for (int i = 0; i < 10; i++) {
            writeJava("Tracked" + i + ".java",
                    "package com.example;\npublic class Tracked" + i + " {\n" +
                    "  public void track" + i + "() {}\n}");
        }

        IndexConfig config = makeConfig(4);
        ProjectIndexManager mgr = makeIndexManager(config);
        mgr.index(projectRoot, "test", false, false);

        // Verify the incremental path still works correctly after parallel full index
        // (state tracker must have captured all 10 files)
        ProjectMeta incrementalMeta = mgr.index(projectRoot, "test", true, false);
        assertThat(incrementalMeta.getMethodCount()).isEqualTo(10);
    }

    // -------------------------------------------------------------------------
    // Stress test — many files, many threads
    // -------------------------------------------------------------------------

    @Test
    void parallelParse_stressTest_50FilesX4Threads() throws Exception {
        for (int i = 0; i < 50; i++) {
            writeJava("Stress" + i + ".java",
                    "package com.example;\npublic class Stress" + i + " {\n" +
                    "  public void alpha" + i + "() {}\n" +
                    "  public void beta" + i + "(int x) { alpha" + i + "(); }\n}");
        }

        JavaCodeParser parser = new JavaCodeParser(List.of(), List.of(), 4);
        ParsedProject project = parser.parseProject(projectRoot, "stress");

        assertThat(project.files()).hasSize(50);
        // 50 files × 2 methods each
        assertThat(project.allMethods()).hasSize(100);
        assertThat(project.allClasses()).hasSize(50);
        // No duplicate file paths
        long distinctFiles = project.files().stream()
                .map(ParsedFile::filePath).distinct().count();
        assertThat(distinctFiles).isEqualTo(50);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void writeJava(String filename, String content) throws IOException {
        Files.writeString(sourceDir.resolve(filename), content);
    }

    private IndexConfig makeConfig(int indexThreads) {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("index-" + indexThreads + "-" + System.nanoTime()));
        config.setIndexThreads(indexThreads);
        return config;
    }

    private LuceneIndexer makeLuceneIndexer(IndexConfig config) {
        return new LuceneIndexer(config);
    }

    private ProjectIndexManager makeIndexManager(IndexConfig config) {
        LuceneIndexer luceneIndexer = makeLuceneIndexer(config);
        TestRegistry registry = new TestRegistry();
        ModuleGraphBuilder noopModuleBuilder = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        int parseThreads = 2;
        List<CodeParser> parsers = List.of(
                new JavaCodeParser(List.of(), List.of(), parseThreads),
                new GenericFileParser(parseThreads));
        return new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopModuleBuilder, parsers);
    }

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
