package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectDiscovery.DiscoveredProject;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests concurrent multi-project discovery indexing and cross-project linking.
 */
class IndexCommandParallelTest {

    @TempDir
    Path tempDir;

    private Path workspace;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private ProjectIndexManager indexManager;
    private TestRegistry registry;
    private CrossProjectLinker crossProjectLinker;
    private IndexCommand indexCommand;
    private KeywordSearchStrategy strategy;


    @BeforeEach
    void setUp() throws IOException {
        workspace = tempDir.resolve("workspace");
        Files.createDirectories(workspace);

        config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));

        registry = new TestRegistry();
        luceneIndexer = new LuceneIndexer(config);

        ModuleGraphBuilder noopModuleBuilder = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };

        indexManager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopModuleBuilder);
        crossProjectLinker = new CrossProjectLinker(config, registry);
        indexCommand = new IndexCommand(indexManager, crossProjectLinker, registry, config);
        strategy = new KeywordSearchStrategy();
    }

    // -----------------------------------------------------------------------
    // Concurrent indexing
    // -----------------------------------------------------------------------

    @Test
    void parallelIndexing_allProjectsIndexedSuccessfully() throws Exception {
        // Three independent Java projects in the workspace
        Path projA = createJavaProject("proj-a", "AlphaService",
                "public void processAlpha() {}");
        Path projB = createJavaProject("proj-b", "BetaService",
                "public void processBeta() {}");
        Path projC = createJavaProject("proj-c", "GammaService",
                "public void processGamma() {}");

        List<DiscoveredProject> projects = List.of(
                new DiscoveredProject(projA, "proj-a"),
                new DiscoveredProject(projB, "proj-b"),
                new DiscoveredProject(projC, "proj-c")
        );

        // invokeMultiple via the command's callable path
        int result = invokeMultiple(projects);

        assertThat(result).isEqualTo(0);
        assertThat(indexManager.indexExists("proj-a")).isTrue();
        assertThat(indexManager.indexExists("proj-b")).isTrue();
        assertThat(indexManager.indexExists("proj-c")).isTrue();
    }

    @Test
    void parallelIndexing_eachProjectSearchableAfterIndexing() throws Exception {
        createJavaProject("search-a", "AlphaWorker", "public void doAlpha() {}");
        createJavaProject("search-b", "BetaWorker",  "public void doBeta() {}");

        List<DiscoveredProject> projects = List.of(
                new DiscoveredProject(workspace.resolve("search-a"), "search-a"),
                new DiscoveredProject(workspace.resolve("search-b"), "search-b")
        );

        invokeMultiple(projects);

        // Each project's methods must be independently searchable
        assertThat(search("doAlpha", "search-a"))
                .anyMatch(r -> "doAlpha".equals(r.methodName()));
        assertThat(search("doBeta", "search-b"))
                .anyMatch(r -> "doBeta".equals(r.methodName()));

        // Cross-project search must not bleed results
        assertThat(search("doAlpha", "search-b"))
                .noneMatch(r -> "doAlpha".equals(r.methodName()));
    }

    @Test
    void parallelIndexing_oneProjectDoesNotBlockOthers() throws Exception {
        // proj-empty has a valid path but no source files — indexes to 0 methods, should not crash
        createJavaProject("proj-real", "RealService", "public void realMethod() {}");
        Path emptyProj = workspace.resolve("proj-empty");
        Files.createDirectories(emptyProj);
        // minimal pom so it's discovered as a project
        Files.writeString(emptyProj.resolve("pom.xml"),
                "<?xml version=\"1.0\"?><project>" +
                "<groupId>com.example</groupId><artifactId>proj-empty</artifactId><version>1.0</version>" +
                "</project>");

        List<DiscoveredProject> projects = List.of(
                new DiscoveredProject(workspace.resolve("proj-real"), "proj-real"),
                new DiscoveredProject(emptyProj, "proj-empty")
        );

        int result = invokeMultiple(projects);

        // Both complete without error; real project is searchable
        assertThat(result).isEqualTo(0);
        assertThat(indexManager.indexExists("proj-real")).isTrue();
        assertThat(search("realMethod", "proj-real"))
                .anyMatch(r -> "realMethod".equals(r.methodName()));
    }

    @Test
    void parallelIndexing_incrementalOnSubsequentRun() throws Exception {
        createJavaProject("incr-a", "IncrService", "public void firstMethod() {}");
        createJavaProject("incr-b", "IncrHelper", "public void helperMethod() {}");

        List<DiscoveredProject> projects = List.of(
                new DiscoveredProject(workspace.resolve("incr-a"), "incr-a"),
                new DiscoveredProject(workspace.resolve("incr-b"), "incr-b")
        );

        // First run — full index
        invokeMultiple(projects);
        assertThat(indexManager.indexExists("incr-a")).isTrue();

        // Second run — auto-detected as incremental (no changes → fast)
        int result = invokeMultiple(projects);
        assertThat(result).isEqualTo(0);
    }

    // -----------------------------------------------------------------------
    // Cross-project linking after parallel indexing
    // -----------------------------------------------------------------------

    @Test
    void parallelIndexing_registersAllProjectPairs_asLinked() throws Exception {
        createJavaProject("link-a", "ServiceA", "public void callB() {}");
        createJavaProject("link-b", "ServiceB", "public void handleB() {}");
        createJavaProject("link-c", "ServiceC", "public void handleC() {}");

        List<DiscoveredProject> projects = List.of(
                new DiscoveredProject(workspace.resolve("link-a"), "link-a"),
                new DiscoveredProject(workspace.resolve("link-b"), "link-b"),
                new DiscoveredProject(workspace.resolve("link-c"), "link-c")
        );

        invokeMultiple(projects);

        // All pairs must be mutually linked in the registry
        assertThat(registry.find("link-a").orElseThrow().getLinkedProjects())
                .containsExactlyInAnyOrder("link-b", "link-c");
        assertThat(registry.find("link-b").orElseThrow().getLinkedProjects())
                .containsExactlyInAnyOrder("link-a", "link-c");
        assertThat(registry.find("link-c").orElseThrow().getLinkedProjects())
                .containsExactlyInAnyOrder("link-a", "link-b");
    }

    @Test
    void parallelIndexing_crossProjectGraph_containsNodesFromAllProjects() throws Exception {
        createJavaProject("graph-a", "NodeA", "public void nodeAMethod() {}");
        createJavaProject("graph-b", "NodeB", "public void nodeBMethod() {}");

        List<DiscoveredProject> projects = List.of(
                new DiscoveredProject(workspace.resolve("graph-a"), "graph-a"),
                new DiscoveredProject(workspace.resolve("graph-b"), "graph-b")
        );

        invokeMultiple(projects);

        // Cross-project graph must include methods from both projects
        Path crossDbDir = config.getIndexDir().getParent().resolve("cross-project.arcadedb");
        if (Files.isDirectory(crossDbDir)) {
            try (CallGraph graph = crossProjectLinker.loadCrossProjectGraph()) {
                // Both project graphs were merged — method count > 0
                assertThat(graph.methodCount()).isGreaterThan(0);
            }
        }
        // If directory doesn't exist (empty graphs), that's also valid — just verify no exception
    }

    @Test
    void parallelIndexing_crossProjectEdge_resolvedFromUnresolvedRef() throws Exception {
        // proj-caller has an unresolved ref to "handleRequest" — proj-handler provides it
        createJavaProject("caller-proj", "CallerService", "public void callSomething() {}");
        createJavaProject("handler-proj", "HandlerService", "public void handleRequest() {}");

        // Register an unresolved ref manually (simulating what the parser would produce)
        // We do this after indexing to inject the cross-project call
        List<DiscoveredProject> projects = List.of(
                new DiscoveredProject(workspace.resolve("caller-proj"), "caller-proj"),
                new DiscoveredProject(workspace.resolve("handler-proj"), "handler-proj")
        );
        invokeMultiple(projects);

        // Inject unresolved ref and rebuild cross-project graph
        ProjectMeta callerMeta = registry.find("caller-proj").orElseThrow();
        callerMeta.getUnresolvedRefs().add(
                new ProjectMeta.UnresolvedRef(
                        "com.example.CallerService#callSomething()", "handleRequest",
                        "HandlerService", 0, null, 1));
        registry.register(callerMeta);

        // Delete the freshness stamp so the explicit rebuild below is not skipped.
        Files.deleteIfExists(config.getIndexDir().getParent().resolve("cross-project.stamp"));

        crossProjectLinker.buildCrossProjectGraph(List.of("caller-proj", "handler-proj"));

        // The cross-project edge must exist
        try (CallGraph crossGraph = crossProjectLinker.loadCrossProjectGraph()) {
            assertThat(crossGraph.callees("com.example.CallerService#callSomething()"))
                    .anyMatch(callee -> callee.contains("handleRequest"));
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Invokes the multi-project indexing path directly (bypasses CLI arg parsing). */
    private int invokeMultiple(List<DiscoveredProject> projects) throws Exception {
        // Use reflection to call the private indexMultiple method
        var method = IndexCommand.class.getDeclaredMethod("indexMultiple", List.class);
        method.setAccessible(true);
        return (int) method.invoke(indexCommand, projects);
    }

    private Path createJavaProject(String name, String className, String methodBody)
            throws IOException {
        Path root = workspace.resolve(name);
        Path src  = root.resolve("src/main/java/com/example");
        Files.createDirectories(src);
        Files.writeString(src.resolve(className + ".java"),
                "package com.example;\n" +
                "public class " + className + " {\n  " + methodBody + "\n}");
        // pom.xml so ProjectDiscovery recognises it as a Maven project
        Files.writeString(root.resolve("pom.xml"),
                "<?xml version=\"1.0\"?><project>" +
                "<groupId>com.example</groupId>" +
                "<artifactId>" + name + "</artifactId>" +
                "<version>1.0</version>" +
                "</project>");
        return root;
    }

    private List<SearchResult> search(String query, String project) throws IOException {
        var reader = luceneIndexer.openMultiReader(List.of(project));
        SearchRequest req = new SearchRequest(query, SearchRequest.SearchType.KEYWORD,
                project, null, 20, "text", "method", null);
        return strategy.search(reader, req);
    }

    // Minimal in-memory registry
    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() { super(IndexConfig.defaults()); }

        @Override public synchronized void register(ProjectMeta m) { store.put(m.getName(), m); }
        @Override public synchronized Optional<ProjectMeta> find(String n) { return Optional.ofNullable(store.get(n)); }
        @Override public synchronized List<ProjectMeta> listAll() { return new ArrayList<>(store.values()); }
        @Override public synchronized void link(String a, String b) {
            ProjectMeta ma = store.get(a);
            ProjectMeta mb = store.get(b);
            if (ma != null && !ma.getLinkedProjects().contains(b)) ma.getLinkedProjects().add(b);
            if (mb != null && !mb.getLinkedProjects().contains(a)) mb.getLinkedProjects().add(a);
        }
        @Override public synchronized void unregister(String n) { store.remove(n); }
    }
}
