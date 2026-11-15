package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.search.SearchEngine;
import org.apache.lucene.index.IndexWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A full re-index builds the call graph in a new generation dir and publishes it through the
 * {@code callgraph.current} pointer, so a reader never sees a cleared or half-built graph and a
 * reader of the old generation can't damage the new one.
 */
class GraphSwapTest {

    private static final String PROJECT = "sample-app";
    private static final String GET_NAME = "com.example.model.User#getName()";
    /** Graph dir (legacy or generation) plus optional sidecar suffix. */
    private static final Pattern GRAPH_ENTRY = Pattern.compile("(callgraph(?:-\\d+)?\\.arcadedb)(\\..*)?");

    @TempDir Path tmp;

    private Path projectRoot;
    private LuceneIndexer luceneIndexer;
    private ProjectRegistry registry;
    private ProjectIndexManager manager;

    @BeforeEach
    void setUp() throws Exception {
        projectRoot = copySample("sample-app");
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        luceneIndexer = new LuceneIndexer(config);
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        manager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopGraph);
    }

    private Path copySample(String as) throws Exception {
        Path src = Paths.get("src/test/resources/test-projects/sample-app");
        Path dst = tmp.resolve(as);
        try (var walk = Files.walk(src)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path t = dst.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        return dst;
    }

    private Path indexDir() { return luceneIndexer.getProjectIndexDir(PROJECT); }

    /** The live graph dir of a project index dir, read from the on-disk layout (pointer, else legacy). */
    private static Path liveGraphDir(Path projectIndexDir) throws Exception {
        Path pointer = projectIndexDir.resolve("callgraph.current");
        return Files.isRegularFile(pointer)
                ? projectIndexDir.resolve(Files.readString(pointer).trim())
                : projectIndexDir.resolve("callgraph.arcadedb");
    }

    private Path liveGraphDir() throws Exception { return liveGraphDir(indexDir()); }

    /** The graph dir names present (sidecars folded into their graph). */
    private Set<String> graphDirs() throws Exception {
        Set<String> out = new TreeSet<>();
        try (var s = Files.list(indexDir())) {
            for (Path p : s.toList()) {
                Matcher m = GRAPH_ENTRY.matcher(p.getFileName().toString());
                if (m.matches()) out.add(m.group(1));
            }
        }
        return out;
    }

    private void indexFull() throws Exception {
        manager.index(projectRoot, PROJECT, false, false, false, false, ProgressListener.SILENT);
    }

    private void indexIncremental() throws Exception {
        manager.index(projectRoot, PROJECT, true, false, false, false, ProgressListener.SILENT);
    }

    private static void addBadge(Path root, String body) throws Exception {
        Path f = root.resolve("src/main/java/com/example/service/Badge.java");
        Files.writeString(f, """
                package com.example.service;

                import com.example.model.User;

                public class Badge {
                %s
                }
                """.formatted(body));
        // Distinct, later mtime so an incremental run sees the edit.
        Files.setLastModifiedTime(f, java.nio.file.attribute.FileTime.fromMillis(
                System.currentTimeMillis() + 10_000 + body.length()));
    }

    private static final String LABEL =
            "    public String label(User user) { return \"[\" + user.getName() + \"]\"; }";
    private static final String SHOUT =
            "    public String shout(User user) { return user.getName().toUpperCase(); }";

    @Test
    void readersSeeOldGraphUntilFullReindexSwaps() throws Exception {
        indexFull();
        addBadge(projectRoot, LABEL);

        AtomicReference<Throwable> indexFailure = new AtomicReference<>();
        int polls = 0;
        try (CallGraph reader = CallGraph.open(liveGraphDir())) {
            assertThat(reader.callers(GET_NAME)).isNotEmpty();

            Thread indexer = new Thread(() -> {
                try { indexFull(); } catch (Throwable t) { indexFailure.set(t); }
            }, "full-reindex");
            indexer.start();
            while (indexer.isAlive()) {
                Set<String> callers = reader.callers(GET_NAME);
                assertThat(callers).as("callers seen by a reader during the full re-index").isNotEmpty();
                polls++;
            }
            indexer.join();
            assertThat(reader.callers(GET_NAME)).isNotEmpty();
        }
        assertThat(indexFailure.get()).isNull();
        assertThat(polls).isPositive();

        // After the swap, a fresh reader sees the rebuilt graph, including the new caller.
        try (CallGraph fresh = CallGraph.open(liveGraphDir())) {
            assertThat(fresh.callers(GET_NAME)).anyMatch(c -> c.startsWith("com.example.service.Badge#label("));
        }
        assertThat(graphDirs()).containsExactly(liveGraphDir().getFileName().toString());
    }

    @Test
    void closingAReaderOfTheOldGraphLeavesTheNewOneIntact() throws Exception {
        indexFull();
        addBadge(projectRoot, LABEL);  // the re-index changes the method count

        CallGraph oldReader = CallGraph.open(liveGraphDir());
        long oldCount = oldReader.methodCount();
        indexFull();
        oldReader.close();  // closes after the swap, as a request in flight would

        // The same sources indexed from scratch, as a second project.
        Path freshRoot = copySample("sample-app-fresh");
        addBadge(freshRoot, LABEL);
        manager.index(freshRoot, "fresh", false, false, false, false, ProgressListener.SILENT);
        long expected;
        try (CallGraph g = CallGraph.open(liveGraphDir(luceneIndexer.getProjectIndexDir("fresh")))) {
            expected = g.methodCount();
        }
        assertThat(expected).isGreaterThan(oldCount);

        Path live = liveGraphDir();
        try (CallGraph g = CallGraph.open(live)) {
            assertThat(g.methodCount()).as("live graph count after the old reader closed").isEqualTo(expected);
        }

        // A writer can still open and patch the live graph.
        addBadge(projectRoot, LABEL + "\n" + SHOUT);
        indexIncremental();
        assertThat(liveGraphDir()).isEqualTo(live);
        addBadge(freshRoot, LABEL + "\n" + SHOUT);
        manager.index(freshRoot, "fresh", false, false, false, false, ProgressListener.SILENT);
        long expectedAfterPatch;
        try (CallGraph g = CallGraph.open(liveGraphDir(luceneIndexer.getProjectIndexDir("fresh")))) {
            expectedAfterPatch = g.methodCount();
        }
        try (CallGraph g = CallGraph.open(live)) {
            assertThat(g.callers(GET_NAME)).anyMatch(c -> c.startsWith("com.example.service.Badge#shout("));
            assertThat(g.methodCount()).as("patched count").isEqualTo(expectedAfterPatch);
        }
        assertThat(live.resolve("database.lck")).as("closed cleanly").doesNotExist();
    }

    @Test
    void readOnProjectWithoutGraphCreatesNothing() throws Exception {
        indexFull();
        try (var s = Files.list(indexDir())) {
            for (Path p : s.toList()) {
                String n = p.getFileName().toString();
                if (GRAPH_ENTRY.matcher(n).matches() || n.equals("callgraph.current")) {
                    try (var w = Files.walk(p)) {
                        for (Path q : w.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(q);
                    }
                }
            }
        }
        SearchEngine engine = new SearchEngine(luceneIndexer, new NoOpEmbeddingProvider(), registry);
        assertThat(engine.findUsages(GET_NAME, "callers", PROJECT).callers()).isEmpty();
        assertThat(engine.findUsages(GET_NAME, "callers").callers()).isEmpty();
        var mcp = new com.pharos.mcp.McpToolRegistry(engine, registry, null,
                new com.pharos.graph.ModuleBoundaryAnalyzer(registry, engine));
        var args = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode().put("fqn", GET_NAME);
        mcp.call("get_callers", args);
        assertThat(graphDirs()).as("a read must not create a graph").isEmpty();
    }

    @Test
    void openExistingNeverCreatesAGraph() {
        Path none = tmp.resolve("nothing-here/callgraph.arcadedb");
        assertThatThrownBy(() -> CallGraph.openExisting(none))
                .isInstanceOf(com.pharos.graph.GraphNotFoundException.class);
        assertThat(none).doesNotExist();
    }

    @Test
    void interruptedRunLeavesPreviousGenerationLiveAndSearchable() throws Exception {
        indexFull();
        Path live = liveGraphDir();
        // A run that built its generation but died before publishing the pointer.
        Path orphan = indexDir().resolve("callgraph-" + (System.currentTimeMillis() + 3_600_000L) + ".arcadedb");
        try (CallGraph g = CallGraph.open(orphan)) {
            g.addMethod("x.Orphan#o()", "x.Orphan");
            g.addMethod("x.Orphan#p()", "x.Orphan");
            g.addCall("x.Orphan#p()", "x.Orphan#o()");
            g.flush();
        }

        assertThat(liveGraphDir()).isEqualTo(live);
        SearchEngine engine = new SearchEngine(luceneIndexer, new NoOpEmbeddingProvider(), registry);
        assertThat(engine.findUsages(GET_NAME, "callers", PROJECT).callers()).isNotEmpty();
        assertThat(engine.findUsages("x.Orphan#o()", "callers", PROJECT).callers()).isEmpty();

        indexFull();
        assertThat(Files.exists(orphan)).as("orphan generation cleaned by the next full run").isFalse();
        assertThat(graphDirs()).containsExactly(liveGraphDir().getFileName().toString());
        assertThat(engine.findUsages(GET_NAME, "callers", PROJECT).callers()).isNotEmpty();
    }

    @Test
    void leftoverGenerationIsCleanedOnlyByARunHoldingTheWriteLock() throws Exception {
        indexFull();
        Path leftover = indexDir().resolve("callgraph-" + (System.currentTimeMillis() + 3_600_000L) + ".arcadedb");
        Files.createDirectories(leftover);
        Files.writeString(leftover.resolve("junk"), "x");

        // Another run holds the project's write lock: this run must fail before touching it.
        try (IndexWriter other = luceneIndexer.openWriter(PROJECT)) {
            assertThatThrownBy(this::indexFull)
                    .isInstanceOf(org.apache.lucene.store.LockObtainFailedException.class);
            assertThat(leftover.resolve("junk")).as("in-progress generation untouched").exists();
        }

        indexFull();
        assertThat(Files.exists(leftover)).isFalse();
        assertThat(graphDirs()).containsExactly(liveGraphDir().getFileName().toString());
        try (CallGraph g = CallGraph.open(liveGraphDir())) {
            assertThat(g.callers(GET_NAME)).isNotEmpty();
        }
    }
}
