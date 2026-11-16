package com.pharos.search;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.IncompatibleIndexException;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IndexVersions;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexFormatReadTest {

    @TempDir Path tmp;

    private LuceneIndexer luceneIndexer;
    private ProjectIndexManager manager;
    private SearchEngine engine;
    private ProjectRegistry registry;
    private ModuleGraphBuilder noopGraph;
    private Path rootB;

    @BeforeEach
    void setUp() throws Exception {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        luceneIndexer = new LuceneIndexer(config);
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        manager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopGraph);
        Path rootA = copy("sample-app", "sample-app-a");
        rootB = copy("sample-app", "sample-app-b");
        indexFull(rootA, "a");
        indexFull(rootB, "b");
        engine = new SearchEngine(luceneIndexer, new NoOpEmbeddingProvider(), registry);
    }

    private Path copy(String name, String as) throws Exception {
        Path src = Paths.get("src/test/resources/test-projects", name);
        Path dst = tmp.resolve(as);
        try (var walk = Files.walk(src)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path t = dst.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        return dst;
    }

    private void indexFull(Path root, String name) throws Exception {
        manager.index(root, name, false, false, false, false, ProgressListener.SILENT);
    }

    /** Stamps {@code project} with {@code format} by hand, as an older or newer pharos would. */
    private void stamp(String project, int format) throws Exception {
        Path f = luceneIndexer.getProjectIndexDir(project).resolve("index-format.json");
        Files.writeString(f, "{\"format\":" + format
                + ",\"pharosVersion\":\"0.9.0\",\"writtenAt\":\"2026-01-01T00:00:00Z\"}");
        // A distinct mtime, so the per-dir status cache never serves the previous stamp.
        Files.setLastModifiedTime(f, java.nio.file.attribute.FileTime.from(
                java.time.Instant.parse("2026-01-01T00:00:00Z")));
    }

    /** Two formats behind: below the N-1 read window, so refused. */
    private void unstamp(String project) throws Exception {
        stamp(project, IndexVersions.INDEX_FORMAT - 2);
    }

    private SearchRequest kw(String q, String project) {
        return new SearchRequest(q, SearchRequest.SearchType.KEYWORD, project, null, 50, "text", null, null, 0);
    }

    /** A term present in both fixtures. */
    private static final String TERM = "email";

    @Test
    void scopedSearchOnOutdatedThrows() throws Exception {
        unstamp("b");
        assertThatThrownBy(() -> engine.searchWithTrace(kw(TERM, "b"), false))
                .isInstanceOf(IncompatibleIndexException.class);
    }

    @Test
    void crossProjectSearchSkipsOutdated() throws Exception {
        SearchResponse before = engine.searchWithTrace(kw(TERM, null), false);
        assertThat(before.results()).anyMatch(r -> r.id().startsWith("b:"));
        assertThat(before.warnings()).isEmpty();

        unstamp("b");
        SearchResponse r = engine.searchWithTrace(kw(TERM, null), false);
        assertThat(r.results()).isNotEmpty().allMatch(x -> x.id().startsWith("a:"));
        assertThat(r.warnings()).hasSize(1);
        assertThat(r.warnings().get(0)).contains("'b'");
    }

    @Test
    void previousFormatIsServedDuringCrossProjectSearch() throws Exception {
        stamp("b", IndexVersions.INDEX_FORMAT - 1);
        assertThat(IndexFormat.status(luceneIndexer.getProjectIndexDir("b")))
                .isEqualTo(IndexFormat.Status.UPGRADE_AVAILABLE);

        SearchResponse r = engine.searchWithTrace(kw(TERM, null), false);
        assertThat(r.results()).anyMatch(x -> x.id().startsWith("b:"));
        assertThat(r.warnings()).isEmpty();
        // scoped search and graph tools serve it too
        assertThat(engine.searchWithTrace(kw(TERM, "b"), false).results()).isNotEmpty();
        List<String> w = new java.util.ArrayList<>();
        assertThat(engine.findUsages(A_FQN, "callers", "b", w).callers()).isNotEmpty();
        assertThat(w).isEmpty();
    }

    @Test
    void allOutdatedGivesEmptyWithWarnings() throws Exception {
        unstamp("a");
        unstamp("b");
        SearchResponse r = engine.searchWithTrace(kw(TERM, null), false);
        assertThat(r.results()).isEmpty();
        assertThat(r.warnings()).hasSize(2);
    }

    @Test
    void reindexClearsWithoutRestart() throws Exception {
        unstamp("b");
        assertThatThrownBy(() -> engine.searchWithTrace(kw(TERM, "b"), false))
                .isInstanceOf(IncompatibleIndexException.class);

        indexFull(rootB, "b");

        assertThat(IndexFormat.status(luceneIndexer.getProjectIndexDir("b")))
                .isEqualTo(IndexFormat.Status.OK);
        assertThat(engine.searchWithTrace(kw(TERM, "b"), false).results()).isNotEmpty();
    }

    @Test
    void graphToolOnOutdatedThrows() throws Exception {
        unstamp("b");
        assertThatThrownBy(() -> engine.findUsages("pkg.Foo#bar()", "callers", "b"))
                .isInstanceOf(IncompatibleIndexException.class);
        // unscoped: skipped, not thrown
        assertThat(engine.findUsages("pkg.Foo#bar()", "callers").callers()).isEmpty();
    }

    private static final String A_FQN = "com.example.model.User#getName()";

    @Test
    void unscopedLookupExposesSkippedProjectWarning() throws Exception {
        unstamp("b");
        List<String> warnings = new java.util.ArrayList<>();
        assertThat(engine.getByFqns(List.of(A_FQN), warnings)).containsKey(A_FQN);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("'b'");

        List<String> w2 = new java.util.ArrayList<>();
        assertThat(engine.getMethodByFqn(A_FQN, w2)).isNotNull();
        assertThat(w2).hasSize(1);
    }

    @Test
    void unscopedMcpGraphAndLookupToolsPrefixWarnings() throws Exception {
        unstamp("b");
        var mcp = new com.pharos.mcp.McpToolRegistry(engine, registry, noopGraph,
                new com.pharos.graph.ModuleBoundaryAnalyzer(registry, engine));
        var args = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode().put("fqn", A_FQN);

        for (String tool : List.of("get_callers", "get_callees", "get_method", "find_usages")) {
            String out = mcp.call(tool, args);
            assertThat(out).as(tool).startsWith("\u26a0 ").contains("'b'");
        }
        // after the stamp is restored the notice disappears
        indexFull(rootB, "b");
        assertThat(mcp.call("get_callers", args)).doesNotContain("\u26a0");
    }
}
