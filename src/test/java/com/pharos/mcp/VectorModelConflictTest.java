package com.pharos.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.web.WebServer;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 gate minors: a search scoped to one project without vectors of the search model is a
 * conflict (HTTP 409 on the web API, an {@code isError} tool result on MCP), never a generic error;
 * the explicit-single rule counts the REQUESTED projects, as a cluster coordinator does.
 */
class VectorModelConflictTest {

    @TempDir Path tmp;
    private Javalin app;
    private int port;
    private SearchEngine engine;
    private ProjectRegistry registry;
    private ModuleGraphBuilder noopGraph;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    /** "Available" model with no vectors in any project (they were indexed without embeddings). */
    private static final class StubModel extends NoOpEmbeddingProvider {
        StubModel() { super("stub-model"); }
        @Override public boolean isAvailable() { return true; }
        @Override public int dimensions() { return 8; }
        @Override public float[] embed(String text) {
            float[] v = new float[8];
            v[Math.abs(text.hashCode()) % 8] = 1f;
            return v;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        LuceneIndexer lucene = new LuceneIndexer(config);
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        ProjectIndexManager manager = new ProjectIndexManager(config, lucene, registry,
                new NoOpEmbeddingProvider(), noopGraph);
        manager.index(copy("sample-app", "a"), "a", false, false, false, false, ProgressListener.SILENT);
        manager.index(copy("sample-app", "b"), "b", false, false, false, false, ProgressListener.SILENT);
        // b two formats behind: below the N-1 read window, so unusable (left out with a warning)
        Path f = lucene.getProjectIndexDir("b").resolve("index-format.json");
        Files.writeString(f, "{\"format\":" + (com.pharos.indexer.IndexVersions.INDEX_FORMAT - 2)
                + ",\"pharosVersion\":\"0.9.0\",\"writtenAt\":\"2026-01-01T00:00:00Z\"}");
        Files.setLastModifiedTime(f, java.nio.file.attribute.FileTime.from(
                java.time.Instant.parse("2026-01-01T00:00:00Z")));
        engine = new SearchEngine(lucene, new StubModel(), registry);
        WebServer server = new WebServer(engine, registry, noopGraph, lucene, new ModuleBoundaryAnalyzer(registry, engine));
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        app = server.start(port, tmp.resolve("pharos-home"));
    }

    @AfterEach
    void tearDown() {
        if (app != null) app.stop();
    }

    @Test
    void scopedVectorSearchWithoutVectorsIs409OnTheWebApi() throws Exception {
        HttpResponse<String> scoped = get("/api/search?q=Calculator&pipeline=vector&project=a");
        assertThat(scoped.statusCode()).as(scoped.body()).isEqualTo(409);
        assertThat(mapper.readTree(scoped.body()).path("error").asText())
                .contains("Project 'a' has no vectors for search embedding model 'stub-model'");

        HttpResponse<String> unscoped = get("/api/search?q=Calculator&pipeline=hybrid");
        assertThat(unscoped.statusCode()).as(unscoped.body()).isEqualTo(200);
        assertThat(unscoped.body()).contains(SearchEngine.noVectorsWarning("a", "stub-model"));
    }

    @Test
    void scopedVectorSearchWithoutVectorsIsAToolErrorOnMcp() throws Exception {
        McpServer mcp = new McpServer(new McpToolRegistry(engine, registry, noopGraph,
                new ModuleBoundaryAnalyzer(registry, engine)));
        String out = mcp.handleMessage("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":"
                + "{\"name\":\"search_code\",\"arguments\":{\"query\":\"Calculator\",\"type\":\"vector\",\"project\":\"a\"}}}");
        JsonNode r = mapper.readTree(out);
        assertThat(r.has("error")).as(out).isFalse();
        assertThat(r.path("result").path("isError").asBoolean()).as(out).isTrue();
        assertThat(r.path("result").path("content").get(0).path("text").asText())
                .startsWith("conflict (409): Project 'a' has no vectors");
    }

    /** Requested [a, b] with b unusable: only "a" remains, but two were requested, so it warns (as the cluster). */
    @Test
    void explicitSingleCountsTheRequestedProjects() throws Exception {
        var resp = engine.searchWithTrace(new SearchRequest("Calculator", SearchRequest.SearchType.HYBRID, null,
                List.of("a", "b"), 10, "text", null, null, 0), false);
        assertThat(resp.warnings()).contains(SearchEngine.noVectorsWarning("a", "stub-model"));
        assertThat(resp.results()).isNotEmpty();
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

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
