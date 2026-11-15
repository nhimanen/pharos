package com.pharos.web;

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

class IndexFormatWebTest {

    @TempDir Path tmp;

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private Javalin app;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        LuceneIndexer luceneIndexer = new LuceneIndexer(config);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        ProjectIndexManager manager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopGraph);
        manager.index(copy("sample-app", "sample-app-a"), "a", false, false, false, false, ProgressListener.SILENT);
        manager.index(copy("sample-app", "sample-app-b"), "b", false, false, false, false, ProgressListener.SILENT);
        Files.delete(luceneIndexer.getProjectIndexDir("b").resolve("index-format.json"));

        SearchEngine engine = new SearchEngine(luceneIndexer, new NoOpEmbeddingProvider(), registry);
        WebServer server = new WebServer(engine, registry, noopGraph, luceneIndexer,
                new ModuleBoundaryAnalyzer(registry, engine));
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        app = server.start(port, tmp.resolve("pharos-home"));
    }

    @AfterEach
    void tearDown() {
        if (app != null) app.stop();
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

    @Test
    void scopedSearchOnOutdatedIs409() throws Exception {
        HttpResponse<String> r = get("/api/search?q=email&type=keyword&project=b");
        assertThat(r.statusCode()).isEqualTo(409);
        JsonNode j = mapper.readTree(r.body());
        assertThat(j.get("status").asText()).isEqualTo("outdated");
        assertThat(j.get("project").asText()).isEqualTo("b");
        assertThat(j.get("error").asText()).contains("b");
    }

    @Test
    void crossProjectSearchCarriesWarnings() throws Exception {
        HttpResponse<String> r = get("/api/search?q=email&type=keyword");
        assertThat(r.statusCode()).isEqualTo(200);
        JsonNode j = mapper.readTree(r.body());
        assertThat(j.get("warnings")).isNotEmpty();
        assertThat(j.get("results")).isNotEmpty();
    }

    @Test
    void projectsListShowsIndexFormat() throws Exception {
        JsonNode arr = mapper.readTree(get("/api/projects").body());
        String statusA = null, statusB = null;
        for (JsonNode p : arr) {
            String s = p.get("indexFormat").get("status").asText();
            if (p.get("name").asText().equals("a")) statusA = s;
            if (p.get("name").asText().equals("b")) statusB = s;
        }
        assertThat(statusA).isEqualTo("ok");
        assertThat(statusB).isEqualTo("outdated");
    }

    @Test
    void graphEndpointsOnOutdatedAre409NotRetried500() throws Exception {
        for (String path : List.of("/api/boundary?project=b",
                "/api/class?fqn=x.Y&context=true",  // unscoped: skipped, not 409
                "/api/usages?fqn=x.Y")) {
            int code = get(path).statusCode();
            if (path.startsWith("/api/boundary")) {
                HttpResponse<String> r = get(path);
                assertThat(r.statusCode()).isEqualTo(409);
                assertThat(mapper.readTree(r.body()).get("status").asText()).isEqualTo("outdated");
            } else {
                assertThat(code).isNotEqualTo(500);
            }
        }
    }

    @Test
    void unscopedPathIncludesWarning() throws Exception {
        JsonNode j = mapper.readTree(get("/api/path?from=x.A%23a()&to=x.B%23b()").body());
        assertThat(j.get("warnings")).hasSize(1);
        assertThat(j.get("warnings").get(0).asText()).contains("'b'");
    }
}
