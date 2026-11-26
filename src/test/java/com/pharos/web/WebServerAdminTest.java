package com.pharos.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cli.LinkCommand;
import com.pharos.cli.RemoveIndexCommand;
import com.pharos.cli.StatsCommand;
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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code POST /api/admin/<command>}: the daemon runs {@code remove-index}, {@code link} and {@code stats}
 * for {@code ./pharos} on its own indexer, local requests with {@code X-Pharos-Admin} only.
 */
class WebServerAdminTest {

    @TempDir Path tmp;

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private Javalin app;
    private int port;
    private ProjectRegistry registry;
    private LuceneIndexer luceneIndexer;

    @BeforeEach
    void setUp() throws Exception {
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
        ProjectIndexManager manager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopGraph);
        manager.index(copy("sample-app", "sample-app-a"), "a", false, false, false, false, ProgressListener.SILENT);
        manager.index(copy("sample-app", "sample-app-b"), "b", false, false, false, false, ProgressListener.SILENT);

        SearchEngine engine = new SearchEngine(luceneIndexer, new NoOpEmbeddingProvider(), registry);
        WebServer server = new WebServer(engine, registry, noopGraph, luceneIndexer,
                new ModuleBoundaryAnalyzer(registry, engine));
        server.setAdminCommands(Map.of(
                "remove-index", () -> new RemoveIndexCommand(registry, manager, config),
                "link", () -> new LinkCommand(registry, config),
                "stats", () -> new StatsCommand(registry, luceneIndexer)));
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

    private HttpResponse<String> admin(String command, String body, boolean header) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/admin/" + command))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (header) b.header(WebServer.ADMIN_HEADER, "1");
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode run(String command, String... args) throws Exception {
        HttpResponse<String> r = admin(command, mapper.writeValueAsString(Map.of("args", List.of(args))), true);
        assertThat(r.statusCode()).as(r.body()).isEqualTo(200);
        return mapper.readTree(r.body());
    }

    @Test
    void statsRunsTheCliCommandInTheDaemon() throws Exception {
        JsonNode j = run("stats", "--project", "a");
        assertThat(j.get("exitCode").asInt()).isZero();
        assertThat(j.get("out").asText()).contains("Project: a", "Lucene docs:").doesNotContain("Project: b");
    }

    @Test
    void removeIndexClosesTheDaemonsReaderAndUnregisters() throws Exception {
        // a search opens (and caches) the daemon's reader of project a
        assertThat(get("/api/search?q=email&type=keyword&project=a").statusCode()).isEqualTo(200);
        Path dirA = luceneIndexer.getProjectIndexDir("a");
        assertThat(dirA).isDirectory();

        JsonNode j = run("remove-index", "a");
        assertThat(j.get("exitCode").asInt()).as(j.toString()).isZero();
        assertThat(j.get("out").asText()).contains("Removed project 'a'.");
        assertThat(registry.find("a")).isEmpty();
        assertThat(dirA).doesNotExist();

        // the daemon keeps serving the other project, and no longer lists a
        assertThat(get("/api/search?q=email&type=keyword&project=b").statusCode()).isEqualTo(200);
        JsonNode projects = mapper.readTree(get("/api/projects").body());
        assertThat(projects.findValuesAsText("name")).contains("b").doesNotContain("a");
    }

    @Test
    void cliUsageErrorsComeBackWithTheirExitCode() throws Exception {
        JsonNode j = run("remove-index");
        assertThat(j.get("exitCode").asInt()).isEqualTo(2);
        assertThat(j.get("err").asText()).contains("Missing project name");

        j = run("remove-index", "--no-such-option");
        assertThat(j.get("exitCode").asInt()).isEqualTo(2);   // picocli's usage error, as on the CLI

        j = run("remove-index", "ghost");
        assertThat(j.get("exitCode").asInt()).isEqualTo(1);
        assertThat(j.get("err").asText()).contains("Project not found: ghost");
    }

    @Test
    void linkRunsInTheDaemon() throws Exception {
        JsonNode j = run("link", "a", "b");
        assertThat(j.get("exitCode").asInt()).as(j.toString()).isZero();
        assertThat(j.get("out").asText()).contains("Linked 'a' and 'b'", "Cross-project graph built.");
        assertThat(registry.find("a").orElseThrow().getLinkedProjects()).contains("b");
    }

    @Test
    void refusedWithoutTheHeaderForUnknownCommandsAndBadBodies() throws Exception {
        assertThat(admin("remove-index", "{\"args\":[\"a\"]}", false).statusCode()).isEqualTo(403);
        assertThat(registry.find("a")).isPresent();
        assertThat(admin("index", "{\"args\":[]}", true).statusCode()).isEqualTo(404);
        assertThat(admin("stats", "{\"args\":\"a\"}", true).statusCode()).isEqualTo(400);
        assertThat(admin("stats", "{\"args\":[1]}", true).statusCode()).isEqualTo(400);
        // an empty body: the command without arguments
        assertThat(mapper.readTree(admin("stats", "", true).body()).get("exitCode").asInt()).isZero();
    }

    @Test
    void onlyLoopbackPeersAreAdmitted() {
        assertThat(WebServer.isLoopback("127.0.0.1")).isTrue();
        assertThat(WebServer.isLoopback("0:0:0:0:0:0:0:1")).isTrue();
        assertThat(WebServer.isLoopback("[::1]")).isTrue();
        assertThat(WebServer.isLoopback("10.0.0.7")).isFalse();
        assertThat(WebServer.isLoopback("192.168.1.52")).isFalse();
        assertThat(WebServer.isLoopback("localhost")).isFalse(); // names are never resolved
        assertThat(WebServer.isLoopback(null)).isFalse();
        assertThat(WebServer.isLoopback("")).isFalse();
    }
}
