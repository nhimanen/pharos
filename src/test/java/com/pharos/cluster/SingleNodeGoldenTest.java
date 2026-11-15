package com.pharos.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import com.pharos.parser.PythonCodeParser;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Characterisation test: snapshots single-node search and graph results so that the cluster
 * refactor can prove behaviour is unchanged. {@link #snapshot} is reused to compare nodes.
 */
class SingleNodeGoldenTest {

    private static final Path GOLDEN_SRC = Path.of("src/test/resources/golden/single-node-search.json");
    private static final String JAVA = "sample-app";
    private static final String PY = "sample-python-app";

    private static final List<String[]> QUERIES = List.of(
            // {project, query}
            new String[]{JAVA, "GreetingService"},
            new String[]{JAVA, "Calculator"},
            new String[]{JAVA, "User"},
            new String[]{JAVA, "generate a greeting message for a user"},
            new String[]{JAVA, "farewell"},
            new String[]{JAVA, "formal greeting email domain"},
            new String[]{JAVA, "divide two numbers"},
            new String[]{JAVA, "absolute value"},
            new String[]{PY, "abs_value"},
            new String[]{PY, "has_valid_email"},
            new String[]{PY, "greet user"},
            new String[]{PY, "multiply"});

    private static final List<String> CALLERS_OF = List.of(
            "com.example.model.User#getName()",
            "com.example.model.User#getEmail()");

    private static final List<String> CALLEES_OF = List.of(
            "com.example.service.GreetingService#greet(User)",
            "com.example.service.GreetingService#farewell(User)",
            "com.example.service.GreetingService#formalGreet(User)");

    @TempDir Path tmp;

    @Test
    void singleNodeSnapshotMatchesGolden() throws Exception {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        LuceneIndexer lucene = new LuceneIndexer(config);
        EmbeddingProvider embedder = EmbeddingProvider.create(config); // NoOp: no model URL

        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(
                    Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        ProjectIndexManager manager = new ProjectIndexManager(
                config, lucene, registry, embedder, noopModules,
                List.of(new JavaCodeParser(), new PythonCodeParser()));
        Path projects = Path.of("src/test/resources/test-projects");
        manager.index(projects.resolve("sample-app"), JAVA, false, false, false,
                false, com.pharos.indexer.ProgressListener.SILENT);
        manager.index(projects.resolve("sample-python-app"), PY, false, false, false,
                false, com.pharos.indexer.ProgressListener.SILENT);

        SearchEngine engine = new SearchEngine(lucene, embedder, registry);
        String actual = snapshot(engine, registry);

        if (!Files.exists(GOLDEN_SRC)) {
            Files.createDirectories(GOLDEN_SRC.getParent());
            Files.writeString(GOLDEN_SRC, actual);
            fail("golden generated — review and commit: " + GOLDEN_SRC);
        }
        assertThat(actual).isEqualTo(Files.readString(GOLDEN_SRC));
    }

    /** Canonical (sorted-key) JSON of search and graph results for the fixed corpus. */
    public static String snapshot(SearchEngine engine, ProjectRegistry registry) throws Exception {
        Map<String, Object> root = new LinkedHashMap<>();

        Map<String, Object> searches = new LinkedHashMap<>();
        for (String[] pq : QUERIES) {
            for (SearchRequest.SearchType type :
                    List.of(SearchRequest.SearchType.KEYWORD, SearchRequest.SearchType.AUTO)) {
                SearchRequest req = new SearchRequest(pq[1], type, pq[0], null, 10,
                        "text", null, null, 0);
                List<SearchResult> results = new ArrayList<>(
                        engine.newSnippetDecorator(15).decorate(engine.search(req), pq[1]));
                // Deterministic tie-break: score desc (as formatted), then id.
                results.sort(Comparator
                        .comparingDouble((SearchResult r) -> -Double.parseDouble(String.format("%.6f", r.score())))
                        .thenComparing(SearchResult::id));
                List<Map<String, Object>> rows = new ArrayList<>();
                for (SearchResult r : results) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id", r.id());
                    row.put("score", String.format("%.6f", r.score()));
                    row.put("snippetStartLine", r.snippet() == null ? -1 : r.snippet().startLine());
                    rows.add(row);
                }
                searches.put(type.name().toLowerCase() + "|" + pq[0] + "|" + pq[1], rows);
            }
        }
        root.put("searches", searches);

        Map<String, Object> graph = new LinkedHashMap<>();
        for (String fqn : CALLERS_OF) {
            graph.put("callers|" + fqn, new TreeSet<>(graphLookup(registry, fqn, true)));
        }
        for (String fqn : CALLEES_OF) {
            graph.put("callees|" + fqn, new TreeSet<>(graphLookup(registry, fqn, false)));
        }
        for (String[] pair : PATHS) {
            graph.put("path|" + pair[0] + "->" + pair[1], shortestPath(registry, pair[0], pair[1]));
        }
        root.put("graph", graph);

        Map<String, Object> classes = new LinkedHashMap<>();
        for (String cls : CLASS_FQNS) {
            SearchEngine.ClassContext ctx = engine.getClassContext(cls);
            Map<String, Object> c = new LinkedHashMap<>();
            if (ctx != null) {
                c.put("body", ctx.body());
                c.put("constructors", ctx.constructors().stream().map(SearchResult::id).sorted().toList());
                c.put("publicMethods", ctx.publicMethods().stream().map(SearchResult::id).sorted().toList());
                Map<String, List<String>> callers = new java.util.TreeMap<>();
                ctx.publicMethodCallers().forEach((k, v) -> callers.put(k, v.stream().sorted().toList()));
                c.put("publicMethodCallers", callers);
            }
            classes.put(cls, c);
        }
        root.put("classContext", classes);

        return new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .writeValueAsString(root) + "\n";
    }

    private static final List<String[]> PATHS = List.of(
            new String[]{"com.example.service.GreetingService#greet(User)", "com.example.model.User#getName()"},
            new String[]{"com.example.service.GreetingService#formalGreet(User)", "com.example.model.User#getEmail()"});

    private static final List<String> CLASS_FQNS = List.of(
            "com.example.service.GreetingService", "com.example.model.User");

    private static List<CallGraph> openGraphs(ProjectRegistry registry) {
        List<CallGraph> graphs = new ArrayList<>();
        for (ProjectMeta meta : registry.listAll().stream()
                .sorted(Comparator.comparing(ProjectMeta::getName)).toList()) {
            Path dbDir = Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb");
            if (Files.isDirectory(dbDir)) graphs.add(CallGraph.open(dbDir));
        }
        return graphs;
    }

    private static List<String> graphLookup(ProjectRegistry registry, String fqn, boolean callers) {
        List<CallGraph> graphs = openGraphs(registry);
        try {
            List<String> out = new ArrayList<>();
            for (CallGraph g : graphs) out.addAll(callers ? g.callers(fqn) : g.callees(fqn));
            return out;
        } finally {
            graphs.forEach(CallGraph::close);
        }
    }

    private static List<String> shortestPath(ProjectRegistry registry, String from, String to) {
        List<CallGraph> graphs = openGraphs(registry);
        try {
            for (CallGraph g : graphs) {
                List<String> p = g.shortestPath(from, to);
                if (p != null && !p.isEmpty()) return p;
            }
            return List.of();
        } finally {
            graphs.forEach(CallGraph::close);
        }
    }
}
