package com.pharos.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.GraphView;
import com.pharos.graph.GraphViews;
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
import java.util.Locale;
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
            // {project, query}; letters are the eval/benchmark.py categories, as far as sample-app allows
            new String[]{JAVA, "Calculator"},                                  // A exact name
            new String[]{JAVA, "greeting service"},                            // B named concept
            new String[]{JAVA, "Computes the absolute value of an integer"},   // C javadoc phrasing
            new String[]{JAVA, "generate a greeting message for a user"},      // D behavioural / intent
            new String[]{JAVA, "division by zero is not allowed"},             // E error handling
            new String[]{JAVA, "user display name and email address"},         // F config-like state (no real config in sample-app)
            new String[]{JAVA, "checks whether user has a valid email"},       // G contract
            new String[]{JAVA, "methods that call getName"},                   // H cross-cutting usage pattern
            new String[]{PY, "abs_value"},
            new String[]{PY, "has_valid_email"},
            new String[]{PY, "formal_greet"},
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
                // Engine order is kept except inside runs of equal formatted score, where Lucene's
                // tie order flaps between runs: those runs are ordered by id (stable elsewhere).
                for (int i = 0; i < results.size(); ) {
                    int j = i + 1;
                    String sc = String.format(Locale.ROOT, "%.6f", results.get(i).score());
                    while (j < results.size()
                            && sc.equals(String.format(Locale.ROOT, "%.6f", results.get(j).score()))) j++;
                    results.subList(i, j).sort(Comparator.comparing(SearchResult::id));
                    i = j;
                }
                List<Map<String, Object>> rows = new ArrayList<>();
                for (SearchResult r : results) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("id", r.id());
                    row.put("score", String.format(Locale.ROOT, "%.6f", r.score()));
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

    /** Single seam for opening call graphs; fails loudly if a registered project has none. */
    private static List<GraphView> openGraphs(ProjectRegistry registry) {
        List<GraphView> graphs = new ArrayList<>();
        for (ProjectMeta meta : registry.listAll().stream()
                .sorted(Comparator.comparing(ProjectMeta::getName)).toList()) {
            Path shardDir = Path.of(meta.getIndexPath());
            // writer/all: the generation named by callgraph.current (legacy dir without one);
            // reader: the generation dir's compact graph
            if (!Files.exists(com.pharos.graph.CallGraph.projectGraphDir(shardDir))
                    && !Files.exists(shardDir.resolve(GraphViews.COMPACT_FILE))) {
                graphs.forEach(GraphView::close);
                throw new IllegalStateException("No call graph for project '" + meta.getName()
                        + "' at " + shardDir);
            }
            graphs.add(GraphViews.open(meta));
        }
        if (graphs.isEmpty()) throw new IllegalStateException("No registered projects with call graphs");
        return graphs;
    }

    private static List<String> graphLookup(ProjectRegistry registry, String fqn, boolean callers) {
        List<GraphView> graphs = openGraphs(registry);
        try {
            List<String> out = new ArrayList<>();
            for (GraphView g : graphs) out.addAll(callers ? g.callers(fqn) : g.callees(fqn));
            return out;
        } finally {
            graphs.forEach(GraphView::close);
        }
    }

    private static List<String> shortestPath(ProjectRegistry registry, String from, String to) {
        List<GraphView> graphs = openGraphs(registry);
        try {
            for (GraphView g : graphs) {
                List<String> p = g.shortestPath(from, to);
                if (p != null && !p.isEmpty()) return p;
            }
            return List.of();
        } finally {
            graphs.forEach(GraphView::close);
        }
    }
}
