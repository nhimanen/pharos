package com.pharos.web;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.*;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pharos.indexer.DocumentMapper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Embedded Javalin HTTP server that exposes REST APIs for the web UI
 * and serves static files (HTML/CSS/JS) from the classpath /web directory.
 */
public class WebServer {

    private static final Logger log = LoggerFactory.getLogger(WebServer.class);

    private final SearchEngine searchEngine;
    private final ProjectRegistry registry;
    private final ModuleGraphBuilder moduleGraphBuilder;
    private final LuceneIndexer luceneIndexer;
    private final ModuleBoundaryAnalyzer boundaryAnalyzer;
    private final ObjectMapper mapper;

    public WebServer(SearchEngine searchEngine, ProjectRegistry registry,
                     ModuleGraphBuilder moduleGraphBuilder, LuceneIndexer luceneIndexer,
                     ModuleBoundaryAnalyzer boundaryAnalyzer) {
        this.searchEngine = searchEngine;
        this.registry = registry;
        this.moduleGraphBuilder = moduleGraphBuilder;
        this.luceneIndexer = luceneIndexer;
        this.boundaryAnalyzer = boundaryAnalyzer;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public void start(int port) {
        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson(mapper, false));
            config.staticFiles.add("/web", Location.CLASSPATH);
        });

        // Root → index.html
        app.get("/", ctx -> {
            try (InputStream is = WebServer.class.getResourceAsStream("/web/index.html")) {
                if (is != null) ctx.contentType("text/html").result(is.readAllBytes());
                else ctx.status(404).result("index.html not found");
            }
        });

        // API: available search pipelines — for UI pipeline selector
        app.get("/api/pipelines", ctx -> ctx.json(searchEngine.listPipelines()));

        // API: project list
        app.get("/api/projects", ctx ->
                ctx.json(registry.listAll().stream()
                        .map(this::projectToMap)
                        .collect(Collectors.toList())));

        // API: module dependency graph
        app.get("/api/graph/modules", ctx -> ctx.json(buildModuleGraphData()));

        // API: class dependency graph for a project
        app.get("/api/graph/classes/{project}", ctx -> {
            String name = ctx.pathParam("project");
            ProjectMeta meta = registry.find(name).orElse(null);
            if (meta == null) { ctx.status(404).result("Project not found: " + name); return; }
            ctx.json(buildClassGraphData(meta));
        });

        // API: call graph for a project
        app.get("/api/graph/call/{project}", ctx -> {
            String name = ctx.pathParam("project");
            int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(150);
            String pkg = ctx.queryParam("package");
            String className = ctx.queryParam("class");
            ProjectMeta meta = registry.find(name).orElse(null);
            if (meta == null) { ctx.status(404).result("Project not found: " + name); return; }
            ctx.json(buildCallGraphData(meta, limit, pkg, className));
        });

        // API: language breakdown for a project
        app.get("/api/languages/{project}", ctx -> {
            String name = ctx.pathParam("project");
            ProjectMeta meta = registry.find(name).orElse(null);
            if (meta == null) { ctx.status(404).result("Project not found: " + name); return; }
            ctx.json(buildLanguageBreakdown(meta));
        });

        // API: search — ?pipeline=<id> takes precedence over ?type=<id> for UI use
        app.get("/api/search", ctx -> {
            String q = ctx.queryParam("q");
            if (q == null || q.isBlank()) { ctx.json(List.of()); return; }
            String project = ctx.queryParam("project");
            String pipeline = ctx.queryParam("pipeline");
            String type = pipeline != null ? pipeline
                    : ctx.queryParamAsClass("type", String.class).getOrDefault("auto");
            int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(20);
            int oversample = ctx.queryParamAsClass("oversample", Integer.class).getOrDefault(0);
            String docType = ctx.queryParam("docType");
            if ("all".equals(docType)) docType = null;
            String scope = ctx.queryParam("scope");
            if ("all".equals(scope)) scope = null;
            if (project != null && project.isBlank()) project = null;
            boolean trace       = "true".equalsIgnoreCase(ctx.queryParam("trace"));
            int snippetLines    = ctx.queryParamAsClass("snippetLines", Integer.class).getOrDefault(15);
            SearchRequest req = new SearchRequest(q, SearchRequest.SearchType.from(type),
                    project, null, limit, "text", docType, scope, oversample);
            try {
                SearchResponse resp = searchEngine.searchWithTrace(req, false);
                List<SearchResult> decorated = searchEngine
                        .newSnippetDecorator(snippetLines, resp)
                        .decorate(resp.results(), q);
                List<Map<String, Object>> results = decorated.stream()
                        .map(this::resultToMap).collect(Collectors.toList());
                boolean needsEnvelope = trace || (resp.suggestions() != null && !resp.suggestions().isEmpty());
                if (needsEnvelope) {
                    Map<String, Object> out = new LinkedHashMap<>();
                    out.put("results", results);
                    if (trace) {
                        Map<String, Object> meta = new LinkedHashMap<>();
                        meta.put("requestedType", type);
                        meta.put("resolvedType",  resp.resolvedType() != null ? resp.resolvedType() : type);
                        meta.put("totalMs",       resp.trace().totalMs());
                        meta.put("stages", resp.trace().spans().stream().map(s -> {
                            Map<String, Object> stage = new LinkedHashMap<>();
                            stage.put("name", s.name()); stage.put("ms", s.durationMs()); return stage;
                        }).collect(Collectors.toList()));
                        out.put("searchMeta", meta);
                    }
                    var sugg = resp.suggestions();
                    if (sugg != null && !sugg.isEmpty()) {
                        Map<String, Object> sm = new LinkedHashMap<>();
                        if (!sugg.fuzzyMatches().isEmpty()) sm.put("fuzzyMatches", sugg.fuzzyMatches());
                        if (!sugg.tokenMatches().isEmpty()) sm.put("tokenMatches", sugg.tokenMatches());
                        if (sugg.filterNote() != null) sm.put("filterNote", sugg.filterNote());
                        out.put("suggestions", sm);
                    }
                    ctx.json(out);
                } else {
                    ctx.json(results);
                }
            } catch (Exception e) {
                log.error("Search failed for query '{}': {}", q, e.getMessage(), e);
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        // API: single method by FQN
        app.get("/api/method", ctx -> {
            String fqn = ctx.queryParam("fqn");
            if (fqn == null) { ctx.status(400).result("fqn required"); return; }
            SearchResult r = searchEngine.getMethodByFqn(fqn);
            if (r == null && fqn.contains("#") && !fqn.contains("(")) {
                // Partial FQN — parameter types omitted. Try name-only lookup.
                List<SearchResult> candidates = searchEngine.findMethodsByPartialFqn(fqn);
                if (candidates.isEmpty()) {
                    ctx.status(404).result("Not found"); return;
                } else if (candidates.size() == 1) {
                    ctx.json(methodToMap(candidates.get(0))); return;
                } else {
                    String list = candidates.stream()
                            .map(c -> "  " + c.id().substring(c.project().length() + 1))
                            .collect(Collectors.joining("\n"));
                    ctx.status(400).result(
                            "Ambiguous FQN — " + candidates.size() + " overloads found. Include parameter types:\n" + list);
                    return;
                }
            }
            if (r == null) { ctx.status(404).result("Not found"); return; }
            ctx.json(methodToMap(r));
        });

        // API: class body — ?fqn=  (add ?context=true for full context including fields/constructors/callers)
        app.get("/api/class", ctx -> {
            String fqn     = ctx.queryParam("fqn");
            boolean withCtx = "true".equalsIgnoreCase(ctx.queryParam("context"));
            if (fqn == null) { ctx.status(400).result("fqn required"); return; }
            try {
                if (withCtx) {
                    SearchEngine.ClassContext context = searchEngine.getClassContext(fqn);
                    if (context == null) { ctx.status(404).result("Not found"); return; }
                    ctx.json(classContextToMap(context));
                } else {
                    SearchResult r = searchEngine.getClassByFqn(fqn);
                    if (r == null) { ctx.status(404).result("Not found"); return; }
                    ctx.json(classToMap(r));
                }
            } catch (Exception e) {
                log.error("getClass failed for '{}': {}", fqn, e.getMessage(), e);
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        // API: callers of a method
        app.get("/api/callers", ctx -> {
            String fqn = ctx.queryParam("fqn");
            String project = ctx.queryParam("project");
            if (fqn == null) { ctx.status(400).result("fqn required"); return; }
            try {
                ctx.json(searchEngine.findCallers(fqn, project).stream()
                        .map(this::refToMap).collect(Collectors.toList()));
            } catch (IOException e) {
                log.warn("findCallers failed for '{}': {}", fqn, e.getMessage());
                ctx.status(503).result(e.getMessage());
            }
        });

        // API: callees of a method
        app.get("/api/callees", ctx -> {
            String fqn = ctx.queryParam("fqn");
            String project = ctx.queryParam("project");
            if (fqn == null) { ctx.status(400).result("fqn required"); return; }
            try {
                ctx.json(searchEngine.findCallees(fqn, project).stream()
                        .map(this::refToMap).collect(Collectors.toList()));
            } catch (IOException e) {
                log.warn("findCallees failed for '{}': {}", fqn, e.getMessage());
                ctx.status(503).result(e.getMessage());
            }
        });

        // API: transitive callers (impact analysis) — ?fqn=&depth=5&maxCallers=2000
        app.get("/api/impact", ctx -> {
            String fqn = ctx.queryParam("fqn");
            if (fqn == null || fqn.isBlank()) { ctx.status(400).result("fqn required"); return; }
            int depth      = Math.min(10, ctx.queryParamAsClass("depth",      Integer.class).getOrDefault(5));
            int maxCallers = ctx.queryParamAsClass("maxCallers", Integer.class).getOrDefault(2000);
            try {
                ctx.json(searchEngine.findTransitiveCallers(fqn, depth, maxCallers));
            } catch (Exception e) {
                log.error("findTransitiveCallers failed for '{}': {}", fqn, e.getMessage(), e);
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        // API: knowledge-graph usages — ?fqn=&kind=all
        app.get("/api/usages", ctx -> {
            String fqn = ctx.queryParam("fqn");
            if (fqn == null || fqn.isBlank()) { ctx.status(400).result("fqn required"); return; }
            String kind = ctx.queryParamAsClass("kind", String.class).getOrDefault("all");
            try {
                ctx.json(searchEngine.findUsages(fqn, kind));
            } catch (Exception e) {
                log.error("findUsages failed for '{}': {}", fqn, e.getMessage(), e);
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        // API: multi-hop call-chain traversal — ?fqn=&depth=2&direction=callees&maxBodyChars=500
        app.get("/api/trace", ctx -> {
            String fqn = ctx.queryParam("fqn");
            if (fqn == null || fqn.isBlank()) { ctx.status(400).result("fqn required"); return; }
            int    depth        = Math.min(4, ctx.queryParamAsClass("depth",        Integer.class).getOrDefault(2));
            String direction    = ctx.queryParamAsClass("direction", String.class).getOrDefault("callees");
            int    maxBodyChars = ctx.queryParamAsClass("maxBodyChars", Integer.class).getOrDefault(500);
            try {
                ctx.json(searchEngine.traceCallChain(fqn, depth, direction, maxBodyChars));
            } catch (Exception e) {
                log.error("traceCallChain failed for '{}': {}", fqn, e.getMessage(), e);
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        // API: call path between two methods (searches each project graph)
        app.get("/api/path", ctx -> {
            String from = ctx.queryParam("from");
            String to = ctx.queryParam("to");
            if (from == null || to == null) { ctx.status(400).result("from and to required"); return; }
            List<String> path = List.of();
            for (ProjectMeta meta : registry.listAll()) {
                try (CallGraph g = loadCallGraphCached(meta)) {
                    List<String> candidate = g.shortestPath(from, to);
                    if (!candidate.isEmpty()) { path = candidate; break; }
                } catch (Exception ignored) {}
            }
            Map<String, Object> res = new LinkedHashMap<>();
            res.put("path", path);
            res.put("hops", path.isEmpty() ? 0 : path.size() - 1);
            ctx.json(res);
        });

        // API: list modules
        app.get("/api/modules", ctx -> {
            String filter = ctx.queryParamAsClass("filter", String.class).getOrDefault("all");
            try (com.pharos.graph.ModuleGraph graph = moduleGraphBuilder.open()) {
                List<Map<String, Object>> nodes = graph.allModules()
                        .filter(n -> switch (filter) {
                            case "indexed"  -> n.isIndexed();
                            case "external" -> !n.isIndexed();
                            default         -> true;
                        })
                        .sorted(Comparator.comparing(com.pharos.graph.ModuleNodeData::moduleKey))
                        .map(n -> {
                            Map<String, Object> m = new LinkedHashMap<>();
                            m.put("moduleKey", n.moduleKey());
                            m.put("groupId", n.groupId());
                            m.put("artifactId", n.artifactId());
                            m.put("version", n.version());
                            m.put("indexed", n.isIndexed());
                            m.put("projectName", n.projectName());
                            m.put("depCount", graph.dependencies(n.moduleKey()).size());
                            m.put("usedByCount", graph.dependents(n.moduleKey()).size());
                            return m;
                        })
                        .collect(Collectors.toList());
                Map<String, Object> res = new LinkedHashMap<>();
                res.put("total", graph.moduleCount());
                res.put("edges", graph.dependencyCount());
                res.put("modules", nodes);
                ctx.json(res);
            }
        });

        // API: module dependencies
        app.get("/api/deps", ctx -> {
            String moduleRef = ctx.queryParam("module");
            boolean transitive = Boolean.parseBoolean(ctx.queryParamAsClass("transitive", String.class).getOrDefault("false"));
            if (moduleRef == null) { ctx.status(400).result("module required"); return; }
            try (com.pharos.graph.ModuleGraph graph = moduleGraphBuilder.open()) {
                com.pharos.graph.ModuleNodeData node = resolveModule(graph, moduleRef);
                if (node == null) { ctx.status(404).result("Module not found: " + moduleRef); return; }
                Set<com.pharos.graph.ModuleNodeData> deps = transitive ? bfsModules(graph, node, true) : graph.dependencies(node.moduleKey());
                Set<com.pharos.graph.ModuleNodeData> dependents = transitive ? bfsModules(graph, node, false) : graph.dependents(node.moduleKey());
                Map<String, Object> res = new LinkedHashMap<>();
                res.put("moduleKey", node.moduleKey());
                res.put("indexed", node.isIndexed());
                res.put("projectName", node.projectName());
                res.put("version", node.version());
                res.put("transitive", transitive);
                res.put("dependencies", deps.stream().sorted(Comparator.comparing(com.pharos.graph.ModuleNodeData::moduleKey))
                        .map(d -> Map.of("moduleKey", d.moduleKey(), "indexed", d.isIndexed())).collect(Collectors.toList()));
                res.put("dependents", dependents.stream().sorted(Comparator.comparing(com.pharos.graph.ModuleNodeData::moduleKey))
                        .map(d -> Map.of("moduleKey", d.moduleKey(), "indexed", d.isIndexed())).collect(Collectors.toList()));
                ctx.json(res);
            }
        });

        // API: module dependency path
        app.get("/api/mod-path", ctx -> {
            String fromRef = ctx.queryParam("from");
            String toRef = ctx.queryParam("to");
            if (fromRef == null || toRef == null) { ctx.status(400).result("from and to required"); return; }
            try (com.pharos.graph.ModuleGraph graph = moduleGraphBuilder.open()) {
                com.pharos.graph.ModuleNodeData fromNode = resolveModule(graph, fromRef);
                com.pharos.graph.ModuleNodeData toNode   = resolveModule(graph, toRef);
                if (fromNode == null) { ctx.status(404).result("Module not found: " + fromRef); return; }
                if (toNode   == null) { ctx.status(404).result("Module not found: " + toRef); return; }
                List<Map<String, Object>> pathNodes = graph.shortestPath(fromNode.moduleKey(), toNode.moduleKey())
                        .stream().map(n -> {
                            Map<String, Object> m = new LinkedHashMap<>();
                            m.put("moduleKey", n.moduleKey());
                            m.put("indexed", n.isIndexed());
                            m.put("projectName", n.projectName());
                            return m;
                        }).collect(Collectors.toList());
                Map<String, Object> res = new LinkedHashMap<>();
                res.put("path", pathNodes);
                res.put("hops", pathNodes.isEmpty() ? 0 : pathNodes.size() - 1);
                ctx.json(res);
            }
        });

        // API: module boundary (entry/exit points)
        app.get("/api/boundary", ctx -> {
            String project = ctx.queryParam("project");
            if (project == null) { ctx.status(400).result("project required"); return; }
            int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(200);
            try {
                ModuleBoundaryAnalyzer.BoundaryResult result =
                        boundaryAnalyzer.analyze(project, limit);
                List<Map<String, Object>> exits = result.exitPoints().stream()
                        .map(ep -> {
                            Map<String, Object> m = new LinkedHashMap<>();
                            m.put("callerFqn", ep.callerFqn());
                            m.put("calleeSimpleName", ep.calleeSimpleName());
                            m.put("line", ep.line());
                            return m;
                        }).collect(Collectors.toList());
                Map<String, Object> res = new LinkedHashMap<>();
                res.put("project", project);
                res.put("entryPoints", result.entryPoints());
                res.put("exitPoints", exits);
                res.put("totalExitPoints", result.totalExitPoints());
                ctx.json(res);
            } catch (Exception e) {
                ctx.status(500).result("Error analyzing boundary: " + e.getMessage());
            }
        });

        // API: project skeleton (public/protected classes and method signatures)
        app.get("/api/skeleton", ctx -> {
            String project = ctx.queryParam("project");
            if (project == null || project.isBlank()) { ctx.status(400).result("project required"); return; }
            int methodLimit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(500);
            String pathFilter = ctx.queryParam("path");   // optional: restrict to files under this path
            int depth = ctx.queryParamAsClass("depth", Integer.class).getOrDefault(-1); // -1 = unlimited
            if (!luceneIndexer.indexExists(project)) {
                ctx.status(404).result("No index found for project: " + project); return;
            }
            try {
                ctx.json(buildSkeletonData(project, methodLimit, pathFilter, depth));
            } catch (IOException e) {
                log.warn("skeleton failed for '{}': {}", project, e.getMessage());
                ctx.status(500).result("Error building skeleton: " + e.getMessage());
            }
        });

        // Health check (used by daemon client to detect readiness)
        app.get("/health", ctx -> ctx.json(Map.of("status", "ok")));

        app.start(port);
        System.out.printf("Web UI available at http://localhost:%d%n", port);
        log.info("Web server started on port {}", port);
    }

    // ── Graph data builders ────────────────────────────────────────────────────

    private Map<String, Object> buildModuleGraphData() throws Exception {
        try (com.pharos.graph.ModuleGraph graph = moduleGraphBuilder.open()) {
            List<Map<String, Object>> nodes = new ArrayList<>();
            List<Map<String, Object>> edges = new ArrayList<>();
            List<com.pharos.graph.ModuleNodeData> allNodes = graph.allModules().collect(Collectors.toList());

            for (com.pharos.graph.ModuleNodeData n : allNodes) {
                Map<String, Object> node = new LinkedHashMap<>();
                node.put("id", n.moduleKey());
                node.put("label", n.artifactId() != null ? n.artifactId() : n.moduleKey());
                node.put("status", n.isIndexed() ? "indexed" : "external");
                node.put("projectName", n.projectName());
                node.put("version", n.version());
                node.put("groupId", n.groupId());
                node.put("depCount", graph.dependencies(n.moduleKey()).size());
                node.put("usedByCount", graph.dependents(n.moduleKey()).size());
                nodes.add(node);
            }

            Set<String> seen = new HashSet<>();
            for (com.pharos.graph.ModuleNodeData source : allNodes) {
                for (com.pharos.graph.ModuleNodeData target : graph.dependencies(source.moduleKey())) {
                    String key = source.moduleKey() + "->" + target.moduleKey();
                    if (seen.add(key)) {
                        Map<String, Object> edge = new LinkedHashMap<>();
                        edge.put("source", source.moduleKey());
                        edge.put("target", target.moduleKey());
                        edges.add(edge);
                    }
                }
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("nodes", nodes);
            result.put("edges", edges);
            return result;
        }
    }

    /** Open the ArcadeDB call graph for the given project. Caller must close. */
    private CallGraph loadCallGraphCached(ProjectMeta meta) throws Exception {
        Path dbDir = Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb");
        return CallGraph.open(dbDir);
    }

    private Map<String, Object> buildClassGraphData(ProjectMeta meta) throws Exception {
        try (CallGraph graph = loadCallGraphCached(meta)) {
        Set<String> knownPkgs = new HashSet<>(meta.getKnownPackages());
        Map<String, String> fqnToFile = buildFqnToFilePath(meta);
        // Map class FQN → filePath (use first method's path as representative)
        Map<String, String> classToFile = new HashMap<>();
        for (Map.Entry<String, String> e : fqnToFile.entrySet())
            classToFile.putIfAbsent(extractFullClass(e.getKey()), e.getValue());

        Set<String> allMethods = graph.allFqns().collect(Collectors.toSet());

        // Count methods per class
        Map<String, Integer> methodCount = new HashMap<>();
        for (String m : allMethods) {
            methodCount.merge(extractFullClass(m), 1, Integer::sum);
        }
        // All classes that appear as nodes in the graph (including callee-only externals)
        Map<String, Integer> classInDegree = new HashMap<>();
        Set<String> allClasses = new HashSet<>(methodCount.keySet());

        // First pass: collect all callee classes so we can include external ones as nodes
        List<Map<String, Object>> edges = new ArrayList<>();
        Set<String> seenEdges = new HashSet<>();

        for (String srcMethod : allMethods) {
            String srcClass = extractFullClass(srcMethod);
            for (String tgtMethod : graph.callees(srcMethod)) {
                String tgtClass = extractFullClass(tgtMethod);
                if (!srcClass.equals(tgtClass)) {
                    allClasses.add(tgtClass);
                    String key = srcClass + "->" + tgtClass;
                    if (seenEdges.add(key)) {
                        classInDegree.merge(tgtClass, 1, Integer::sum);
                        Map<String, Object> edge = new LinkedHashMap<>();
                        edge.put("source", srcClass);
                        edge.put("target", tgtClass);
                        edges.add(edge);
                    }
                }
            }
        }

        List<Map<String, Object>> nodes = allClasses.stream().map(cls -> {
            String pkg = extractPackageOfClass(cls);
            boolean external = !knownPkgs.contains(pkg);
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("id", cls);
            n.put("label", extractSimpleClass(cls));
            n.put("packageName", pkg);
            n.put("kind", "class");
            n.put("methodCount", methodCount.getOrDefault(cls, 0));
            n.put("inDegree", classInDegree.getOrDefault(cls, 0));
            n.put("external", external);
            String fp = classToFile.get(cls);
            if (fp != null) n.put("filePath", fp);
            return n;
        }).collect(Collectors.toList());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("project", meta.getName());
        result.put("nodes", nodes);
        result.put("edges", edges);
        result.put("total", allClasses.size());
        return result;
        } // end try-with-resources CallGraph
    }

    private Map<String, Object> buildCallGraphData(ProjectMeta meta, int limit, String pkg, String className) throws Exception {
        try (CallGraph graph = loadCallGraphCached(meta)) {
        Set<String> knownPkgs = new HashSet<>(meta.getKnownPackages());
        Map<String, String> fqnToFile = buildFqnToFilePath(meta);

        Set<String> methods = graph.allFqns().collect(Collectors.toSet());
        boolean classView = className != null && !className.isBlank();
        if (classView) {
            methods = methods.stream()
                    .filter(m -> extractFullClass(m).equals(className))
                    .collect(Collectors.toSet());
        } else if (pkg != null && !pkg.isBlank()) {
            methods = methods.stream().filter(m -> m.startsWith(pkg)).collect(Collectors.toSet());
        }

        final Set<String> filtered = methods;
        Map<String, Integer> inDegree = new HashMap<>();
        for (String m : filtered) {
            inDegree.put(m, (int) graph.callers(m).stream().filter(filtered::contains).count());
        }

        List<String> top = filtered.stream()
                .sorted((a, b) -> Integer.compare(
                        inDegree.getOrDefault(b, 0), inDegree.getOrDefault(a, 0)))
                .limit(limit)
                .collect(Collectors.toList());
        Set<String> topSet = new HashSet<>(top);

        // For class-level drill-down, also include external callees as nodes
        Map<String, Map<String, Object>> nodeMap = new LinkedHashMap<>();
        for (String fqn : top) {
            String p = extractPackage(fqn);
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("id", fqn);
            n.put("label", extractMethodName(fqn));
            n.put("className", extractClassName(fqn));
            n.put("packageName", p);
            n.put("kind", inferKind(fqn));
            n.put("inDegree", inDegree.getOrDefault(fqn, 0));
            n.put("external", !knownPkgs.contains(p));
            String fp = fqnToFile.get(fqn);
            if (fp != null) n.put("filePath", fp);
            nodeMap.put(fqn, n);
        }

        List<Map<String, Object>> edges = new ArrayList<>();
        Set<String> seenEdges = new HashSet<>();
        for (String source : topSet) {
            for (String target : graph.callees(source)) {
                String key = source + "->" + target;
                if (!seenEdges.add(key)) continue;
                if (!topSet.contains(target)) {
                    if (classView) {
                        // Add external callee as a node so it can be hidden/shown
                        if (!nodeMap.containsKey(target)) {
                            String p = extractPackage(target);
                            Map<String, Object> ext = new LinkedHashMap<>();
                            ext.put("id", target);
                            ext.put("label", extractMethodName(target));
                            ext.put("className", extractClassName(target));
                            ext.put("packageName", p);
                            ext.put("kind", inferKind(target));
                            ext.put("inDegree", 0);
                            ext.put("external", !knownPkgs.contains(p));
                            String tfp = fqnToFile.get(target);
                            if (tfp != null) ext.put("filePath", tfp);
                            nodeMap.put(target, ext);
                        }
                    } else {
                        continue; // non-class view: skip out-of-set targets
                    }
                }
                Map<String, Object> edge = new LinkedHashMap<>();
                edge.put("source", source);
                edge.put("target", target);
                edges.add(edge);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("project", meta.getName());
        result.put("nodes", new ArrayList<>(nodeMap.values()));
        result.put("edges", edges);
        result.put("total", filtered.size());
        result.put("truncated", filtered.size() > limit);
        return result;
        } // end try-with-resources CallGraph
    }

    // ── Language breakdown ─────────────────────────────────────────────────────

    /** Returns a sorted map of file-extension → doc count for the given project. */
    private Map<String, Integer> buildLanguageBreakdown(ProjectMeta meta) throws IOException {
        if (!luceneIndexer.indexExists(meta.getName())) return Map.of();
        IndexReader reader = luceneIndexer.openReader(meta.getName());
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs all = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (var sd : all.scoreDocs) {
            String fp = sf.document(sd.doc).get(DocumentMapper.F_FILE_PATH);
            if (fp == null || fp.isBlank()) continue;
            String ext = extension(fp);
            counts.merge(ext, 1, Integer::sum);
        }
        // Return sorted by count descending
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue,
                        (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * Builds a structural skeleton of a project: public/protected classes and their method signatures.
     * Returns a list of {className, qualifiedClassName, filePath, methods:[signature,...]} sorted by class name.
     * Capped at {@code methodLimit} total methods.
     *
     * @param pathFilter  if non-null, only include files whose path starts with this prefix
     * @param depth       if >= 0, only include files within this many directory levels below pathFilter
     */
    private List<Map<String, Object>> buildSkeletonData(String projectName, int methodLimit,
                                                         String pathFilter, int depth) throws IOException {
        // Normalise the path filter so we can do prefix matching
        final String normPath = (pathFilter != null && !pathFilter.isBlank())
                ? pathFilter.replace('\\', '/').replaceAll("/$", "")
                : null;
        final int depthLimit = depth; // -1 = unlimited

        IndexReader reader = luceneIndexer.openReader(projectName);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs all = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();

        // qualifiedClassName → ordered list of method signatures
        Map<String, List<String>> classToMethods = new LinkedHashMap<>();
        // qualifiedClassName → simple className (for display)
        Map<String, String> classSimpleName = new LinkedHashMap<>();
        // qualifiedClassName → filePath
        Map<String, String> classFilePath = new LinkedHashMap<>();

        int totalMethods = 0;
        for (var sd : all.scoreDocs) {
            if (totalMethods >= methodLimit) break;
            var doc = sf.document(sd.doc);
            String docType = doc.get(DocumentMapper.F_DOC_TYPE);
            if (!"method".equals(docType)) continue;

            String access = doc.get(DocumentMapper.F_ACCESS);
            if (access == null || access.isBlank()) continue;
            if (!"public".equals(access) && !"protected".equals(access)) continue;

            String filePath = doc.get(DocumentMapper.F_FILE_PATH);
            if (normPath != null) {
                String normFile = filePath != null ? filePath.replace('\\', '/') : "";
                if (!normFile.startsWith(normPath)) continue;
                if (depthLimit >= 0) {
                    // Count extra path separators beyond the prefix
                    String remainder = normFile.substring(normPath.length());
                    if (remainder.startsWith("/")) remainder = remainder.substring(1);
                    long extra = remainder.isEmpty() ? 0 : remainder.chars().filter(c -> c == '/').count() + 1;
                    if (extra > depthLimit) continue;
                }
            }

            String qualClass = doc.get(DocumentMapper.F_QUALIFIED_CLASS);
            if (qualClass == null || qualClass.isBlank()) continue;
            String sig = doc.get(DocumentMapper.F_SIGNATURE);
            if (sig == null || sig.isBlank()) continue;
            String simpleName = doc.get(DocumentMapper.F_CLASS_NAME);

            classToMethods.computeIfAbsent(qualClass, k -> new ArrayList<>()).add(sig);
            classSimpleName.putIfAbsent(qualClass, simpleName != null ? simpleName : extractSimpleClass(qualClass));
            classFilePath.putIfAbsent(qualClass, filePath != null ? filePath : "");
            totalMethods++;
        }

        return classToMethods.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("className", classSimpleName.getOrDefault(e.getKey(), extractSimpleClass(e.getKey())));
                    m.put("qualifiedClassName", e.getKey());
                    m.put("filePath", classFilePath.getOrDefault(e.getKey(), ""));
                    m.put("methods", e.getValue());
                    return m;
                })
                .collect(Collectors.toList());
    }

    /** Builds a FQN → filePath map by scanning the Lucene index for a project. */
    private Map<String, String> buildFqnToFilePath(ProjectMeta meta) throws IOException {
        if (!luceneIndexer.indexExists(meta.getName())) return Map.of();
        IndexReader reader = luceneIndexer.openReader(meta.getName());
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs all = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
        StoredFields sf = searcher.storedFields();
        Map<String, String> map = new HashMap<>();
        for (var sd : all.scoreDocs) {
            var doc = sf.document(sd.doc);
            String id = doc.get(DocumentMapper.F_ID);       // "projectName:fqn"
            String fp = doc.get(DocumentMapper.F_FILE_PATH);
            if (id == null || fp == null) continue;
            int colon = id.indexOf(':');
            String fqn = colon >= 0 ? id.substring(colon + 1) : id;
            map.put(fqn, fp);
        }
        return map;
    }

    private static String extension(String filePath) {
        int dot = filePath.lastIndexOf('.');
        if (dot < 0 || dot == filePath.length() - 1) return "other";
        return filePath.substring(dot + 1).toLowerCase();
    }

    // ── DTO mappers ────────────────────────────────────────────────────────────

    private Map<String, Object> classContextToMap(SearchEngine.ClassContext ctx) {
        SearchResult cls = ctx.classResult();
        String fqnStr = cls.id().substring(cls.project().length() + 1);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fqn",      fqnStr);
        m.put("label",    cls.label());
        m.put("project",  cls.project());
        m.put("javadoc",  cls.javadoc());
        m.put("body",     ctx.body());
        m.put("filePath", cls.filePath());
        m.put("startLine", cls.startLine());
        m.put("endLine",   cls.endLine());
        m.put("fields", ctx.fields().stream().map(f -> {
            Map<String, Object> fm = new LinkedHashMap<>();
            fm.put("fqn",       f.fqn());
            fm.put("name",      f.fieldName());
            fm.put("type",      f.fieldType());
            fm.put("accessMod", f.accessMod());
            return fm;
        }).collect(Collectors.toList()));
        m.put("constructors", ctx.constructors().stream().map(c -> {
            Map<String, Object> cm = new LinkedHashMap<>();
            cm.put("fqn",       c.id().substring(c.project().length() + 1));
            cm.put("signature", c.signature());
            cm.put("javadoc",   c.javadoc());
            return cm;
        }).collect(Collectors.toList()));
        m.put("publicMethods", ctx.publicMethods().stream().map(pm -> {
            String pmFqn = pm.id().substring(pm.project().length() + 1);
            Map<String, Object> pm2 = new LinkedHashMap<>();
            pm2.put("fqn",       pmFqn);
            pm2.put("signature", pm.signature());
            pm2.put("javadoc",   pm.javadoc());
            pm2.put("callers",   ctx.publicMethodCallers().getOrDefault(pmFqn, List.of()));
            return pm2;
        }).collect(Collectors.toList()));
        return m;
    }

    private Map<String, Object> projectToMap(ProjectMeta p) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", p.getName());
        m.put("rootPath", p.getRootPath());
        m.put("fileCount", p.getFileCount());
        m.put("methodCount", p.getMethodCount());
        m.put("classCount", p.getClassCount());
        m.put("lastIndexed", p.getLastIndexed());
        m.put("groupId", p.getGroupId());
        m.put("artifactId", p.getArtifactId());
        m.put("indexed", luceneIndexer.indexExists(p.getName()));
        return m;
    }

    private Map<String, Object> resultToMap(SearchResult r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.id());
        m.put("label", r.label());
        m.put("project", r.project());
        m.put("className", r.className());
        m.put("signature", r.signature());
        m.put("javadoc", r.javadoc());
        m.put("filePath", r.filePath());
        m.put("startLine", r.startLine());
        m.put("endLine", r.endLine());
        m.put("score", r.score());
        m.put("docType", r.docType());
        m.put("searchType", r.searchType());
        // Snippet fields — populated by SnippetDecorator when present
        Snippet snip = r.snippet();
        if (snip != null) {
            m.put("snippet",          snip.text());
            m.put("snippetStartLine", snip.startLine());
            m.put("snippetEndLine",   snip.endLine());
        }
        return m;
    }

    private Map<String, Object> methodToMap(SearchResult r) {
        Map<String, Object> m = resultToMap(r);
        m.put("packageName", r.packageName());
        m.put("methodName", r.methodName());
        m.put("returnType", r.returnType());
        m.put("body", r.body());
        m.put("accessModifier", r.accessModifier());
        return m;
    }

    private Map<String, Object> classToMap(SearchResult r) {
        Map<String, Object> m = resultToMap(r);
        m.put("packageName", r.packageName());
        m.put("className", r.className());
        m.put("qualifiedClassName", r.qualifiedClassName());
        m.put("accessModifier", r.accessModifier());
        String sourceBody = SourceReader.readRange(r.filePath(), r.startLine(), r.endLine());
        // Fall back to the index's synthesized body if the source file isn't readable
        m.put("body", sourceBody != null ? sourceBody : r.body());
        m.put("sourceAvailable", sourceBody != null);
        return m;
    }

    private Map<String, Object> refToMap(SearchResult r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.id());
        m.put("label", r.label());
        m.put("project", r.project());
        m.put("filePath", r.filePath());
        m.put("startLine", r.startLine());
        m.put("docType", r.docType());
        return m;
    }

    // ── Module helpers ─────────────────────────────────────────────────────────

    private static com.pharos.graph.ModuleNodeData resolveModule(com.pharos.graph.ModuleGraph graph, String ref) {
        Optional<com.pharos.graph.ModuleNodeData> n = graph.findByKey(ref);
        if (n.isPresent()) return n.get();
        return graph.findByProjectName(ref).orElse(null);
    }

    private static Set<com.pharos.graph.ModuleNodeData> bfsModules(com.pharos.graph.ModuleGraph graph,
                                                                     com.pharos.graph.ModuleNodeData start,
                                                                     boolean outbound) {
        Set<com.pharos.graph.ModuleNodeData> visited = new LinkedHashSet<>();
        Deque<com.pharos.graph.ModuleNodeData> queue = new ArrayDeque<>();
        queue.add(start);
        while (!queue.isEmpty()) {
            com.pharos.graph.ModuleNodeData cur = queue.pop();
            Set<com.pharos.graph.ModuleNodeData> next = outbound
                    ? graph.dependencies(cur.moduleKey()) : graph.dependents(cur.moduleKey());
            for (com.pharos.graph.ModuleNodeData n : next) {
                if (visited.add(n)) queue.add(n);
            }
        }
        visited.remove(start);
        return visited;
    }

    // ── FQN helpers ────────────────────────────────────────────────────────────

    private static String extractFullClass(String methodFqn) {
        int hash = methodFqn.indexOf('#');
        return hash >= 0 ? methodFqn.substring(0, hash) : methodFqn;
    }

    private static String extractSimpleClass(String fullClass) {
        int dot = fullClass.lastIndexOf('.');
        return dot >= 0 ? fullClass.substring(dot + 1) : fullClass;
    }

    private static String extractPackageOfClass(String fullClass) {
        int dot = fullClass.lastIndexOf('.');
        return dot >= 0 ? fullClass.substring(0, dot) : "";
    }

    private static String extractClassName(String fqn) {
        int hash = fqn.indexOf('#');
        String cls = hash >= 0 ? fqn.substring(0, hash) : fqn;
        int dot = cls.lastIndexOf('.');
        return dot >= 0 ? cls.substring(dot + 1) : cls;
    }

    private static String extractMethodName(String fqn) {
        int hash = fqn.indexOf('#');
        if (hash < 0) return fqn;
        String rest = fqn.substring(hash + 1);
        int paren = rest.indexOf('(');
        return paren >= 0 ? rest.substring(0, paren) : rest;
    }

    private static String extractPackage(String fqn) {
        int hash = fqn.indexOf('#');
        String cls = hash >= 0 ? fqn.substring(0, hash) : fqn;
        int dot = cls.lastIndexOf('.');
        return dot >= 0 ? cls.substring(0, dot) : "";
    }

    private static String inferKind(String fqn) {
        String cls = extractClassName(fqn);
        String method = extractMethodName(fqn);
        if (method.equals(cls) || "<init>".equals(method) || "__init__".equals(method))
            return "constructor";
        return "method";
    }
}
