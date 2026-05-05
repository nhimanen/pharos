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
    private final ObjectMapper mapper;

    // CallGraph cache: project name → (file lastModified, loaded graph)
    private final Map<String, long[]> callGraphMtime = new HashMap<>();
    private final Map<String, CallGraph> callGraphCache = new HashMap<>();

    public WebServer(SearchEngine searchEngine, ProjectRegistry registry,
                     ModuleGraphBuilder moduleGraphBuilder, LuceneIndexer luceneIndexer) {
        this.searchEngine = searchEngine;
        this.registry = registry;
        this.moduleGraphBuilder = moduleGraphBuilder;
        this.luceneIndexer = luceneIndexer;
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

        // API: search
        app.get("/api/search", ctx -> {
            String q = ctx.queryParam("q");
            if (q == null || q.isBlank()) { ctx.json(List.of()); return; }
            String project = ctx.queryParam("project");
            String type = ctx.queryParamAsClass("type", String.class).getOrDefault("hybrid");
            int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(20);
            String docType = ctx.queryParam("docType");
            if ("all".equals(docType)) docType = null;
            if (project != null && project.isBlank()) project = null;
            SearchRequest req = new SearchRequest(q, SearchRequest.SearchType.from(type),
                    project, null, limit, "text", docType);
            ctx.json(searchEngine.search(req).stream()
                    .map(this::resultToMap).collect(Collectors.toList()));
        });

        // API: single method by FQN
        app.get("/api/method", ctx -> {
            String fqn = ctx.queryParam("fqn");
            if (fqn == null) { ctx.status(400).result("fqn required"); return; }
            SearchResult r = searchEngine.getMethodByFqn(fqn);
            if (r == null) { ctx.status(404).result("Not found"); return; }
            ctx.json(methodToMap(r));
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

        app.start(port);
        System.out.printf("Web UI available at http://localhost:%d%n", port);
        log.info("Web server started on port {}", port);
    }

    // ── Graph data builders ────────────────────────────────────────────────────

    private Map<String, Object> buildModuleGraphData() throws Exception {
        ModuleGraph graph = moduleGraphBuilder.load();
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();

        for (ModuleNode n : graph.allNodes()) {
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("id", n.getModuleKey());
            node.put("label", n.getArtifactId() != null ? n.getArtifactId() : n.getModuleKey());
            node.put("status", n.isIndexed() ? "indexed" : "external");
            node.put("projectName", n.getProjectName());
            node.put("version", n.getVersion());
            node.put("groupId", n.getGroupId());
            node.put("depCount", graph.getDependencies(n).size());
            node.put("usedByCount", graph.getDependents(n).size());
            nodes.add(node);
        }

        Set<String> seen = new HashSet<>();
        for (ModuleNode source : graph.allNodes()) {
            for (ModuleNode target : graph.getDependencies(source)) {
                String key = source.getModuleKey() + "->" + target.getModuleKey();
                if (seen.add(key)) {
                    Map<String, Object> edge = new LinkedHashMap<>();
                    edge.put("source", source.getModuleKey());
                    edge.put("target", target.getModuleKey());
                    edges.add(edge);
                }
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodes", nodes);
        result.put("edges", edges);
        return result;
    }

    /** Load CallGraph for a project, using an in-memory cache invalidated by file mtime. */
    private synchronized CallGraph loadCallGraphCached(ProjectMeta meta) throws Exception {
        Path graphFile = Path.of(meta.getIndexPath()).resolve("graph.graphml");
        long mtime = Files.exists(graphFile) ? Files.getLastModifiedTime(graphFile).toMillis() : 0L;
        long[] cached = callGraphMtime.get(meta.getName());
        if (cached != null && cached[0] == mtime) {
            return callGraphCache.get(meta.getName());
        }
        log.info("Loading call graph for '{}' ({} bytes)…", meta.getName(),
                Files.exists(graphFile) ? Files.size(graphFile) : 0);
        long t0 = System.currentTimeMillis();
        CallGraph graph = new CallGraphSerializer().load(graphFile);
        log.info("Loaded call graph for '{}': {} nodes, {} edges in {}ms",
                meta.getName(), graph.nodeCount(), graph.edgeCount(), System.currentTimeMillis() - t0);
        callGraphCache.put(meta.getName(), graph);
        callGraphMtime.put(meta.getName(), new long[]{mtime});
        return graph;
    }

    private Map<String, Object> buildClassGraphData(ProjectMeta meta) throws Exception {
        CallGraph graph = loadCallGraphCached(meta);
        Set<String> knownPkgs = new HashSet<>(meta.getKnownPackages());
        Map<String, String> fqnToFile = buildFqnToFilePath(meta);
        // Map class FQN → filePath (use first method's path as representative)
        Map<String, String> classToFile = new HashMap<>();
        for (Map.Entry<String, String> e : fqnToFile.entrySet())
            classToFile.putIfAbsent(extractFullClass(e.getKey()), e.getValue());

        Set<String> allMethods = new HashSet<>(graph.allMethods());

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
            for (String tgtMethod : graph.getCallees(srcMethod)) {
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
    }

    private Map<String, Object> buildCallGraphData(ProjectMeta meta, int limit, String pkg, String className) throws Exception {
        CallGraph graph = loadCallGraphCached(meta);
        Set<String> knownPkgs = new HashSet<>(meta.getKnownPackages());
        Map<String, String> fqnToFile = buildFqnToFilePath(meta);

        Set<String> methods = new HashSet<>(graph.allMethods());
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
            inDegree.put(m, (int) graph.getCallers(m).stream().filter(filtered::contains).count());
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
            for (String target : graph.getCallees(source)) {
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
        m.put("signature", r.signature());
        m.put("javadoc", r.javadoc());
        m.put("filePath", r.filePath());
        m.put("startLine", r.startLine());
        m.put("endLine", r.endLine());
        m.put("score", r.score());
        m.put("docType", r.docType());
        return m;
    }

    private Map<String, Object> methodToMap(SearchResult r) {
        Map<String, Object> m = resultToMap(r);
        m.put("packageName", r.packageName());
        m.put("className", r.className());
        m.put("methodName", r.methodName());
        m.put("returnType", r.returnType());
        m.put("body", r.body());
        m.put("accessModifier", r.accessModifier());
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
