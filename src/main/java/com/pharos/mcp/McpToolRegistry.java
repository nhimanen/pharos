package com.pharos.mcp;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNodeData;
import com.pharos.search.CallChainResult;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchTrace;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
import com.pharos.search.Snippet;
import com.pharos.search.SourceReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Registry of MCP tools exposed to Claude Code.
 * Each tool maps to a SearchEngine or CallGraph operation.
 *
 * Tools:
 * - search_code:     BM25/vector/hybrid search
 * - get_callers:     who calls a method (graph lookup)
 * - get_callees:     what a method calls (graph lookup)
 * - find_call_path:  shortest path between two methods
 * - list_projects:   show indexed projects
 * - get_method:      retrieve full method body by FQN
 * - get_class:       retrieve full class body (fields, enum constants, annotations) by FQN
 */
public class McpToolRegistry {

    private final SearchEngine searchEngine;
    private final ProjectRegistry registry;
    private final ModuleGraphBuilder moduleGraphBuilder;
    private final ModuleBoundaryAnalyzer boundaryAnalyzer;
    private final ObjectMapper mapper = new ObjectMapper();

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(McpToolRegistry.class);

    /** No-op: ArcadeDB graphs are memory-mapped on disk — no warmup needed. */
    public void warmUp() {}

    public McpToolRegistry(SearchEngine searchEngine, ProjectRegistry registry) {
        this(searchEngine, registry, new ModuleGraphBuilder(registry),
                new ModuleBoundaryAnalyzer(registry, searchEngine));
    }

    public McpToolRegistry(SearchEngine searchEngine, ProjectRegistry registry,
                            ModuleGraphBuilder moduleGraphBuilder,
                            ModuleBoundaryAnalyzer boundaryAnalyzer) {
        this.searchEngine = searchEngine;
        this.registry = registry;
        this.moduleGraphBuilder = moduleGraphBuilder;
        this.boundaryAnalyzer = boundaryAnalyzer;
    }

    public List<Map<String, Object>> getToolDefinitions() {
        return List.of(
                toolDef("search_code",
                        "Search for code across indexed Java projects using BM25, vector, or hybrid search",
                        Map.of(
                                "query", Map.of("type", "string", "description", "Natural language or keyword search query"),
                                "type", Map.of("type", "string", "enum", List.of("auto", "keyword", "vector", "hybrid", "unified"), "description", "Search strategy: auto (default) classifies and picks automatically; keyword for identifiers; vector for semantic; hybrid for two-pass keyword+vector; unified for single-pass BM25 boosted by multi-vector similarity (requires re-indexed project)"),
                                "project", Map.of("type", "string", "description", "Restrict search to a specific project (optional)"),
                                "limit", Map.of("type", "integer", "description", "Maximum results (default: 10)"),
                                "expand", Map.of("type", "boolean", "description", "Append callee methods of top-3 hits as related results (default: false)"),
                                "doc_type", Map.of("type", "string", "enum", List.of("method", "class", "all"), "description", "Filter by document type: method, class, or all (default: all)"),
                                "scope", Map.of("type", "string", "enum", List.of("prod", "test", "docs", "all"), "description", "Filter by source scope: prod (production Java), test (test/benchmark), docs (non-Java files), or all (default: all)"),
                                "snippet_lines", Map.of("type", "integer", "description", "Lines of source to include per result as a snippet centred on the best-matching content (default: 15, min: 5, max: 50)")
                        ),
                        List.of("query")),
                toolDef("find_transitive_callers",
                        "Find all unique methods that transitively call the given method, up to a depth limit. " +
                        "Returns a flat deduplicated set grouped by hop distance — no source bodies. " +
                        "Use for impact analysis: 'what breaks if I change this method?'",
                        Map.of(
                                "fqn",         Map.of("type", "string", "description", "Method FQN to find callers for"),
                                "depth",       Map.of("type", "integer", "description", "Max hops to traverse (default: 5, max: 10)"),
                                "max_callers", Map.of("type", "integer", "description", "Cap on total unique callers returned (default: 2000)")
                        ),
                        List.of("fqn")),
                toolDef("find_usages",
                        "Find all usages of a method, class, field, or annotation across the knowledge graph. " +
                        "Returns callers, subclasses, field readers/writers, annotated elements, and type references.",
                        Map.of(
                                "fqn",  Map.of("type", "string",
                                        "description", "FQN of the method, class, field, or annotation to search for"),
                                "kind", Map.of("type", "string",
                                        "enum", List.of("all", "callers", "subclasses", "field_readers",
                                                "field_writers", "annotated", "type_refs"),
                                        "description", "Which usage kinds to return (default: all)")
                        ),
                        List.of("fqn")),
                toolDef("trace_call_chain",
                        "Traverse the call graph from a method, returning bodies of all visited nodes in one call. " +
                        "Use instead of repeated get_callers/get_callees + get_method calls.",
                        Map.of(
                                "fqn", Map.of("type", "string", "description", "Starting method FQN"),
                                "depth", Map.of("type", "integer", "description", "Hops to traverse (default: 2, max: 4)"),
                                "direction", Map.of("type", "string", "enum", List.of("callees", "callers", "both"),
                                        "description", "callees = what this method calls; callers = who calls this method (default: callees)"),
                                "max_body_chars", Map.of("type", "integer",
                                        "description", "Truncate each method body to this many characters (default: 500, 0 = no truncation)")
                        ),
                        List.of("fqn")),
                toolDef("get_callers",
                        "Find all methods that call the specified method (by fully qualified name)",
                        Map.of("fqn", Map.of("type", "string", "description", "Fully qualified method: com.example.MyClass#myMethod(String,int)")),
                        List.of("fqn")),
                toolDef("get_callees",
                        "Find all methods called by the specified method",
                        Map.of("fqn", Map.of("type", "string", "description", "Fully qualified method: com.example.MyClass#myMethod(String,int)")),
                        List.of("fqn")),
                toolDef("find_call_path",
                        "Find the shortest call chain between two methods",
                        Map.of(
                                "from_fqn", Map.of("type", "string", "description", "Source method FQN"),
                                "to_fqn", Map.of("type", "string", "description", "Target method FQN")
                        ),
                        List.of("from_fqn", "to_fqn")),
                toolDef("list_projects",
                        "List all indexed projects with their statistics",
                        Map.of(),
                        List.of()),
                toolDef("get_method",
                        "Retrieve full method body and context by fully qualified name",
                        Map.of("fqn", Map.of("type", "string", "description", "Fully qualified method: com.example.MyClass#myMethod(String,int)")),
                        List.of("fqn")),
                toolDef("get_methods",
                        "Retrieve full bodies for multiple methods or classes in one call. Prefer this over repeated get_method calls.",
                        Map.of("fqns", Map.of("type", "array", "items", Map.of("type", "string"),
                                "description", "List of fully qualified method or class names")),
                        List.of("fqns")),
                toolDef("get_class",
                        "Retrieve a class by qualified name. With context=true returns full context: body, declared fields, " +
                        "constructors (shows injected dependencies), public methods with their direct callers. " +
                        "Replaces 4-5 separate tool calls when you need to understand a class before modifying it.",
                        Map.of(
                                "fqn",     Map.of("type", "string", "description", "Qualified class name: com.example.MyClass"),
                                "context", Map.of("type", "boolean", "description",
                                        "When true, return full context (fields, constructors, public methods + callers). Default: false (body only)")
                        ),
                        List.of("fqn")),

                // Module-level graph tools
                toolDef("list_modules",
                        "List all known modules in the dependency graph (both indexed and external)",
                        Map.of("status_filter", Map.of("type", "string",
                                "enum", List.of("all", "indexed", "external"),
                                "description", "Filter by status: all (default), indexed, or external")),
                        List.of()),
                toolDef("get_module_deps",
                        "Show direct dependencies and dependents of a module",
                        Map.of(
                                "module", Map.of("type", "string",
                                        "description", "Module key (groupId:artifactId) or project name"),
                                "transitive", Map.of("type", "boolean",
                                        "description", "Include transitive dependencies (default: false)")
                        ),
                        List.of("module")),
                toolDef("find_module_path",
                        "Find the shortest dependency path between two modules",
                        Map.of(
                                "from_module", Map.of("type", "string",
                                        "description", "Source module key or project name"),
                                "to_module", Map.of("type", "string",
                                        "description", "Target module key or project name")
                        ),
                        List.of("from_module", "to_module")),
                toolDef("get_module_boundary",
                        "Get entry points (methods called by other modules) and exit points (cross-module calls) for an indexed module",
                        Map.of("project", Map.of("type", "string",
                                "description", "Project name of an indexed module")),
                        List.of("project"))
        );
    }

    public String call(String toolName, JsonNode args) throws Exception {
        return switch (toolName) {
            case "search_code"       -> callSearchCode(args);
            case "find_transitive_callers" -> callFindTransitiveCallers(args);
            case "find_usages"             -> callFindUsages(args);
            case "trace_call_chain"  -> callTraceCallChain(args);
            case "get_callers"       -> callGetCallers(args);
            case "get_callees"     -> callGetCallees(args);
            case "find_call_path"  -> callFindCallPath(args);
            case "list_projects"   -> callListProjects();
            case "get_method"          -> callGetMethod(args);
            case "get_methods"         -> callGetMethods(args);
            case "get_class"           -> callGetClass(args);
            case "list_modules"        -> callListModules(args);
            case "get_module_deps"     -> callGetModuleDeps(args);
            case "find_module_path"    -> callFindModulePath(args);
            case "get_module_boundary" -> callGetModuleBoundary(args);
            default -> "Unknown tool: " + toolName;
        };
    }

    private String callSearchCode(JsonNode args) throws Exception {
        String query = args.path("query").asText();
        String type = args.path("type").asText("auto");
        String project = args.path("project").asText(null);
        int limit = args.path("limit").asInt(10);
        boolean expand = args.path("expand").asBoolean(false);
        String docTypeRaw = args.path("doc_type").asText("all");
        String docType = "all".equals(docTypeRaw) ? null : docTypeRaw;
        String scopeRaw = args.path("scope").asText("all");
        String scope = "all".equals(scopeRaw) ? null : scopeRaw;
        int snippetLines = Math.min(50, Math.max(5, args.path("snippet_lines").asInt(15)));

        if (project != null && project.isEmpty()) project = null;

        SearchRequest req = new SearchRequest(
                query, SearchRequest.SearchType.from(type), project, null, limit, "text", docType, scope, 0);
        SearchResponse response = searchEngine.searchWithTrace(req, expand);
        List<SearchResult> results = response.results();
        if (log.isDebugEnabled()) {
            log.debug("search_code trace [{}]:\n{}", query, response.trace().format());
        }

        List<SearchResult> decorated = searchEngine.newSnippetDecorator(snippetLines, response)
                .decorate(results, query);

        long primaryCount = decorated.stream().filter(r -> !"related".equals(r.searchType())).count();
        List<Map<String, Object>> resultList = new ArrayList<>();
        for (int i = 0; i < decorated.size(); i++) {
            SearchResult r = decorated.get(i);
            Snippet snip = r.snippet();
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("rank", i + 1);
            item.put("fqn", fqnFromResult(r));
            item.put("label", r.label());
            item.put("project", r.project());
            item.put("docType", r.docType());
            item.put("signature", r.signature());
            item.put("returnType", r.returnType());
            item.put("accessModifier", r.accessModifier());
            item.put("javadoc", r.javadoc());
            item.put("snippet", snip != null ? snip.text() : null);
            item.put("snippetStartLine", snip != null ? snip.startLine() : r.startLine());
            item.put("snippetEndLine", snip != null ? snip.endLine() : r.endLine());
            item.put("startLine", r.startLine());
            item.put("endLine", r.endLine());
            item.put("filePath", r.filePath());
            item.put("score", r.score());
            item.put("searchType", r.searchType());
            item.put("related", "related".equals(r.searchType()));
            resultList.add(item);
        }

        // Build searchMeta block from trace + resolved type
        SearchTrace trace = response.trace();
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("requestedType", type);
        meta.put("resolvedType",  response.resolvedType() != null ? response.resolvedType() : type);
        meta.put("totalMs", trace.totalMs());
        if (!trace.spans().isEmpty()) {
            List<Map<String, Object>> stages = trace.spans().stream().map(s -> {
                Map<String, Object> stage = new LinkedHashMap<>();
                stage.put("name", s.name());
                stage.put("ms",   s.durationMs());
                return stage;
            }).collect(Collectors.toList());
            meta.put("stages", stages);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("query",      query);
        out.put("total",      primaryCount);
        if (expand && results.size() > primaryCount) out.put("related", results.size() - primaryCount);
        out.put("searchMeta", meta);
        out.put("results",    resultList);

        // Include zero-result hints when the search returned nothing
        var suggestions = response.suggestions();
        if (suggestions != null && !suggestions.isEmpty()) {
            Map<String, Object> sugg = new LinkedHashMap<>();
            if (!suggestions.fuzzyMatches().isEmpty()) {
                sugg.put("fuzzyMatches", suggestions.fuzzyMatches().stream().map(fm -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("fqn", fm.fqn()); m.put("label", fm.label());
                    m.put("editDistance", fm.editDistance()); return m;
                }).collect(Collectors.toList()));
            }
            if (!suggestions.tokenMatches().isEmpty()) sugg.put("tokenMatches", suggestions.tokenMatches());
            if (suggestions.filterNote() != null) sugg.put("filterNote", suggestions.filterNote());
            out.put("suggestions", sugg);
        }

        return mapper.writeValueAsString(out);
    }

    private String callFindTransitiveCallers(JsonNode args) throws Exception {
        String fqn       = args.path("fqn").asText();
        int    depth     = Math.min(10, Math.max(1, args.path("depth").asInt(5)));
        int    maxCallers = args.path("max_callers").asInt(2000);

        SearchEngine.TransitiveCallersResult result =
                searchEngine.findTransitiveCallers(fqn, depth, maxCallers);

        // Group callers by depth for easy agent consumption
        Map<Integer, List<String>> byDepth = new java.util.TreeMap<>();
        for (var entry : result.callers()) {
            byDepth.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(entry.getKey());
        }

        List<Map<String, Object>> callerList = result.callers().stream().map(e -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("fqn",   e.getKey());
            m.put("depth", e.getValue());
            return m;
        }).collect(Collectors.toList());

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("root",          result.root());
        out.put("maxDepth",      result.maxDepth());
        out.put("totalCallers",  result.totalCallers());
        out.put("truncated",     result.truncated());
        out.put("byDepth",       byDepth);
        out.put("callers",       callerList);
        return mapper.writeValueAsString(out);
    }

    private String callFindUsages(JsonNode args) throws Exception {
        String fqn  = args.path("fqn").asText();
        String kind = args.path("kind").asText("all");

        SearchEngine.UsageResult u = searchEngine.findUsages(fqn, kind);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("fqn",             u.fqn());
        out.put("callers",         u.callers());
        out.put("subclasses",      u.subclasses());
        out.put("superTypes",      u.superTypes());
        out.put("fieldReaders",    u.fieldReaders());
        out.put("fieldWriters",    u.fieldWriters());
        out.put("annotatedWith",   u.annotatedWith());
        out.put("methodsReturning", u.methodsReturning());
        out.put("methodsTaking",   u.methodsTaking());
        return mapper.writeValueAsString(out);
    }

    private String callTraceCallChain(JsonNode args) throws Exception {
        String fqn         = args.path("fqn").asText();
        int    depth       = Math.min(4, Math.max(1, args.path("depth").asInt(2)));
        String direction   = args.path("direction").asText("callees");
        int    maxBodyChars = args.path("max_body_chars").asInt(500);

        CallChainResult chain = searchEngine.traceCallChain(fqn, depth, direction, maxBodyChars);

        List<Map<String, Object>> nodeList = chain.nodes().stream().map(n -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("fqn",       n.fqn());
            m.put("label",     n.label());
            m.put("depth",     n.depth());
            m.put("signature", n.signature());
            m.put("filePath",  n.filePath());
            m.put("startLine", n.startLine());
            m.put("endLine",   n.endLine());
            m.put("body",      n.body());
            m.put("children",  n.children());
            return m;
        }).collect(Collectors.toList());

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("root",       chain.root());
        out.put("direction",  chain.direction());
        out.put("maxDepth",   chain.maxDepth());
        out.put("totalNodes", chain.totalNodes());
        out.put("truncated",  chain.truncated());
        out.put("nodes",      nodeList);
        return mapper.writeValueAsString(out);
    }

    private String callGetCallers(JsonNode args) throws Exception {
        String fqn = args.path("fqn").asText();
        List<SearchResult> results = searchEngine.findCallers(fqn, null);

        List<Map<String, Object>> callerList = new ArrayList<>();
        if (results.isEmpty()) {
            // Fall back to graph-only lookup (no file/line info available)
            getFromGraph(fqn, true).forEach(callerFqn -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("fqn", callerFqn);
                callerList.add(item);
            });
        } else {
            for (SearchResult r : results) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("fqn", fqnFromResult(r));
                item.put("label", r.label());
                item.put("filePath", r.filePath());
                item.put("startLine", r.startLine());
                callerList.add(item);
            }
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("fqn", fqn);
        out.put("count", callerList.size());
        out.put("callers", callerList);
        return mapper.writeValueAsString(out);
    }

    private String callGetCallees(JsonNode args) throws Exception {
        String fqn = args.path("fqn").asText();
        Set<String> callees = getFromGraph(fqn, false);

        List<Map<String, Object>> calleeList = callees.stream()
                .map(c -> { Map<String, Object> m = new LinkedHashMap<>(); m.put("fqn", c); return m; })
                .collect(Collectors.toList());

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("fqn", fqn);
        out.put("count", calleeList.size());
        out.put("callees", calleeList);
        return mapper.writeValueAsString(out);
    }

    private String callFindCallPath(JsonNode args) throws Exception {
        String from = args.path("from_fqn").asText();
        String to = args.path("to_fqn").asText();

        for (ProjectMeta meta : registry.listAll()) {
            Path dbDir = Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb");
            if (!Files.isDirectory(dbDir)) continue;
            try (CallGraph g = CallGraph.open(dbDir)) {
                List<String> path = g.shortestPath(from, to);
                if (!path.isEmpty()) {
                    List<Map<String, Object>> steps = path.stream()
                            .map(n -> { Map<String, Object> m = new LinkedHashMap<>(); m.put("fqn", n); return m; })
                            .collect(Collectors.toList());
                    Map<String, Object> out = new LinkedHashMap<>();
                    out.put("from", from);
                    out.put("to", to);
                    out.put("hops", path.size() - 1);
                    out.put("path", steps);
                    return mapper.writeValueAsString(out);
                }
            } catch (Exception ignored) {}
        }
        return "No call path found from `" + from + "` to `" + to + "`";
    }

    private String callListProjects() throws Exception {
        List<ProjectMeta> projects = registry.listAll();
        if (projects.isEmpty()) return "No projects indexed yet. Run `pharos index <path>` first.";

        List<Map<String, Object>> projectList = projects.stream().map(p -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", p.getName());
            m.put("methodCount", p.getMethodCount());
            m.put("classCount", p.getClassCount());
            m.put("lastIndexed", p.getLastIndexed() != null ? p.getLastIndexed().toString() : null);
            return m;
        }).collect(Collectors.toList());

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("count", projectList.size());
        out.put("projects", projectList);
        return mapper.writeValueAsString(out);
    }

    private String callGetMethod(JsonNode args) throws Exception {
        String fqn = args.path("fqn").asText();
        Map<String, SearchResult> found = searchEngine.getByFqns(List.of(fqn));
        SearchResult r = found.get(fqn);
        if (r == null && fqn.contains("#") && !fqn.contains("(")) {
            // Partial FQN — parameter types omitted. Try name-only lookup.
            List<SearchResult> candidates = searchEngine.findMethodsByPartialFqn(fqn);
            if (candidates.isEmpty()) return "Method not found: " + fqn;
            if (candidates.size() == 1) return mapper.writeValueAsString(resultToMap(candidates.get(0)));
            String list = candidates.stream()
                    .map(c -> c.id().substring(c.project().length() + 1))
                    .collect(Collectors.joining(", "));
            return "Ambiguous FQN — " + candidates.size() + " overloads found. Include parameter types: " + list;
        }
        if (r == null) return "Method not found: " + fqn;
        return mapper.writeValueAsString(resultToMap(r));
    }

    private String callGetMethods(JsonNode args) throws Exception {
        JsonNode fqnsNode = args.path("fqns");
        if (!fqnsNode.isArray() || fqnsNode.isEmpty()) return "No FQNs provided";

        List<String> fqns = new ArrayList<>();
        fqnsNode.forEach(n -> { if (!n.asText().isBlank()) fqns.add(n.asText()); });

        Map<String, SearchResult> found = searchEngine.getByFqns(fqns);

        List<Map<String, Object>> results = new ArrayList<>();
        List<String> notFound = new ArrayList<>();
        for (String fqn : fqns) {
            SearchResult r = found.get(fqn);
            if (r == null) { notFound.add(fqn); continue; }
            results.add(resultToMap(r));
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("requested", fqns.size());
        out.put("found", results.size());
        out.put("results", results);
        if (!notFound.isEmpty()) out.put("notFound", notFound);
        return mapper.writeValueAsString(out);
    }

    /** Converts a SearchResult to its JSON map representation (used by both get_method and get_methods). */
    private Map<String, Object> resultToMap(SearchResult r) {
        String body = r.body();
        if ("class".equals(r.docType())) {
            String fileBody = SourceReader.readRange(r.filePath(), r.startLine(), r.endLine());
            if (fileBody != null) body = fileBody;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fqn", fqnFromResult(r));
        m.put("label", r.label());
        m.put("project", r.project());
        m.put("docType", r.docType());
        m.put("signature", r.signature());
        m.put("returnType", r.returnType());
        m.put("accessModifier", r.accessModifier());
        m.put("javadoc", r.javadoc());
        m.put("body", body);
        m.put("filePath", r.filePath());
        m.put("startLine", r.startLine());
        m.put("endLine", r.endLine());
        return m;
    }

    private String callGetClass(JsonNode args) throws Exception {
        String fqn     = args.path("fqn").asText();
        boolean withCtx = args.path("context").asBoolean(false);

        if (withCtx) {
            SearchEngine.ClassContext ctx = searchEngine.getClassContext(fqn);
            if (ctx == null) return "Class not found: " + fqn;
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("fqn",       fqnFromResult(ctx.classResult()));
            out.put("label",     ctx.classResult().label());
            out.put("project",   ctx.classResult().project());
            out.put("javadoc",   ctx.classResult().javadoc());
            out.put("body",      ctx.body());
            out.put("filePath",  ctx.classResult().filePath());
            out.put("startLine", ctx.classResult().startLine());
            out.put("endLine",   ctx.classResult().endLine());
            out.put("fields", ctx.fields().stream().map(f -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("fqn", f.fqn()); m.put("name", f.fieldName());
                m.put("type", f.fieldType()); m.put("accessMod", f.accessMod());
                return m;
            }).collect(Collectors.toList()));
            out.put("constructors", ctx.constructors().stream().map(c -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("fqn", fqnFromResult(c)); m.put("signature", c.signature());
                m.put("javadoc", c.javadoc());
                return m;
            }).collect(Collectors.toList()));
            out.put("publicMethods", ctx.publicMethods().stream().map(pm -> {
                String pmFqn = fqnFromResult(pm);
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("fqn", pmFqn); m.put("signature", pm.signature());
                m.put("javadoc", pm.javadoc());
                m.put("callers", ctx.publicMethodCallers().getOrDefault(pmFqn, List.of()));
                return m;
            }).collect(Collectors.toList()));
            return mapper.writeValueAsString(out);
        }

        // Default: body only
        SearchResult result = searchEngine.getClassByFqn(fqn);
        if (result == null) return "Class not found: " + fqn;
        String body = SourceReader.readRange(result.filePath(), result.startLine(), result.endLine());
        if (body == null) {
            body = result.body() != null
                    ? "// source file unavailable; indexed body follows\n" + result.body()
                    : "// source file unavailable";
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("fqn",       fqnFromResult(result));
        out.put("label",     result.label());
        out.put("project",   result.project());
        out.put("javadoc",   result.javadoc());
        out.put("body",      body);
        out.put("filePath",  result.filePath());
        out.put("startLine", result.startLine());
        out.put("endLine",   result.endLine());
        return mapper.writeValueAsString(out);
    }

    private String callListModules(JsonNode args) throws Exception {
        String filter = args.path("status_filter").asText("all");
        try (ModuleGraph graph = moduleGraphBuilder.open()) {
            long total = graph.moduleCount();
            if (total == 0) {
                return "No modules registered yet. Run `pharos index` on a Maven project first.";
            }

            List<ModuleNodeData> allNodes = graph.allModules().collect(Collectors.toList());
            long indexed  = allNodes.stream().filter(ModuleNodeData::isIndexed).count();

            List<Map<String, Object>> moduleList = allNodes.stream()
                    .filter(n -> switch (filter) {
                        case "indexed"  -> n.isIndexed();
                        case "external" -> !n.isIndexed();
                        default         -> true;
                    })
                    .sorted(Comparator.comparing(ModuleNodeData::moduleKey))
                    .map(n -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("moduleKey", n.moduleKey());
                        m.put("status", n.isIndexed() ? "indexed" : "external");
                        m.put("projectName", n.isIndexed() ? n.projectName() : null);
                        m.put("version", n.version());
                        m.put("dependencyCount", graph.dependencies(n.moduleKey()).size());
                        m.put("dependentCount", graph.dependents(n.moduleKey()).size());
                        return m;
                    })
                    .collect(Collectors.toList());

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("total", total);
            out.put("indexed", indexed);
            out.put("external", total - indexed);
            out.put("edgeCount", graph.dependencyCount());
            out.put("modules", moduleList);
            return mapper.writeValueAsString(out);
        }
    }

    private String callGetModuleDeps(JsonNode args) throws Exception {
        String moduleRef  = args.path("module").asText();
        boolean transitive = args.path("transitive").asBoolean(false);

        try (ModuleGraph graph = moduleGraphBuilder.open()) {
            ModuleNodeData node = resolveModule(graph, moduleRef);
            if (node == null) {
                return "Module not found: `" + moduleRef + "`. Use `list_modules` to see available modules.";
            }

            Set<ModuleNodeData> deps = transitive
                    ? bfsModules(graph, node, true)
                    : graph.dependencies(node.moduleKey());
            Set<ModuleNodeData> dependents = transitive
                    ? bfsModules(graph, node, false)
                    : graph.dependents(node.moduleKey());

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("moduleKey", node.moduleKey());
            out.put("status", node.isIndexed() ? "indexed" : "external");
            out.put("projectName", node.isIndexed() ? node.projectName() : null);
            out.put("version", node.version());
            out.put("transitive", transitive);
            out.put("dependencies", deps.stream()
                    .sorted(Comparator.comparing(ModuleNodeData::moduleKey))
                    .map(d -> { Map<String, Object> m = new LinkedHashMap<>();
                                m.put("moduleKey", d.moduleKey());
                                m.put("status", d.isIndexed() ? "indexed" : "external");
                                return m; })
                    .collect(Collectors.toList()));
            out.put("dependents", dependents.stream()
                    .sorted(Comparator.comparing(ModuleNodeData::moduleKey))
                    .map(d -> { Map<String, Object> m = new LinkedHashMap<>();
                                m.put("moduleKey", d.moduleKey());
                                m.put("status", d.isIndexed() ? "indexed" : "external");
                                return m; })
                    .collect(Collectors.toList()));
            return mapper.writeValueAsString(out);
        }
    }

    private String callFindModulePath(JsonNode args) throws Exception {
        String fromRef = args.path("from_module").asText();
        String toRef   = args.path("to_module").asText();

        try (ModuleGraph graph = moduleGraphBuilder.open()) {
            ModuleNodeData fromNode = resolveModule(graph, fromRef);
            ModuleNodeData toNode   = resolveModule(graph, toRef);
            if (fromNode == null) return "Module not found: `" + fromRef + "`";
            if (toNode   == null) return "Module not found: `" + toRef + "`";

            List<ModuleNodeData> path = graph.shortestPath(fromNode.moduleKey(), toNode.moduleKey());
            if (path.isEmpty()) {
                return "No dependency path from `" + fromRef + "` to `" + toRef + "`";
            }

            List<Map<String, Object>> steps = path.stream().map(n -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("moduleKey", n.moduleKey());
                m.put("status", n.isIndexed() ? "indexed" : "external");
                m.put("projectName", n.isIndexed() ? n.projectName() : null);
                return m;
            }).collect(Collectors.toList());

            Map<String, Object> out = new LinkedHashMap<>();
            out.put("from", fromRef);
            out.put("to", toRef);
            out.put("hops", path.size() - 1);
            out.put("path", steps);
            return mapper.writeValueAsString(out);
        }
    }

    private String callGetModuleBoundary(JsonNode args) throws Exception {
        String project = args.path("project").asText();
        ModuleBoundaryAnalyzer.BoundaryResult result = boundaryAnalyzer.analyze(project);

        List<Map<String, Object>> entryPoints = result.entryPoints().stream()
                .map(fqn -> { Map<String, Object> m = new LinkedHashMap<>(); m.put("fqn", fqn); return m; })
                .collect(Collectors.toList());

        List<Map<String, Object>> exitPoints = result.exitPoints().stream().limit(30)
                .map(ep -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("callerFqn", ep.callerFqn());
                    m.put("calleeSimpleName", ep.calleeSimpleName());
                    m.put("line", ep.line());
                    return m;
                })
                .collect(Collectors.toList());

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("project", project);
        out.put("entryPointCount", result.entryPoints().size());
        out.put("entryPoints", entryPoints);
        out.put("exitPointCount", result.exitPoints().size());
        out.put("exitPoints", exitPoints);
        if (result.exitPoints().size() > 30) out.put("exitPointsTruncated", true);
        return mapper.writeValueAsString(out);
    }

    private static ModuleNodeData resolveModule(ModuleGraph graph, String ref) {
        Optional<ModuleNodeData> n = graph.findByKey(ref);
        if (n.isPresent()) return n.get();
        return graph.findByProjectName(ref).orElse(null);
    }

    private static Set<ModuleNodeData> bfsModules(ModuleGraph graph,
                                                    ModuleNodeData start, boolean outbound) {
        Set<ModuleNodeData> visited = new LinkedHashSet<>();
        Deque<ModuleNodeData> queue = new ArrayDeque<>();
        queue.add(start);
        while (!queue.isEmpty()) {
            ModuleNodeData cur = queue.pop();
            Set<ModuleNodeData> next = outbound
                    ? graph.dependencies(cur.moduleKey()) : graph.dependents(cur.moduleKey());
            for (ModuleNodeData n : next) {
                if (visited.add(n)) queue.add(n);
            }
        }
        visited.remove(start);
        return visited;
    }

    /** Extracts the bare FQN from a SearchResult (strips the "project:" prefix from id). */
    private static String fqnFromResult(SearchResult r) {
        String id = r.id();
        String prefix = r.project() + ":";
        return id.startsWith(prefix) ? id.substring(prefix.length()) : id;
    }

    private Set<String> getFromGraph(String fqn, boolean callers) {
        Set<String> result = new LinkedHashSet<>();
        for (ProjectMeta meta : registry.listAll()) {
            Path dbDir = Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb");
            if (!Files.isDirectory(dbDir)) continue;
            try (CallGraph graph = CallGraph.open(dbDir)) {
                result.addAll(callers ? graph.callers(fqn) : graph.callees(fqn));
            } catch (Exception ignored) {}
        }
        return result;
    }

    private Map<String, Object> toolDef(String name, String description,
                                          Map<String, Object> properties, List<String> required) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", properties);
        if (!required.isEmpty()) schema.put("required", required);

        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", name);
        tool.put("description", description);
        tool.put("inputSchema", schema);
        return tool;
    }
}
