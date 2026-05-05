package com.pharos.mcp;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CallGraphSerializer;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNode;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
 */
public class McpToolRegistry {

    private final SearchEngine searchEngine;
    private final ProjectRegistry registry;
    private final ModuleGraphBuilder moduleGraphBuilder;
    private final ModuleBoundaryAnalyzer boundaryAnalyzer;
    private final ObjectMapper mapper = new ObjectMapper();

    /** Cache: graphml path -> (lastModified, CallGraph). Avoids re-parsing large graph files per call. */
    private final ConcurrentHashMap<Path, long[]> graphMtimeCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Path, CallGraph> graphCache = new ConcurrentHashMap<>();

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(McpToolRegistry.class);

    /** Eagerly load all project call graphs into cache in a background thread. */
    public void warmUp() {
        Thread t = new Thread(() -> {
            for (ProjectMeta meta : registry.listAll()) {
                Path graphFile = Path.of(meta.getIndexPath()).resolve("graph.graphml");
                try {
                    long start = System.currentTimeMillis();
                    loadGraphCached(graphFile);
                    log.info("Warmed up graph for '{}' in {}ms", meta.getName(), System.currentTimeMillis() - start);
                } catch (Exception e) {
                    log.warn("Failed to warm up graph for '{}': {}", meta.getName(), e.getMessage());
                }
            }
        }, "pharos-graph-warmup");
        t.setDaemon(true);
        t.start();
    }

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
                                "type", Map.of("type", "string", "enum", List.of("keyword", "vector", "hybrid"), "description", "Search strategy (default: hybrid)"),
                                "project", Map.of("type", "string", "description", "Restrict search to a specific project (optional)"),
                                "limit", Map.of("type", "integer", "description", "Maximum results (default: 10)"),
                                "expand", Map.of("type", "boolean", "description", "Append callee methods of top-3 hits as related results (default: false)"),
                                "doc_type", Map.of("type", "string", "enum", List.of("method", "class", "all"), "description", "Filter by document type: method, class, or all (default: all)")
                        ),
                        List.of("query")),
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
            case "search_code"     -> callSearchCode(args);
            case "get_callers"     -> callGetCallers(args);
            case "get_callees"     -> callGetCallees(args);
            case "find_call_path"  -> callFindCallPath(args);
            case "list_projects"   -> callListProjects();
            case "get_method"          -> callGetMethod(args);
            case "list_modules"        -> callListModules(args);
            case "get_module_deps"     -> callGetModuleDeps(args);
            case "find_module_path"    -> callFindModulePath(args);
            case "get_module_boundary" -> callGetModuleBoundary(args);
            default -> "Unknown tool: " + toolName;
        };
    }

    private String callSearchCode(JsonNode args) throws Exception {
        String query = args.path("query").asText();
        String type = args.path("type").asText("hybrid");
        String project = args.path("project").asText(null);
        int limit = args.path("limit").asInt(10);
        boolean expand = args.path("expand").asBoolean(false);
        String docTypeRaw = args.path("doc_type").asText("all");
        String docType = "all".equals(docTypeRaw) ? null : docTypeRaw;

        if (project != null && project.isEmpty()) project = null;

        SearchRequest req = new SearchRequest(
                query, SearchRequest.SearchType.from(type), project, null, limit, "text", docType);
        SearchResponse response = searchEngine.searchWithTrace(req, expand);
        List<SearchResult> results = response.results();
        if (log.isDebugEnabled()) {
            log.debug("search_code trace [{}]:\n{}", query, response.trace().format());
        }

        if (results.isEmpty()) return "No results found for: " + query;

        long primaryCount = results.stream().filter(r -> !"related".equals(r.searchType())).count();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Found %d result(s) for \"%s\"%s:\n\n",
                primaryCount, query,
                expand && results.size() > primaryCount
                        ? " + " + (results.size() - primaryCount) + " related"
                        : ""));
        for (int i = 0; i < results.size(); i++) {
            SearchResult r = results.get(i);
            boolean related = "related".equals(r.searchType());
            sb.append(String.format("%d. **%s** [%s]%s\n",
                    i + 1, r.label(), r.project(), related ? " *(related)*" : ""));
            sb.append(String.format("   `%s`\n", r.signature()));
            if (r.javadoc() != null && !r.javadoc().isBlank()) {
                String javadoc = r.javadoc().replaceAll("\\s+", " ").trim();
                if (javadoc.length() > 150) javadoc = javadoc.substring(0, 150) + "...";
                sb.append(String.format("   *%s*\n", javadoc));
            }
            sb.append(String.format("   `%s:%d-%d`\n\n", r.filePath(), r.startLine(), r.endLine()));
        }
        return sb.toString();
    }

    private String callGetCallers(JsonNode args) throws Exception {
        String fqn = args.path("fqn").asText();
        List<SearchResult> results = searchEngine.findCallers(fqn, null);

        if (results.isEmpty()) {
            // Try graph lookup
            Set<String> callers = getFromGraph(fqn, true);
            if (callers.isEmpty()) return "No callers found for: " + fqn;
            return "Callers of `" + fqn + "`:\n" + callers.stream()
                    .map(c -> "- `" + c + "`")
                    .collect(Collectors.joining("\n"));
        }

        return "Callers of `" + fqn + "` (" + results.size() + "):\n" +
                results.stream()
                        .map(r -> String.format("- `%s` in `%s:%d`", r.label(), r.filePath(), r.startLine()))
                        .collect(Collectors.joining("\n"));
    }

    private String callGetCallees(JsonNode args) throws Exception {
        String fqn = args.path("fqn").asText();
        Set<String> callees = getFromGraph(fqn, false);
        if (callees.isEmpty()) return "No callees found for: " + fqn;
        return "Methods called by `" + fqn + "` (" + callees.size() + "):\n" +
                callees.stream().map(c -> "- `" + c + "`").collect(Collectors.joining("\n"));
    }

    private String callFindCallPath(JsonNode args) throws Exception {
        String from = args.path("from_fqn").asText();
        String to = args.path("to_fqn").asText();

        // Load merged graph from all projects
        CallGraph merged = new CallGraph();
        for (ProjectMeta meta : registry.listAll()) {
            Path graphFile = Path.of(meta.getIndexPath()).resolve("graph.graphml");
            try {
                CallGraph g = loadGraphCached(graphFile);
                g.allMethods().forEach(merged::addMethod);
                g.getInternalGraph().edgeSet().forEach(edge -> {
                    merged.addCall(g.getInternalGraph().getEdgeSource(edge),
                            g.getInternalGraph().getEdgeTarget(edge));
                });
            } catch (Exception ignored) {}
        }

        List<String> path = merged.findPath(from, to);
        if (path.isEmpty()) return "No call path found from `" + from + "` to `" + to + "`";

        return "Call path (" + (path.size() - 1) + " hops):\n" +
                path.stream().map(n -> "→ `" + n + "`").collect(Collectors.joining("\n"));
    }

    private String callListProjects() {
        List<ProjectMeta> projects = registry.listAll();
        if (projects.isEmpty()) return "No projects indexed yet. Run `pharos index <path>` first.";
        return "Indexed projects:\n" + projects.stream()
                .map(p -> String.format("- **%s**: %d methods, %d classes (last indexed: %s)",
                        p.getName(), p.getMethodCount(), p.getClassCount(), p.getLastIndexed()))
                .collect(Collectors.joining("\n"));
    }

    private String callGetMethod(JsonNode args) throws Exception {
        String fqn = args.path("fqn").asText();
        SearchResult result = searchEngine.getMethodByFqn(fqn);
        if (result == null) return "Method not found: " + fqn;
        return String.format("**%s** [%s]\n\n```java\n%s\n```\n\n`%s:%d-%d`",
                result.label(), result.project(), result.body(),
                result.filePath(), result.startLine(), result.endLine());
    }

    private String callListModules(JsonNode args) throws Exception {
        String filter = args.path("status_filter").asText("all");
        ModuleGraph graph = moduleGraphBuilder.load();
        if (graph.nodeCount() == 0) {
            return "No modules registered yet. Run `pharos index` on a Maven project first.";
        }

        StringBuilder sb = new StringBuilder();
        long indexed  = graph.allNodes().stream().filter(ModuleNode::isIndexed).count();
        long external = graph.nodeCount() - indexed;
        sb.append(String.format("Module graph: **%d** nodes (%d indexed, %d external), **%d** edges\n\n",
                graph.nodeCount(), indexed, external, graph.edgeCount()));

        graph.allNodes().stream()
                .filter(n -> switch (filter) {
                    case "indexed"  -> n.isIndexed();
                    case "external" -> !n.isIndexed();
                    default         -> true;
                })
                .sorted((a, b) -> a.getModuleKey().compareTo(b.getModuleKey()))
                .forEach(n -> {
                    int deps   = graph.getDependencies(n).size();
                    int usedBy = graph.getDependents(n).size();
                    String status = n.isIndexed()
                            ? "✓ indexed [" + n.getProjectName() + "]"
                            : "⊘ external";
                    sb.append(String.format("- `%s` %s — deps: %d, used-by: %d, version: %s\n",
                            n.getModuleKey(), status, deps, usedBy, n.getVersion()));
                });
        return sb.toString();
    }

    private String callGetModuleDeps(JsonNode args) throws Exception {
        String moduleRef  = args.path("module").asText();
        boolean transitive = args.path("transitive").asBoolean(false);

        ModuleGraph graph = moduleGraphBuilder.load();
        ModuleNode node = resolveModule(graph, moduleRef);
        if (node == null) {
            return "Module not found: `" + moduleRef + "`. Use `list_modules` to see available modules.";
        }

        java.util.Set<ModuleNode> deps = transitive
                ? bfsModules(graph, node, true)
                : graph.getDependencies(node);
        java.util.Set<ModuleNode> dependents = transitive
                ? bfsModules(graph, node, false)
                : graph.getDependents(node);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("**%s** [%s] version: %s\n\n",
                node.getModuleKey(),
                node.isIndexed() ? "indexed/" + node.getProjectName() : "external",
                node.getVersion()));

        sb.append(String.format("**Dependencies (%s%d):**\n",
                transitive ? "transitive, " : "", deps.size()));
        deps.stream().sorted((a,b) -> a.getModuleKey().compareTo(b.getModuleKey()))
                .forEach(d -> sb.append(String.format("- → `%s` [%s]\n",
                        d.getModuleKey(), d.isIndexed() ? "indexed" : "external")));

        sb.append(String.format("\n**Used by (%s%d):**\n",
                transitive ? "transitive, " : "", dependents.size()));
        dependents.stream().sorted((a,b) -> a.getModuleKey().compareTo(b.getModuleKey()))
                .forEach(d -> sb.append(String.format("- ← `%s` [%s]\n",
                        d.getModuleKey(), d.isIndexed() ? "indexed" : "external")));
        return sb.toString();
    }

    private String callFindModulePath(JsonNode args) throws Exception {
        String fromRef = args.path("from_module").asText();
        String toRef   = args.path("to_module").asText();

        ModuleGraph graph = moduleGraphBuilder.load();
        ModuleNode fromNode = resolveModule(graph, fromRef);
        ModuleNode toNode   = resolveModule(graph, toRef);
        if (fromNode == null) return "Module not found: `" + fromRef + "`";
        if (toNode   == null) return "Module not found: `" + toRef + "`";

        java.util.List<ModuleNode> path =
                graph.findPath(fromNode.getModuleKey(), toNode.getModuleKey());
        if (path.isEmpty()) {
            return "No dependency path from `" + fromRef + "` to `" + toRef + "`";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Dependency path (%d hop%s):\n\n",
                path.size() - 1, path.size() - 1 == 1 ? "" : "s"));
        for (int i = 0; i < path.size(); i++) {
            ModuleNode n = path.get(i);
            String marker = i == 0 ? "**START**" : i == path.size()-1 ? "**END**" : "→";
            sb.append(String.format("%s `%s` [%s]\n", marker, n.getModuleKey(),
                    n.isIndexed() ? "indexed/" + n.getProjectName() : "external"));
        }
        return sb.toString();
    }

    private String callGetModuleBoundary(JsonNode args) throws Exception {
        String project = args.path("project").asText();
        ModuleBoundaryAnalyzer.BoundaryResult result = boundaryAnalyzer.analyze(project);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("**Module boundary for `%s`**\n\n", project));

        sb.append(String.format("**Entry points** (methods called by other linked modules): %d\n",
                result.entryPoints().size()));
        if (result.entryPoints().isEmpty()) {
            sb.append("- *(none — no linked projects have been indexed yet)*\n");
        } else {
            result.entryPoints().forEach(fqn ->
                    sb.append(String.format("- `%s`\n", fqn)));
        }

        sb.append(String.format("\n**Exit points** (unresolved external calls): %d\n",
                result.exitPoints().size()));
        if (result.exitPoints().isEmpty()) {
            sb.append("- *(none recorded)*\n");
        } else {
            result.exitPoints().stream().limit(30).forEach(ep ->
                    sb.append(String.format("- `%s` → `%s` (line %d)\n",
                            ep.callerFqn(), ep.calleeSimpleName(), ep.line())));
            if (result.exitPoints().size() > 30) {
                sb.append(String.format("- *... and %d more*\n", result.exitPoints().size() - 30));
            }
        }
        return sb.toString();
    }

    private static ModuleNode resolveModule(ModuleGraph graph, String ref) {
        ModuleNode n = graph.findByKey(ref);
        if (n != null) return n;
        return graph.findByProjectName(ref).orElse(null);
    }

    private static java.util.Set<ModuleNode> bfsModules(ModuleGraph graph,
                                                          ModuleNode start, boolean outbound) {
        java.util.Set<ModuleNode> visited = new java.util.LinkedHashSet<>();
        java.util.Deque<ModuleNode> queue = new java.util.ArrayDeque<>();
        queue.add(start);
        while (!queue.isEmpty()) {
            ModuleNode cur = queue.pop();
            java.util.Set<ModuleNode> next = outbound
                    ? graph.getDependencies(cur) : graph.getDependents(cur);
            for (ModuleNode n : next) {
                if (visited.add(n)) queue.add(n);
            }
        }
        visited.remove(start);
        return visited;
    }

    private CallGraph loadGraphCached(Path graphFile) throws Exception {
        long mtime = Files.exists(graphFile) ? Files.getLastModifiedTime(graphFile).toMillis() : 0;
        long[] cached = graphMtimeCache.get(graphFile);
        if (cached != null && cached[0] == mtime) {
            return graphCache.get(graphFile);
        }
        CallGraph graph = new CallGraphSerializer().load(graphFile);
        graphMtimeCache.put(graphFile, new long[]{mtime});
        graphCache.put(graphFile, graph);
        return graph;
    }

    private Set<String> getFromGraph(String fqn, boolean callers) {
        Set<String> result = new LinkedHashSet<>();
        for (ProjectMeta meta : registry.listAll()) {
            Path graphFile = Path.of(meta.getIndexPath()).resolve("graph.graphml");
            try {
                CallGraph graph = loadGraphCached(graphFile);
                result.addAll(callers ? graph.getCallers(fqn) : graph.getCallees(fqn));
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
