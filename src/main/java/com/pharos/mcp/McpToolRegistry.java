package com.pharos.mcp;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.graph.ModuleNodeData;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
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
                                "type", Map.of("type", "string", "enum", List.of("keyword", "vector", "hybrid"), "description", "Search strategy (default: hybrid)"),
                                "project", Map.of("type", "string", "description", "Restrict search to a specific project (optional)"),
                                "limit", Map.of("type", "integer", "description", "Maximum results (default: 10)"),
                                "expand", Map.of("type", "boolean", "description", "Append callee methods of top-3 hits as related results (default: false)"),
                                "doc_type", Map.of("type", "string", "enum", List.of("method", "class", "all"), "description", "Filter by document type: method, class, or all (default: all)"),
                                "scope", Map.of("type", "string", "enum", List.of("prod", "test", "docs", "all"), "description", "Filter by source scope: prod (production Java), test (test/benchmark), docs (non-Java files), or all (default: all)")
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
                toolDef("get_class",
                        "Retrieve full class body (fields, enum constants, class-level annotations, nested types) by qualified class name",
                        Map.of("fqn", Map.of("type", "string", "description", "Qualified class name: com.example.MyClass")),
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
        String type = args.path("type").asText("hybrid");
        String project = args.path("project").asText(null);
        int limit = args.path("limit").asInt(10);
        boolean expand = args.path("expand").asBoolean(false);
        String docTypeRaw = args.path("doc_type").asText("all");
        String docType = "all".equals(docTypeRaw) ? null : docTypeRaw;
        String scopeRaw = args.path("scope").asText("all");
        String scope = "all".equals(scopeRaw) ? null : scopeRaw;

        if (project != null && project.isEmpty()) project = null;

        SearchRequest req = new SearchRequest(
                query, SearchRequest.SearchType.from(type), project, null, limit, "text", docType, scope, 0);
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

        // Search each per-project graph for the path
        for (ProjectMeta meta : registry.listAll()) {
            Path dbDir = Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb");
            if (!Files.isDirectory(dbDir)) continue;
            try (CallGraph g = CallGraph.open(dbDir)) {
                List<String> path = g.shortestPath(from, to);
                if (!path.isEmpty()) {
                    return "Call path (" + (path.size() - 1) + " hops):\n" +
                            path.stream().map(n -> "→ `" + n + "`").collect(Collectors.joining("\n"));
                }
            } catch (Exception ignored) {}
        }
        return "No call path found from `" + from + "` to `" + to + "`";
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

    private String callGetClass(JsonNode args) throws Exception {
        String fqn = args.path("fqn").asText();
        SearchResult result = searchEngine.getClassByFqn(fqn);
        if (result == null) return "Class not found: " + fqn;
        String body = SourceReader.readRange(result.filePath(), result.startLine(), result.endLine());
        if (body == null) {
            // Fallback: file not readable — emit the synthesized body the index stored
            body = result.body() != null
                    ? "// source file unavailable; indexed body follows\n" + result.body()
                    : "// source file unavailable";
        }
        return String.format("**%s** [%s]\n\n```java\n%s\n```\n\n`%s:%d-%d`",
                result.label(), result.project(), body,
                result.filePath(), result.startLine(), result.endLine());
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
            long external = total - indexed;
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Module graph: **%d** nodes (%d indexed, %d external), **%d** edges\n\n",
                    total, indexed, external, graph.dependencyCount()));

            allNodes.stream()
                    .filter(n -> switch (filter) {
                        case "indexed"  -> n.isIndexed();
                        case "external" -> !n.isIndexed();
                        default         -> true;
                    })
                    .sorted(Comparator.comparing(ModuleNodeData::moduleKey))
                    .forEach(n -> {
                        int deps   = graph.dependencies(n.moduleKey()).size();
                        int usedBy = graph.dependents(n.moduleKey()).size();
                        String status = n.isIndexed()
                                ? "✓ indexed [" + n.projectName() + "]"
                                : "⊘ external";
                        sb.append(String.format("- `%s` %s — deps: %d, used-by: %d, version: %s\n",
                                n.moduleKey(), status, deps, usedBy, n.version()));
                    });
            return sb.toString();
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

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("**%s** [%s] version: %s\n\n",
                    node.moduleKey(),
                    node.isIndexed() ? "indexed/" + node.projectName() : "external",
                    node.version()));

            sb.append(String.format("**Dependencies (%s%d):**\n",
                    transitive ? "transitive, " : "", deps.size()));
            deps.stream().sorted(Comparator.comparing(ModuleNodeData::moduleKey))
                    .forEach(d -> sb.append(String.format("- → `%s` [%s]\n",
                            d.moduleKey(), d.isIndexed() ? "indexed" : "external")));

            sb.append(String.format("\n**Used by (%s%d):**\n",
                    transitive ? "transitive, " : "", dependents.size()));
            dependents.stream().sorted(Comparator.comparing(ModuleNodeData::moduleKey))
                    .forEach(d -> sb.append(String.format("- ← `%s` [%s]\n",
                            d.moduleKey(), d.isIndexed() ? "indexed" : "external")));
            return sb.toString();
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

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Dependency path (%d hop%s):\n\n",
                    path.size() - 1, path.size() - 1 == 1 ? "" : "s"));
            for (int i = 0; i < path.size(); i++) {
                ModuleNodeData n = path.get(i);
                String marker = i == 0 ? "**START**" : i == path.size()-1 ? "**END**" : "→";
                sb.append(String.format("%s `%s` [%s]\n", marker, n.moduleKey(),
                        n.isIndexed() ? "indexed/" + n.projectName() : "external"));
            }
            return sb.toString();
        }
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
