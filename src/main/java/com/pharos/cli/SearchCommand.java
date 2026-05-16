package com.pharos.cli;

import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
import com.pharos.search.Snippet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import picocli.CommandLine.*;

import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "search",
        description = "Search for code by keyword, vector, or hybrid",
        mixinStandardHelpOptions = true
)
public class SearchCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Search query")
    private String query;

    @Option(names = {"--project", "-p"},
            description = "Restrict search to a specific project")
    private String project;

    @Option(names = {"--type", "-t"},
            description = "Search type: auto | keyword | vector | hybrid | unified (default: auto)",
            defaultValue = "auto")
    private String type;

    @Option(names = {"--limit", "-n"},
            description = "Maximum number of results (default: 20)",
            defaultValue = "20")
    private int limit;

    @Option(names = {"--format", "-f"},
            description = "Output format: text | json (default: text)",
            defaultValue = "text")
    private String format;

    @Option(names = {"--expand"},
            description = "Expand results with callee methods from top-3 hits (neighborhood expansion)")
    private boolean expand = false;

    @Option(names = {"--doc-type"},
            description = "Filter by document type: method | class | chunk | document | all (default: all)",
            defaultValue = "all")
    private String docType;

    @Option(names = {"--scope"},
            description = "Filter by source scope: prod | test | docs | all (default: all)",
            defaultValue = "all")
    private String scope;

    @Option(names = {"--trace"},
            description = "Print pipeline trace (resolved type, stage timings) to stderr after results")
    private boolean trace = false;

    @Option(names = {"--snippet-lines"},
            description = "Lines of source to include as snippet centred on best keyword/vector match (default: 15)",
            defaultValue = "15")
    private int snippetLines = 15;

    private final SearchEngine searchEngine;

    public SearchCommand(SearchEngine searchEngine) {
        this.searchEngine = searchEngine;
    }

    @Override
    public Integer call() {
        try {
            String resolvedDocType = "all".equalsIgnoreCase(docType) ? null : docType;
            String resolvedScope   = "all".equalsIgnoreCase(scope)   ? null : scope;
            SearchRequest req = new SearchRequest(
                    query, SearchRequest.SearchType.from(type),
                    project, null, limit, format, resolvedDocType, resolvedScope, 0);

            SearchResponse response = searchEngine.searchWithTrace(req, expand);
            List<SearchResult> results = searchEngine
                    .newSnippetDecorator(snippetLines, response)
                    .decorate(response.results(), query);
            long elapsedMs = response.trace().totalMs();

            if (results.isEmpty()) {
                System.out.printf("No results found for: %s  (%.3fs)%n", query, elapsedMs / 1000.0);
                return 0;
            }

            if ("json".equals(format)) {
                printJson(results, elapsedMs, response);
            } else {
                printText(results, elapsedMs);
            }
            if (trace) {
                String requested = type;
                String resolved  = response.resolvedType() != null ? response.resolvedType() : type;
                String typeInfo  = resolved.equals(requested) ? requested : requested + " → " + resolved;
                System.err.printf("%n[trace] type=%-20s total=%dms%n", typeInfo, elapsedMs);
                for (var span : response.trace().spans()) {
                    System.err.printf("[trace]   %-28s %dms%n", span.name(), span.durationMs());
                }
            }
            return 0;
        } catch (Exception e) {
            System.err.println("Search error: " + e.getMessage());
            if (Boolean.getBoolean("pharos.verbose")) e.printStackTrace();
            return 1;
        }
    }

    private void printText(List<SearchResult> results, long elapsedMs) {
        long primaryCount = results.stream().filter(r -> !"related".equals(r.searchType())).count();
        System.out.printf("Found %d result(s) for \"%s\" [%s]%s  (%.3fs):%n%n",
                primaryCount, query, type,
                expand && results.size() > primaryCount
                        ? " + " + (results.size() - primaryCount) + " related"
                        : "",
                elapsedMs / 1000.0);
        for (int i = 0; i < results.size(); i++) {
            SearchResult r = results.get(i);
            String tag = "related".equals(r.searchType()) ? " ~related" : "";
            boolean isClass = "class".equals(r.docType()) || "document".equals(r.docType());
            boolean isChunk = "chunk".equals(r.docType());
            String typeTag = isClass ? "  [" + (r.docType() != null ? r.docType().toUpperCase() : "CLASS") + "]"
                    : isChunk ? "  [CHUNK]" : "";
            System.out.printf("%d. [%s%s] %s%s%n",
                    i + 1, r.project(), tag, r.label(), typeTag);
            if (isClass) {
                // Class/document result: show kind and description
                String kind = r.signature() != null ? r.signature() : r.docType();
                System.out.printf("   %s%n", kind.isBlank() ? r.qualifiedClassName() : kind);
            } else if (isChunk) {
                // Chunk result: show breadcrumb (stored in signature), then a content snippet
                if (r.signature() != null && !r.signature().isBlank()) {
                    System.out.printf("   %s%n", r.signature());
                }
                if (r.body() != null && !r.body().isBlank()) {
                    String snippet = r.body().replaceAll("\\s+", " ").trim();
                    if (snippet.length() > 200) snippet = snippet.substring(0, 197) + "...";
                    System.out.printf("   %s%n", snippet);
                }
            } else {
                System.out.printf("   %s%n", r.signature());
            }
            if (!isChunk && r.javadoc() != null && !r.javadoc().isBlank()) {
                String javadoc = r.javadoc().length() > 120 ? r.javadoc().substring(0, 120) + "..." : r.javadoc();
                System.out.printf("   /** %s */%n", javadoc.replaceAll("\\s+", " ").trim());
            }
            // Show snippet location — prefer precise snippet range over full method range
            Snippet snip = r.snippet();
            if (snip != null && snip.text() != null && !snip.text().isBlank()) {
                System.out.printf("   %s:%d-%d (score: %.4f)%n",
                        r.filePath(), snip.startLine(), snip.endLine(), r.score());
                for (String line : snip.text().split("\n")) {
                    System.out.printf("   %s%n", line);
                }
                System.out.println();
            } else {
                System.out.printf("   %s:%d-%d (score: %.4f)%n%n",
                        r.filePath(), r.startLine(), r.endLine(), r.score());
            }
            boolean showBody = false;
            if (showBody && r.body() != null) {
                System.out.println("   " + r.body().replace("\n", "\n   "));
                System.out.println();
            }
        }
    }

    private void printJson(List<SearchResult> results, long elapsedMs, SearchResponse response) throws Exception {
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        long primaryCount = results.stream().filter(r -> !"related".equals(r.searchType())).count();
        var envelope = mapper.createObjectNode()
                .put("total", primaryCount)
                .put("latencyMs", elapsedMs);
        if (trace) {
            String resolved = response.resolvedType() != null ? response.resolvedType() : type;
            var meta = envelope.putObject("searchMeta");
            meta.put("requestedType", type);
            meta.put("resolvedType", resolved);
            meta.put("totalMs", elapsedMs);
            var stages = meta.putArray("stages");
            for (var span : response.trace().spans()) {
                stages.addObject().put("name", span.name()).put("ms", span.durationMs());
            }
        }
        // Enrich results with snippet fields before serialisation
        var resultArray = mapper.createArrayNode();
        for (SearchResult r : results) {
            var node = mapper.valueToTree(r);
            Snippet snip = r.snippet();
            if (snip != null) {
                ((com.fasterxml.jackson.databind.node.ObjectNode) node)
                        .put("snippet",          snip.text())
                        .put("snippetStartLine", snip.startLine())
                        .put("snippetEndLine",   snip.endLine());
            }
            resultArray.add(node);
        }
        envelope.set("results", resultArray);
        System.out.println(mapper.writeValueAsString(envelope));
    }
}
