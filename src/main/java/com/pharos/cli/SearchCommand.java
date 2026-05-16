package com.pharos.cli;

import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResponse;
import com.pharos.search.SearchResult;
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
            description = "Search type: keyword | vector | hybrid (default: hybrid)",
            defaultValue = "hybrid")
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
            List<SearchResult> results = response.results();
            long elapsedMs = response.trace().totalMs();

            boolean debug = false;
            if (debug) {
                System.err.println(response.trace().format());
            }

            if (results.isEmpty()) {
                System.out.printf("No results found for: %s  (%.3fs)%n", query, elapsedMs / 1000.0);
                return 0;
            }

            if ("json".equals(format)) {
                printJson(results, elapsedMs);
            } else {
                printText(results, elapsedMs);
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
            System.out.printf("   %s:%d-%d (score: %.4f)%n%n",
                    r.filePath(), r.startLine(), r.endLine(), r.score());
            boolean showBody = false;
            if (showBody && r.body() != null) {
                System.out.println("   " + r.body().replace("\n", "\n   "));
                System.out.println();
            }
        }
    }

    private void printJson(List<SearchResult> results, long elapsedMs) throws Exception {
        ObjectMapper mapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT);
        long primaryCount = results.stream().filter(r -> !"related".equals(r.searchType())).count();
        var envelope = mapper.createObjectNode()
                .put("total", primaryCount)
                .put("latencyMs", elapsedMs);
        envelope.set("results", mapper.valueToTree(results));
        System.out.println(mapper.writeValueAsString(envelope));
    }
}
