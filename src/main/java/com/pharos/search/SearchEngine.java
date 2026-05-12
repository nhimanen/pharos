package com.pharos.search;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Facade for all search operations. Routes to keyword, vector, or hybrid strategy
 * and handles multi-project index opening.
 */
public class SearchEngine {

    private static final Logger log = LoggerFactory.getLogger(SearchEngine.class);

    /**
     * Score multiplier applied to results whose project matches the requested project.
     * Only active when {@link SearchRequest#project()} is non-null (single-project search).
     * Modelled after Solr's boost-by-field pattern: results from the focal project bubble
     * up above same-relevance hits from other projects loaded via cross-project linking.
     */
    private static final float PROJECT_AFFINITY_BOOST = 1.5f;

    /** Boost applied when the query contains an explicit {@code project:query} or {@code in:lang} modifier. */
    private static final float QUERY_HINT_BOOST = 1.8f;

    /** Softer boost when a project name or language keyword appears in the query without a modifier colon. */
    private static final float IMPLICIT_HINT_BOOST = 1.3f;

    /**
     * Language keyword → file extension suffixes recognised by the {@code in:<lang>} modifier.
     * The value is matched as a suffix of {@link SearchResult#filePath()}.
     */
    private static final Map<String, String> LANG_EXTENSIONS = Map.ofEntries(
            Map.entry("java",       ".java"),
            Map.entry("python",     ".py"),
            Map.entry("py",         ".py"),
            Map.entry("kotlin",     ".kt"),
            Map.entry("kt",         ".kt"),
            Map.entry("scala",      ".scala"),
            Map.entry("groovy",     ".groovy"),
            Map.entry("javascript", ".js"),
            Map.entry("js",         ".js"),
            Map.entry("typescript", ".ts"),
            Map.entry("ts",         ".ts"),
            Map.entry("go",         ".go"),
            Map.entry("rust",       ".rs"),
            Map.entry("rs",         ".rs"),
            Map.entry("ruby",       ".rb"),
            Map.entry("rb",         ".rb"),
            Map.entry("csharp",     ".cs"),
            Map.entry("cs",         ".cs"),
            Map.entry("cpp",        ".cpp"),
            Map.entry("php",        ".php"),
            Map.entry("swift",      ".swift"),
            Map.entry("markdown",   ".md"),
            Map.entry("md",         ".md")
    );

    /**
     * Parsed modifiers extracted from the raw query string.
     *
     * @param cleanedQuery       the query with explicit modifier tokens stripped
     * @param projectBoost       project name to boost, or null
     * @param projectExplicit    true = found via {@code project:query} (stronger boost)
     * @param langExtension      file extension to boost, or null
     * @param langExplicit       true = found via {@code in:lang} (stronger boost)
     */
    private record QueryHints(String cleanedQuery, String projectBoost, boolean projectExplicit,
                               String langExtension, boolean langExplicit) {}

    private final LuceneIndexer luceneIndexer;
    private final EmbeddingProvider embedder;
    private final ProjectRegistry registry;
    private final KeywordSearchStrategy keywordStrategy;
    private final HybridSearchStrategy hybridStrategy;
    private final VectorSearchStrategy vectorStrategy;

    public SearchEngine(LuceneIndexer luceneIndexer, EmbeddingProvider embedder,
                        ProjectRegistry registry) {
        this.luceneIndexer = luceneIndexer;
        this.embedder = embedder;
        this.registry = registry;
        this.keywordStrategy = new KeywordSearchStrategy();
        this.hybridStrategy = new HybridSearchStrategy(embedder);
        this.vectorStrategy = new VectorSearchStrategy(embedder);
    }

    public List<SearchResult> search(SearchRequest req) throws IOException {
        return search(req, false);
    }

    /**
     * Search with optional neighborhood expansion.
     *
     * When {@code expand} is true, appends callee methods of the top-3 primary
     * results as additional "related" entries (score = parent_score * 0.5).
     * Deduplicates against primary results by document ID.
     */
    public List<SearchResult> search(SearchRequest req, boolean expand) throws IOException {
        return searchWithTrace(req, expand).results();
    }

    /**
     * Search and return results together with a detailed timing trace.
     * Use this from {@link com.pharos.cli.SearchCommand} when {@code --debug} is set.
     */
    public SearchResponse searchWithTrace(SearchRequest req, boolean expand) throws IOException {
        SearchTrace trace = new SearchTrace();
        trace.start();

        // Parse query modifiers (project:query, in:lang) before dispatching
        List<String> knownProjects = registry.listAll().stream()
                .map(ProjectMeta::getName).collect(Collectors.toList());
        QueryHints hints = parseQueryHints(req.query(), knownProjects);
        if (!hints.cleanedQuery().equals(req.query())) {
            req = new SearchRequest(hints.cleanedQuery(), req.type(), req.project(),
                    req.projects(), req.limit(), req.outputFormat(), req.docType());
            log.debug("Query rewritten: '{}' → project={} lang={} query='{}'",
                    hints.cleanedQuery(), hints.projectBoost(), hints.langExtension(), hints.cleanedQuery());
        }

        List<String> projects = resolveProjects(req);
        if (projects.isEmpty()) {
            log.warn("No indexed projects found. Run 'pharos index <path>' first.");
            return new SearchResponse(List.of(), trace);
        }

        IndexReader reader = luceneIndexer.openMultiReader(projects);

        List<SearchResult> primary;
        switch (req.type()) {
            case KEYWORD -> {
                long t = System.currentTimeMillis();
                primary = keywordStrategy.search(reader, req);
                trace.record("keyword search", t);
            }
            case VECTOR -> {
                long tEmbed = System.currentTimeMillis();
                // Embedding happens inside VectorSearchStrategy; we measure the whole call
                primary = vectorStrategy.search(reader, req);
                trace.record("vector search (incl. embed)", tEmbed);
            }
            case HYBRID -> {
                long tKw = System.currentTimeMillis();
                List<SearchResult> kwResults = keywordStrategy.search(reader, req);
                trace.record("keyword search", tKw);

                if (!embedder.isAvailable()) {
                    primary = kwResults;
                    trace.record("vector search skipped (no embedder)", System.currentTimeMillis());
                } else {
                    long tVec = System.currentTimeMillis();
                    List<SearchResult> vecResults = vectorStrategy.search(reader, req);
                    trace.record("vector search (incl. embed)", tVec);
                    long tRrf = System.currentTimeMillis();
                    primary = hybridStrategy.fuse(kwResults, vecResults, req.limit(), req.query());
                    trace.record("rrf fusion", tRrf);
                }
            }
            default -> primary = List.of();
        }

        // Project-affinity boost: when the caller has scoped the search to a specific project,
        // multiply scores of results from that project by PROJECT_AFFINITY_BOOST so they rank
        // above cross-project hits that may have leaked in via neighborhood expansion or linked
        // indexes. Applied after all strategy-level boosts (graph in-degree, source-path penalty)
        // so the multipliers compose correctly.
        primary = applyProjectAffinityBoost(primary, req.project());
        primary = applyQueryHintBoosts(primary, hints);

        if (!expand || primary.isEmpty()) {
            return new SearchResponse(primary, trace);
        }

        long tExpand = System.currentTimeMillis();
        List<SearchResult> expanded = expandNeighborhood(primary, req.project());
        trace.record("neighborhood expansion", tExpand);
        return new SearchResponse(expanded, trace);
    }

    /**
     * Parses query modifier tokens from the raw query string:
     * <ul>
     *   <li>{@code project:rest} — if the token before {@code :} matches a known project name,
     *       treat it as a project boost hint and use everything after {@code :} as the query.</li>
     *   <li>{@code in:lang} — boosts results whose filePath matches the language's extension.</li>
     * </ul>
     * Both modifiers are removed from the query that is sent to the search strategy so they
     * don't pollute BM25 scoring.
     */
    static QueryHints parseQueryHints(String rawQuery, List<String> knownProjects) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return new QueryHints(rawQuery, null, false, null, false);
        }

        String projectBoost = null;
        boolean projectExplicit = false;
        String langExtension = null;
        boolean langExplicit = false;
        String query = rawQuery.trim();

        // ── Explicit project:<rest> prefix ─────────────────────────────
        // The very first token may be "project:rest" — if "project" is a known project name,
        // treat it as a strong boost directive and strip it from the query.
        int firstColon = query.indexOf(':');
        int firstSpace = query.indexOf(' ');
        // Colon must be in the first token: before the first space, or no space at all
        if (firstColon > 0 && (firstSpace < 0 || firstColon < firstSpace)) {
            String prefix = query.substring(0, firstColon).toLowerCase();
            String after  = query.substring(firstColon + 1).trim();
            for (String p : knownProjects) {
                if (p.equalsIgnoreCase(prefix)) {
                    projectBoost = p;
                    projectExplicit = true;
                    query = after;
                    break;
                }
            }
        }

        // ── Scan tokens for in:<lang> and implicit mentions ─────────────
        String[] tokens = query.split("\\s+");
        List<String> kept = new ArrayList<>();
        for (String tok : tokens) {
            String lower = tok.toLowerCase();

            // Explicit in:lang modifier — strip from query
            if (lower.startsWith("in:")) {
                String lang = lower.substring(3);
                String ext = LANG_EXTENSIONS.get(lang);
                if (ext != null) {
                    langExtension = ext;
                    langExplicit = true;
                    continue;
                }
            }

            // Implicit language keyword anywhere in query (softer boost, keep in query)
            if (langExtension == null) {
                String ext = LANG_EXTENSIONS.get(lower);
                if (ext != null) {
                    langExtension = ext;
                    langExplicit = false;
                    // keep token in query — it still carries BM25 signal
                }
            }

            // Implicit project name mention (softer boost, keep in query)
            if (projectBoost == null) {
                for (String p : knownProjects) {
                    if (p.equalsIgnoreCase(lower)) {
                        projectBoost = p;
                        projectExplicit = false;
                        break;
                    }
                }
            }

            kept.add(tok);
        }
        query = String.join(" ", kept).trim();
        if (query.isEmpty()) query = rawQuery.trim(); // safety: never blank

        return new QueryHints(query, projectBoost, projectExplicit, langExtension, langExplicit);
    }

    /**
     * Applies {@value #QUERY_HINT_BOOST}x to results that match the extracted query hints:
     * project name from {@code project:query} syntax and/or language from {@code in:lang}.
     * Both boosts stack (multiply) when both modifiers are present.
     */
    private List<SearchResult> applyQueryHintBoosts(List<SearchResult> results, QueryHints hints) {
        if (hints.projectBoost() == null && hints.langExtension() == null) return results;
        float projMult = hints.projectExplicit() ? QUERY_HINT_BOOST : IMPLICIT_HINT_BOOST;
        float langMult = hints.langExplicit()    ? QUERY_HINT_BOOST : IMPLICIT_HINT_BOOST;
        boolean changed = false;
        List<SearchResult> out = new ArrayList<>(results.size());
        for (SearchResult r : results) {
            float mult = 1.0f;
            if (hints.projectBoost() != null && hints.projectBoost().equals(r.project())) {
                mult *= projMult;
            }
            if (hints.langExtension() != null && r.filePath() != null
                    && r.filePath().endsWith(hints.langExtension())) {
                mult *= langMult;
            }
            if (mult != 1.0f) {
                r = new SearchResult(r.id(), r.project(), r.packageName(),
                        r.className(), r.qualifiedClassName(), r.methodName(),
                        r.signature(), r.returnType(), r.body(), r.javadoc(),
                        r.accessModifier(), r.filePath(),
                        r.startLine(), r.endLine(),
                        r.score() * mult, r.searchType(), r.docType());
                changed = true;
            }
            out.add(r);
        }
        if (!changed) return results;
        out.sort(java.util.Comparator.comparingDouble(SearchResult::score).reversed());
        return out;
    }

    /**
     * Applies a score multiplier ({@value #PROJECT_AFFINITY_BOOST}x) to results whose
     * {@link SearchResult#project()} matches {@code requestedProject}.  When
     * {@code requestedProject} is null or blank — i.e. a cross-project search — no boost
     * is applied and the list is returned unchanged.
     *
     * <p>Results are re-sorted by descending score after the multiplier is applied so the
     * ranking stays consistent with the boosted values.
     */
    private List<SearchResult> applyProjectAffinityBoost(List<SearchResult> results, String requestedProject) {
        if (requestedProject == null || requestedProject.isEmpty()) {
            return results;
        }
        return results.stream()
                .map(r -> {
                    if (requestedProject.equals(r.project())) {
                        return new SearchResult(
                                r.id(), r.project(), r.packageName(),
                                r.className(), r.qualifiedClassName(), r.methodName(),
                                r.signature(), r.returnType(), r.body(), r.javadoc(),
                                r.accessModifier(), r.filePath(),
                                r.startLine(), r.endLine(),
                                r.score() * PROJECT_AFFINITY_BOOST, r.searchType(), r.docType()
                        );
                    }
                    return r;
                })
                .sorted(java.util.Comparator.comparingDouble(SearchResult::score).reversed())
                .collect(Collectors.toList());
    }

    /**
     * Expands primary results by fetching callee documents for the top-3 hits.
     * Related results get score = parent_score * 0.5, searchType = "related".
     * Already-seen IDs are skipped to avoid duplicates.
     */
    private List<SearchResult> expandNeighborhood(List<SearchResult> primary, String project) throws IOException {
        Set<String> seen = new LinkedHashSet<>();
        for (SearchResult r : primary) seen.add(r.id());

        List<SearchResult> expanded = new ArrayList<>(primary);

        // Expand callees of top-3 results
        int expandLimit = Math.min(3, primary.size());
        for (int i = 0; i < expandLimit; i++) {
            SearchResult parent = primary.get(i);
            // Extract the FQN from the id: id format is "project:fqn"
            String id = parent.id();
            int colon = id.indexOf(':');
            if (colon < 0) continue;
            String fqn = id.substring(colon + 1);

            List<SearchResult> callees = findCallees(fqn, project);
            for (SearchResult callee : callees) {
                if (!seen.contains(callee.id())) {
                    seen.add(callee.id());
                    expanded.add(new SearchResult(
                            callee.id(), callee.project(), callee.packageName(),
                            callee.className(), callee.qualifiedClassName(), callee.methodName(),
                            callee.signature(), callee.returnType(), callee.body(), callee.javadoc(),
                            callee.accessModifier(), callee.filePath(),
                            callee.startLine(), callee.endLine(),
                            parent.score() * 0.5f, "related", callee.docType()
                    ));
                }
            }
        }
        return expanded;
    }

    /** Look up a method by its exact FQN (e.g., "com.example.MyClass#myMethod(String,int)"). */
    public SearchResult getMethodByFqn(String fqn) throws IOException {
        List<String> projects = registry.listAll().stream()
                .map(ProjectMeta::getName)
                .collect(Collectors.toList());
        if (projects.isEmpty()) return null;

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Search across all projects by matching the FQN suffix of the id field
        // id format: "projectName:fqn" — search all projects
        for (String project : projects) {
            String id = project + ":" + fqn;
            TopDocs hits = searcher.search(new TermQuery(new Term(DocumentMapper.F_ID, id)), 1);
            if (hits.totalHits.value() > 0) {
                StoredFields storedFields = searcher.storedFields();
                Document doc = storedFields.document(hits.scoreDocs[0].doc);
                return KeywordSearchStrategy.docToResult(doc, 1.0f, "exact");
            }
        }
        return null;
    }

    /** Find all methods that call the given FQN (reverse lookup via calledMethods field). */
    public List<SearchResult> findCallers(String calleeFqn, String project) throws IOException {
        SearchRequest req = SearchRequest.keyword("", project, 100);
        List<String> projects = resolveProjects(req);
        if (projects.isEmpty()) return List.of();

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);

        TermQuery calleeQuery = new TermQuery(new Term(DocumentMapper.F_CALLED_METHODS, calleeFqn));
        TopDocs hits = searcher.search(calleeQuery, 100);
        return KeywordSearchStrategy.toResults(searcher, hits, "graph");
    }

    /** Find all methods that the given FQN calls (forward lookup via calledMethods field). */
    public List<SearchResult> findCallees(String callerFqn, String project) throws IOException {
        SearchResult caller = getMethodByFqn(callerFqn);
        if (caller == null) return List.of();

        // The calledMethods field stores FQNs; we look them up directly
        // For now return the caller with its callees via a direct document lookup
        List<String> projects = resolveProjects(SearchRequest.keyword("", project, 100));
        if (projects.isEmpty()) return List.of();

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);
        StoredFields storedFields = searcher.storedFields();

        // Find the caller document to get its calledMethods values
        for (String proj : projects) {
            String id = proj + ":" + callerFqn;
            TopDocs hits = searcher.search(new TermQuery(new Term(DocumentMapper.F_ID, id)), 1);
            if (hits.totalHits.value() > 0) {
                Document doc = storedFields.document(hits.scoreDocs[0].doc);
                String[] calledFqns = doc.getValues(DocumentMapper.F_CALLED_METHODS);
                if (calledFqns.length == 0) return List.of();

                // Look up each callee
                return java.util.Arrays.stream(calledFqns)
                        .map(calleeFqn -> {
                            try { return getMethodByFqn(calleeFqn); } catch (IOException e) { return null; }
                        })
                        .filter(r -> r != null)
                        .collect(Collectors.toList());
            }
        }
        return List.of();
    }

    private List<String> resolveProjects(SearchRequest req) {
        if (req.project() != null && !req.project().isEmpty()) {
            return List.of(req.project());
        }
        if (req.projects() != null && !req.projects().isEmpty()) {
            return req.projects();
        }
        // All indexed projects
        return registry.listAll().stream()
                .map(ProjectMeta::getName)
                .filter(name -> luceneIndexer.indexExists(name))
                .collect(Collectors.toList());
    }
}
