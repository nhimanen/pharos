package com.pharos.search;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.search.SourceReader;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.pipeline.BordaMerger;
import com.pharos.search.pipeline.UnifiedRetrievalStage;
import com.pharos.search.pipeline.CrossEncoder;
import com.pharos.search.pipeline.CrossEncoderMerger;
import com.pharos.search.pipeline.CrossEncoderReranker;
import com.pharos.search.pipeline.DiversityReranker;
import com.pharos.search.pipeline.KeywordRetrievalStage;
import com.pharos.search.pipeline.NoOpCrossEncoder;
import com.pharos.search.pipeline.PipelineDescriptor;
import com.pharos.search.pipeline.RouterDispatcher;
import com.pharos.search.pipeline.SearchPipeline;
import com.pharos.search.pipeline.VectorRetrievalStage;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumMap;
import java.util.LinkedHashMap;
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
    private final ProjectRegistry registry;
    private final KeywordSearchStrategy keywordStrategy;
    private final VectorSearchStrategy  vectorStrategy;
    private final EmbeddingProvider     embedder;
    private final QueryRouter queryRouter;
    private final ZeroResultAdvisor zeroResultAdvisor;
    private final Map<SearchRequest.SearchType, SearchPipeline> pipelines;
    private final List<PipelineDescriptor> pipelineDescriptors;

    public SearchEngine(LuceneIndexer luceneIndexer, EmbeddingProvider embedder,
                        ProjectRegistry registry) {
        this(luceneIndexer, embedder, registry, new NoOpCrossEncoder(), new FstQueryClassifier());
    }

    public SearchEngine(LuceneIndexer luceneIndexer, EmbeddingProvider embedder,
                        ProjectRegistry registry, CrossEncoder crossEncoder) {
        this(luceneIndexer, embedder, registry, crossEncoder, new FstQueryClassifier());
    }

    public SearchEngine(LuceneIndexer luceneIndexer, EmbeddingProvider embedder,
                        ProjectRegistry registry, CrossEncoder crossEncoder,
                        QueryRouter queryRouter) {
        this.luceneIndexer = luceneIndexer;
        this.registry      = registry;
        this.embedder      = embedder;
        this.queryRouter   = queryRouter;

        KeywordSearchStrategy kw  = new KeywordSearchStrategy();
        this.keywordStrategy = kw;
        VectorSearchStrategy  vec = new VectorSearchStrategy(embedder);
        this.vectorStrategy  = vec;
        this.zeroResultAdvisor = new ZeroResultAdvisor(luceneIndexer, registry, kw);

        KeywordRetrievalStage  kwStage       = new KeywordRetrievalStage(kw);
        VectorRetrievalStage   vecStage      = new VectorRetrievalStage(vec);
        UnifiedRetrievalStage  unifiedStage  = new UnifiedRetrievalStage(kw, embedder);
        BordaMerger            borda         = new BordaMerger();
        CrossEncoderMerger     ceMerger      = new CrossEncoderMerger(crossEncoder);
        CrossEncoderReranker   ceReranker    = new CrossEncoderReranker(crossEncoder);
        DiversityReranker      diversityReranker = new DiversityReranker(0.5f);

        // Child pipelines for the auto dispatcher (no router — classification already on req)
        SearchPipeline kwPipeline = SearchPipeline.builder().retriever(kwStage).build();
        SearchPipeline hyPipeline = SearchPipeline.builder()
                .retriever(kwStage).retriever(vecStage).merger(borda).build();
        SearchPipeline hyRePipeline = SearchPipeline.builder()
                .retriever(kwStage).retriever(vecStage).merger(borda).reranker(ceReranker).build();

        Map<SearchRequest.SearchType, SearchPipeline> map = new EnumMap<>(SearchRequest.SearchType.class);

        // AUTO: QueryRouter classifies once, RouterDispatcher picks the right child pipeline.
        // CONFIG intent → hybrid-reranked: CE bridges vocabulary gaps ("configure" ↔ "set", etc.)
        map.put(SearchRequest.SearchType.AUTO,
                SearchPipeline.builder()
                        .router(queryRouter)
                        .retriever(new RouterDispatcher(
                                Map.of(SearchRequest.SearchType.KEYWORD,         kwPipeline,
                                       SearchRequest.SearchType.HYBRID,          hyPipeline,
                                       SearchRequest.SearchType.HYBRID_RERANKED, hyRePipeline),
                                SearchRequest.SearchType.HYBRID))
                        .build());

        // UNIFIED: QueryRouter classifies once, UnifiedRetrievalStage reads intent for adaptive weights
        map.put(SearchRequest.SearchType.UNIFIED,
                SearchPipeline.builder().router(queryRouter).retriever(unifiedStage).build());

        map.put(SearchRequest.SearchType.KEYWORD,
                SearchPipeline.builder().retriever(kwStage).build());
        map.put(SearchRequest.SearchType.VECTOR,
                SearchPipeline.builder().retriever(vecStage).build());
        map.put(SearchRequest.SearchType.HYBRID,
                SearchPipeline.builder().retriever(kwStage).retriever(vecStage).merger(borda).build());
        map.put(SearchRequest.SearchType.HYBRID_RERANKED,
                SearchPipeline.builder().retriever(kwStage).retriever(vecStage).merger(borda).reranker(ceReranker).build());
        map.put(SearchRequest.SearchType.HYBRID_CROSS_ENCODER_MERGE,
                SearchPipeline.builder().retriever(kwStage).retriever(vecStage).merger(ceMerger).build());
        map.put(SearchRequest.SearchType.HYBRID_DIVERSE,
                SearchPipeline.builder().retriever(kwStage).retriever(vecStage).oversample(3).premerge(diversityReranker).merger(borda).build());
        map.put(SearchRequest.SearchType.HYBRID_RERANKED_DIVERSE,
                SearchPipeline.builder().retriever(kwStage).retriever(vecStage).oversample(3).premerge(diversityReranker).merger(borda).reranker(ceReranker).build());
        this.pipelines = Map.copyOf(map);

        boolean vecAvailable = embedder.isAvailable();
        boolean ceAvailable  = crossEncoder.isAvailable();
        this.pipelineDescriptors = List.of(
            new PipelineDescriptor("auto",           "Auto",
                    "Automatically selects Keyword or Hybrid based on query shape", true),
            new PipelineDescriptor("keyword",        "Keyword (BM25)",
                    "BM25 keyword search with field boosts and graph in-degree scoring", true),
            new PipelineDescriptor("vector",         "Semantic (Vector)",
                    "HNSW nearest-neighbor search over embedded method/class bodies", vecAvailable),
            new PipelineDescriptor("hybrid",         "Hybrid",
                    "Borda-count fusion of keyword and vector results with agreement bonus", true),
            new PipelineDescriptor("unified",        "Unified",
                    "Single BM25 pass with vector similarity as a score boost; no separate merge step", vecAvailable),
            new PipelineDescriptor("hybrid-reranked","Hybrid + Reranker",
                    "Hybrid fusion followed by cross-encoder reranking over the merged list", ceAvailable),
            new PipelineDescriptor("hybrid-ce-merge",          "CE Merge",
                    "Cross-encoder scores all deduplicated candidates and acts as the merge step", ceAvailable),
            new PipelineDescriptor("hybrid-diverse",           "Hybrid + Diversity",
                    "Hybrid fusion with doc-type diversity reranking to balance method/class/chunk results", true),
            new PipelineDescriptor("hybrid-reranked-diverse",  "Hybrid + CE + Diversity",
                    "Cross-encoder reranking followed by doc-type diversity reranking", ceAvailable)
        );
    }

    /** Returns the ordered list of available pipelines for UI display. */
    public List<PipelineDescriptor> listPipelines() {
        return pipelineDescriptors;
    }

    /**
     * Creates a {@link SnippetDecorator} backed by this engine's keyword analyzer.
     * The decorator uses the same Lucene analysis chain as search-time query parsing,
     * so stop-word removal and synonym expansion are consistent between query and body tokens.
     *
     * @param windowLines lines of context per snippet (clamped to [5, 50])
     */
    /** Creates a decorator with no resolver signal (for callers without a SearchResponse). */
    public SnippetDecorator newSnippetDecorator(int windowLines) {
        return new SnippetDecorator(windowLines, SnippetResolver.none(), SnippetResolver.none());
    }

    /** Creates a decorator with both keyword and vector position resolvers from a SearchResponse. */
    public SnippetDecorator newSnippetDecorator(int windowLines, SearchResponse response) {
        SnippetResolver kw  = response.keywordResolver()  != null ? response.keywordResolver()  : SnippetResolver.none();
        SnippetResolver vec = response.vectorResolver() != null ? response.vectorResolver() : SnippetResolver.none();
        return new SnippetDecorator(windowLines, kw, vec);
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
                    req.projects(), req.limit(), req.outputFormat(), req.docType(), req.scope(), req.oversampleFactor());
            log.debug("Query rewritten: '{}' → project={} lang={} query='{}'",
                    hints.cleanedQuery(), hints.projectBoost(), hints.langExtension(), hints.cleanedQuery());
        }

        List<String> projects = resolveProjects(req);
        if (projects.isEmpty()) {
            log.warn("No indexed projects found. Run 'pharos index <path>' first.");
            return new SearchResponse(List.of(), trace, req.type().name().toLowerCase(), null);
        }

        IndexReader reader = luceneIndexer.openMultiReader(projects);

        // Classification for AUTO and UNIFIED is now handled inside SearchPipeline via QueryRouter.
        // For other types, resolvedType is simply req.type(); the pipeline runs as configured.
        SearchRequest.SearchType resolvedType = req.type();
        final String resolvedTypeName = resolvedType.name().toLowerCase();

        // Query-type-adaptive candidate pool: when the caller hasn't set an explicit
        // oversample factor, inject one based on the resolved query type and shape.
        // Identifier/CamelCase queries have high BM25 precision — a large vector pool
        // adds noise.  NL queries need broader vector recall.
        // UNIFIED and VECTOR manage their own oversampling internally; leave them alone.
        if (req.oversampleFactor() == 0) {
            int adaptive = adaptiveOversample(resolvedType, hints.cleanedQuery());
            if (adaptive > 0) {
                req = new SearchRequest(req.query(), req.type(), req.project(), req.projects(),
                        req.limit(), req.outputFormat(), req.docType(), req.scope(), adaptive);
                log.debug("Adaptive oversample: type={} query='{}' factor={}",
                        resolvedTypeName, hints.cleanedQuery(), adaptive);
            }
        }

        SearchPipeline pipeline = pipelines.getOrDefault(resolvedType,
                pipelines.get(SearchRequest.SearchType.HYBRID));
        List<SearchResult> primary = pipeline.execute(reader, req, trace);

        primary = applyProjectAffinityBoost(primary, req.project());
        primary = applyQueryHintBoosts(primary, hints);

        if (primary.isEmpty()) {
            ZeroResultAdvisor.Suggestions suggestions = zeroResultAdvisor.advise(req, projects);
            return new SearchResponse(List.of(), trace, resolvedTypeName, suggestions, null, null);
        }

        // Build snippet resolvers lazily — called only for final results, not all candidates.
        SnippetResolver kwResolver  = keywordStrategy.buildKeywordResolver(
                keywordStrategy.buildQuery(new SearchRequest(
                        hints.cleanedQuery(), resolvedType, req.project(), req.projects(),
                        req.limit(), req.outputFormat(), req.docType(), req.scope(), req.oversampleFactor())));
        SnippetResolver vecResolver = embedder.isAvailable()
                ? vectorStrategy.buildVectorResolver(reader, embedder.embed(hints.cleanedQuery()))
                : SnippetResolver.none();

        if (!expand) {
            return new SearchResponse(primary, trace, resolvedTypeName, null, kwResolver, vecResolver);
        }

        long tExpand = System.currentTimeMillis();
        List<SearchResult> expanded = expandNeighborhood(primary, req.project());
        trace.record("neighborhood expansion", tExpand);
        return new SearchResponse(expanded, trace, resolvedTypeName, null, kwResolver, vecResolver);
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
        return getByFqn(fqn);
    }

    /**
     * Partial FQN lookup for when parameter types are omitted.
     * Accepts "pkg.Class#method" or "Class#method" — the "#" must be present.
     * Returns all matching methods (may be multiple overloads).
     *
     * <p>Uses a PrefixQuery on the {@code F_ID} StringField (format:
     * {@code "project:qualifiedClass#methodName("}) — not analyzed, so the
     * fully-qualified class name is matched exactly.
     */
    public List<SearchResult> findMethodsByPartialFqn(String partialFqn) throws IOException {
        int hash = partialFqn.indexOf('#');
        if (hash < 0) return List.of();
        String className  = partialFqn.substring(0, hash);
        String methodName = partialFqn.substring(hash + 1);
        if (className.isEmpty() || methodName.isEmpty()) return List.of();

        List<String> projects = registry.listAll().stream()
                .map(ProjectMeta::getName).collect(Collectors.toList());
        if (projects.isEmpty()) return List.of();

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);

        // F_ID is a StringField (not analyzed): "project:qualifiedClass#methodName(params)"
        // A PrefixQuery on "project:qualifiedClass#methodName(" matches all overloads.
        BooleanQuery.Builder qb = new BooleanQuery.Builder();
        for (String project : projects) {
            String idPrefix = project + ":" + className + "#" + methodName + "(";
            qb.add(new PrefixQuery(new Term(DocumentMapper.F_ID, idPrefix)),
                    BooleanClause.Occur.SHOULD);
        }
        qb.setMinimumNumberShouldMatch(1);

        TopDocs hits = searcher.search(qb.build(), 50);
        return KeywordSearchStrategy.toResults(searcher, hits, "exact");
    }

    /**
     * Look up a class by its qualified name (e.g., "com.example.MyClass").
     * Returns the class document if it was indexed (docType="class"), else null.
     * The returned {@link SearchResult} carries the file path and line range; callers
     * that need the full source body (including field initializers and enum constants
     * that aren't individually indexed) should read the file slice themselves.
     */
    public SearchResult getClassByFqn(String qualifiedClassName) throws IOException {
        SearchResult r = getByFqn(qualifiedClassName);
        if (r == null || !"class".equals(r.docType())) return null;
        return r;
    }

    private SearchResult getByFqn(String fqn) throws IOException {
        List<String> projects = registry.listAll().stream()
                .map(ProjectMeta::getName)
                .collect(Collectors.toList());
        if (projects.isEmpty()) return null;

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);

        // id format: "projectName:fqn" — search across all projects for an exact match
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

    /**
     * Bulk lookup by FQN — opens the index once and fetches all requested methods/classes
     * in a single {@link TermInSetQuery} pass.
     *
     * @param fqns bare FQNs (no project prefix) — methods or classes
     * @return map from FQN to result; FQNs not found are absent from the map
     */
    public Map<String, SearchResult> getByFqns(List<String> fqns) throws IOException {
        if (fqns.isEmpty()) return Map.of();

        List<String> projects = registry.listAll().stream()
                .map(ProjectMeta::getName).collect(Collectors.toList());
        if (projects.isEmpty()) return Map.of();

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Build all "project:fqn" id candidates for TermInSetQuery
        List<BytesRef> ids = new ArrayList<>();
        for (String project : projects) {
            for (String fqn : fqns) {
                ids.add(new BytesRef(project + ":" + fqn));
            }
        }

        int maxHits = fqns.size() * projects.size();
        TopDocs hits = searcher.search(new TermInSetQuery(DocumentMapper.F_ID, ids), maxHits);

        Map<String, SearchResult> result = new LinkedHashMap<>();
        StoredFields storedFields = searcher.storedFields();
        for (var sd : hits.scoreDocs) {
            Document doc = storedFields.document(sd.doc);
            SearchResult r = KeywordSearchStrategy.docToResult(doc, 1.0f, "exact");
            String fqn = r.id().substring(r.project().length() + 1); // strip "project:" prefix
            result.putIfAbsent(fqn, r); // first project match wins
        }
        return result;
    }

    /**
     * Traverses the call graph in BFS order up to {@code depth} hops from {@code rootFqn},
     * bulk-fetches method bodies for every visited node in one Lucene pass, and returns a
     * flat node list (BFS order) with children FQNs attached.
     *
     * @param direction  {@code "callers"}, {@code "callees"}, or {@code "both"}
     * @param maxBodyChars  body text is truncated to this length (0 = no truncation)
     */
    public CallChainResult traceCallChain(String rootFqn, int depth, String direction,
                                          int maxBodyChars) throws IOException {
        List<CallGraph> graphs = openAllGraphs();
        try {
            BfsResult bfs = bfsCallGraph(graphs, rootFqn, depth, direction, 50);

            // Bulk-fetch bodies — one Lucene query for all visited FQNs.
            Map<String, SearchResult> bodies = getByFqns(new ArrayList<>(bfs.fqnDepth().keySet()));

            List<CallChainResult.ChainNode> nodes = bfs.fqnDepth().entrySet().stream()
                    .sorted(Comparator.comparingInt(Map.Entry::getValue))
                    .map(e -> {
                        String       fqn      = e.getKey();
                        int          nd       = e.getValue();
                        SearchResult r        = bodies.get(fqn);
                        List<String> children = bfs.fqnChildren().getOrDefault(fqn, List.of());
                        String body = r != null ? r.body() : null;
                        if (body != null && maxBodyChars > 0 && body.length() > maxBodyChars)
                            body = body.substring(0, maxBodyChars) + "\n// ...";
                        return new CallChainResult.ChainNode(
                                fqn, r != null ? r.label() : fqn, nd,
                                r != null ? r.signature() : null,
                                r != null ? r.filePath() : null,
                                r != null ? r.startLine() : 0,
                                r != null ? r.endLine() : 0,
                                body, children);
                    })
                    .collect(Collectors.toList());

            return new CallChainResult(rootFqn, direction, depth,
                    nodes.size(), bfs.truncated(), nodes);
        } finally {
            for (CallGraph g : graphs) { try { g.close(); } catch (Exception ignored) {} }
        }
    }

    // -------------------------------------------------------------------------
    // Shared BFS infrastructure
    // -------------------------------------------------------------------------

    private record BfsResult(
            Map<String, Integer>      fqnDepth,    // fqn → hop distance from root
            Map<String, List<String>> fqnChildren, // fqn → neighbor FQNs at next depth
            boolean truncated
    ) {}

    /** Opens an ArcadeDB call graph for every indexed project. Caller must close all returned graphs. */
    private List<CallGraph> openAllGraphs() {
        List<CallGraph> graphs = new ArrayList<>();
        for (ProjectMeta meta : registry.listAll()) {
            Path dbDir = Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb");
            if (Files.isDirectory(dbDir)) {
                try { graphs.add(CallGraph.open(dbDir)); } catch (Exception ignored) {}
            }
        }
        return graphs;
    }

    /**
     * BFS over the call graph starting from {@code rootFqn}.
     * Deduplicates visited FQNs — each node appears once at its minimum depth.
     *
     * @param maxNodes hard cap on the total number of unique FQNs collected (including root)
     */
    private static BfsResult bfsCallGraph(List<CallGraph> graphs, String rootFqn,
                                           int depth, String direction, int maxNodes) {
        Map<String, Integer>      fqnDepth    = new LinkedHashMap<>();
        Map<String, List<String>> fqnChildren = new LinkedHashMap<>();
        Deque<String>             queue       = new ArrayDeque<>();
        boolean                   truncated   = false;

        fqnDepth.put(rootFqn, 0);
        queue.add(rootFqn);

        outer:
        while (!queue.isEmpty()) {
            String current      = queue.poll();
            int    currentDepth = fqnDepth.get(current);
            if (currentDepth >= depth) continue;

            Set<String> neighbors = neighborsFromGraphs(graphs, current, direction);
            List<String> children = new ArrayList<>(neighbors);
            fqnChildren.put(current, children);

            for (String neighbor : children) {
                if (fqnDepth.containsKey(neighbor)) continue;
                if (fqnDepth.size() >= maxNodes) { truncated = true; break outer; }
                fqnDepth.put(neighbor, currentDepth + 1);
                queue.add(neighbor);
            }
        }
        return new BfsResult(fqnDepth, fqnChildren, truncated);
    }

    // -------------------------------------------------------------------------
    // Transitive callers — lightweight impact-analysis variant (no body fetch)
    // -------------------------------------------------------------------------

    /**
     * Impact surface of changing {@code rootFqn}: all unique transitive callers up to
     * {@code depth} hops, returned as a flat deduplicated set grouped by distance.
     * Unlike {@link #traceCallChain}, this fetches no source bodies, allowing much
     * larger traversals suitable for impact analysis on widely-used APIs.
     *
     * @param maxCallers cap on unique callers collected (default 2000)
     */
    public record TransitiveCallersResult(
            String root,
            int    maxDepth,
            int    totalCallers,
            boolean truncated,
            List<Map.Entry<String, Integer>> callers  // fqn → depth, sorted by depth then fqn
    ) {}

    public TransitiveCallersResult findTransitiveCallers(String rootFqn, int depth,
                                                          int maxCallers) throws IOException {
        List<CallGraph> graphs = openAllGraphs();
        try {
            // +1 to include root in the cap budget, then exclude it from results
            BfsResult bfs = bfsCallGraph(graphs, rootFqn, depth, "callers",
                    Math.max(1, maxCallers + 1));

            List<Map.Entry<String, Integer>> callers = bfs.fqnDepth().entrySet().stream()
                    .filter(e -> !e.getKey().equals(rootFqn))   // exclude root itself
                    .sorted(Comparator.comparingInt(Map.Entry<String, Integer>::getValue)
                            .thenComparing(Map.Entry::getKey))
                    .collect(Collectors.toList());

            return new TransitiveCallersResult(
                    rootFqn, depth, callers.size(), bfs.truncated(), callers);
        } finally {
            for (CallGraph g : graphs) { try { g.close(); } catch (Exception ignored) {} }
        }
    }

    /**
     * Returns an adaptive candidate-pool oversample factor based on query type and shape,
     * or 0 to leave the pipeline's own default in place.
     *
     * <p>Rationale (Hornet / hybrid-search research):
     * <ul>
     *   <li><b>KEYWORD</b> — pure BM25; no vector candidates are fetched, so oversample = 1
     *       (passing 1 is a no-op since the KEYWORD pipeline has no vector retriever).</li>
     *   <li><b>HYBRID + CamelCase query</b> — BM25 precision is high for identifier queries;
     *       a large vector pool introduces noise.  Use 1 = "no extra candidates beyond limit".</li>
     *   <li><b>HYBRID + natural-language query</b> — semantic recall matters; keep the
     *       standard 3× pool so the vector retriever can surface paraphrased matches.</li>
     *   <li><b>VECTOR / UNIFIED / others</b> — these pipelines manage their own oversampling
     *       internally; return 0 to leave them untouched.</li>
     * </ul>
     */
    private static int adaptiveOversample(SearchRequest.SearchType resolvedType, String query) {
        return switch (resolvedType) {
            case KEYWORD -> 1;
            case HYBRID  -> queryHasCamelCase(query) ? 1 : 3;
            default      -> 0; // VECTOR, UNIFIED, HYBRID_* — handle internally
        };
    }

    /** Returns true when the query contains at least one uppercase letter (CamelCase signal). */
    private static boolean queryHasCamelCase(String query) {
        return query != null && query.chars().anyMatch(Character::isUpperCase);
    }

    private static Set<String> neighborsFromGraphs(List<CallGraph> graphs,
                                                    String fqn, String direction) {
        Set<String> result = new LinkedHashSet<>();
        for (CallGraph g : graphs) {
            try {
                if (!"callees".equals(direction)) result.addAll(g.callers(fqn));
                if (!"callers".equals(direction)) result.addAll(g.callees(fqn));
            } catch (Exception ignored) {}
        }
        return result;
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

    // -------------------------------------------------------------------------
    // Knowledge-graph queries
    // -------------------------------------------------------------------------

    /**
     * Result of a {@link #findUsages} call, grouping usages by kind.
     * Lists contain FQNs (methods, classes, fields) or annotation names.
     */
    public record UsageResult(
            String fqn,
            List<String> callers,
            List<String> subclasses,
            List<String> superTypes,
            List<String> fieldReaders,
            List<String> fieldWriters,
            List<String> annotatedWith,
            List<String> methodsReturning,
            List<String> methodsTaking
    ) {}

    /**
     * Finds all usages of {@code fqn} across every kind of structural relationship
     * stored in the knowledge graph.
     *
     * <p>Results are aggregated across all indexed projects' call graphs.
     *
     * @param kind  {@code "all"} | {@code "callers"} | {@code "subclasses"} |
     *              {@code "field_readers"} | {@code "field_writers"} |
     *              {@code "annotated"} | {@code "type_refs"}
     */
    public UsageResult findUsages(String fqn, String kind) throws IOException {
        boolean all = "all".equals(kind);
        List<String> callers        = new ArrayList<>();
        List<String> subclasses     = new ArrayList<>();
        List<String> superTypes     = new ArrayList<>();
        List<String> fieldReaders   = new ArrayList<>();
        List<String> fieldWriters   = new ArrayList<>();
        List<String> annotatedWith  = new ArrayList<>();
        List<String> returning      = new ArrayList<>();
        List<String> taking         = new ArrayList<>();

        List<CallGraph> graphs = openAllGraphs();
        try {
            for (CallGraph g : graphs) {
                if (all || "callers".equals(kind))
                    callers.addAll(g.callers(fqn));
                if (all || "subclasses".equals(kind)) {
                    subclasses.addAll(g.directSubclasses(fqn));
                    superTypes.addAll(g.directSuperTypes(fqn));
                }
                if (all || "field_readers".equals(kind))
                    fieldReaders.addAll(g.fieldReaders(fqn));
                if (all || "field_writers".equals(kind))
                    fieldWriters.addAll(g.fieldWriters(fqn));
                if (all || "annotated".equals(kind))
                    annotatedWith.addAll(g.annotatedWith(fqn));
                if (all || "type_refs".equals(kind)) {
                    returning.addAll(g.methodsReturning(fqn));
                    taking.addAll(g.methodsTaking(fqn));
                }
            }
        } finally {
            for (CallGraph g : graphs) { try { g.close(); } catch (Exception ignored) {} }
        }

        return new UsageResult(fqn, callers, subclasses, superTypes,
                fieldReaders, fieldWriters, annotatedWith, returning, taking);
    }

    // -------------------------------------------------------------------------
    // Class context
    // -------------------------------------------------------------------------

    /**
     * Aggregated context for a single class: body, fields, constructors,
     * public methods, and their direct callers — assembled in one call.
     */
    public record ClassContext(
            SearchResult classResult,
            String body,                                   // full source body from file
            List<CallGraph.FieldInfo> fields,              // declared fields (from knowledge graph)
            List<SearchResult> constructors,               // <init> method documents
            List<SearchResult> publicMethods,              // public method documents
            Map<String, List<String>> publicMethodCallers  // public method fqn → caller fqns (max 10 each)
    ) {}

    /**
     * Fetches all context needed to understand a class in a single call.
     * Opens the knowledge graph once for field data and once for caller lookups.
     *
     * @param classFqn qualified class name, e.g. {@code "com.example.AuthService"}
     * @return fully assembled context, or null if the class is not indexed
     */
    public ClassContext getClassContext(String classFqn) throws IOException {
        SearchResult cls = getClassByFqn(classFqn);
        if (cls == null) return null;

        // Full body from source file (same logic as MCP get_class)
        String body = SourceReader.readRange(cls.filePath(), cls.startLine(), cls.endLine());
        if (body == null) body = cls.body();

        // All method documents for this class from Lucene.
        // F_QUALIFIED_CLASS is a TextField (analyzed), so TermQuery on a dotted FQN never matches.
        // Instead, use PrefixQuery on the StringField F_ID ("project:FQN#") which is exact.
        List<String> projects = resolveProjects(SearchRequest.keyword("", null, 100));
        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);
        BooleanQuery.Builder idPrefixes = new BooleanQuery.Builder().setMinimumNumberShouldMatch(1);
        for (String proj : projects) {
            idPrefixes.add(new PrefixQuery(new Term(DocumentMapper.F_ID, proj + ":" + classFqn + "#")),
                    BooleanClause.Occur.SHOULD);
        }
        BooleanQuery methodsQuery = new BooleanQuery.Builder()
                .add(idPrefixes.build(), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, "method")),
                        BooleanClause.Occur.MUST)
                .build();
        TopDocs hits = searcher.search(methodsQuery, 200);
        List<SearchResult> allMethods = KeywordSearchStrategy.toResults(searcher, hits, "exact");

        List<SearchResult> constructors  = new ArrayList<>();
        List<SearchResult> publicMethods = new ArrayList<>();
        for (SearchResult m : allMethods) {
            if ("<init>".equals(m.methodName())) constructors.add(m);
            else if ("public".equals(m.accessModifier()))  publicMethods.add(m);
        }

        // Fields from knowledge graph
        List<CallGraph.FieldInfo> fields = new ArrayList<>();
        List<CallGraph> kgGraphs = openAllGraphs();
        try {
            for (CallGraph g : kgGraphs) fields.addAll(g.getFieldsOf(classFqn));
        } finally {
            for (CallGraph g : kgGraphs) { try { g.close(); } catch (Exception ignored) {} }
        }

        // Callers for each public method (cap at 10 per method)
        Map<String, List<String>> callerMap = new LinkedHashMap<>();
        for (SearchResult m : publicMethods) {
            String methodFqn = m.id().substring(m.project().length() + 1);
            List<SearchResult> callers = findCallers(methodFqn, null);
            if (!callers.isEmpty()) {
                callerMap.put(methodFqn, callers.stream()
                        .limit(10)
                        .map(r -> r.id().substring(r.project().length() + 1))
                        .collect(Collectors.toList()));
            }
        }

        return new ClassContext(cls, body, fields, constructors, publicMethods, callerMap);
    }

    /**
     * BFS over the inheritance graph returning all transitive subclasses of {@code classFqn}.
     */
    public List<String> getSubclassHierarchy(String classFqn) {
        List<String> result = new ArrayList<>();
        List<CallGraph> graphs = openAllGraphs();
        try {
            for (CallGraph g : graphs) result.addAll(g.allSubclasses(classFqn));
        } finally {
            for (CallGraph g : graphs) { try { g.close(); } catch (Exception ignored) {} }
        }
        return result;
    }
}
