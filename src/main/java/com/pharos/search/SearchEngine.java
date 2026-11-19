package com.pharos.search;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.GraphView;
import com.pharos.graph.GraphViews;
import com.pharos.search.SourceReader;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IncompatibleIndexException;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.routing.CatalogTooOldException;
import com.pharos.cluster.routing.CatalogUnavailableException;
import com.pharos.search.distributed.DistributedSearch;
import com.pharos.search.distributed.LocalShardService;
import com.pharos.search.pipeline.CrossEncoder;
import com.pharos.search.pipeline.NoOpCrossEncoder;
import com.pharos.search.pipeline.PipelineConfig;
import com.pharos.search.pipeline.PipelineDescriptor;
import com.pharos.search.pipeline.PipelineFactory;
import com.pharos.search.pipeline.SearchPipeline;
import com.pharos.search.pipeline.UnifiedRetrievalStage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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
    /**
     * Built on first use: pipeline selection asks the embedder / cross-encoder whether they are
     * available, which loads their models. Building in the constructor loaded both models in
     * every pharos process, including ones that only do graph lookups.
     */
    private final java.util.function.Supplier<PipelineFactory.Result> builtPipelines;

    /** Where the search-time models stand; see {@link #warmUpInBackground()}. */
    public enum ModelState {
        /** No embedding model is configured: keyword search only, nothing to load. */
        NONE,
        /** Configured and not loaded yet (or the warm-up has not run). */
        LOADING,
        READY,
        /** The configured model could not be loaded: vector search is unavailable. */
        FAILED
    }

    /**
     * How a warm-up settled.
     *
     * @param failure why the model is unavailable (FAILED), else null
     * @param millis  time from the start of the warm-up
     */
    public record ModelWarmup(ModelState state, String failure, long millis) {}

    /**
     * The fan-out coordinator of a reader node (Phase 3); null on every other node, which always
     * searches locally. Even when set, a query whose shards are all held here runs the local path.
     */
    private volatile DistributedSearch distributed;

    /** True when an embedding model is configured (a bare no-op provider means none is). */
    private final boolean modelConfigured;
    private final java.util.concurrent.CompletableFuture<ModelWarmup> warmup =
            new java.util.concurrent.CompletableFuture<>();
    private volatile ModelState modelState;
    private volatile String modelFailure;

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
        this(luceneIndexer, embedder, registry, crossEncoder, queryRouter,
                com.pharos.config.IndexConfig.DEFAULT_BASE);
    }

    /**
     * @param configBase where {@code synonyms.txt} and {@code pipelines.yaml} are read from
     *                   (the pharos home by default); tests pass their own to stay hermetic
     */
    public SearchEngine(LuceneIndexer luceneIndexer, EmbeddingProvider embedder,
                        ProjectRegistry registry, CrossEncoder crossEncoder,
                        QueryRouter queryRouter, java.nio.file.Path configBase) {
        this.luceneIndexer = luceneIndexer;
        this.registry      = registry;
        this.embedder      = embedder;
        this.queryRouter   = queryRouter;
        this.modelConfigured = !(embedder instanceof com.pharos.embedding.NoOpEmbeddingProvider);
        this.modelState = modelConfigured ? ModelState.LOADING : ModelState.NONE;

        KeywordSearchStrategy kw  = new KeywordSearchStrategy(
                configBase.resolve("synonyms.txt"), new org.apache.lucene.search.SearcherFactory());
        this.keywordStrategy = kw;
        VectorSearchStrategy  vec = new VectorSearchStrategy(embedder);
        this.vectorStrategy  = vec;
        this.zeroResultAdvisor = new ZeroResultAdvisor(luceneIndexer, registry, kw);

        this.builtPipelines = memoize(() ->
                new PipelineFactory(kw, vec, embedder, crossEncoder, queryRouter).build(loadPipelineConfigs(configBase)));
    }

    /** Makes this engine a coordinator for shards it does not hold (reader nodes only). */
    public void setDistributed(DistributedSearch distributed) {
        this.distributed = distributed;
    }

    /** Why a shard held here is degraded (null when it is not); readers only. */
    private volatile Function<String, String> localDegradation = shard -> null;

    /**
     * Reader nodes: why a shard this node holds is degraded (null when it is not). A search that
     * reads such a shard here carries a warning naming it.
     */
    public void setLocalDegradation(Function<String, String> reason) {
        this.localDegradation = java.util.Objects.requireNonNull(reason, "reason");
    }

    /** One warning per shard of {@code shards} that is degraded on this node. */
    private void warnDegraded(java.util.Collection<String> shards, List<String> warnings) {
        Function<String, String> why = localDegradation;
        for (String shard : shards) {
            String reason = why.apply(shard);
            if (reason == null) continue;
            String w = "shard " + shard + " is degraded on this reader: " + reason + "; its results may be incomplete";
            if (!warnings.contains(w)) warnings.add(w);
        }
    }

    /**
     * The shard side of fan-out over this engine's index, analysers and embedder: what a reader's
     * {@code /internal/*} endpoints and a coordinator's own shards run.
     *
     * @param serves           whether this node holds a shard
     * @param modelFingerprint the fingerprint of this node's search model's configuration, or null
     */
    public LocalShardService shardService(Predicate<String> serves, String modelFingerprint) {
        return new LocalShardService(luceneIndexer, serves, keywordStrategy, vectorStrategy,
                new UnifiedRetrievalStage(keywordStrategy, embedder), registry,
                new LocalShardService.Model(embedder.modelId(), embedder.dimensions(), modelFingerprint));
    }

    /** The query embedder (the coordinator embeds each query once and sends the vector). */
    public EmbeddingProvider embedder() {
        return embedder;
    }

    /** The vector strategy (snippet hints of a coordinator's own shards). */
    public VectorSearchStrategy vectorStrategy() {
        return vectorStrategy;
    }

    /** The projects a request names, in order; empty for "every project". */
    private static List<String> requestedProjects(SearchRequest req) {
        if (req.project() != null && !req.project().isEmpty()) return List.of(req.project());
        if (req.projects() != null && !req.projects().isEmpty()) return req.projects();
        return List.of();
    }

    /**
     * The fan-out of a graph or lookup call over {@code project} (null: every shard), or null when
     * this node holds every shard involved (then the local code runs, unchanged).
     */
    private record FanOut<T>(T value) {}

    /**
     * Runs {@code op} over the fan-out of a graph or lookup call over {@code project} (null: every
     * shard), adding its warnings; null when this node holds every shard involved (the local code
     * then runs, unchanged), or when the shard catalog was never read (local, with a warning).
     */
    private <T> FanOut<T> fannedOut(String project, List<String> warnings, Function<DistributedSearch.Query, T> op) {
        DistributedSearch ds = distributed;
        if (ds == null) return null;
        DistributedSearch.Query q;
        try {
            q = ds.open(project == null || project.isEmpty() ? List.of() : List.of(project));
        } catch (CatalogUnavailableException e) { // never read, or too old: this reader's shards
            warnings.add(catalogFallbackWarning(e));
            return null;
        }
        try (q) {
            if (!q.fansOut()) return null;
            T value = op.apply(q);
            warnings.addAll(q.warnings());
            return new FanOut<>(value);
        }
    }

    /**
     * The project names a query may mention ({@code project:query}, or a bare project name): this
     * node's projects plus, on a reader, every shard of the cluster catalog, so a coordinator boosts a
     * named project it does not hold exactly as a single node holding it would.
     */
    List<String> knownProjects() {
        Set<String> names = new java.util.LinkedHashSet<>();
        for (ProjectMeta m : registry.listAll()) if (m.getName() != null) names.add(m.getName());
        DistributedSearch ds = distributed;
        if (ds != null) names.addAll(ds.catalogShardNames());
        return List.copyOf(names);
    }

    static final String CATALOG_UNREAD_WARNING = "searching only this reader's shards: ";

    /**
     * The warning of an answer from this reader's shards alone because the catalog cannot route: why,
     * and, for a catalog too old, the shards of its last good read this reader does not hold.
     */
    private String catalogFallbackWarning(CatalogUnavailableException e) {
        String w = CATALOG_UNREAD_WARNING + e.getMessage();
        if (e instanceof CatalogTooOldException old) {
            Set<String> held = new java.util.HashSet<>();
            for (ProjectMeta m : registry.listAll()) if (m.getName() != null) held.add(m.getName());
            List<String> missing = old.lastKnown().stream().map(ShardId::value).filter(n -> !held.contains(n)).toList();
            if (missing.isEmpty()) { // this reader holds every shard of the last good catalog: nothing is lost
                return "catalog is " + old.ageSeconds() + " s old; serving this reader's shards (complete)";
            }
            w += "; not searched: " + String.join(", ", missing);
        }
        return w;
    }

    /** Returns the ordered list of available pipelines for UI display. */
    public List<PipelineDescriptor> listPipelines() {
        return builtPipelines.get().descriptors();
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
        QueryHints hints = parseQueryHints(req.query(), knownProjects());
        if (!hints.cleanedQuery().equals(req.query())) {
            req = new SearchRequest(hints.cleanedQuery(), req.type(), req.project(),
                    req.projects(), req.limit(), req.outputFormat(), req.docType(), req.scope(), req.oversampleFactor());
            log.debug("Query rewritten: '{}' → project={} lang={} query='{}'",
                    hints.cleanedQuery(), hints.projectBoost(), hints.langExtension(), hints.cleanedQuery());
        }

        List<String> warnings = new ArrayList<>();
        if (modelState == ModelState.FAILED && requiresVectors(req.type())) {
            warnings.add("vector search unavailable: " + modelFailure + "; vector results are missing");
        }
        List<String> projects;
        DistributedSearch ds = distributed;
        DistributedSearch.Query fanout = null;
        if (ds != null) {
            try {
                fanout = ds.open(requestedProjects(req));
            } catch (CatalogUnavailableException e) { // never read, or too old: as before fan-out, this reader's shards
                warnings.add(catalogFallbackWarning(e));
            }
        }
        if (fanout != null && fanout.fansOut()) {
            try (DistributedSearch.Query q = fanout) {
                warnDegraded(q.localShards(), warnings);
                return searchFannedOut(req, hints, expand, trace, warnings, q);
            }
        }
        if (fanout != null) fanout.close(); // routed, nothing pinned: released all the same
        if (fanout != null && !fanout.unservedShards().isEmpty()) {
            // Shards nobody serves are named, never silently dropped; the held ones search locally.
            warnings.addAll(fanout.warnings());
            Set<String> held = Set.copyOf(fanout.localShards());
            boolean explicit = !requestedProjects(req).isEmpty();
            List<String> heldProjects = resolveProjects(req).stream().filter(held::contains).toList();
            projects = usable(heldProjects, explicit && heldProjects.size() == 1, warnings);
        } else {
            projects = resolveUsable(req, warnings);
        }
        warnDegraded(projects, warnings);
        if (projects.isEmpty()) {
            if (warnings.isEmpty()) {
                log.warn("No indexed projects found. Run 'pharos index <path>' first.");
            }
            return new SearchResponse(List.of(), trace, req.type().name().toLowerCase(),
                    null, null, null, List.copyOf(warnings));
        }

        // Vector-shaped queries and projects without vectors of the configured search model: an
        // explicitly requested single project fails loud (silently empty results are the failure
        // mode an unsuspecting user would otherwise see after swapping searchEmbeddingModel without
        // re-indexing); any other search leaves those projects out of the vector part, with a
        // warning, and still runs the keyword part over them (as a cluster coordinator does).
        if (requiresVectors(req.type()) && embedder.isAvailable()) {
            // the REQUESTED count, as a coordinator decides: [a, b] with b unusable still only warns for a
            boolean explicitSingle = requestedProjects(req).size() == 1;
            for (String p : projectsWithoutSearchModel(projects)) {
                if (explicitSingle) throw new VectorModelUnavailableException(searchModelMissingMessage(p));
                warnings.add(noVectorsWarning(p, embedder.modelId()));
            }
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

        Map<SearchRequest.SearchType, SearchPipeline> pipelines = builtPipelines.get().pipelines();
        SearchPipeline pipeline = pipelines.getOrDefault(resolvedType,
                pipelines.get(SearchRequest.SearchType.HYBRID));
        List<SearchResult> primary = pipeline.execute(reader, req, trace);

        primary = applyProjectAffinityBoost(primary, req.project());
        primary = applyQueryHintBoosts(primary, hints);

        if (primary.isEmpty()) {
            ZeroResultAdvisor.Suggestions suggestions = zeroResultAdvisor.advise(req, projects);
            return new SearchResponse(List.of(), trace, resolvedTypeName, suggestions, null, null, List.copyOf(warnings));
        }

        // Build snippet resolvers lazily — called only for final results, not all candidates.
        SnippetResolver kwResolver  = keywordStrategy.buildKeywordResolver(
                keywordStrategy.buildQuery(new SearchRequest(
                        hints.cleanedQuery(), resolvedType, req.project(), req.projects(),
                        req.limit(), req.outputFormat(), req.docType(), req.scope(), req.oversampleFactor())));
        // Deferred: the query is embedded (and the model loaded) only when a snippet is actually
        // resolved — not for undecorated or empty responses.
        String snippetQuery = hints.cleanedQuery();
        SnippetResolver vecResolver = new SnippetResolver() {
            private volatile SnippetResolver inner;
            @Override public Snippet resolve(SearchResult r) {
                SnippetResolver i = inner;
                if (i == null) {
                    synchronized (this) {
                        i = inner;
                        if (i == null) {
                            try {
                                i = embedder.isAvailable()
                                        ? vectorStrategy.buildVectorResolver(reader, embedder.embed(snippetQuery))
                                        : SnippetResolver.none();
                            } catch (RuntimeException e) {
                                // keyword snippets still work; don't re-embed and fail for every result
                                log.warn("Vector snippet hints unavailable for this search: {}", e.getMessage());
                                i = SnippetResolver.none();
                            }
                            inner = i;
                        }
                    }
                }
                return i.resolve(r);
            }
        };

        if (!expand) {
            return new SearchResponse(primary, trace, resolvedTypeName, null, kwResolver, vecResolver, List.copyOf(warnings));
        }

        long tExpand = System.currentTimeMillis();
        List<SearchResult> expanded = expandNeighborhood(primary, req.project());
        trace.record("neighborhood expansion", tExpand);
        return new SearchResponse(expanded, trace, resolvedTypeName, null, kwResolver, vecResolver, List.copyOf(warnings));
    }

    /**
     * The coordinator path of {@link #searchWithTrace}: the same pipeline, with every retrieval stage
     * fanned out over the query's shards ({@link DistributedSearch}),
     * then the same boosts, snippets and expansion as the local path. Shards left out are named in
     * the warnings.
     */
    private SearchResponse searchFannedOut(SearchRequest req, QueryHints hints, boolean expand, SearchTrace trace,
                                           List<String> warnings,
                                           DistributedSearch.Query fanout)
            throws IOException {
        SearchRequest.SearchType resolvedType = req.type();
        final String resolvedTypeName = resolvedType.name().toLowerCase();
        if (req.oversampleFactor() == 0) {
            int adaptive = adaptiveOversample(resolvedType, hints.cleanedQuery());
            if (adaptive > 0) {
                req = new SearchRequest(req.query(), req.type(), req.project(), req.projects(),
                        req.limit(), req.outputFormat(), req.docType(), req.scope(), adaptive);
            }
        }
        Map<SearchRequest.SearchType, SearchPipeline> pipelines = builtPipelines.get().pipelines();
        SearchPipeline pipeline = pipelines.getOrDefault(resolvedType,
                pipelines.get(SearchRequest.SearchType.HYBRID));
        // Mergers and rerankers read result fields only: they get no reader here.
        List<SearchResult> primary = pipeline.execute(null, req, trace, fanout.runner());
        primary = applyProjectAffinityBoost(primary, req.project());
        primary = applyQueryHintBoosts(primary, hints);

        if (primary.isEmpty()) {
            warnings.addAll(fanout.warnings());
            // Suggestions from the shards held here (cheap: local terms only); none when it holds none.
            List<String> held = fanout.localShards();
            ZeroResultAdvisor.Suggestions suggestions = held.isEmpty() ? null : zeroResultAdvisor.advise(req, held);
            return new SearchResponse(List.of(), trace, resolvedTypeName, suggestions, null, null, List.copyOf(warnings));
        }
        SnippetResolver kwResolver = keywordStrategy.buildKeywordResolver(
                keywordStrategy.buildQuery(new SearchRequest(
                        hints.cleanedQuery(), resolvedType, req.project(), req.projects(),
                        req.limit(), req.outputFormat(), req.docType(), req.scope(), req.oversampleFactor())));
        SnippetResolver vecResolver = fanout.vectorResolver(primary, hints.cleanedQuery());
        if (expand) {
            long tExpand = System.currentTimeMillis();
            primary = fanout.expand(primary);
            trace.record("neighborhood expansion", tExpand);
        }
        warnings.addAll(fanout.warnings());
        return new SearchResponse(primary, trace, resolvedTypeName, null, kwResolver, vecResolver,
                List.copyOf(warnings));
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
        return getByFqn(fqn, new ArrayList<>());
    }

    /** As {@link #getMethodByFqn(String)}; skipped incompatible projects are explained in {@code warnings}. */
    public SearchResult getMethodByFqn(String fqn, List<String> warnings) throws IOException {
        return getByFqn(fqn, warnings);
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
        return findMethodsByPartialFqn(partialFqn, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public List<SearchResult> findMethodsByPartialFqn(String partialFqn, List<String> warnings) throws IOException {
        int hash = partialFqn.indexOf('#');
        if (hash < 0) return List.of();
        String className  = partialFqn.substring(0, hash);
        String methodName = partialFqn.substring(hash + 1);
        if (className.isEmpty() || methodName.isEmpty()) return List.of();

        List<String> projects = allUsable(warnings);
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
        return getClassByFqn(qualifiedClassName, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public SearchResult getClassByFqn(String qualifiedClassName, List<String> warnings) throws IOException {
        SearchResult r = getByFqn(qualifiedClassName, warnings);
        if (r == null || !"class".equals(r.docType())) return null;
        return r;
    }

    private SearchResult getByFqn(String fqn, List<String> warnings) throws IOException {
        FanOut<SearchResult> fanned = fannedOut(null, warnings, q -> q.docs(List.of(fqn)).get(fqn));
        if (fanned != null) return fanned.value();
        List<String> projects = allUsable(warnings);
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
        return getByFqns(fqns, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public Map<String, SearchResult> getByFqns(List<String> fqns, List<String> warnings) throws IOException {
        if (fqns.isEmpty()) return Map.of();
        FanOut<Map<String, SearchResult>> fanned = fannedOut(null, warnings, q -> q.docs(fqns));
        if (fanned != null) return fanned.value();

        List<String> projects = allUsable(warnings);
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
        return traceCallChain(rootFqn, depth, direction, maxBodyChars, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public CallChainResult traceCallChain(String rootFqn, int depth, String direction,
                                          int maxBodyChars, List<String> warnings) throws IOException {
        FanOut<BfsResult> fanned = fannedOut(null, warnings,
                q -> bfsLevels(level -> q.neighbors(level, direction), rootFqn, depth, 50));
        List<GraphView> graphs = fanned != null ? List.of() : openAllGraphs(null, warnings);
        try {
            BfsResult bfs = fanned != null ? fanned.value() : bfsCallGraph(graphs, rootFqn, depth, direction, 50);

            // Bulk-fetch bodies — one Lucene query for all visited FQNs.
            Map<String, SearchResult> bodies = getByFqns(new ArrayList<>(bfs.fqnDepth().keySet()), warnings);

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
            for (GraphView g : graphs) { try { g.close(); } catch (Exception ignored) {} }
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
    private List<GraphView> openAllGraphs() {
        return openAllGraphs(null, new ArrayList<>());
    }

    /**
     * As {@link #openAllGraphs()}, optionally scoped to one project. Projects whose index format
     * is incompatible are skipped (logged) when searching all; a scoped one rethrows.
     */
    private List<GraphView> openAllGraphs(String onlyProject, List<String> warnings) {
        List<GraphView> graphs = new ArrayList<>();
        for (ProjectMeta meta : registry.listAll()) {
            if (onlyProject != null && !onlyProject.equals(meta.getName())) continue;
            // GraphViews checks the index format, then opens existing graphs only: a project
            // without one is skipped, nothing is created.
            try {
                graphs.add(GraphViews.open(meta));
            } catch (IncompatibleIndexException e) {
                if (onlyProject != null) throw e;
                log.warn("Skipping call graph of '{}': {}", meta.getName(), e.getMessage());
                warnings.add(e.getMessage());
            } catch (Exception e) {
                log.warn("Skipping unreadable graph of project {}: {}", meta.getName(), e.toString());
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
    private static BfsResult bfsCallGraph(List<GraphView> graphs, String rootFqn,
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

    /**
     * {@link #bfsCallGraph} for a fan-out: the same traversal (FIFO order is level order), with the
     * neighbours of a whole level fetched in one call per shard.
     */
    private static BfsResult bfsLevels(Function<List<String>, Map<String, Set<String>>> neighbors,
                                       String rootFqn, int depth, int maxNodes) {
        Map<String, Integer>      fqnDepth    = new LinkedHashMap<>();
        Map<String, List<String>> fqnChildren = new LinkedHashMap<>();
        boolean truncated = false;
        fqnDepth.put(rootFqn, 0);
        List<String> level = List.of(rootFqn);
        outer:
        while (!level.isEmpty()) {
            int d = fqnDepth.get(level.get(0));
            if (d >= depth) break;
            Map<String, Set<String>> nb = neighbors.apply(level);
            List<String> next = new ArrayList<>();
            for (String current : level) {
                List<String> children = new ArrayList<>(nb.getOrDefault(current, Set.of()));
                fqnChildren.put(current, children);
                for (String neighbor : children) {
                    if (fqnDepth.containsKey(neighbor)) continue;
                    if (fqnDepth.size() >= maxNodes) { truncated = true; break outer; }
                    fqnDepth.put(neighbor, d + 1);
                    next.add(neighbor);
                }
            }
            level = next;
        }
        return new BfsResult(fqnDepth, fqnChildren, truncated);
    }

    /**
     * Callers (or callees) of {@code fqn} over a fan-out's call graphs; empty when this node holds
     * every shard (the caller then reads its local graphs as before).
     */
    public Optional<Set<String>> fannedOutNeighbors(String fqn, boolean callers, List<String> warnings) {
        FanOut<Set<String>> fanned = fannedOut(null, warnings,
                q -> q.neighbors(List.of(fqn), callers ? "callers" : "callees").getOrDefault(fqn, Set.of()));
        return fanned == null ? Optional.empty() : Optional.of(fanned.value());
    }

    /** The shortest call path over a fan-out's shards (first found in shard order); empty when all are held here. */
    public Optional<List<String>> fannedOutCallPath(String from, String to, List<String> warnings) {
        FanOut<List<String>> fanned = fannedOut(null, warnings, q -> q.callPath(from, to));
        return fanned == null ? Optional.empty() : Optional.of(fanned.value());
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
        return findTransitiveCallers(rootFqn, depth, maxCallers, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public TransitiveCallersResult findTransitiveCallers(String rootFqn, int depth,
                                                          int maxCallers, List<String> warnings) throws IOException {
        FanOut<BfsResult> fanned = fannedOut(null, warnings,
                q -> bfsLevels(level -> q.neighbors(level, "callers"), rootFqn, depth, Math.max(1, maxCallers + 1)));
        List<GraphView> graphs = fanned != null ? List.of() : openAllGraphs(null, warnings);
        try {
            // +1 to include root in the cap budget, then exclude it from results
            BfsResult bfs = fanned != null ? fanned.value() : bfsCallGraph(graphs, rootFqn, depth, "callers",
                    Math.max(1, maxCallers + 1));

            List<Map.Entry<String, Integer>> callers = bfs.fqnDepth().entrySet().stream()
                    .filter(e -> !e.getKey().equals(rootFqn))   // exclude root itself
                    .sorted(Comparator.comparingInt(Map.Entry<String, Integer>::getValue)
                            .thenComparing(Map.Entry::getKey))
                    .collect(Collectors.toList());

            return new TransitiveCallersResult(
                    rootFqn, depth, callers.size(), bfs.truncated(), callers);
        } finally {
            for (GraphView g : graphs) { try { g.close(); } catch (Exception ignored) {} }
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
    /**
     * Loads the search-time models (query embedder, cross-encoder) on a background daemon
     * thread, so a long-running server answers health and graph requests immediately and its
     * first search does not wait for the multi-second model load. One-shot CLI commands don't
     * call this and never load models they don't use. A search arriving mid-warm-up waits for
     * the same one-time load rather than starting a second one.
     *
     * <p>{@link #modelWarmup()} completes when it settles. A configured model that does not load
     * is logged at ERROR and settles as {@link ModelState#FAILED}: the node still becomes ready,
     * and vector searches carry a warning instead of silently returning nothing.
     */
    public Thread warmUpInBackground() {
        Thread t = new Thread(() -> {
            long t0 = System.nanoTime();
            ModelWarmup result;
            try {
                builtPipelines.get();                       // isAvailable() → loads embedder + cross-encoder
                if (!modelConfigured) {
                    result = new ModelWarmup(ModelState.NONE, null, elapsedMs(t0));
                } else if (!embedder.isAvailable()) {
                    result = new ModelWarmup(ModelState.FAILED, "embedding model '" + embedder.modelId()
                            + "' failed to load", elapsedMs(t0));
                } else {
                    embedder.embed("warm up");              // first ONNX run initialises the session
                    result = new ModelWarmup(ModelState.READY, null, elapsedMs(t0));
                }
            } catch (Throwable e) {
                result = new ModelWarmup(modelConfigured ? ModelState.FAILED : ModelState.NONE,
                        modelConfigured ? "embedding model '" + embedder.modelId() + "' failed to load: " + e
                                : null, elapsedMs(t0));
                if (!modelConfigured) log.warn("Model warm-up failed: {}", e.toString());
            }
            settle(result);
        }, "pharos-model-warmup");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private void settle(ModelWarmup result) {
        modelFailure = result.failure();
        modelState = result.state();
        if (result.state() == ModelState.FAILED) {
            log.error("SEARCH MODEL UNAVAILABLE after {} ms: {}. Vector and hybrid search answer without vectors"
                    + " (each such search carries a warning); keyword search is unaffected.",
                    result.millis(), result.failure());
        } else {
            log.info("Search models ready ({} ms)", result.millis());
        }
        warmup.complete(result);
    }

    private static long elapsedMs(long t0) {
        return (System.nanoTime() - t0) / 1_000_000;
    }

    /** Completes when {@link #warmUpInBackground()} has settled; never completes if it never runs. */
    public java.util.concurrent.CompletableFuture<ModelWarmup> modelWarmup() {
        return warmup;
    }

    /**
     * {@link #modelWarmup()}, or after {@code max} a {@link ModelState#FAILED} result naming the
     * timeout (logged at ERROR), so a hanging load can never hold a node's readiness forever. The
     * load itself keeps running and the engine's {@link #modelState()} follows it.
     */
    public java.util.concurrent.CompletableFuture<ModelWarmup> modelsSettled(java.time.Duration max) {
        java.util.concurrent.CompletableFuture<ModelWarmup> out = new java.util.concurrent.CompletableFuture<>();
        warmup.whenComplete((r, e) -> out.complete(r));
        java.util.concurrent.CompletableFuture.delayedExecutor(max.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                .execute(() -> {
                    if (out.complete(new ModelWarmup(ModelState.FAILED, "model warm-up timed out after "
                            + max.toMillis() / 1000.0 + " s", max.toMillis()))) {
                        log.error("SEARCH MODEL WARM-UP TIMED OUT after {} s: becoming ready anyway; searches that"
                                + " need the model wait for it until it loads", max.toMillis() / 1000.0);
                    }
                });
        return out;
    }

    public ModelState modelState() {
        return modelState;
    }

    /** Thread-safe, compute-once supplier. */
    private static <T> java.util.function.Supplier<T> memoize(java.util.function.Supplier<T> s) {
        return new java.util.function.Supplier<>() {
            private volatile T value;
            @Override public T get() {
                T v = value;
                if (v == null) {
                    synchronized (this) {
                        v = value;
                        if (v == null) value = v = s.get();
                    }
                }
                return v;
            }
        };
    }

    /**
     * Loads pipeline configuration from {@code ~/.pharos/pipelines.yaml} if it exists,
     * otherwise falls back to the bundled {@code pipelines.yaml} on the classpath.
     * This lets operators customise or extend the pipeline set without recompiling.
     */
    private static List<PipelineConfig> loadPipelineConfigs(java.nio.file.Path configBase) {
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        try {
            java.nio.file.Path external = configBase.resolve("pipelines.yaml");
            java.io.InputStream in;
            if (java.nio.file.Files.isReadable(external)) {
                log.info("Loading pipeline config from {}", external);
                in = java.nio.file.Files.newInputStream(external);
            } else {
                in = SearchEngine.class.getClassLoader().getResourceAsStream("pipelines.yaml");
                if (in == null) throw new IllegalStateException("pipelines.yaml not found on classpath");
            }
            try (in) {
                return yaml.readValue(in, PipelineConfig.Root.class).pipelines();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load pipeline configuration", e);
        }
    }

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

    private static Set<String> neighborsFromGraphs(List<GraphView> graphs,
                                                    String fqn, String direction) {
        Set<String> result = new LinkedHashSet<>();
        for (GraphView g : graphs) {
            try {
                if (!"callees".equals(direction)) result.addAll(g.callers(fqn));
                if (!"callers".equals(direction)) result.addAll(g.callees(fqn));
            } catch (Exception ignored) {}
        }
        return result;
    }

    /** Find all methods that call the given FQN (reverse lookup via calledMethods field). */
    public List<SearchResult> findCallers(String calleeFqn, String project) throws IOException {
        return findCallers(calleeFqn, project, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public List<SearchResult> findCallers(String calleeFqn, String project, List<String> warnings) throws IOException {
        FanOut<List<SearchResult>> fanned = fannedOut(project, warnings, q -> q.callers(calleeFqn, 100));
        if (fanned != null) return fanned.value();
        SearchRequest req = SearchRequest.keyword("", project, 100);
        List<String> projects = resolveUsable(req, warnings);
        if (projects.isEmpty()) return List.of();

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);

        TermQuery calleeQuery = new TermQuery(new Term(DocumentMapper.F_CALLED_METHODS, calleeFqn));
        TopDocs hits = searcher.search(calleeQuery, 100);
        return KeywordSearchStrategy.toResults(searcher, hits, "graph");
    }

    /** Find all methods that the given FQN calls (forward lookup via calledMethods field). */
    public List<SearchResult> findCallees(String callerFqn, String project) throws IOException {
        return findCallees(callerFqn, project, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public List<SearchResult> findCallees(String callerFqn, String project, List<String> warnings) throws IOException {
        SearchResult caller = getMethodByFqn(callerFqn, warnings);
        if (caller == null) return List.of();
        // callee fqns from the shard holding the caller, their documents from any shard
        FanOut<List<String>> fanned = fannedOut(project, warnings, q -> q.calleeFqns(callerFqn));
        if (fanned != null) {
            List<String> called = fanned.value();
            Map<String, SearchResult> docs = getByFqns(called, warnings);
            return called.stream().map(docs::get).filter(r -> r != null).collect(Collectors.toList());
        }

        // The calledMethods field stores FQNs; we look them up directly
        // For now return the caller with its callees via a direct document lookup
        List<String> projects = resolveUsable(SearchRequest.keyword("", project, 100), warnings);
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

    /**
     * Returns true for query types that route through a vector-search stage and
     * therefore need a vector field present in the index.
     */
    private static boolean requiresVectors(SearchRequest.SearchType type) {
        return switch (type) {
            case VECTOR, HYBRID, HYBRID_RERANKED, HYBRID_CROSS_ENCODER_MERGE,
                 HYBRID_DIVERSE, HYBRID_RERANKED_DIVERSE, UNIFIED -> true;
            default -> false;  // KEYWORD, AUTO (which may resolve to keyword)
        };
    }

    /**
     * The resolved projects whose {@code embeddedModels} lack the model id the
     * configured search embedder uses. Pre-upgrade indexes have an empty
     * {@code embeddedModels} list and are allowed only when the search embedder
     * is the synthesized legacy one — they're served from the legacy
     * {@code vectorEmbedding} field.
     */
    private List<String> projectsWithoutSearchModel(List<String> projects) {
        List<String> out = new ArrayList<>();
        for (String p : projects) {
            ProjectMeta meta = registry.find(p).orElse(null);
            if (meta == null) continue;  // shouldn't happen; resolveProjects already filtered
            if (!hasSearchModel(meta)) out.add(p);
        }
        return out;
    }

    private boolean hasSearchModel(ProjectMeta meta) {
        String modelId = embedder.modelId();
        List<String> embeddedModels = meta.getEmbeddedModels();
        return embeddedModels.contains(modelId)
                || (embeddedModels.isEmpty() && com.pharos.config.IndexConfig.LEGACY_MODEL_ID.equals(modelId));
    }

    private String searchModelMissingMessage(String project) {
        String modelId = embedder.modelId();
        List<String> embeddedModels = registry.find(project).map(ProjectMeta::getEmbeddedModels).orElse(List.of());
        String available = embeddedModels.isEmpty() ? "<legacy / none recorded>" : String.join(", ", embeddedModels);
        return String.format("Project '%s' has no vectors for search embedding model '%s'. "
                + "Available model(s): %s. Run: pharos embed --model=%s %s", project, modelId, available, modelId, project);
    }

    /**
     * The warning of a search that leaves {@code project} out of its vector part because it has no
     * vectors of {@code modelId}: the same text on a single node and on a cluster coordinator.
     */
    public static String noVectorsWarning(String project, String modelId) {
        return "project " + project + " has no vectors for model " + modelId + "; vector results exclude it";
    }

    /**
     * Drops projects whose index format is incompatible, appending the explanation of each to
     * {@code warnings}. An explicitly requested single project that is incompatible rethrows.
     */
    private List<String> usable(List<String> projects, boolean explicitSingle, List<String> warnings) {
        List<String> ok = new ArrayList<>(projects.size());
        for (String p : projects) {
            try {
                IndexFormat.check(p, luceneIndexer.getProjectIndexDir(p));
                ok.add(p);
            } catch (IncompatibleIndexException e) {
                if (explicitSingle) throw e;
                warnings.add(e.getMessage());
            }
        }
        return ok;
    }

    private List<String> resolveUsable(SearchRequest req, List<String> warnings) {
        boolean explicit = (req.project() != null && !req.project().isEmpty())
                || (req.projects() != null && !req.projects().isEmpty());
        List<String> projects = resolveProjects(req);
        return usable(projects, explicit && projects.size() == 1, warnings);
    }

    /** Every registered project with a compatible index; skipped ones are explained in {@code warnings}. */
    private List<String> allUsable(List<String> warnings) {
        return usable(registry.listAll().stream().map(ProjectMeta::getName).collect(Collectors.toList()),
                false, warnings);
    }

    private List<String> allUsable() {
        return allUsable(new ArrayList<>());
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
        return findUsages(fqn, kind, null);
    }

    /** As {@link #findUsages(String, String)}, scoped to one project when {@code project} is non-null. */
    public UsageResult findUsages(String fqn, String kind, String project) throws IOException {
        return findUsages(fqn, kind, project, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public UsageResult findUsages(String fqn, String kind, String project, List<String> warnings) throws IOException {
        boolean all = "all".equals(kind);
        List<String> callers        = new ArrayList<>();
        List<String> subclasses     = new ArrayList<>();
        List<String> superTypes     = new ArrayList<>();
        List<String> fieldReaders   = new ArrayList<>();
        List<String> fieldWriters   = new ArrayList<>();
        List<String> annotatedWith  = new ArrayList<>();
        List<String> returning      = new ArrayList<>();
        List<String> taking         = new ArrayList<>();

        FanOut<Map<String, List<String>>> fanned = fannedOut(project, warnings, q -> q.usages(fqn, kind));
        if (fanned != null) {
            Map<String, List<String>> u = fanned.value();
            return new UsageResult(fqn, u.getOrDefault("callers", List.of()),
                    u.getOrDefault("subclasses", List.of()), u.getOrDefault("superTypes", List.of()),
                    u.getOrDefault("fieldReaders", List.of()), u.getOrDefault("fieldWriters", List.of()),
                    u.getOrDefault("annotatedWith", List.of()), u.getOrDefault("methodsReturning", List.of()),
                    u.getOrDefault("methodsTaking", List.of()));
        }
        List<GraphView> graphs = openAllGraphs(project, warnings);
        try {
            for (GraphView g : graphs) {
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
            for (GraphView g : graphs) { try { g.close(); } catch (Exception ignored) {} }
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
        return getClassContext(classFqn, new ArrayList<>());
    }

    /** As above; skipped incompatible projects are explained in {@code warnings}. */
    public ClassContext getClassContext(String classFqn, List<String> warnings) throws IOException {
        SearchResult cls = getClassByFqn(classFqn, warnings);
        if (cls == null) return null;

        // Full body from source file (same logic as MCP get_class)
        String body = SourceReader.readRange(cls.filePath(), cls.startLine(), cls.endLine());
        if (body == null) body = cls.body();

        // All method documents for this class from Lucene.
        // F_QUALIFIED_CLASS is a TextField (analyzed), so TermQuery on a dotted FQN never matches.
        // Instead, use PrefixQuery on the StringField F_ID ("project:FQN#") which is exact.
        List<String> projects = allUsable(warnings);
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
        List<GraphView> kgGraphs = openAllGraphs(null, warnings);
        try {
            for (GraphView g : kgGraphs) fields.addAll(g.getFieldsOf(classFqn));
        } finally {
            for (GraphView g : kgGraphs) { try { g.close(); } catch (Exception ignored) {} }
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
        List<GraphView> graphs = openAllGraphs();
        try {
            for (GraphView g : graphs) result.addAll(g.allSubclasses(classFqn));
        } finally {
            for (GraphView g : graphs) { try { g.close(); } catch (Exception ignored) {} }
        }
        return result;
    }
}
