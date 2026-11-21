package com.pharos.web;

import com.pharos.indexer.IncompatibleIndexException;
import com.pharos.indexer.IndexFormat;
import com.pharos.cluster.NodeRole;
import com.pharos.config.IndexConfig;
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
    /** Readers have no module graph yet (it is not replicated): module endpoints answer 501. */
    private final boolean readerNode;
    private final ObjectMapper mapper;
    /** False while a reader's first sync is running; see {@link #setReadiness}. */
    private volatile java.util.function.BooleanSupplier readiness = () -> true;
    private volatile java.util.function.Supplier<Map<String, Object>> readerCacheStats = () -> null;
    /** Why a ready node can't answer every query, or null; see {@link #setDegradation}. */
    private volatile java.util.function.Supplier<String> degradation = () -> null;
    private volatile java.util.function.Supplier<Map<String, Object>> writerDisk = () -> null;
    /** A peer-pull reader's writers, conflicts and unavailable shards; null result: omitted. */
    private volatile java.util.function.Supplier<Map<String, Object>> peers = () -> null;
    private volatile java.util.function.Supplier<java.util.List<String>> degradedShards = java.util.List::of;
    private volatile java.util.function.BooleanSupplier draining = () -> false;
    /** Requests in flight for {@link #awaitIdle}; guarded by itself (notified on every decrement). */
    private final int[] active = new int[1];
    private volatile java.util.function.Supplier<java.util.List<com.pharos.cluster.membership.ReaderRegistry.LiveReader>> liveReaders = null;
    private volatile com.pharos.cluster.internal.InternalApi internalApi = null;
    private final com.pharos.cluster.membership.LoadCounter load = new com.pharos.cluster.membership.LoadCounter();
    /** Source of {@code /cluster/status} and {@code /cluster/events}; without one both answer 404. */
    private volatile com.pharos.cluster.status.StatusService clusterStatus = null;
    /** {@code /replication/*} on a peer-mode writer; null elsewhere (the routes stay absent). */
    private volatile com.pharos.cluster.peer.ReplicationEndpoint replication = null;
    /** {@code cluster.tls}: when set, the server listens with TLS only. */
    private volatile Tls tls = null;

    /** The keystore the TLS connector serves from. */
    public record Tls(Path keystore, String type, char[] password) {
        public Tls {
            java.util.Objects.requireNonNull(keystore, "keystore");
            type = type == null || type.isBlank() ? "PKCS12" : type;
            java.util.Objects.requireNonNull(password, "password");
        }
    }

    /**
     * Listen with TLS only (a Jetty SSL connector on the port; plain HTTP connections are refused at
     * the handshake). Without it the server speaks plain HTTP.
     */
    public void setTls(Tls tls) {
        this.tls = java.util.Objects.requireNonNull(tls, "tls");
    }

    /**
     * A peer-pull reader's writer view reported by {@code /health} as {@code peers} (writers,
     * {@code shard X claimed by A and B} conflicts, unavailable shards). Informational: it changes
     * neither the status nor readiness.
     */
    public void setPeers(java.util.function.Supplier<Map<String, Object>> peers) {
        this.peers = java.util.Objects.requireNonNull(peers, "peers");
    }

    /** Mounts {@code /replication/*} (a peer-mode writer). */
    public void setReplicationEndpoint(com.pharos.cluster.peer.ReplicationEndpoint endpoint) {
        this.replication = java.util.Objects.requireNonNull(endpoint, "endpoint");
    }

    /** The connector's idle timeout when set; null keeps Jetty's default (30 s). */
    private volatile java.time.Duration idleTimeout = null;

    /**
     * Overrides the idle timeout of every connection (Jetty's default is 30 s). A {@code latest}
     * long-poll raises it for its own connection only, for as long as it waits.
     */
    public void setIdleTimeout(java.time.Duration timeout) {
        this.idleTimeout = java.util.Objects.requireNonNull(timeout, "timeout");
    }

    /** Checks a presented cluster token (constant time); without one, no request gets the token view. */
    private volatile java.util.function.Predicate<String> clusterToken = t -> false;

    public WebServer(SearchEngine searchEngine, ProjectRegistry registry,
                     ModuleGraphBuilder moduleGraphBuilder, LuceneIndexer luceneIndexer,
                     ModuleBoundaryAnalyzer boundaryAnalyzer) {
        this(searchEngine, registry, moduleGraphBuilder, luceneIndexer, boundaryAnalyzer, NodeRole.ALL);
    }

    public WebServer(SearchEngine searchEngine, ProjectRegistry registry,
                     ModuleGraphBuilder moduleGraphBuilder, LuceneIndexer luceneIndexer,
                     ModuleBoundaryAnalyzer boundaryAnalyzer, NodeRole role) {
        this.readerNode = role == NodeRole.READER;
        this.searchEngine = searchEngine;
        this.registry = registry;
        this.moduleGraphBuilder = moduleGraphBuilder;
        this.luceneIndexer = luceneIndexer;
        this.boundaryAnalyzer = boundaryAnalyzer;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Readiness reported by {@code /health} (HTTP 503 {@code {"status":"warming","ready":false}}
     * until it turns true) and warned about on searches answered before it does.
     */
    public void setReadiness(java.util.function.BooleanSupplier readiness) {
        this.readiness = java.util.Objects.requireNonNull(readiness, "readiness");
    }

    /** Reader cache stats reported by {@code /health} as {@code readerCache}; null result: omitted. */
    /**
     * A work-queue writer's disk guard ({@code freeBytes}, {@code totalBytes}, {@code low}), reported
     * by {@code /health} as {@code writerDisk}. Informational: low disk never fails readiness and
     * doesn't change {@code status} (the writer keeps serving its last publishes).
     */
    public void setWriterDisk(java.util.function.Supplier<Map<String, Object>> disk) {
        this.writerDisk = java.util.Objects.requireNonNull(disk, "disk");
    }

    public void setReaderCacheStats(java.util.function.Supplier<Map<String, Object>> stats) {
        this.readerCacheStats = java.util.Objects.requireNonNull(stats, "stats");
    }

    /**
     * Degradation reported by {@code /health}: while it returns a reason (non-null), a ready node
     * answers HTTP 200 {@code {"status":"degraded","reason":…}} (a lazy reader whose served generation
     * was garbage-collected on the bucket); see {@link #setDegradedShards}.
     */
    public void setDegradation(java.util.function.Supplier<String> reason) {
        this.degradation = java.util.Objects.requireNonNull(reason, "reason");
    }

    /**
     * The shards this reader can't serve fully, reported by {@code /health} as {@code degradedShards}
     * with status {@code degraded} and HTTP 200: the node stays routable for its other shards (the
     * coordinators' routers skip it for these shards only, from its heartbeat).
     */
    public void setDegradedShards(java.util.function.Supplier<java.util.List<String>> shards) {
        this.degradedShards = java.util.Objects.requireNonNull(shards, "shards");
    }

    /**
     * Draining reported by {@code /health}: while it returns true the node answers HTTP 503
     * {@code {"status":"draining"}} (whatever else holds), so the router and load balancers stop
     * sending it requests; requests already running, and any still arriving, are served.
     */
    public void setDraining(java.util.function.BooleanSupplier draining) {
        this.draining = java.util.Objects.requireNonNull(draining, "draining");
    }

    /** Requests being served now ({@code /api/*}, {@code /internal/*}, {@code /cluster/*}); what a drain waits for. */
    public int inFlightRequests() {
        synchronized (active) {
            return active[0];
        }
    }

    /** Waits up to {@code max} for {@link #inFlightRequests} to reach 0; true when it did. */
    public boolean awaitIdle(java.time.Duration max) {
        long deadline = System.nanoTime() + max.toNanos();
        synchronized (active) {
            while (active[0] > 0) {
                long left = deadline - System.nanoTime();
                if (left <= 0) return false;
                try {
                    java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(active, left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Paths whose requests count as in-flight work for a drain (not {@code /health}, static files or
     * the long-lived {@code /cluster/events} stream, which a drain would otherwise wait out).
     */
    static boolean drainCounted(String path) {
        if (path.equals(EVENTS_PATH)) return false;
        return path.startsWith("/api/") || path.startsWith("/internal/") || path.startsWith("/cluster/");
    }

    static final String EVENTS_PATH = "/cluster/events";

    /**
     * Source of {@code GET /cluster/status} (the status JSON) and {@code GET /cluster/events}
     * (server-sent events), served on every role. Without the cluster token the status is the public
     * view (no reader {@code advertiseUrl}); see {@link #setClusterToken}.
     */
    public void setClusterStatus(com.pharos.cluster.status.StatusService status) {
        this.clusterStatus = java.util.Objects.requireNonNull(status, "status");
    }

    /**
     * How {@code /cluster/status} checks {@code X-Pharos-Cluster-Token} for the token view (with the
     * readers' {@code advertiseUrl}): a constant-time matcher, see
     * {@link com.pharos.cluster.internal.InternalApi#tokenMatcher}.
     */
    public void setClusterToken(java.util.function.Predicate<String> matcher) {
        this.clusterToken = java.util.Objects.requireNonNull(matcher, "matcher");
    }

    /** Request load counter the reader heartbeat reports ({@code inFlight}, {@code qps}). */
    public com.pharos.cluster.membership.LoadCounter load() { return load; }

    /**
     * The {@code /internal/*} shard API. Mounted on reader nodes only: on any other role the API is
     * ignored and the routes stay absent (404).
     */
    public void setInternalApi(com.pharos.cluster.internal.InternalApi api) {
        if (!readerNode) {
            log.warn("ignoring the internal shard API: /internal/* is served by reader nodes only");
            return;
        }
        this.internalApi = java.util.Objects.requireNonNull(api, "api");
    }

    /**
     * Source of {@code GET /cluster/readers}; without one the endpoint answers 404. It requires the
     * cluster token ({@code X-Pharos-Cluster-Token}, checked by the internal API): 401 otherwise.
     */
    public void setLiveReaders(java.util.function.Supplier<java.util.List<com.pharos.cluster.membership.ReaderRegistry.LiveReader>> s) {
        this.liveReaders = java.util.Objects.requireNonNull(s, "s");
    }

    /** {@code {readers:[{node, advertiseUrl, shards, draining, state, degradedShards, ageSeconds}]}}. */
    Map<String, Object> readersPayload() {
        java.util.List<Map<String, Object>> rows = new java.util.ArrayList<>();
        for (var l : liveReaders.get()) {
            var h = l.heartbeat();
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("node", h.node());
            r.put("advertiseUrl", h.advertiseUrl());
            r.put("shards", h.shards());
            r.put("draining", h.draining());
            r.put("state", h.state());
            r.put("degradedShards", h.degradedShards());
            r.put("ageSeconds", l.ageSeconds());
            rows.add(r);
        }
        return Map.of("readers", rows);
    }

    /** One {@code /health} answer: the HTTP status and the payload, from one read of each signal. */
    record HealthAnswer(int status, Map<String, Object> body) {}

    /**
     * {@code /health}, one code path for every state:
     * <ul>
     *   <li>503 {@code draining} once a reader is shutting down (it finishes its in-flight requests);</li>
     *   <li>503 {@code warming} while a reader's first sync or model load runs;</li>
     *   <li>200 {@code degraded} (with {@code degradedShards} and a {@code reason}) while a ready reader
     *       can't fully serve some shards: it stays routable for the others, and routers skip it for
     *       those shards only;</li>
     *   <li>200 {@code ok} otherwise.</li>
     * </ul>
     * On a work-queue writer the body also has {@code writerDisk: {freeBytes, totalBytes, low}}
     * (additive and informational: low disk changes neither the status nor readiness).
     * The 503 keeps load balancers and the router from sending traffic. A client that only wants
     * liveness treats any answer as "running". Every signal is read once.
     */
    HealthAnswer healthAnswer() {
        boolean drain = draining.getAsBoolean();
        boolean ready = readiness.getAsBoolean();
        if (drain) { // wins over every other state: the node is going away
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("status", "draining");
            out.put("ready", ready);
            return new HealthAnswer(503, out);
        }
        String reason = ready ? degradation.get() : null;
        java.util.List<String> shards = ready ? degradedShards.get() : java.util.List.of();
        boolean degraded = reason != null || (shards != null && !shards.isEmpty());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", !ready ? "warming" : degraded ? "degraded" : "ok");
        out.put("ready", ready);
        if (shards != null && !shards.isEmpty()) out.put("degradedShards", shards);
        if (reason != null) out.put("reason", reason);
        Map<String, Object> cache = readerCacheStats.get();
        if (cache != null) out.put("readerCache", cache);
        Map<String, Object> disk = writerDisk.get();
        if (disk != null) out.put("writerDisk", disk);
        Map<String, Object> peerView = peers.get();
        if (peerView != null) out.put("peers", peerView);
        return new HealthAnswer(ready ? 200 : 503, out);
    }

    /** The {@code /health} payload; see {@link #healthAnswer}. */
    Map<String, Object> health() {
        return healthAnswer().body();
    }

    static final String SYNCING_WARNING =
            "reader is still warming up (first sync or model load); results may be incomplete";

    /** {@code /health}'s HTTP status (see {@link #healthAnswer}). */
    int healthStatus() {
        return healthAnswer().status();
    }

    /** Module endpoints need the module graph, which reader nodes do not have yet: 501. */
    private boolean refuseOnReader(io.javalin.http.Context ctx) {
        if (!readerNode) return false;
        ctx.status(501).result(ctx.path() + " is not available on reader nodes yet");
        return true;
    }

    public Javalin start(int port) {
        return start(port, IndexConfig.DEFAULT_BASE);
    }

    /** @param pharosDir directory that receives the usage log ({@code logs/usage.jsonl}) */
    public Javalin start(int port, Path pharosDir) {
        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson(mapper, false));
            config.staticFiles.add("/web", Location.CLASSPATH);
            // Global on a reader: Javalin has one request-size cap, so every route (not only /internal/*)
            // accepts bodies up to 4 MiB there (other roles keep Javalin's default).
            if (readerNode) config.http.maxRequestSize = com.pharos.cluster.internal.InternalApi.MAX_BODY_BYTES;
            Tls t = tls;
            java.time.Duration idle = idleTimeout;
            if (t != null || idle != null) {
                // Our own connector replaces Javalin's default one (which it adds only when none is set).
                config.jetty.addConnector((server, http) -> connector(server, http, port, t, idle));
            }
        });

        // ── Usage logging — one JSONL line per /api/* request to logs/usage.jsonl ──
        UsageLogger usage = new UsageLogger(pharosDir);
        app.before(ctx -> {
            if (drainCounted(ctx.path())) {
                ctx.attribute("drain.counted", Boolean.TRUE);
                synchronized (active) {
                    active[0]++;
                }
            }
            if (ctx.path().startsWith("/api/")) {
                ctx.attribute("usage.start", System.currentTimeMillis());
                ctx.attribute("load.counted", Boolean.TRUE);
                ctx.attribute("load.start", System.nanoTime());
                load.begin();
            }
        });
        app.after(ctx -> {
            if (ctx.attribute("drain.counted") != null) {
                ctx.attribute("drain.counted", null);
                synchronized (active) {
                    active[0]--;
                    active.notifyAll();
                }
            }
            if (ctx.attribute("load.counted") != null) {
                ctx.attribute("load.counted", null);
                Long began = ctx.attribute("load.start");
                load.end(began == null ? -1 : System.nanoTime() - began); // the warm p95 window
            }
            Long start = ctx.attribute("usage.start");
            if (start == null) return;          // not an /api/* call, or before-handler skipped it
            long latency = System.currentTimeMillis() - start;
            // Collapse Map<String,List<String>> -> Map<String,String> (first value); most
            // params are single-valued, and the log stays human-readable as one-liners.
            Map<String, String> params = new LinkedHashMap<>();
            ctx.queryParamMap().forEach((k, vs) ->
                    params.put(k, vs == null || vs.isEmpty() ? "" : vs.get(0)));
            Integer resultCount       = ctx.attribute("usage.resultCount");
            @SuppressWarnings("unchecked")
            List<String> resultIds    = ctx.attribute("usage.resultIds");
            usage.log(UsageLogger.Entry.of(
                    ctx.path(),
                    ctx.method().name(),
                    params,
                    latency,
                    ctx.statusCode(),
                    resultCount,
                    resultIds,
                    ctx.userAgent()
            ));
        });

        // Incompatible index format → 409 with a machine-readable body
        app.exception(IncompatibleIndexException.class, (e, ctx) -> {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("error", e.getMessage());
            body.put("project", e.project());
            body.put("status", e.status().wireName());
            ctx.status(409).json(body);
        });

        // A search scoped to one project without vectors of the search model → 409 (a conflict
        // between the request and the index, as an incompatible index), never a generic 500
        app.exception(com.pharos.search.VectorModelUnavailableException.class, (e, ctx) ->
                ctx.status(409).json(Map.of("error", e.getMessage())));

        // Fan-out routing (reader nodes): a project name that is no shard id → 400; the shard
        // catalog unreadable → 503
        app.exception(com.pharos.cluster.routing.InvalidProjectException.class, (e, ctx) ->
                ctx.status(400).json(Map.of("error", e.getMessage())));
        app.exception(com.pharos.cluster.routing.CatalogUnavailableException.class, (e, ctx) ->
                ctx.status(503).json(Map.of("error", e.getMessage())));
        // A fanned-out call none of whose shards answered (none held here) → 503 with the reasons, so
        // pharos router retries it once on another reader instead of passing on an empty answer
        app.exception(com.pharos.cluster.routing.NoShardAnsweredException.class, (e, ctx) ->
                ctx.status(503).json(Map.of("error", e.getMessage(), "warnings", e.warnings())));

        // Project without a call graph (never indexed with one, or a failed build) → 404
        app.exception(com.pharos.graph.GraphNotFoundException.class, (e, ctx) ->
                ctx.status(404).json(Map.of("error", e.getMessage())));

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
        app.get("/api/projects", ctx -> {
            List<ProjectMeta> projects = registry.listAll();
            recordResults(ctx, projects.size(),
                    projects.stream().map(ProjectMeta::getName).collect(Collectors.toList()));
            ctx.json(projects.stream().map(this::projectToMap).collect(Collectors.toList()));
        });

        // API: module dependency graph
        app.get("/api/graph/modules", ctx -> {
            if (refuseOnReader(ctx)) return;
            ctx.json(buildModuleGraphData());
        });

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
            if (!readiness.getAsBoolean()) {
                log.warn("Search '{}' answered before the first sync finished", q);
                warningsHeader(ctx, List.of(SYNCING_WARNING)); // kept on a 409/500 answer too
            }
            try {
                SearchResponse resp = searchEngine.searchWithTrace(req, false);
                List<SearchResult> decorated = searchEngine
                        .newSnippetDecorator(snippetLines, resp)
                        .decorate(resp.results(), q);
                recordResults(ctx, decorated.size(),
                        decorated.stream().map(SearchResult::id).collect(Collectors.toList()));
                List<Map<String, Object>> results = decorated.stream()
                        .map(this::resultToMap).collect(Collectors.toList());
                boolean needsEnvelope = trace || !resp.warnings().isEmpty()
                        || (resp.suggestions() != null && !resp.suggestions().isEmpty());
                if (needsEnvelope) {
                    Map<String, Object> out = new LinkedHashMap<>();
                    out.put("results", results);
                    out.put("warnings", resp.warnings());
                    if (trace) {
                        Map<String, Object> meta = new LinkedHashMap<>();
                        meta.put("requestedType", type);
                        meta.put("resolvedType",  resp.resolvedType() != null ? resp.resolvedType() : type);
                        meta.put("totalMs",       resp.trace().totalMs());
                        meta.put("stages", resp.trace().spans().stream().map(s -> {
                            Map<String, Object> stage = new LinkedHashMap<>();
                            stage.put("name", s.name()); stage.put("ms", s.durationMs()); return stage;
                        }).collect(Collectors.toList()));
                        var cls = resp.trace().getClassification();
                        if (cls != null) {
                            meta.put("classIntent",  cls.intent());
                            meta.put("classType",    cls.type().name().toLowerCase());
                            if (cls.docType() != null) meta.put("classDocType", cls.docType());
                        }
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
            } catch (IncompatibleIndexException | com.pharos.cluster.routing.InvalidProjectException
                     | com.pharos.cluster.routing.CatalogUnavailableException
                     | com.pharos.cluster.routing.NoShardAnsweredException
                     | com.pharos.search.VectorModelUnavailableException e) {
                throw e;  // handled by the 409 exception mapper above
            } catch (Exception e) {
                log.error("Search failed for query '{}': {}", q, e.getMessage(), e);
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        // API: rate a search result — appends a judgment to eval/goldenset.jsonl
        app.post("/api/rate", ctx -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> body = ctx.bodyAsClass(Map.class);
            String query    = (String) body.getOrDefault("query", "");
            String project  = (String) body.getOrDefault("project", "");
            String pipeline = (String) body.getOrDefault("pipeline", "");
            String resultId = (String) body.getOrDefault("result_id", "");
            Object rankObj  = body.get("rank");
            int    rank     = (rankObj instanceof Number n) ? n.intValue() : 0;
            Object ratingRaw = body.get("rating");
            String rating   = (ratingRaw instanceof String s) ? s : null;

            if (query.isBlank() || resultId.isBlank()) {
                ctx.status(400).json(Map.of("error", "query and result_id required"));
                return;
            }
            appendRating(query, project, pipeline, resultId, rank, rating);
            ctx.json(Map.of("status", "ok"));
        });

        // API: single method by FQN
        app.get("/api/method", ctx -> {
            String fqn = ctx.queryParam("fqn");
            if (fqn == null) { ctx.status(400).result("fqn required"); return; }
            List<String> warnings = new ArrayList<>();
            SearchResult r = searchEngine.getMethodByFqn(fqn, warnings);
            warningsHeader(ctx, warnings);
            if (r == null && fqn.contains("#") && !fqn.contains("(")) {
                // Partial FQN — parameter types omitted. Try name-only lookup.
                List<SearchResult> candidates = searchEngine.findMethodsByPartialFqn(fqn, warnings);
                warningsHeader(ctx, warnings);
                if (candidates.isEmpty()) {
                    recordSingle(ctx, null);
                    ctx.status(404).result("Not found"); return;
                } else if (candidates.size() == 1) {
                    recordSingle(ctx, candidates.get(0).id());
                    ctx.json(methodToMap(candidates.get(0))); return;
                } else {
                    recordResults(ctx, candidates.size(),
                            candidates.stream().map(SearchResult::id).collect(Collectors.toList()));
                    String list = candidates.stream()
                            .map(c -> "  " + c.id().substring(c.project().length() + 1))
                            .collect(Collectors.joining("\n"));
                    ctx.status(400).result(
                            "Ambiguous FQN — " + candidates.size() + " overloads found. Include parameter types:\n" + list);
                    return;
                }
            }
            if (r == null) { recordSingle(ctx, null); ctx.status(404).result("Not found"); return; }
            recordSingle(ctx, r.id());
            ctx.json(methodToMap(r));
        });

        // API: class body — ?fqn=  (add ?context=true for full context including fields/constructors/callers)
        app.get("/api/class", ctx -> {
            String fqn     = ctx.queryParam("fqn");
            boolean withCtx = "true".equalsIgnoreCase(ctx.queryParam("context"));
            if (fqn == null) { ctx.status(400).result("fqn required"); return; }
            List<String> warnings = new ArrayList<>();
            try {
                if (withCtx) {
                    SearchEngine.ClassContext context = searchEngine.getClassContext(fqn, warnings);
                    warningsHeader(ctx, warnings);
                    if (context == null) { recordSingle(ctx, null); ctx.status(404).result("Not found"); return; }
                    recordSingle(ctx, fqn);
                    ctx.json(classContextToMap(context));
                } else {
                    SearchResult r = searchEngine.getClassByFqn(fqn, warnings);
                    warningsHeader(ctx, warnings);
                    if (r == null) { recordSingle(ctx, null); ctx.status(404).result("Not found"); return; }
                    recordSingle(ctx, r.id());
                    ctx.json(classToMap(r));
                }
            } catch (IncompatibleIndexException | com.pharos.cluster.routing.InvalidProjectException
                     | com.pharos.cluster.routing.CatalogUnavailableException
                     | com.pharos.cluster.routing.NoShardAnsweredException
                     | com.pharos.search.VectorModelUnavailableException e) {
                throw e;  // 409 via the registered exception handler
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
            List<String> warnings = new ArrayList<>();
            try {
                List<SearchResult> callers = searchEngine.findCallers(fqn, project, warnings);
                warningsHeader(ctx, warnings);
                recordResults(ctx, callers.size(),
                        callers.stream().map(SearchResult::id).collect(Collectors.toList()));
                ctx.json(callers.stream().map(this::refToMap).collect(Collectors.toList()));
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
            List<String> warnings = new ArrayList<>();
            try {
                List<SearchResult> callees = searchEngine.findCallees(fqn, project, warnings);
                warningsHeader(ctx, warnings);
                recordResults(ctx, callees.size(),
                        callees.stream().map(SearchResult::id).collect(Collectors.toList()));
                ctx.json(callees.stream().map(this::refToMap).collect(Collectors.toList()));
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
                List<String> warnings = new ArrayList<>();
                var res = searchEngine.findTransitiveCallers(fqn, depth, maxCallers, warnings);
                warningsHeader(ctx, warnings);
                ctx.json(res);
            } catch (IncompatibleIndexException | com.pharos.cluster.routing.InvalidProjectException
                     | com.pharos.cluster.routing.CatalogUnavailableException
                     | com.pharos.cluster.routing.NoShardAnsweredException
                     | com.pharos.search.VectorModelUnavailableException e) {
                throw e;  // 409 via the registered exception handler
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
                List<String> warnings = new ArrayList<>();
                var res = searchEngine.findUsages(fqn, kind, null, warnings);
                warningsHeader(ctx, warnings);
                ctx.json(res);
            } catch (IncompatibleIndexException | com.pharos.cluster.routing.InvalidProjectException
                     | com.pharos.cluster.routing.CatalogUnavailableException
                     | com.pharos.cluster.routing.NoShardAnsweredException
                     | com.pharos.search.VectorModelUnavailableException e) {
                throw e;  // 409 via the registered exception handler
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
                List<String> warnings = new ArrayList<>();
                var res = searchEngine.traceCallChain(fqn, depth, direction, maxBodyChars, warnings);
                warningsHeader(ctx, warnings);
                ctx.json(res);
            } catch (IncompatibleIndexException | com.pharos.cluster.routing.InvalidProjectException
                     | com.pharos.cluster.routing.CatalogUnavailableException
                     | com.pharos.cluster.routing.NoShardAnsweredException
                     | com.pharos.search.VectorModelUnavailableException e) {
                throw e;  // 409 via the registered exception handler
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
            List<String> warnings = new ArrayList<>();
            java.util.Optional<List<String>> fanned = searchEngine.fannedOutCallPath(from, to, warnings);
            if (fanned.isPresent()) path = fanned.get();
            else for (ProjectMeta meta : registry.listAll()) {
                try (GraphView g = loadCallGraphCached(meta)) {
                    List<String> candidate = g.shortestPath(from, to);
                    if (!candidate.isEmpty()) { path = candidate; break; }
                } catch (IncompatibleIndexException e) {
                    warnings.add(e.getMessage());
                } catch (Exception ignored) {}
            }
            recordResults(ctx, path.size(), path);
            Map<String, Object> res = new LinkedHashMap<>();
            res.put("path", path);
            res.put("hops", path.isEmpty() ? 0 : path.size() - 1);
            if (!warnings.isEmpty()) res.put("warnings", warnings);
            ctx.json(res);
        });

        // API: list modules
        app.get("/api/modules", ctx -> {
            if (refuseOnReader(ctx)) return;
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
            if (refuseOnReader(ctx)) return;
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
            if (refuseOnReader(ctx)) return;
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
            if (refuseOnReader(ctx)) return;
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
            } catch (IncompatibleIndexException | com.pharos.cluster.routing.InvalidProjectException
                     | com.pharos.cluster.routing.CatalogUnavailableException
                     | com.pharos.cluster.routing.NoShardAnsweredException
                     | com.pharos.search.VectorModelUnavailableException e) {
                throw e;  // 409 via the registered exception handler
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
        app.get("/health", ctx -> {
            HealthAnswer h = healthAnswer(); // status and body from the same reads
            ctx.status(h.status()).json(h.body());
        });

        // The internal topology (every reader's advertiseUrl): cluster members only, with the
        // cluster token as on /internal/*. Nothing in pharos reads it (coordinators and the router
        // list readers from the bucket); it is for operators and the gate scripts.
        app.get("/cluster/readers", ctx -> {
            if (liveReaders == null) {
                ctx.status(404).result("not a cluster reader");
                return;
            }
            com.pharos.cluster.internal.InternalApi api = internalApi;
            if (api == null || !api.authorized(ctx)) {
                ctx.status(401).json(Map.of("error", "unauthorized: send " + com.pharos.cluster.internal.InternalApi.TOKEN_HEADER));
                return;
            }
            ctx.json(readersPayload());
        });

        // The cluster's layout, health and proposals; the same JSON on every node for the same bucket
        // state. No token: it carries neither the token nor object-store credentials.
        app.get("/cluster/status", ctx -> {
            com.pharos.cluster.status.StatusService st = clusterStatus;
            if (st == null) {
                ctx.status(404).json(Map.of("error", "no cluster status on this node"));
                return;
            }
            boolean member = clusterToken.test(ctx.header(com.pharos.cluster.internal.InternalApi.TOKEN_HEADER));
            try {
                ctx.contentType("application/json").result(st.currentJson(member));
            } catch (java.io.IOException e) {
                ctx.status(503).json(Map.of("error", "cluster status unavailable: " + e.getMessage()));
            }
        });
        // Proposal and layout changes as server-sent events (appeared|changed|cleared|layout). Events
        // carry no addresses, so token and public clients get the same stream. At most
        // StatusEvents.MAX_CLIENTS streams per node (503 above).
        app.get(EVENTS_PATH, ctx -> {
            com.pharos.cluster.status.StatusService st = clusterStatus;
            com.pharos.cluster.status.StatusEvents ev = st == null ? null : st.events();
            if (ev == null) {
                ctx.status(404).json(Map.of("error", "no cluster events on this node"));
                return;
            }
            if (!"text/event-stream".equals(ctx.header("Accept"))) { // the SSE handler's own condition
                ctx.status(406).json(Map.of("error", "send Accept: text/event-stream"));
                return;
            }
            if (!ev.tryReserve()) {
                ctx.status(503).json(Map.of("error", "too many /cluster/events streams on this node (max "
                        + com.pharos.cluster.status.StatusEvents.MAX_CLIENTS + ")"));
                return;
            }
            try {
                new io.javalin.http.sse.SseHandler(client -> ev.add(com.pharos.cluster.status.StatusEvents.of(client)))
                        .handle(ctx);
            } catch (Exception e) {
                ev.release();
                throw e;
            }
        });

        if (readerNode && internalApi != null) internalApi.mount(app);
        com.pharos.cluster.peer.ReplicationEndpoint repl = replication;
        if (repl != null) repl.mount(app);

        app.start(port);
        System.out.printf("Web UI available at %s://localhost:%d%n", tls != null ? "https" : "http", port);
        log.info("Web server started on port {}", port);
        return app;
    }

    /** The listening connector: TLS (from {@link Tls}) or plain HTTP; {@code idle} null keeps the default. */
    static org.eclipse.jetty.server.ServerConnector connector(org.eclipse.jetty.server.Server server,
                                                              org.eclipse.jetty.server.HttpConfiguration http,
                                                              int port, Tls tls, java.time.Duration idle) {
        org.eclipse.jetty.server.ServerConnector c;
        if (tls != null) {
            if (!java.nio.file.Files.isRegularFile(tls.keystore())) {
                throw new IllegalStateException("cluster.tls.keystore " + tls.keystore() + " does not exist");
            }
            org.eclipse.jetty.util.ssl.SslContextFactory.Server ssl = new org.eclipse.jetty.util.ssl.SslContextFactory.Server();
            ssl.setKeyStorePath(tls.keystore().toAbsolutePath().toString());
            ssl.setKeyStoreType(tls.type());
            ssl.setKeyStorePassword(new String(tls.password()));
            org.eclipse.jetty.server.HttpConfiguration https = new org.eclipse.jetty.server.HttpConfiguration(http);
            org.eclipse.jetty.server.SecureRequestCustomizer secure = new org.eclipse.jetty.server.SecureRequestCustomizer();
            // Clients verify the host name against the certificate; the server does not police SNI.
            secure.setSniHostCheck(false);
            https.addCustomizer(secure);
            c = new org.eclipse.jetty.server.ServerConnector(server,
                    new org.eclipse.jetty.server.SslConnectionFactory(ssl, org.eclipse.jetty.http.HttpVersion.HTTP_1_1.asString()),
                    new org.eclipse.jetty.server.HttpConnectionFactory(https));
        } else {
            c = new org.eclipse.jetty.server.ServerConnector(server, new org.eclipse.jetty.server.HttpConnectionFactory(http));
        }
        c.setPort(port);
        if (idle != null) c.setIdleTimeout(idle.toMillis());
        return c;
    }

    private static final com.fasterxml.jackson.databind.ObjectWriter ASCII_JSON = new ObjectMapper()
            .writer().with(com.fasterxml.jackson.core.JsonGenerator.Feature.ESCAPE_NON_ASCII);

    /**
     * Surfaces skipped-project notices without changing the JSON body: header
     * {@code X-Pharos-Warnings} holds a JSON array of strings, non-ASCII escaped as backslash-u sequences.
     */
    private static void warningsHeader(io.javalin.http.Context ctx, List<String> warnings) {
        if (warnings.isEmpty()) return;
        try {
            ctx.header("X-Pharos-Warnings", ASCII_JSON.writeValueAsString(warnings.stream().distinct().toList()));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.warn("Could not encode warnings header: {}", e.getMessage());
        }
    }

    // ── Usage logging helpers ──────────────────────────────────────────────────

    /**
     * Attach {@code resultCount} and (optionally) {@code resultIds} to the request context
     * so the {@code after} handler can write them into the usage log. IDs are capped at
     * 20 to keep log lines a reasonable size; the count is unbounded.
     */
    private static void recordResults(io.javalin.http.Context ctx, int count, List<String> ids) {
        ctx.attribute("usage.resultCount", count);
        if (ids != null && !ids.isEmpty()) {
            ctx.attribute("usage.resultIds", ids.size() > 20 ? ids.subList(0, 20) : ids);
        }
    }

    /** Shortcut for endpoints that return a single result (or none, for 404). */
    private static void recordSingle(io.javalin.http.Context ctx, String id) {
        ctx.attribute("usage.resultCount", id == null ? 0 : 1);
        ctx.attribute("usage.resultIds", id == null ? List.of() : List.of(id));
    }

    // ── Rating / goldenset ────────────────────────────────────────────────────

    private static final Object RATING_LOCK = new Object();

    private void appendRating(String query, String project, String pipeline,
                               String resultId, int rank, String rating) throws IOException {
        Path file = Path.of("eval/goldenset.jsonl");
        Files.createDirectories(file.getParent());
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("query",     query);
        entry.put("project",   project);
        entry.put("pipeline",  pipeline);
        entry.put("result_id", resultId);
        entry.put("rank",      rank);
        entry.put("rating",    rating);
        entry.put("ts",        java.time.Instant.now().toString());
        String line = mapper.writeValueAsString(entry) + "\n";
        synchronized (RATING_LOCK) {
            Files.writeString(file, line,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND);
        }
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

    /** Open the read-only call graph for the given project. Caller must close. */
    private GraphView loadCallGraphCached(ProjectMeta meta) throws Exception {
        return GraphViews.open(meta); // checks the index format first (IncompatibleIndexException)
    }

    private Map<String, Object> buildClassGraphData(ProjectMeta meta) throws Exception {
        try (GraphView graph = loadCallGraphCached(meta)) {
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
        try (GraphView graph = loadCallGraphCached(meta)) {
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
        m.put("embeddedModels", p.getEmbeddedModels());
        Path indexDir = IndexFormat.dir(p.getIndexPath());
        IndexFormat.Stamp stamp = IndexFormat.read(indexDir);
        Map<String, Object> fmt = new LinkedHashMap<>();
        fmt.put("status", IndexFormat.status(indexDir).wireName());
        fmt.put("format", stamp.format());
        fmt.put("pharosVersion", stamp.pharosVersion());
        m.put("indexFormat", fmt);
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
        if (r.qualifiedClassName() != null) m.put("qualifiedClassName", r.qualifiedClassName());
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
