package com.pharos.search.distributed;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.CalleesRequest;
import com.pharos.cluster.internal.CalleesResponse;
import com.pharos.cluster.internal.GenerationMismatchException;
import com.pharos.cluster.internal.GraphRequest;
import com.pharos.cluster.internal.GraphResponse;
import com.pharos.cluster.internal.ShardCallException;
import com.pharos.cluster.internal.ShardClient;
import com.pharos.cluster.internal.ShardSearchRequest;
import com.pharos.cluster.internal.ShardSearchResponse;
import com.pharos.cluster.internal.ShardUnavailableException;
import com.pharos.cluster.internal.SnippetsRequest;
import com.pharos.cluster.internal.SnippetsResponse;
import com.pharos.cluster.internal.TermStatsRequest;
import com.pharos.cluster.internal.TermStatsResponse;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.routing.ShardRouter;
import com.pharos.cluster.routing.ShardRouter.Route;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import com.pharos.search.Snippet;
import com.pharos.search.SnippetResolver;
import com.pharos.search.VectorSearchStrategy;
import com.pharos.search.pipeline.KeywordRetrievalStage;
import com.pharos.search.pipeline.RetrievalStage;
import com.pharos.search.pipeline.SearchPipeline;
import com.pharos.search.pipeline.UnifiedRetrievalStage;
import com.pharos.search.pipeline.VectorRetrievalStage;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * The coordinator of a reader that does not hold every shard a query needs (Phase 3 fan-out).
 *
 * <p>Per query ({@link #open}):
 * <ol>
 *   <li>Route the query's shards ({@link ShardRouter}); a shard this reader holds is searched in
 *       process ({@link LocalShardService}), the others on a live reader serving them. A coordinator
 *       holding every requested shard does not fan out at all ({@link Query#fansOut()} is false):
 *       {@code SearchEngine} keeps its local {@code MultiReader} path, unchanged.</li>
 *   <li>The pipeline classifies the query once, here; the classification travels in the request.
 *       The query is embedded once, here, and the vector is sent.</li>
 *   <li>Term statistics are gathered from every shard and summed ({@link GlobalStats}); this first
 *       contact <b>pins</b> each remote shard to the reader and generation that answered. Every later
 *       call of the query for that shard (raw search, snippets, callees, graph) carries that
 *       generation and goes to that reader first; a replica answering for a dead pinned reader must
 *       serve the same generation (409 otherwise), so one query never mixes generations of a shard.
 *       A local shard is pinned once per query too ({@link LocalShardService#pin}).</li>
 *   <li>Each retrieval stage fetches raw candidates (top {@code limit}) from every shard with the
 *       global statistics, merges them k-way by (raw score desc, shard ordinal asc, keeping each
 *       shard's order), truncates to {@code limit} and only then shapes the merged list
 *       ({@link CandidateShaper}: graph boost over that list's max in-degree, as a single node does).</li>
 *   <li>Mergers and rerankers run on result fields only, at the coordinator.</li>
 *   <li>Neighbourhood expansion asks the owning shard for callees; vector snippet hints come from the
 *       owning shard's pinned reader ({@code /internal/snippets}).</li>
 * </ol>
 *
 * <p><b>Failures.</b> A shard whose readers all fail (each tried at most twice, see
 * {@link ShardClient#callPreferring}) is left out of the rest of the query and named in a warning:
 * {@code shard <name> unavailable: <reason>; results exclude it}. The other shards still answer.
 *
 * <p><b>Exactness.</b> BM25 (keyword, and unified's recall pass) scores with corpus-wide statistics,
 * so keyword results equal a single node's over the same shards. Known gaps, documented in the task
 * report: SynonymQuery/MultiPhraseQuery skip a term's statistics on a shard lacking that term (see
 * {@link GlobalStatsSearcher}); vector and unified across more than one shard are approximate (each
 * shard's HNSW search and unified pool are its own, not a {@code MultiReader}'s); ties between shards
 * break by shard ordinal (the order of the requested projects, else the catalog's name order).
 * A single shard through fan-out is bitwise its own local search.
 *
 * <p>Shard calls run in parallel on a bounded pool of daemon threads.
 */
public final class DistributedSearch implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DistributedSearch.class);

    private final ShardRouter router;
    private final ShardClient client;
    private final LocalShardService local;
    private final EmbeddingProvider embedder;
    private final VectorSearchStrategy vector;
    private final Duration waitLimit;
    private final ExecutorService pool;

    /**
     * @param fanoutTimeout the per-call timeout of {@code client}; a query waits at most three times
     *                      it for one round of shard calls (two attempts plus slack)
     */
    public DistributedSearch(ShardRouter router, ShardClient client, LocalShardService local,
                             EmbeddingProvider embedder, VectorSearchStrategy vector, int parallelism,
                             Duration fanoutTimeout) {
        this.router = router;
        this.client = client;
        this.local = local;
        this.embedder = embedder;
        this.vector = vector;
        this.waitLimit = fanoutTimeout.multipliedBy(3);
        AtomicInteger n = new AtomicInteger();
        ThreadPoolExecutor p = new ThreadPoolExecutor(parallelism, parallelism, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), r -> {
                    Thread t = new Thread(r, "pharos-fanout-" + n.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                });
        p.allowCoreThreadTimeOut(true);
        this.pool = p;
    }

    /**
     * Routes a query over {@code projects} (empty: every shard of the catalog), in that order.
     *
     * @throws com.pharos.cluster.routing.InvalidProjectException     a name is not a shard id (400)
     * @throws com.pharos.cluster.routing.CatalogUnavailableException the catalog cannot be read (503)
     */
    public Query open(List<String> projects) {
        List<Route> routes = router.route(projects);
        if (projects != null && !projects.isEmpty()) { // ordinals follow the requested order
            Map<String, Route> byName = new HashMap<>();
            for (Route r : routes) byName.put(r.shard().value(), r);
            List<Route> ordered = new ArrayList<>(routes.size());
            for (String p : new LinkedHashSet<>(projects)) {
                Route r = byName.remove(new ShardId(p).value());
                if (r != null) ordered.add(r);
            }
            routes = ordered;
        }
        List<ShardState> shards = new ArrayList<>(routes.size());
        for (Route r : routes) {
            shards.add(new ShardState(shards.size(), r, router.servesLocally(r.shard())));
        }
        return new Query(shards);
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }

    /** One shard of one query: where it is searched and the generation pinned for it. */
    static final class ShardState {
        final int ordinal;
        final Route route;
        final boolean local;
        LocalShardService.Pin pin;        // local shards, pinned on first use
        ReaderHeartbeat reader;           // remote shards: the reader that answered last
        Long generation;                  // remote shards: pinned by the first answer
        String unavailable;               // why the shard is left out, once it is

        ShardState(int ordinal, Route route, boolean local) {
            this.ordinal = ordinal;
            this.route = route;
            this.local = local;
        }

        String name() {
            return route.shard().value();
        }
    }

    @FunctionalInterface
    private interface ShardWork<T> {
        T run(ShardState s) throws Exception;
    }

    /** Merges per-shard raw lists (in shard ordinal order) k-way and keeps the top {@code limit}. */
    static List<RawCandidate> mergeRaw(List<List<RawCandidate>> perShard, int limit) {
        int[] pos = new int[perShard.size()];
        List<RawCandidate> out = new ArrayList<>(Math.max(0, limit));
        while (out.size() < limit) {
            int best = -1;
            float bestScore = 0;
            for (int i = 0; i < perShard.size(); i++) {
                List<RawCandidate> l = perShard.get(i);
                if (pos[i] >= l.size()) continue;
                float s = l.get(pos[i]).rawScore();
                if (best < 0 || s > bestScore) { // strict: a tie keeps the lower ordinal
                    best = i;
                    bestScore = s;
                }
            }
            if (best < 0) break;
            out.add(perShard.get(best).get(pos[best]++));
        }
        return out;
    }

    /** The fan-out of one query. Not for concurrent use by several queries; its shard calls run in parallel. */
    public final class Query {

        private final List<ShardState> shards;
        private final List<String> warnings = new ArrayList<>();
        private final Map<String, float[]> vectors = new HashMap<>();
        private GlobalStats globalStats;
        private String statsKey;

        Query(List<ShardState> shards) {
            this.shards = shards;
        }

        /** True when some shard is not held here and a live reader serves it: the query fans out. */
        public boolean fansOut() {
            for (ShardState s : shards) {
                if (!s.local && !s.route.candidates().isEmpty()) return true;
            }
            return false;
        }

        /** The shards this reader holds, in query order. */
        public List<String> localShards() {
            List<String> out = new ArrayList<>();
            for (ShardState s : shards) if (s.local) out.add(s.name());
            return out;
        }

        /** The shards no live reader serves (not held here either), in query order. */
        public List<String> unservedShards() {
            List<String> out = new ArrayList<>();
            for (ShardState s : shards) if (!s.local && s.route.candidates().isEmpty()) out.add(s.name());
            return out;
        }

        /** One warning per shard left out so far, in the order they were lost. */
        public synchronized List<String> warnings() {
            for (ShardState s : shards) {
                if (!s.local && s.route.candidates().isEmpty() && s.unavailable == null) {
                    markUnavailable(s, "no live reader serves it");
                }
            }
            return List.copyOf(warnings);
        }

        private synchronized void markUnavailable(ShardState s, String reason) {
            if (s.unavailable != null) return;
            s.unavailable = reason == null ? "unknown error" : reason;
            String w = "shard " + s.name() + " unavailable: " + s.unavailable + "; results exclude it";
            warnings.add(w);
            log.warn("Query fan-out: {}", w);
        }

        private synchronized List<ShardState> available() {
            List<ShardState> out = new ArrayList<>();
            for (ShardState s : shards) {
                if (s.unavailable != null) continue;
                if (!s.local && s.route.candidates().isEmpty()) {
                    markUnavailable(s, "no live reader serves it");
                    continue;
                }
                out.add(s);
            }
            return out;
        }

        private ShardState byName(String shard) {
            for (ShardState s : shards) if (s.name().equals(shard)) return s;
            return null;
        }

        private LocalShardService.Pin pin(ShardState s) throws IOException {
            synchronized (s) {
                if (s.pin == null) s.pin = local.pin(s.route.shard());
                return s.pin;
            }
        }

        /** One call for a remote shard, on its pinned reader first, carrying its pinned generation. */
        private <Q, R> R remote(ShardState s, ShardClient.Endpoint<Q, R> endpoint,
                                BiFunction<ReaderHeartbeat, Long, Q> request) {
            ReaderHeartbeat preferred;
            Long generation;
            synchronized (s) {
                preferred = s.reader;
                generation = s.generation;
            }
            ShardClient.Answer<R> a = client.callPreferring(s.route, preferred, endpoint,
                    hb -> request.apply(hb, generation));
            long served = endpoint.servedGeneration().applyAsLong(a.response());
            synchronized (s) {
                if (s.generation == null) {
                    s.generation = served;
                } else if (s.generation != served) { // never mix generations of one shard in a query
                    throw new GenerationMismatchException(a.reader().node(), served);
                }
                s.reader = a.reader();
            }
            return a.response();
        }

        /** Runs {@code work} for each shard in parallel; failed shards are marked unavailable. */
        private <T> Map<ShardState, T> fanOut(List<ShardState> targets, ShardWork<T> work) {
            Map<ShardState, Future<T>> futures = new LinkedHashMap<>();
            for (ShardState s : targets) {
                Callable<T> c = () -> work.run(s);
                futures.put(s, pool.submit(c));
            }
            long deadline = System.nanoTime() + waitLimit.toNanos();
            Map<ShardState, T> out = new LinkedHashMap<>();
            for (Map.Entry<ShardState, Future<T>> e : futures.entrySet()) {
                ShardState s = e.getKey();
                try {
                    T v = e.getValue().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    if (v != null) out.put(s, v);
                } catch (TimeoutException te) {
                    e.getValue().cancel(true);
                    markUnavailable(s, "no answer within " + waitLimit.toMillis() + " ms");
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    e.getValue().cancel(true);
                    markUnavailable(s, "interrupted");
                } catch (ExecutionException ee) {
                    markUnavailable(s, reason(ee.getCause()));
                }
            }
            return out;
        }

        private static String reason(Throwable t) {
            if (t instanceof ShardUnavailableException su) {
                return su.detail() != null ? su.detail() : "no live reader answered";
            }
            if (t instanceof GenerationMismatchException gm) {
                return "its generation changed during the query (" + gm.getMessage() + ")";
            }
            if (t instanceof ShardCallException) return t.getMessage();
            return t == null ? null : t.toString();
        }

        // ── search ────────────────────────────────────────────────────────────

        /** The pipeline's stage runner for this query: every retrieval stage fans out. */
        public SearchPipeline.StageRunner runner() {
            return this::retrieve;
        }

        private synchronized float[] queryVector(String query) {
            if (vectors.containsKey(query)) return vectors.get(query);
            float[] v = embedder.isAvailable() ? embedder.embed(query) : null; // embedded once, here
            vectors.put(query, v);
            return v;
        }

        /** Global statistics of the query's BM25 terms, gathered once per query (pins remote shards). */
        private GlobalStats globalStats(SearchRequest req) {
            KeywordSearchStrategy kw = local.keyword();
            org.apache.lucene.search.Query q = kw.buildQuery(req);
            Set<Term> terms = q == null ? Set.of() : GlobalStats.queryTerms(q);
            String key = terms.toString();
            synchronized (this) {
                if (globalStats != null && key.equals(statsKey)) return globalStats;
            }
            TreeSet<String> fields = new TreeSet<>();
            for (Term t : terms) fields.add(t.field());
            List<TermStatsRequest.Term> wire = new ArrayList<>(terms.size());
            for (Term t : terms) wire.add(new TermStatsRequest.Term(t.field(), t.text()));
            String hash = local.synonymsHash();
            Map<ShardState, TermStatsResponse> answers = fanOut(available(), s -> s.local
                    ? new TermStatsResponse(pin(s).generation(), local.termStats(pin(s), fields, terms))
                    : remote(s, ShardClient.TERM_STATS,
                            (hb, gen) -> new TermStatsRequest(s.name(), List.copyOf(fields), wire, hash)));
            GlobalStats merged = GlobalStats.merge(new ArrayList<>(answers.values()));
            synchronized (this) {
                globalStats = merged;
                statsKey = key;
            }
            return merged;
        }

        private List<SearchResult> retrieve(RetrievalStage stage, SearchRequest req, SearchTrace trace)
                throws IOException {
            long t0 = System.currentTimeMillis();
            String retriever;
            String type;
            if (stage instanceof KeywordRetrievalStage) {
                retriever = "keyword";
                type = KeywordSearchStrategy.TYPE;
            } else if (stage instanceof VectorRetrievalStage) {
                retriever = "vector";
                type = VectorSearchStrategy.TYPE;
            } else if (stage instanceof UnifiedRetrievalStage) {
                retriever = "unified";
                type = UnifiedRetrievalStage.TYPE;
            } else {
                throw new UnsupportedOperationException(stage.getClass().getSimpleName() + " cannot fan out");
            }
            float[] vec = "keyword".equals(retriever) ? null : queryVector(req.query());
            if ("vector".equals(retriever) && vec == null) return List.of(); // as the local stage: no vectors
            GlobalStats stats = "vector".equals(retriever) ? null : globalStats(req);
            String hash = local.synonymsHash();
            Map<ShardState, ShardSearchResponse> answers = fanOut(available(), s -> s.local
                    ? local.search(pin(s), retriever, req, vec, stats)
                    : remote(s, ShardClient.SHARD_SEARCH,
                            (hb, gen) -> new ShardSearchRequest(s.name(), gen, retriever, req, vec, stats, hash)));
            List<List<RawCandidate>> lists = new ArrayList<>();
            String intent = null;
            boolean first = true;
            for (ShardSearchResponse r : answers.values()) { // ordinal order
                lists.add(r.candidates() == null ? List.of() : r.candidates());
                if (first) intent = r.shapingIntent();
                first = false;
            }
            List<RawCandidate> merged = mergeRaw(lists, req.limit());
            String shapeIntent = switch (retriever) {
                case "keyword" -> KeywordSearchStrategy.shapingIntent(req);
                case "vector" -> null;
                default -> answers.isEmpty() ? null : intent;
            };
            if (trace != null) trace.record(retriever + " fan-out (" + answers.size() + " shards)", t0);
            return CandidateShaper.shape(merged, type, shapeIntent);
        }

        // ── snippets and expansion ───────────────────────────────────────────

        /**
         * Vector snippet hints for the final {@code results}: from the pinned reader of each result's
         * shard (in process for local shards, one {@code /internal/snippets} call per remote shard,
         * made on first use). The query is embedded only when a hint is first asked for.
         */
        public SnippetResolver vectorResolver(List<SearchResult> results, String query) {
            Map<String, List<String>> idsByShard = new LinkedHashMap<>();
            for (SearchResult r : results) idsByShard.computeIfAbsent(r.project(), k -> new ArrayList<>()).add(r.id());
            Map<String, SnippetResolver> perShard = new HashMap<>();
            return new SnippetResolver() {
                @Override public Snippet resolve(SearchResult r) {
                    SnippetResolver inner;
                    synchronized (perShard) {
                        inner = perShard.computeIfAbsent(r.project(), shard -> shardResolver(shard,
                                idsByShard.getOrDefault(shard, List.of()), query));
                    }
                    return inner.resolve(r);
                }
            };
        }

        private SnippetResolver shardResolver(String shard, List<String> ids, String query) {
            ShardState s = byName(shard);
            if (s == null || s.unavailable != null) return SnippetResolver.none();
            try {
                float[] vec = queryVector(query);
                if (vec == null) return SnippetResolver.none();
                if (s.local) return vector.buildVectorResolver(pin(s).reader(), vec);
                SnippetsResponse resp = remote(s, ShardClient.SNIPPETS,
                        (hb, gen) -> new SnippetsRequest(shard, gen, ids, query, vec));
                Map<String, Snippet> hints = resp.snippets() == null ? Map.of() : resp.snippets();
                return r -> hints.get(r.id());
            } catch (Exception e) {
                log.warn("Vector snippet hints of shard {} unavailable for this search: {}", shard, e.toString());
                return SnippetResolver.none();
            }
        }

        /**
         * Neighbourhood expansion over the fan-out: callees of the top-3 results come from the shard
         * owning each result; their documents from the query's shards (first shard in query order
         * holding a callee wins). Related results score {@code parent × 0.5}, type {@code related}.
         */
        public List<SearchResult> expand(List<SearchResult> primary) {
            Set<String> seen = new LinkedHashSet<>();
            for (SearchResult r : primary) seen.add(r.id());
            List<SearchResult> expanded = new ArrayList<>(primary);
            int n = Math.min(3, primary.size());
            List<List<String>> calleesOf = new ArrayList<>(n);
            List<String> all = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                SearchResult parent = primary.get(i);
                int colon = parent.id().indexOf(':');
                List<String> callees = colon < 0 ? List.of() : calledBy(parent.project(), parent.id().substring(colon + 1));
                calleesOf.add(callees);
                all.addAll(callees);
            }
            Map<String, SearchResult> docs = docs(new ArrayList<>(new LinkedHashSet<>(all)));
            for (int i = 0; i < n; i++) {
                SearchResult parent = primary.get(i);
                for (String fqn : calleesOf.get(i)) {
                    SearchResult c = docs.get(fqn);
                    if (c == null || !seen.add(c.id())) continue;
                    expanded.add(new SearchResult(c.id(), c.project(), c.packageName(), c.className(),
                            c.qualifiedClassName(), c.methodName(), c.signature(), c.returnType(), c.body(),
                            c.javadoc(), c.accessModifier(), c.filePath(), c.startLine(), c.endLine(),
                            parent.score() * 0.5f, "related", c.docType()));
                }
            }
            return expanded;
        }

        /** The methods {@code fqn} calls, read on its owning shard (empty when it is not there). */
        public List<String> calledBy(String shard, String fqn) {
            ShardState s = byName(shard);
            if (s == null) return List.of();
            Map<ShardState, Map<String, List<String>>> r = fanOut(available().contains(s) ? List.of(s) : List.of(),
                    st -> st.local ? local.callees(pin(st), List.of(fqn))
                            : remote(st, ShardClient.CALLEES,
                                    (hb, gen) -> new CalleesRequest(st.name(), gen, List.of(fqn))).callees());
            Map<String, List<String>> m = r.get(s);
            List<String> out = m == null ? null : m.get(fqn);
            return out == null ? List.of() : out;
        }

        // ── graph and lookups ────────────────────────────────────────────────

        private GraphResponse graph(ShardState s, String op, List<String> fqns, String direction, String kind,
                                    String to, int limit) throws IOException {
            if (s.local) {
                LocalShardService.Pin p = pin(s);
                return local.graph(p, new GraphRequest(s.name(), p.generation(), op, fqns, direction, kind, to, limit));
            }
            return remote(s, ShardClient.GRAPH,
                    (hb, gen) -> new GraphRequest(s.name(), gen, op, fqns, direction, kind, to, limit));
        }

        private Map<ShardState, GraphResponse> graphAll(String op, List<String> fqns, String direction, String kind,
                                                        String to, int limit) {
            return fanOut(available(), s -> graph(s, op, fqns, direction, kind, to, limit));
        }

        /** Documents of bare {@code fqns}; the first shard in query order holding one wins. */
        public Map<String, SearchResult> docs(List<String> fqns) {
            if (fqns.isEmpty()) return Map.of();
            Map<String, SearchResult> out = new LinkedHashMap<>();
            for (GraphResponse r : graphAll("docs", fqns, null, null, null, 0).values()) {
                r.docs().forEach(out::putIfAbsent);
            }
            Map<String, SearchResult> ordered = new LinkedHashMap<>();
            for (String fqn : fqns) if (out.containsKey(fqn)) ordered.put(fqn, out.get(fqn));
            return ordered;
        }

        /** Callers and/or callees ({@code direction}) of each fqn, unioned over the shards in query order. */
        public Map<String, Set<String>> neighbors(Collection<String> fqns, String direction) {
            Map<String, Set<String>> out = new LinkedHashMap<>();
            for (String f : fqns) out.put(f, new LinkedHashSet<>());
            if (fqns.isEmpty()) return out;
            for (GraphResponse r : graphAll("neighbors", List.copyOf(fqns), direction, null, null, 0).values()) {
                r.lists().forEach((f, n) -> out.computeIfAbsent(f, k -> new LinkedHashSet<>()).addAll(n));
            }
            return out;
        }

        /** Usage lists of {@code fqn} by kind, unioned over the shards in query order. */
        public Map<String, List<String>> usages(String fqn, String kind) {
            Map<String, List<String>> out = new LinkedHashMap<>();
            for (GraphResponse r : graphAll("usages", List.of(fqn), null, kind, null, 0).values()) {
                r.lists().forEach((k, v) -> out.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
            }
            return out;
        }

        /** The first non-empty shortest call path in shard order (paths do not cross shards). */
        public List<String> callPath(String from, String to) {
            for (GraphResponse r : graphAll("path", List.of(from), null, null, to, 0).values()) {
                List<String> p = r.lists().get("path");
                if (p != null && !p.isEmpty()) return p;
            }
            return List.of();
        }

        /**
         * Methods calling {@code fqn}: each shard's top {@code limit} raw candidates (its own BM25
         * statistics), merged and shaped as type {@code graph} over the merged list.
         */
        public List<SearchResult> callers(String fqn, int limit) {
            List<List<RawCandidate>> lists = new ArrayList<>();
            for (GraphResponse r : graphAll("callers", List.of(fqn), null, null, null, limit).values()) {
                lists.add(r.candidates());
            }
            return CandidateShaper.shape(mergeRaw(lists, limit), "graph", null);
        }

        /** The callee fqns of {@code callerFqn} from the first shard in query order holding it. */
        public List<String> calleeFqns(String callerFqn) {
            for (GraphResponse r : graphAll("called", List.of(callerFqn), null, null, null, 0).values()) {
                List<String> c = r.lists().get(callerFqn);
                if (c != null) return c;
            }
            return List.of();
        }
    }
}
