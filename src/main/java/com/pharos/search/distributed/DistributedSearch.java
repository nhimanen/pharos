package com.pharos.search.distributed;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.CalleesRequest;
import com.pharos.cluster.internal.GenerationMismatchException;
import com.pharos.cluster.internal.GraphRequest;
import com.pharos.cluster.internal.GraphResponse;
import com.pharos.cluster.internal.ModelMismatchException;
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
import com.pharos.indexer.DocumentMapper;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

/**
 * The coordinator of a reader that does not hold every shard a query needs (Phase 3 fan-out).
 *
 * <p>Per query ({@link #open}, closed when the query is done):
 * <ol>
 *   <li>Route the query's shards ({@link ShardRouter}); a shard this reader holds is searched in
 *       process ({@link LocalShardService}), the others on a live reader serving them. A coordinator
 *       holding every requested shard does not fan out at all ({@link Query#fansOut()} is false):
 *       {@code SearchEngine} keeps its local {@code MultiReader} path, unchanged.</li>
 *   <li>The pipeline classifies the query once, here; the classification travels in the request.
 *       The query is embedded once, here, and the vector is sent with the model's id, dimensions and
 *       fingerprint (a shard without vectors of that model refuses).</li>
 *   <li>Term statistics are gathered from every shard and summed ({@link GlobalStats}); this first
 *       contact <b>pins</b> each remote shard to the reader and generation that answered. Every later
 *       call of the query for that shard (raw search, snippets, callees, graph) carries that
 *       generation and goes to that reader first; a replica answering for a dead pinned reader must
 *       serve the same generation, so one query never mixes generations of a shard. (Before any
 *       results of the shard were used, a 409 from a reader that just swapped re-pins the shard to
 *       its new generation once.) A local shard is pinned once per query too, holding a reference on
 *       its reader until the query closes, so a swap meanwhile cannot close it.</li>
 *   <li>Each retrieval stage fetches raw candidates (top {@code limit}) from every shard with the
 *       global statistics, merges them k-way by (raw score desc, shard ordinal asc, keeping each
 *       shard's order), truncates to {@code limit} and only then shapes the merged list
 *       ({@link CandidateShaper}: graph boost over that list's max in-degree, as a single node does).</li>
 *   <li>Mergers and rerankers run on result fields only, at the coordinator.</li>
 *   <li>Neighbourhood expansion asks the owning shard for callees; vector snippet hints come from the
 *       owning shard's pinned reader ({@code /internal/snippets}).</li>
 * </ol>
 *
 * <p><b>Time.</b> One deadline per query ({@code cluster.queryTimeoutMillis}), measured from
 * {@link #open}, covers every round; each attempt of a shard call gets at most its share of what is
 * left, so a retry on a second replica still runs after a stalled first one. Remote calls run on
 * virtual threads (at most {@code perNodeConcurrency} open to one reader, in {@link ShardClient});
 * a coordinator's own shards are searched on the query's thread. Outstanding calls are cancelled when
 * the deadline passes, the thread is interrupted, or the query is closed.
 *
 * <p><b>Failures.</b> A shard whose readers all fail is left out of the rest of the query and named in
 * a warning: {@code shard <name> unavailable: <reason>; results exclude it}. A shard without vectors
 * of the coordinator's model is left out of the retrievals that use the query vector: vector, and
 * unified as a whole (its BM25 recall pass included), with {@code shard <name> has no vectors for
 * model <id>}; its keyword retrieval still runs. The other shards still answer.
 *
 * <p><b>Exactness.</b> BM25 (keyword, and unified's recall pass) scores with corpus-wide statistics,
 * so keyword results equal a single node's over the same shards. Known gaps: SynonymQuery/
 * MultiPhraseQuery skip a term's statistics on a shard lacking that term (see
 * {@link GlobalStatsSearcher}); vector and unified across more than one shard are approximate (each
 * shard's HNSW search and unified pool are its own, not a {@code MultiReader}'s); ties between shards
 * break by shard ordinal (the order of the requested projects, else the catalog's name order).
 * A single shard through fan-out is bitwise its own local search.
 */
public final class DistributedSearch implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DistributedSearch.class);

    /** An unscoped query routed with a catalog older than this carries a warning. */
    static final long CATALOG_AGE_WARNING_SECONDS = 60;

    private final ShardRouter router;
    private final ShardClient client;
    private final LocalShardService local;
    private final EmbeddingProvider embedder;
    private final VectorSearchStrategy vector;
    private final Duration queryTimeout;
    private final LongSupplier catalogAgeSeconds;
    private final ExecutorService remote = Executors.newVirtualThreadPerTaskExecutor();
    /** Tests only: false scores each shard with its own statistics (the control of the parity test). */
    volatile boolean globalStatistics = true;

    /**
     * @param queryTimeout      the budget of one query, every round included
     * @param catalogAgeSeconds age of the shard catalog the router uses (negative: unknown)
     */
    public DistributedSearch(ShardRouter router, ShardClient client, LocalShardService local,
                             EmbeddingProvider embedder, VectorSearchStrategy vector, Duration queryTimeout,
                             LongSupplier catalogAgeSeconds) {
        this.router = router;
        this.client = client;
        this.local = local;
        this.embedder = embedder;
        this.vector = vector;
        this.queryTimeout = queryTimeout;
        this.catalogAgeSeconds = catalogAgeSeconds;
    }

    /** True while this coordinator marks {@code node} suspect (it failed a call). */
    public boolean suspect(String node) {
        return client.health().isSuspect(node);
    }

    /**
     * Routes a query over {@code projects} (empty: every shard of the catalog), in that order. Close
     * the returned query when done with it (its results and resolvers stay usable).
     *
     * @throws com.pharos.cluster.routing.InvalidProjectException     a name is not a shard id (400)
     * @throws com.pharos.cluster.routing.CatalogUnavailableException the catalog cannot be used (503)
     */
    public Query open(List<String> projects) {
        long start = System.nanoTime();
        boolean unscoped = projects == null || projects.isEmpty();
        List<Route> routes = router.route(projects);
        if (!unscoped) { // ordinals follow the requested order
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
        Query q = new Query(shards, start + queryTimeout.toNanos());
        long age = unscoped ? catalogAgeSeconds.getAsLong() : -1;
        if (age > CATALOG_AGE_WARNING_SECONDS) q.warn("routing with a catalog " + age + " s old");
        return q;
    }

    @Override
    public void close() {
        remote.shutdownNow();
    }

    /** One shard of one query: where it is searched and the generation pinned for it. */
    static final class ShardState {
        final int ordinal;
        final Route route;
        final boolean local;
        LocalShardService.Pin pin;        // local shards, pinned on first use, released on close
        ReaderHeartbeat reader;           // remote shards: the reader that answered last
        Long generation;                  // remote shards: pinned by the first answer
        boolean consumed;                 // results of the pinned generation were used
        boolean repinned;                 // the one re-pin after a swap was used
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

    /** The fan-out of one query; its shard calls run in parallel. Close it when the query is done. */
    public final class Query implements AutoCloseable {

        private final List<ShardState> shards;
        private final long deadlineNanos;
        private final List<String> warnings = new ArrayList<>();
        private final Map<String, float[]> vectors = new HashMap<>();
        private final Set<Future<?>> outstanding = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final Set<String> vectorless = new LinkedHashSet<>();
        private GlobalStats globalStats;
        private String statsKey;
        private boolean closed;

        Query(List<ShardState> shards, long deadlineNanos) {
            this.shards = shards;
            this.deadlineNanos = deadlineNanos;
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

        /** One warning per shard left out so far (plus routing notices), in the order they arose. */
        public synchronized List<String> warnings() {
            for (ShardState s : shards) {
                if (!s.local && s.route.candidates().isEmpty() && s.unavailable == null) {
                    markUnavailable(s, noReaderReason(s));
                }
            }
            return List.copyOf(warnings);
        }

        synchronized void warn(String w) {
            if (!warnings.contains(w)) warnings.add(w);
        }

        /** Why no reader is a candidate for {@code s}: none serves it, or every one is degraded for it. */
        private static String noReaderReason(ShardState s) {
            List<String> degraded = s.route.degraded();
            return degraded.isEmpty() ? "no live reader serves it"
                    : "every live reader serving it is degraded (" + String.join(", ", degraded) + ")";
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
                    markUnavailable(s, noReaderReason(s));
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

        private long remainingNanos() {
            return deadlineNanos - System.nanoTime();
        }

        private LocalShardService.Pin pin(ShardState s) throws IOException {
            synchronized (s) {
                if (s.pin == null) {
                    if (closed) throw new IllegalStateException("query closed");
                    s.pin = local.pin(s.route.shard());
                }
                return s.pin;
            }
        }

        /** Releases the local readers this query pinned and cancels its outstanding shard calls. */
        @Override
        public void close() {
            synchronized (this) {
                if (closed) return;
                closed = true;
            }
            for (Future<?> f : outstanding) f.cancel(true);
            for (ShardState s : shards) {
                synchronized (s) {
                    if (s.pin != null) {
                        try {
                            s.pin.close();
                        } catch (IOException e) {
                            log.warn("Releasing the reader of shard {} failed: {}", s.name(), e.toString());
                        }
                        s.pin = null;
                    }
                }
            }
        }

        /**
         * One call for a remote shard, on its pinned reader first, carrying its pinned generation and
         * the query's deadline. {@code consumes}: the response's results are used (after which the
         * shard may no longer move to another generation).
         */
        private <Q, R> R remote(ShardState s, ShardClient.Endpoint<Q, R> endpoint, boolean consumes,
                                BiFunction<ReaderHeartbeat, Long, Q> request) {
            ReaderHeartbeat preferred;
            Long generation;
            synchronized (s) {
                preferred = s.reader;
                generation = s.generation;
            }
            ShardClient.Answer<R> a;
            try {
                a = client.callPreferring(s.route, preferred, endpoint, hb -> request.apply(hb, generation),
                        deadlineNanos);
            } catch (GenerationMismatchException g) {
                // A reader that swapped before any of the shard's results were used: follow it once.
                ReaderHeartbeat swapped = readerNamed(s, g.node());
                synchronized (s) {
                    if (s.consumed || s.repinned || swapped == null || g.servedGeneration() < 0) throw g;
                    s.repinned = true;
                    s.generation = g.servedGeneration();
                    s.reader = swapped;
                }
                Long next = g.servedGeneration();
                a = client.callPreferring(s.route, swapped, endpoint, hb -> request.apply(hb, next), deadlineNanos);
            }
            long served = endpoint.servedGeneration().applyAsLong(a.response());
            synchronized (s) {
                if (s.generation == null) {
                    s.generation = served;
                } else if (s.generation != served) { // never mix generations of one shard in a query
                    throw new GenerationMismatchException(a.reader().node(), served);
                }
                s.reader = a.reader();
                if (consumes) s.consumed = true;
            }
            return a.response();
        }

        private ReaderHeartbeat readerNamed(ShardState s, String node) {
            synchronized (s) {
                if (s.reader != null && s.reader.node().equals(node)) return s.reader;
            }
            for (ReaderHeartbeat r : s.route.candidates()) if (r.node().equals(node)) return r;
            return null;
        }

        /**
         * Runs {@code work} for each shard: remote ones in parallel on virtual threads, local ones on
         * this thread meanwhile; waits for the remote ones until the query's deadline. A shard that
         * fails or misses the deadline is marked unavailable; {@code onError} may claim a failure first
         * (return true: handled, the shard stays available).
         */
        private <T> Map<ShardState, T> fanOut(List<ShardState> targets, ShardWork<T> work,
                                              java.util.function.BiPredicate<ShardState, Throwable> onError) {
            Map<ShardState, Future<T>> futures = new LinkedHashMap<>();
            // Only remote calls run on the futures cancel(true) may interrupt: they do HTTP only. Local
            // shards (file-backed Lucene and graph reads, where an interrupt would close a shared
            // FileChannel of an NIOFS or lazy remote directory) run on this thread, which nothing interrupts.
            for (ShardState s : targets) {
                if (s.local) continue;
                Future<T> f = remote.submit(() -> work.run(s));
                outstanding.add(f);
                futures.put(s, f);
            }
            Map<ShardState, T> done = new HashMap<>();
            for (ShardState s : targets) {
                if (!s.local) continue;
                try {
                    T v = work.run(s);
                    if (v != null) done.put(s, v);
                } catch (Exception e) {
                    if (!onError.test(s, e)) markUnavailable(s, reason(e));
                }
            }
            for (Map.Entry<ShardState, Future<T>> e : futures.entrySet()) {
                ShardState s = e.getKey();
                Future<T> f = e.getValue();
                try {
                    T v = f.get(Math.max(0, remainingNanos()), TimeUnit.NANOSECONDS);
                    if (v != null) done.put(s, v);
                } catch (TimeoutException te) {
                    f.cancel(true);
                    markUnavailable(s, "no answer within the query timeout of " + queryTimeout.toMillis() + " ms");
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    for (Future<T> other : futures.values()) other.cancel(true);
                    markUnavailable(s, "interrupted");
                } catch (java.util.concurrent.CancellationException ce) {
                    markUnavailable(s, "cancelled");
                } catch (ExecutionException ee) {
                    if (!onError.test(s, ee.getCause())) markUnavailable(s, reason(ee.getCause()));
                } finally {
                    outstanding.remove(f);
                }
            }
            Map<ShardState, T> out = new LinkedHashMap<>(); // ordinal order
            for (ShardState s : targets) if (done.containsKey(s)) out.put(s, done.get(s));
            return out;
        }

        private <T> Map<ShardState, T> fanOut(List<ShardState> targets, ShardWork<T> work) {
            return fanOut(targets, work, (s, t) -> false);
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

        /** True when {@code t} says the shard has no vectors for the model (its last attempt's cause). */
        private static ModelMismatchException modelMismatch(Throwable t) {
            for (Throwable c = t; c != null; c = c.getCause()) {
                if (c instanceof ModelMismatchException m) return m;
            }
            return null;
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

        /** Sums the shards' statistics of {@code terms} (one round; it pins remote shards). */
        private GlobalStats gather(Set<Term> terms, String synonymsHash) {
            TreeSet<String> fields = new TreeSet<>();
            for (Term t : terms) fields.add(t.field());
            List<TermStatsRequest.Term> wire = new ArrayList<>(terms.size());
            for (Term t : terms) wire.add(new TermStatsRequest.Term(t.field(), t.text()));
            Map<ShardState, TermStatsResponse> answers = fanOut(available(), s -> s.local
                    ? new TermStatsResponse(pin(s).generation(), local.termStats(pin(s), fields, terms))
                    : remote(s, ShardClient.TERM_STATS, false,
                            (hb, gen) -> new TermStatsRequest(s.name(), List.copyOf(fields), wire, synonymsHash)));
            return GlobalStats.merge(new ArrayList<>(answers.values()));
        }

        /** Global statistics of the query's BM25 terms, gathered once per query. */
        private GlobalStats globalStats(SearchRequest req) {
            KeywordSearchStrategy kw = local.keyword();
            org.apache.lucene.search.Query q = kw.buildQuery(req);
            Set<Term> terms = q == null ? Set.of() : GlobalStats.queryTerms(q);
            String key = terms.toString();
            synchronized (this) {
                if (globalStats != null && key.equals(statsKey)) return globalStats;
            }
            GlobalStats merged = gather(terms, local.synonymsHash());
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
            GlobalStats stats = "vector".equals(retriever) || !globalStatistics ? null : globalStats(req);
            List<ShardState> targets = new ArrayList<>();
            for (ShardState s : available()) {
                synchronized (this) {
                    if (vec == null || !vectorless.contains(s.name())) targets.add(s);
                }
            }
            List<List<RawCandidate>> lists = new ArrayList<>();
            String intent = null;
            int answered;
            String mode = "keyword".equals(retriever) || targets.size() == 1 ? retriever : retriever + "-2";
            switch (mode) {
                case "keyword" -> {
                    Map<ShardState, ShardSearchResponse> r = round(targets, "keyword", req, null, stats, s -> null, s -> null, false);
                    r.values().forEach(x -> lists.add(candidates(x)));
                    answered = r.size();
                }
                case "vector", "unified" -> {
                    // One shard: its own single-round retrieval is the exact answer; no second round trip.
                    Map<ShardState, ShardSearchResponse> r = round(targets, retriever, req, vec, stats,
                            s -> null, s -> null, false);
                    for (ShardSearchResponse x : r.values()) {
                        lists.add(candidates(x));
                        intent = x.shapingIntent();
                    }
                    answered = r.size();
                }
                case "vector-2" -> {
                    // Round 1: each shard's HNSW first pass; the global top firstPassK of them...
                    Map<ShardState, ShardSearchResponse> first = round(targets, "vector-knn", req, vec, null,
                            s -> null, s -> null, false);
                    boolean chunks = first.values().stream().anyMatch(ShardSearchResponse::chunkVectors);
                    Map<ShardState, List<ScoredId>> sel = selectTop(first, ShardSearchResponse::recall,
                            VectorSearchStrategy.firstPassK(req.limit()));
                    // ...round 2: rescored on their shards (as one index would rescore them)
                    Map<ShardState, ShardSearchResponse> second = round(new ArrayList<>(sel.keySet()), "vector-rescore",
                            req, vec, null, sel::get, s -> null, chunks);
                    second.values().forEach(x -> lists.add(candidates(x)));
                    answered = first.size();
                }
                default -> { // unified-2
                    Map<ShardState, ShardSearchResponse> first = round(targets, "unified-recall", req, vec, stats,
                            s -> null, s -> null, false);
                    int cs = UnifiedRetrievalStage.candidateSize(req.limit());
                    Map<ShardState, List<ScoredId>> bm25 = selectTop(first, ShardSearchResponse::recall, cs);
                    Map<ShardState, List<ScoredId>> knn = vec == null ? Map.of()
                            : selectTop(first, ShardSearchResponse::knn, cs);
                    List<ShardState> chosen = new ArrayList<>();
                    for (ShardState s : first.keySet()) if (bm25.containsKey(s) || knn.containsKey(s)) chosen.add(s);
                    Map<ShardState, ShardSearchResponse> second = round(chosen, "unified-rescore", req, vec, stats,
                            s -> bm25.getOrDefault(s, List.of()), s -> knn.getOrDefault(s, List.of()), false);
                    boolean firstAnswer = true;
                    for (ShardSearchResponse x : second.values()) {
                        lists.add(candidates(x));
                        if (firstAnswer) intent = x.shapingIntent();
                        firstAnswer = false;
                    }
                    if (second.isEmpty()) intent = vec == null ? null : vectorPassIntent(req);
                    answered = first.size();
                }
            }
            List<RawCandidate> merged = mergeRaw(lists, req.limit());
            String shapeIntent = switch (retriever) {
                case "keyword" -> KeywordSearchStrategy.shapingIntent(req);
                case "vector" -> null;
                default -> answered == 0 ? (vec == null ? null : vectorPassIntent(req)) : intent;
            };
            if (trace != null) trace.record(retriever + " fan-out (" + answered + " shards)", t0);
            return CandidateShaper.shape(merged, type, shapeIntent);
        }

        private static String vectorPassIntent(SearchRequest req) {
            return req.classification() != null ? req.classification().intent() : "default";
        }

        private static List<RawCandidate> candidates(ShardSearchResponse r) {
            return r.candidates() == null ? List.of() : r.candidates();
        }

        /**
         * The global top {@code k} of the shards' first-round candidates (k-way by score desc, shard
         * ordinal asc, each shard's order kept), grouped back by shard in that global order.
         */
        private Map<ShardState, List<ScoredId>> selectTop(Map<ShardState, ShardSearchResponse> first,
                                                          java.util.function.Function<ShardSearchResponse, List<ScoredId>> list,
                                                          int k) {
            List<ShardState> order = new ArrayList<>(first.keySet());
            List<List<ScoredId>> lists = new ArrayList<>();
            for (ShardState s : order) {
                List<ScoredId> l = list.apply(first.get(s));
                lists.add(l == null ? List.of() : l);
            }
            int[] pos = new int[lists.size()];
            Map<ShardState, List<ScoredId>> out = new LinkedHashMap<>();
            for (int taken = 0; taken < k; taken++) {
                int best = -1;
                float bestScore = 0;
                for (int i = 0; i < lists.size(); i++) {
                    if (pos[i] >= lists.get(i).size()) continue;
                    float sc = lists.get(i).get(pos[i]).score();
                    if (best < 0 || sc > bestScore) {
                        best = i;
                        bestScore = sc;
                    }
                }
                if (best < 0) break;
                out.computeIfAbsent(order.get(best), x -> new ArrayList<>()).add(lists.get(best).get(pos[best]++));
            }
            Map<ShardState, List<ScoredId>> ordered = new LinkedHashMap<>(); // shard ordinal order
            for (ShardState s : order) if (out.containsKey(s)) ordered.put(s, out.get(s));
            return ordered;
        }

        /**
         * One round of shard-search calls; a shard refusing the model ({@link ModelMismatchException})
         * is left out of this query's vector retrieval with a warning, not marked unavailable.
         */
        private Map<ShardState, ShardSearchResponse> round(List<ShardState> targets, String retriever,
                                                           SearchRequest req, float[] vec, GlobalStats stats,
                                                           java.util.function.Function<ShardState, List<ScoredId>> selected,
                                                           java.util.function.Function<ShardState, List<ScoredId>> selectedKnn,
                                                           boolean chunks) {
            String hash = local.synonymsHash();
            LocalShardService.Model model = local.model();
            String modelId = vec == null || model == null ? null : model.modelId();
            int dims = vec == null ? 0 : vec.length;
            String fingerprint = vec == null || model == null ? null : model.fingerprint();
            return fanOut(targets, s -> {
                if (s.local) {
                    if (vec != null) local.checkModel(s.route.shard(), modelId, dims, fingerprint);
                    return local.search(pin(s), retriever, req, vec, stats, selected.apply(s), selectedKnn.apply(s), chunks);
                }
                return remote(s, ShardClient.SHARD_SEARCH, true, // first-round doc ids bind the generation too
                        (hb, gen) -> new ShardSearchRequest(s.name(), gen, retriever, req, vec, stats, hash, modelId,
                                dims, fingerprint, selected.apply(s), selectedKnn.apply(s), chunks));
            }, (s, t) -> {
                ModelMismatchException m = modelMismatch(t);
                if (m == null) return false;
                synchronized (this) {
                    vectorless.add(s.name());
                }
                warn("shard " + s.name() + " has no vectors for model " + modelId
                        + (m.getMessage() != null && m.getMessage().startsWith("no vectors") ? "" : " (" + m.getMessage() + ")"));
                return true;
            });
        }

        // ── snippets and expansion ───────────────────────────────────────────

        /**
         * Vector snippet hints for the final {@code results}: from the pinned reader of each result's
         * shard (in process for local shards, one {@code /internal/snippets} call per remote shard,
         * made on first use). The query is embedded only when a hint is first asked for. Usable after
         * the query is closed: a local reader is then used only while it is still open.
         */
        public SnippetResolver vectorResolver(List<SearchResult> results, String query) {
            Map<String, List<String>> idsByShard = new LinkedHashMap<>();
            for (SearchResult r : results) idsByShard.computeIfAbsent(r.project(), k -> new ArrayList<>()).add(r.id());
            Map<String, SnippetResolver> perShard = new HashMap<>();
            return r -> {
                SnippetResolver inner;
                synchronized (perShard) {
                    inner = perShard.computeIfAbsent(r.project(), shard -> shardResolver(shard,
                            idsByShard.getOrDefault(shard, List.of()), query));
                }
                return inner.resolve(r);
            };
        }

        private SnippetResolver shardResolver(String shard, List<String> ids, String query) {
            ShardState s = byName(shard);
            if (s == null || s.unavailable != null) return SnippetResolver.none();
            try {
                float[] vec = queryVector(query);
                if (vec == null) return SnippetResolver.none();
                if (s.local) {
                    org.apache.lucene.index.IndexReader reader;
                    synchronized (s) {
                        reader = s.pin == null ? null : s.pin.reader();
                    }
                    if (reader == null) return SnippetResolver.none();
                    SnippetResolver inner = vector.buildVectorResolver(reader, vec);
                    return r -> {
                        if (!reader.tryIncRef()) return null; // released and closed since
                        try {
                            return inner.resolve(r);
                        } finally {
                            try {
                                reader.decRef();
                            } catch (IOException ignored) {
                            }
                        }
                    };
                }
                SnippetsResponse resp = remote(s, ShardClient.SNIPPETS, true,
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
            if (s == null || !available().contains(s)) return List.of();
            Map<ShardState, Map<String, List<String>>> r = fanOut(List.of(s),
                    st -> st.local ? local.callees(pin(st), List.of(fqn))
                            : remote(st, ShardClient.CALLEES, true,
                                    (hb, gen) -> new CalleesRequest(st.name(), gen, List.of(fqn))).callees());
            Map<String, List<String>> m = r.get(s);
            List<String> out = m == null ? null : m.get(fqn);
            return out == null ? List.of() : out;
        }

        // ── graph and lookups ────────────────────────────────────────────────

        private GraphResponse graph(ShardState s, String op, List<String> fqns, String direction, String kind,
                                    String to, int limit, GlobalStats stats) throws IOException {
            if (s.local) {
                LocalShardService.Pin p = pin(s);
                return local.graph(p, new GraphRequest(s.name(), p.generation(), op, fqns, direction, kind, to,
                        limit, stats));
            }
            return remote(s, ShardClient.GRAPH, true,
                    (hb, gen) -> new GraphRequest(s.name(), gen, op, fqns, direction, kind, to, limit, stats));
        }

        private Map<ShardState, GraphResponse> graphAll(String op, List<String> fqns, String direction, String kind,
                                                        String to, int limit, GlobalStats stats) {
            return fanOut(available(), s -> graph(s, op, fqns, direction, kind, to, limit, stats));
        }

        private Map<ShardState, GraphResponse> graphAll(String op, List<String> fqns, String direction, String kind,
                                                        String to, int limit) {
            return graphAll(op, fqns, direction, kind, to, limit, null);
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
         * Methods calling {@code fqn}: each shard's top {@code limit} raw candidates scored with the
         * summed statistics of the one {@code calledMethods} term (as a single node scores them),
         * merged and shaped as type {@code graph} over the merged list.
         */
        public List<SearchResult> callers(String fqn, int limit) {
            GlobalStats stats = globalStatistics
                    ? gather(Set.of(new Term(DocumentMapper.F_CALLED_METHODS, fqn)), null) : null;
            List<List<RawCandidate>> lists = new ArrayList<>();
            for (GraphResponse r : graphAll("callers", List.of(fqn), null, null, null, limit, stats).values()) {
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
