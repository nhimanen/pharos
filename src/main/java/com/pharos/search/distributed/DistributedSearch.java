package com.pharos.search.distributed;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.CalleesRequest;
import com.pharos.cluster.internal.GenerationMismatchException;
import com.pharos.cluster.internal.SynonymsMismatchException;
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
import com.pharos.cluster.routing.ShardFilter;
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
 * of the coordinator's model is left out of the vector part of the query, with {@code project <name>
 * has no vectors for model <id>; vector results exclude it} (the warning a single node gives): its
 * keyword retrieval still runs, and unified still scores its documents as a single node scores
 * documents without vectors. A query explicitly scoped to that one project
 * fails instead ({@link com.pharos.search.VectorModelUnavailableException}), as on a single node. The
 * other shards still answer.
 *
 * <p><b>Exactness.</b> BM25 (keyword, and unified's recall pass) scores with corpus-wide statistics,
 * synonym expansions included (every expanded term's global statistics, see
 * {@link GlobalStatsSearcher}), so keyword results equal a single node's over the same shards.
 * Known gaps: vector and unified across more than one shard are approximate (each
 * shard's HNSW search and unified pool are its own, not a {@code MultiReader}'s); ties between shards
 * break by shard ordinal (the order of the requested projects, else the catalog's name order).
 * A single shard through fan-out is bitwise its own local search.
 *
 * <p><b>Split projects (Phase 6).</b> A query names projects; the router resolves them to shards with
 * <b>one</b> read of the catalog's project ↔ shards mapping, which the query keeps: every later call of
 * the query uses that version's shards, so a reshard flip meanwhile never mixes two spec versions in
 * one query. A query over a split project always runs here (its shards are searched in process when
 * held), with global statistics over all its shards, so it scores bitwise as the unsplit project. All
 * shards of a split project are kept on <b>one index run</b> ({@code projectGeneration}): before any of
 * its results are used, every shard is pinned and a shard answering from another run than the newest
 * one seen is moved to a replica serving that run (a local one is re-pinned), else left out with a
 * warning; a moved file is therefore never seen twice. Right after a reshard the catalog pins the
 * generation of each shard of the new version (and of the retired one, which the router falls back to
 * while the readers have not activated the new one): those shards are read at exactly those
 * generations instead. Graph lookups of a split project ask one of its
 * shards: they all serve the same project graph.
 */
public final class DistributedSearch implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DistributedSearch.class);

    /**
     * The most query terms (synonym expansions included) whose statistics a query ships to its shards;
     * well below the {@code /internal} cap of {@link com.pharos.cluster.internal.InternalApi#MAX_ITEMS}.
     * Terms beyond it are scored with each shard's own statistics, with a warning.
     */
    static final int MAX_SHIPPED_TERMS = 5_000;

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
    /** The cap on shipped terms ({@link #MAX_SHIPPED_TERMS}); lowered by tests only. */
    volatile int maxShippedTerms = MAX_SHIPPED_TERMS;
    /** Tests only: false scores each shard with its own statistics (the control of the parity test). */
    volatile boolean globalStatistics = true;
    /** Tests only: false turns off the one re-plan of a query (ruling P6-R16; the control of its tests). */
    volatile boolean retries = true;
    /** Tests only: runs after a query's statistics round (a reader may swap a generation here). */
    static volatile java.util.function.Consumer<String> afterStatsRound;
    /** Tests only: runs when a query is about to be planned again (P6-R16), before the wait for fresh state. */
    static volatile Runnable beforeReplan;
    /** The longest a re-plan waits for fresh heartbeats, catalog or synonyms (ruling P6-R16). */
    static final Duration REPLAN_WAIT = Duration.ofSeconds(2);

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

    /** The catalog's shard names (see {@link ShardRouter#catalogShardNames}); never throws. */
    public List<String> catalogShardNames() {
        return router.catalogShardNames();
    }

    /** The catalog's project names (see {@link ShardRouter#catalogProjectNames}); never throws. */
    public List<String> catalogProjectNames() {
        return router.catalogProjectNames();
    }

    /** The catalog's project ↔ shards mapping (see {@link ShardRouter#catalogProjects}); never throws. */
    public java.util.Optional<com.pharos.cluster.sharding.ProjectShards> catalogProjects() {
        return router.catalogProjects();
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
        return open(projects, ShardFilter.NONE);
    }

    /**
     * As {@link #open(List)}, pruning split projects' shards with {@code filter} (see
     * {@link ShardFilter}); {@link Query#routingNotes} says what was pruned and what could not be.
     */
    public Query open(List<String> projects, ShardFilter filter) {
        long start = System.nanoTime();
        boolean unscoped = projects == null || projects.isEmpty();
        ShardRouter.RoutePlan plan = router.plan(projects, filter);
        List<ShardState> shards = new ArrayList<>(plan.shards().size());
        List<ShardState> alternates = new ArrayList<>();
        for (ShardRouter.PlannedShard p : plan.shards()) { // ordinals follow the requested order
            if (p.alternate()) continue;
            ShardState st = new ShardState(shards.size(), p.route(), router.servesLocally(p.shard()), p.project(), p.split());
            st.statsOnly = p.statsOnly();
            st.expectedGeneration = p.generation();
            shards.add(st);
        }
        for (ShardRouter.PlannedShard p : plan.shards()) { // the other version of a project just resharded
            if (!p.alternate()) continue;
            ShardState st = new ShardState(shards.size() + alternates.size(), p.route(), router.servesLocally(p.shard()),
                    p.project(), p.split());
            st.statsOnly = p.statsOnly();
            st.expectedGeneration = p.generation();
            alternates.add(st);
        }
        boolean explicitSingle = !unscoped && new LinkedHashSet<>(projects).size() == 1;
        Query q = new Query(shards, alternates, start + queryTimeout.toNanos(), explicitSingle, plan);
        try {
            q.openSignature = signature(plan);
        } catch (RuntimeException e) { // only a re-plan's wait reads it: never fail a query for it
            q.openSignature = "unknown: " + e;
        }
        // a split project whose shards a filter all pruned still runs here (empty, with its warning)
        for (String p : unscoped ? plan.catalog().projects() : new LinkedHashSet<>(projects)) {
            if (plan.catalog().isSplit(p)) q.hasSplitProject = true;
        }
        long age = unscoped ? catalogAgeSeconds.getAsLong() : -1;
        if (age > CATALOG_AGE_WARNING_SECONDS) q.warn("routing with a catalog " + age + " s old");
        for (String w : plan.warnings()) q.warn(w);
        java.util.Set<String> noFilters = new java.util.TreeSet<>();
        for (ShardRouter.PlannedShard p : plan.shards()) if (!p.alternate()) noFilters.addAll(p.route().noFilters());
        if (!noFilters.isEmpty()) {
            q.warn("key filters: reader(s) " + String.join(", ", noFilters)
                    + " run a pharos without key filter support and do not answer this query");
        }
        return q;
    }

    /** True when a query that lost shards to a generation or synonyms change is planned once more (P6-R16). */
    public boolean retries() {
        return retries;
    }

    /**
     * Ruling P6-R16: plans {@code failed}'s query again, once. It lost shards because a reader swapped a
     * generation under it ("generation changed during the query") or analyses with other synonyms. First
     * waits, at most {@link #REPLAN_WAIT}, until this coordinator's view has moved on: the readers'
     * heartbeats or the catalog for the query's shards, or this node's synonyms. Then routes afresh, so
     * the new query reads the generations (and one spec version) the readers serve now, with new term
     * statistics.
     */
    public Query reopen(Query failed, List<String> projects, ShardFilter filter) {
        Runnable hook = beforeReplan;
        if (hook != null) hook.run();
        long end = System.nanoTime() + REPLAN_WAIT.toNanos();
        while (System.nanoTime() < end && stateSignature(projects, filter).equals(failed.openSignature)) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return open(projects, filter);
    }

    /** What a re-plan waits to change: each planned shard's pin and its readers' generations, and the synonyms. */
    private String stateSignature(List<String> projects, ShardFilter filter) {
        try {
            return signature(router.plan(projects, filter));
        } catch (RuntimeException e) {
            return "unplannable: " + e.getMessage();
        }
    }

    private String signature(ShardRouter.RoutePlan plan) {
        StringBuilder sb = new StringBuilder();
        for (ShardRouter.PlannedShard p : plan.shards()) {
            sb.append(p.shard().value()).append('@').append(p.generation()).append(p.alternate() ? " alternate" : "");
            for (ReaderHeartbeat r : p.route().candidates()) {
                sb.append(' ').append(r.node()).append(':')
                        .append(r.generations() == null ? null : r.generations().get(p.shard().value()));
            }
            sb.append(';');
        }
        return sb.append('|').append(local == null ? "none" : local.synonymsHash()).toString();
    }

    @Override
    public void close() {
        remote.shutdownNow();
        client.close(); // its HttpClient's selector thread would outlive a replaced coordinator
    }

    /** One shard of one query: where it is searched and the generation pinned for it. */
    static final class ShardState {
        final int ordinal;
        final Route route;
        /**
         * Searched in process on this node's reader. Starts as "this node holds the shard"; turns false
         * (served remotely for the rest of the query) when the local reader lags the run the query reads
         * the shard's project from and a replica serves that run ({@code alignRuns}). Every branch reads it.
         */
        volatile boolean local;
        final String project;             // the project this shard serves (catalog mapping of the query)
        final boolean split;              // that project is split: its shards stay on one index run
        boolean statsOnly;                // pruned by the key filter: answers term statistics only (P6-R13)
        Long projectGeneration;           // split shards: the index run of the pinned generation
        Long expectedGeneration;          // split shards of a pinned version (a reshard's): the generation to read
        LocalShardService.Pin pin;        // local shards, pinned on first use, released on close
        ReaderHeartbeat reader;           // remote shards: the reader that answered last
        Long generation;                  // remote shards: pinned by the first answer
        boolean consumed;                 // results of the pinned generation were used
        boolean repinned;                 // the one re-pin after a swap was used
        String unavailable;               // why the shard is left out, once it is

        ShardState(int ordinal, Route route, boolean local, String project, boolean split) {
            this.ordinal = ordinal;
            this.route = route;
            this.local = local;
            this.project = project;
            this.split = split;
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
        return mergeRaw(perShard, limit, false);
    }

    /**
     * As {@link #mergeRaw(List, int)}; with {@code stableTies} equal scores are ordered by
     * {@link KeywordSearchStrategy#tieKey} (the keyword ranking's order, which a single node uses too),
     * instead of by shard ordinal.
     */
    static List<RawCandidate> mergeRaw(List<List<RawCandidate>> perShard, int limit, boolean stableTies) {
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
                } else if (stableTies && s == bestScore && KeywordSearchStrategy.tieKey(l.get(pos[i]))
                        .compareTo(KeywordSearchStrategy.tieKey(perShard.get(best).get(pos[best]))) < 0) {
                    best = i;
                }
            }
            if (best < 0) break;
            out.add(perShard.get(best).get(pos[best]++));
        }
        return out;
    }

    /** The fan-out of one query; its shard calls run in parallel. Close it when the query is done. */
    public final class Query implements AutoCloseable {

        /** The query's shards; replaced once by {@link #reconcileRuns} when it reads a project's alternate version. */
        private volatile List<ShardState> shards;
        /** The other version of each project resharded just now ({@link ShardRouter.PlannedShard#alternate}). */
        private final List<ShardState> alternates;
        /** Trace notes that arise while the query runs (a switch to a project's other version). */
        private final List<String> lateNotes = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final long deadlineNanos;
        private final List<String> warnings = new ArrayList<>();
        private final Map<String, float[]> vectors = new HashMap<>();
        private final Set<Future<?>> outstanding = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final Set<String> vectorless = new LinkedHashSet<>();
        /**
         * Of {@link #vectorless}, the shards whose project has no vectors of the model at all (no such
         * field): unified still sends them the query vector, unchecked, so their documents score as a
         * single node scores documents without vectors (the BM25 fallback of its rescorer).
         */
        private final Set<String> noModelVectors = new LinkedHashSet<>();
        private GlobalStats globalStats;
        private String statsKey;
        private boolean closed;
        /** Scoped to exactly one project: a shard without vectors of the model fails the query, as locally. */
        private final boolean explicitSingle;
        private final ShardRouter.RoutePlan plan;
        /** Result id → the shard that returned it (a split project's results name the project, not the shard). */
        private final Map<String, ShardState> owners = new java.util.concurrent.ConcurrentHashMap<>();
        /** The query names (or, unscoped, covers) a split project. */
        private volatile boolean hasSplitProject;
        /** Every split project's shards were pinned to one index run ({@link #reconcileRuns}). */
        private boolean reconciled;
        /** The coordinator's view when the query was planned (see {@link DistributedSearch#reopen}). */
        private volatile String openSignature = "";
        /** Why the query lost a shard in a way a re-plan can mend (ruling P6-R16), else null. */
        private volatile String retryCause;

        Query(List<ShardState> shards, List<ShardState> alternates, long deadlineNanos, boolean explicitSingle,
              ShardRouter.RoutePlan plan) {
            this.shards = List.copyOf(shards);
            this.alternates = List.copyOf(alternates);
            this.deadlineNanos = deadlineNanos;
            this.explicitSingle = explicitSingle;
            this.plan = plan;
        }

        /**
         * True when the query runs here, over its shards: some shard is not held here and a live reader
         * serves it, or a shard belongs to a split project (its shards are always queried as one set,
         * in process when held, never through the local path that knows projects only by name).
         */
        public boolean fansOut() {
            if (hasSplitProject) return true;
            for (ShardState s : shards) {
                if (s.split) return true;
                if (!s.local && !s.route.candidates().isEmpty()) return true;
            }
            return false;
        }

        /** The catalog version this query was routed with (its project ↔ shards mapping). */
        public com.pharos.cluster.sharding.ProjectShards catalog() {
            return plan.catalog();
        }

        /** What the router pruned, and why a filter could not prune where it could not (the query trace). */
        public List<String> routingNotes() {
            return plan.notes();
        }

        /** Notes of what the query decided while it ran (it read a just-resharded project's other version). */
        public List<String> lateNotes() {
            return List.copyOf(lateNotes);
        }

        /**
         * Why a shard was left out because a reader swapped its generation during the query or analyses
         * with other synonyms (a re-plan, {@link DistributedSearch#reopen}, can mend both); null when no
         * shard was left out for such a reason.
         */
        public String retryCause() {
            return retryCause;
        }

        /** The shards of the query in order (their names). */
        public List<String> shardNames() {
            List<String> out = new ArrayList<>(shards.size());
            for (ShardState s : shards) out.add(s.name());
            return out;
        }

        /**
         * Throws {@link com.pharos.cluster.routing.NoShardAnsweredException} when the query has shards,
         * this reader holds none of them, and every one was left out: nothing answered, so an empty
         * result would be indistinguishable from "no matches".
         */
        public void requireAnAnswer() {
            List<String> w = warnings(); // marks the shards no live reader serves first
            synchronized (this) {
                if (shards.isEmpty()) return;
                for (ShardState s : shards) if (!s.statsOnly && (s.local || s.unavailable == null)) return;
            }
            throw new com.pharos.cluster.routing.NoShardAnsweredException(w);
        }

        /** The shards this reader holds, in query order. */
        public List<String> localShards() {
            List<String> out = new ArrayList<>();
            for (ShardState s : shards) if (s.local && !s.statsOnly) out.add(s.name());
            return out;
        }

        /** The shards no live reader serves (not held here either), in query order. */
        public List<String> unservedShards() {
            List<String> out = new ArrayList<>();
            for (ShardState s : shards) if (!s.statsOnly && !s.local && s.route.candidates().isEmpty()) out.add(s.name());
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
            if (degraded.isEmpty() && !s.route.noFilters().isEmpty()) {
                return "every live reader serving it lacks key filter support (" + String.join(", ", s.route.noFilters())
                        + "); upgrade them to filter";
            }
            return degraded.isEmpty() ? "no live reader serves it"
                    : "every live reader serving it is degraded (" + String.join(", ", degraded) + ")";
        }

        /** {@link #markUnavailable(ShardState, String)} for a failure; notes one a re-plan can mend (P6-R16). */
        private void markUnavailable(ShardState s, Throwable t) {
            for (Throwable c = t; c != null; c = c.getCause()) {
                if (c instanceof GenerationMismatchException || c instanceof SynonymsMismatchException) {
                    if (retryCause == null) retryCause = "shard " + s.name() + ": " + reason(t);
                    break;
                }
            }
            markUnavailable(s, reason(t));
        }

        private synchronized void markUnavailable(ShardState s, String reason) {
            if (s.unavailable != null) return;
            s.unavailable = reason == null ? "unknown error" : reason;
            String w = "shard " + s.name() + " unavailable: " + s.unavailable + (s.statsOnly
                    ? "; the query's statistics exclude it (it is not searched: the key filter pruned it)"
                    : "; results exclude it");
            warnings.add(w);
            log.warn("Query fan-out: {}", w);
        }

        /** The available shards the query searches: not the stats-only ones (P6-R13). */
        private List<ShardState> searchable() {
            List<ShardState> out = new ArrayList<>();
            for (ShardState s : available()) if (!s.statsOnly) out.add(s);
            return out;
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
                boolean follow;
                synchronized (s) {
                    // a split project's shard stays on the run its project was pinned to: no re-pin
                    follow = !(s.split || s.consumed || s.repinned || swapped == null || g.servedGeneration() < 0);
                    if (follow) {
                        s.repinned = true;
                        s.generation = g.servedGeneration();
                        s.reader = swapped;
                    }
                }
                if (follow) {
                    Long next = g.servedGeneration();
                    a = client.callPreferring(s.route, swapped, endpoint, hb -> request.apply(hb, next), deadlineNanos);
                } else {
                    // P6-R16: another reader may still serve the pinned generation (a reader in its activation
                    // hold, or one that has not synced yet): the shard is retried there, statistics unchanged
                    a = atPinnedGenerationElsewhere(s, g.node(), endpoint, request, generation);
                    if (a == null) throw g;
                }
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

        /** The shard's call answered at {@code generation} by a candidate other than {@code swapped}, else null. */
        private <Q, R> ShardClient.Answer<R> atPinnedGenerationElsewhere(ShardState s, String swapped,
                                                                         ShardClient.Endpoint<Q, R> endpoint,
                                                                         BiFunction<ReaderHeartbeat, Long, Q> request,
                                                                         Long generation) {
            if (generation == null) return null;
            for (ReaderHeartbeat r : s.route.candidates()) {
                if (r.node().equals(swapped)) continue;
                long left = remainingNanos();
                if (left <= 0) return null;
                try {
                    R resp = client.callOn(r, endpoint, request.apply(r, generation), left / 2);
                    if (endpoint.servedGeneration().applyAsLong(resp) == generation) return new ShardClient.Answer<>(r, resp);
                } catch (RuntimeException e) {
                    log.debug("Query: reader {} does not serve shard {} at generation {}: {}", r.node(), s.name(),
                            generation, e.toString());
                }
            }
            return null;
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
            return fanOut(targets, work, onError, true);
        }

        private <T> Map<ShardState, T> fanOut(List<ShardState> targets, ShardWork<T> work,
                                              java.util.function.BiPredicate<ShardState, Throwable> onError,
                                              boolean reconcileFirst) {
            if (reconcileFirst) {
                reconcileRuns();
                List<ShardState> left = new ArrayList<>();
                synchronized (this) {
                    for (ShardState s : targets) if (s.unavailable == null) left.add(s);
                }
                targets = left;
            }
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
                    if (!onError.test(s, e)) markUnavailable(s, e);
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
                    if (!onError.test(s, ee.getCause())) markUnavailable(s, ee.getCause());
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

        private final Object reconcileLock = new Object();

        /**
         * Keeps every split project's shards on one index run ({@code projectGeneration}), before any of
         * their results are used (the first fan-out of the query runs it, once): pins each of them (an
         * empty term-statistics call for a remote one, a local pin otherwise), takes the newest run seen
         * per project as the query's, and moves every shard pinned to another run: a local one is pinned
         * again (its reader activated the newer set meanwhile), a remote one is asked on its other live
         * readers until one serves that run. A shard no reader serves at that run is left out with a
         * warning: the query never mixes two runs of a project (a moved file seen twice, or not at all).
         */
        private void reconcileRuns() {
            synchronized (reconcileLock) {
                synchronized (this) {
                    if (reconciled) return;
                }
                try {
                    List<ShardState> split = new ArrayList<>();
                    for (ShardState s : shards) if (s.split) split.add(s);
                    if (split.isEmpty()) return;
                    List<ShardState> targets = new ArrayList<>();
                    List<ShardState> pinned = new ArrayList<>();
                    for (ShardState s : available()) {
                        if (s.split && s.expectedGeneration != null) pinned.add(s);
                        else if (s.split) targets.add(s);
                    }
                    if (!pinned.isEmpty()) alignPinnedProjects(pinned);
                    Map<ShardState, Long> runs = fanOut(targets, this::pinRun, (s, t) -> false, false);
                    runs.forEach((s, run) -> s.projectGeneration = run);
                    Map<String, List<ShardState>> byProject = new LinkedHashMap<>();
                    for (ShardState s : targets) byProject.computeIfAbsent(s.project, k -> new ArrayList<>()).add(s);
                    for (Map.Entry<String, List<ShardState>> e : byProject.entrySet()) {
                        alignRuns(e.getKey(), e.getValue());
                    }
                } finally {
                    synchronized (this) {
                        reconciled = true;
                    }
                }
            }
        }

        /**
         * The shards of a version whose generations the catalog pins (a reshard's: the flipped version
         * until the project's next index run, or the retired one a query falls back to): each must be read
         * at exactly its generation. A shard pinned to another one is pinned again (a local reader that
         * activated the version meanwhile) or moved to a live reader serving it. When a shard of a project
         * can't be, and the plan carries the project's other version (the router chose by heartbeats, which
         * lag), the query reads that version instead if all of its shards can be; else the shard is left out
         * with a warning. A query therefore reads one version of the project, every shard at the generation
         * the version is made of: a moved file is never seen twice, nor dropped silently.
         */
        private void alignPinnedProjects(List<ShardState> pinned) {
            Map<String, List<ShardState>> byProject = new LinkedHashMap<>();
            for (ShardState s : pinned) byProject.computeIfAbsent(s.project, k -> new ArrayList<>()).add(s);
            for (Map.Entry<String, List<ShardState>> e : byProject.entrySet()) {
                Map<ShardState, String> failed = alignGenerations(e.getValue());
                if (failed.isEmpty()) continue;
                List<ShardState> alt = new ArrayList<>();
                for (ShardState s : alternates) if (s.project.equals(e.getKey())) alt.add(s);
                if (!alt.isEmpty() && servedAsPinned(alt)) {
                    switchTo(e.getKey(), alt);
                    continue;
                }
                failed.forEach(this::markUnavailable);
            }
        }

        /** True when every shard of {@code version} (another version of a project) is served as it pins. */
        private boolean servedAsPinned(List<ShardState> version) {
            List<ShardState> pinned = new ArrayList<>();
            for (ShardState s : version) {
                if (!s.local && s.route.candidates().isEmpty()) return false;
                if (s.split && s.expectedGeneration != null) pinned.add(s);
            }
            return alignGenerations(pinned).isEmpty();
        }

        /** The query reads {@code version} of {@code project} instead of the version it was planned with. */
        private void switchTo(String project, List<ShardState> version) {
            List<ShardState> dropped = new ArrayList<>();
            synchronized (this) {
                List<ShardState> next = new ArrayList<>();
                boolean placed = false;
                for (ShardState s : shards) {
                    if (!s.project.equals(project)) {
                        next.add(s);
                        continue;
                    }
                    dropped.add(s);
                    if (!placed) {
                        next.addAll(version);
                        placed = true;
                    }
                }
                if (!placed) next.addAll(version);
                shards = List.copyOf(next);
            }
            for (ShardState s : dropped) {
                synchronized (s) {
                    if (s.pin != null) {
                        try {
                            s.pin.close();
                        } catch (IOException ex) {
                            log.warn("Releasing the reader of shard {} failed: {}", s.name(), ex.toString());
                        }
                        s.pin = null;
                    }
                }
            }
            java.util.Set<ShardId> active = new java.util.HashSet<>(plan.catalog().shardsOf(project));
            java.util.Set<ShardId> read = new java.util.HashSet<>();
            for (ShardState s : version) read.add(s.route.shard());
            String note = "project " + project + ": " + (read.equals(active)
                    ? "the readers serve its new spec version (just resharded); this query reads it"
                    : "the readers do not serve its new spec version (just resharded) yet; this query reads the"
                      + " previous version");
            lateNotes.add(note);
            log.debug("Query: {}", note);
        }

        /**
         * Pins every shard of {@code pinned} at its expected generation (see {@link #alignPinnedProjects});
         * returns the ones that could not be, with why. Marks nothing unavailable.
         */
        private Map<ShardState, String> alignGenerations(List<ShardState> pinned) {
            Map<ShardState, String> failed = new LinkedHashMap<>();
            Map<ShardState, Long> gens = fanOut(pinned, this::pinGeneration, (s, t) -> {
                failed.put(s, reason(t));
                return true;
            }, false);
            for (ShardState s : pinned) {
                if (failed.containsKey(s) || s.unavailable != null) continue;
                Long have = gens.get(s);
                if (have == null) {
                    failed.put(s, "no answer");
                    continue;
                }
                if (have.longValue() == s.expectedGeneration) continue;
                long want = s.expectedGeneration;
                if (s.local) {
                    synchronized (s) {
                        try {
                            if (s.pin != null) s.pin.close();
                            s.pin = null;
                            have = pin(s).generation();
                        } catch (IOException | RuntimeException e) {
                            have = null;
                        }
                    }
                    if (have != null && have == want) continue;
                }
                if (moveRemoteGeneration(s, want)) continue;
                failed.put(s, "no live reader serves it at generation " + want + ", which the version of project "
                        + s.project + " this query reads is made of (found generation " + have + ")");
            }
            return failed;
        }

        /** Pins {@code s} and returns its pinned generation. */
        private Long pinGeneration(ShardState s) throws IOException {
            if (s.local) return pin(s).generation();
            TermStatsResponse r = remote(s, ShardClient.TERM_STATS, false,
                    (hb, gen) -> new TermStatsRequest(s.name(), List.of(), List.of(), local.synonymsHash()));
            return r.generation();
        }

        /**
         * Asks the live readers of {@code s} (in route order) for its generation {@code generation}; pins
         * the shard to the first that serves it (served remotely from then on). False when none does.
         */
        private boolean moveRemoteGeneration(ShardState s, long generation) {
            ReaderHeartbeat current;
            synchronized (s) {
                current = s.reader;
            }
            for (ReaderHeartbeat r : s.route.candidates()) {
                if (current != null && r.node().equals(current.node())) continue;
                long left = remainingNanos();
                if (left <= 0) return false;
                try {
                    TermStatsResponse resp = client.callOn(r, ShardClient.TERM_STATS,
                            new TermStatsRequest(s.name(), List.of(), List.of(), local.synonymsHash()), left / 2);
                    if (resp.generation() != generation) continue;
                    synchronized (s) {
                        s.reader = r;
                        s.generation = resp.generation();
                        s.projectGeneration = resp.projectGeneration();
                        if (s.local) {
                            s.local = false;
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
                    return true;
                } catch (RuntimeException e) {
                    log.debug("Query: reader {} could not serve shard {} at generation {}: {}", r.node(), s.name(),
                            generation, e.toString());
                }
            }
            return false;
        }

        /** Pins {@code s} and returns the index run of its pinned generation (0 when it states none). */
        private Long pinRun(ShardState s) throws IOException {
            if (s.local) {
                Long run = pin(s).projectGeneration();
                return run == null ? 0L : run;
            }
            TermStatsResponse r = remote(s, ShardClient.TERM_STATS, false,
                    (hb, gen) -> new TermStatsRequest(s.name(), List.of(), List.of(), local.synonymsHash()));
            return r.projectGeneration() == null ? 0L : r.projectGeneration();
        }

        /** Moves the shards of one split project onto the newest run any of them is pinned to. */
        private void alignRuns(String project, List<ShardState> projectShards) {
            for (int round = 0; round < 3; round++) {
                long target = 0;
                for (ShardState s : projectShards) {
                    if (s.unavailable == null && s.projectGeneration != null) target = Math.max(target, s.projectGeneration);
                }
                boolean moved = false;
                for (ShardState s : projectShards) {
                    if (s.unavailable != null || s.projectGeneration == null || s.projectGeneration == target) continue;
                    long from = s.projectGeneration;
                    Long now = s.local ? repinLocal(s) : moveRemote(s, target);
                    if (s.local && (now == null || now < target)) {
                        // this node's reader lags the query's run: a replica at that run serves the shard instead
                        now = moveRemote(s, target);
                    }
                    if (now != null && now == target) {
                        log.debug("Query: shard {} moved from run {} to run {} of project {}", s.name(), from, target, project);
                        continue;
                    }
                    if (now != null && now > target) { // the local set flipped once more: align on that run
                        moved = true;
                        continue;
                    }
                    markUnavailable(s, "no live reader serves it at index run " + target + " of project " + project
                            + ", the run the query reads its other shards from (still at run " + from + ")");
                }
                if (!moved) return;
            }
        }

        /** Pins a local shard again (its reader activated another set); its new run. */
        private Long repinLocal(ShardState s) {
            synchronized (s) {
                try {
                    if (s.pin != null) s.pin.close();
                    s.pin = null;
                    Long run = pin(s).projectGeneration();
                    s.projectGeneration = run == null ? 0L : run;
                    return s.projectGeneration;
                } catch (IOException | RuntimeException e) {
                    return null;
                }
            }
        }

        /**
         * Asks the other live readers of a shard, in route order, for its generation at run {@code target};
         * pins the shard to the first that serves it (a local shard is then served remotely). The run, or
         * null when none does.
         */
        private Long moveRemote(ShardState s, long target) {
            ReaderHeartbeat current;
            synchronized (s) {
                current = s.reader;
            }
            for (ReaderHeartbeat r : s.route.candidates()) {
                if (current != null && r.node().equals(current.node())) continue;
                long left = remainingNanos();
                if (left <= 0) return null;
                try {
                    TermStatsResponse resp = client.callOn(r, ShardClient.TERM_STATS,
                            new TermStatsRequest(s.name(), List.of(), List.of(), local.synonymsHash()), left / 2);
                    if (resp.projectGeneration() != null && resp.projectGeneration() == target) {
                        synchronized (s) {
                            s.reader = r;
                            s.generation = resp.generation();
                            s.projectGeneration = target;
                            if (s.local) { // served remotely from now on: release the local reader
                                s.local = false;
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
                        return target;
                    }
                } catch (RuntimeException e) {
                    log.debug("Query: reader {} could not serve shard {} at run {}: {}", r.node(), s.name(), target,
                            e.toString());
                }
            }
            return null;
        }

        /** The shard that returned result {@code r}, else the shard named like its project. */
        private String ownerName(SearchResult r) {
            ShardState o = owners.get(r.id());
            return o != null ? o.name() : r.project();
        }

        /**
         * The shards graph-file lookups (neighbours, usages, paths) ask: every available shard, but one
         * per split project (they all serve the project's one graph of the same run).
         */
        private List<ShardState> graphFileTargets() {
            List<ShardState> out = new ArrayList<>();
            Set<String> splitSeen = new java.util.HashSet<>();
            for (ShardState s : searchable()) {
                if (s.split && !splitSeen.add(s.project)) continue;
                out.add(s);
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
            if (t instanceof com.pharos.search.KeyFiltersUnsupportedException) return t.getMessage();
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
            int cap = maxShippedTerms;
            boolean capped = terms.size() > cap;
            log.debug("Fan-out query ships {} term(s) (synonym expansions included){}", terms.size(),
                    capped ? "; capped at " + cap : "");
            if (capped) {
                Set<Term> shipped = new LinkedHashSet<>();
                for (Term t : terms) {
                    if (shipped.size() == cap) break;
                    shipped.add(t);
                }
                warn("query expands to " + terms.size() + " terms; the " + (terms.size() - cap)
                        + " beyond " + cap + " are scored with each shard's own statistics");
                terms = shipped;
            }
            GlobalStats merged = gather(terms, local.synonymsHash());
            if (capped) merged = merged.asPartial();
            java.util.function.Consumer<String> hook = afterStatsRound;
            if (hook != null) hook.accept(req.query());
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
            boolean unified = "unified".equals(retriever);
            for (ShardState s : searchable()) {
                synchronized (this) {
                    // unified keeps a shard whose project lacks the model's vectors (its documents score
                    // by the rescorer's BM25 fallback, as on a single node)
                    if (vec == null || !vectorless.contains(s.name()) || (unified && noModelVectors.contains(s.name()))) {
                        targets.add(s);
                    }
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
                    if (unified) r = withoutVectorRetry(r, targets, retriever, req, vec, stats);
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
                    first = withoutVectorRetry(first, targets, "unified-recall", req, vec, stats);
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
            List<RawCandidate> merged = mergeRaw(lists, req.limit(), "keyword".equals(retriever));
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
            String queryModelId = vec == null || model == null ? null : model.modelId();
            boolean unifiedRound = retriever.startsWith("unified");
            Map<ShardState, ShardSearchResponse> answers = fanOut(targets, s -> {
                boolean unchecked; // unified on a shard without the model's vectors: sent unchecked
                boolean drop;
                synchronized (this) {
                    unchecked = vec != null && unifiedRound && noModelVectors.contains(s.name());
                    drop = vec != null && !unchecked && vectorless.contains(s.name());
                }
                float[] v = drop ? null : vec;
                String modelId = v == null || model == null || unchecked ? null : model.modelId();
                int dims = v == null ? 0 : v.length;
                String fingerprint = v == null || model == null ? null : model.fingerprint();
                if (s.local) {
                    if (modelId != null) local.checkModel(s.route.shard(), modelId, dims, fingerprint);
                    return local.search(pin(s), retriever, req, v, stats, selected.apply(s), selectedKnn.apply(s), chunks);
                }
                return remote(s, ShardClient.SHARD_SEARCH, true, // first-round doc ids bind the generation too
                        (hb, gen) -> new ShardSearchRequest(s.name(), gen, retriever, req, v, stats, hash, modelId,
                                dims, fingerprint, selected.apply(s), selectedKnn.apply(s), chunks));
            }, (s, t) -> {
                ModelMismatchException m = modelMismatch(t);
                if (m == null) return false;
                boolean noVectors = m.reason() == ModelMismatchException.Reason.NO_VECTORS;
                String detail = noVectors ? ""
                        : " (" + m.getMessage() + ")";
                if (explicitSingle) {
                    throw new com.pharos.search.VectorModelUnavailableException(String.format(
                            "Project '%s' has no vectors for search embedding model '%s'%s. Run: pharos embed --model=%s %s",
                            s.project, queryModelId, detail, queryModelId, s.project));
                }
                synchronized (this) {
                    vectorless.add(s.name());
                    if (noVectors) noModelVectors.add(s.name());
                }
                warn(com.pharos.search.SearchEngine.noVectorsWarning(s.project, queryModelId + detail));
                return true;
            });
            answers.forEach((s, resp) -> {
                for (RawCandidate c : candidates(resp)) {
                    if (c.result() != null && c.result().id() != null) owners.put(c.result().id(), s);
                }
            });
            return answers;
        }

        /**
         * Unified keeps a shard whose project has no vectors of the model: the shards of {@code targets}
         * that refused the query vector in {@code answered}'s round for that reason are asked again
         * with it unchecked (see {@link #noModelVectors}), and the answers are merged in ordinal order.
         */
        private Map<ShardState, ShardSearchResponse> withoutVectorRetry(Map<ShardState, ShardSearchResponse> answered,
                                                                       List<ShardState> targets, String retriever,
                                                                       SearchRequest req, float[] vec, GlobalStats stats) {
            if (vec == null) return answered;
            List<ShardState> again = new ArrayList<>();
            for (ShardState s : targets) {
                synchronized (this) {
                    if (!answered.containsKey(s) && noModelVectors.contains(s.name()) && s.unavailable == null) again.add(s);
                }
            }
            if (again.isEmpty()) return answered;
            Map<ShardState, ShardSearchResponse> more = round(again, retriever, req, vec, stats,
                    s -> null, s -> null, false);
            Map<ShardState, ShardSearchResponse> out = new LinkedHashMap<>();
            for (ShardState s : targets) {
                if (answered.containsKey(s)) out.put(s, answered.get(s));
                else if (more.containsKey(s)) out.put(s, more.get(s));
            }
            return out;
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
            for (SearchResult r : results) idsByShard.computeIfAbsent(ownerName(r), k -> new ArrayList<>()).add(r.id());
            Map<String, SnippetResolver> perShard = new HashMap<>();
            return r -> {
                SnippetResolver inner;
                String owner = ownerName(r);
                synchronized (perShard) {
                    inner = perShard.computeIfAbsent(owner, shard -> shardResolver(shard,
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
                List<String> callees = colon < 0 ? List.of() : calledBy(ownerName(parent), parent.id().substring(colon + 1));
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
            boolean graphFile = "neighbors".equals(op) || "usages".equals(op) || "path".equals(op);
            reconcileRuns(); // before choosing the one shard of each split project
            return fanOut(graphFile ? graphFileTargets() : searchable(),
                    s -> graph(s, op, fqns, direction, kind, to, limit, stats));
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
