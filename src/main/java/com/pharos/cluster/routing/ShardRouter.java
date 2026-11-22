package com.pharos.cluster.routing;

import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.MembershipSource;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.sharding.ProjectShards;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

/**
 * Picks, per shard, the live readers that may serve it. A reader's heartbeat lists the shards it
 * actually holds (never "all"), so a shard is routable exactly to readers naming it. Draining
 * readers are skipped, and a reader is skipped for the shards its heartbeat lists as degraded (only
 * those); warming readers are a last resort and suspect ones are tried only when nothing else is
 * left (see {@link #candidates}); within a tier, by least {@code inFlight}, rotating among ties.
 *
 * <p><b>Projects and shards (Phase 6).</b> A query names projects; {@link #plan} resolves each to its
 * shards through one read of the catalog's project ↔ shards mapping ({@link ProjectShards}), which the
 * plan carries so the query uses that one version throughout. Nothing is derived from a shard id: a
 * project the catalog does not know is routed as its {@code [project]} shard, as before. A
 * {@link ShardFilter} prunes a split project's shards against its spec; what it can't prune fans out,
 * and the plan's notes say why. The shards of one split project share one rotation turn and one load
 * snapshot, so with readers holding the whole project they all go to the same reader first.
 */
public final class ShardRouter {

    /**
     * {@code candidates} is empty when no live reader serves {@code shard}; {@code degraded} names the
     * live readers left out because the shard is degraded on them.
     */
    public record Route(ShardId shard, List<ReaderHeartbeat> candidates, List<String> degraded,
                        List<String> noFilters) {
        public Route {
            degraded = degraded == null ? List.of() : List.copyOf(degraded);
            noFilters = noFilters == null ? List.of() : List.copyOf(noFilters);
        }

        public Route(ShardId shard, List<ReaderHeartbeat> candidates, List<String> degraded) {
            this(shard, candidates, degraded, List.of());
        }

        public Route(ShardId shard, List<ReaderHeartbeat> candidates) {
            this(shard, candidates, List.of());
        }
    }

    private final Supplier<List<ReaderHeartbeat>> live;
    private final ShardCatalog catalog;
    private final ReaderHealth health;
    private final Predicate<ShardId> local;
    private final ToIntFunction<String> coordinatorInFlight;
    /** Rotation turns: per shard, or per project for the shards of a split project. */
    private final Map<String, AtomicLong> rotation = new ConcurrentHashMap<>();

    /**
     * @param live   the live readers (this node's own entry included)
     * @param local  whether this reader holds a shard
     */
    public ShardRouter(Supplier<List<ReaderHeartbeat>> live, ShardCatalog catalog, ReaderHealth health,
                       Predicate<ShardId> local) {
        this(live, catalog, health, local, node -> 0); // a coordinator passes ShardClient::inFlight (ClusterRuntime)
    }

    /**
     * @param coordinatorInFlight calls this coordinator currently has open to a node; added to the
     *                            heartbeat's {@code inFlight} (which lags by a heartbeat) when ordering
     */
    public ShardRouter(Supplier<List<ReaderHeartbeat>> live, ShardCatalog catalog, ReaderHealth health,
                       Predicate<ShardId> local, ToIntFunction<String> coordinatorInFlight) {
        this(MembershipSource.of(live), catalog, health, local, coordinatorInFlight);
    }

    /**
     * Routes over {@code membership}: the bucket's reader registry, or the peer writers' merged
     * registries (no bucket); both list the same live readers for the same heartbeats.
     */
    public ShardRouter(MembershipSource membership, ShardCatalog catalog, ReaderHealth health,
                       Predicate<ShardId> local, ToIntFunction<String> coordinatorInFlight) {
        this.coordinatorInFlight = coordinatorInFlight;
        this.live = java.util.Objects.requireNonNull(membership, "membership")::live;
        this.catalog = catalog;
        this.health = health;
        this.local = local;
    }

    /** One shard of a plan: its route, the project it serves, and whether that project is split. */
    public record PlannedShard(Route route, String project, boolean split, boolean statsOnly) {
        public PlannedShard(Route route, String project, boolean split) {
            this(route, project, split, false);
        }

        public ShardId shard() { return route.shard(); }
    }

    /**
     * A query's routing: the catalog version it was planned with, the shards in query order (the
     * requested projects' order, else project name order; within a project, catalog order), and the
     * routing notes (what a filter pruned or could not prune).
     */
    public record RoutePlan(ProjectShards catalog, List<PlannedShard> shards, List<String> notes,
                            List<String> warnings) {
        public RoutePlan {
            shards = List.copyOf(shards);
            notes = List.copyOf(notes);
            warnings = List.copyOf(warnings);
        }

        /** The routes of the shards the query searches (not the stats-only ones). */
        public List<Route> routes() {
            List<Route> out = new ArrayList<>(shards.size());
            for (PlannedShard s : shards) if (!s.statsOnly()) out.add(s.route());
            return out;
        }
    }

    /** One route per shard of the requested projects in shard-name order; every project's shards when empty. */
    public List<Route> route(Collection<String> projects) {
        List<Route> out = new ArrayList<>(plan(projects, ShardFilter.NONE).routes());
        out.sort(Comparator.comparing(r -> r.shard().value()));
        return out;
    }

    /**
     * Routes the shards of {@code projects} (empty: every project of the catalog) after pruning them
     * with {@code filter}, from one read of the catalog mapping (see the class comment).
     *
     * @throws InvalidProjectException     a requested name is not a valid project name
     * @throws CatalogUnavailableException unscoped, and the catalog cannot be used
     */
    public RoutePlan plan(Collection<String> projects, ShardFilter filter) {
        boolean unscoped = projects == null || projects.isEmpty();
        List<String> requested = new ArrayList<>();
        if (!unscoped) {
            for (String p : new java.util.LinkedHashSet<>(projects)) {
                if (!ShardId.isProjectName(p)) {
                    try {
                        ShardId.ofProject(p);
                    } catch (IllegalArgumentException e) {
                        throw new InvalidProjectException(e.getMessage(), e);
                    }
                }
                requested.add(p);
            }
        }
        ProjectShards mapping;
        try {
            mapping = catalog.projectShards();
        } catch (CatalogTooOldException e) {
            if (unscoped) throw e;
            mapping = e.lastKnownProjects(); // a scoped query still maps its projects with the last good read
        } catch (CatalogUnavailableException e) {
            if (unscoped) throw e;
            mapping = ProjectShards.identity(List.of()); // never read: every project is its [project] shard
        } catch (IOException e) {
            if (unscoped) throw new CatalogUnavailableException("unreadable shard catalog: " + e.getMessage(), e);
            mapping = ProjectShards.identity(List.of());
        }
        List<String> names = unscoped ? new ArrayList<>(mapping.projects()) : requested;
        ShardFilter f = filter == null ? ShardFilter.NONE : filter;
        boolean needsFilters = !f.isEmpty();
        List<ReaderHeartbeat> readers = live.get();
        Map<String, Integer> loads = new java.util.HashMap<>(); // one load snapshot per plan
        for (ReaderHeartbeat r : readers) loads.put(r.node(), r.inFlight() + coordinatorInFlight.applyAsInt(r.node()));
        List<PlannedShard> out = new ArrayList<>();
        List<String> notes = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        for (String project : names) {
            List<ShardId> shards = mapping.shardsOf(project);
            boolean split = mapping.isSplit(project);
            if (shards.isEmpty()) shards = List.of(ShardId.ofProject(project)); // unknown: its [project] shard
            java.util.Set<ShardId> searched = new java.util.HashSet<>(shards);
            if (split) {
                ShardFilter.Pruned pruned = f.prune(project, mapping.specOf(project), shards);
                notes.addAll(pruned.notes());
                if (pruned.kept().isEmpty()) { // neither round: nothing of the project can match
                    warnings.add("project " + project + ": the key filter matches none of its " + shards.size()
                            + " shards under spec " + mapping.specOf(project) + "; no results from it");
                    shards = List.of();
                } else if (pruned.kept().size() < shards.size()) {
                    // P6-R13: the statistics stay the whole project's (as one node filtering scores), so
                    // every shard answers the term-statistics round; only the kept ones are searched
                    notes.add("project " + project + ": stats: " + shards.size() + " shards, search: "
                            + pruned.kept().size() + " of " + shards.size());
                }
                searched = new java.util.HashSet<>(pruned.kept());
            }
            String turnKey = split ? project : null;
            long turn = -1;
            for (ShardId shard : shards) {
                String key = turnKey != null ? turnKey : shard.value();
                if (turnKey == null || turn < 0) {
                    turn = rotation.computeIfAbsent(key, k -> new AtomicLong()).getAndIncrement();
                }
                List<String> degraded = new ArrayList<>();
                List<String> noFilters = new ArrayList<>();
                List<ReaderHeartbeat> usable = readers;
                boolean statsOnly = !searched.contains(shard);
                if (needsFilters && !statsOnly) { // a reader that would ignore the filter must not search for it
                    usable = new ArrayList<>();
                    for (ReaderHeartbeat r : readers) {
                        if (r.supportsFilters()) usable.add(r);
                        else if (r.shards() != null && r.shards().contains(shard.value())) noFilters.add(r.node());
                    }
                }
                out.add(new PlannedShard(new Route(shard, candidates(shard, usable, degraded, loads, turn), degraded,
                        noFilters), project, split, statsOnly));
            }
        }
        return new RoutePlan(mapping, out, notes, warnings);
    }

    /**
     * The shard names of the catalog for naming purposes (e.g. a query mentioning a project): the
     * current catalog, the last good read when it is too old to route with, empty before any read.
     * Never throws and never blocks on a catalog read another query is already making.
     */
    public List<String> catalogShardNames() {
        List<ShardId> shards;
        try {
            shards = catalog.shards();
        } catch (CatalogTooOldException e) {
            shards = e.lastKnown();
        } catch (IOException | RuntimeException e) {
            return List.of();
        }
        List<String> out = new ArrayList<>(shards.size());
        for (ShardId s : shards) out.add(s.value());
        return out;
    }

    /**
     * The project names of the catalog (e.g. a query mentioning a project), with the same fallbacks as
     * {@link #catalogShardNames}: a split project is one name, never one per shard.
     */
    public List<String> catalogProjectNames() {
        ProjectShards mapping;
        try {
            mapping = catalog.projectShards();
        } catch (CatalogTooOldException e) {
            mapping = e.lastKnownProjects();
        } catch (IOException | RuntimeException e) {
            return List.of();
        }
        return List.copyOf(mapping.projects());
    }

    /** True when this reader holds the shard. */
    public boolean servesLocally(ShardId shard) {
        return local.test(shard);
    }

    /**
     * The readers to try for {@code shard}, in order. Draining readers, and readers listing the
     * shard as degraded, never get it. Ready (or otherwise degraded) readers come first; a reader
     * still {@code warming} only when no such reader is left; and when every remaining reader is
     * suspect, those are tried anyway, least recently marked first (fail open: one call can clear a
     * mark, so a shard whose readers all failed once is not lost for a whole TTL).
     *
     * @param degraded receives the readers left out because {@code shard} is degraded on them
     */
    private List<ReaderHeartbeat> candidates(ShardId shard, List<ReaderHeartbeat> readers, List<String> degraded,
                                             Map<String, Integer> loads, long turn) {
        List<ReaderHeartbeat> eligible = new ArrayList<>();
        List<ReaderHeartbeat> warming = new ArrayList<>();
        List<ReaderHeartbeat> suspect = new ArrayList<>();
        for (ReaderHeartbeat r : readers) {
            if (r.draining() || r.shards() == null || !r.shards().contains(shard.value())) continue;
            if (r.degradedShards().contains(shard.value())) { // only for this shard: the rest stays routable
                degraded.add(r.node());
                continue;
            }
            if (health.isSuspect(r.node())) suspect.add(r);
            else if (ReaderHeartbeat.WARMING.equals(r.state())) warming.add(r);
            else eligible.add(r);
        }
        List<ReaderHeartbeat> out = ordered(eligible, loads, turn);
        out.addAll(ordered(warming, loads, turn));
        if (out.isEmpty() && !suspect.isEmpty()) {
            Map<String, Long> until = new java.util.HashMap<>();
            for (ReaderHeartbeat r : suspect) until.put(r.node(), health.suspectUntil(r.node()));
            suspect.sort(Comparator.comparingLong((ReaderHeartbeat r) -> until.get(r.node()))
                    .thenComparing(ReaderHeartbeat::node));
            out.addAll(suspect);
        }
        return out;
    }

    /**
     * {@code eligible} by least load, rotating among ties by {@code turn}. {@code loads} is one snapshot
     * per plan: the coordinator's in-flight counts move while it runs, and a load read twice could break
     * the sort's contract, leave an empty tie group, or order the shards of one project differently.
     */
    private List<ReaderHeartbeat> ordered(List<ReaderHeartbeat> eligible, Map<String, Integer> loads, long turn) {
        if (eligible.isEmpty()) return new ArrayList<>();
        ToIntFunction<ReaderHeartbeat> load = r -> loads.getOrDefault(r.node(), r.inFlight());
        eligible.sort(Comparator.comparingInt(load).thenComparing(ReaderHeartbeat::node));
        List<ReaderHeartbeat> out = new ArrayList<>(eligible.size());
        for (int i = 0; i < eligible.size(); ) {
            int j = i;
            while (j < eligible.size() && load.applyAsInt(eligible.get(j)) == load.applyAsInt(eligible.get(i))) j++;
            int size = j - i;
            int shift = (int) Math.floorMod(turn, (long) size);
            for (int k = 0; k < size; k++) out.add(eligible.get(i + (k + shift) % size));
            i = j;
        }
        return out;
    }
}
