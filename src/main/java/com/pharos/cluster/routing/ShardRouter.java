package com.pharos.cluster.routing;

import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.ReaderHeartbeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
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
 */
public final class ShardRouter {

    /**
     * {@code candidates} is empty when no live reader serves {@code shard}; {@code degraded} names the
     * live readers left out because the shard is degraded on them.
     */
    public record Route(ShardId shard, List<ReaderHeartbeat> candidates, List<String> degraded) {
        public Route {
            degraded = degraded == null ? List.of() : List.copyOf(degraded);
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
    private final Map<ShardId, AtomicLong> rotation = new ConcurrentHashMap<>();

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
        this.coordinatorInFlight = coordinatorInFlight;
        this.live = live;
        this.catalog = catalog;
        this.health = health;
        this.local = local;
    }

    /** One route per requested project in shard order; every catalog shard when {@code projects} is empty. */
    public List<Route> route(Collection<String> projects) {
        TreeSet<String> names = new TreeSet<>();
        if (projects == null || projects.isEmpty()) {
            try {
                for (ShardId s : catalog.shards()) names.add(s.value());
            } catch (IOException e) {
                throw new CatalogUnavailableException("unreadable shard catalog: " + e.getMessage(), e);
            }
        } else {
            for (String p : projects) {
                try {
                    names.add(new ShardId(p).value());
                } catch (IllegalArgumentException e) {
                    throw new InvalidProjectException(e.getMessage(), e);
                }
            }
        }
        List<ReaderHeartbeat> readers = live.get();
        List<Route> routes = new ArrayList<>(names.size());
        for (String n : names) {
            ShardId shard = new ShardId(n);
            List<String> degraded = new ArrayList<>();
            routes.add(new Route(shard, candidates(shard, readers, degraded), degraded));
        }
        return routes;
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
    private List<ReaderHeartbeat> candidates(ShardId shard, List<ReaderHeartbeat> readers, List<String> degraded) {
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
        List<ReaderHeartbeat> out = ordered(shard, eligible);
        out.addAll(ordered(shard, warming));
        if (out.isEmpty() && !suspect.isEmpty()) {
            Map<String, Long> until = new java.util.HashMap<>();
            for (ReaderHeartbeat r : suspect) until.put(r.node(), health.suspectUntil(r.node()));
            suspect.sort(Comparator.comparingLong((ReaderHeartbeat r) -> until.get(r.node()))
                    .thenComparing(ReaderHeartbeat::node));
            out.addAll(suspect);
        }
        return out;
    }

    /** {@code eligible} by least load, rotating among ties. */
    private List<ReaderHeartbeat> ordered(ShardId shard, List<ReaderHeartbeat> eligible) {
        if (eligible.isEmpty()) return new ArrayList<>();
        // One snapshot of each reader's load: the coordinator's in-flight counts move while this runs,
        // and a load read twice could break the sort's contract or leave an empty tie group.
        Map<String, Integer> loads = new java.util.HashMap<>();
        for (ReaderHeartbeat r : eligible) {
            loads.put(r.node(), r.inFlight() + coordinatorInFlight.applyAsInt(r.node()));
        }
        ToIntFunction<ReaderHeartbeat> load = r -> loads.get(r.node());
        eligible.sort(Comparator.comparingInt(load).thenComparing(ReaderHeartbeat::node));
        long turn = rotation.computeIfAbsent(shard, s -> new AtomicLong()).getAndIncrement();
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
