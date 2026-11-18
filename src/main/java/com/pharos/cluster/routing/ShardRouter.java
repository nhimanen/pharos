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
 * actually holds (never "all"), so a shard is routable exactly to readers naming it. Draining and
 * suspect readers are skipped; the rest are ordered by least {@code inFlight}, rotating among ties.
 */
public final class ShardRouter {

    /** {@code candidates} is empty when no live reader serves {@code shard}. */
    public record Route(ShardId shard, List<ReaderHeartbeat> candidates) {}

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
            routes.add(new Route(shard, candidates(shard, readers)));
        }
        return routes;
    }

    /** True when this reader holds the shard. */
    public boolean servesLocally(ShardId shard) {
        return local.test(shard);
    }

    private List<ReaderHeartbeat> candidates(ShardId shard, List<ReaderHeartbeat> readers) {
        List<ReaderHeartbeat> eligible = new ArrayList<>();
        for (ReaderHeartbeat r : readers) {
            if (!r.draining() && r.shards() != null && r.shards().contains(shard.value())
                    && !health.isSuspect(r.node())) {
                eligible.add(r);
            }
        }
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
