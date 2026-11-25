package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.SynonymsMerge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The shard catalog of a peer-mode reader: every discovered writer's {@code GET /replication/catalog}
 * merged into one live view, re-read at most every {@code cluster.peers.refreshSeconds} (by
 * {@link #shards} when due, or {@link #refresh}). It is what {@code ReplicaSyncer}, {@code CachedCatalog}
 * and {@code ShardRouter} consume, so readers and coordinators work as with a bucket.
 *
 * <p><b>Writers.</b> A writer is identified by the node id it reports; two URLs answering with one id
 * are one writer (the URL it was first seen at is kept). A writer that does not answer a refresh
 * (unreachable, draining, or no longer discovered) is <i>missing</i>; missing for
 * {@value #DROP_AFTER} refreshes in a row, it is <i>dropped</i>.
 *
 * <p><b>Shards.</b> Each shard a live writer lists has an owner, the writer {@link PeerReplicaSource}
 * syncs it from. A shard keeps its owner while that writer is missing (syncs fail; the reader keeps
 * serving its generation). When the owner is dropped the shard stays listed with no owner, so its
 * cached generation keeps serving ("kill the writer and readers keep serving"), and so do shards this
 * reader holds that no writer has claimed yet (a writer down when the reader started). A shard leaves
 * the list only when its owner is live and stops listing it with no one else claiming it.
 *
 * <p><b>Conflicts (Review Focus 0).</b> When several live writers list a shard, the one with the
 * higher {@code LATEST} owns it, on a tie the lowest node id (P5-R9: a pure function of the claims).
 * A writer that is missing but not yet dropped keeps its last-known claims and {@code LATEST} in
 * this, and a shard never moves away from such an owner (C1): one missed refresh must not roll the
 * reader back to a lower claimer. Each conflict is reported as {@code shard X claimed by A and B}
 * (in {@code /health}), also while its owner is missing.
 *
 * <p>Before any writer ever answered, {@link #shards} lists the held shards, or throws when there
 * are none, so a reader whose writers are down at start serves what it has on disk.
 */
public final class PeerCatalog implements ShardCatalog {

    private static final Logger log = LoggerFactory.getLogger(PeerCatalog.class);
    /** Refreshes in a row a writer may miss before it is dropped. */
    public static final int DROP_AFTER = 3;
    private static final java.util.Comparator<ShardId> BY_ID = java.util.Comparator.comparing(ShardId::value);

    /** A writer as readers address it. */
    public record WriterRef(String node, URI url) {
        @Override
        public String toString() { return node + " (" + url + ")"; }
    }

    /** One writer's state for {@code /health}. */
    public record WriterStatus(String node, String url, boolean live, int missedRefreshes, int shards, String error) {}

    private static final class Tracked {
        final String node;
        URI url;
        Map<ShardId, Long> shards = Map.of();
        int missed;
        String error;

        Tracked(String node, URI url) {
            this.node = node;
            this.url = url;
        }
    }

    private record Answer(URI url, String node, Map<ShardId, Long> shards) {}

    private final PeerDiscovery discovery;
    private final PeerHttp http;
    private final long refreshNanos;
    private final LongSupplier clock;
    private volatile Supplier<Set<ShardId>> held = Set::of;

    // guarded by this
    private final Map<String, Tracked> writers = new TreeMap<>();
    private final Map<ShardId, String> owner = new TreeMap<>(BY_ID);
    private final Set<ShardId> orphans = new TreeSet<>(BY_ID);
    private final Set<ShardId> released = new HashSet<>();
    private List<String> conflicts = List.of();
    private String lastError;
    private boolean everAnswered;
    private long refreshedAt;
    private boolean refreshed;

    public PeerCatalog(PeerDiscovery discovery, PeerHttp http, Duration refreshEvery, LongSupplier nanoClock) {
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.http = Objects.requireNonNull(http, "http");
        this.refreshNanos = Objects.requireNonNull(refreshEvery, "refreshEvery").toNanos();
        this.clock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    /** The shards this reader holds (its registry): listed until a live owner releases them. */
    public void setHeld(Supplier<Set<ShardId>> held) {
        this.held = Objects.requireNonNull(held, "held");
    }

    public PeerDiscovery discovery() { return discovery; }

    /** The refresh interval ({@code cluster.peers.refreshSeconds}). */
    public Duration refreshInterval() { return Duration.ofNanos(refreshNanos); }

    @Override
    public List<ShardId> shards() throws IOException {
        refreshIfDue();
        List<ShardId> out = knownShards();
        synchronized (this) {
            if (!everAnswered && out.isEmpty()) {
                throw new IOException("no writer reachable through cluster.peers.discovery '"
                        + discovery.spec().text() + "'" + (lastError == null ? "" : ": " + lastError));
            }
            return out;
        }
    }

    /** What {@link #shards} lists, without refreshing (sorted): owned, orphaned and held shards. */
    public List<ShardId> knownShards() {
        Set<ShardId> h = heldNow();
        synchronized (this) {
            Set<ShardId> out = new TreeSet<>(BY_ID);
            out.addAll(owner.keySet());
            out.addAll(orphans);
            for (ShardId s : h) if (!released.contains(s)) out.add(s);
            return List.copyOf(out);
        }
    }

    /**
     * Refreshes when the last refresh is {@code refreshSeconds} old (or there was none). Concurrent
     * callers (the syncer, a coordinator's catalog cache) share one refresh: a writer's missed refresh
     * is never counted twice.
     */
    public void refreshIfDue() {
        refreshLock.lock();
        try {
            synchronized (this) {
                if (refreshed && clock.getAsLong() - refreshedAt < refreshNanos) return;
            }
            refresh();
        } finally {
            refreshLock.unlock();
        }
    }

    /** Serialises refreshes. */
    private final java.util.concurrent.locks.ReentrantLock refreshLock = new java.util.concurrent.locks.ReentrantLock();

    /** The writer a shard is synced from; empty when no tracked writer owns it. */
    public synchronized Optional<WriterRef> owner(ShardId shard) {
        String node = owner.get(shard);
        Tracked t = node == null ? null : writers.get(node);
        return t == null ? Optional.empty() : Optional.of(new WriterRef(t.node, t.url));
    }

    /** True when the shard's owner answered the last refresh. */
    public synchronized boolean ownerLive(ShardId shard) {
        String node = owner.get(shard);
        Tracked t = node == null ? null : writers.get(node);
        return t != null && t.missed == 0;
    }

    /** The {@code LATEST} the shard's owner listed at the last refresh it answered. */
    public synchronized Optional<Long> listedLatest(ShardId shard) {
        String node = owner.get(shard);
        Tracked t = node == null ? null : writers.get(node);
        return t == null ? Optional.empty() : Optional.ofNullable(t.shards.get(shard));
    }

    /** Writers that answered the last refresh, by node id. */
    public synchronized List<WriterRef> liveWriters() {
        return writers.values().stream().filter(t -> t.missed == 0).map(t -> new WriterRef(t.node, t.url)).toList();
    }

    /** Every tracked (live or missing) writer. */
    public synchronized List<WriterStatus> writers() {
        return writers.values().stream().map(t -> new WriterStatus(t.node, t.url.toString(), t.missed == 0,
                t.missed, t.shards.size(), t.error)).toList();
    }

    /** {@code shard X claimed by A and B (syncing from A, LATEST 7)}, one per conflicting shard. */
    public synchronized List<String> conflicts() { return conflicts; }

    /** Why the last refresh found no live writer; null when one answered. */
    public synchronized String lastError() { return lastError; }

    /** Shards listed with no owner (their writer was dropped). */
    public synchronized Set<ShardId> orphans() { return Set.copyOf(orphans); }

    /**
     * Re-reads discovery and every candidate's catalog (in parallel), then updates writers, owners
     * and conflicts. Never throws: failures are recorded per writer.
     */
    public void refresh() {
        refreshLock.lock();
        try {
            refreshNow();
        } finally {
            refreshLock.unlock();
        }
    }

    private void refreshNow() {
        Set<URI> candidates;
        String discoveryError = null;
        try {
            candidates = discovery.candidates();
        } catch (IOException | RuntimeException e) {
            candidates = Set.of();
            discoveryError = "discovery '" + discovery.spec().text() + "' failed: " + e.getMessage();
            log.warn("Peer discovery failed: {}", e.toString());
        }
        Map<URI, CompletableFuture<Answer>> calls = new LinkedHashMap<>();
        for (URI u : candidates) calls.put(u, CompletableFuture.supplyAsync(() -> read(u), PeerHttp.VIRTUAL));
        List<Answer> answers = new ArrayList<>();
        Map<URI, String> errors = new HashMap<>();
        calls.forEach((u, f) -> {
            try {
                answers.add(f.join());
            } catch (java.util.concurrent.CompletionException e) {
                Throwable c = e.getCause() == null ? e : e.getCause();
                errors.put(u, c.getMessage() == null ? c.toString() : c.getMessage());
            }
        });
        apply(answers, errors, discoveryError);
    }

    private Answer read(URI url) {
        JsonNode n;
        try {
            n = http.getJson(url, ReplicationEndpoint.PREFIX + "/catalog");
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e.getMessage(), e);
        }
        String node = n == null ? null : n.path("writer").asText(null);
        if (node == null || node.isBlank()) throw new IllegalStateException(url + " sent a catalog without a writer id");
        Map<ShardId, Long> shards = new TreeMap<>(BY_ID);
        for (JsonNode row : n.path("shards")) {
            try {
                long latest = row.path("latest").asLong(-1);
                if (latest > 0) shards.put(new ShardId(row.path("shard").asText()), latest);
            } catch (IllegalArgumentException bad) {
                log.warn("Writer {} ({}) lists an invalid shard id; skipping it", node, url);
            }
        }
        return new Answer(url, node, Map.copyOf(shards));
    }

    private synchronized void apply(List<Answer> answers, Map<URI, String> errors, String discoveryError) {
        refreshed = true;
        refreshedAt = clock.getAsLong();
        // one writer per node id: keep the URL it was first seen at while that still answers
        Map<String, Answer> byNode = new TreeMap<>();
        for (Answer a : answers) {
            Answer prev = byNode.get(a.node());
            if (prev == null || better(a, prev, writers.get(a.node()))) byNode.put(a.node(), a);
        }
        for (Answer a : byNode.values()) {
            Tracked t = writers.computeIfAbsent(a.node(), n -> {
                log.info("Peer writer {} ({}) joined, listing {} shard(s)", n, a.url(), a.shards().size());
                return new Tracked(n, a.url());
            });
            if (t.missed > 0) log.info("Peer writer {} ({}) is back", t.node, a.url());
            t.url = a.url();
            t.shards = a.shards();
            t.missed = 0;
            t.error = null;
        }
        for (Tracked t : List.copyOf(writers.values())) {
            if (byNode.containsKey(t.node)) continue;
            t.missed++;
            String err = errors.get(t.url);
            t.error = err != null ? err : discoveryError != null ? discoveryError : "no longer discovered";
            if (t.missed >= DROP_AFTER) {
                writers.remove(t.node);
                log.warn("Peer writer {} ({}) dropped after {} missed refreshes ({}); its shards keep serving their"
                        + " cached generations", t.node, t.url, t.missed, t.error);
            } else {
                log.warn("Peer writer {} ({}) missed a refresh ({} of {}): {}", t.node, t.url, t.missed, DROP_AFTER,
                        t.error);
            }
        }
        if (!byNode.isEmpty()) everAnswered = true;
        lastError = !byNode.isEmpty() ? null : discoveryError != null ? discoveryError
                : errors.isEmpty() ? "no writer discovered" : String.join("; ", errors.entrySet().stream()
                        .map(e -> e.getKey() + ": " + e.getValue()).sorted().toList());

        // Claims of every tracked writer: a missing one (not yet dropped) keeps its last-known claim and
        // LATEST, so one missed refresh never hands its shards to a lower claimer (C1).
        Map<ShardId, List<Tracked>> claims = new TreeMap<>(BY_ID);
        for (Tracked t : writers.values()) {
            for (ShardId s : t.shards.keySet()) claims.computeIfAbsent(s, k -> new ArrayList<>()).add(t);
        }
        List<String> newConflicts = new ArrayList<>();
        for (Map.Entry<ShardId, List<Tracked>> e : claims.entrySet()) {
            ShardId shard = e.getKey();
            List<Tracked> by = e.getValue();
            String current = owner.get(shard);
            Tracked currentOwner = current == null ? null : writers.get(current);
            Tracked win;
            if (currentOwner != null && currentOwner.missed > 0 && by.contains(currentOwner)) {
                win = currentOwner; // never moves away from a missing, not yet dropped owner
            } else {
                win = winner(shard, by);
            }
            if (!win.node.equals(current)) {
                if (current != null) log.info("Shard {}: now synced from writer {} (was {})", shard, win.node, current);
            }
            owner.put(shard, win.node);
            orphans.remove(shard);
            released.remove(shard);
            if (by.size() > 1) {
                List<String> names = by.stream().map(t -> t.node).sorted().toList();
                String msg = "shard " + shard + " claimed by " + SynonymsMerge.joinNames(names) + " (syncing from "
                        + win.node + ", LATEST " + win.shards.get(shard) + ")";
                newConflicts.add(msg);
                if (!conflicts.contains(msg)) log.warn("Conflicting shard claims: {}", msg);
            }
        }
        for (ShardId shard : List.copyOf(owner.keySet())) {
            if (claims.containsKey(shard)) continue;
            Tracked t = writers.get(owner.get(shard));
            if (t == null) { // the owner was dropped: keep serving the cached generation
                owner.remove(shard);
                orphans.add(shard);
            } else if (t.missed == 0) { // the owner is live and no longer lists it
                owner.remove(shard);
                released.add(shard);
                log.info("Shard {} is no longer listed by its writer {}", shard, t.node);
            } // else: the owner is missing; it keeps the shard until it is dropped
        }
        conflicts = List.copyOf(newConflicts);
    }

    /**
     * The claimer with the highest {@code LATEST}; on a tie the lowest node id (P5-R9): a pure
     * function of the claims, so readers with different histories agree and it never alternates.
     */
    static Tracked winner(ShardId shard, List<Tracked> by) {
        Tracked win = null;
        for (Tracked t : by) {
            if (win == null || t.shards.get(shard) > win.shards.get(shard)
                    || (t.shards.get(shard).equals(win.shards.get(shard)) && t.node.compareTo(win.node) < 0)) {
                win = t;
            }
        }
        return win;
    }

    /** Of two URLs answering for one node: the one it is tracked at, else the lower URL. */
    private static boolean better(Answer a, Answer prev, Tracked tracked) {
        if (tracked != null && tracked.url.equals(prev.url())) return false;
        if (tracked != null && tracked.url.equals(a.url())) return true;
        return a.url().toString().compareTo(prev.url().toString()) < 0;
    }

    private Set<ShardId> heldNow() {
        try {
            return held.get();
        } catch (RuntimeException e) {
            log.debug("reading the held shards failed: {}", e.toString());
            return Set.of();
        }
    }
}
