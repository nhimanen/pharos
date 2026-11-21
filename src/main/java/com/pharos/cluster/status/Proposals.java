package com.pharos.cluster.status;

import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.status.StatusSnapshot.ReaderObs;
import com.pharos.cluster.status.StatusSnapshot.ShardObs;
import com.pharos.cluster.status.StatusSnapshot.WriterObs;
import com.pharos.cluster.writer.WriterHeartbeat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The proposal rules (spec table) and their hysteresis, as pure functions of a snapshot, the
 * previous {@link StatusState} and the settings. Pharos applies none of them.
 *
 * <table>
 *   <tr><th>kind</th><th>target</th><th>condition</th><th>window</th></tr>
 *   <tr><td>{@value #ADD_READERS}</td><td>{@code readers}</td><td>warm p95 (the highest of the
 *       readers that report one) or QPS per reader above target</td><td>5 min</td></tr>
 *   <tr><td>{@value #REMOVE_READERS}</td><td>{@code readers}</td><td>more than one reader, QPS per
 *       reader below 20% of target, warm p95 within target</td><td>30 min</td></tr>
 *   <tr><td>{@value #ADD_WRITERS}</td><td>{@code writers}</td><td>a shard's oldest unpublished
 *       change older than the backlog target, and fewer live writers than workable shards</td><td>0</td></tr>
 *   <tr><td>{@value #REMOVE_WRITERS}</td><td>{@code writers}</td><td>more than one live writer,
 *       none indexing, nothing pending</td><td>30 min</td></tr>
 *   <tr><td>{@value #RESHARD}</td><td>the shard</td><td>its size above 50% of the smallest reader
 *       cache cap</td><td>0</td></tr>
 *   <tr><td>{@value #WRITER_DISK}</td><td>the writer</td><td>the writer reports low disk</td><td>0</td></tr>
 * </table>
 * Readers in state {@code draining} are not counted. Live writers are the work-queue writers that
 * are not stopping.
 *
 * <p><b>Hysteresis</b> ({@link #advance}): a rule's {@code since} is the snapshot time its condition
 * was first seen holding; once {@code asOf - since} reaches the rule's window the proposal is in
 * force. A condition seen false starts {@code falseSince}; seen true again within
 * {@code clearGraceSeconds} it carries on as if never interrupted, otherwise the rule (and its
 * proposal) clears. The grace makes the state robust to nodes racing with slightly different
 * snapshots: one stale observation cannot reset or clear a rule.
 */
public final class Proposals {

    public static final String ADD_READERS = "add-readers";
    public static final String REMOVE_READERS = "remove-readers";
    public static final String ADD_WRITERS = "add-writers";
    public static final String REMOVE_WRITERS = "remove-writers";
    public static final String RESHARD = "reshard";
    public static final String WRITER_DISK = "writer-disk";

    static final String READERS = "readers";
    static final String WRITERS = "writers";

    private Proposals() {}

    /** One rule evaluated on a snapshot: whether its condition holds, and its texts and metrics. */
    public record Candidate(String id, String kind, String target, boolean holds, String reason, String action,
                            Map<String, Object> metrics) {}

    /** The stable proposal id: lower-case hex SHA-1 of {@code kind + target}. */
    public static String id(String kind, String target) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            return HexFormat.of().formatHex(md.digest((kind + target).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    // ── backlog (shared with StatusBuilder) ──────────────────────────────────────────────────────

    /**
     * The live writer that reports a shard: among those listing it in {@code leases}, the one the
     * lease object names, else the newest heartbeat (ties by node).
     */
    static Optional<WriterObs> reporter(StatusSnapshot s, ShardObs shard) {
        WriterObs best = null;
        for (WriterObs w : s.writers()) {
            if (!w.heartbeat().leases().contains(shard.shard())) continue;
            if (shard.lease() != null && !shard.lease().released()
                    && w.heartbeat().node().equals(shard.lease().writer())) {
                return Optional.of(w);
            }
            if (best == null || w.lastModified().isAfter(best.lastModified())) best = w;
        }
        return Optional.ofNullable(best);
    }

    static WriterHeartbeat.Backlog reportedBacklog(StatusSnapshot s, ShardObs shard) {
        return reporter(s, shard).map(w -> w.heartbeat().backlog().get(shard.shard())).orElse(null);
    }

    /**
     * Bucket-time stamp of a reported pending change: the heartbeat's LastModified minus the age the
     * writer measured (a duration, so the writer's clock offset does not matter).
     */
    static long derivedPendingSince(WriterObs w, WriterHeartbeat.Backlog b) {
        long at = w.lastModified().getEpochSecond();
        Long age = b.oldestUnpublishedChangeAgeSeconds();
        return age == null ? at : at - Math.max(0, age);
    }

    /**
     * Since when (epoch s, bucket time) the shard has an unpublished change, or null: when its
     * reporter says pending, the earlier of the stored stamp and the reported one; when it says not
     * pending, null; when no live writer reports it, the stored stamp (it survives a takeover).
     */
    static Long pendingSince(StatusSnapshot s, StatusState state, ShardObs shard) {
        Optional<WriterObs> w = reporter(s, shard);
        WriterHeartbeat.Backlog b = w.map(x -> x.heartbeat().backlog().get(shard.shard())).orElse(null);
        Long stored = state.pendingSince().get(shard.shard());
        if (b == null) return stored;
        if (!b.pending()) return null;
        long derived = derivedPendingSince(w.get(), b);
        return stored == null ? derived : Math.min(stored, derived);
    }

    /** The scale summary of a snapshot. */
    static StatusState.Summary summary(StatusSnapshot s) {
        return new StatusState.Summary(s.shards().size(), s.writers().size(), s.readers().size());
    }

    // ── rules ────────────────────────────────────────────────────────────────────────────────────

    /** Every rule's candidate on {@code s} (single node: none), by id. */
    public static Map<String, Candidate> candidates(StatusSnapshot s, StatusState state, StatusSettings cfg) {
        Map<String, Candidate> out = new TreeMap<>();
        if (!StatusSnapshot.CLUSTER.equals(s.mode())) return out;
        readerRules(s, cfg, out);
        writerRules(s, state, cfg, out);
        reshardRules(s, out);
        for (WriterObs w : s.writers()) {
            if (!w.heartbeat().lowOnDisk()) continue;
            String node = w.heartbeat().node();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("freeBytes", w.heartbeat().disk().freeBytes());
            m.put("totalBytes", w.heartbeat().disk().totalBytes());
            put(out, WRITER_DISK, node, true, "writer " + node + " low on disk",
                    "add writer capacity / free disk on " + node, m);
        }
        return out;
    }

    private static void put(Map<String, Candidate> out, String kind, String target, boolean holds, String reason,
                            String action, Map<String, Object> metrics) {
        String id = id(kind, target);
        out.put(id, new Candidate(id, kind, target, holds, reason, action, new TreeMap<>(metrics)));
    }

    private static double round2(double v) {
        return Math.round(v * 100) / 100.0;
    }

    private static String minutes(long seconds) {
        if (seconds % 60 == 0) return seconds / 60 + " min";
        return seconds + " s";
    }

    private static void readerRules(StatusSnapshot s, StatusSettings cfg, Map<String, Candidate> out) {
        List<ReaderHeartbeat> active = new ArrayList<>();
        for (ReaderObs r : s.readers()) {
            if (!ReaderHeartbeat.DRAINING.equals(r.heartbeat().state())) active.add(r.heartbeat());
        }
        int n = active.size();
        if (n == 0) return;
        double total = 0;
        Long p95 = null;
        for (ReaderHeartbeat h : active) {
            total += h.qps();
            if (h.warmP95Millis() != null) p95 = p95 == null ? h.warmP95Millis() : Math.max(p95, h.warmP95Millis());
        }
        double perReader = total / n;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("readers", n);
        m.put("qpsTotal", round2(total));
        m.put("qpsPerReader", round2(perReader));
        m.put("warmP95Millis", p95);
        m.put("targetQpsPerReader", cfg.qpsPerReader());
        m.put("targetWarmP95Millis", cfg.warmP95Millis());

        boolean qpsHigh = perReader > cfg.qpsPerReader();
        boolean p95High = p95 != null && p95 > cfg.warmP95Millis();
        double ratio = Math.max(perReader / cfg.qpsPerReader(), p95 == null ? 0 : (double) p95 / cfg.warmP95Millis());
        int add = Math.max(1, (int) Math.ceil(n * ratio) - n);
        String why = qpsHigh && p95High ? "QPS per reader and warm p95 above target"
                : qpsHigh ? "QPS per reader above target" : "warm p95 above target";
        put(out, ADD_READERS, READERS, qpsHigh || p95High,
                why + " for " + minutes(cfg.addReadersSeconds()), "Start " + add + " reader" + (add == 1 ? "" : "s"), m);

        int needed = Math.max(1, (int) Math.ceil(total / (cfg.qpsPerReader() * 0.5)));
        int remove = n - needed;
        boolean low = n > 1 && remove >= 1 && perReader < StatusSettings.REMOVE_READERS_LOAD * cfg.qpsPerReader()
                && !p95High;
        put(out, REMOVE_READERS, READERS, low,
                "readers below " + Math.round(StatusSettings.REMOVE_READERS_LOAD * 100) + "% load for "
                        + minutes(cfg.removeReadersSeconds()),
                "Drain and stop " + Math.max(1, remove) + " reader" + (Math.max(1, remove) == 1 ? "" : "s"), m);
    }

    private static void writerRules(StatusSnapshot s, StatusState state, StatusSettings cfg,
                                    Map<String, Candidate> out) {
        List<WriterHeartbeat> live = new ArrayList<>();
        Set<String> pinned = new TreeSet<>();
        for (WriterObs w : s.writers()) {
            if (!w.heartbeat().workQueue() || w.heartbeat().stopping()) continue;
            live.add(w.heartbeat());
            pinned.addAll(w.heartbeat().pinned());
        }
        int workable = 0;
        int pendingOver = 0;
        int pending = 0;
        Long oldest = null;
        long now = s.asOf().getEpochSecond();
        for (ShardObs sh : s.shards()) {
            if (sh.hasSource() || pinned.contains(sh.shard())) workable++;
            Long since = pendingSince(s, state, sh);
            if (since == null) continue;
            pending++;
            long age = Math.max(0, now - since);
            oldest = oldest == null ? age : Math.max(oldest, age);
            if (age > cfg.backlogSeconds()) pendingOver++;
        }
        boolean anyWriterKnown = !s.writers().isEmpty() || workable > 0;
        if (!anyWriterKnown) return;
        int writers = live.size();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("writers", writers);
        m.put("workableShards", workable);
        m.put("shardsPending", pending);
        m.put("shardsPendingOverTarget", pendingOver);
        m.put("oldestPendingAgeSeconds", oldest);
        m.put("targetBacklogSeconds", cfg.backlogSeconds());

        int add = Math.max(1, Math.min(workable - writers, pendingOver));
        put(out, ADD_WRITERS, WRITERS, pendingOver > 0 && writers < workable,
                "indexing backlog above target and fewer writers than shards",
                "Start " + add + " writer" + (add == 1 ? "" : "s") + "; leases rebalance", m);

        boolean idle = writers > 1 && pending == 0 && live.stream().allMatch(h -> h.indexing() == null);
        int remove = Math.max(1, writers - 1);
        put(out, REMOVE_WRITERS, WRITERS, idle, "writers idle for " + minutes(cfg.removeWritersSeconds()),
                "Stop " + remove + " writer" + (remove == 1 ? "" : "s") + "; leases move", m);
    }

    private static void reshardRules(StatusSnapshot s, Map<String, Candidate> out) {
        Long cap = null;
        for (ReaderObs r : s.readers()) {
            Map<String, Object> rc = r.heartbeat().readerCache();
            if (rc == null || !(rc.get("cacheMaxBytes") instanceof Number n) || n.longValue() <= 0) continue;
            cap = cap == null ? n.longValue() : Math.min(cap, n.longValue());
        }
        if (cap == null) return;
        for (ShardObs sh : s.shards()) {
            if (sh.sizeBytes() == null) continue;
            double share = (double) sh.sizeBytes() / cap;
            int parts = 2;
            while (parts < 1024 && (double) sh.sizeBytes() / parts > StatusSettings.RESHARD_CACHE_SHARE * cap) parts *= 2;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("sizeBytes", sh.sizeBytes());
            m.put("readerCacheMaxBytes", cap);
            m.put("shareOfCache", round2(share));
            put(out, RESHARD, sh.shard(), share > StatusSettings.RESHARD_CACHE_SHARE,
                    "shard above " + Math.round(StatusSettings.RESHARD_CACHE_SHARE * 100) + "% of a reader's cache cap",
                    "New shard spec, e.g. [project, hash(path) % " + parts + "]", m);
        }
    }

    // ── hysteresis ───────────────────────────────────────────────────────────────────────────────

    /**
     * The next state after observing {@code s}. Returns {@code prev} itself when {@code s} is older
     * than the state's {@code asOf} (another node already recorded a newer observation), when it is
     * dated by a node clock ({@link StatusSnapshot#nodeClock}), or when nothing worth a write
     * changed; otherwise a new state stamped with {@code s.asOf()}.
     */
    public static StatusState advance(StatusSnapshot s, StatusState prev, StatusSettings cfg) {
        StatusState base = prev == null ? StatusState.EMPTY : prev;
        long now = s.asOf().getEpochSecond();
        if (now < base.asOf() || s.nodeClock()) return base;
        Map<String, Candidate> cands = candidates(s, base, cfg);

        TreeMap<String, StatusState.Rule> rules = new TreeMap<>();
        Set<String> ids = new TreeSet<>(base.rules().keySet());
        ids.addAll(cands.keySet());
        for (String id : ids) {
            Candidate c = cands.get(id);
            StatusState.Rule r = base.rules().get(id);
            if (c != null && c.holds()) {
                long since = r == null ? now : r.since();
                boolean active = now - since >= cfg.window(c.kind());
                rules.put(id, new StatusState.Rule(c.kind(), c.target(), since, null, active,
                        active ? c.reason() : null, active ? c.action() : null));
            } else if (r != null) {
                long falseSince = r.falseSince() == null ? now : r.falseSince();
                if (now - falseSince >= cfg.clearGraceSeconds()) continue; // cleared
                rules.put(id, new StatusState.Rule(r.kind(), r.target(), r.since(), falseSince, r.active(), r.reason(),
                        r.action()));
            }
        }

        TreeMap<String, Long> pending = new TreeMap<>();
        for (ShardObs sh : s.shards()) {
            Long since = pendingSince(s, base, sh);
            if (since != null) pending.put(sh.shard(), since);
        }

        StatusState.Summary layout = summary(s);
        StatusState next = new StatusState(StatusState.VERSION, now, rules, pending, layout);
        return next.sameContent(base) ? base : next;
    }

    /** The proposals in force in {@code state}, with the metrics of {@code cands}, sorted by kind and target. */
    static List<com.pharos.cluster.status.ClusterStatus.Proposal> inForce(StatusState state,
                                                                          Map<String, Candidate> cands) {
        List<com.pharos.cluster.status.ClusterStatus.Proposal> out = new ArrayList<>();
        for (Map.Entry<String, StatusState.Rule> e : state.rules().entrySet()) {
            StatusState.Rule r = e.getValue();
            if (!r.active()) continue;
            Candidate c = cands.get(e.getKey());
            out.add(new com.pharos.cluster.status.ClusterStatus.Proposal(e.getKey(), r.kind(), r.target(), r.reason(),
                    c == null ? new TreeMap<>() : c.metrics(), r.action(), Instant.ofEpochSecond(r.since()).toString()));
        }
        out.sort(java.util.Comparator.comparing(com.pharos.cluster.status.ClusterStatus.Proposal::kind)
                .thenComparing(com.pharos.cluster.status.ClusterStatus.Proposal::target));
        return out;
    }
}
