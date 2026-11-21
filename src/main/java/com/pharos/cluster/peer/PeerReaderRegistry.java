package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.cluster.membership.ReaderHeartbeat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * A peer-mode writer's reader registry, fed by {@code POST /replication/readers} (the reader's
 * {@link ReaderHeartbeat} JSON, unchanged) and read by {@code GET /replication/readers}: what
 * {@code catalog/readers/} is in the bucket.
 *
 * <p><b>Liveness</b> is judged by this writer's own clock (monotonic), with the bucket registry's
 * window: a reader whose last POST is more than {@code readerTtlSeconds} old, in whole seconds as
 * {@code BucketClock.expired} counts them, is dropped. Readers on other hosts never compare clocks
 * with it: their own ages are never sent.
 *
 * <p><b>Withdrawal.</b> {@code DELETE /replication/readers/<node>?startedAt=…} (a reader closing)
 * removes the entry and leaves a tombstone for that process ({@code startedAt}, the reader process
 * start it sends in every heartbeat), so a heartbeat of the same process still in flight cannot bring
 * it back; a restarted reader (a newer {@code startedAt}) is accepted at once. Tombstones expire
 * after one window.
 *
 * <p>The heartbeat is kept as received (a JSON object) and served back verbatim, so fields a newer
 * reader adds (such as {@code generationsHeld}) reach coordinators through an older writer.
 * At most {@value #MAX_READERS} readers are held; a new one beyond that is refused.
 */
public final class PeerReaderRegistry {

    /** The most readers one writer holds. */
    public static final int MAX_READERS = 4096;
    /** The largest heartbeat body accepted. */
    public static final int MAX_BODY_BYTES = 64 * 1024;
    /** A node id: any printable characters, not blank, at most 256 of them ({@code cluster.nodeId} is free-form). */
    private static final Pattern NODE = Pattern.compile("(?=.*\\S)[^\\p{Cntrl}]{1,256}");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** The outcome of a POST. */
    public enum Result { ACCEPTED, WITHDRAWN, FULL }

    /** One live reader: its heartbeat as received, parsed, and its age by this writer's clock. */
    public record Entry(ObjectNode raw, ReaderHeartbeat heartbeat, long ageMillis) {}

    private record Held(ObjectNode raw, ReaderHeartbeat heartbeat, long receivedNanos) {}

    private record Tombstone(long startedAt, long atNanos) {}

    private final long ttlSeconds;
    private final LongSupplier nanoClock;
    private final long createdNanos;
    // guarded by this
    private final Map<String, Held> readers = new HashMap<>();
    private final Map<String, Tombstone> tombstones = new HashMap<>();

    public PeerReaderRegistry(Duration readerTtl, LongSupplier nanoClock) {
        this.ttlSeconds = Objects.requireNonNull(readerTtl, "readerTtl").toSeconds();
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.createdNanos = nanoClock.getAsLong();
        if (ttlSeconds <= 0) throw new IllegalArgumentException("readerTtl must be at least one second");
    }

    /** The liveness window ({@code cluster.readerTtlSeconds}). */
    public long ttlSeconds() { return ttlSeconds; }

    /**
     * How long this registry has been receiving heartbeats. Younger than one window, it may not yet
     * have heard from every live reader (a writer that just restarted).
     */
    public long uptimeMillis() {
        return Math.max(0, (nanoClock.getAsLong() - createdNanos) / 1_000_000);
    }

    /**
     * Parses and validates a heartbeat body.
     *
     * @throws IllegalArgumentException naming what is wrong (never echoing the body)
     */
    public static ObjectNode parse(byte[] body) {
        if (body == null || body.length == 0) throw new IllegalArgumentException("empty heartbeat");
        if (body.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("heartbeat larger than " + MAX_BODY_BYTES + " bytes");
        }
        JsonNode n;
        try {
            n = MAPPER.readTree(body);
        } catch (IOException e) {
            throw new IllegalArgumentException("heartbeat is not JSON");
        }
        if (!(n instanceof ObjectNode o)) throw new IllegalArgumentException("heartbeat is not a JSON object");
        ReaderHeartbeat hb = toHeartbeat(o);
        if (hb.node() == null || !NODE.matcher(hb.node()).matches()) {
            throw new IllegalArgumentException("heartbeat has no valid node id");
        }
        return o;
    }

    private static ReaderHeartbeat toHeartbeat(ObjectNode o) {
        try {
            return MAPPER.treeToValue(o, ReaderHeartbeat.class);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("heartbeat does not match the reader heartbeat format");
        }
    }

    /** True when {@code node} is a node id this registry accepts. */
    public static boolean validNode(String node) {
        return node != null && NODE.matcher(node).matches();
    }

    /** Records a heartbeat {@link #parse}d from a POST, now by this writer's clock. */
    public synchronized Result accept(ObjectNode raw) {
        ReaderHeartbeat hb = toHeartbeat(raw);
        long now = nanoClock.getAsLong();
        prune(now);
        Tombstone t = tombstones.get(hb.node());
        if (t != null) {
            if (hb.startedAt() <= t.startedAt()) return Result.WITHDRAWN; // the closed process, delayed
            tombstones.remove(hb.node()); // a new process of that node
        }
        if (!readers.containsKey(hb.node()) && readers.size() >= MAX_READERS) return Result.FULL;
        readers.put(hb.node(), new Held(raw.deepCopy(), hb, now));
        return Result.ACCEPTED;
    }

    /**
     * Withdraws {@code node}'s process {@code startedAt}: its entry goes (unless a newer process of the
     * node already beat here) and later heartbeats of that process are ignored for one window.
     */
    public synchronized void withdraw(String node, long startedAt) {
        long now = nanoClock.getAsLong();
        prune(now);
        Held h = readers.get(node);
        if (h != null && h.heartbeat().startedAt() > startedAt) return; // a newer process: not this one's
        readers.remove(node);
        Tombstone prev = tombstones.get(node);
        if (prev == null || prev.startedAt() < startedAt) {
            if (prev == null && tombstones.size() >= MAX_READERS) return; // bounded; the entry is gone anyway
            tombstones.put(node, new Tombstone(startedAt, now));
        }
    }

    /** The live readers, sorted by node id, with their ages by this writer's clock. */
    public synchronized List<Entry> live() {
        long now = nanoClock.getAsLong();
        prune(now);
        List<Entry> out = new ArrayList<>(readers.size());
        for (Held h : readers.values()) {
            out.add(new Entry(h.raw().deepCopy(), h.heartbeat(), Math.max(0, (now - h.receivedNanos()) / 1_000_000)));
        }
        out.sort(Comparator.comparing(e -> e.heartbeat().node()));
        return out;
    }

    /** The withdrawn reader processes still tombstoned: node id → the {@code startedAt} withdrawn. */
    public synchronized Map<String, Long> withdrawn() {
        prune(nanoClock.getAsLong());
        Map<String, Long> out = new java.util.TreeMap<>();
        tombstones.forEach((node, t) -> out.put(node, t.startedAt()));
        return out;
    }

    // Holding this. Expired: older than the window in whole seconds (as BucketClock.expired).
    private void prune(long now) {
        for (Iterator<Held> it = readers.values().iterator(); it.hasNext(); ) {
            if (expired(now - it.next().receivedNanos())) it.remove();
        }
        tombstones.values().removeIf(t -> expired(now - t.atNanos()));
    }

    private boolean expired(long ageNanos) {
        return ageNanos / 1_000_000_000L > ttlSeconds;
    }
}
