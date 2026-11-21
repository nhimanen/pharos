package com.pharos.cluster.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.membership.MembershipSource;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.membership.ReaderRegistry;
import com.pharos.config.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Reader membership without a bucket: the peer-pull twin of {@link ReaderRegistry}.
 *
 * <p>Every {@code heartbeatSeconds} a member POSTs its {@link ReaderHeartbeat} (the bucket JSON,
 * unchanged) to every live writer of its {@link PeerCatalog}, then lists
 * {@code GET /replication/readers} from each of them and merges the answers: a reader any answering
 * writer holds is live, with its newest heartbeat (the one a writer received last, by the age that
 * writer reports). Each writer judges liveness by its own clock with {@code readerTtlSeconds}
 * ({@link PeerReaderRegistry}), so node clocks are never compared. An observer ({@code pharos router})
 * only lists.
 *
 * <p>As with the bucket: when no POST of a member succeeded, or no writer answered the listing, the
 * previous list stays (it cannot tell "readers are gone" from "the writers are unreachable"), with a
 * WARN at most once a minute. {@link #drain} POSTs the draining heartbeat at once;
 * {@link #close} withdraws this process from every live writer ({@code DELETE}, which tombstones it
 * there), so coordinators drop it on their next tick.
 */
public final class PeerMembership implements MembershipSource {

    private static final Logger log = LoggerFactory.getLogger(PeerMembership.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long WARN_EVERY_NANOS = Duration.ofSeconds(60).toNanos();

    /** One reader as one writer reported it. */
    public record Seen(ReaderHeartbeat heartbeat, long ageMillis) {}

    /**
     * One writer's {@code GET /replication/readers}: its clock ({@code nowMillis}, epoch millis),
     * when the answer arrived here ({@code answeredNanos}, this node's monotonic clock) and its readers.
     */
    public record WriterReaders(PeerCatalog.WriterRef writer, long nowMillis, long answeredNanos, long uptimeMillis,
                                List<Seen> readers, Map<String, Long> withdrawn) {
        public WriterReaders {
            readers = List.copyOf(readers);
            withdrawn = withdrawn == null ? Map.of() : Map.copyOf(withdrawn);
        }
    }

    /** A merged reader: its newest heartbeat and when that arrived at its writer, on this node's monotonic clock. */
    public record Merged(ReaderHeartbeat heartbeat, long receivedNanos) {}

    private final PeerCatalog catalog;
    private final PeerHttp http;
    private final Supplier<ReaderHeartbeat> self;
    private final int heartbeatSeconds;
    private final long ttlNanos;
    /** The last merged list, for {@link #carryOver}. Under mu. */
    private Map<String, Merged> previous = Map.of();
    private final Duration timeout;
    private final LongSupplier nanoClock;
    private final long startedAt = System.currentTimeMillis();
    private final Object mu = new Object();
    private volatile List<ReaderRegistry.LiveReader> live = List.of();
    private volatile List<WriterReaders> lastAnswers = List.of();
    private boolean draining;   // under mu
    private boolean closed;     // under mu
    private long lastWarn;
    private ScheduledExecutorService scheduler;

    /**
     * @param self this reader's heartbeat; null: an observer that never POSTs
     */
    public PeerMembership(PeerCatalog catalog, PeerHttp http, ClusterConfig cfg, Supplier<ReaderHeartbeat> self,
                          LongSupplier nanoClock) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.http = Objects.requireNonNull(http, "http");
        this.self = self;
        this.heartbeatSeconds = cfg.getHeartbeatSeconds();
        this.ttlNanos = TimeUnit.SECONDS.toNanos(cfg.getReaderTtlSeconds());
        this.timeout = callTimeout(cfg.getHeartbeatSeconds());
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.lastWarn = nanoClock.getAsLong() - WARN_EVERY_NANOS;
    }

    /** A member: heartbeats as {@code self}. */
    public PeerMembership(PeerCatalog catalog, PeerHttp http, ClusterConfig cfg, Supplier<ReaderHeartbeat> self) {
        this(catalog, http, cfg, Objects.requireNonNull(self, "self"), System::nanoTime);
    }

    /** A read-only view ({@code pharos router}): lists every {@code heartbeatSeconds}, never POSTs. */
    public static PeerMembership observer(PeerCatalog catalog, PeerHttp http, ClusterConfig cfg) {
        return new PeerMembership(catalog, http, cfg, null, System::nanoTime);
    }

    /** One membership call's bound: within one heartbeat, at most {@link PeerHttp#CONTROL_TIMEOUT}. */
    public static Duration callTimeout(int heartbeatSeconds) {
        Duration hb = Duration.ofSeconds(Math.max(1, heartbeatSeconds));
        return hb.compareTo(PeerHttp.CONTROL_TIMEOUT) < 0 ? hb : PeerHttp.CONTROL_TIMEOUT;
    }

    /** The generations this reader holds in full, {@code {shard: generation}}; none by default. */
    private volatile Supplier<Map<String, Long>> generationsHeld = Map::of;

    /**
     * Where the heartbeat's {@code generationsHeld} comes from ({@link PeerFileSource#generationsHeld}):
     * writers list this reader as a peer for those generations.
     */
    public void setGenerationsHeld(Supplier<Map<String, Long>> held) {
        this.generationsHeld = Objects.requireNonNull(held, "held");
    }

    /** The catalog whose live writers this membership heartbeats to. */
    public PeerCatalog catalog() { return catalog; }

    @Override
    public void start() {
        synchronized (mu) {
            if (scheduler != null || closed) return;
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-reader-heartbeat");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(this::tickQuietly, 0, heartbeatSeconds, TimeUnit.SECONDS);
        }
    }

    private void tickQuietly() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.warn("Reader membership tick failed (retrying next tick): {}", e.toString());
        }
    }

    /** One heartbeat: refresh the catalog when due, POST to every live writer, then merge their lists. */
    public void tick() {
        catalog.refreshIfDue(); // outside mu: a hung discovery must not block drain() or close()
        synchronized (mu) {
            if (closed) return;
            List<PeerCatalog.WriterRef> writers = catalog.liveWriters();
            if (self != null && post(writers) == 0) {
                warnStale(writers.isEmpty() ? "no live writer to heartbeat to"
                        : "no writer accepted our heartbeat");
                return;
            }
            refresh(writers);
        }
    }

    /** Marks this reader draining and POSTs that to every live writer at once. */
    @Override
    public void drain() {
        synchronized (mu) {
            if (closed || self == null) return;
            draining = true;
            List<PeerCatalog.WriterRef> writers = catalog.liveWriters();
            post(writers);
            refresh(writers);
        }
    }

    @Override
    public List<ReaderRegistry.LiveReader> liveWithAge() {
        return live;
    }

    /** The writers' answers of the last listing (empty before one). */
    public List<WriterReaders> lastAnswers() {
        return lastAnswers;
    }

    // Holding mu. The number of writers that accepted the heartbeat.
    private int post(List<PeerCatalog.WriterRef> writers) {
        if (writers.isEmpty()) return 0;
        byte[] body;
        try {
            ReaderHeartbeat hb = self.get();
            com.fasterxml.jackson.databind.node.ObjectNode json =
                    MAPPER.valueToTree(hb.with(draining || hb.draining(), startedAt));
            Map<String, Long> held = generationsHeld.get();
            if (held != null && !held.isEmpty()) { // P5-R5: optional; older readers omit it
                json.set(PeerReaderRegistry.GENERATIONS_HELD, MAPPER.valueToTree(new TreeMap<>(held)));
            }
            body = MAPPER.writeValueAsBytes(json);
        } catch (IOException | RuntimeException e) {
            log.warn("Building the reader heartbeat failed (retrying next tick): {}", e.toString());
            return 0;
        }
        Map<PeerCatalog.WriterRef, CompletableFuture<HttpResponse<byte[]>>> calls = new LinkedHashMap<>();
        for (PeerCatalog.WriterRef w : writers) {
            HttpRequest req = http.request(w.url(), MembershipRoutes.READERS, timeout)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .header("Content-Type", "application/json").build();
            calls.put(w, http.sendAsync(req));
        }
        int ok = 0;
        for (Map.Entry<PeerCatalog.WriterRef, CompletableFuture<HttpResponse<byte[]>>> e : calls.entrySet()) {
            try {
                HttpResponse<byte[]> r = e.getValue().get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
                if (r.statusCode() == 204) {
                    ok++;
                } else {
                    log.debug("Heartbeat to writer {} answered {}", e.getKey(), PeerHttp.describe(r.statusCode(), r.body()));
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                e.getValue().cancel(true);
                log.debug("Heartbeat to writer {} failed: {}", e.getKey(), ex.toString());
            }
        }
        return ok;
    }

    // Holding mu.
    private void refresh(List<PeerCatalog.WriterRef> writers) {
        List<WriterReaders> answers = fetch(http, writers, timeout, nanoClock);
        if (answers.isEmpty()) {
            warnStale(writers.isEmpty() ? "no live writer" : "no writer answered GET /replication/readers");
            return;
        }
        lastAnswers = answers;
        long now = nanoClock.getAsLong();
        Map<String, Merged> merged = carryOver(merge(answers), answers, previous, now, ttlNanos);
        previous = merged;
        List<ReaderRegistry.LiveReader> out = new ArrayList<>();
        for (Merged m : merged.values()) {
            out.add(new ReaderRegistry.LiveReader(m.heartbeat(), Math.max(0, (now - m.receivedNanos()) / 1_000_000_000L)));
        }
        live = List.copyOf(out);
    }

    private void warnStale(String why) {
        long now = nanoClock.getAsLong();
        if (now - lastWarn >= WARN_EVERY_NANOS) {
            lastWarn = now;
            log.warn("Keeping the previous live-reader list: {}", why);
        }
    }

    /**
     * {@code GET /replication/readers} from every writer in parallel, each bounded by {@code timeout};
     * the writers that answered (a failure is logged at DEBUG and left out).
     */
    public static List<WriterReaders> fetch(PeerHttp http, List<PeerCatalog.WriterRef> writers, Duration timeout,
                                            LongSupplier nanoClock) {
        Map<PeerCatalog.WriterRef, CompletableFuture<HttpResponse<byte[]>>> calls = new LinkedHashMap<>();
        for (PeerCatalog.WriterRef w : writers) {
            calls.put(w, http.sendAsync(http.request(w.url(), MembershipRoutes.READERS, timeout).build()));
        }
        List<WriterReaders> out = new ArrayList<>();
        for (Map.Entry<PeerCatalog.WriterRef, CompletableFuture<HttpResponse<byte[]>>> e : calls.entrySet()) {
            PeerCatalog.WriterRef w = e.getKey();
            try {
                HttpResponse<byte[]> r = e.getValue().get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
                long answered = nanoClock.getAsLong();
                if (r.statusCode() != 200) {
                    log.debug("Readers of writer {} answered {}", w, PeerHttp.describe(r.statusCode(), r.body()));
                    continue;
                }
                JsonNode n = MAPPER.readTree(r.body());
                long now = n.path("now").asLong(-1);
                if (now <= 0) {
                    log.debug("Readers of writer {}: no clock in the answer", w);
                    continue;
                }
                List<Seen> seen = new ArrayList<>();
                for (JsonNode row : n.path("readers")) {
                    try {
                        ReaderHeartbeat hb = MAPPER.treeToValue(row.path("heartbeat"), ReaderHeartbeat.class);
                        long age = row.path("ageMillis").asLong(-1);
                        if (hb == null || hb.node() == null || age < 0) continue;
                        seen.add(new Seen(hb, age));
                    } catch (IOException | RuntimeException bad) {
                        log.debug("Readers of writer {}: skipping an unreadable heartbeat: {}", w, bad.toString());
                    }
                }
                Map<String, Long> withdrawn = new TreeMap<>();
                for (JsonNode row : n.path("withdrawn")) {
                    String node = row.path("node").asText(null);
                    if (node != null && row.path("startedAt").canConvertToLong()) {
                        withdrawn.merge(node, row.path("startedAt").asLong(), Math::max);
                    }
                }
                out.add(new WriterReaders(w, now, answered, n.path("uptimeMillis").asLong(-1), seen, withdrawn));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                e.getValue().cancel(true);
                log.debug("Listing the readers of writer {} failed: {}", w, ex.toString());
            }
        }
        return out;
    }

    /**
     * The readers of {@code answers} by node id: each with its newest heartbeat across the writers
     * (the latest arrival, {@code answeredNanos - ageMillis}; ties to the lower writer id).
     */
    public static Map<String, Merged> merge(List<WriterReaders> answers) {
        List<WriterReaders> sorted = new ArrayList<>(answers);
        sorted.sort(Comparator.comparing(a -> a.writer().node()));
        Map<String, Merged> out = new TreeMap<>();
        for (WriterReaders a : sorted) {
            for (Seen s : a.readers()) {
                long received = a.answeredNanos() - s.ageMillis() * 1_000_000L;
                Merged prev = out.get(s.heartbeat().node());
                if (prev == null || received - prev.receivedNanos() > 0) {
                    out.put(s.heartbeat().node(), new Merged(s.heartbeat(), received));
                }
            }
        }
        return out;
    }

    /**
     * {@code merged}, plus the readers of {@code previous} it lacks whose last heartbeat is still within
     * the window, when every answering writer's registry is younger than the window: writers that just
     * restarted may not have heard from every live reader yet, and dropping those for a tick would leave
     * their shards unroutable. Once any answering writer is older than the window, its list is
     * authoritative and nothing is carried over; a process any writer reports withdrawn never is.
     */
    static Map<String, Merged> carryOver(Map<String, Merged> merged, List<WriterReaders> answers,
                                         Map<String, Merged> previous, long nowNanos, long ttlNanos) {
        long ttlMillis = TimeUnit.NANOSECONDS.toMillis(ttlNanos);
        boolean allYoung = !answers.isEmpty()
                && answers.stream().allMatch(a -> a.uptimeMillis() >= 0 && a.uptimeMillis() < ttlMillis);
        if (!allYoung || previous.isEmpty()) return merged;
        Map<String, Merged> out = new TreeMap<>(merged);
        Map<String, Long> withdrawn = new java.util.HashMap<>();
        for (WriterReaders a : answers) a.withdrawn().forEach((node, started) -> withdrawn.merge(node, started, Math::max));
        previous.forEach((node, m) -> {
            Long gone = withdrawn.get(node);
            if (gone != null && m.heartbeat().startedAt() <= gone) return;
            if (!out.containsKey(node) && nowNanos - m.receivedNanos() <= ttlNanos) out.put(node, m);
        });
        return out;
    }

    /** Stops heartbeating and withdraws this process from every live writer. */
    @Override
    public void close() {
        ScheduledExecutorService s;
        synchronized (mu) {
            if (closed) return;
            closed = true;
            s = scheduler;
            scheduler = null;
        }
        if (s != null) {
            s.shutdownNow();
            try {
                s.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        synchronized (mu) {
            live = List.of();
            if (self == null) return;
            String node;
            try {
                node = self.get().node();
            } catch (RuntimeException e) {
                log.warn("Withdrawing from reader membership failed (writers drop us after the TTL): {}", e.toString());
                return;
            }
            String path = MembershipRoutes.READERS + "/" + URLEncoder.encode(node, StandardCharsets.UTF_8)
                    .replace("+", "%20") + "?startedAt=" + startedAt;
            List<CompletableFuture<HttpResponse<byte[]>>> calls = new ArrayList<>();
            for (PeerCatalog.WriterRef w : catalog.liveWriters()) {
                calls.add(http.sendAsync(http.request(w.url(), path, timeout).DELETE().build()));
            }
            for (CompletableFuture<HttpResponse<byte[]>> c : calls) {
                try {
                    c.get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    c.cancel(true);
                    log.debug("Withdrawing from a writer failed (it drops us after the TTL): {}", e.toString());
                }
            }
        }
    }
}
