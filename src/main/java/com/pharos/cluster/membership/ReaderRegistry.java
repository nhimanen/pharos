package com.pharos.cluster.membership;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.config.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Membership of the reader tier: this node's heartbeat object in the bucket and the list of live
 * readers.
 *
 * <p>Liveness uses the bucket's own clock, never a node's: an entry is live when its LastModified
 * is within {@code readerTtlSeconds} of the newest LastModified in the listing (this node's own
 * heartbeat, written a moment earlier, is in the listing, so that is the bucket's "now"). Clock
 * skew between nodes therefore cannot expire or resurrect a reader.
 *
 * <p>A failed write or listing logs a WARN and is retried on the next tick; it never stops the
 * reader.
 */
public final class ReaderRegistry implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReaderRegistry.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A live reader and its age by the bucket's clock (own heartbeat's LastModified is "now"). */
    public record LiveReader(ReaderHeartbeat heartbeat, long ageSeconds) {}

    private static final long WARN_EVERY_NANOS = Duration.ofSeconds(60).toNanos();

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final ClusterConfig cfg;
    private final Supplier<ReaderHeartbeat> self;
    private final String key;
    private final long startedAt = System.currentTimeMillis();
    /** Guards writes, refreshes and close: after close() nothing writes our object again. */
    private final Object mu = new Object();
    private volatile List<LiveReader> live = List.of();
    private volatile boolean lastWriteOk;
    private boolean draining;      // under mu
    private boolean closed;        // under mu
    private long lastWarn = System.nanoTime() - WARN_EVERY_NANOS;
    private ScheduledExecutorService scheduler;

    public ReaderRegistry(ObjectStore store, ObjectStoreLayout layout, ClusterConfig cfg,
                          Supplier<ReaderHeartbeat> self) {
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.self = Objects.requireNonNull(self, "self");
        this.key = layout.readerKey(cfg.effectiveNodeId());
    }

    /** Beats now and then every {@code heartbeatSeconds}, all on a daemon thread (never blocks the caller). */
    public void start() {
        synchronized (mu) {
            if (scheduler != null || closed) return;
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-reader-heartbeat");
                t.setDaemon(true);
                return t;
            });
            long every = cfg.getHeartbeatSeconds();
            scheduler.scheduleWithFixedDelay(this::tick, 0, every, TimeUnit.SECONDS);
        }
    }

    /** One heartbeat: write our object, then refresh the live list. Never throws. */
    void tick() {
        synchronized (mu) {
            if (closed) return;
            write();
            refresh();
        }
    }

    /** Marks this reader draining and publishes that at once. */
    public void drain() {
        synchronized (mu) {
            if (closed) return;
            draining = true;
            write();
            refresh();
        }
    }

    /** The live readers as of the last refresh (this node's own entry included). */
    public List<ReaderHeartbeat> live() {
        return live.stream().map(LiveReader::heartbeat).toList();
    }

    /** Like {@link #live()}, with each reader's age in seconds. */
    public List<LiveReader> liveWithAge() {
        return live;
    }

    // Holding mu.
    private void write() {
        if (closed) return;
        try {
            ReaderHeartbeat hb = self.get();
            store.put(key, MAPPER.writeValueAsBytes(hb.with(draining || hb.draining(), startedAt)));
            lastWriteOk = true;
        } catch (IOException | RuntimeException e) {
            lastWriteOk = false;
            log.warn("Writing the reader heartbeat {} failed (retrying next tick): {}", key, e.toString());
        }
    }

    private void warnStale(String why) {
        long now = System.nanoTime();
        if (now - lastWarn >= WARN_EVERY_NANOS) {
            lastWarn = now;
            log.warn("Keeping the previous live-reader list: {}", why);
        }
    }

    // Holding mu. Liveness is measured against this node's own heartbeat object (the bucket's clock),
    // and only when our latest write succeeded: otherwise we cannot tell "peers are dead" from
    // "the bucket is unreachable", so the previous list stays.
    void refresh() {
        if (!lastWriteOk) {
            warnStale("our own heartbeat write failed");
            return;
        }
        try {
            List<ObjectInfo> infos = store.list(layout.readersPrefix()).stream()
                    .filter(i -> i.key().endsWith(".json") && i.lastModified() != null).toList();
            Instant now = infos.stream().filter(i -> i.key().equals(key)).map(ObjectInfo::lastModified)
                    .findFirst().orElse(null);
            if (now == null) {
                warnStale("our own heartbeat is missing from the listing");
                return;
            }
            Duration ttl = Duration.ofSeconds(cfg.getReaderTtlSeconds());
            List<LiveReader> out = new ArrayList<>();
            for (ObjectInfo info : infos) {
                Duration age = Duration.between(info.lastModified(), now);
                if (age.compareTo(ttl) > 0) continue;
                try {
                    store.get(info.key()).ifPresent(o -> {
                        try {
                            out.add(new LiveReader(MAPPER.readValue(o.data(), ReaderHeartbeat.class),
                                    Math.max(0, age.toSeconds())));
                        } catch (IOException e) {
                            log.warn("Ignoring unreadable reader heartbeat {}: {}", info.key(), e.toString());
                        }
                    });
                } catch (IOException e) {
                    log.warn("Reading the reader heartbeat {} failed: {}", info.key(), e.toString());
                }
            }
            out.sort(Comparator.comparing(l -> l.heartbeat().node()));
            live = List.copyOf(out);
        } catch (IOException | RuntimeException e) {
            log.warn("Listing the live readers failed (keeping the previous list): {}", e.toString());
        }
    }

    /** Stops the heartbeat and removes our object so peers drop this reader at once. */
    @Override
    public void close() {
        ScheduledExecutorService s;
        synchronized (mu) {
            if (closed) return;
            closed = true; // an in-flight tick finished first (it holds mu); later ones return at once
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
            try {
                store.delete(key);
            } catch (IOException | RuntimeException e) {
                log.warn("Removing the reader heartbeat {} failed (it expires after the TTL): {}", key, e.toString());
            }
            live = List.of();
        }
    }
}
