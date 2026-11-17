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

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final ClusterConfig cfg;
    private final Supplier<ReaderHeartbeat> self;
    private final String key;
    private volatile List<ReaderHeartbeat> live = List.of();
    private volatile boolean draining;
    private volatile boolean closed;
    private ScheduledExecutorService scheduler;

    public ReaderRegistry(ObjectStore store, ObjectStoreLayout layout, ClusterConfig cfg,
                          Supplier<ReaderHeartbeat> self) {
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.self = Objects.requireNonNull(self, "self");
        this.key = layout.readerKey(cfg.effectiveNodeId());
    }

    /** Writes now (on the caller's thread), then every {@code heartbeatSeconds} on a daemon thread. */
    public synchronized void start() {
        if (scheduler != null || closed) return;
        tick();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "pharos-reader-heartbeat");
            t.setDaemon(true);
            return t;
        });
        long every = cfg.getHeartbeatSeconds();
        scheduler.scheduleWithFixedDelay(this::tick, every, every, TimeUnit.SECONDS);
    }

    /** One heartbeat: write our object, then refresh the live list. Never throws. */
    void tick() {
        if (closed) return;
        write();
        refresh();
    }

    /** Marks this reader draining and publishes that at once. */
    public void drain() {
        draining = true;
        write();
        refresh();
    }

    /** The live readers as of the last refresh (this node's own entry included). */
    public List<ReaderHeartbeat> live() {
        return live;
    }

    private void write() {
        try {
            ReaderHeartbeat hb = self.get();
            if (draining && !hb.draining()) {
                hb = new ReaderHeartbeat(hb.node(), hb.advertiseUrl(), hb.shards(), true, hb.readerCache(),
                        hb.inFlight(), hb.qps(), 0);
            }
            store.put(key, MAPPER.writeValueAsBytes(hb.withAge(0)));
        } catch (IOException | RuntimeException e) {
            log.warn("Writing the reader heartbeat {} failed (retrying next tick): {}", key, e.toString());
        }
    }

    private void refresh() {
        try {
            List<ObjectInfo> infos = store.list(layout.readersPrefix()).stream()
                    .filter(i -> i.key().endsWith(".json") && i.lastModified() != null).toList();
            Instant now = infos.stream().map(ObjectInfo::lastModified).max(Comparator.naturalOrder())
                    .orElse(Instant.EPOCH);
            Duration ttl = Duration.ofSeconds(cfg.getReaderTtlSeconds());
            List<ReaderHeartbeat> out = new ArrayList<>();
            for (ObjectInfo info : infos) {
                Duration age = Duration.between(info.lastModified(), now);
                if (age.compareTo(ttl) > 0) continue;
                try {
                    store.get(info.key()).ifPresent(o -> {
                        try {
                            out.add(MAPPER.readValue(o.data(), ReaderHeartbeat.class).withAge(age.toSeconds()));
                        } catch (IOException e) {
                            log.warn("Ignoring unreadable reader heartbeat {}: {}", info.key(), e.toString());
                        }
                    });
                } catch (IOException e) {
                    log.warn("Reading the reader heartbeat {} failed: {}", info.key(), e.toString());
                }
            }
            out.sort(Comparator.comparing(ReaderHeartbeat::node));
            live = List.copyOf(out);
        } catch (IOException | RuntimeException e) {
            log.warn("Listing the live readers failed (keeping the previous list): {}", e.toString());
        }
    }

    /** Stops the heartbeat and removes our object so peers drop this reader at once. */
    @Override
    public void close() {
        ScheduledExecutorService s;
        synchronized (this) {
            if (closed) return;
            closed = true;
            s = scheduler;
            scheduler = null;
        }
        if (s != null) s.shutdownNow();
        try {
            store.delete(key);
        } catch (IOException | RuntimeException e) {
            log.warn("Removing the reader heartbeat {} failed (it expires after the TTL): {}", key, e.toString());
        }
        live = List.of();
    }
}
