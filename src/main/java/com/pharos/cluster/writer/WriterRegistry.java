package com.pharos.cluster.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.objectstore.BucketClock;
import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
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
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Membership of the work-queue writers: this writer's heartbeat {@code catalog/writers/<node>.json}
 * and the list of live writers. The writer-side twin of {@code ReaderRegistry}, with the same
 * liveness rule ({@link BucketClock#unexpired}): an entry is live while its LastModified is within
 * the TTL ({@code cluster.writer.leaseTtlSeconds}) of this writer's own heartbeat's LastModified,
 * the bucket's "now". Clock skew between nodes cannot expire or resurrect a writer.
 *
 * <p>{@link #bucketNow} is the {@link LeaseManager.BucketNow} of this writer's leases: the
 * LastModified of its own heartbeat read with {@link ObjectStore#stat}, the same call the lease
 * manager dates leases with, so both sides of an expiry comparison carry the same precision (and
 * {@link BucketClock} truncates both to seconds anyway).
 *
 * <p>A failed write or listing logs a WARN and keeps the previous live list; until our own write
 * succeeds again, {@link #lastWriteOk()} is false and the balancer makes no decision.
 */
public final class WriterRegistry implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WriterRegistry.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** A live writer and its heartbeat's age by the bucket's clock. */
    public record LiveWriter(WriterHeartbeat heartbeat, long ageSeconds) {}

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final String key;
    private final Duration ttl;
    private final Duration every;
    private final Supplier<WriterHeartbeat> self;
    private final Object mu = new Object();
    private volatile List<LiveWriter> live = List.of();
    private volatile boolean lastWriteOk;
    private boolean closed;                 // under mu
    private ScheduledExecutorService scheduler;

    /**
     * @param ttl   liveness TTL of a heartbeat
     * @param every heartbeat interval ({@code cluster.heartbeatSeconds}); must be below the TTL
     * @param self  this writer's current heartbeat
     */
    public WriterRegistry(ObjectStore store, ObjectStoreLayout layout, String node, Duration ttl, Duration every,
                          Supplier<WriterHeartbeat> self) {
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.key = layout.writerKey(Objects.requireNonNull(node, "node"));
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.every = Objects.requireNonNull(every, "every");
        this.self = Objects.requireNonNull(self, "self");
    }

    /** Beats every {@code every} on a daemon thread, starting now. */
    public void start() {
        synchronized (mu) {
            if (scheduler != null || closed) return;
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-writer-heartbeat");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleWithFixedDelay(this::beat, 0, Math.max(1, every.toMillis()), TimeUnit.MILLISECONDS);
        }
    }

    /** Writes our heartbeat, then refreshes the live list. Never throws. */
    public void beat() {
        synchronized (mu) {
            if (closed) return;
            try {
                store.put(key, MAPPER.writeValueAsBytes(self.get()));
                lastWriteOk = true;
            } catch (IOException | RuntimeException e) {
                lastWriteOk = false;
                log.warn("Writing the writer heartbeat {} failed (retrying next beat): {}", key, e.toString());
                return;
            }
            refresh();
        }
    }

    // Holding mu, after a successful own write.
    private void refresh() {
        try {
            List<ObjectInfo> infos = store.list(layout.writersPrefix()).stream()
                    .filter(i -> i.key().endsWith(".json") && i.lastModified() != null).toList();
            Optional<Instant> now = BucketClock.nowFrom(infos, key);
            if (now.isEmpty()) {
                log.warn("Keeping the previous live-writer list: our heartbeat {} is missing from the listing", key);
                return;
            }
            List<LiveWriter> out = new ArrayList<>();
            for (BucketClock.Aged aged : BucketClock.unexpired(infos, now.get(), ttl)) {
                try {
                    store.get(aged.info().key()).ifPresent(o -> {
                        try {
                            out.add(new LiveWriter(MAPPER.readValue(o.data(), WriterHeartbeat.class),
                                    Math.max(0, aged.age().toSeconds())));
                        } catch (IOException e) {
                            log.warn("Ignoring unreadable writer heartbeat {}: {}", aged.info().key(), e.toString());
                        }
                    });
                } catch (IOException | RuntimeException e) {
                    log.warn("Reading the writer heartbeat {} failed: {}", aged.info().key(), e.toString());
                }
            }
            out.sort(Comparator.comparing(l -> l.heartbeat().node()));
            live = List.copyOf(out);
        } catch (IOException | RuntimeException e) {
            log.warn("Listing the live writers failed (keeping the previous list): {}", e.toString());
        }
    }

    /** True when our latest heartbeat write succeeded (the live list is current). */
    public boolean lastWriteOk() {
        return lastWriteOk;
    }

    /** The live writers as of the last beat, this one included, sorted by node. */
    public List<LiveWriter> live() {
        return live;
    }

    /** The bucket's "now": our heartbeat's LastModified by {@code stat}; empty before the first write. */
    public Optional<Instant> bucketNow() throws IOException {
        return store.stat(key).map(ObjectInfo::lastModified);
    }

    /** Stops beating and deletes our heartbeat, so peers drop this writer at once. Idempotent. */
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
            try {
                store.delete(key);
            } catch (IOException | RuntimeException e) {
                log.warn("Removing the writer heartbeat {} failed (it expires after {}): {}", key, ttl, e.toString());
            }
            live = List.of();
        }
    }
}
