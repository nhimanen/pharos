package com.pharos.cluster.membership;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.BoundedCall;
import com.pharos.cluster.objectstore.BucketClock;
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
import java.util.concurrent.locks.ReentrantLock;
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
    /** Serialises the ticks (held across their store calls); shutdown waits for it only so long. */
    private final ReentrantLock lock = new ReentrantLock();
    private final Object mu = new Object(); // guards scheduler; never held across a store call
    private volatile List<LiveReader> live = List.of();
    private volatile boolean lastWriteOk;
    private volatile boolean draining;
    private volatile boolean closed;
    private long lastWarn = System.nanoTime() - WARN_EVERY_NANOS;
    private ScheduledExecutorService scheduler;

    public ReaderRegistry(ObjectStore store, ObjectStoreLayout layout, ClusterConfig cfg,
                          Supplier<ReaderHeartbeat> self) {
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.self = self; // null: an observer
        this.key = self == null ? null : layout.readerKey(cfg.effectiveNodeId());
    }

    /**
     * A read-only registry ({@code pharos router}): it lists the live readers every
     * {@code heartbeatSeconds} and never writes, drains or deletes an object. With no heartbeat of
     * its own, the bucket's "now" is the newest heartbeat's LastModified: when every reader stopped
     * beating, all still look live to it; the router's own {@code /health} polls catch those.
     */
    public static ReaderRegistry observer(ObjectStore store, ObjectStoreLayout layout, ClusterConfig cfg) {
        return new ReaderRegistry(store, layout, cfg, null);
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
        lock.lock();
        try {
            if (closed) return;
            write();
            if (closed) {
                // closed while the put was in flight: its delete may have run first
                if (self != null && lastWriteOk) deleteQuietly();
                return;
            }
            refresh();
        } finally {
            lock.unlock();
        }
    }

    /** The deadline of {@link #drain()} and of {@link #close()}'s heartbeat delete. */
    static final Duration DEFAULT_SHUTDOWN_CALL = Duration.ofSeconds(5);

    /** {@link #drain(Duration)} within {@link #DEFAULT_SHUTDOWN_CALL}. */
    public void drain() {
        drain(DEFAULT_SHUTDOWN_CALL);
    }

    /**
     * Marks this reader draining and publishes that at once. Waits for a tick in progress and for
     * the write only until {@code timeout}, then gives up with a WARN: the next tick carries
     * {@code draining} (the flag is set first), and the heartbeat is removed on close or expires.
     * Never throws.
     */
    public void drain(Duration timeout) {
        if (closed || self == null) return;
        draining = true;
        long deadline = System.nanoTime() + Math.max(0, timeout.toNanos());
        try {
            if (!lock.tryLock(Math.max(0, timeout.toNanos()), TimeUnit.NANOSECONDS)) {
                log.warn("Stopping: gave up announcing draining in the reader heartbeat {}: a heartbeat write has been"
                        + " in progress for over {} ms (the bucket is slow)", key, BoundedCall.millis(timeout));
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            if (closed) return;
            ReaderHeartbeat hb = self.get();
            byte[] body = MAPPER.writeValueAsBytes(hb.with(true, startedAt));
            BoundedCall.call("put " + key, BoundedCall.until(deadline), () -> store.put(key, body));
            lastWriteOk = true;
        } catch (IOException | RuntimeException e) {
            log.warn("Stopping: gave up announcing draining in the reader heartbeat {}: {}", key, e.toString());
        } finally {
            lock.unlock();
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

    // Holding the lock.
    private void write() {
        if (closed || self == null) return;
        try {
            ReaderHeartbeat hb = self.get();
            store.put(key, MAPPER.writeValueAsBytes(hb.with(draining || hb.draining(), startedAt)));
            lastWriteOk = true;
        } catch (IOException | RuntimeException e) {
            lastWriteOk = false;
            if (!closed) log.warn("Writing the reader heartbeat {} failed (retrying next tick): {}", key, e.toString());
        }
    }

    private void warnStale(String why) {
        long now = System.nanoTime();
        if (now - lastWarn >= WARN_EVERY_NANOS) {
            lastWarn = now;
            log.warn("Keeping the previous live-reader list: {}", why);
        }
    }

    // Holding the lock. Liveness is measured against this node's own heartbeat object (the bucket's clock),
    // and only when our latest write succeeded: otherwise we cannot tell "peers are dead" from
    // "the bucket is unreachable", so the previous list stays.
    void refresh() {
        if (self != null && !lastWriteOk) {
            warnStale("our own heartbeat write failed");
            return;
        }
        try {
            List<ObjectInfo> infos = store.list(layout.readersPrefix()).stream()
                    .filter(i -> i.key().endsWith(".json") && i.lastModified() != null).toList();
            Instant now = BucketClock.nowFrom(infos, self == null ? null : key).orElse(null);
            if (now == null && self == null) {
                live = List.of(); // no reader announces itself
                return;
            }
            if (now == null) {
                warnStale("our own heartbeat is missing from the listing");
                return;
            }
            Duration ttl = Duration.ofSeconds(cfg.getReaderTtlSeconds());
            List<LiveReader> out = new ArrayList<>();
            for (BucketClock.Aged aged : BucketClock.unexpired(infos, now, ttl)) {
                ObjectInfo info = aged.info();
                Duration age = aged.age();
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

    /** {@link #close(Duration)} within {@link #DEFAULT_SHUTDOWN_CALL}. */
    @Override
    public void close() {
        close(DEFAULT_SHUTDOWN_CALL);
    }

    /**
     * Stops the heartbeat and removes our object so peers drop this reader at once. A tick stuck on
     * the bucket is abandoned (its thread is a daemon; if its put lands later, it deletes what it
     * wrote), and the delete gives up at {@code within} with a WARN: the heartbeat then expires after
     * the TTL. Idempotent; returns within {@code within}.
     */
    public void close(Duration within) {
        long deadline = System.nanoTime() + Math.max(0, within.toNanos());
        ScheduledExecutorService s;
        synchronized (mu) {
            if (closed) return;
            closed = true; // later ticks return at once; one in flight deletes what it wrote
            s = scheduler;
            scheduler = null;
        }
        if (s != null) s.shutdownNow(); // a tick blocked in a socket read ignores the interrupt: not awaited
        live = List.of();
        if (self == null) return; // an observer owns no object
        try {
            BoundedCall.call("delete " + key, BoundedCall.until(deadline), () -> {
                store.delete(key);
                return null;
            });
        } catch (IOException | RuntimeException e) {
            log.warn("Stopping: gave up removing the reader heartbeat {} (it expires after {} s): {}", key,
                    cfg.getReaderTtlSeconds(), e.toString());
        }
    }

    private void deleteQuietly() {
        try {
            store.delete(key);
        } catch (IOException | RuntimeException e) {
            log.debug("Removing the reader heartbeat {} after a late tick failed: {}", key, e.toString());
        }
    }
}
