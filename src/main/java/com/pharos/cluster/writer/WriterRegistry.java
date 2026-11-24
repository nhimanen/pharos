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
import java.util.concurrent.locks.ReentrantLock;
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
 *
 * <p><b>Shutdown is bounded.</b> A beat holds the registry's lock across its store calls, and on a
 * saturated bucket one S3 call can take minutes. {@link #announce} and {@link #close(Duration)}
 * therefore wait for that lock, and for their own store call, only until their deadline, and log
 * that they gave up (the heartbeat then expires after the TTL). A beat that was in flight when the
 * registry closed deletes the heartbeat it wrote, so a slow put cannot bring it back.
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
    /** Serialises the beats (held across their store calls); shutdown waits for it only so long. */
    private final ReentrantLock lock = new ReentrantLock();
    private final Object mu = new Object(); // guards scheduler; never held across a store call
    private volatile List<LiveWriter> live = List.of();
    private volatile boolean lastWriteOk;
    private volatile boolean closed;
    private ScheduledExecutorService scheduler; // under mu

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
        lock.lock();
        try {
            if (closed) return;
            try {
                store.put(key, MAPPER.writeValueAsBytes(self.get()));
                lastWriteOk = true;
            } catch (IOException | RuntimeException e) {
                lastWriteOk = false;
                if (!closed) log.warn("Writing the writer heartbeat {} failed (retrying next beat): {}", key, e.toString());
                return;
            }
            if (closed) {
                // closed while the put was in flight: its delete may have run first
                deleteQuietly();
                return;
            }
            refresh();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Shutdown: writes this writer's heartbeat once (it says {@code stopping}), so peers stop
     * counting it. Waits for a beat in progress and for the write only until {@code timeout}, then
     * gives up with a WARN. Never throws.
     *
     * @return true when the heartbeat was written
     */
    public boolean announce(Duration timeout) {
        long deadline = System.nanoTime() + Math.max(0, timeout.toNanos());
        try {
            if (!lock.tryLock(Math.max(0, timeout.toNanos()), TimeUnit.NANOSECONDS)) {
                log.warn("Stopping: gave up announcing it in the writer heartbeat {}: a heartbeat write has been in"
                        + " progress for over {} ms (the bucket is slow); peers see it stop when the heartbeat goes"
                        + " or expires", key, BoundedCall.millis(timeout));
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        try {
            if (closed) return false;
            byte[] body = MAPPER.writeValueAsBytes(self.get());
            BoundedCall.call("put " + key, BoundedCall.until(deadline), () -> store.put(key, body));
            lastWriteOk = true;
            return true;
        } catch (IOException | RuntimeException e) {
            log.warn("Stopping: gave up announcing it in the writer heartbeat {}: {}", key, e.toString());
            return false;
        } finally {
            lock.unlock();
        }
    }

    // Holding the lock, after a successful own write.
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

    /** The deadline of {@link #close()}. */
    static final Duration DEFAULT_CLOSE = Duration.ofSeconds(10);

    /** {@link #close(Duration)} within {@link #DEFAULT_CLOSE}. */
    @Override
    public void close() {
        close(DEFAULT_CLOSE);
    }

    /**
     * Stops beating and deletes our heartbeat, so peers drop this writer at once. A beat stuck on
     * the bucket is abandoned (its thread is a daemon; if its put lands later, it deletes what it
     * wrote), and the delete gives up at {@code within} with a WARN: the heartbeat then expires after
     * the TTL. Idempotent; returns within {@code within}.
     */
    public void close(Duration within) {
        long deadline = System.nanoTime() + Math.max(0, within.toNanos());
        ScheduledExecutorService s;
        synchronized (mu) {
            if (closed) return;
            closed = true;
            s = scheduler;
            scheduler = null;
        }
        if (s != null) s.shutdownNow(); // a beat blocked in a socket read ignores the interrupt: not awaited
        live = List.of();
        try {
            BoundedCall.call("delete " + key, BoundedCall.until(deadline), () -> {
                store.delete(key);
                return null;
            });
        } catch (IOException | RuntimeException e) {
            log.warn("Stopping: gave up removing the writer heartbeat {} (it expires after {} s): {}", key,
                    ttl.toSeconds(), e.toString());
        }
    }

    private void deleteQuietly() {
        try {
            store.delete(key);
        } catch (IOException | RuntimeException e) {
            log.debug("Removing the writer heartbeat {} after a late beat failed: {}", key, e.toString());
        }
    }
}
