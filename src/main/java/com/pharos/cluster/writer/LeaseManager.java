package com.pharos.cluster.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.BucketClock;
import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.StoredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Shard leases of one writer (work-queue mode): {@code shards/<shard>/lease} =
 * {@code {"writer": node, "expires": …}}, owned through conditional puts.
 *
 * <ul>
 *   <li><b>Acquire:</b> {@code putIfAbsent} on a free shard; on a released or expired lease, a
 *       {@code putIfMatch} on the ETag just read. Of several writers racing for one lease, exactly
 *       one conditional put wins; the others get {@link Optional#empty()} and back off.</li>
 *   <li><b>Expiry</b> is judged by the bucket's clock only (see {@link BucketClock}): a lease is
 *       expired when its LastModified (via {@link ObjectStore#stat}) is more than the TTL older
 *       than the bucket's "now", the LastModified of this writer's own latest write (its heartbeat,
 *       from {@link BucketNow}, or its own latest lease write, whichever is newer). The JSON
 *       {@code expires} field is informational. Without any own write there is no "now", and a held
 *       lease is never taken over.</li>
 *   <li><b>Renewal</b> is a {@code putIfMatch} on the held ETag, every TTL/3 on a daemon thread
 *       ({@link #start}). A failed renewal (an I/O error, or the lease taken) marks the lease lost
 *       at once: {@link #assertHeld} throws from then on, until the lease is acquired again.</li>
 *   <li><b>Release</b> is a {@code putIfMatch} that marks the lease {@code "released": true}, so a
 *       lease taken over in the meantime is never clobbered, and any writer may take it at once.</li>
 * </ul>
 *
 * <p>Ownership is guarded by the lease CAS, publishing by the {@code LATEST} CAS: a check here
 * followed by a takeover before the publisher's {@code LATEST} CAS cannot be closed by any read,
 * and the {@code LATEST} CAS keeps that race harmless (the new owner's next publish conflicts and
 * re-reads {@code LATEST}).
 *
 * <p><b>Locking and deadlines.</b> The manager-wide lock guards only the in-memory state and is
 * never held across a store call. Calls for one shard are serialised by that shard's own lock, so
 * a slow bucket answer for one lease never delays another lease's renewal. Every store call on the
 * lease path (and {@link BucketNow#now}) runs with a deadline of TTL/6 on a separate daemon pool;
 * a call past it counts as failed (a failed renewal loses that lease), so a hung S3 request cannot
 * outlast the TTL. Other S3 callers are unaffected.
 *
 * <p><b>Caveats.</b>
 * <ul>
 *   <li>Call {@link #release} before {@link #close}: {@code close} stops the renewer and the call
 *       pool, so a later release cannot write and the lease simply expires after the TTL.</li>
 *   <li>An ambiguous acquire (a {@code putIfAbsent} or takeover put whose answer is lost: an I/O
 *       error or the deadline after the bucket applied it) leaves a lease naming this writer whose
 *       ETag it never learned. Nobody holds it in memory, so the shard stays unowned for one TTL,
 *       until it expires and anyone (this writer included) takes it over.</li>
 * </ul>
 */
public final class LeaseManager implements LeaseGuard, Closeable {

    private static final Logger log = LoggerFactory.getLogger(LeaseManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** The LastModified of this writer's own latest write (e.g. its heartbeat); empty when unknown. */
    @FunctionalInterface
    public interface BucketNow {
        BucketNow NONE = Optional::empty;

        Optional<Instant> now() throws IOException;
    }

    /** A lease this manager holds; {@code lastModified} is null when the stat after the write failed. */
    private record Held(Lease lease, Instant lastModified, boolean lost) {
        Held lostCopy() { return new Held(lease, lastModified, true); }
    }

    /** A store call on the lease path. */
    @FunctionalInterface
    private interface StoreCall<T> {
        T call() throws IOException;
    }

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final String writer;
    private final Duration ttl;
    private final Duration ioTimeout;
    private final BucketNow ownWrite;
    private final Clock clock;
    /** Guards held, closed and renewer; never held across a store call. */
    private final Object mu = new Object();
    private final Map<ShardId, Held> held = new HashMap<>();
    /** Serialises the store calls of one shard (held across them; bounded by the deadline). */
    private final Map<ShardId, Object> shardLocks = new ConcurrentHashMap<>();
    private final ExecutorService io = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "pharos-lease-io");
        t.setDaemon(true);
        return t;
    });
    private ScheduledExecutorService renewer;
    private boolean closed;

    /**
     * @param writer   this writer's node id, written into the lease
     * @param ttl      lease TTL ({@code cluster.writer.leaseTtlSeconds}); store calls get TTL/6 each
     * @param ownWrite the bucket's "now" from this writer's own latest write; {@link BucketNow#NONE}
     *                 leaves only its own lease writes
     * @param clock    local clock, only for the informational {@code expires} field
     */
    public LeaseManager(ObjectStore store, ObjectStoreLayout layout, String writer, Duration ttl,
                        BucketNow ownWrite, Clock clock) {
        this(store, layout, writer, ttl, ownWrite, clock, ttl == null ? null : ttl.dividedBy(6));
    }

    /**
     * Leases on other keys than the shard leases: {@code leaseKey} maps the id a caller acquires to its
     * lease object (the per-project reshard lease, {@link ObjectStoreLayout#reshardLeaseKey}, keyed by
     * {@code ShardId.ofProject(project)}).
     */
    public LeaseManager(ObjectStore store, ObjectStoreLayout layout, String writer, Duration ttl,
                        BucketNow ownWrite, Clock clock, java.util.function.Function<ShardId, String> leaseKey) {
        this(store, layout, writer, ttl, ownWrite, clock, ttl == null ? null : ttl.dividedBy(6));
        this.leaseKey = Objects.requireNonNull(leaseKey, "leaseKey");
    }

    /** The lease object of an id: the shard lease unless another mapping was given. */
    private java.util.function.Function<ShardId, String> leaseKey;

    /** Visible for tests: an explicit deadline per store call. */
    LeaseManager(ObjectStore store, ObjectStoreLayout layout, String writer, Duration ttl,
                 BucketNow ownWrite, Clock clock, Duration ioTimeout) {
        this.leaseKey = layout::leaseKey;
        this.store = Objects.requireNonNull(store, "store");
        this.layout = Objects.requireNonNull(layout, "layout");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        if (ttl.isNegative() || ttl.isZero()) throw new IllegalArgumentException("lease TTL must be positive: " + ttl);
        this.ioTimeout = Objects.requireNonNull(ioTimeout, "ioTimeout");
        if (ioTimeout.isNegative() || ioTimeout.isZero()) {
            throw new IllegalArgumentException("lease store deadline must be positive: " + ioTimeout);
        }
        this.ownWrite = Objects.requireNonNull(ownWrite, "ownWrite");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public String writer() { return writer; }

    public Duration ttl() { return ttl; }

    /** Renews every held lease every TTL/3 on a daemon thread. */
    public void start() {
        synchronized (mu) {
            if (renewer != null || closed) return;
            renewer = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-lease-renewer");
                t.setDaemon(true);
                return t;
            });
            long every = Math.max(1, ttl.dividedBy(3).toMillis());
            renewer.scheduleWithFixedDelay(this::renewAll, every, every, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * One renewal round: every held lease is renewed in parallel, so a slow one (bounded by the
     * deadline) never holds up the others. Never throws.
     */
    void renewAll() {
        List<Lease> leases;
        synchronized (mu) {
            if (closed) return;
            leases = held.values().stream().filter(h -> !h.lost()).map(Held::lease).toList();
        }
        List<Callable<Boolean>> tasks = leases.stream().<Callable<Boolean>>map(l -> () -> {
            try {
                return renew(l);
            } catch (RuntimeException e) {
                log.warn("Renewing the lease of shard {} failed: {}", l.shard(), e.toString());
                return false;
            }
        }).toList();
        try {
            io.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RejectedExecutionException e) {
            // closed meanwhile
        }
    }

    /**
     * Takes the shard's lease when it is absent, released or expired (by the bucket's clock), by a
     * conditional put. Returns the held lease when this manager already holds it.
     *
     * @return empty when another writer holds a live lease, or won the race for it
     * @throws IOException on a store error or a call past the deadline (nothing is held then)
     */
    public Optional<Lease> tryAcquire(ShardId shard) throws IOException {
        synchronized (lockFor(shard)) {
            Held mine = current(shard);
            if (mine != null && !mine.lost()) return Optional.of(mine.lease());
            String key = leaseKey.apply(shard);
            Optional<StoredObject> cur = call("get " + key, () -> store.get(key));
            Optional<String> etag;
            if (cur.isEmpty()) {
                etag = call("putIfAbsent " + key, () -> store.putIfAbsent(key, body(false)));
            } else {
                // A lease seen lost that still carries our last ETag was written by nobody since:
                // reclaim it at once instead of waiting out the TTL (the put is still conditional).
                boolean untouchedSinceOurs = mine != null && mine.lease().etag().equals(cur.get().etag());
                if (!untouchedSinceOurs && !takeable(shard, key, cur.get())) return Optional.empty();
                String seen = cur.get().etag();
                etag = call("putIfMatch " + key, () -> store.putIfMatch(key, body(false), seen));
            }
            if (etag.isEmpty()) {
                log.debug("Lost the race for the lease of shard {}", shard);
                return Optional.empty();
            }
            Held h = recordWrite(shard, key, etag.get(), mine);
            log.info("Acquired the lease of shard {} as writer {}", shard, writer);
            return Optional.of(h.lease());
        }
    }

    // Holding the shard lock. Released, or expired by the bucket's clock. The body is read before the
    // stat, so a write between them makes the stat newer: the lease looks younger, never older.
    private boolean takeable(ShardId shard, String key, StoredObject cur) throws IOException {
        JsonNode body = parse(cur.data());
        if (body != null && body.path("released").asBoolean(false)) return true;
        Optional<ObjectInfo> info = call("stat " + key, () -> store.stat(key));
        if (info.isEmpty() || info.get().lastModified() == null) return false; // deleted meanwhile: retry later
        Optional<Instant> now = bucketNow();
        if (now.isEmpty()) {
            log.debug("Cannot judge the lease of shard {}: no own write to read the bucket's clock from", shard);
            return false;
        }
        boolean expired = BucketClock.expired(info.get().lastModified(), now.get(), ttl);
        if (expired) {
            log.info("Lease of shard {} (writer {}) expired {} ago by the bucket's clock; taking it over",
                    shard, body == null ? "?" : body.path("writer").asText("?"),
                    BucketClock.age(info.get().lastModified(), now.get()).minus(ttl));
        }
        return expired;
    }

    /**
     * Renews a held lease by {@code If-Match} on its current ETag. A failure (I/O, the deadline, or
     * the lease taken) marks it lost.
     *
     * @return false when this manager does not hold the shard's lease (any more)
     */
    public boolean renew(Lease lease) {
        ShardId shard = lease.shard();
        synchronized (lockFor(shard)) {
            Held h;
            synchronized (mu) {
                h = held.get(shard);
                if (h == null || h.lost() || closed) return false;
            }
            String key = leaseKey.apply(shard);
            String etagNow = h.lease().etag();
            Optional<String> etag;
            try {
                etag = call("putIfMatch " + key, () -> store.putIfMatch(key, body(false), etagNow));
            } catch (IOException | RuntimeException e) {
                markLost(shard, h, "renewal failed: " + e);
                return false;
            }
            if (etag.isEmpty()) {
                markLost(shard, h, "it was taken by another writer");
                return false;
            }
            recordWrite(shard, key, etag.get(), h);
            return true;
        }
    }

    /**
     * Releases a held lease (marks it released by a conditional put). Never throws; a failure leaves
     * it to expire. Call it before {@link #close}.
     */
    public void release(Lease lease) {
        ShardId shard = lease.shard();
        synchronized (lockFor(shard)) {
            Held h;
            synchronized (mu) {
                h = held.remove(shard);
            }
            if (h == null || h.lost()) return;
            String key = leaseKey.apply(shard);
            try {
                if (call("putIfMatch " + key, () -> store.putIfMatch(key, body(true), h.lease().etag())).isPresent()) {
                    log.info("Released the lease of shard {}", shard);
                } else {
                    log.warn("Lease of shard {} was already taken when releasing it", shard);
                }
            } catch (IOException | RuntimeException e) {
                log.warn("Releasing the lease of shard {} failed (it expires after {}): {}", shard, ttl, e.toString());
            }
        }
    }

    /** True while this manager holds the shard's lease and has not seen it lost. Never waits on the store. */
    public boolean isHeld(ShardId shard) {
        synchronized (mu) {
            Held h = held.get(shard);
            return h != null && !h.lost();
        }
    }

    /** The leases currently held (not lost). Never waits on the store. */
    public List<Lease> held() {
        synchronized (mu) {
            return held.values().stream().filter(h -> !h.lost()).map(Held::lease)
                    .sorted(java.util.Comparator.comparing(l -> l.shard().value())).toList();
        }
    }

    /**
     * Re-reads the lease: it must still carry the ETag of this writer's latest write, and must not be
     * expired by the bucket's clock. A lease seen lost stays lost.
     */
    @Override
    public void assertHeld(ShardId shard) throws PublishConflictException {
        synchronized (lockFor(shard)) {
            Held h = current(shard);
            if (h == null) throw new PublishConflictException("writer " + writer + " holds no lease on shard " + shard);
            if (h.lost()) throw new PublishConflictException("writer " + writer + " lost the lease on shard " + shard);
            String key = leaseKey.apply(shard);
            try {
                Optional<StoredObject> cur = call("get " + key, () -> store.get(key));
                if (cur.isEmpty() || !h.lease().etag().equals(cur.get().etag())) {
                    markLost(shard, h, cur.isEmpty() ? "the lease object is gone" : "it was taken by another writer");
                    throw new PublishConflictException("writer " + writer + " lost the lease on shard " + shard);
                }
                Optional<ObjectInfo> info = call("stat " + key, () -> store.stat(key));
                Optional<Instant> now = bucketNow();
                if (info.isPresent() && info.get().lastModified() != null && now.isPresent()
                        && BucketClock.expired(info.get().lastModified(), now.get(), ttl)) {
                    markLost(shard, h, "it expired by the bucket's clock");
                    throw new PublishConflictException("the lease of writer " + writer + " on shard " + shard
                            + " expired");
                }
            } catch (PublishConflictException e) {
                throw e;
            } catch (IOException | RuntimeException e) {
                throw new PublishConflictException("cannot confirm the lease on shard " + shard + ": " + e, e);
            }
        }
    }

    /**
     * Stops the renewer and the store-call pool. Held leases are not released: call
     * {@link #release} first (a graceful stop), or they expire after the TTL.
     */
    @Override
    public void close() {
        ScheduledExecutorService s;
        synchronized (mu) {
            if (closed) return;
            closed = true;
            s = renewer;
            renewer = null;
        }
        if (s != null) {
            s.shutdownNow();
            try {
                s.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        io.shutdownNow();
    }

    private Object lockFor(ShardId shard) {
        return shardLocks.computeIfAbsent(shard, k -> new Object());
    }

    private Held current(ShardId shard) {
        synchronized (mu) {
            return held.get(shard);
        }
    }

    /** Runs a store call on the pool with the deadline; past it, the call is cancelled and fails. */
    private <T> T call(String what, StoreCall<T> c) throws IOException {
        Future<T> f;
        try {
            f = io.submit(c::call);
        } catch (RejectedExecutionException e) {
            throw new IOException("lease manager closed (" + what + ")", e);
        }
        try {
            return f.get(ioTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            f.cancel(true);
            throw new IOException(what + " timed out after " + ioTimeout, e);
        } catch (InterruptedException e) {
            f.cancel(true);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException(what + " interrupted");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new IOException(what + " failed: " + cause, cause);
        }
    }

    // Applies under mu, only if the shard's entry is still the one the caller started from.
    private void markLost(ShardId shard, Held expected, String why) {
        synchronized (mu) {
            Held h = held.get(shard);
            if (h == null || h != expected || h.lost()) return;
            held.put(shard, h.lostCopy());
        }
        log.warn("Lease of shard {} lost ({}): writer {} stops publishing it", shard, why, writer);
    }

    // Holding the shard lock. Records a successful lease write; its bucket LastModified (by a stat,
    // best effort) dates the lease and serves as an own write for the bucket's "now". The entry is
    // replaced only if it is still {@code expected} (what the caller started from).
    private Held recordWrite(ShardId shard, String key, String etag, Held expected) {
        Instant lastModified = null;
        try {
            lastModified = call("stat " + key, () -> store.stat(key)).map(ObjectInfo::lastModified).orElse(null);
        } catch (IOException | RuntimeException e) {
            log.debug("Stat of the lease of shard {} failed: {}", shard, e.toString());
        }
        Instant expires = (lastModified != null ? lastModified : clock.instant()).plus(ttl);
        Held h = new Held(new Lease(shard, writer, expires, etag), lastModified, false);
        synchronized (mu) {
            if (held.get(shard) == expected) held.put(shard, h);
        }
        return h;
    }

    // The newest of this writer's own writes: the injected one (heartbeat) and its leases.
    private Optional<Instant> bucketNow() throws IOException {
        Instant best = call("own write", ownWrite::now).orElse(null);
        synchronized (mu) {
            for (Held h : held.values()) {
                if (h.lastModified() != null && (best == null || h.lastModified().isAfter(best))) best = h.lastModified();
            }
        }
        return Optional.ofNullable(best);
    }

    private byte[] body(boolean released) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("writer", writer);
        m.put("expires", clock.instant().plus(ttl).toString());
        if (released) m.put("released", true);
        try {
            return MAPPER.writeValueAsBytes(m);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static JsonNode parse(byte[] data) {
        try {
            return MAPPER.readTree(data);
        } catch (IOException e) {
            return null; // unreadable: judged by its age alone
        }
    }
}
