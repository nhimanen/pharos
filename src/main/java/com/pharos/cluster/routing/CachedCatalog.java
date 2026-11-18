package com.pharos.cluster.routing;

import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * The shard catalog as a coordinator routes with it: re-read at most every {@code refreshEvery}, in
 * the background once a first read succeeded, so no query waits on the object store. A failed read
 * backs off for {@code refreshEvery} and keeps the last good catalog; once that is older than
 * {@code maxAge} ({@code cluster.maxCatalogAgeSeconds}), unscoped routing is refused (503). Before
 * any good read, {@link #shards} throws {@link CatalogNeverReadException} (one blocking attempt per
 * back-off period; concurrent callers do not wait on it).
 */
public final class CachedCatalog implements ShardCatalog {

    private static final Logger log = LoggerFactory.getLogger(CachedCatalog.class);

    private final ShardCatalog base;
    private final long refreshNanos;
    private final long maxAgeNanos;
    private final LongSupplier clock;
    private final Executor refresher;
    private final ReentrantLock firstRead = new ReentrantLock();
    private static final long IDLE = Long.MIN_VALUE;
    /**
     * When the running background read started, or {@link #IDLE}. One atomic value is both the
     * "running" flag and its start time, so a concurrent query never sees a read running with the
     * start time of an earlier one (and takes it for hung).
     */
    private final AtomicLong runningSince = new AtomicLong(IDLE);
    /**
     * Bumped per refresh. Only the current refresh may store its result or clear
     * {@link #runningSince}: a late finish of an abandoned one changes nothing.
     */
    private final AtomicLong refreshToken = new AtomicLong();
    /** The token of the abandoned read still running, or 0: at most one is ever abandoned at a time. */
    private final AtomicLong abandoned = new AtomicLong();
    private final long hungNanos;

    private volatile List<ShardId> cached;
    private volatile long readAt;          // last good read
    private volatile long attemptAt;       // last attempt, good or not
    private volatile boolean attempted;
    private volatile IOException lastFailure;

    public CachedCatalog(ShardCatalog base, Duration refreshEvery, Duration maxAge, LongSupplier nanoClock,
                         Executor refresher) {
        this(base, refreshEvery, maxAge, nanoClock, refresher, Duration.ofSeconds(60));
    }

    /**
     * @param hungAfter a background read still running after this (the object store's own timeouts
     *                  should have ended it) is given up: the next query starts a new one
     */
    public CachedCatalog(ShardCatalog base, Duration refreshEvery, Duration maxAge, LongSupplier nanoClock,
                         Executor refresher, Duration hungAfter) {
        this.hungNanos = hungAfter.toNanos();
        this.base = base;
        this.refreshNanos = refreshEvery.toNanos();
        this.maxAgeNanos = maxAge.toNanos();
        this.clock = nanoClock;
        this.refresher = refresher;
    }

    @Override
    public List<ShardId> shards() throws IOException {
        long now = clock.getAsLong();
        List<ShardId> current = cached;
        if (current == null) return firstShards(now);
        long since = runningSince.get();
        if (since != IDLE && now - since > hungNanos) giveUp(since, now); // watchdog: a read that hangs
        if (now - attemptAt >= refreshNanos && runningSince.compareAndSet(IDLE, now)) {
            attemptAt = now;
            long token = refreshToken.incrementAndGet();
            try {
                refresher.execute(() -> refresh(token));
            } catch (RuntimeException e) {
                runningSince.compareAndSet(now, IDLE);
            }
        }
        long age = now - readAt;
        if (age > maxAgeNanos) {
            throw new CatalogTooOldException("the shard catalog was last read " + age / 1_000_000_000L
                    + " s ago, over cluster.maxCatalogAgeSeconds (" + maxAgeNanos / 1_000_000_000L + " s)"
                    + (lastFailure == null ? "" : ": " + lastFailure.getMessage()), lastFailure, current,
                    age / 1_000_000_000L);
        }
        return current;
    }

    private List<ShardId> firstShards(long now) {
        if (attempted && now - attemptAt < refreshNanos) throw neverRead();
        if (!firstRead.tryLock()) throw neverRead(); // another query is reading it: do not wait
        try {
            if (cached != null) return cached;
            attempted = true;
            attemptAt = now;
            try {
                List<ShardId> read = List.copyOf(base.shards());
                readAt = clock.getAsLong();
                lastFailure = null;
                cached = read;
                return read;
            } catch (IOException e) {
                lastFailure = e;
                throw neverRead();
            } catch (RuntimeException e) {
                lastFailure = new IOException(e.toString(), e);
                throw neverRead();
            }
        } finally {
            firstRead.unlock();
        }
    }

    private CatalogNeverReadException neverRead() {
        IOException f = lastFailure;
        return new CatalogNeverReadException("the shard catalog has not been read yet"
                + (f == null ? "" : ": " + f.getMessage()), f);
    }

    /**
     * Abandons the read running since {@code since} so the next query starts another, unless an
     * abandoned read is still running: then this one is waited for too (no pile-up of hung reads).
     */
    private void giveUp(long since, long now) {
        long token = refreshToken.get();
        if (!abandoned.compareAndSet(0, token)) return; // one abandoned read still runs
        synchronized (this) {
            if (refreshToken.get() != token || runningSince.get() != since) { // finished or replaced meanwhile
                abandoned.compareAndSet(token, 0);
                return;
            }
            refreshToken.incrementAndGet(); // its late result is discarded
            runningSince.set(IDLE);
        }
        log.warn("The background read of the shard catalog has run {} s; starting another",
                (now - since) / 1_000_000_000L);
    }

    private void refresh(long token) {
        List<ShardId> read = null;
        Exception failure = null;
        try {
            read = List.copyOf(base.shards());
        } catch (IOException | RuntimeException e) {
            failure = e;
        }
        boolean current;
        synchronized (this) { // stores only while still the current refresh
            current = refreshToken.get() == token;
            if (current) {
                if (read != null) {
                    readAt = clock.getAsLong();
                    lastFailure = null;
                    cached = read;
                } else {
                    lastFailure = failure instanceof IOException io ? io : new IOException(failure.toString(), failure);
                }
                runningSince.set(IDLE);
            }
        }
        abandoned.compareAndSet(token, 0); // an abandoned read has finished
        if (current && failure != null) {
            log.warn("Reading the shard catalog failed; routing with the one read {} s ago (retrying in {} s): {}",
                    ageSeconds(), refreshNanos / 1_000_000_000L, failure.toString());
        }
    }

    /** Seconds since the last good read; -1 before the first. */
    public long ageSeconds() {
        return cached == null ? -1 : (clock.getAsLong() - readAt) / 1_000_000_000L;
    }
}
