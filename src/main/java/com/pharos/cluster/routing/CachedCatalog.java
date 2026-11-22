package com.pharos.cluster.routing;

import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.sharding.ProjectShards;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * The shard catalog as a coordinator routes with it: re-read at most every {@code refreshEvery}, in
 * the background once a first read succeeded, by a timer ({@link #tick}, which the reader runs every
 * {@code refreshEvery}) and by queries. A failed read backs off for {@code refreshEvery} and keeps the
 * last good catalog. Before any good read, {@link #shards} throws {@link CatalogNeverReadException}
 * (one blocking attempt per back-off period; concurrent callers do not wait on it).
 *
 * <p><b>Stale versus failing.</b> A catalog that is merely old (the timer was not running, or the
 * node was idle) is not an error: a query that finds it older than {@code 2 × refreshEvery} while
 * nothing has failed waits for the read in flight, at most {@code maxWait}, and then routes with
 * whatever it has, without warning. Only while reads are actually <i>failing</i> (the last one
 * failed, or one has been running longer than {@code refreshEvery}) does the age count:
 * {@link #staleSeconds} reports it (the query warning) and, past {@code maxAge}
 * ({@code cluster.maxCatalogAgeSeconds}), unscoped routing is refused ({@link CatalogTooOldException}).
 *
 * <p><b>One read, one version.</b> What is cached is one read of the base catalog as a whole: its
 * shards and its project ↔ shards mapping ({@link #projectShards}) always come from the same version.
 * A caller that needs both takes the {@link ProjectShards} once and uses only it, so a re-read landing
 * meanwhile (a reshard flip) never mixes two versions in one query.
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
    /** The longest a query waits for the read in flight when the catalog is stale and nothing failed. */
    private final long maxWaitNanos;

    private volatile ProjectShards cached;
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
        this(base, refreshEvery, maxAge, nanoClock, refresher, hungAfter, Duration.ZERO);
    }

    /**
     * @param maxWait how long a query waits for the read in flight when the catalog is stale and no
     *                read failed (bounded by the caller to a small part of the query budget)
     */
    public CachedCatalog(ShardCatalog base, Duration refreshEvery, Duration maxAge, LongSupplier nanoClock,
                         Executor refresher, Duration hungAfter, Duration maxWait) {
        this.maxWaitNanos = Math.max(0, maxWait.toNanos());
        this.hungNanos = hungAfter.toNanos();
        this.base = base;
        this.refreshNanos = refreshEvery.toNanos();
        this.maxAgeNanos = maxAge.toNanos();
        this.clock = nanoClock;
        this.refresher = refresher;
    }

    @Override
    public List<ShardId> shards() throws IOException {
        return projectShards().listed();
    }

    /**
     * The cached catalog version as a whole (see the class comment), with the same freshness rules and
     * exceptions as {@link #shards}.
     */
    @Override
    public ProjectShards projectShards() throws IOException {
        long now = clock.getAsLong();
        ProjectShards current = cached;
        if (current == null) return firstShards(now);
        startRefreshIfDue(now);
        if (now - readAt > 2 * refreshNanos && lastFailure == null) {
            awaitRefresh(); // stale, not failing: the read just started (or running) is likely quick
            current = cached;
            now = clock.getAsLong();
        }
        long age = now - readAt;
        if (age > maxAgeNanos && failing(now)) {
            IOException f = lastFailure;
            throw new CatalogTooOldException("the shard catalog was last read " + age / 1_000_000_000L
                    + " s ago, over cluster.maxCatalogAgeSeconds (" + maxAgeNanos / 1_000_000_000L + " s)"
                    + (f == null ? "" : ": " + f.getMessage()), f, current, age / 1_000_000_000L);
        }
        return current;
    }

    /**
     * The background timer's step: starts a re-read when one is due (never before the first good
     * read, which stays a query's). Never blocks and never throws.
     */
    public void tick() {
        if (cached == null) return;
        try {
            startRefreshIfDue(clock.getAsLong());
        } catch (RuntimeException e) {
            log.debug("Catalog refresh tick failed: {}", e.toString());
        }
    }

    private void startRefreshIfDue(long now) {
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
    }

    /** Waits, at most {@code maxWait} of real time, for the background read in flight to finish. */
    private void awaitRefresh() {
        if (maxWaitNanos == 0) return;
        long end = System.nanoTime() + maxWaitNanos;
        synchronized (this) {
            while (runningSince.get() != IDLE) {
                long left = end - System.nanoTime();
                if (left <= 0) return;
                try {
                    TimeUnit.NANOSECONDS.timedWait(this, left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** Reads are failing: the last one failed, or the one in flight has run longer than refreshEvery. */
    private boolean failing(long now) {
        if (lastFailure != null) return true;
        long since = runningSince.get();
        return since != IDLE && now - since > refreshNanos;
    }

    private ProjectShards firstShards(long now) {
        if (attempted && now - attemptAt < refreshNanos) throw neverRead();
        if (!firstRead.tryLock()) throw neverRead(); // another query is reading it: do not wait
        try {
            if (cached != null) return cached;
            attempted = true;
            attemptAt = now;
            try {
                ProjectShards read = base.projectShards();
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
            notifyAll(); // queries waiting on it route with what they have
        }
        log.warn("The background read of the shard catalog has run {} s; starting another",
                (now - since) / 1_000_000_000L);
    }

    private void refresh(long token) {
        ProjectShards read = null;
        Exception failure = null;
        try {
            read = base.projectShards();
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
                notifyAll(); // queries waiting in awaitRefresh
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

    /**
     * {@link #ageSeconds} while reads are failing (what a query warns about); 0 while they are not,
     * however old the last read (an idle node's catalog is not stale); -1 before the first read.
     */
    public long staleSeconds() {
        if (cached == null) return -1;
        long now = clock.getAsLong();
        return failing(now) ? (now - readAt) / 1_000_000_000L : 0;
    }
}
