package com.pharos.cluster.routing;

import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean refreshing = new AtomicBoolean();

    private volatile List<ShardId> cached;
    private volatile long readAt;          // last good read
    private volatile long attemptAt;       // last attempt, good or not
    private volatile boolean attempted;
    private volatile IOException lastFailure;

    public CachedCatalog(ShardCatalog base, Duration refreshEvery, Duration maxAge, LongSupplier nanoClock,
                         Executor refresher) {
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
        if (now - attemptAt >= refreshNanos && refreshing.compareAndSet(false, true)) {
            attemptAt = now;
            try {
                refresher.execute(this::refresh);
            } catch (RuntimeException e) {
                refreshing.set(false);
            }
        }
        long age = now - readAt;
        if (age > maxAgeNanos) {
            throw new CatalogUnavailableException("the shard catalog was last read " + age / 1_000_000_000L
                    + " s ago, over cluster.maxCatalogAgeSeconds (" + maxAgeNanos / 1_000_000_000L + " s)"
                    + (lastFailure == null ? "" : ": " + lastFailure.getMessage()), lastFailure);
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

    private void refresh() {
        try {
            List<ShardId> read = List.copyOf(base.shards());
            readAt = clock.getAsLong();
            lastFailure = null;
            cached = read;
        } catch (IOException | RuntimeException e) {
            lastFailure = e instanceof IOException io ? io : new IOException(e.toString(), e);
            log.warn("Reading the shard catalog failed; routing with the one read {} s ago (retrying in {} s): {}",
                    ageSeconds(), refreshNanos / 1_000_000_000L, e.toString());
        } finally {
            refreshing.set(false);
        }
    }

    /** Seconds since the last good read; -1 before the first. */
    public long ageSeconds() {
        return cached == null ? -1 : (clock.getAsLong() - readAt) / 1_000_000_000L;
    }
}
