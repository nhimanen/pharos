package com.pharos.cluster.warm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * On-disk LRU cache of fixed-size object blocks, filled on demand by ranged reads.
 *
 * <p><b>Layout.</b> {@code <dir>/<first 2 hex of sha256(objectKey)>/<sha256(objectKey)>/<index>.blk},
 * written via {@code <index>.blk.tmp}, fsynced, then moved into place atomically. A
 * {@code block-size} marker records the block size the files were cut with. At startup the cache
 * refuses a non-empty dir without the marker (it may not be a cache dir), wipes the blocks of a
 * cache cut with another block size (block {@code i} would cover other bytes), deletes temp files
 * and blocks whose length can't be valid, and seeds the LRU by file mtime. Only paths that match the
 * layout are ever deleted. Empty hash dirs are only removed by that startup scan.
 *
 * <p><b>Leases and the cap.</b> Readers {@link #acquire} a {@link Lease}: a read-only
 * {@link MemorySegment} over the block that stays valid until the lease is closed. Only leased
 * blocks are pinned. Every other block may be evicted, least recently used first, whenever the
 * on-disk bytes exceed {@code maxBytes}. The cap is exceeded only by leased blocks and by in-flight
 * fetches (at most {@code (maxConcurrentFetches + prefetchThreads) × blockSize} of temp files).
 *
 * <p><b>Fetching.</b> Concurrent acquires of one missing block share one fetch (single-flight).
 * Acquires (a query's misses) run on a pool of {@code maxConcurrentFetches} threads; speculative
 * {@link #prefetch}es on a separate pool of {@link #prefetchThreads} threads, so a burst of prefetches
 * never queues a query's miss behind it. An acquire of a block whose prefetch is still queued
 * submits the same fetch to the acquire pool; whichever pool starts it first runs it. A prefetch is
 * skipped when the block could not be cached on disk (usable space below the block plus a margin). A
 * failed fetch fails every waiter and caches nothing; the next acquire retries. A block that can't
 * be written to the cache — the dir rejects writes, usable space is below the block plus a margin,
 * the fetch hits "no space left", or the fsync/move/map fails — is still served, from memory owned
 * by the cache, with a rate-limited WARN (the "uncached path"). A disk-full fetch is retried once
 * into memory; any other fetch error is not retried, so a network failure never costs a second GET.
 *
 * <p><b>Unmapping.</b> Each block lives in its own {@link Arena#ofShared()}, which is closed only
 * once the block has left the cache <em>and</em> its lease count is zero — never while a lease
 * could still read the segment. Arenas are closed and evicted files deleted outside the cache lock;
 * a fetch of a block whose old file is still being deleted waits before moving the new file in.
 *
 * <p><b>Operational constraints.</b> Nothing else may modify the cache dir while a cache runs on it
 * (one process per dir, enforced by the reader's sync lock): truncating a mapped block file makes a
 * read fault with SIGBUS / {@link InternalError}. On Windows a mapped file can't be deleted, so an
 * evicted block still mapped elsewhere would fail to delete; the cache targets Linux/macOS and only
 * logs such failures.
 */
public final class BlockCache implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(BlockCache.class);

    /** Minimum interval between two thrash (or two write-failure) WARNs. */
    static final long WARN_INTERVAL_MILLIS = 60_000;
    /** A caching fetch needs this much usable space beyond the block itself. */
    static final long SPACE_MARGIN_BYTES = 1L << 20;
    /** A hit rewrites the block's mtime (the LRU order seen by the next start) at most this often. */
    private static final long TOUCH_GRANULARITY_MILLIS = 1_000;
    static final String BLOCK_SIZE_MARKER = "block-size";
    private static final Pattern PREFIX = Pattern.compile("[0-9a-f]{2}");
    private static final Pattern HASH = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern BLOCK_NAME = Pattern.compile("(0|[1-9][0-9]{0,17})\\.blk");
    /** Our own temp ({@code N.blk.tmp}) and a fetcher's temp next to it ({@code N.blk.tmp<random>.part}). */
    private static final Pattern TEMP_NAME = Pattern.compile("(0|[1-9][0-9]{0,17})\\.blk\\.tmp([0-9]*\\.part)?");

    /** Fetches {@code length} bytes at {@code offset} of {@code key.objectKey()}. */
    @FunctionalInterface
    public interface BlockFetcher {
        /** Writes exactly the requested bytes to {@code target} (the cache's temp file). */
        void fetch(BlockKey key, long offset, long length, Path target) throws IOException;

        /**
         * Puts exactly {@code length} bytes into {@code target} (position 0 → {@code length}); used
         * when the block can't be cached on disk. The default spills through a temp file in
         * {@code java.io.tmpdir}, so it still needs disk space there. A fetcher that can stream the
         * bytes (a ranged GET body) should override it so a full disk can't fail the read.
         */
        default void fetchToMemory(BlockKey key, long offset, long length, ByteBuffer target) throws IOException {
            Path tmp = Files.createTempFile("pharos-block-", ".tmp");
            try {
                fetch(key, offset, length, tmp);
                readFully(tmp, length, target);
            } finally {
                Files.deleteIfExists(tmp);
            }
        }
    }

    private final Path dir;
    private final long maxBytes;
    private final int blockSize;
    private final LongSupplier clock;
    private final LongSupplier usableSpace;
    private final ExecutorService fetchers;
    /** Speculative prefetches only; never shared with acquires. */
    private final ExecutorService prefetchers;
    /** Test seam: makes mapping a freshly moved block fail. */
    volatile boolean failMappingForTest;

    private final Object lock = new Object();
    // All fields below are guarded by lock.
    /** Cached blocks in access order (eldest = least recently used). */
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(64, 0.75f, true);
    private final Map<String, Flight> inflight = new HashMap<>();
    /** Ids whose evicted file is being deleted outside the lock; a fetch of the id waits before its move. */
    private final Set<String> deleting = new HashSet<>();
    private long bytesOnDisk;
    private long hits;
    private long misses;
    private long bytesFetched;
    private long evictions;
    private long leasesOpen;
    private long writeFailures;
    private long thrashWarnings;
    private boolean warnedThrash;
    private long lastThrashWarn;
    private boolean warnedWrite;
    private long lastWriteWarn;
    private boolean closed;

    public BlockCache(Path dir, long maxBytes, int blockSize, int maxConcurrentFetches) throws IOException {
        this(dir, maxBytes, blockSize, maxConcurrentFetches, System::currentTimeMillis, null);
    }

    /**
     * Visible for tests: {@code clock} (epoch millis) drives WARN rate limits and block mtimes;
     * {@code usableSpace} (bytes) replaces the file store's usable space when non-null.
     */
    BlockCache(Path dir, long maxBytes, int blockSize, int maxConcurrentFetches, LongSupplier clock,
               LongSupplier usableSpace) throws IOException {
        if (blockSize <= 0) throw new IllegalArgumentException("blockSize must be positive: " + blockSize);
        if (maxBytes <= 0) throw new IllegalArgumentException("maxBytes must be positive: " + maxBytes);
        if (maxConcurrentFetches <= 0) {
            throw new IllegalArgumentException("maxConcurrentFetches must be positive: " + maxConcurrentFetches);
        }
        this.dir = dir.toAbsolutePath();
        this.maxBytes = maxBytes;
        this.blockSize = blockSize;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.usableSpace = usableSpace != null ? usableSpace : this::fileStoreUsableSpace;
        Files.createDirectories(this.dir);
        recover();
        this.fetchers = daemonPool(maxConcurrentFetches, "pharos-block-fetch-");
        this.prefetchers = daemonPool(prefetchThreads(maxConcurrentFetches), "pharos-block-prefetch-");
    }

    /** Threads of the prefetch pool: a quarter of the fetch slots, at least 2, at most all of them. */
    static int prefetchThreads(int maxConcurrentFetches) {
        return Math.min(maxConcurrentFetches, Math.max(2, maxConcurrentFetches / 4));
    }

    private static ExecutorService daemonPool(int threads, String prefix) {
        AtomicInteger n = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, prefix + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    // ---------------------------------------------------------------- acquire / release

    /**
     * Returns a lease on block {@code key}, fetching it with {@code fetcher} if it is not cached.
     * {@code blockLength} is the block's true length: {@code blockSize}, or less for an object's last
     * block. Blocks until the block is available.
     *
     * @throws IllegalArgumentException when {@code blockLength} is out of range, or differs from the
     *         length this block is already cached or being fetched with
     * @throws IOException when the fetch fails (nothing is cached; the next acquire retries)
     * @throws InterruptedIOException when the calling thread is interrupted while waiting; the shared
     *         fetch keeps running for the other waiters
     */
    public Lease acquire(BlockKey key, long blockLength, BlockFetcher fetcher) throws IOException {
        return acquire(key, objectHash(key.objectKey()), blockLength, fetcher);
    }

    /** {@link #acquire} with {@code hash} = {@link #objectHash} of the key's object, computed by the caller once. */
    Lease acquire(BlockKey key, String hash, long blockLength, BlockFetcher fetcher) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(fetcher, "fetcher");
        if (blockLength <= 0 || blockLength > blockSize) {
            throw new IllegalArgumentException("block length " + blockLength + " of block " + key.index() + " of '"
                    + key.objectKey() + "' is outside (0, " + blockSize + "]");
        }
        String id = hash + "/" + key.index();
        Entry hit = null;
        boolean touch = false;
        Flight flight = null;
        Cleanup cleanup = new Cleanup();
        try {
            synchronized (lock) {
                if (closed) throw new IllegalStateException("block cache " + dir + " is closed");
                Entry e = entries.get(id);
                if (e != null && e.verified && e.length != blockLength) {
                    throw lengthMismatch(key, blockLength, e.length);
                }
                if (e != null) {
                    if (e.length == blockLength && ensureMapped(e)) {
                        e.verified = true;
                        hits++;
                        e.leases++;
                        leasesOpen++;
                        hit = e;
                        long now = clock.getAsLong();
                        if (now - e.lastTouch >= TOUCH_GRANULARITY_MILLIS) {
                            e.lastTouch = now;
                            touch = true;
                        }
                    } else {
                        log.warn("cached block {} of '{}' at {} is invalid (expected {} bytes); fetching it again",
                                key.index(), key.objectKey(), e.file, blockLength);
                        drop(e, cleanup);
                    }
                }
                if (hit == null) {
                    flight = inflight.get(id);
                    if (flight != null && flight.length != blockLength) {
                        throw lengthMismatch(key, blockLength, flight.length);
                    }
                    misses++;
                    if (flight == null) {
                        flight = new Flight(blockLength, key, hash, id, fetcher);
                        inflight.put(id, flight);
                        flight.submitted = true;
                        fetchers.execute(flight::run);
                    } else if (!flight.submitted) {
                        // a queued prefetch: run it on the acquire pool too, whichever starts first wins
                        flight.submitted = true;
                        fetchers.execute(flight::run);
                    }
                    flight.waiters++;
                }
            }
        } finally {
            cleanup.run();
        }
        if (hit != null) {
            if (touch) setMtime(hit.file, hit.lastTouch);
            return new Lease(this, hit);
        }
        return await(flight, key);
    }

    /**
     * Starts fetching block {@code key} in the background if it is neither cached nor being fetched,
     * and returns without waiting. The returned future completes when the block is in the cache (or
     * was served by the fetch), immediately for a cached block, and exceptionally when the fetch fails
     * or the cache is closed; a failed prefetch caches nothing, so the next acquire retries. A
     * prefetch shares the single-flight fetch with concurrent acquires, takes no lease and does not
     * count as a hit or a miss. It runs on the prefetch pool, never on the acquire pool. When the block
     * could not be cached on disk (usable space below the block plus a margin) nothing is fetched and
     * the returned future is already complete.
     *
     * @throws IllegalArgumentException as for {@link #acquire}
     */
    public CompletableFuture<Void> prefetch(BlockKey key, long blockLength, BlockFetcher fetcher) {
        return prefetch(key, objectHash(key.objectKey()), blockLength, fetcher);
    }

    /** {@link #prefetch} with {@code hash} = {@link #objectHash} of the key's object, computed by the caller once. */
    CompletableFuture<Void> prefetch(BlockKey key, String hash, long blockLength, BlockFetcher fetcher) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(fetcher, "fetcher");
        if (blockLength <= 0 || blockLength > blockSize) {
            throw new IllegalArgumentException("block length " + blockLength + " of block " + key.index() + " of '"
                    + key.objectKey() + "' is outside (0, " + blockSize + "]");
        }
        String id = hash + "/" + key.index();
        synchronized (lock) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("block cache " + dir + " is closed"));
            }
            Entry e = entries.get(id);
            if (e != null && e.verified && e.length != blockLength) throw lengthMismatch(key, blockLength, e.length);
            // An unverified entry of another length is dropped and refetched by the next acquire.
            if (e != null) return CompletableFuture.completedFuture(null);
            Flight flight = inflight.get(id);
            if (flight != null && flight.length != blockLength) throw lengthMismatch(key, blockLength, flight.length);
            if (flight == null) {
                // Speculative: never worth the uncached path, whose memory nobody would read.
                if (lowOnSpace(blockLength)) return CompletableFuture.completedFuture(null);
                flight = new Flight(blockLength, key, hash, id, fetcher);
                inflight.put(id, flight);
                prefetchers.execute(flight::run);
            }
            return flight.done.thenApply(entry -> null);
        }
    }

    /**
     * Cancels the prefetch of block {@code key} when it is still queued and no acquire waits for it:
     * it will never fetch, and its future fails with a {@link java.util.concurrent.CancellationException}.
     *
     * @return true when it was cancelled; false when it is running, awaited, done or unknown
     */
    boolean cancelQueued(BlockKey key, String hash) {
        Flight f;
        synchronized (lock) {
            f = inflight.get(hash + "/" + key.index());
            if (f == null || f.submitted || f.waiters > 0 || !f.started.compareAndSet(false, true)) return false;
            inflight.remove(f.id);
            f.settled = true;
        }
        f.done.completeExceptionally(new java.util.concurrent.CancellationException(
                "prefetch of block " + key.index() + " of '" + key.objectKey() + "' cancelled"));
        return true;
    }

    /** Work that may throw an {@link IOException}. */
    @FunctionalInterface
    interface IoTask {
        void run() throws IOException;
    }

    /**
     * Runs {@code task} on the prefetch pool (speculative work that must never delay an acquire);
     * the future fails when the task does or the cache is closed.
     */
    CompletableFuture<Void> runOnPrefetchPool(IoTask task) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        try {
            prefetchers.execute(() -> {
                try {
                    task.run();
                    f.complete(null);
                } catch (Throwable t) {
                    f.completeExceptionally(t);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            f.completeExceptionally(new IllegalStateException("block cache " + dir + " is closed", e));
        }
        return f;
    }

    private static IllegalArgumentException lengthMismatch(BlockKey key, long asked, long known) {
        return new IllegalArgumentException("block " + key.index() + " of '" + key.objectKey()
                + "' requested with length " + asked + " but it is " + known + " bytes long");
    }

    private Lease await(Flight flight, BlockKey key) throws IOException {
        try {
            return new Lease(this, flight.done.get());
        } catch (InterruptedException ie) {
            Entry reserved = null;
            synchronized (lock) {
                if (flight.result != null) {
                    reserved = flight.result; // a lease was reserved for this thread
                } else if (!flight.settled) {
                    flight.waiters--; // the fetch completes without a reservation for this thread
                }
            }
            if (reserved != null) release(reserved); // give back the lease reserved for this thread
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted waiting for block " + key.index() + " of '"
                    + key.objectKey() + "'");
        } catch (ExecutionException ee) {
            Throwable c = ee.getCause();
            if (c instanceof Error err) throw err;
            throw new IOException("fetching block " + key.index() + " of '" + key.objectKey() + "' failed: "
                    + c.getMessage(), c);
        }
    }

    private void release(Entry e) {
        Cleanup cleanup = new Cleanup();
        synchronized (lock) {
            e.leases--;
            leasesOpen--;
            if (e.leases == 0) {
                if (e.removed) closeArena(e, cleanup);
                else if (bytesOnDisk > maxBytes) evictOverCap(true, cleanup);
            }
        }
        cleanup.run();
    }

    // ---------------------------------------------------------------- fetch

    private void runFetch(Flight flight, BlockKey key, String hash, String id, long length, BlockFetcher fetcher) {
        Entry entry = null;
        Throwable failure = null;
        try {
            entry = fetchBlock(key, hash, id, length, fetcher);
        } catch (Throwable t) {
            failure = t;
        }
        Cleanup cleanup = new Cleanup();
        boolean publish = false;
        synchronized (lock) {
            if (closed) { // close() already failed this flight
                if (entry != null) closeArena(entry, cleanup);
            } else {
                inflight.remove(id);
                flight.settled = true;
                publish = true;
                if (failure == null) {
                    bytesFetched += length;
                    // Reserve one lease per waiter before anyone can see the entry: eviction can't take
                    // it and an uncached entry's memory can't be freed before every waiter has read it.
                    entry.leases = flight.waiters;
                    leasesOpen += flight.waiters;
                    if (entry.cached) {
                        entries.put(id, entry);
                        bytesOnDisk += length;
                        evictOverCap(true, cleanup);
                    } else {
                        entry.removed = true; // never in the cache: free on last release
                        if (entry.leases == 0) closeArena(entry, cleanup);
                    }
                    flight.result = entry;
                }
            }
        }
        // Evicted files are gone before any waiter returns, so the cap holds on disk when they read.
        cleanup.run();
        if (publish) {
            if (failure != null) flight.done.completeExceptionally(failure);
            else flight.done.complete(entry);
        }
    }

    private Entry fetchBlock(BlockKey key, String hash, String id, long length, BlockFetcher fetcher)
            throws IOException {
        long offset = Math.multiplyExact(key.index(), (long) blockSize);
        Path file = blockFile(hash, key.index());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        String reason = prepareTemp(file, tmp, length);
        if (reason != null) {
            noteWriteFailure(key, reason);
            return fetchToMemory(key, id, offset, length, fetcher);
        }
        boolean moved = false;
        try {
            try {
                fetcher.fetch(key, offset, length, tmp);
            } catch (IOException e) {
                // Retry once into memory only when the disk is the problem; a network error is not retried.
                if (!isDiskFull(e) && !lowOnSpace(length)) throw e;
                deleteQuietly(tmp);
                noteWriteFailure(key, "disk full during the fetch: " + e);
                return fetchToMemory(key, id, offset, length, fetcher);
            }
            long got = Files.size(tmp);
            if (got != length) {
                throw new IOException("fetched block " + key.index() + " of '" + key.objectKey() + "' has " + got
                        + " bytes, expected " + length);
            }
            try {
                try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                    ch.force(true);
                }
                awaitNoPendingDelete(id);
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                moved = true;
            } catch (InterruptedIOException e) {
                throw e;
            } catch (IOException e) {
                noteWriteFailure(key, "cannot persist the block: " + e);
                return loadIntoMemory(id, tmp, length);
            }
            Entry e = new Entry(id, file, length, true);
            e.verified = true;
            e.lastTouch = clock.getAsLong();
            setMtime(file, e.lastTouch);
            try {
                if (failMappingForTest) throw new IOException("injected map failure");
                map(e);
                return e;
            } catch (IOException | RuntimeException ex) {
                noteWriteFailure(key, "cannot map the block: " + ex);
                try {
                    return loadIntoMemory(id, file, length);
                } finally {
                    deleteQuietly(file);
                }
            }
        } finally {
            if (!moved) deleteQuietly(tmp);
        }
    }

    /** Creates the temp file for a caching fetch; returns why the block can't be cached, or null. */
    private String prepareTemp(Path file, Path tmp, long length) {
        long free = usableSpace.getAsLong();
        if (free < length + SPACE_MARGIN_BYTES) {
            return "only " + free + " bytes usable, the block needs " + length + " plus a margin of "
                    + SPACE_MARGIN_BYTES;
        }
        try {
            Files.createDirectories(file.getParent());
            Files.deleteIfExists(tmp);
            Files.createFile(tmp);
            return null;
        } catch (IOException e) {
            return "the cache dir does not take writes: " + e;
        }
    }

    private boolean lowOnSpace(long length) {
        return usableSpace.getAsLong() < length + SPACE_MARGIN_BYTES;
    }

    /** ENOSPC / EDQUOT anywhere in the cause chain (the JDK reports them as message text only). */
    static boolean isDiskFull(Throwable t) {
        int depth = 0;
        for (Throwable c = t; c != null && depth < 16; c = c.getCause(), depth++) {
            String m = c.getMessage();
            if (m != null && (m.contains("No space left on device") || m.contains("Disk quota exceeded")
                    || m.contains("not enough space on the disk"))) {
                return true;
            }
        }
        return false;
    }

    private long fileStoreUsableSpace() {
        try {
            return Files.getFileStore(dir).getUsableSpace();
        } catch (IOException e) {
            return Long.MAX_VALUE; // unknown: let the write itself tell
        }
    }

    /** The uncached path: the fetcher fills off-heap memory owned by the entry's arena. */
    private static Entry fetchToMemory(BlockKey key, String id, long offset, long length, BlockFetcher fetcher)
            throws IOException {
        Arena arena = Arena.ofShared();
        try {
            MemorySegment seg = arena.allocate(length);
            ByteBuffer buf = seg.asByteBuffer();
            fetcher.fetchToMemory(key, offset, length, buf);
            if (buf.position() != length) {
                throw new IOException("fetched block " + key.index() + " of '" + key.objectKey() + "' has "
                        + buf.position() + " bytes, expected " + length);
            }
            Entry e = new Entry(id, null, length, false);
            e.arena = arena;
            e.segment = seg.asReadOnly();
            return e;
        } catch (IOException | RuntimeException | Error ex) {
            arena.close();
            throw ex;
        }
    }

    /** An uncached entry from bytes already on disk (a temp or block file the caller deletes). */
    private static Entry loadIntoMemory(String id, Path source, long length) throws IOException {
        Arena arena = Arena.ofShared();
        try {
            MemorySegment seg = arena.allocate(length);
            readFully(source, length, seg.asByteBuffer());
            Entry e = new Entry(id, null, length, false);
            e.arena = arena;
            e.segment = seg.asReadOnly();
            return e;
        } catch (IOException | RuntimeException ex) {
            arena.close();
            throw ex;
        }
    }

    private static void readFully(Path source, long length, ByteBuffer target) throws IOException {
        try (FileChannel ch = FileChannel.open(source, StandardOpenOption.READ)) {
            if (ch.size() != length) {
                throw new IOException(source + " has " + ch.size() + " bytes, expected " + length);
            }
            while (target.position() < length) {
                if (ch.read(target) < 0) throw new EOFException(source + " shrank while reading");
            }
        }
    }

    private void awaitNoPendingDelete(String id) throws InterruptedIOException {
        synchronized (lock) {
            while (deleting.contains(id)) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("interrupted waiting for an eviction of " + id);
                }
            }
        }
    }

    // ---------------------------------------------------------------- mapping and eviction

    private static void map(Entry e) throws IOException {
        Arena arena = Arena.ofShared();
        try (FileChannel ch = FileChannel.open(e.file, StandardOpenOption.READ)) {
            long size = ch.size();
            if (size != e.length) throw new IOException(e.file + " has " + size + " bytes, expected " + e.length);
            e.segment = ch.map(FileChannel.MapMode.READ_ONLY, 0, e.length, arena);
            e.arena = arena;
        } catch (IOException | RuntimeException ex) {
            arena.close();
            throw ex;
        }
    }

    /** Maps a seeded entry on first use, checking its on-disk length; false when it is unusable. Under lock. */
    private static boolean ensureMapped(Entry e) {
        if (e.segment != null) return true;
        try {
            map(e);
            return true;
        } catch (IOException | RuntimeException ex) {
            log.debug("cannot map cached block {}: {}", e.file, ex.toString());
            return false;
        }
    }

    /** Removes an invalid entry from the cache (not an eviction). Under lock. */
    private void drop(Entry e, Cleanup cleanup) {
        entries.remove(e.id);
        e.removed = true;
        bytesOnDisk -= e.length;
        deleteLater(e, cleanup);
        if (e.leases == 0) closeArena(e, cleanup);
    }

    /** Evicts unleased blocks, least recently used first, until at or under the cap. Under lock. */
    private void evictOverCap(boolean warn, Cleanup cleanup) {
        if (bytesOnDisk <= maxBytes) return;
        int evicted = 0;
        Iterator<Entry> it = entries.values().iterator();
        while (bytesOnDisk > maxBytes && it.hasNext()) {
            Entry e = it.next();
            if (e.leases > 0) continue;
            it.remove();
            e.removed = true;
            bytesOnDisk -= e.length;
            evictions++;
            evicted++;
            deleteLater(e, cleanup);
            closeArena(e, cleanup);
        }
        if (evicted > 0 && warn) maybeWarnThrash();
    }

    private void deleteLater(Entry e, Cleanup cleanup) {
        deleting.add(e.id);
        cleanup.deletes.add(e);
    }

    /** Detaches the entry's arena under the lock; the cleanup closes it after the lock is released. */
    private static void closeArena(Entry e, Cleanup cleanup) {
        if (e.arena != null) {
            cleanup.arenas.add(e.arena);
            e.arena = null;
            e.segment = null;
        }
    }

    private void maybeWarnThrash() {
        long now = clock.getAsLong();
        if (warnedThrash && now - lastThrashWarn < WARN_INTERVAL_MILLIS) return;
        warnedThrash = true;
        lastThrashWarn = now;
        thrashWarnings++;
        long total = hits + misses;
        String rate = total == 0 ? "n/a" : String.format(Locale.ROOT, "%.1f%%", 100.0 * hits / total);
        log.warn("block cache {} is evicting: the working set exceeds the cap of {} bytes; hit rate {}"
                        + " ({} hits, {} misses, {} evictions so far)",
                dir, maxBytes, rate, hits, misses, evictions);
    }

    private void noteWriteFailure(BlockKey key, String reason) {
        synchronized (lock) {
            writeFailures++;
            long now = clock.getAsLong();
            if (warnedWrite && now - lastWriteWarn < WARN_INTERVAL_MILLIS) return;
            warnedWrite = true;
            lastWriteWarn = now;
        }
        log.warn("fetched block {} of '{}' could not be cached in {} ({}); serving it uncached",
                key.index(), key.objectKey(), dir, reason);
    }

    /** Work collected under the lock and done after releasing it: close arenas, delete evicted files. */
    private final class Cleanup {
        final List<Arena> arenas = new ArrayList<>(2);
        final List<Entry> deletes = new ArrayList<>(2);

        void run() {
            try {
                for (Arena a : arenas) {
                    try {
                        a.close();
                    } catch (RuntimeException e) {
                        log.warn("cannot release a block mapping: {}", e.toString());
                    }
                }
                for (Entry e : deletes) deleteQuietly(e.file);
            } finally {
                arenas.clear();
                if (!deletes.isEmpty()) {
                    synchronized (lock) {
                        for (Entry e : deletes) deleting.remove(e.id);
                        lock.notifyAll();
                    }
                    deletes.clear();
                }
            }
        }
    }

    // ---------------------------------------------------------------- recovery

    private void recover() throws IOException {
        Path marker = dir.resolve(BLOCK_SIZE_MARKER);
        String want = Integer.toString(blockSize);
        if (!Files.exists(marker)) {
            try (Stream<Path> s = Files.list(dir)) {
                if (s.findAny().isPresent()) {
                    throw new IOException("block cache dir " + dir + " is not empty but has no " + BLOCK_SIZE_MARKER
                            + " marker; refusing to use it (point the cache at an empty or dedicated dir)");
                }
            }
            Files.writeString(marker, want);
        } else if (!Files.readString(marker, StandardCharsets.UTF_8).trim().equals(want)) {
            log.info("block cache {}: block size changed to {}; dropping blocks cut with the old size", dir, want);
            scan(true, new ArrayList<>());
            Files.writeString(marker, want);
        }
        List<Entry> seeds = new ArrayList<>();
        scan(false, seeds);
        seeds.sort(Comparator.<Entry>comparingLong(e -> e.lastTouch).thenComparing(e -> e.id));
        Cleanup cleanup = new Cleanup();
        synchronized (lock) {
            for (Entry e : seeds) {
                entries.put(e.id, e);
                bytesOnDisk += e.length;
            }
            evictOverCap(false, cleanup);
        }
        cleanup.run();
        if (!seeds.isEmpty()) {
            log.info("block cache {}: {} blocks, {} bytes on disk (cap {})", dir, entries.size(), bytesOnDisk, maxBytes);
        }
    }

    /**
     * Walks the layout dirs only. Deletes temp files and (with {@code wipe}, all; otherwise
     * invalid-length) block files; seeds the rest. Files outside the layout are left alone.
     */
    private void scan(boolean wipe, List<Entry> seeds) throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path d, BasicFileAttributes attrs) {
                if (d.equals(dir) || isPrefixDir(d) || isHashDir(d)) return FileVisitResult.CONTINUE;
                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.getParent() == null || !isHashDir(file.getParent())) return FileVisitResult.CONTINUE;
                String name = file.getFileName().toString();
                if (TEMP_NAME.matcher(name).matches()) {
                    Files.deleteIfExists(file);
                } else if (BLOCK_NAME.matcher(name).matches()) {
                    Entry e = wipe ? null : seed(file, attrs);
                    if (e == null) Files.deleteIfExists(file);
                    else seeds.add(e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                log.debug("block cache scan skipped {}: {}", file, exc.toString());
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) {
                if (!d.equals(dir)) { // only layout dirs get here
                    try {
                        Files.deleteIfExists(d);
                    } catch (DirectoryNotEmptyException ignored) {
                        // still holds blocks (or files that are not ours)
                    } catch (IOException e) {
                        log.debug("cannot remove empty cache dir {}: {}", d, e.toString());
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private boolean isPrefixDir(Path d) {
        return dir.equals(d.getParent()) && PREFIX.matcher(d.getFileName().toString()).matches();
    }

    private boolean isHashDir(Path d) {
        Path prefix = d.getParent();
        if (prefix == null || !isPrefixDir(prefix)) return false;
        String hash = d.getFileName().toString();
        return HASH.matcher(hash).matches() && hash.startsWith(prefix.getFileName().toString());
    }

    /** A seed entry for a block file with a possible length; null when the length can't be valid. */
    private Entry seed(Path file, BasicFileAttributes attrs) {
        long size = attrs.size();
        if (size <= 0 || size > blockSize) return null;
        String name = file.getFileName().toString();
        long index = Long.parseLong(name.substring(0, name.length() - ".blk".length()));
        Entry e = new Entry(file.getParent().getFileName() + "/" + index, file, size, true);
        e.lastTouch = attrs.lastModifiedTime().toMillis();
        return e;
    }

    // ---------------------------------------------------------------- stats / close / helpers

    public WarmStats stats() {
        synchronized (lock) {
            return new WarmStats(hits, misses, bytesFetched, evictions, bytesOnDisk, leasesOpen, writeFailures,
                    thrashWarnings);
        }
    }

    /**
     * Drops block {@code key} from the cache (its bytes failed a check by the reader); the next acquire
     * fetches it again. Open leases keep their mapping until they close.
     */
    void invalidate(BlockKey key, String hash) {
        Cleanup cleanup = new Cleanup();
        synchronized (lock) {
            Entry e = entries.get(hash + "/" + key.index());
            if (e != null) drop(e, cleanup);
        }
        cleanup.run();
    }

    /** True when block {@code key} is in the cache now (on disk); in-flight fetches don't count. */
    public boolean isCached(BlockKey key) {
        String id = objectHash(key.objectKey()) + "/" + key.index();
        synchronized (lock) {
            return entries.containsKey(id);
        }
    }

    public Path dir() { return dir; }

    public long maxBytes() { return maxBytes; }

    public int blockSize() { return blockSize; }

    /**
     * Stops fetching and fails pending acquires. Unleased mappings are released now, leased ones when
     * their lease closes. Block files stay on disk for the next start.
     */
    @Override
    public void close() {
        Cleanup cleanup = new Cleanup();
        synchronized (lock) {
            if (closed) return;
            closed = true;
            for (Flight f : inflight.values()) {
                f.done.completeExceptionally(new IOException("block cache " + dir + " is closed"));
            }
            inflight.clear();
            for (Entry e : entries.values()) {
                e.removed = true;
                if (e.leases == 0) closeArena(e, cleanup);
            }
            entries.clear();
            bytesOnDisk = 0;
        }
        cleanup.run();
        fetchers.shutdownNow();
        prefetchers.shutdownNow();
    }

    private Path blockFile(String hash, long index) {
        return dir.resolve(hash.substring(0, 2)).resolve(hash).resolve(index + ".blk");
    }

    /** The hash naming an object's block dir: lower-case hex SHA-256 of the object key. */
    static String objectHash(String objectKey) {
        return sha256(objectKey);
    }

    private static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void setMtime(Path file, long millis) {
        try {
            Files.setLastModifiedTime(file, FileTime.fromMillis(millis));
        } catch (IOException e) {
            log.debug("cannot touch {}: {}", file, e.toString());
        }
    }

    private static void deleteQuietly(Path file) {
        if (file == null) return;
        try {
            Files.deleteIfExists(file);
        } catch (NoSuchFileException ignored) {
            // already gone
        } catch (IOException e) {
            log.debug("cannot delete {}: {}", file, e.toString());
        }
    }

    // ---------------------------------------------------------------- types

    /** One block; fields other than the finals are guarded by the cache lock. */
    private static final class Entry {
        final String id;
        /** The block file; null for an uncached (memory-only) entry. */
        final Path file;
        final long length;
        /** False for a block served from memory because it could not be written to the cache dir. */
        final boolean cached;
        Arena arena;
        MemorySegment segment;
        int leases;
        /** Out of the cache (evicted, dropped, closed, or never cached): the arena closes at zero leases. */
        boolean removed;
        /** Its length is known good (fetched, or matched an acquire); a seeded file's is not, yet. */
        boolean verified;
        long lastTouch;

        Entry(String id, Path file, long length, boolean cached) {
            this.id = id;
            this.file = file;
            this.length = length;
            this.cached = cached;
        }
    }

    /**
     * A fetch in progress; {@code waiters} (under the lock) is how many leases to reserve on success.
     * It may be submitted to both pools (a queued prefetch that an acquire needs); {@link #run} fetches
     * only on the first call.
     */
    private final class Flight {
        final CompletableFuture<Entry> done = new CompletableFuture<>();
        final long length;
        final BlockKey key;
        final String hash;
        final String id;
        final BlockFetcher fetcher;
        private final AtomicBoolean started = new AtomicBoolean();
        /** On the acquire pool (under the lock). */
        boolean submitted;
        int waiters;
        /** Set when the fetch has finished (under the lock), before {@link #done} completes. */
        boolean settled;
        /** The fetched entry, holding one reserved lease per waiter; null until settled successfully. */
        Entry result;

        Flight(long length, BlockKey key, String hash, String id, BlockFetcher fetcher) {
            this.length = length;
            this.key = key;
            this.hash = hash;
            this.id = id;
            this.fetcher = fetcher;
        }

        void run() {
            if (started.compareAndSet(false, true)) runFetch(this, key, hash, id, length, fetcher);
        }
    }

    /** A pin on one block. {@link #segment()} is valid until {@link #close()}; close is idempotent. */
    public static final class Lease implements AutoCloseable {
        private final BlockCache cache;
        private final Entry entry;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(BlockCache cache, Entry entry) {
            this.cache = cache;
            this.entry = entry;
        }

        /** Read-only view of the block's bytes. */
        public MemorySegment segment() {
            if (closed.get()) throw new IllegalStateException("lease is closed");
            return entry.segment;
        }

        /** The block's length in bytes. */
        public long length() { return entry.length; }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) cache.release(entry);
        }
    }
}
