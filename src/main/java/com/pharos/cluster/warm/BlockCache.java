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
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * On-disk LRU cache of fixed-size object blocks, filled on demand by ranged reads.
 *
 * <p>Layout: {@code <dir>/<first 2 hex of sha256(objectKey)>/<sha256(objectKey)>/<index>.blk}, written
 * via {@code <index>.blk.tmp} and an atomic move. A {@code block-size} marker records the block size
 * the files were cut with; a cache cut with another size is wiped at startup (block {@code i} would
 * otherwise cover different bytes).
 *
 * <p>Readers {@link #acquire} a {@link Lease}: a read-only {@link MemorySegment} over the block that
 * stays valid until the lease is closed. Only leased blocks are pinned. Every other block may be
 * evicted, least recently used first, whenever the on-disk bytes exceed {@code maxBytes}. The cap
 * is exceeded only by leased blocks and by in-flight fetches (at most
 * {@code maxConcurrentFetches × blockSize} of temp files).
 *
 * <p>Concurrent acquires of one missing block share one fetch (single-flight). A failed fetch fails
 * every waiter and caches nothing; the next acquire retries. A block that was fetched but could not
 * be written to the cache dir (read-only, disk full on move) is still served, from memory, with a
 * WARN.
 *
 * <p>Unmapping: each cached block is mapped into its own {@link Arena#ofShared()}, which is closed
 * only once the block has left the cache <em>and</em> its lease count is zero — never while a lease
 * could still be reading the segment.
 */
public final class BlockCache implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(BlockCache.class);

    /** Minimum interval between two thrash (or two write-failure) WARNs. */
    static final long WARN_INTERVAL_MILLIS = 60_000;
    /** A hit rewrites the block's mtime (the LRU order seen by the next start) at most this often. */
    private static final long TOUCH_GRANULARITY_MILLIS = 1_000;
    private static final String BLOCK_SIZE_MARKER = "block-size";
    private static final Pattern HASH = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern BLOCK_NAME = Pattern.compile("(0|[1-9][0-9]{0,18})\\.blk");

    /** Fetches {@code length} bytes at {@code offset} of {@code key.objectKey()} into {@code target}. */
    @FunctionalInterface
    public interface BlockFetcher {
        void fetch(BlockKey key, long offset, long length, Path target) throws IOException;
    }

    private final Path dir;
    private final long maxBytes;
    private final int blockSize;
    private final LongSupplier clock;
    private final ExecutorService fetchers;

    private final Object lock = new Object();
    // All fields below are guarded by lock.
    /** Cached blocks in access order (eldest = least recently used). */
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(64, 0.75f, true);
    private final Map<String, Flight> inflight = new HashMap<>();
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
        this(dir, maxBytes, blockSize, maxConcurrentFetches, System::currentTimeMillis);
    }

    /** Visible for tests: {@code clock} (epoch millis) drives WARN rate limits and block mtimes. */
    BlockCache(Path dir, long maxBytes, int blockSize, int maxConcurrentFetches, LongSupplier clock)
            throws IOException {
        if (blockSize <= 0) throw new IllegalArgumentException("blockSize must be positive: " + blockSize);
        if (maxBytes <= 0) throw new IllegalArgumentException("maxBytes must be positive: " + maxBytes);
        if (maxConcurrentFetches <= 0) {
            throw new IllegalArgumentException("maxConcurrentFetches must be positive: " + maxConcurrentFetches);
        }
        this.dir = dir.toAbsolutePath();
        this.maxBytes = maxBytes;
        this.blockSize = blockSize;
        this.clock = Objects.requireNonNull(clock, "clock");
        Files.createDirectories(this.dir);
        recover();
        AtomicInteger n = new AtomicInteger();
        this.fetchers = Executors.newFixedThreadPool(maxConcurrentFetches, r -> {
            Thread t = new Thread(r, "pharos-block-fetch-" + n.incrementAndGet());
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
     * @throws IOException when the fetch fails (nothing is cached; the next acquire retries)
     * @throws InterruptedIOException when the calling thread is interrupted while waiting; the shared
     *         fetch keeps running for the other waiters
     */
    public Lease acquire(BlockKey key, long blockLength, BlockFetcher fetcher) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(fetcher, "fetcher");
        if (blockLength <= 0 || blockLength > blockSize) {
            throw new IllegalArgumentException("block length " + blockLength + " of block " + key.index() + " of '"
                    + key.objectKey() + "' is outside (0, " + blockSize + "]");
        }
        String hash = sha256(key.objectKey());
        String id = hash + "/" + key.index();
        Entry hit = null;
        boolean touch = false;
        Flight flight = null;
        synchronized (lock) {
            if (closed) throw new IllegalStateException("block cache " + dir + " is closed");
            Entry e = entries.get(id);
            if (e != null) {
                if (e.length == blockLength && ensureMapped(e)) {
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
                    drop(e);
                }
            }
            if (hit == null) {
                misses++;
                flight = inflight.get(id);
                if (flight == null) {
                    flight = new Flight();
                    inflight.put(id, flight);
                    Flight f = flight;
                    fetchers.execute(() -> runFetch(f, key, hash, id, blockLength, fetcher));
                }
                flight.waiters++;
            }
        }
        if (hit != null) {
            if (touch) setMtime(hit.file, hit.lastTouch);
            return new Lease(this, hit);
        }
        return await(flight, key);
    }

    private Lease await(Flight flight, BlockKey key) throws IOException {
        try {
            return new Lease(this, flight.done.get());
        } catch (InterruptedException ie) {
            synchronized (lock) {
                if (!flight.done.isDone()) {
                    flight.waiters--; // the fetch completes without a reservation for this thread
                } else if (!flight.done.isCompletedExceptionally()) {
                    release(flight.done.join()); // give back the lease reserved for this thread
                }
            }
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
        synchronized (lock) {
            e.leases--;
            leasesOpen--;
            if (e.leases == 0) {
                if (e.removed) closeArena(e);
                else if (bytesOnDisk > maxBytes) evictOverCap(true);
            }
        }
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
        synchronized (lock) {
            if (closed) { // close() already failed this flight
                if (entry != null) closeArena(entry);
                return;
            }
            inflight.remove(id);
            if (failure != null) {
                flight.done.completeExceptionally(failure);
                return;
            }
            bytesFetched += length;
            // Reserve one lease per waiter before anyone can see the entry: eviction can't take it
            // and an uncached entry's memory can't be freed before every waiter has read it.
            entry.leases = flight.waiters;
            leasesOpen += flight.waiters;
            if (entry.cached) {
                entries.put(id, entry);
                bytesOnDisk += length;
                evictOverCap(true);
            } else {
                entry.removed = true; // never in the cache: free on last release
                if (entry.leases == 0) closeArena(entry);
            }
            flight.done.complete(entry);
        }
    }

    private Entry fetchBlock(BlockKey key, String hash, String id, long length, BlockFetcher fetcher)
            throws IOException {
        Path file = blockFile(hash, key.index());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Path target;
        boolean toCache;
        try {
            Files.createDirectories(file.getParent());
            Files.deleteIfExists(tmp);
            Files.createFile(tmp); // probe: does the cache dir take writes?
            target = tmp;
            toCache = true;
        } catch (IOException e) {
            noteWriteFailure(key, e);
            target = Files.createTempFile("pharos-block-", ".tmp");
            toCache = false;
        }
        boolean moved = false;
        try {
            fetcher.fetch(key, Math.multiplyExact(key.index(), (long) blockSize), length, target);
            long got = Files.size(target);
            if (got != length) {
                throw new IOException("fetched block " + key.index() + " of '" + key.objectKey() + "' has " + got
                        + " bytes, expected " + length);
            }
            if (toCache) {
                try {
                    Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    moved = true;
                } catch (IOException e) {
                    noteWriteFailure(key, e);
                }
            }
            if (moved) {
                Entry e = new Entry(id, file, length, true);
                e.lastTouch = clock.getAsLong();
                setMtime(file, e.lastTouch);
                try {
                    map(e);
                } catch (IOException | RuntimeException ex) {
                    deleteQuietly(file);
                    throw ex;
                }
                return e;
            }
            return loadIntoMemory(id, target, length);
        } finally {
            if (!moved) deleteQuietly(target);
        }
    }

    /** An uncached entry: the fetched bytes copied into off-heap memory owned by the entry's arena. */
    private static Entry loadIntoMemory(String id, Path source, long length) throws IOException {
        Entry e = new Entry(id, source, length, false);
        Arena arena = Arena.ofShared();
        try (FileChannel ch = FileChannel.open(source, StandardOpenOption.READ)) {
            MemorySegment seg = arena.allocate(length);
            ByteBuffer buf = seg.asByteBuffer();
            while (buf.hasRemaining()) {
                if (ch.read(buf) < 0) throw new EOFException("fetched block " + source + " shrank while reading");
            }
            e.arena = arena;
            e.segment = seg.asReadOnly();
            return e;
        } catch (IOException | RuntimeException ex) {
            arena.close();
            throw ex;
        }
    }

    // ---------------------------------------------------------------- mapping and eviction (under lock)

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

    /** Maps a seeded entry on first use, checking its on-disk length; false when it is unusable. */
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

    /** Removes an invalid entry from the cache and deletes its file (not an eviction). */
    private void drop(Entry e) {
        entries.remove(e.id);
        e.removed = true;
        bytesOnDisk -= e.length;
        deleteQuietly(e.file);
        if (e.leases == 0) closeArena(e);
    }

    /** Evicts unleased blocks, least recently used first, until at or under the cap. */
    private void evictOverCap(boolean warn) {
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
            deleteQuietly(e.file);
            closeArena(e);
        }
        if (evicted > 0 && warn) maybeWarnThrash();
    }

    private void maybeWarnThrash() {
        long now = clock.getAsLong();
        if (warnedThrash && now - lastThrashWarn < WARN_INTERVAL_MILLIS) return;
        warnedThrash = true;
        lastThrashWarn = now;
        thrashWarnings++;
        long total = hits + misses;
        String rate = total == 0 ? "n/a" : String.format(java.util.Locale.ROOT, "%.1f%%", 100.0 * hits / total);
        log.warn("block cache {} is evicting: the working set exceeds the cap of {} bytes; hit rate {}"
                        + " ({} hits, {} misses, {} evictions so far)",
                dir, maxBytes, rate, hits, misses, evictions);
    }

    private void noteWriteFailure(BlockKey key, IOException e) {
        synchronized (lock) {
            writeFailures++;
            long now = clock.getAsLong();
            if (warnedWrite && now - lastWriteWarn < WARN_INTERVAL_MILLIS) return;
            warnedWrite = true;
            lastWriteWarn = now;
        }
        log.warn("fetched block {} of '{}' could not be cached in {} ({}); serving it uncached",
                key.index(), key.objectKey(), dir, e.toString());
    }

    private static void closeArena(Entry e) {
        if (e.arena != null) {
            e.arena.close();
            e.arena = null;
            e.segment = null;
        }
    }

    // ---------------------------------------------------------------- recovery

    private void recover() throws IOException {
        if (!blockSizeMatches()) {
            wipeBlocks();
            Files.writeString(dir.resolve(BLOCK_SIZE_MARKER), Integer.toString(blockSize));
        }
        List<Entry> seeds = new ArrayList<>();
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String name = file.getFileName().toString();
                if (name.endsWith(".tmp") || name.endsWith(".part")) {
                    Files.deleteIfExists(file);
                } else if (name.endsWith(".blk")) {
                    Entry e = seed(file, attrs);
                    if (e == null) Files.deleteIfExists(file);
                    else seeds.add(e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException exc) {
                if (!d.equals(dir)) {
                    try {
                        Files.deleteIfExists(d);
                    } catch (DirectoryNotEmptyException ignored) {
                        // still holds blocks
                    } catch (IOException e) {
                        log.debug("cannot remove empty cache dir {}: {}", d, e.toString());
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        seeds.sort(Comparator.<Entry>comparingLong(e -> e.lastTouch).thenComparing(e -> e.id));
        for (Entry e : seeds) {
            entries.put(e.id, e);
            bytesOnDisk += e.length;
        }
        evictOverCap(false);
        if (!seeds.isEmpty()) {
            log.info("block cache {}: {} blocks, {} bytes on disk (cap {})", dir, entries.size(), bytesOnDisk, maxBytes);
        }
    }

    /** A seed entry for a block file in the expected place with a possible length; null otherwise. */
    private Entry seed(Path file, BasicFileAttributes attrs) {
        Path hashDir = file.getParent();
        Path prefixDir = hashDir == null ? null : hashDir.getParent();
        if (prefixDir == null || !dir.equals(prefixDir.getParent())) return null;
        String hash = hashDir.getFileName().toString();
        if (!HASH.matcher(hash).matches() || !prefixDir.getFileName().toString().equals(hash.substring(0, 2))) {
            return null;
        }
        String name = file.getFileName().toString();
        if (!BLOCK_NAME.matcher(name).matches()) return null;
        long size = attrs.size();
        if (size <= 0 || size > blockSize) return null;
        long index;
        try {
            index = Long.parseLong(name.substring(0, name.length() - ".blk".length()));
        } catch (NumberFormatException e) {
            return null;
        }
        Entry e = new Entry(hash + "/" + index, file, size, true);
        e.lastTouch = attrs.lastModifiedTime().toMillis();
        return e;
    }

    private boolean blockSizeMatches() {
        Path marker = dir.resolve(BLOCK_SIZE_MARKER);
        try {
            return Files.readString(marker, StandardCharsets.UTF_8).trim().equals(Integer.toString(blockSize));
        } catch (IOException e) {
            return false;
        }
    }

    /** Deletes every block and temp file: they were cut with another (or an unknown) block size. */
    private void wipeBlocks() throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String name = file.getFileName().toString();
                if (name.endsWith(".blk") || name.endsWith(".tmp")) Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    // ---------------------------------------------------------------- stats / close / helpers

    public WarmStats stats() {
        synchronized (lock) {
            return new WarmStats(hits, misses, bytesFetched, evictions, bytesOnDisk, leasesOpen, writeFailures,
                    thrashWarnings);
        }
    }

    public long maxBytes() { return maxBytes; }

    public int blockSize() { return blockSize; }

    /**
     * Stops fetching and fails pending acquires. Unleased mappings are released now, leased ones when
     * their lease closes. Block files stay on disk for the next start.
     */
    @Override
    public void close() {
        synchronized (lock) {
            if (closed) return;
            closed = true;
            for (Flight f : inflight.values()) {
                f.done.completeExceptionally(new IOException("block cache " + dir + " is closed"));
            }
            inflight.clear();
            for (Entry e : entries.values()) {
                e.removed = true;
                if (e.leases == 0) closeArena(e);
            }
            entries.clear();
            bytesOnDisk = 0;
        }
        fetchers.shutdownNow();
    }

    private Path blockFile(String hash, long index) {
        return dir.resolve(hash.substring(0, 2)).resolve(hash).resolve(index + ".blk");
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
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            log.debug("cannot delete {}: {}", file, e.toString());
        }
    }

    // ---------------------------------------------------------------- types

    /** One block; fields other than the finals are guarded by the cache lock. */
    private static final class Entry {
        final String id;
        final Path file;
        final long length;
        /** False for a block served from memory because it could not be written to the cache dir. */
        final boolean cached;
        Arena arena;
        MemorySegment segment;
        int leases;
        /** Out of the cache (evicted, dropped, closed, or never cached): the arena closes at zero leases. */
        boolean removed;
        long lastTouch;

        Entry(String id, Path file, long length, boolean cached) {
            this.id = id;
            this.file = file;
            this.length = length;
            this.cached = cached;
        }
    }

    /** A fetch in progress; {@code waiters} (under the lock) is how many leases to reserve on success. */
    private static final class Flight {
        final CompletableFuture<Entry> done = new CompletableFuture<>();
        int waiters;
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
