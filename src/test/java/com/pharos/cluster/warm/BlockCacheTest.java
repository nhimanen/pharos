package com.pharos.cluster.warm;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockCacheTest {

    private static final int BS = 1024;
    private static final String OBJ = "shards/p/objects/abc123";

    @TempDir Path dir;
    private final AtomicLong now = new AtomicLong(1_000_000_000_000L);
    private final AtomicLong usable = new AtomicLong(Long.MAX_VALUE);
    private final List<BlockCache> open = new ArrayList<>();
    private final List<ListAppender<ILoggingEvent>> appenders = new ArrayList<>();

    @AfterEach
    void closeCaches() {
        for (BlockCache c : open) c.close();
        Logger logger = (Logger) LoggerFactory.getLogger(BlockCache.class);
        appenders.forEach(logger::detachAppender);
        dir.toFile().setWritable(true);
    }

    private BlockCache cache(long maxBlocks) throws IOException {
        BlockCache c = new BlockCache(dir, maxBlocks * BS, BS, 4, now::get, usable::get);
        open.add(c);
        return c;
    }

    /** Deterministic content: byte i of the object is a function of the key and i. */
    private static byte expected(BlockKey key, long objectOffset) {
        return (byte) (key.objectKey().hashCode() * 31 + objectOffset * 7);
    }

    private static final class FakeFetcher implements BlockCache.BlockFetcher {
        final AtomicInteger calls = new AtomicInteger();
        volatile CountDownLatch gate;
        volatile IOException failWith;
        volatile int shortBy;
        /** Fail the on-disk fetch the way a full disk does, after a partial write. */
        volatile boolean enospc;
        final AtomicInteger memCalls = new AtomicInteger();

        @Override
        public void fetchToMemory(BlockKey key, long offset, long length, java.nio.ByteBuffer target) {
            memCalls.incrementAndGet();
            for (int i = 0; i < length; i++) target.put(expected(key, offset + i));
        }

        @Override
        public void fetch(BlockKey key, long offset, long length, Path target) throws IOException {
            calls.incrementAndGet();
            if (enospc) {
                Files.write(target, new byte[(int) length / 3]);
                throw new java.nio.file.FileSystemException(target.toString(), null, "No space left on device");
            }
            CountDownLatch g = gate;
            if (g != null) {
                try {
                    if (!g.await(10, TimeUnit.SECONDS)) throw new IOException("gate timeout");
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
            byte[] b = new byte[(int) length - shortBy];
            for (int i = 0; i < b.length; i++) b[i] = expected(key, offset + i);
            if (failWith != null) {
                Files.write(target, java.util.Arrays.copyOf(b, b.length / 2)); // partial write, then fail
                throw failWith;
            }
            Files.write(target, b);
        }
    }

    private static void assertContent(BlockCache.Lease lease, BlockKey key, long blockLength) {
        MemorySegment seg = lease.segment();
        assertThat(lease.length()).isEqualTo(blockLength);
        assertThat(seg.byteSize()).isEqualTo(blockLength);
        assertThat(seg.isReadOnly()).isTrue();
        long base = key.index() * BS;
        for (long i = 0; i < blockLength; i += 97) {
            assertThat(seg.get(ValueLayout.JAVA_BYTE, i)).isEqualTo(expected(key, base + i));
        }
        assertThat(seg.get(ValueLayout.JAVA_BYTE, blockLength - 1)).isEqualTo(expected(key, base + blockLength - 1));
    }

    private Path blockFile(BlockKey key) throws Exception {
        String h = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(key.objectKey().getBytes(StandardCharsets.UTF_8)));
        return dir.resolve(h.substring(0, 2)).resolve(h).resolve(key.index() + ".blk");
    }

    private void touchAll(BlockCache c, FakeFetcher f, int from, int to) throws IOException {
        for (int i = from; i < to; i++) {
            now.addAndGet(10_000);
            try (BlockCache.Lease l = c.acquire(new BlockKey(OBJ, i), BS, f)) {
                assertContent(l, new BlockKey(OBJ, i), BS);
            }
        }
    }

    private List<Path> filesUnder(Path root) throws IOException {
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(Files::isRegularFile)
                    .filter(f -> !f.getFileName().toString().equals("block-size")).toList();
        }
    }

    @Test
    void missFetchesOnceThenHits() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        BlockKey k = new BlockKey(OBJ, 3);
        try (BlockCache.Lease l = c.acquire(k, BS, f)) {
            assertContent(l, k, BS);
            assertThat(c.stats().leasesOpen()).isEqualTo(1);
        }
        try (BlockCache.Lease l = c.acquire(k, BS, f)) {
            assertContent(l, k, BS);
        }
        assertThat(f.calls).hasValue(1);
        WarmStats s = c.stats();
        assertThat(s.hits()).isEqualTo(1);
        assertThat(s.misses()).isEqualTo(1);
        assertThat(s.bytesFetched()).isEqualTo(BS);
        assertThat(s.bytesOnDisk()).isEqualTo(BS);
        assertThat(s.leasesOpen()).isZero();
        assertThat(blockFile(k)).exists().hasSize(BS);
        assertThat(filesUnder(dir)).containsExactly(blockFile(k));
    }

    @Test
    void shortLastBlockIsServedAtItsTrueLength() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        BlockKey k = new BlockKey(OBJ, 5);
        try (BlockCache.Lease l = c.acquire(k, 100, f)) {
            assertContent(l, k, 100);
        }
        try (BlockCache.Lease l = c.acquire(k, 100, f)) {
            assertContent(l, k, 100);
        }
        assertThat(f.calls).hasValue(1);
        assertThatThrownBy(() -> c.acquire(k, BS + 1, f)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> c.acquire(k, 0, f)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void concurrentMissesSingleFlight() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        f.gate = new CountDownLatch(1);
        BlockKey k = new BlockKey(OBJ, 0);
        int n = 16;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            List<Future<BlockCache.Lease>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) futures.add(pool.submit(() -> c.acquire(k, BS, f)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (c.stats().misses() < n && System.nanoTime() < deadline) Thread.onSpinWait();
            assertThat(c.stats().misses()).isEqualTo(n);
            f.gate.countDown();
            List<BlockCache.Lease> leases = new ArrayList<>();
            for (Future<BlockCache.Lease> fu : futures) leases.add(fu.get(10, TimeUnit.SECONDS));
            assertThat(f.calls).hasValue(1);
            assertThat(c.stats().leasesOpen()).isEqualTo(n);
            for (BlockCache.Lease l : leases) {
                assertContent(l, k, BS);
                l.close();
            }
            assertThat(c.stats().leasesOpen()).isZero();
            assertThat(c.stats().bytesFetched()).isEqualTo(BS);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void evictsLeastRecentlyUsedOverCap() throws Exception {
        BlockCache c = cache(4);
        FakeFetcher f = new FakeFetcher();
        touchAll(c, f, 0, 6);
        assertThat(blockFile(new BlockKey(OBJ, 0))).doesNotExist();
        assertThat(blockFile(new BlockKey(OBJ, 1))).doesNotExist();
        for (int i = 2; i < 6; i++) assertThat(blockFile(new BlockKey(OBJ, i))).exists();
        assertThat(c.stats().bytesOnDisk()).isLessThanOrEqualTo(4L * BS);
        assertThat(c.stats().evictions()).isEqualTo(2);
        touchAll(c, f, 2, 3);
        assertThat(f.calls).hasValue(6); // block 2 was a hit
        touchAll(c, f, 0, 1);
        assertThat(f.calls).hasValue(7); // block 0 is fetched again
        assertThat(blockFile(new BlockKey(OBJ, 3))).doesNotExist(); // 3 was the LRU after 2 was touched
    }

    @Test
    void neverEvictsLeasedBlock() throws Exception {
        BlockCache c = cache(2);
        FakeFetcher f = new FakeFetcher();
        List<BlockCache.Lease> leases = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            now.addAndGet(10_000);
            leases.add(c.acquire(new BlockKey(OBJ, i), BS, f));
        }
        assertThat(c.stats().bytesOnDisk()).isEqualTo(3L * BS); // over cap only while leased
        for (int i = 0; i < 3; i++) {
            assertThat(blockFile(new BlockKey(OBJ, i))).exists();
            assertContent(leases.get(i), new BlockKey(OBJ, i), BS);
        }
        for (BlockCache.Lease l : leases) l.close();
        assertThat(c.stats().bytesOnDisk()).isLessThanOrEqualTo(2L * BS);
        assertThat(blockFile(new BlockKey(OBJ, 0))).doesNotExist();
        assertThat(c.stats().leasesOpen()).isZero();
    }

    @Test
    void segmentOfEvictedBlockStaysValidUntilLeaseCloses() throws Exception {
        BlockCache c = cache(4);
        FakeFetcher f = new FakeFetcher();
        BlockKey k0 = new BlockKey(OBJ, 0);
        BlockCache.Lease held = c.acquire(k0, BS, f);
        BlockCache.Lease dup = c.acquire(k0, BS, f);
        dup.close();
        dup.close(); // idempotent
        assertThat(c.stats().leasesOpen()).isEqualTo(1);
        touchAll(c, f, 1, 10); // pressure: everything but the leased block cycles
        assertThat(blockFile(k0)).exists();
        assertContent(held, k0, BS);
        MemorySegment seg = held.segment();
        held.close();
        assertThatThrownBy(held::segment).isInstanceOf(IllegalStateException.class);
        touchAll(c, f, 10, 14); // block 0 is now evictable
        assertThat(blockFile(k0)).doesNotExist();
        assertThatThrownBy(() -> seg.get(ValueLayout.JAVA_BYTE, 0)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void truncatedBlockOnDiskIsRefetched() throws Exception {
        BlockKey k = new BlockKey(OBJ, 1);
        FakeFetcher f = new FakeFetcher();
        BlockCache first = cache(8);
        first.acquire(k, BS, f).close();
        first.close();
        open.remove(first);
        try (var ch = java.nio.channels.FileChannel.open(blockFile(k), java.nio.file.StandardOpenOption.WRITE)) {
            ch.truncate(100); // crash mid-write / tampering: a plausible but wrong length
        }
        BlockCache c = cache(8);
        try (BlockCache.Lease l = c.acquire(k, BS, f)) {
            assertContent(l, k, BS);
        }
        assertThat(f.calls).hasValue(2);
        assertThat(blockFile(k)).hasSize(BS);
        assertThat(c.stats().bytesOnDisk()).isEqualTo(BS);
    }

    @Test
    void tmpFilesRemovedOnStartup() throws Exception {
        BlockCache earlier = cache(8); // a cache dir from an earlier run
        earlier.close();
        open.remove(earlier);
        BlockKey k = new BlockKey(OBJ, 0);
        Path blk = blockFile(k);
        Files.createDirectories(blk.getParent());
        Path tmp = blk.resolveSibling("0.blk.tmp");
        Path empty = blk.resolveSibling("1.blk");
        Path oversized = blk.resolveSibling("2.blk");
        Path good = blk.resolveSibling("3.blk");
        Files.write(tmp, new byte[10]);
        Files.write(empty, new byte[0]);
        Files.write(oversized, new byte[BS + 1]);
        Files.write(good, new byte[BS]);
        Path part = blk.resolveSibling("4.blk.tmp8812345.part"); // a fetcher's own temp file
        Files.write(part, new byte[10]);
        Path foreignInHashDir = blk.resolveSibling("readme.txt");
        Path foreignTop = dir.resolve("notes").resolve("7.blk");
        Files.write(foreignInHashDir, new byte[3]);
        Files.createDirectories(foreignTop.getParent());
        Files.write(foreignTop, new byte[0]);
        BlockCache c = cache(8);
        assertThat(part).doesNotExist();
        assertThat(foreignInHashDir).exists();
        assertThat(foreignTop).exists();
        assertThat(tmp).doesNotExist();
        assertThat(empty).doesNotExist();
        assertThat(oversized).doesNotExist();
        assertThat(good).exists();
        assertThat(c.stats().bytesOnDisk()).isEqualTo(BS);
    }

    @Test
    void refusesNonEmptyDirWithoutMarker() throws Exception {
        Path stranger = dir.resolve("important.txt");
        Files.writeString(stranger, "not a cache");
        assertThatThrownBy(() -> new BlockCache(dir, 8L * BS, BS, 2, now::get, usable::get))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(dir.toString())
                .hasMessageContaining("marker");
        assertThat(stranger).exists();
    }

    @Test
    void lowUsableSpaceServesUncached() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        ListAppender<ILoggingEvent> log = capture();
        usable.set(BS); // a block fits, the safety margin does not
        BlockKey k = new BlockKey(OBJ, 0);
        try (BlockCache.Lease l = c.acquire(k, BS, f)) {
            assertContent(l, k, BS);
        }
        assertThat(f.calls).hasValue(0);
        assertThat(f.memCalls).hasValue(1);
        assertThat(c.stats().writeFailures()).isEqualTo(1);
        assertThat(c.stats().bytesOnDisk()).isZero();
        assertThat(c.stats().leasesOpen()).isZero();
        assertThat(filesUnder(dir)).isEmpty();
        assertThat(warnings(log)).anyMatch(m -> m.contains("could not be cached"));
    }

    @Test
    void diskFullMidFetchIsRetriedOnceIntoMemory() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        f.enospc = true;
        BlockKey k = new BlockKey(OBJ, 2);
        try (BlockCache.Lease l = c.acquire(k, BS, f)) {
            assertContent(l, k, BS);
        }
        assertThat(f.calls).hasValue(1);
        assertThat(f.memCalls).hasValue(1);
        assertThat(c.stats().writeFailures()).isEqualTo(1);
        assertThat(filesUnder(dir)).isEmpty();
        assertThat(c.stats().bytesOnDisk()).isZero();
    }

    @Test
    void networkErrorIsNotRetried() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        f.failWith = new IOException("connection reset");
        assertThatThrownBy(() -> c.acquire(new BlockKey(OBJ, 0), BS, f))
                .isInstanceOf(IOException.class).hasMessageContaining("connection reset");
        assertThat(f.calls).hasValue(1);
        assertThat(f.memCalls).hasValue(0);
        assertThat(c.stats().writeFailures()).isZero();
    }

    @Test
    void defaultMemoryFetchUsesThePathFetcher() throws Exception {
        BlockCache c = cache(8);
        usable.set(0);
        AtomicInteger calls = new AtomicInteger();
        BlockCache.BlockFetcher pathOnly = (key, offset, length, target) -> {
            calls.incrementAndGet();
            byte[] b = new byte[(int) length];
            for (int i = 0; i < b.length; i++) b[i] = expected(key, offset + i);
            Files.write(target, b);
        };
        BlockKey k = new BlockKey(OBJ, 1);
        try (BlockCache.Lease l = c.acquire(k, BS, pathOnly)) {
            assertContent(l, k, BS);
        }
        assertThat(calls).hasValue(1);
        assertThat(filesUnder(dir)).isEmpty();
    }

    @Test
    void mapFailureAfterMoveServesFromMemory() throws Exception {
        BlockCache c = cache(8);
        c.failMappingForTest = true;
        FakeFetcher f = new FakeFetcher();
        BlockKey k = new BlockKey(OBJ, 0);
        try (BlockCache.Lease l = c.acquire(k, BS, f)) {
            assertContent(l, k, BS);
        }
        assertThat(f.calls).hasValue(1);
        assertThat(c.stats().writeFailures()).isEqualTo(1);
        assertThat(c.stats().bytesOnDisk()).isZero();
        assertThat(blockFile(k)).doesNotExist();
    }

    @Test
    void differentLengthForTheSameBlockIsRejected() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        f.gate = new CountDownLatch(1);
        BlockKey k = new BlockKey(OBJ, 0);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<BlockCache.Lease> first = pool.submit(() -> c.acquire(k, BS, f));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (c.stats().misses() < 1 && System.nanoTime() < deadline) Thread.onSpinWait();
            assertThatThrownBy(() -> c.acquire(k, 100, f)).isInstanceOf(IllegalArgumentException.class);
            f.gate.countDown();
            first.get(10, TimeUnit.SECONDS).close();
        } finally {
            pool.shutdownNow();
        }
        assertThatThrownBy(() -> c.acquire(k, 100, f)).isInstanceOf(IllegalArgumentException.class);
        assertThat(f.calls).hasValue(1);
        assertThat(blockFile(k)).hasSize(BS);
    }

    @Test
    void restartSeedsLruFromDisk() throws Exception {
        FakeFetcher f = new FakeFetcher();
        BlockCache first = cache(4);
        touchAll(first, f, 0, 4);
        touchAll(first, f, 0, 1); // block 0 becomes most recently used; mtime advances
        first.close();
        open.remove(first);
        assertThat(f.calls).hasValue(4);

        BlockCache c = cache(4);
        assertThat(c.stats().bytesOnDisk()).isEqualTo(4L * BS);
        touchAll(c, f, 4, 5); // a new block evicts the LRU, which is block 1 (not 0)
        assertThat(blockFile(new BlockKey(OBJ, 1))).doesNotExist();
        assertThat(blockFile(new BlockKey(OBJ, 0))).exists();
        touchAll(c, f, 0, 1);
        touchAll(c, f, 2, 4);
        assertThat(f.calls).hasValue(5); // 0, 2, 3 were served from the seeded cache
    }

    @Test
    void blockSizeChangeWipesCache() throws Exception {
        FakeFetcher f = new FakeFetcher();
        BlockCache first = cache(4);
        touchAll(first, f, 0, 2);
        first.close();
        open.remove(first);
        Path foreign = dir.resolve("keep.me");
        Files.writeString(foreign, "x");
        BlockCache c = new BlockCache(dir, 8L * BS, BS / 2, 2, now::get, usable::get); // block i now covers other bytes
        open.add(c);
        assertThat(c.stats().bytesOnDisk()).isZero();
        assertThat(filesUnder(dir)).containsExactly(foreign);
    }

    @Test
    void restartWithSmallerCapEvictsDownToCap() throws Exception {
        FakeFetcher f = new FakeFetcher();
        BlockCache first = cache(6);
        touchAll(first, f, 0, 6);
        first.close();
        open.remove(first);
        BlockCache c = cache(4);
        assertThat(c.stats().bytesOnDisk()).isEqualTo(4L * BS);
        assertThat(blockFile(new BlockKey(OBJ, 0))).doesNotExist();
        assertThat(blockFile(new BlockKey(OBJ, 5))).exists();
    }

    @Test
    void fetchFailurePropagatesAndCachesNothing() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        f.failWith = new IOException("boom: object missing");
        f.gate = new CountDownLatch(1);
        BlockKey k = new BlockKey(OBJ, 0);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> a = pool.submit(() -> c.acquire(k, BS, f));
            Future<?> b = pool.submit(() -> c.acquire(k, BS, f));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (c.stats().misses() < 2 && System.nanoTime() < deadline) Thread.onSpinWait();
            f.gate.countDown();
            for (Future<?> fu : List.of(a, b)) {
                assertThatThrownBy(() -> fu.get(10, TimeUnit.SECONDS))
                        .rootCause().isInstanceOf(IOException.class).hasMessageContaining("boom");
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(f.calls).hasValue(1);
        assertThat(filesUnder(dir)).isEmpty();
        assertThat(c.stats().bytesOnDisk()).isZero();
        assertThat(c.stats().leasesOpen()).isZero();

        f.gate = null;
        f.failWith = null;
        try (BlockCache.Lease l = c.acquire(k, BS, f)) { // the next acquire retries
            assertContent(l, k, BS);
        }
        assertThat(f.calls).hasValue(2);
    }

    @Test
    void shortFetchIsAnErrorAndCachesNothing() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        f.shortBy = 10;
        assertThatThrownBy(() -> c.acquire(new BlockKey(OBJ, 0), BS, f))
                .isInstanceOf(IOException.class).hasMessageContaining(OBJ);
        assertThat(filesUnder(dir)).isEmpty();
        assertThat(c.stats().bytesOnDisk()).isZero();
    }

    @Test
    void interruptedWaiterDoesNotCancelSharedFetch() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        f.gate = new CountDownLatch(1);
        BlockKey k = new BlockKey(OBJ, 0);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Thread[] victim = new Thread[1];
            CountDownLatch victimStarted = new CountDownLatch(1);
            Future<?> a = pool.submit(() -> {
                victim[0] = Thread.currentThread();
                victimStarted.countDown();
                return c.acquire(k, BS, f);
            });
            victimStarted.await();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (c.stats().misses() < 1 && System.nanoTime() < deadline) Thread.onSpinWait();
            Future<BlockCache.Lease> b = pool.submit(() -> c.acquire(k, BS, f));
            while (c.stats().misses() < 2 && System.nanoTime() < deadline) Thread.onSpinWait();
            victim[0].interrupt();
            assertThatThrownBy(() -> a.get(10, TimeUnit.SECONDS)).rootCause().isInstanceOf(InterruptedIOException.class);
            f.gate.countDown();
            try (BlockCache.Lease l = b.get(10, TimeUnit.SECONDS)) {
                assertContent(l, k, BS);
            }
            assertThat(f.calls).hasValue(1);
            assertThat(c.stats().leasesOpen()).isZero(); // the interrupted waiter's reservation was released
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void diskWriteFailureStillServesRead() throws Exception {
        BlockCache c = cache(8);
        FakeFetcher f = new FakeFetcher();
        assertThat(dir.toFile().setWritable(false)).isTrue();
        org.junit.jupiter.api.Assumptions.assumeFalse(Files.isWritable(dir), "running with root-like permissions");
        ListAppender<ILoggingEvent> log = capture();
        BlockKey k = new BlockKey(OBJ, 0);
        try (BlockCache.Lease l = c.acquire(k, BS, f)) {
            assertContent(l, k, BS);
            assertThat(c.stats().leasesOpen()).isEqualTo(1);
        }
        assertThat(f.calls).hasValue(0);
        assertThat(f.memCalls).hasValue(1);
        WarmStats s = c.stats();
        assertThat(s.writeFailures()).isEqualTo(1);
        assertThat(s.bytesOnDisk()).isZero();
        assertThat(s.leasesOpen()).isZero();
        assertThat(blockFile(k)).doesNotExist();
        assertThat(warnings(log)).anyMatch(m -> m.contains("could not be cached"));
        dir.toFile().setWritable(true);
        try (BlockCache.Lease l = c.acquire(k, BS, f)) { // not cached, so fetched again — and cached now
            assertContent(l, k, BS);
        }
        assertThat(f.calls).hasValue(1);
        assertThat(blockFile(k)).exists();
    }

    @Test
    void thrashWarnRateLimited() throws Exception {
        BlockCache c = cache(4);
        FakeFetcher f = new FakeFetcher();
        ListAppender<ILoggingEvent> log = capture();
        long t0 = now.get();
        for (int i = 0; i < 10; i++) {
            try (BlockCache.Lease l = c.acquire(new BlockKey(OBJ, i), BS, f)) { assertThat(l.length()).isEqualTo(BS); }
        }
        assertThat(c.stats().evictions()).isEqualTo(6);
        assertThat(c.stats().thrashWarnings()).isEqualTo(1);
        now.set(t0 + 30_000);
        for (int i = 10; i < 13; i++) c.acquire(new BlockKey(OBJ, i), BS, f).close();
        assertThat(c.stats().thrashWarnings()).isEqualTo(1);
        now.set(t0 + 61_000);
        c.acquire(new BlockKey(OBJ, 13), BS, f).close();
        assertThat(c.stats().thrashWarnings()).isEqualTo(2);
        List<String> warns = warnings(log).stream().filter(m -> m.contains("hit rate")).toList();
        assertThat(warns).hasSize(2);
    }

    @Test
    void closedCacheRefusesAcquire() throws Exception {
        BlockCache c = cache(4);
        c.close();
        assertThatThrownBy(() -> c.acquire(new BlockKey(OBJ, 0), BS, new FakeFetcher()))
                .isInstanceOf(IllegalStateException.class);
    }

    private ListAppender<ILoggingEvent> capture() {
        ListAppender<ILoggingEvent> a = new ListAppender<>();
        a.start();
        Logger logger = (Logger) LoggerFactory.getLogger(BlockCache.class);
        logger.setLevel(Level.INFO);
        logger.addAppender(a);
        appenders.add(a);
        return a;
    }

    private static List<String> warnings(ListAppender<ILoggingEvent> a) {
        return a.list.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
    }
}
