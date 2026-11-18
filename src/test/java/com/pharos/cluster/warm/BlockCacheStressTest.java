package com.pharos.cluster.warm;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import org.apache.lucene.store.IndexInput;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Many threads reading random ranges while the cap forces constant eviction: every byte read must
 * be exact, no lease may see a closed mapping (an {@link IllegalStateException} from a closed arena),
 * and the accounting must balance once everyone is done. Exercises the lock-free hit path against
 * eviction, invalidation and single-flight fetches.
 */
class BlockCacheStressTest {

    private static final int BS = 4096;
    private static final int OBJECTS = 12;
    private static final int BLOCKS_PER_OBJECT = 10;
    private static final int THREADS = 8;

    @TempDir Path dir;

    private static byte expected(String objectKey, long offset) {
        return (byte) (objectKey.hashCode() * 31 + offset * 7 + (offset >>> 11));
    }

    private static String key(int o) {
        return "shards/s/objects/obj" + o;
    }

    /** Serves deterministic bytes; counts fetches. */
    private static final class Fetcher implements BlockCache.BlockFetcher {
        final AtomicLong fetches = new AtomicLong();

        @Override
        public void fetch(BlockKey key, long offset, long length, Path target) throws IOException {
            fetches.incrementAndGet();
            byte[] b = new byte[(int) length];
            for (int i = 0; i < b.length; i++) b[i] = expected(key.objectKey(), offset + i);
            if (ThreadLocalRandom.current().nextInt(8) == 0) Thread.yield();
            Files.write(target, b);
        }
    }

    /** Lease-level: random blocks, random byte checks, overlapping leases, invalidations, prefetches. */
    @RepeatedTest(3)
    void leasesStayValidAndBytesExactUnderConstantEviction() throws Exception {
        BlockCache cache = new BlockCache(dir.resolve("c"), 6L * BS, BS, 4);
        Fetcher fetcher = new Fetcher();
        AtomicLong acquires = new AtomicLong();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                fs.add(pool.submit(() -> {
                    ThreadLocalRandom r = ThreadLocalRandom.current();
                    try {
                        start.await();
                        for (int i = 0; i < 4000; i++) {
                            // a skewed choice: a hot set that fits the cap and a cold tail that evicts it
                            int o = r.nextInt(4) == 0 ? r.nextInt(OBJECTS) : r.nextInt(2);
                            long b = r.nextInt(4) == 0 ? r.nextInt(BLOCKS_PER_OBJECT) : r.nextInt(2);
                            BlockKey k = new BlockKey(key(o), b);
                            int op = r.nextInt(100);
                            if (op < 3) {
                                cache.invalidate(k, BlockCache.objectHash(k.objectKey()));
                                continue;
                            }
                            if (op < 8) {
                                cache.prefetch(k, BS, fetcher);
                                continue;
                            }
                            BlockCache.Lease second = null;
                            try (BlockCache.Lease l = cache.acquire(k, BS, fetcher)) {
                                acquires.incrementAndGet();
                                if (op < 20) { // hold two leases at once
                                    BlockKey k2 = new BlockKey(key(r.nextInt(OBJECTS)), r.nextInt(BLOCKS_PER_OBJECT));
                                    second = cache.acquire(k2, BS, fetcher);
                                    acquires.incrementAndGet();
                                    check(second, k2, r);
                                }
                                check(l, k, r);
                                if (r.nextInt(16) == 0) Thread.yield(); // let evictions run while leased
                                check(l, k, r);
                            } finally {
                                if (second != null) second.close();
                            }
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : fs) f.get(120, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertThat(failures).as("first failure: %s", failures.peek()).isEmpty();
        WarmStats st = cache.stats();
        assertThat(st.leasesOpen()).isZero();
        assertThat(st.hits() + st.misses()).isEqualTo(acquires.get());
        assertThat(st.evictions()).as("the cap forced evictions").isPositive();
        assertThat(st.bytesOnDisk()).isLessThanOrEqualTo(6L * BS);
        assertThat(st.bytesOnDisk()).isEqualTo(blockBytesOnDisk(dir.resolve("c")));
        cache.close();
    }

    private static void check(BlockCache.Lease l, BlockKey k, ThreadLocalRandom r) {
        MemorySegment s = l.segment();
        long base = k.index() * BS;
        for (int j = 0; j < 8; j++) {
            long at = r.nextLong(l.length());
            byte got = s.get(ValueLayout.JAVA_BYTE, at);
            if (got != expected(k.objectKey(), base + at)) {
                throw new AssertionError("byte " + (base + at) + " of " + k.objectKey() + ": " + got);
            }
        }
    }

    private static long blockBytesOnDisk(Path cacheDir) throws IOException {
        try (Stream<Path> s = Files.walk(cacheDir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".blk")).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }).sum();
        }
    }

    // ---------------------------------------------------------------- through the IndexInput

    /** In-memory objects; only the range calls are used. */
    private static ReplicaSource source(java.util.Map<String, byte[]> objects) {
        return new ReplicaSource() {
            @Override public Optional<Generation> latest(ShardId shard) { throw new UnsupportedOperationException(); }
            @Override public ShardManifest manifest(ShardId shard, Generation gen) { throw new UnsupportedOperationException(); }
            @Override public void download(ManifestFile file, Path target) { throw new UnsupportedOperationException(); }
            @Override public void downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException {
                byte[] d = objects.get(file.key());
                Files.write(target, java.util.Arrays.copyOfRange(d, (int) offset, (int) (offset + length)));
            }
            @Override public InputStream openRange(ManifestFile file, long offset, long length) {
                byte[] d = objects.get(file.key());
                return new ByteArrayInputStream(d, (int) offset, (int) length);
            }
            @Override public Optional<byte[]> synonyms() { return Optional.empty(); }
        };
    }

    /** IndexInput-level: clones read random ranges and primitives across block edges while blocks are evicted. */
    @RepeatedTest(3)
    void indexInputReadsAreExactUnderConstantEviction() throws Exception {
        int files = 6;
        int fileLength = 9 * BS + 123;
        java.util.Map<String, byte[]> objects = new java.util.HashMap<>();
        List<ManifestFile> mfs = new ArrayList<>();
        for (int f = 0; f < files; f++) {
            String k = key(100 + f);
            byte[] d = new byte[fileLength];
            for (int i = 0; i < d.length; i++) d[i] = expected(k, i);
            objects.put(k, d);
            mfs.add(new ManifestFile(ManifestFile.LUCENE, "_" + f + ".bin", k, fileLength, "0"));
        }
        BlockCache cache = new BlockCache(dir.resolve("i"), 8L * BS, BS, 4);
        ReplicaSource src = source(objects);
        List<CachingRemoteIndexInput> inputs = new ArrayList<>();
        for (ManifestFile mf : mfs) {
            inputs.add(new CachingRemoteIndexInput("stress " + mf.name(),
                    new CachingRemoteIndexInput.RemoteFile(new ShardId("s"), 1, mf, cache, src)));
        }
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                fs.add(pool.submit(() -> {
                    ThreadLocalRandom r = ThreadLocalRandom.current();
                    try {
                        start.await();
                        for (int i = 0; i < 3000; i++) {
                            int f = r.nextInt(files);
                            byte[] want = objects.get(mfs.get(f).key());
                            try (IndexInput in = inputs.get(f).clone()) {
                                long p = r.nextLong(fileLength - 64);
                                in.seek(p);
                                switch (r.nextInt(5)) {
                                    case 0 -> {
                                        int n = (int) Math.min(fileLength - p, r.nextInt(3 * BS));
                                        byte[] b = new byte[n];
                                        in.readBytes(b, 0, n);
                                        for (int j = 0; j < n; j++) {
                                            if (b[j] != want[(int) p + j]) throw new AssertionError("readBytes at " + (p + j));
                                        }
                                    }
                                    case 1 -> {
                                        long v = in.readLong();
                                        long e = java.nio.ByteBuffer.wrap(want, (int) p, 8)
                                                .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                                        if (v != e) throw new AssertionError("readLong at " + p);
                                    }
                                    case 2 -> {
                                        int v = in.readInt();
                                        int e = java.nio.ByteBuffer.wrap(want, (int) p, 4)
                                                .order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
                                        if (v != e) throw new AssertionError("readInt at " + p);
                                    }
                                    case 3 -> {
                                        for (int j = 0; j < 40; j++) {
                                            if (in.readByte() != want[(int) p + j]) throw new AssertionError("readByte at " + (p + j));
                                        }
                                    }
                                    default -> {
                                        int n = (int) Math.min((fileLength - p) / 4, 1 + r.nextInt(600));
                                        float[] got = new float[n];
                                        in.readFloats(got, 0, n);
                                        java.nio.FloatBuffer fb = java.nio.ByteBuffer.wrap(want, (int) p, n * 4)
                                                .order(java.nio.ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
                                        for (int j = 0; j < n; j++) {
                                            if (Float.floatToRawIntBits(got[j]) != Float.floatToRawIntBits(fb.get(j))) {
                                                throw new AssertionError("readFloats at " + (p + 4L * j));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : fs) f.get(120, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertThat(failures).as("first failure: %s", failures.peek()).isEmpty();
        WarmStats st = cache.stats();
        assertThat(st.leasesOpen()).isZero();
        assertThat(st.evictions()).isPositive();
        assertThat(st.bytesOnDisk()).isLessThanOrEqualTo(8L * BS).isEqualTo(blockBytesOnDisk(dir.resolve("i")));
        cache.close();
    }

    /**
     * Large blocks (128 KiB) and long sequential runs, so the refill window grows to its maximum and
     * shrinks again on seeks, while other threads keep evicting: readByte and readBytes stay byte-exact.
     */
    @RepeatedTest(3)
    void longSequentialRunsWithSeeksAreExactWithLargeBlocks() throws Exception {
        int bs = 128 * 1024;
        int files = 4;
        int fileLength = 5 * bs + 777;
        java.util.Map<String, byte[]> objects = new java.util.HashMap<>();
        List<ManifestFile> mfs = new ArrayList<>();
        for (int f = 0; f < files; f++) {
            String k = key(200 + f);
            byte[] d = new byte[fileLength];
            for (int i = 0; i < d.length; i++) d[i] = expected(k, i);
            objects.put(k, d);
            mfs.add(new ManifestFile(ManifestFile.LUCENE, "_" + f + ".big", k, fileLength, "0"));
        }
        BlockCache cache = new BlockCache(dir.resolve("big"), 6L * bs, bs, 4); // 6 of 24 blocks fit
        ReplicaSource src = source(objects);
        List<CachingRemoteIndexInput> inputs = new ArrayList<>();
        for (ManifestFile mf : mfs) {
            inputs.add(new CachingRemoteIndexInput("big " + mf.name(),
                    new CachingRemoteIndexInput.RemoteFile(new ShardId("s"), 1, mf, cache, src)));
        }
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                fs.add(pool.submit(() -> {
                    ThreadLocalRandom r = ThreadLocalRandom.current();
                    try {
                        start.await();
                        for (int i = 0; i < 60; i++) {
                            int f = r.nextInt(files);
                            byte[] want = objects.get(mfs.get(f).key());
                            try (IndexInput in = inputs.get(f).clone()) {
                                for (int seek = 0; seek < 6; seek++) {
                                    long p = r.nextLong(fileLength - 1);
                                    in.seek(p);
                                    int run = (int) Math.min(fileLength - p, r.nextInt(300_000));
                                    int done = 0;
                                    while (done < run) {
                                        if (r.nextInt(4) == 0) {
                                            int n = Math.min(run - done, 1 + r.nextInt(20_000));
                                            byte[] b = new byte[n];
                                            in.readBytes(b, 0, n);
                                            for (int j = 0; j < n; j++) {
                                                if (b[j] != want[(int) p + done + j]) {
                                                    throw new AssertionError("readBytes at " + (p + done + j));
                                                }
                                            }
                                            done += n;
                                        } else {
                                            int n = Math.min(run - done, 1 + r.nextInt(5_000));
                                            for (int j = 0; j < n; j++) {
                                                if (in.readByte() != want[(int) p + done + j]) {
                                                    throw new AssertionError("readByte at " + (p + done + j));
                                                }
                                            }
                                            done += n;
                                        }
                                    }
                                    if (in.getFilePointer() != p + run) throw new AssertionError("position");
                                }
                            }
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : fs) f.get(300, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertThat(failures).as("first failure: %s", failures.peek()).isEmpty();
        WarmStats st = cache.stats();
        assertThat(st.leasesOpen()).isZero();
        assertThat(st.evictions()).isPositive();
        assertThat(st.bytesOnDisk()).isLessThanOrEqualTo(6L * bs).isEqualTo(blockBytesOnDisk(dir.resolve("big")));
        cache.close();
    }
}
