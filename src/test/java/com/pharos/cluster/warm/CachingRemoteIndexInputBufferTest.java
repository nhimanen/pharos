package com.pharos.cluster.warm;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ShardManifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** The input's refill window: grows on sequential reads, shrinks on random ones; medium reads bypass it. */
class CachingRemoteIndexInputBufferTest {

    private static final int BS = 128 * 1024;

    @TempDir Path dir;
    private BlockCache cache;

    @AfterEach
    void close() {
        if (cache != null) cache.close();
    }

    private static ReplicaSource source(String key, byte[] data) {
        return new ReplicaSource() {
            @Override public Optional<Generation> latest(ShardId shard) { throw new UnsupportedOperationException(); }
            @Override public ShardManifest manifest(ShardId shard, Generation gen) { throw new UnsupportedOperationException(); }
            @Override public void download(ManifestFile file, Path target) { throw new UnsupportedOperationException(); }
            @Override public void downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException {
                Files.write(target, Arrays.copyOfRange(data, (int) offset, (int) (offset + length)));
            }
            @Override public InputStream openRange(ManifestFile file, long offset, long length) {
                return new ByteArrayInputStream(data, (int) offset, (int) length);
            }
            @Override public Optional<byte[]> synonyms() { return Optional.empty(); }
        };
    }

    private byte[] data;

    private CachingRemoteIndexInput input() throws IOException {
        data = new byte[3 * BS];
        for (int i = 0; i < data.length; i++) data[i] = (byte) (i * 31 + (i >>> 9));
        cache = new BlockCache(dir, 16L * BS, BS, 2);
        ManifestFile mf = new ManifestFile(ManifestFile.LUCENE, "_0.bin", "k/_0.bin", data.length, "0");
        return new CachingRemoteIndexInput("buffer test",
                new CachingRemoteIndexInput.RemoteFile(new ShardId("s"), 1, mf, cache, source("k/_0.bin", data)));
    }

    private long leases() {
        WarmStats st = cache.stats();
        return st.hits() + st.misses();
    }

    @Test
    void theWindowGrowsOnASequentialScanAndShrinksOnARandomRefill() throws IOException {
        try (CachingRemoteIndexInput in = input()) {
            for (int i = 0; i < 200_000; i++) {
                assertThat(in.readByte()).isEqualTo(data[i]);
            }
            assertThat(in.lastFillLength()).as("grown by the scan").isEqualTo(CachingRemoteIndexInput.MAX_BUFFER);
            long at = 2L * BS + 12_345;
            in.seek(at);
            int v = in.readInt();
            assertThat(v).isEqualTo(ByteBuffer.wrap(data, (int) at, 4).order(ByteOrder.LITTLE_ENDIAN).getInt());
            assertThat(in.lastFillLength()).as("a random refill copies a small window")
                    .isLessThanOrEqualTo(CachingRemoteIndexInput.MIN_BUFFER);
        }
    }

    @Test
    void aMediumReadOnAFreshCloneTakesOneLease() throws IOException {
        try (CachingRemoteIndexInput in = input()) {
            in.seek(1000);
            in.readByte(); // fetch the block once
            CachingRemoteIndexInput clone = in.clone();
            clone.seek(5000);
            byte[] six = new byte[6 * 1024];
            long before = leases();
            clone.readBytes(six, 0, six.length);
            assertThat(leases() - before).as("one lease, not one per 1 KiB refill").isEqualTo(1);
            assertThat(six).isEqualTo(Arrays.copyOfRange(data, 5000, 5000 + six.length));
        }
    }
}
