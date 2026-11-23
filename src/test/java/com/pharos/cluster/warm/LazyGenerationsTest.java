package com.pharos.cluster.warm;

import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.config.ClusterConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class LazyGenerationsTest {

    @TempDir Path tmp;

    private static ReplicaSource unusedSource() {
        return (ReplicaSource) Proxy.newProxyInstance(LazyGenerationsTest.class.getClassLoader(),
                new Class<?>[]{ReplicaSource.class}, (p, m, a) -> { throw new UnsupportedOperationException(); });
    }

    private static ManifestFile lucene(String name, long length) {
        return new ManifestFile(ManifestFile.LUCENE, name, "k/" + name, length, "0");
    }

    @Test
    void aCompoundFileNoLargerThanOneBlockIsEager() {
        ClusterConfig.ReaderConfig cfg = new ClusterConfig.ReaderConfig();
        cfg.setBlockSizeBytes(64 * 1024);
        cfg.setEagerMaxBytes(0);
        try (LazyGenerations lazy = new LazyGenerations(tmp, cfg, unusedSource())) {
            assertThat(lazy.eager(lucene("_0.cfs", 64 * 1024))).as("exactly one block").isTrue();
            assertThat(lazy.eager(lucene("_0.cfs", 10))).isTrue();
            assertThat(lazy.eager(lucene("_0.cfs", 64 * 1024 + 1))).as("more than one block").isFalse();
            assertThat(lazy.eager(lucene("_0.tim", 64 * 1024))).as("not a compound file").isFalse();
        }
    }

    // ---------------------------------------------------------------- compound-file prefetch

    private static final int BLOCK = 64 * 1024;
    private static final Set<String> PREFETCHED = Set.of("tip", "tmd", "tim", "vex", "vem");

    /** One flushed compound segment: terms, vectors and a few MB of incompressible stored fields. */
    private static Path compoundIndex(Path dir) throws Exception {
        Random rnd = new Random(42);
        String[] words = new String[2000];
        for (int i = 0; i < words.length; i++) words[i] = "w" + Integer.toString(rnd.nextInt(1 << 30), 36);
        org.apache.lucene.index.IndexWriterConfig iwc =
                new org.apache.lucene.index.IndexWriterConfig(new org.apache.lucene.analysis.standard.StandardAnalyzer())
                        .setUseCompoundFile(true).setRAMBufferSizeMB(256);
        try (var d = org.apache.lucene.store.FSDirectory.open(dir);
             var w = new org.apache.lucene.index.IndexWriter(d, iwc)) {
            for (int i = 0; i < 4000; i++) {
                var doc = new org.apache.lucene.document.Document();
                StringBuilder body = new StringBuilder();
                for (int k = 0; k < 20; k++) body.append(words[rnd.nextInt(words.length)]).append(' ');
                doc.add(new org.apache.lucene.document.TextField("body", body.toString(),
                        org.apache.lucene.document.Field.Store.NO));
                byte[] payload = new byte[800];
                rnd.nextBytes(payload);
                doc.add(new org.apache.lucene.document.StoredField("payload", payload));
                float[] v = new float[16];
                for (int k = 0; k < v.length; k++) v[k] = rnd.nextFloat();
                doc.add(new org.apache.lucene.document.KnnFloatVectorField("v", v));
                w.addDocument(doc);
            }
            w.commit();
        }
        return dir;
    }

    /** Where each sub-file of the compound file starts in its bytes, found by content (an independent oracle). */
    private static List<long[]> prefetchedRanges(Path dir, byte[] cfs) throws Exception {
        List<long[]> out = new ArrayList<>();
        try (var d = org.apache.lucene.store.FSDirectory.open(dir)) {
            var si = org.apache.lucene.index.SegmentInfos.readLatestCommit(d).info(0).info;
            try (var cd = si.getCodec().compoundFormat().getCompoundReader(d, si)) {
                for (String name : cd.listAll()) {
                    if (!PREFETCHED.contains(name.substring(name.lastIndexOf('.') + 1))) continue;
                    byte[] sub = new byte[(int) cd.fileLength(name)];
                    try (var in = cd.openInput(name, org.apache.lucene.store.IOContext.DEFAULT)) {
                        in.readBytes(sub, 0, sub.length);
                    }
                    int at = indexOf(cfs, sub);
                    assertThat(at).as(name + " found in the .cfs").isNotNegative();
                    out.add(new long[] {at, sub.length});
                }
            }
        }
        return out;
    }

    private static int indexOf(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= hay.length; i++) {
            for (int j = 0; j < needle.length; j++) if (hay[i + j] != needle[j]) continue outer;
            return i;
        }
        return -1;
    }

    /**
     * Published segments are often single compound files, whose names never match the prefetch
     * extensions: the prefetch reads the eager {@code .cfe} entry table and fetches exactly the blocks
     * of the {@code .tip/.tmd/.tim/.vex/.vem} entries inside the {@code .cfs}.
     */
    @Test
    void prefetchFetchesTheTermsAndVectorGraphEntriesInsideACompoundFile() throws Exception {
        Path src = compoundIndex(tmp.resolve("src"));
        var store = new com.pharos.cluster.objectstore.InMemoryObjectStore();
        List<ManifestFile> files = new ArrayList<>();
        for (String name : org.apache.lucene.store.FSDirectory.listAll(src)) {
            if (name.equals("write.lock")) continue;
            byte[] data = Files.readAllBytes(src.resolve(name));
            String key = "pharos/shards/t/objects/" + name;
            store.put(key, data);
            long crc;
            try (var d = org.apache.lucene.store.FSDirectory.open(src);
                 var in = d.openInput(name, org.apache.lucene.store.IOContext.READONCE)) {
                crc = org.apache.lucene.codecs.CodecUtil.retrieveChecksum(in);
            }
            files.add(new ManifestFile(ManifestFile.LUCENE, name, key, data.length, Long.toHexString(crc)));
        }
        String cfsName = files.stream().map(ManifestFile::name).filter(n -> n.endsWith(".cfs")).findFirst().orElseThrow();
        byte[] cfs = Files.readAllBytes(src.resolve(cfsName));
        var manifest = new com.pharos.cluster.objectstore.ShardManifest(
                com.pharos.cluster.objectstore.ShardManifest.FORMAT_VERSION, "t", 1, null, Instant.now(), "writer", 0,
                files.stream().mapToLong(ManifestFile::length).sum(), null, files, null, null);

        TreeSet<Long> expected = new TreeSet<>();
        for (long[] r : prefetchedRanges(src, cfs)) {
            for (long b = r[0] / BLOCK; b <= (r[0] + r[1] - 1) / BLOCK; b++) expected.add(b);
        }
        long totalBlocks = (cfs.length + BLOCK - 1) / BLOCK;
        assertThat(expected).as("fixture: the prefetch set is a strict subset of the .cfs").isNotEmpty()
                .hasSizeLessThan((int) totalBlocks);
        long expectedBytes = expected.stream().mapToLong(b -> Math.min(BLOCK, cfs.length - b * BLOCK)).sum();

        ClusterConfig.ReaderConfig cfg = new ClusterConfig.ReaderConfig();
        cfg.setBlockSizeBytes(BLOCK);
        cfg.setEagerMaxBytes(0);
        cfg.setCacheMaxBytes(64L << 20);
        Path cacheDir = tmp.resolve("cache");
        Path luceneDir = cacheDir.resolve("t").resolve("gen-1").resolve("lucene");
        Files.createDirectories(luceneDir);
        var source = new com.pharos.cluster.objectstore.ObjectStoreReplicaSource(store,
                new com.pharos.cluster.objectstore.ObjectStoreLayout("pharos"));
        try (LazyGenerations lazy = new LazyGenerations(cacheDir, cfg, source)) {
            for (ManifestFile f : files) {
                if (lazy.eager(f)) Files.copy(src.resolve(f.name()), luceneDir.resolve(f.name()));
            }
            assertThat(lazy.eager(files.stream().filter(f -> f.name().equals(cfsName)).findFirst().orElseThrow()))
                    .as("the .cfs is remote").isFalse();
            LazyGenerations.Prefetch p = lazy.prefetch(manifest, luceneDir);
            p.done().get(30, TimeUnit.SECONDS);
            assertThat(p.blocks()).isEqualTo(expected.size());
            assertThat(p.bytes()).isEqualTo(expectedBytes);
            assertThat(lazy.blockCache().stats().bytesFetched()).isEqualTo(expectedBytes);

            // A damaged entry table is detected by its checksum; the prefetch then skips the
            // compound entries (fetched on first read) instead of failing the sync.
            Path cfe = luceneDir.resolve(cfsName.replace(".cfs", ".cfe"));
            byte[] t = Files.readAllBytes(cfe);
            t[t.length / 2] ^= 0x5a;
            Files.write(cfe, t);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> LazyGenerations.readCompoundEntries(cfe))
                    .isInstanceOf(org.apache.lucene.index.CorruptIndexException.class);
            LazyGenerations.Prefetch again = lazy.prefetch(manifest, luceneDir);
            again.done().get(30, TimeUnit.SECONDS);
            assertThat(again.blocks()).isZero();
        }
    }
}
