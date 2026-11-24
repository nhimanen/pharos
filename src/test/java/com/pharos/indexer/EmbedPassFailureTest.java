package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.embedding.DjlEmbeddingProvider;
import com.pharos.embedding.EmbeddingException;
import com.pharos.embedding.EmbeddingProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class EmbedPassFailureTest {
    @TempDir Path tmp;

    /** Embeds via the real tolerant fallback; texts in {@code bad} fail. */
    static class FakeProvider implements EmbeddingProvider {
        final Set<String> bad;
        FakeProvider(Set<String> bad) { this.bad = bad; }
        /** A provider whose model is broken: even the canary fails. */
        static FakeProvider broken(String... texts) {
            var s = new java.util.HashSet<>(java.util.Arrays.asList(texts));
            s.add(DjlEmbeddingProvider.CANARY_TEXT);
            return new FakeProvider(s);
        }
        public String modelId() { return "fake"; }
        public int dimensions() { return 2; }
        public boolean isAvailable() { return true; }
        public float[] embed(String t) {
            if (bad.contains(t)) throw new EmbeddingException("boom " + t, null);
            return new float[]{1f, 2f};
        }
        @Override public float[][] embedBatch(List<String> texts) {
            return DjlEmbeddingProvider.embedEachTolerant("fake", texts, this::embed);
        }
    }

    private Path spool(String... texts) throws Exception {
        Path p = tmp.resolve("spool.bin");
        try (var out = new DataOutputStream(Files.newOutputStream(p))) {
            for (String t : texts) {
                byte[] b = t.getBytes(StandardCharsets.UTF_8);
                out.writeInt(b.length);
                out.write(b);
            }
        }
        return p;
    }

    private ProjectIndexManager manager() {
        return new ProjectIndexManager(IndexConfig.defaults(), null, null, new FakeProvider(Set.of()), null);
    }

    private ProjectIndexManager.EmbedRuntime runtime(FakeProvider p, PersistentEmbeddingCache cache) {
        return new ProjectIndexManager.EmbedRuntime(p, cache, tmp.resolve("slots.bin"), 4, 1);
    }

    @Test
    void oneBadTextGetsNanSentinelAndRunCompletes() throws Exception {
        var cache = new PersistentEmbeddingCache(tmp, "fp", 2);
        manager().runEmbedPass(spool("a", "BAD", "c"), 3,
                runtime(new FakeProvider(Set.of("BAD")), cache), (s, c, t) -> {}, "p");
        ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(tmp.resolve("slots.bin")));
        assertEquals(1f, b.getFloat(0));
        assertTrue(Float.isNaN(b.getFloat(2 * 4)), "failed slot carries the NaN sentinel");
        assertEquals(1f, b.getFloat(4 * 4));
    }

    @Test
    void allFailBatchThrows() throws Exception {
        assertThrows(EmbeddingException.class, () -> manager().runEmbedPass(
                spool("x", "y"), 2,
                runtime(FakeProvider.broken("x", "y"), null), (s, c, t) -> {}, "p"));
    }

    @Test
    void cacheIsSavedWhenTheRunAborts() throws Exception {
        var cache = new PersistentEmbeddingCache(tmp, "fp", 2);
        // chunkSize 4: chunk 1 = a,b,c,d (fine), chunk 2 = x,y (all fail -> abort)
        assertThrows(EmbeddingException.class, () -> manager().runEmbedPass(
                spool("a", "b", "c", "d", "x", "y"), 6,
                runtime(FakeProvider.broken("x", "y"), cache), (s, c, t) -> {}, "p"));
        var reloaded = new PersistentEmbeddingCache(tmp, "fp", 2);
        assertNotNull(reloaded.get(PersistentEmbeddingCache.sha256Hex("a")));
    }

    @Test
    void singleBadTextAsOnlyMissDoesNotAbort() throws Exception {
        var cache = new PersistentEmbeddingCache(tmp, "fp", 2);
        manager().runEmbedPass(spool("BAD"), 1,
                runtime(new FakeProvider(Set.of("BAD")), cache), (s, c, t) -> {}, "p");
        ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(tmp.resolve("slots.bin")));
        assertTrue(Float.isNaN(b.getFloat(0)));
    }
}
