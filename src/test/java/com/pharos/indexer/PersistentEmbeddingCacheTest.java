package com.pharos.indexer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class PersistentEmbeddingCacheTest {

    @TempDir
    Path dir;

    // ── get / put ──────────────────────────────────────────────────────────

    @Test
    void get_emptyCache_returnsNull() {
        PersistentEmbeddingCache cache = new PersistentEmbeddingCache(dir, "fp1", 4);
        assertThat(cache.get("nosuchkey")).isNull();
    }

    @Test
    void put_thenGet_returnsExactVector() {
        PersistentEmbeddingCache cache = new PersistentEmbeddingCache(dir, "fp1", 4);
        float[] vec = {1f, 2f, 3f, 4f};
        String key = PersistentEmbeddingCache.sha256Hex("text1");

        cache.put(key, vec);

        assertThat(cache.get(key)).containsExactly(vec[0], vec[1], vec[2], vec[3]);
    }

    // ── persistence ───────────────────────────────────────────────────────

    @Test
    void save_thenReload_persistsSingleEntry() {
        float[] vec = {0.1f, 0.2f, 0.3f, 0.4f};
        String key = PersistentEmbeddingCache.sha256Hex("some embedding text");
        PersistentEmbeddingCache c1 = new PersistentEmbeddingCache(dir, "fp1", 4);
        c1.put(key, vec);
        c1.save();

        PersistentEmbeddingCache c2 = new PersistentEmbeddingCache(dir, "fp1", 4);
        float[] loaded = c2.get(key);

        assertThat(loaded).isNotNull();
        assertThat(loaded).containsExactly(vec[0], vec[1], vec[2], vec[3]);
    }

    @Test
    void save_multipleEntries_allRoundtrip() {
        PersistentEmbeddingCache c1 = new PersistentEmbeddingCache(dir, "fp1", 4);
        String[] keys = new String[20];
        for (int i = 0; i < 20; i++) {
            keys[i] = PersistentEmbeddingCache.sha256Hex("text sample " + i);
            c1.put(keys[i], new float[]{i, i + 1f, i + 2f, i + 3f});
        }
        c1.save();

        PersistentEmbeddingCache c2 = new PersistentEmbeddingCache(dir, "fp1", 4);
        assertThat(c2.size()).isEqualTo(20);
        for (int i = 0; i < 20; i++) {
            float[] v = c2.get(keys[i]);
            assertThat(v).isNotNull();
            assertThat(v[0]).isEqualTo((float) i);
        }
    }

    @Test
    void save_beforeAnyPut_producesEmptyCache() {
        PersistentEmbeddingCache c1 = new PersistentEmbeddingCache(dir, "fp1", 4);
        c1.save();

        PersistentEmbeddingCache c2 = new PersistentEmbeddingCache(dir, "fp1", 4);
        assertThat(c2.size()).isEqualTo(0);
    }

    // ── invalidation ──────────────────────────────────────────────────────

    @Test
    void load_differentFingerprint_invalidatesCache() {
        String key = PersistentEmbeddingCache.sha256Hex("text");
        PersistentEmbeddingCache c1 = new PersistentEmbeddingCache(dir, "model-a:4", 4);
        c1.put(key, new float[]{1f, 2f, 3f, 4f});
        c1.save();

        PersistentEmbeddingCache c2 = new PersistentEmbeddingCache(dir, "model-b:4", 4);
        assertThat(c2.size()).isEqualTo(0);
        assertThat(c2.get(key)).isNull();
    }

    @Test
    void load_differentDims_invalidatesCache() {
        String key = PersistentEmbeddingCache.sha256Hex("text");
        PersistentEmbeddingCache c1 = new PersistentEmbeddingCache(dir, "fp1", 4);
        c1.put(key, new float[]{1f, 2f, 3f, 4f});
        c1.save();

        PersistentEmbeddingCache c2 = new PersistentEmbeddingCache(dir, "fp1", 8);
        assertThat(c2.size()).isEqualTo(0);
    }

    // ── hit / miss stats ──────────────────────────────────────────────────

    @Test
    void stats_trackHitsAndMisses() {
        String k1 = PersistentEmbeddingCache.sha256Hex("alpha");
        PersistentEmbeddingCache cache = new PersistentEmbeddingCache(dir, "fp1", 4);
        cache.put(k1, new float[]{1f, 2f, 3f, 4f});

        String missing = PersistentEmbeddingCache.sha256Hex("no such text");
        cache.get(k1);       // hit
        cache.get(missing);  // miss
        cache.get(k1);       // hit

        assertThat(cache.hits()).isEqualTo(2);
        assertThat(cache.misses()).isEqualTo(1);
    }

    @Test
    void stats_freshCache_bothZero() {
        PersistentEmbeddingCache cache = new PersistentEmbeddingCache(dir, "fp1", 4);
        assertThat(cache.hits()).isEqualTo(0);
        assertThat(cache.misses()).isEqualTo(0);
    }

    // ── sha256Hex ─────────────────────────────────────────────────────────

    @Test
    void sha256Hex_sameSameInput_sameHash() {
        String h1 = PersistentEmbeddingCache.sha256Hex("hello world");
        String h2 = PersistentEmbeddingCache.sha256Hex("hello world");
        assertThat(h1).isEqualTo(h2);
    }

    @Test
    void sha256Hex_differentInputs_differentHashes() {
        String h1 = PersistentEmbeddingCache.sha256Hex("alpha");
        String h2 = PersistentEmbeddingCache.sha256Hex("beta");
        assertThat(h1).isNotEqualTo(h2);
    }

    @Test
    void sha256Hex_producesHexStringOf64Chars() {
        String h = PersistentEmbeddingCache.sha256Hex("test");
        assertThat(h).hasSize(64).matches("[0-9a-f]+");
    }
}
