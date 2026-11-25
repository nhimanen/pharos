package com.pharos.indexer;

import com.pharos.embedding.EmbeddingProvider;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Test double: deterministic 8-dim vectors, counts every text it embeds; an optional hook runs per batch. */
public final class CountingEmbedder implements EmbeddingProvider {
    public final AtomicInteger calls = new AtomicInteger();
    public volatile Consumer<Integer> onBatch = n -> {};

    @Override public String modelId() { return "test-counting"; }
    @Override public int dimensions() { return 8; }
    @Override public boolean isAvailable() { return true; }

    @Override public float[] embed(String text) {
        calls.incrementAndGet();
        float[] v = new float[8];
        int h = text.hashCode();
        for (int i = 0; i < 8; i++) v[i] = ((h >>> (i * 4)) & 0xF) / 15f + 0.01f;
        return v;
    }

    @Override public float[][] embedBatch(List<String> texts) {
        onBatch.accept(texts.size());
        float[][] out = new float[texts.size()][];
        for (int i = 0; i < texts.size(); i++) out[i] = embed(texts.get(i));
        return out;
    }
}
