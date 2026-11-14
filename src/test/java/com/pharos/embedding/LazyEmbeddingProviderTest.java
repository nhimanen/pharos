package com.pharos.embedding;

import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class LazyEmbeddingProviderTest {

    @Test
    void configOnlyAccessors_doNotLoadTheModel() {
        AtomicInteger loads = new AtomicInteger();
        LazyEmbeddingProvider p = new LazyEmbeddingProvider("m", 768, () -> { loads.incrementAndGet(); return stub(); });

        assertThat(p.modelId()).isEqualTo("m");
        assertThat(p.dimensions()).isEqualTo(768);
        assertThat(p.isLoaded()).isFalse();
        assertThat(loads).hasValue(0);
    }

    @Test
    void firstUse_loadsOnce_andDelegates() {
        AtomicInteger loads = new AtomicInteger();
        LazyEmbeddingProvider p = new LazyEmbeddingProvider("m", 4, () -> { loads.incrementAndGet(); return stub(); });

        assertThat(p.isAvailable()).isTrue();
        assertThat(p.embed("x")).containsExactly(1, 2, 3, 4);
        assertThat(p.embedBatch(List.of("a", "b"))).hasDimensions(2, 4);
        assertThat(loads).hasValue(1);
    }

    @Test
    void concurrentFirstUse_constructsTheModelOnce() throws Exception {
        AtomicInteger loads = new AtomicInteger();
        LazyEmbeddingProvider p = new LazyEmbeddingProvider("m", 4, () -> {
            loads.incrementAndGet();
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            return stub();
        });
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch go = new CountDownLatch(1);
        for (int i = 0; i < 8; i++) pool.submit(() -> { go.await(); return p.embed("x"); });
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(loads).hasValue(1);
    }

    @Test
    void failedLoad_isUnavailableNotAnException() {
        LazyEmbeddingProvider p = new LazyEmbeddingProvider("m", 4, () -> new NoOpEmbeddingProvider("m"));
        assertThat(p.isAvailable()).isFalse();
        LazyEmbeddingProvider nullFactory = new LazyEmbeddingProvider("m", 4, () -> null);
        assertThat(nullFactory.isAvailable()).isFalse();
    }

    @Test
    void createAll_returnsUnloadedProviders() {
        IndexConfig config = IndexConfig.defaults();
        EmbeddingProviderConfig c = new EmbeddingProviderConfig();
        c.setType("djl");
        c.setModelId("never-loaded");
        c.setUrl("hf://does-not/exist");   // would fail (and download) if it were loaded
        c.setDimensions(8);
        config.getEmbeddingProviders().add(c);

        List<EmbeddingProvider> all = EmbeddingProvider.createAll(config);
        assertThat(all).singleElement().isInstanceOfSatisfying(LazyEmbeddingProvider.class, lp -> {
            assertThat(lp.isLoaded()).isFalse();
            assertThat(lp.modelId()).isEqualTo("never-loaded");
            assertThat(lp.dimensions()).isEqualTo(8);
        });
        assertThat(EmbeddingProvider.searchProvider(config, all)).isSameAs(all.get(0));
    }

    private static EmbeddingProvider stub() {
        return new EmbeddingProvider() {
            @Override public String modelId() { return "m"; }
            @Override public float[] embed(String text) { return new float[]{1, 2, 3, 4}; }
            @Override public int dimensions() { return 4; }
            @Override public boolean isAvailable() { return true; }
        };
    }
}
