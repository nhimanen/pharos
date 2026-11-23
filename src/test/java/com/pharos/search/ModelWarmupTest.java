package com.pharos.search;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.embedding.LazyEmbeddingProvider;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.indexer.LuceneIndexer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The background model warm-up a reader's readiness waits for: it settles as loaded, failed (loudly,
 * never blocking forever) or timed out, and a failed model turns into a search warning.
 */
class ModelWarmupTest {

    @TempDir Path tmp;

    private LuceneIndexer lucene;
    private ProjectRegistry registry;

    @BeforeEach
    void setUp() {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        lucene = new LuceneIndexer(config);
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
    }

    @AfterEach
    void tearDown() throws Exception {
        lucene.close();
    }

    /** An available provider that counts embeds. */
    private static final class Working implements EmbeddingProvider {
        final AtomicInteger embeds = new AtomicInteger();
        @Override public String modelId() { return "m"; }
        @Override public float[] embed(String text) { embeds.incrementAndGet(); return new float[] {1, 0}; }
        @Override public int dimensions() { return 2; }
        @Override public boolean isAvailable() { return true; }
    }

    private SearchEngine engine(EmbeddingProvider p) {
        return new SearchEngine(lucene, p, registry);
    }

    @Test
    void noConfiguredModelIsSettledAtOnce() throws Exception {
        SearchEngine e = engine(new NoOpEmbeddingProvider());
        e.warmUpInBackground();
        SearchEngine.ModelWarmup w = e.modelWarmup().get(10, TimeUnit.SECONDS);
        assertThat(w.state()).isEqualTo(SearchEngine.ModelState.NONE);
        assertThat(e.modelState()).isEqualTo(SearchEngine.ModelState.NONE);
    }

    @Test
    void aLoadedModelIsReadyAfterOneWarmUpEmbed() throws Exception {
        Working p = new Working();
        SearchEngine e = engine(p);
        assertThat(e.modelState()).isEqualTo(SearchEngine.ModelState.LOADING);
        e.warmUpInBackground();
        SearchEngine.ModelWarmup w = e.modelWarmup().get(10, TimeUnit.SECONDS);
        assertThat(w.state()).isEqualTo(SearchEngine.ModelState.READY);
        assertThat(w.failure()).isNull();
        assertThat(p.embeds).hasValue(1);
    }

    /** A configured model that fails to load settles as FAILED and every vector search says so. */
    @Test
    void aModelThatFailsToLoadSettlesAsFailedAndWarnsOnVectorSearch() throws Exception {
        SearchEngine e = engine(new LazyEmbeddingProvider("jina-x", 768, () -> null));
        e.warmUpInBackground();
        SearchEngine.ModelWarmup w = e.modelWarmup().get(10, TimeUnit.SECONDS);
        assertThat(w.state()).isEqualTo(SearchEngine.ModelState.FAILED);
        assertThat(w.failure()).contains("jina-x");

        SearchResponse vec = e.searchWithTrace(new SearchRequest("anything", SearchRequest.SearchType.VECTOR,
                null, null, 10, "text", null, null, 0), false);
        assertThat(vec.warnings()).anySatisfy(s -> assertThat(s).contains("vector search unavailable")
                .contains("jina-x"));
        SearchResponse kw = e.searchWithTrace(new SearchRequest("anything", SearchRequest.SearchType.KEYWORD,
                null, null, 10, "text", null, null, 0), false);
        assertThat(kw.warnings()).noneSatisfy(s -> assertThat(s).contains("vector search unavailable"));
    }

    /** A load that hangs does not block readiness forever: the settled future times out. */
    @Test
    void aHangingLoadSettlesAfterTheTimeout() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        EmbeddingProvider hanging = new LazyEmbeddingProvider("slow", 2, () -> {
            try {
                release.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return new Working();
        });
        SearchEngine e = engine(hanging);
        try {
            e.warmUpInBackground();
            SearchEngine.ModelWarmup w = e.modelsSettled(Duration.ofMillis(200)).get(10, TimeUnit.SECONDS);
            assertThat(w.state()).isEqualTo(SearchEngine.ModelState.FAILED);
            assertThat(w.failure()).contains("timed out");
            assertThat(e.modelWarmup()).as("the real load is still running").isNotDone();
        } finally {
            release.countDown();
        }
        assertThat(e.modelWarmup().get(10, TimeUnit.SECONDS).state()).isEqualTo(SearchEngine.ModelState.READY);
        assertThat(List.of(e.modelState())).containsExactly(SearchEngine.ModelState.READY);
    }
}
