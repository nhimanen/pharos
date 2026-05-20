package com.pharos.search;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.LuceneIndexer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Pins down the search-time guard added in
 * {@link SearchEngine#searchWithTrace(SearchRequest, boolean)}: when the
 * configured search-embedding model has no vectors for one of the queried
 * projects, the request must fail loud with {@link VectorModelUnavailableException}
 * rather than silently return empty results.
 */
class SearchModelMismatchTest {

    @TempDir
    Path tempDir;

    private IndexConfig config;
    private ProjectRegistry registry;
    private LuceneIndexer luceneIndexer;

    @BeforeEach
    void setUp() {
        config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));
        registry = new InMemoryRegistry();
        luceneIndexer = new LuceneIndexer(config);
    }

    @Test
    void vectorSearch_failsWhenProjectLacksTheConfiguredModel() {
        ProjectMeta meta = new ProjectMeta("proj-a", "/tmp/proj-a",
                tempDir.resolve("indexes/proj-a").toString());
        meta.setEmbeddedModels(List.of("model-A"));   // only model-A
        registry.register(meta);

        SearchEngine engine = new SearchEngine(
                luceneIndexer, new StubProvider("model-B"), registry);

        SearchRequest req = new SearchRequest("test",
                SearchRequest.SearchType.VECTOR, "proj-a", null, 10, "text",
                null, null, 0);

        assertThatThrownBy(() -> engine.searchWithTrace(req, false))
                .isInstanceOf(VectorModelUnavailableException.class)
                .hasMessageContaining("proj-a")
                .hasMessageContaining("model-B")
                .hasMessageContaining("model-A")               // names the available one
                .hasMessageContaining("pharos embed --model=model-B proj-a"); // suggests the fix
    }

    @Test
    void keywordSearch_doesNotTriggerVectorValidation() {
        // A project that has NO vectors for the configured model should still
        // serve a pure-keyword query without complaint.
        ProjectMeta meta = new ProjectMeta("proj-a", "/tmp/proj-a",
                tempDir.resolve("indexes/proj-a").toString());
        meta.setEmbeddedModels(List.of("model-A"));
        registry.register(meta);

        SearchEngine engine = new SearchEngine(
                luceneIndexer, new StubProvider("model-B"), registry);

        SearchRequest req = new SearchRequest("test",
                SearchRequest.SearchType.KEYWORD, "proj-a", null, 10, "text",
                null, null, 0);

        // Keyword search hits openMultiReader which will fail on the missing
        // index dir — but the *validation* path must not throw before that.
        // We assert specifically that VectorModelUnavailableException is NOT
        // raised; any other exception from openMultiReader is acceptable.
        try {
            engine.searchWithTrace(req, false);
        } catch (VectorModelUnavailableException e) {
            failBecauseExceptionWasNotThrown(VectorModelUnavailableException.class);
        } catch (Exception ignored) {
            // expected: missing index dir produces some other failure mode
        }
    }

    @Test
    void vectorSearch_succeedsWhenLegacyProjectMatchesLegacyEmbedder() {
        // Pre-upgrade index: embeddedModels is empty, the search-embedding model
        // is the synthesized LEGACY_MODEL_ID. Validation must pass; the strategy
        // will then read from the legacy vectorEmbedding field.
        ProjectMeta meta = new ProjectMeta("legacy-proj", "/tmp/legacy-proj",
                tempDir.resolve("indexes/legacy-proj").toString());
        meta.setEmbeddedModels(List.of());            // empty — pre-upgrade
        registry.register(meta);

        SearchEngine engine = new SearchEngine(
                luceneIndexer, new StubProvider(IndexConfig.LEGACY_MODEL_ID), registry);

        SearchRequest req = new SearchRequest("test",
                SearchRequest.SearchType.VECTOR, "legacy-proj", null, 10, "text",
                null, null, 0);

        // Validation passes — no exception from our code. The underlying
        // openMultiReader will likely fail (no index on disk) but that's a
        // different code path, not the validation.
        assertThatThrownBy(() -> engine.searchWithTrace(req, false))
                .isNotInstanceOf(VectorModelUnavailableException.class);
    }

    @Test
    void vectorSearch_succeedsWhenAllProjectsHaveTheModel() {
        ProjectMeta a = new ProjectMeta("proj-a", "/tmp/proj-a",
                tempDir.resolve("indexes/proj-a").toString());
        a.setEmbeddedModels(List.of("model-A", "model-B"));
        ProjectMeta b = new ProjectMeta("proj-b", "/tmp/proj-b",
                tempDir.resolve("indexes/proj-b").toString());
        b.setEmbeddedModels(List.of("model-B"));
        registry.register(a);
        registry.register(b);

        SearchEngine engine = new SearchEngine(
                luceneIndexer, new StubProvider("model-B"), registry);

        SearchRequest req = new SearchRequest("test",
                SearchRequest.SearchType.HYBRID, null,
                List.of("proj-a", "proj-b"), 10, "text", null, null, 0);

        // Validation should pass. Downstream Lucene reader open fails (no real
        // index) but that's outside the scope of this test.
        assertThatThrownBy(() -> engine.searchWithTrace(req, false))
                .isNotInstanceOf(VectorModelUnavailableException.class);
    }

    @Test
    void unavailableEmbedder_skipsValidation() {
        // When the embedder isn't available (NoOp), vector search should fall
        // through to whatever the pipeline does for that case — the validation
        // gate is not the right place to fail.
        ProjectMeta meta = new ProjectMeta("proj-a", "/tmp/proj-a",
                tempDir.resolve("indexes/proj-a").toString());
        meta.setEmbeddedModels(List.of("model-Z"));   // anything; won't be checked
        registry.register(meta);

        SearchEngine engine = new SearchEngine(
                luceneIndexer, new com.pharos.embedding.NoOpEmbeddingProvider("model-B"),
                registry);

        SearchRequest req = new SearchRequest("test",
                SearchRequest.SearchType.VECTOR, "proj-a", null, 10, "text",
                null, null, 0);

        assertThatThrownBy(() -> engine.searchWithTrace(req, false))
                .isNotInstanceOf(VectorModelUnavailableException.class);
    }

    // ── Test doubles ──────────────────────────────────────────────────────────

    /** Minimal EmbeddingProvider stub used by the validation tests. */
    private static final class StubProvider implements EmbeddingProvider {
        private final String modelId;
        StubProvider(String modelId) { this.modelId = modelId; }
        @Override public String modelId() { return modelId; }
        @Override public float[] embed(String text) { return new float[4]; }
        @Override public int dimensions() { return 4; }
        @Override public boolean isAvailable() { return true; }
    }

    /** In-memory registry stub — same pattern as RemoveIndexCommandTest. */
    private static final class InMemoryRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();
        InMemoryRegistry() { super(IndexConfig.defaults()); }
        @Override public synchronized void register(ProjectMeta meta) {
            store.put(meta.getName(), meta);
        }
        @Override public synchronized Optional<ProjectMeta> find(String name) {
            return Optional.ofNullable(store.get(name));
        }
        @Override public synchronized List<ProjectMeta> listAll() {
            return new ArrayList<>(store.values());
        }
        @Override public synchronized void unregister(String name) { store.remove(name); }
        @Override public synchronized void link(String p1, String p2) {}
        @Override public synchronized void unlinkAll(String name) {}
    }
}
