package com.pharos.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.graph.ModuleGraphBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Embedding caches must survive the fingerprint-format change and must not interfere across
 * models.
 *
 * <p>Before multi-provider embeddings the fingerprint was {@code url:dims}; it is now
 * {@code modelId:dims:url:model}. Every cache written before the upgrade looked like it belonged
 * to a different model, was deleted on load, and the next re-index re-embedded the whole
 * project from scratch (hours on CPU for a large codebase).
 */
class EmbeddingCacheUpgradeTest {

    @TempDir Path tmp;
    private static final ObjectMapper JSON = new ObjectMapper();

    // ── IndexVersions ────────────────────────────────────────────────────────

    @Test
    void legacyFingerprints_ofMigratedLegacyProvider_isThePreUpgradeFormat() {
        EmbeddingProviderConfig legacy = legacyProvider("hf://jinaai/jina-embeddings-v2-base-code", 768);

        assertThat(IndexVersions.modelFingerprint(legacy))
                .isEqualTo("legacy:768:hf://jinaai/jina-embeddings-v2-base-code:");
        // exactly what pre-upgrade pharos wrote into embed-cache.meta and file-state.json
        assertThat(IndexVersions.legacyFingerprints(legacy))
                .containsExactly("hf://jinaai/jina-embeddings-v2-base-code:768");
    }

    @Test
    void legacyFingerprints_emptyWithoutUrl() {
        EmbeddingProviderConfig noUrl = legacyProvider(null, 768);
        assertThat(IndexVersions.legacyFingerprints(noUrl)).isEmpty();
    }

    // ── PersistentEmbeddingCache ─────────────────────────────────────────────

    @Test
    void cache_acceptsLegacyFingerprint_andRewritesItInCurrentFormat() throws Exception {
        String key = PersistentEmbeddingCache.sha256Hex("text");
        PersistentEmbeddingCache old = new PersistentEmbeddingCache(tmp, "hf://m:4", 4);
        old.put(key, new float[]{1, 2, 3, 4});
        old.save();

        PersistentEmbeddingCache upgraded = PersistentEmbeddingCache.forModel(
                tmp, IndexConfig.LEGACY_MODEL_ID, "legacy:4:hf://m:", List.of("hf://m:4"), 4);
        assertThat(upgraded.get(key)).containsExactly(1, 2, 3, 4);

        upgraded.save();
        assertThat(meta(tmp.resolve("embed-cache.meta")).get("fingerprint")).isEqualTo("legacy:4:hf://m:");
    }

    @Test
    void cache_withOtherModelsFingerprint_isIgnoredButNotDeleted() {
        String key = PersistentEmbeddingCache.sha256Hex("text");
        PersistentEmbeddingCache a = new PersistentEmbeddingCache(tmp, "model-a:4", 4);
        a.put(key, new float[]{1, 2, 3, 4});
        a.save();

        PersistentEmbeddingCache b = new PersistentEmbeddingCache(tmp, "model-b:4", 4);
        assertThat(b.size()).isZero();
        assertThat(tmp.resolve("embed-cache.bin")).exists();
        assertThat(new PersistentEmbeddingCache(tmp, "model-a:4", 4).get(key)).isNotNull();
    }

    @Test
    void perModelCaches_doNotClobberEachOther() {
        String key = PersistentEmbeddingCache.sha256Hex("text");
        PersistentEmbeddingCache legacy = PersistentEmbeddingCache.forModel(tmp, "legacy", "fp-legacy", List.of(), 4);
        PersistentEmbeddingCache other  = PersistentEmbeddingCache.forModel(tmp, "Qwen/Qwen3@1024", "fp-qwen", List.of(), 4);
        legacy.put(key, new float[]{1, 1, 1, 1});
        other.put(key, new float[]{2, 2, 2, 2});
        legacy.save();
        other.save();

        assertThat(other.dataFile().getFileName().toString()).isEqualTo("embed-cache.qwen_qwen3_1024.bin");
        assertThat(PersistentEmbeddingCache.forModel(tmp, "legacy", "fp-legacy", List.of(), 4).get(key))
                .containsExactly(1, 1, 1, 1);
        assertThat(PersistentEmbeddingCache.forModel(tmp, "Qwen/Qwen3@1024", "fp-qwen", List.of(), 4).get(key))
                .containsExactly(2, 2, 2, 2);
    }

    @Test
    void perModelCache_adoptsSharedFileWrittenForTheSameModel_only() {
        String key = PersistentEmbeddingCache.sha256Hex("text");
        PersistentEmbeddingCache shared = new PersistentEmbeddingCache(tmp, "fp-x", 4);
        shared.put(key, new float[]{3, 3, 3, 3});
        shared.save();

        assertThat(PersistentEmbeddingCache.forModel(tmp, "model-x", "fp-x", List.of(), 4).get(key)).isNotNull();
        assertThat(PersistentEmbeddingCache.forModel(tmp, "model-y", "fp-y", List.of(), 4).get(key)).isNull();
    }

    // ── FileStateTracker ─────────────────────────────────────────────────────

    @Test
    void tracker_treatsLegacyFingerprintAsCurrent() throws Exception {
        Path src = Files.writeString(tmp.resolve("A.java"), "class A {}");
        FileStateTracker old = new FileStateTracker(tmp, 4, "hf://m:4");
        old.track(src);
        old.save();

        assertThat(new FileStateTracker(tmp, 4, "legacy:4:hf://m:").hasOutdatedEmbeddings())
                .as("without aliases the old stamp looks like another model").isTrue();
        FileStateTracker upgraded = new FileStateTracker(tmp, 4, "legacy:4:hf://m:", List.of("hf://m:4"));
        assertThat(upgraded.hasOutdatedEmbeddings()).isFalse();
        assertThat(upgraded.isEmbeddingOutdated(src)).isFalse();
    }

    // ── End to end: upgrading pharos does not re-embed an indexed project ───

    @Nested
    class Upgrade {
        Path projectRoot;
        IndexConfig config;
        SelectiveIndexUpdateTest.CountingEmbeddingProvider embedder;
        ProjectIndexManager manager;

        @BeforeEach
        void setUp() throws Exception {
            projectRoot = tmp.resolve("project");
            Path pkg = Files.createDirectories(projectRoot.resolve("src/main/java/com/example"));
            Files.writeString(pkg.resolve("A.java"), "package com.example;\npublic class A { public void alpha() {} }");
            Files.writeString(pkg.resolve("B.java"), "package com.example;\npublic class B { public void beta() {} }");

            // Same shape IndexConfig.load() synthesizes from an old {embeddingModelUrl, embeddingDimensions} config.
            config = IndexConfig.defaults();
            config.setIndexDir(tmp.resolve("indexes"));
            config.getEmbeddingProviders().add(legacyProvider("test-model", 4));
            embedder = new SelectiveIndexUpdateTest.CountingEmbeddingProvider(4) {
                @Override public String modelId() { return IndexConfig.LEGACY_MODEL_ID; }
            };
            SelectiveIndexUpdateTest.TestRegistry registry = new SelectiveIndexUpdateTest.TestRegistry();
            ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
                @Override
                public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                        com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                    return List.of();
                }
            };
            manager = new ProjectIndexManager(config, new LuceneIndexer(config), registry, embedder, noopGraph);
        }

        @Test
        void reindexAfterUpgrade_servesEveryEmbeddingFromThePreUpgradeCache() throws Exception {
            manager.index(projectRoot, "test", false, true);
            assertThat(embedder.callCount()).isGreaterThan(0);
            Path dir = config.getIndexDir().resolve("test");
            assertThat(dir.resolve("embed-cache.bin")).as("legacy model keeps the unsuffixed cache file").exists();

            downgradeToPreUpgradeFingerprints(dir, "test-model:4");

            embedder.reset();
            manager.index(projectRoot, "test", false, true);   // full re-index
            assertThat(embedder.callCount()).as("all embeddings served from the old cache").isZero();
        }

        @Test
        void incrementalAfterUpgrade_doesNotMarkUnchangedFilesStale() throws Exception {
            manager.index(projectRoot, "test", false, true);
            Path dir = config.getIndexDir().resolve("test");
            downgradeToPreUpgradeFingerprints(dir, "test-model:4");

            FileStateTracker tracker = new FileStateTracker(dir, IndexVersions.CHUNKING_VERSION,
                    "legacy:4:test-model:", IndexVersions.legacyFingerprints(legacyProvider("test-model", 4)));
            assertThat(tracker.hasOutdatedEmbeddings()).isFalse();

            embedder.reset();
            manager.index(projectRoot, "test", true, true);    // incremental, nothing changed
            assertThat(embedder.callCount()).isZero();
        }

        /** Rewrites cache meta + file-state.json as a pre-multi-provider pharos left them. */
        private void downgradeToPreUpgradeFingerprints(Path dir, String oldFp) throws Exception {
            Path meta = dir.resolve("embed-cache.meta");
            ObjectNode m = (ObjectNode) JSON.readTree(meta.toFile());
            m.put("fingerprint", oldFp);
            JSON.writeValue(meta.toFile(), m);

            Path state = dir.resolve("file-state.json");
            String s = Files.readString(state);
            assertThat(s).contains("legacy:4:test-model:");
            Files.writeString(state, s.replace("legacy:4:test-model:", oldFp));
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** Mirrors IndexConfig#migrateLegacyEmbeddingConfig. */
    private static EmbeddingProviderConfig legacyProvider(String url, int dims) {
        EmbeddingProviderConfig c = new EmbeddingProviderConfig();
        c.setType("djl");
        c.setModelId(IndexConfig.LEGACY_MODEL_ID);
        c.setUrl(url);
        c.setDimensions(dims);
        c.setMaxTokens(512);
        return c;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> meta(Path p) throws Exception {
        return JSON.readValue(p.toFile(), Map.class);
    }
}
