package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for the selective-index-update feature:
 * persistent embedding cache, version-triggered dirty expansion, and
 * ONNX call elimination for unchanged embedding texts.
 *
 * <p>Uses a {@link CountingEmbeddingProvider} that returns deterministic
 * 4-dim vectors and counts how many texts actually went through the embedder
 * (as opposed to being served from the {@link PersistentEmbeddingCache}).
 */
class SelectiveIndexUpdateTest {

    @TempDir Path tempDir;

    private Path projectRoot;
    private Path indexDir;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private CountingEmbeddingProvider embedder;
    private ProjectIndexManager indexManager;

    @BeforeEach
    void setUp() throws IOException {
        projectRoot = tempDir.resolve("project");
        Files.createDirectories(projectRoot.resolve("src/main/java/com/example"));
        indexDir = tempDir.resolve("indexes");

        config = IndexConfig.defaults();
        config.setIndexDir(indexDir);
        // A fixed model URL makes the fingerprint deterministic: "test-model:4"
        config.setEmbeddingModelUrl("test-model");
        config.setEmbeddingDimensions(4);

        embedder = new CountingEmbeddingProvider(4);
        indexManager = buildManager();
    }

    // ── Cache populated on first index ────────────────────────────────────

    @Test
    void fullIndex_populatesEmbeddingCacheOnDisk() throws Exception {
        writeJava("A.java", "package com.example;\npublic class A { public void alpha() {} }");

        indexManager.index(projectRoot, "test", false, true);

        assertThat(indexDir.resolve("test/embed-cache.bin")).exists();
        assertThat(indexDir.resolve("test/embed-cache.meta")).exists();
        assertThat(embedder.callCount()).isGreaterThan(0);
    }

    // ── Second full index: identical content → all cache hits → 0 ONNX calls

    @Test
    void secondFullIndex_sameContent_zeroOnnxCalls() throws Exception {
        writeJava("A.java", "package com.example;\npublic class A { public void alpha() {} }");
        writeJava("B.java", "package com.example;\npublic class B { public void beta() {} }");

        indexManager.index(projectRoot, "test", false, true);
        assertThat(embedder.callCount()).isGreaterThan(0);

        embedder.reset();
        indexManager.index(projectRoot, "test", false, true);

        assertThat(embedder.callCount()).isEqualTo(0);
    }

    // ── Version bump: all tracked files are added to the dirty set ───────

    @Test
    void versionBump_allFilesAddedToDirtySet_allReembedded() throws Exception {
        writeJava("A.java", "package com.example;\npublic class A { public void alpha() {} }");
        writeJava("B.java", "package com.example;\npublic class B { public void beta() {} }");

        // Full index: CHUNKING_VERSION=1 stamped into file-state.json
        indexManager.index(projectRoot, "test", false, true);

        // Simulate a chunking version bump: downgrade stored chunkingVersion to 0
        // (normally this happens because CHUNKING_VERSION constant is bumped in code)
        downgradeChunkingVersion("test");

        // Incremental run with no source changes:
        //   hasOutdatedEmbeddings() → true → all tracked files added to dirty set
        //   both A.java and B.java are re-processed → embed count > 0
        embedder.reset();
        indexManager.index(projectRoot, "test", true, true);

        // All files must have been re-embedded (version-triggered dirty expansion worked)
        assertThat(embedder.callCount()).isGreaterThan(0);
    }

    // ── Full→full: same content → all cache hits → 0 ONNX calls ─────────

    @Test
    void fullIndex_cacheHits_eliminateOnnxCallsOnRepeatRun() throws Exception {
        writeJava("A.java", "package com.example;\npublic class A { public void alpha() {} }");
        writeJava("B.java", "package com.example;\npublic class B { public void beta() {} }");

        // First full index: texts are new → all ONNX calls → cache populated
        indexManager.index(projectRoot, "test", false, true);
        assertThat(embedder.callCount()).isGreaterThan(0);

        // Second full index: same source → identical chunk texts → all cache hits → 0 ONNX
        embedder.reset();
        indexManager.index(projectRoot, "test", false, true);

        assertThat(embedder.callCount()).isEqualTo(0);
    }

    // ── Model change: cache invalidated → full re-embed ───────────────────

    @Test
    void modelChange_invalidatesCache_forcesFullReembed() throws Exception {
        writeJava("A.java", "package com.example;\npublic class A { public void alpha() {} }");

        indexManager.index(projectRoot, "test", false, true);
        int firstCount = embedder.callCount();
        assertThat(firstCount).isGreaterThan(0);

        // Switch model → fingerprint "test-model:4" → "other-model:4"
        // PersistentEmbeddingCache detects mismatch and invalidates on construction.
        // hasOutdatedEmbeddings() also returns true (modelFingerprint stored != current).
        config.setEmbeddingModelUrl("other-model");

        embedder.reset();
        indexManager.index(projectRoot, "test", true, true);

        assertThat(embedder.callCount()).isGreaterThan(0);
    }

    // ── New file in incremental → only that file's texts are embedded ─────

    @Test
    void incrementalIndex_newFile_onlyNewTextsEmbedded() throws Exception {
        // Give A.java enough methods that its embed count is clearly more than a single-method file
        writeJava("A.java",
                "package com.example;\npublic class A {\n" +
                "  public void m1() {}\n  public void m2() {}\n  public void m3() {}\n}");
        indexManager.index(projectRoot, "test", false, true);
        int fullCount = embedder.callCount(); // ≥ 4 (3 methods + 1 class)
        assertThat(fullCount).isGreaterThan(3);

        // Add B.java with one method; A.java is unchanged and not in the dirty set at all
        writeJava("B.java", "package com.example;\npublic class B { public void beta() {} }");
        embedder.reset();
        indexManager.index(projectRoot, "test", true, true);

        // Only B.java's texts reach the embedder — strictly fewer than the full-index count
        assertThat(embedder.callCount()).isGreaterThan(0).isLessThan(fullCount);
    }

    // ── Source change always re-embeds the changed file ───────────────────

    @Test
    void sourceDirtyFile_alwaysReembedded() throws Exception {
        writeJava("A.java", "package com.example;\npublic class A { public void alpha() {} }");
        indexManager.index(projectRoot, "test", false, true);

        // Modify method body — embedding text changes → cache miss → ONNX called
        Thread.sleep(50);
        writeJava("A.java",
                "package com.example;\npublic class A {\n" +
                "  public void alpha() { return; }\n" +
                "  public void newMethod() {}\n}");

        embedder.reset();
        indexManager.index(projectRoot, "test", true, true);

        assertThat(embedder.callCount()).isGreaterThan(0);
    }

    // ── After version bump + re-index, versions are updated ──────────────

    @Test
    void afterVersionBumpReindex_newVersionStamped_noFurtherOutdatedDetected() throws Exception {
        writeJava("A.java", "package com.example;\npublic class A { public void alpha() {} }");

        indexManager.index(projectRoot, "test", false, true);
        downgradeChunkingVersion("test");

        // First incremental after version bump → re-processes all, stamps new version
        indexManager.index(projectRoot, "test", true, true);

        // Second incremental → no version mismatch, no source changes → exits early
        embedder.reset();
        indexManager.index(projectRoot, "test", true, true);

        assertThat(embedder.callCount()).isEqualTo(0);
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private void writeJava(String filename, String content) throws IOException {
        Path file = projectRoot.resolve("src/main/java/com/example").resolve(filename);
        Files.writeString(file, content);
    }

    /**
     * Downgrades the stored chunkingVersion for all tracked files to 0,
     * simulating the state left by an older version of pharos before
     * {@code CHUNKING_VERSION} was introduced or bumped.
     */
    private void downgradeChunkingVersion(String projectName) throws IOException {
        Path stateFile = indexDir.resolve(projectName + "/file-state.json");
        String state = Files.readString(stateFile);
        // Jackson compact JSON: "chunkingVersion":N (no spaces)
        state = state.replace("\"chunkingVersion\":" + IndexVersions.CHUNKING_VERSION,
                              "\"chunkingVersion\":0");
        Files.writeString(stateFile, state);
    }

    private ProjectIndexManager buildManager() {
        TestRegistry registry = new TestRegistry();
        LuceneIndexer li = new LuceneIndexer(config);
        luceneIndexer = li;
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        return new ProjectIndexManager(config, li, registry, embedder, noopGraph);
    }

    // ── Inner: counting embedding stub ───────────────────────────────────

    static class CountingEmbeddingProvider implements EmbeddingProvider {
        private final int dims;
        private final AtomicInteger count = new AtomicInteger(0);

        CountingEmbeddingProvider(int dims) { this.dims = dims; }

        @Override
        public float[] embed(String text) {
            if (text == null) return null;
            count.incrementAndGet();
            // Deterministic vector derived from text hash so same text → same vector
            float[] v = new float[dims];
            int h = text.hashCode();
            for (int i = 0; i < dims; i++) v[i] = ((h >>> i) & 0xFF) / 255f;
            return v;
        }

        @Override
        public float[][] embedBatch(List<String> texts) {
            float[][] results = new float[texts.size()][];
            for (int i = 0; i < texts.size(); i++) results[i] = embed(texts.get(i));
            return results;
        }

        @Override public int dimensions() { return dims; }
        @Override public boolean isAvailable() { return true; }

        public int callCount() { return count.get(); }
        public void reset() { count.set(0); }
    }

    // ── Inner: in-memory registry ─────────────────────────────────────────

    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() { super(IndexConfig.defaults()); }

        @Override public synchronized void register(ProjectMeta m) { store.put(m.getName(), m); }
        @Override public synchronized Optional<ProjectMeta> find(String n) { return Optional.ofNullable(store.get(n)); }
        @Override public synchronized List<ProjectMeta> listAll() { return new ArrayList<>(store.values()); }
        @Override public synchronized void link(String a, String b) {}
        @Override public synchronized void unregister(String n) { store.remove(n); }
    }
}
