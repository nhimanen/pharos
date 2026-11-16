package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.VectorSearchStrategy;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Index-time and search-time vector field naming must agree for every model id, in particular
 * the synthesized {@code legacy} one (old single-model config), which search reads from
 * {@code vectorEmbedding}. Previously the writers used {@code vec.legacy}, so vector search on
 * a freshly built index returned nothing.
 */
class LegacyModelVectorFieldTest {

    @TempDir Path tmp;

    @ParameterizedTest
    @ValueSource(strings = {"legacy", "jina-code-v2"})
    void fullIndexThenVectorSearch_returnsResults(String modelId) throws Exception {
        Path root = tmp.resolve("project");
        Path pkg = Files.createDirectories(root.resolve("src/main/java/com/example"));
        Files.writeString(pkg.resolve("A.java"), "package com.example;\npublic class A { public void alphaMethod() {} }");
        Files.writeString(pkg.resolve("B.java"), "package com.example;\npublic class B { public void betaMethod() {} }");

        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        LuceneIndexer lucene = new LuceneIndexer(config);
        SelectiveIndexUpdateTest.TestRegistry registry = new SelectiveIndexUpdateTest.TestRegistry();
        EmbeddingProvider provider = new FakeProvider(modelId);
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path r, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) { return List.of(); }
        };
        ProjectIndexManager manager = new ProjectIndexManager(config, lucene, registry, List.of(provider), noopGraph);
        manager.index(root, "test", true, true);

        SearchEngine engine = new SearchEngine(lucene, provider, registry);
        List<SearchResult> vec = engine.search(new SearchRequest("alphaMethod",
                SearchRequest.SearchType.VECTOR, "test", null, 10, "text", null, null, 0));
        assertThat(vec).as("vector search on a fresh index with model '%s'", modelId).isNotEmpty();

        try (DirectoryReader r = DirectoryReader.open(
                new org.apache.lucene.store.NIOFSDirectory(lucene.getLucenePath("test")))) {
            String expected = DocumentMapper.vectorFieldFor(modelId);
            boolean found = false;
            for (LeafReaderContext c : r.leaves()) found |= c.reader().getFieldInfos().fieldInfo(expected) != null;
            assertThat(found).as("writer must use the field readers resolve: " + expected).isTrue();
            if (!"legacy".equals(modelId)) assertThat(expected).isEqualTo("vec." + modelId);
        }
    }

    // ── Migration of indexes written by 1.4.0-1.4.1 (vectors under vec.legacy) ──────────────

    private IndexConfig config;
    private LuceneIndexer lucene;
    private SelectiveIndexUpdateTest.TestRegistry registry;
    private ProjectIndexManager manager;
    private Path root;

    private void setUpProject(EmbeddingProvider provider) throws Exception {
        root = tmp.resolve("mig");
        Path pkg = Files.createDirectories(root.resolve("src/main/java/com/example"));
        Files.writeString(pkg.resolve("A.java"), "package com.example;\npublic class A { public void alphaMethod() { int a = 1; } }");
        Files.writeString(pkg.resolve("B.java"), "package com.example;\npublic class B { public void betaMethod() { int b = 2; } }");
        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes-mig"));
        lucene = new LuceneIndexer(config);
        registry = new SelectiveIndexUpdateTest.TestRegistry();
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path r, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) { return List.of(); }
        };
        manager = new ProjectIndexManager(config, lucene, registry, List.of(provider), noopGraph);
    }

    /** Turns a healthy index into what 1.4.0/1.4.1 wrote: every doc's vector under vec.legacy. */
    private void downgradeToMisnamed() throws Exception {
        var parser = new com.pharos.parser.JavaCodeParser();
        try (IndexWriter w = lucene.openWriter("mig")) {
            w.deleteAll();
            for (String f : List.of("A", "B")) {
                Path src = root.resolve("src/main/java/com/example/" + f + ".java");
                List<String> lines = Files.readAllLines(src);
                for (var m : parser.parseFile(src, "mig", root).methods()) {
                    Document d = DocumentMapper.toDocument(m, (float[]) null, 0, List.of(), lines);
                    d.add(new KnnFloatVectorField("vec.legacy", new float[]{1, 0, 0, 0}, VectorSimilarityFunction.COSINE));
                    w.addDocument(d);
                }
            }
            w.commit();
        }
    }

    private boolean liveMisnamed() throws Exception {
        try (DirectoryReader r = DirectoryReader.open(new org.apache.lucene.store.NIOFSDirectory(lucene.getLucenePath("mig")))) {
            return LegacyVectorFields.hasLiveMisnamedVectors(r);
        }
    }

    private int docsWithChunkVectors() throws Exception {
        try (DirectoryReader r = DirectoryReader.open(new org.apache.lucene.store.NIOFSDirectory(lucene.getLucenePath("mig")))) {
            int n = 0;
            for (LeafReaderContext c : r.leaves()) {
                var bdv = c.reader().getBinaryDocValues(DocumentMapper.F_CHUNK_VECTORS_LEGACY);
                var live = c.reader().getLiveDocs();
                if (bdv == null) continue;
                for (int d = bdv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = bdv.nextDoc()) {
                    if (live == null || live.get(d)) n++;
                }
            }
            return n;
        }
    }

    /** A method larger than the chunker's window, so it gets several chunk vectors. */
    private void addBigMethod() throws Exception {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < 400; i++) body.append("        int v").append(i).append(" = ").append(i).append(" * 31;\n");
        Files.writeString(root.resolve("src/main/java/com/example/Big.java"),
                "package com.example;\npublic class Big {\n    public void huge() {\n" + body + "    }\n}\n");
    }

    @Test
    void indexOnVecLegacyIndex_runsFullReindexServedFromTheCache() throws Exception {
        CountingProvider provider = new CountingProvider("legacy");
        setUpProject(provider);
        addBigMethod();
        manager.index(root, "mig", false, true);            // seeds the full-index embedding cache
        assertThat(docsWithChunkVectors()).as("multi-chunk method has chunk vectors").isGreaterThan(0);
        downgradeToMisnamed();
        assertThat(liveMisnamed()).isTrue();
        int before = provider.calls.get();

        manager.index(root, "mig", true, true);             // plain `pharos index`, no --full

        assertThat(liveMisnamed()).as("no live vec.legacy / chunkVec.legacy doc left").isFalse();
        assertThat(provider.calls.get()).as("full re-index reuses the cached full-index embeddings").isEqualTo(before);
        assertThat(docsWithChunkVectors()).as("multi-chunk methods keep their chunk vectors").isGreaterThan(0);
        SearchEngine engine = new SearchEngine(lucene, provider, registry);
        assertThat(engine.search(new SearchRequest("alphaMethod", SearchRequest.SearchType.VECTOR,
                "mig", null, 10, "text", null, null, 0))).isNotEmpty();
    }

    @Test
    void healthyLegacyIndexStaysIncremental() throws Exception {
        CountingProvider provider = new CountingProvider("legacy");
        setUpProject(provider);
        manager.index(root, "mig", false, true);
        java.util.List<String> msgs = new java.util.ArrayList<>();
        manager.index(root, "mig", true, true, (stage, done, total) -> msgs.add(stage));
        assertThat(msgs).noneMatch(m -> m.contains("full re-index"));
    }

    @Test
    void namedModelIsNotReembeddedOnIncrementalRun() throws Exception {
        CountingProvider provider = new CountingProvider("jina-code-v2");
        setUpProject(provider);
        manager.index(root, "mig", false, true);
        int calls = provider.calls.get();
        manager.index(root, "mig", true, true);
        assertThat(provider.calls.get()).isEqualTo(calls);
        assertThat(liveMisnamed()).isFalse();
    }

    @Test
    void embedOnVecLegacyIndex_endsClean() throws Exception {
        CountingProvider provider = new CountingProvider("legacy");
        setUpProject(provider);
        manager.index(root, "mig", false, true);
        downgradeToMisnamed();
        ProjectMeta meta = registry.find("mig").orElseThrow();

        int updated = new MultiModelEmbedder(config, lucene, registry).embed(meta, provider, "fp", false);

        assertThat(updated).isGreaterThan(0);
        assertThat(liveMisnamed()).as("no live vec.legacy / chunkVec.legacy doc left").isFalse();
        assertThat(registry.find("mig").orElseThrow().getEmbeddedModels()).contains("legacy");
    }

    static class CountingProvider extends FakeProvider {
        final java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        CountingProvider(String id) { super(id); }
        @Override public float[] embed(String text) { calls.incrementAndGet(); return super.embed(text); }
    }

    static class FakeProvider implements EmbeddingProvider {
        private final String id;
        FakeProvider(String id) { this.id = id; }
        @Override public String modelId() { return id; }
        @Override public float[] embed(String text) {
            if (text == null) return null;
            float[] v = new float[4];
            int h = text.hashCode();
            for (int i = 0; i < 4; i++) v[i] = ((h >>> (i * 4)) & 0xFF) / 255f + 0.01f;
            return v;
        }
        @Override public float[][] embedBatch(List<String> texts) {
            float[][] out = new float[texts.size()][];
            for (int i = 0; i < out.length; i++) out[i] = embed(texts.get(i));
            return out;
        }
        @Override public int dimensions() { return 4; }
        @Override public boolean isAvailable() { return true; }
    }
}
