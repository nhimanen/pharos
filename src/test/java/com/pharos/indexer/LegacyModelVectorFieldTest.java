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

    private static Document vecDoc(String id, String field, float... v) {
        Document d = new Document();
        d.add(new StringField(DocumentMapper.F_ID, id, Field.Store.YES));
        d.add(new KnnFloatVectorField(field, v, VectorSimilarityFunction.COSINE));
        return d;
    }

    private static ByteBuffersDirectory dirOf(Document... docs) throws Exception {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            for (Document d : docs) w.addDocument(d);
        }
        return dir;
    }

    private static List<SearchResult> vectorSearch(org.apache.lucene.index.IndexReader r) throws Exception {
        return new VectorSearchStrategy(new FakeProvider("legacy"))
                .search(r, new SearchRequest("q", SearchRequest.SearchType.VECTOR, null, null, 10, "text", null, null, 0));
    }

    /** Indexes written by 1.4.0/1.4.1 hold legacy-model vectors in vec.legacy; they must stay searchable. */
    @Test
    void indexWithOnlyMisnamedVecLegacy_isStillSearchable() throws Exception {
        try (DirectoryReader r = DirectoryReader.open(dirOf(vecDoc("p:x", "vec.legacy", 1, 0, 0, 0)))) {
            assertThat(vectorSearch(r)).hasSize(1);
        }
    }

    /** A partially re-embedded index holds both kinds of docs; neither may become invisible. */
    @Test
    void singleIndexWithBothFields_returnsHitsFromBoth() throws Exception {
        try (DirectoryReader r = DirectoryReader.open(dirOf(
                vecDoc("p:old", "vec.legacy", 1, 0, 0, 0),
                vecDoc("p:new", "vectorEmbedding", 0, 1, 0, 0)))) {
            assertThat(vectorSearch(r)).extracting(SearchResult::id).containsExactlyInAnyOrder("p:old", "p:new");
        }
    }

    /** Multi-project search: an old project (vec.legacy) next to a new one (vectorEmbedding). */
    @Test
    void multiReaderMixingOldAndNewProjects_returnsHitsFromBoth() throws Exception {
        try (DirectoryReader a = DirectoryReader.open(dirOf(vecDoc("old:x", "vec.legacy", 1, 0, 0, 0)));
             DirectoryReader b = DirectoryReader.open(dirOf(vecDoc("new:y", "vectorEmbedding", 0, 1, 0, 0)));
             org.apache.lucene.index.MultiReader mr = new org.apache.lucene.index.MultiReader(a, b)) {
            assertThat(vectorSearch(mr)).extracting(SearchResult::id).containsExactlyInAnyOrder("old:x", "new:y");
        }
    }

    /** `pharos embed` on a vec.legacy-only index migrates the docs to vectorEmbedding. */
    @Test
    void embedOnVecLegacyIndex_migratesToVectorEmbedding() throws Exception {
        Path root = tmp.resolve("oldproj");
        Path pkg = Files.createDirectories(root.resolve("src/main/java/com/example"));
        Path src = pkg.resolve("C.java");
        Files.writeString(src, "package com.example;\npublic class C { public void callee() { int a = 1; } }\n");

        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes2"));
        LuceneIndexer lucene = new LuceneIndexer(config);
        SelectiveIndexUpdateTest.TestRegistry registry = new SelectiveIndexUpdateTest.TestRegistry();
        ProjectMeta meta = new ProjectMeta("old", root.toString(), lucene.getProjectIndexDir("old").toString());
        registry.register(meta);

        var parsed = new com.pharos.parser.JavaCodeParser().parseFile(src, "old", root);
        List<String> lines = Files.readAllLines(src);
        try (IndexWriter w = lucene.openWriter("old")) {
            for (var m : parsed.methods()) {
                Document d = DocumentMapper.toDocument(m, (float[]) null, 0, List.of(), lines);
                d.add(new KnnFloatVectorField("vec.legacy", new float[]{1, 0, 0, 0}, VectorSimilarityFunction.COSINE));
                w.addDocument(d);
            }
            w.commit();
        }

        int updated = new MultiModelEmbedder(config, lucene, registry)
                .embed(meta, new FakeProvider("legacy"), "fp", false);
        assertThat(updated).isGreaterThan(0);

        try (DirectoryReader r = DirectoryReader.open(new org.apache.lucene.store.NIOFSDirectory(lucene.getLucenePath("old")))) {
            boolean canonical = false, misnamed = false;
            for (LeafReaderContext c : r.leaves()) {
                canonical |= c.reader().getFieldInfos().fieldInfo("vectorEmbedding") != null;
                var fi = c.reader().getFieldInfos().fieldInfo("vec.legacy");
                misnamed |= fi != null && c.reader().getFloatVectorValues("vec.legacy") != null
                        && c.reader().getFloatVectorValues("vec.legacy").size() > 0;
            }
            assertThat(canonical).as("rewritten docs use vectorEmbedding").isTrue();
            assertThat(misnamed).as("no doc keeps vec.legacy after the rewrite").isFalse();
        }
        assertThat(registry.find("old").orElseThrow().getEmbeddedModels()).contains("legacy");
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
