package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code pharos embed --model} on an index whose ids are not unique.
 *
 * <p>Two files declaring the same package-private class produce two documents with the same id
 * (the remaining real-world source of duplicate ids, e.g. lucene's build-infra {@code Entry}).
 * The embedder must update both documents in place: not collapse them into one (the old
 * {@code updateDocument(Term(id))} deleted every doc with the id and wrote back one), and not
 * skip them all (the old lookup keyed parsed members by FQN without the project prefix, so no
 * Java doc ever matched).
 */
class MultiModelEmbedderTest {

    @TempDir Path tmp;

    private Path projectRoot;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private SelectiveIndexUpdateTest.TestRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        projectRoot = tmp.resolve("project");
        Path pkg = projectRoot.resolve("src/main/java/com/example");
        Files.createDirectories(pkg);
        Files.writeString(pkg.resolve("A.java"),
                "package com.example;\nclass Dup {\n  void run() { int a = 1; }\n}\n");
        Files.writeString(pkg.resolve("B.java"),
                "package com.example;\nclass Dup {\n  void run() { int b = 2; }\n}\n");
        Files.writeString(pkg.resolve("C.java"), """
                package com.example;
                public class C {
                  public void caller() { callee(); }
                  public void callee() {}
                }
                """);

        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        config.setEmbeddingModelUrl("test-model");
        config.setEmbeddingDimensions(4);
        luceneIndexer = new LuceneIndexer(config);
        registry = new SelectiveIndexUpdateTest.TestRegistry();
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        new ProjectIndexManager(config, luceneIndexer, registry,
                new SelectiveIndexUpdateTest.CountingEmbeddingProvider(4), noopGraph)
                .index(projectRoot, "test", false, true);
    }

    @Test
    void addingAModelKeepsEveryDuplicateIdDocAndEmbedsEachOne() throws Exception {
        assertThat(docs("test:com.example.Dup#run()")).hasSize(2);
        int totalBefore = numDocs();

        int updated = new MultiModelEmbedder(config, luceneIndexer, registry)
                .embed(registry.find("test").orElseThrow(), new NamedProvider("model-b", 4), "fp-b", false);

        assertThat(updated).as("Java docs must match their parsed members").isGreaterThan(0);
        assertThat(numDocs()).as("no document lost or duplicated").isEqualTo(totalBefore);

        List<Document> dups = docs("test:com.example.Dup#run()");
        assertThat(dups).hasSize(2);
        assertThat(dups).extracting(d -> d.get(DocumentMapper.F_FILE_PATH))
                .anySatisfy(p -> assertThat(p).endsWith("A.java"))
                .anySatisfy(p -> assertThat(p).endsWith("B.java"));
        // each rewritten doc keeps its own body (not the other file's)
        for (Document d : dups) {
            String expected = d.get(DocumentMapper.F_FILE_PATH).endsWith("A.java") ? "int a = 1" : "int b = 2";
            assertThat(d.get(DocumentMapper.F_BODY)).contains(expected);
        }
        assertThat(docsWithVectorField(DocumentMapper.vectorFieldName("model-b")))
                .as("both duplicate-id method docs got the new model's vector")
                .contains("test:com.example.Dup#run()@A.java", "test:com.example.Dup#run()@B.java");
    }

    @Test
    void addingAModelPreservesGraphDerivedFields() throws Exception {
        Document before = docs("test:com.example.C#callee()").get(0);
        int inDegreeBefore = before.getField(DocumentMapper.F_IN_DEGREE).numericValue().intValue();
        String callersBefore = before.get(DocumentMapper.F_CALLER_CONTEXT);
        assertThat(inDegreeBefore).isEqualTo(1);

        new MultiModelEmbedder(config, luceneIndexer, registry)
                .embed(registry.find("test").orElseThrow(), new NamedProvider("model-b", 4), "fp-b", false);

        Document after = docs("test:com.example.C#callee()").get(0);
        assertThat(after.getField(DocumentMapper.F_IN_DEGREE).numericValue().intValue()).isEqualTo(inDegreeBefore);
        assertThat(after.get(DocumentMapper.F_CALLER_CONTEXT)).isEqualTo(callersBefore);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private DirectoryReader open() throws Exception {
        return DirectoryReader.open(new NIOFSDirectory(luceneIndexer.getLucenePath("test")));
    }

    private int numDocs() throws Exception {
        try (DirectoryReader r = open()) { return r.numDocs(); }
    }

    private List<Document> docs(String id) throws Exception {
        try (DirectoryReader r = open()) {
            IndexSearcher s = new IndexSearcher(r);
            List<Document> out = new ArrayList<>();
            for (ScoreDoc sd : s.search(new TermQuery(new Term(DocumentMapper.F_ID, id)), 100).scoreDocs) {
                out.add(s.storedFields().document(sd.doc));
            }
            return out;
        }
    }

    /** "id@fileName" of every doc carrying a value for the given KNN field. */
    private List<String> docsWithVectorField(String field) throws Exception {
        try (DirectoryReader r = open()) {
            List<String> out = new ArrayList<>();
            for (LeafReaderContext ctx : ((IndexReader) r).leaves()) {
                var vals = ctx.reader().getFloatVectorValues(field);
                if (vals == null) continue;
                var it = vals.iterator();
                for (int d = it.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = it.nextDoc()) {
                    Document doc = ctx.reader().storedFields().document(d);
                    out.add(doc.get(DocumentMapper.F_ID) + "@"
                            + Path.of(doc.get(DocumentMapper.F_FILE_PATH)).getFileName());
                }
            }
            return out;
        }
    }

    /** Deterministic provider with a configurable model id. */
    static final class NamedProvider extends SelectiveIndexUpdateTest.CountingEmbeddingProvider {
        private final String id;
        NamedProvider(String id, int dims) { super(dims); this.id = id; }
        @Override public String modelId() { return id; }
    }
}
