package com.pharos.search;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.parser.model.ParsedMethod;
import org.apache.lucene.index.IndexWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link SearchEngine#findMethodsByPartialFqn} — the fallback
 * used when an agent supplies a FQN without parameter types.
 */
class FindMethodsByPartialFqnTest {

    private static final String PROJECT = "test-project";
    private static final String PKG     = "com.example";
    private static final String CLASS   = "MyService";
    private static final String QCLASS  = PKG + "." + CLASS;

    @TempDir
    Path tempDir;

    private SearchEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));

        LuceneIndexer luceneIndexer = new LuceneIndexer(config);

        // In-memory registry — avoids writing to ~/.pharos
        ProjectRegistry registry = new TestRegistry();
        registry.register(new ProjectMeta(PROJECT, "/fake/path", tempDir.resolve("indexes").toString()));

        engine = new SearchEngine(luceneIndexer, new NoOpEmbeddingProvider(), registry);

        // Index three methods:
        //   MyService#uniqueMethod(String)     — only overload
        //   MyService#overloaded(String)       — overload 1
        //   MyService#overloaded(String,int)   — overload 2
        try (IndexWriter writer = luceneIndexer.openWriterFresh(PROJECT)) {
            writer.addDocument(DocumentMapper.toDocument(
                    method("uniqueMethod", List.of("String"), List.of("value")), null, 0, List.of()));
            writer.addDocument(DocumentMapper.toDocument(
                    method("overloaded", List.of("String"), List.of("s")), null, 0, List.of()));
            writer.addDocument(DocumentMapper.toDocument(
                    method("overloaded", List.of("String", "int"), List.of("s", "n")), null, 0, List.of()));
            writer.commit();
        }
    }

    @Test
    void uniqueMethod_returnsExactlyOneResult() throws Exception {
        List<SearchResult> results = engine.findMethodsByPartialFqn(QCLASS + "#uniqueMethod");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).methodName()).isEqualTo("uniqueMethod");
        assertThat(results.get(0).qualifiedClassName()).isEqualTo(QCLASS);
    }

    @Test
    void overloadedMethod_returnsBothOverloads() throws Exception {
        List<SearchResult> results = engine.findMethodsByPartialFqn(QCLASS + "#overloaded");

        assertThat(results).hasSize(2);
        assertThat(results).allMatch(r -> "overloaded".equals(r.methodName()));
    }

    @Test
    void nonExistentMethod_returnsEmpty() throws Exception {
        List<SearchResult> results = engine.findMethodsByPartialFqn(QCLASS + "#doesNotExist");

        assertThat(results).isEmpty();
    }

    @Test
    void noHashInFqn_returnsEmpty() throws Exception {
        // Guard: caller forgot the # entirely — not a partial FQN at all
        assertThat(engine.findMethodsByPartialFqn(QCLASS)).isEmpty();
    }

    @Test
    void shortClassName_returnsEmpty() throws Exception {
        // Unqualified class name doesn't match the stored qualifiedClassName
        assertThat(engine.findMethodsByPartialFqn(CLASS + "#uniqueMethod")).isEmpty();
    }

    @Test
    void exactFqnWithParams_isUnaffected() throws Exception {
        // Existing exact lookup still works; this method is not findMethodsByPartialFqn,
        // but verify the indexed data is correct for a fully-qualified lookup too.
        SearchResult r = engine.getMethodByFqn(QCLASS + "#uniqueMethod(String)");
        assertThat(r).isNotNull();
        assertThat(r.methodName()).isEqualTo("uniqueMethod");
    }

    // --- helpers ---

    private static ParsedMethod method(String name, List<String> paramTypes, List<String> paramNames) {
        String id = ParsedMethod.buildId(PROJECT, QCLASS, name, paramTypes);
        String sig = "public void " + name + "(" + String.join(", ", paramNames) + ")";
        return new ParsedMethod(
                id, PROJECT, PKG, CLASS, QCLASS,
                name, sig, "void",
                paramTypes, paramNames,
                "{ }", null, List.of(), "public",
                false, false, false, false,
                List.of(), List.of(), "/fake/MyService.java", 1, 5
        );
    }

    // Minimal in-memory registry that avoids writing to ~/.pharos/
    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() { super(IndexConfig.defaults()); }

        @Override public synchronized void register(ProjectMeta meta) { store.put(meta.getName(), meta); }
        @Override public synchronized Optional<ProjectMeta> find(String name) { return Optional.ofNullable(store.get(name)); }
        @Override public synchronized List<ProjectMeta> listAll() { return new ArrayList<>(store.values()); }
        @Override public synchronized void link(String p1, String p2) {}
        @Override public synchronized void unregister(String name) { store.remove(name); }
    }
}
