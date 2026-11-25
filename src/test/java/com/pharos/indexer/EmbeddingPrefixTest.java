package com.pharos.indexer;

import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.embedding.LazyEmbeddingProvider;
import com.pharos.embedding.OpenAiHttpEmbeddingProvider;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Query/document embedding routing: callers use embedQuery for queries and the document side
 * (embedBatch/embedDocument) for chunks, never adding prefixes themselves; providers apply them.
 */
class EmbeddingPrefixTest {

    @TempDir Path tmp;

    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private SelectiveIndexUpdateTest.TestRegistry registry;
    private Recording provider;
    private ProjectIndexManager manager;

    /** Records which API each text arrived through. Prefix application is the real provider's job. */
    static final class Recording extends SelectiveIndexUpdateTest.CountingEmbeddingProvider {
        final List<String> queries = new CopyOnWriteArrayList<>();
        final List<String> documents = new CopyOnWriteArrayList<>();
        final List<String> warmups = new CopyOnWriteArrayList<>();
        Recording() { super(4); }
        @Override public float[] embedQuery(String q) { queries.add(q); return super.embed(q); }
        @Override public float[] embedDocument(String d) { documents.add(d); return super.embed(d); }
        @Override public float[][] embedBatch(List<String> texts) {
            documents.addAll(texts);
            float[][] r = new float[texts.size()][];
            for (int i = 0; i < r.length; i++) r[i] = super.embed(texts.get(i));
            return r;
        }
        @Override public float[] embed(String text) { warmups.add(text); return super.embed(text); }
    }

    @BeforeEach
    void setUp() throws Exception {
        Path root = tmp.resolve("project");
        Path pkg = Files.createDirectories(root.resolve("src/main/java/com/example"));
        Files.writeString(pkg.resolve("A.java"), "package com.example;\npublic class A { public void alpha() {} }");
        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        config.setEmbeddingModelUrl("test-model");
        config.setEmbeddingDimensions(4);
        luceneIndexer = new LuceneIndexer(config);
        registry = new SelectiveIndexUpdateTest.TestRegistry();
        provider = new Recording();
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override public synchronized List<String> incorporate(Path r, ProjectMeta m,
                    com.pharos.parser.MavenPomReader.PomInfo p) { return List.of(); }
        };
        manager = new ProjectIndexManager(config, luceneIndexer, registry, provider, noopGraph);
        manager.index(root, "test", true, true);
    }

    @Test
    void indexing_usesDocumentSideOnly() {
        assertThat(provider.documents).isNotEmpty();
        assertThat(provider.queries).isEmpty();
        assertThat(provider.warmups).isEmpty();
    }

    @Test
    void vectorHybridAndUnifiedSearch_embedTheQueryAsAQuery() throws Exception {
        SearchEngine engine = new SearchEngine(luceneIndexer, provider, registry);
        for (SearchRequest.SearchType type : List.of(SearchRequest.SearchType.VECTOR,
                SearchRequest.SearchType.HYBRID, SearchRequest.SearchType.UNIFIED,
                SearchRequest.SearchType.HYBRID_RERANKED)) {
            provider.queries.clear();
            provider.documents.clear();
            engine.searchWithTrace(new SearchRequest("alpha method", type, "test", null, 5,
                    "text", null, null, 0), false);
            assertThat(provider.queries).as("%s embeds the user query via embedQuery", type)
                    .contains("alpha method");
            assertThat(provider.documents).as("%s must not embed the query as a document", type)
                    .doesNotContain("alpha method");
        }
    }

    @Test
    void lazyProvider_forwardsQueryAndDocumentSides() {
        Recording inner = new Recording();
        LazyEmbeddingProvider lazy = new LazyEmbeddingProvider("m", 4, () -> inner);
        lazy.embedQuery("q");
        lazy.embedDocument("d");
        lazy.embedBatch(List.of("b"));
        lazy.embedChunked(List.of("c"), 8);
        assertThat(inner.queries).containsExactly("q");
        assertThat(inner.documents).containsExactly("d", "b", "c");
        assertThat(inner.warmups).isEmpty();
    }

    @Test
    void withPrefix_skipsBlankAndEmpty() {
        assertThat(EmbeddingProvider.withPrefix("p: ", "x")).isEqualTo("p: x");
        assertThat(EmbeddingProvider.withPrefix("", "x")).isEqualTo("x");
        assertThat(EmbeddingProvider.withPrefix("p: ", " ")).isEqualTo(" ");
        assertThat(EmbeddingProvider.withPrefix("p: ", (String) null)).isNull();
    }

    @Test
    void openAiProvider_appliesQueryAndDocumentPrefixes() throws Exception {
        List<String> bodies = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/embeddings", ex -> {
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            bodies.add(body);
            int inputs = 1;   // every call in this test sends exactly one text
            StringBuilder sb = new StringBuilder("{\"data\":[");
            for (int i = 0; i < inputs; i++) {
                if (i > 0) sb.append(',');
                sb.append("{\"index\":").append(i).append(",\"embedding\":[1,0,0,0]}");
            }
            sb.append("]}");
            byte[] out = sb.toString().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        server.start();
        try {
            EmbeddingProviderConfig cfg = new EmbeddingProviderConfig();
            cfg.setType("openai");
            cfg.setModelId("m");
            cfg.setUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/v1");
            cfg.setModel("m");
            cfg.setDimensions(4);
            cfg.setQueryPrefix("Q: ");
            cfg.setDocumentPrefix("D: ");
            OpenAiHttpEmbeddingProvider p = new OpenAiHttpEmbeddingProvider(cfg);
            bodies.clear();   // constructor issues an unprefixed self-test request

            p.embedQuery("find");
            p.embedDocument("chunk");
            p.embedBatch(List.of("one"));
            p.embed("raw");

            assertThat(bodies).hasSize(4);
            assertThat(bodies.get(0)).contains("Q: find");
            assertThat(bodies.get(1)).contains("D: chunk");
            assertThat(bodies.get(2)).contains("D: one");
            assertThat(bodies.get(3)).contains("raw").doesNotContain("Q: raw").doesNotContain("D: raw");
        } finally {
            server.stop(0);
        }
    }
}
