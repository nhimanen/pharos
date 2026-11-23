package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The embeddings opt-in flow on a project indexed keyword-only: {@code pharos embed} can only add a
 * model next to existing vectors (it walks vector-bearing docs), so on an index without any it
 * must fail loudly and must not record the model as embedded. {@code pharos index --full} is the
 * path that actually embeds such a project.
 */
class EmbedNeedsVectorsTest {

    @TempDir Path tmp;

    private Path root;
    private LuceneIndexer lucene;
    private SelectiveIndexUpdateTest.TestRegistry registry;
    private IndexConfig config;
    private ModuleGraphBuilder noopGraph;

    @BeforeEach
    void setUp() throws Exception {
        root = tmp.resolve("project");
        Path pkg = Files.createDirectories(root.resolve("src/main/java/com/example"));
        Files.writeString(pkg.resolve("A.java"), "package com.example;\npublic class A { public void alphaMethod() { int a = 1; } }");
        Files.writeString(pkg.resolve("B.java"), "package com.example;\npublic class B { public void betaMethod() { int b = 2; } }");
        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        lucene = new LuceneIndexer(config);
        registry = new SelectiveIndexUpdateTest.TestRegistry();
        noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path r, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) { return List.of(); }
        };
        // Keyword-only index: what every project looks like before `pharos embeddings enable`.
        new ProjectIndexManager(config, lucene, registry, List.of(), noopGraph).index(root, "test", false, false);
    }

    @Test
    void embedOnKeywordOnlyIndex_failsWithIndexFullHint_andRegistersNothing() {
        ProjectMeta meta = registry.find("test").orElseThrow();
        List<String> before = List.copyOf(meta.getEmbeddedModels());

        assertThatThrownBy(() -> new MultiModelEmbedder(config, lucene, registry)
                .embed(meta, new LegacyModelVectorFieldTest.FakeProvider("granite-r2"), "fp", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("this index has no vectors yet")
                .hasMessageContaining("pharos index --full")
                .hasMessageContaining(root.toString());

        assertThat(registry.find("test").orElseThrow().getEmbeddedModels())
                .as("a model that embedded nothing must not be recorded").isEqualTo(before);
    }

    @Test
    void embedThatUpdatesNothing_doesNotRegisterTheModel() throws Exception {
        new ProjectIndexManager(config, lucene, registry,
                List.of(new LegacyModelVectorFieldTest.FakeProvider("model-a")), noopGraph)
                .index(root, "test", false, true);
        // Sources gone: every doc is skipped as missing-file, so nothing gets model-b vectors.
        Files.delete(root.resolve("src/main/java/com/example/A.java"));
        Files.delete(root.resolve("src/main/java/com/example/B.java"));

        int updated = new MultiModelEmbedder(config, lucene, registry).embed(registry.find("test").orElseThrow(),
                new LegacyModelVectorFieldTest.FakeProvider("model-b"), "fp-b", false);

        assertThat(updated).isZero();
        assertThat(registry.find("test").orElseThrow().getEmbeddedModels()).doesNotContain("model-b");
    }

    @Test
    void indexFullAfterOptIn_makesVectorSearchWork() throws Exception {
        EmbeddingProvider provider = new LegacyModelVectorFieldTest.FakeProvider("granite-r2");
        new ProjectIndexManager(config, lucene, registry, List.of(provider), noopGraph)
                .index(root, "test", false, true);   // pharos index --full <root>

        List<SearchResult> vec = new SearchEngine(lucene, provider, registry).search(new SearchRequest("alphaMethod",
                SearchRequest.SearchType.VECTOR, "test", null, 10, "text", null, null, 0));
        assertThat(vec).isNotEmpty();
        assertThat(registry.find("test").orElseThrow().getEmbeddedModels()).contains("granite-r2");
    }
}
