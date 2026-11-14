package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.embedding.LazyEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.search.SearchEngine;
import com.pharos.search.pipeline.LazyCrossEncoder;
import com.pharos.search.pipeline.NoOpCrossEncoder;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * `index --no-embed` must neither load the embedding model nor claim files were embedded.
 *
 * <p>Previously the no-embed run stamped every file with the configured model's fingerprint,
 * so a later normal `index` (incremental) considered the unchanged files' embeddings current
 * and never embedded them — those documents stayed without vectors. Computing that fingerprint
 * was also what forced the model to load for a command that never embeds.
 */
class NoEmbedThenEmbedTest {

    @TempDir Path tmp;

    private Path projectRoot;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private SelectiveIndexUpdateTest.CountingEmbeddingProvider counting;
    private AtomicInteger loads;
    private ProjectIndexManager manager;
    private SelectiveIndexUpdateTest.TestRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        projectRoot = tmp.resolve("project");
        Path pkg = Files.createDirectories(projectRoot.resolve("src/main/java/com/example"));
        Files.writeString(pkg.resolve("A.java"), "package com.example;\npublic class A { public void alpha() {} }");
        Files.writeString(pkg.resolve("B.java"), "package com.example;\npublic class B { public void beta() {} }");

        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        config.setEmbeddingModelUrl("test-model");
        config.setEmbeddingDimensions(4);
        luceneIndexer = new LuceneIndexer(config);
        registry = new SelectiveIndexUpdateTest.TestRegistry();

        counting = new SelectiveIndexUpdateTest.CountingEmbeddingProvider(4);
        loads = new AtomicInteger();
        EmbeddingProvider lazy = new LazyEmbeddingProvider(counting.modelId(), 4,
                () -> { loads.incrementAndGet(); return counting; });
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        manager = new ProjectIndexManager(config, luceneIndexer, registry, lazy, noopGraph);
    }

    @Test
    void noEmbedIndex_neverLoadsTheModel() throws Exception {
        manager.index(projectRoot, "test", false, false);
        assertThat(loads).as("index --no-embed must not load the embedding model").hasValue(0);
    }

    @Test
    void normalIndexAfterNoEmbed_embedsTheUnchangedFiles() throws Exception {
        manager.index(projectRoot, "test", false, false);       // index --no-embed
        assertThat(docsWithAnyVector()).isZero();

        manager.index(projectRoot, "test", true, true);         // later: index (incremental)

        assertThat(counting.callCount()).as("unchanged files must still get embedded").isGreaterThan(0);
        assertThat(docsWithAnyVector()).isGreaterThan(0);
    }

    @Test
    void constructingTheSearchEngine_loadsNoModel() {
        AtomicInteger ceLoads = new AtomicInteger();
        new SearchEngine(luceneIndexer, new LazyEmbeddingProvider("m", 4, () -> { loads.incrementAndGet(); return counting; }),
                registry, new LazyCrossEncoder(() -> { ceLoads.incrementAndGet(); return new NoOpCrossEncoder(); }));
        assertThat(loads).hasValue(0);
        assertThat(ceLoads).hasValue(0);
    }

    private int docsWithAnyVector() throws Exception {
        try (DirectoryReader r = DirectoryReader.open(new NIOFSDirectory(luceneIndexer.getLucenePath("test")))) {
            int n = 0;
            for (LeafReaderContext ctx : r.leaves()) {
                for (var fi : ctx.reader().getFieldInfos()) {
                    if (fi.getVectorDimension() > 0) {
                        var v = ctx.reader().getFloatVectorValues(fi.name);
                        if (v != null) n += v.size();
                    }
                }
            }
            return n;
        }
    }
}
