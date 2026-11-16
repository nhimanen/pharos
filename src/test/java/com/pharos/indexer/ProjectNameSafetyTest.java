package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A project named "." must never be able to delete (or write into) the indexes root. */
class ProjectNameSafetyTest {

    @TempDir Path tmp;
    private IndexConfig config;
    private LuceneIndexer lucene;
    private ProjectRegistry registry;
    private ProjectIndexManager manager;
    private Path indexRoot;
    private Path sibling;

    @BeforeEach
    void setUp() throws Exception {
        config = IndexConfig.defaults();
        indexRoot = tmp.resolve("indexes");
        config.setIndexDir(indexRoot);
        lucene = new LuceneIndexer(config);
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        ModuleGraphBuilder noop = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) { return List.of(); }
        };
        manager = new ProjectIndexManager(config, lucene, registry, new NoOpEmbeddingProvider(), noop);
        sibling = indexRoot.resolve("other");
        Files.createDirectories(sibling.resolve("lucene"));
        Files.writeString(sibling.resolve("lucene/seg"), "data");
        registry.register(new ProjectMeta(".", tmp.toString(), indexRoot.resolve(".").toString()));
    }

    private void assertUntouched() {
        assertThat(indexRoot).isDirectory();
        assertThat(sibling.resolve("lucene/seg")).hasContent("data");
    }

    @Test
    void removeProjectDotRefusesAndDeletesNothing() {
        assertThatThrownBy(() -> manager.removeProject("."))
                .isInstanceOf(Exception.class).hasMessageContaining("'.'");
        assertUntouched();
    }

    @Test
    void deleteIndexDotRefusesAndDeletesNothing() throws Exception {
        Files.createDirectories(indexRoot.resolve("lucene"));
        Files.writeString(indexRoot.resolve("lucene/x"), "x");
        assertThatThrownBy(() -> lucene.deleteIndex("."))
                .isInstanceOf(Exception.class);
        assertThatThrownBy(() -> lucene.deleteIndex(".."))
                .isInstanceOf(Exception.class);
        assertThat(indexRoot.resolve("lucene/x")).exists();
        assertUntouched();
    }

    @Test
    void indexUnderDotNameIsRejected() throws Exception {
        Path root = Files.createDirectories(tmp.resolve("proj"));
        assertThatThrownBy(() -> manager.index(root, ".", false, true, false, ProgressListener.SILENT))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("--project");
        assertUntouched();
    }

    @Test
    void writersRefuseDotAndCreateNothing() throws Exception {
        assertThatThrownBy(() -> lucene.openWriter(".")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> lucene.openWriterFresh(".")).isInstanceOf(IllegalArgumentException.class);
        assertThat(indexRoot.resolve("lucene")).doesNotExist();
        assertThat(indexRoot.resolve("write.lock")).doesNotExist();
        assertUntouched();
    }

    @Test
    void backfillRefusesDot() {
        var b = new EmbeddingCacheBackfiller(config, lucene, registry);
        assertThatThrownBy(() -> b.backfill(".")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--project");
        assertThat(indexRoot.resolve("embed-cache.bin")).doesNotExist();
    }

    @Test
    void embedderRefusesDot() {
        var e = new MultiModelEmbedder(config, lucene, registry);
        ProjectMeta meta = registry.find(".").orElseThrow();
        assertThatThrownBy(() -> e.embed(meta, new NoOpEmbeddingProvider(), "fp", false))
                .isInstanceOf(IllegalArgumentException.class);
        assertUntouched();
    }

    @Test
    void backfillAllSkipsInvalidEntries() throws Exception {
        var b = new EmbeddingCacheBackfiller(config, lucene, registry);
        assertThat(b.backfillAll()).isZero(); // must not throw or touch the index root
        assertUntouched();
    }
}
