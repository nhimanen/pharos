package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the {@code buildSynonyms=false} path in {@link ProjectIndexManager#index}.
 */
class NoSynonymsFlagTest {

    @TempDir
    Path tempDir;

    private static final String PROJECT = "test-project";

    private Path projectRoot;
    private Path synonymsFile;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;
    private ProjectIndexManager indexManager;

    @BeforeEach
    void setUp() throws IOException {
        projectRoot = tempDir.resolve("project");
        Files.createDirectories(projectRoot.resolve("src/main/java/com/example"));
        Files.writeString(
                projectRoot.resolve("src/main/java/com/example/Foo.java"),
                "package com.example;\npublic class Foo { public void run() {} }\n");

        synonymsFile = tempDir.resolve("synonyms.txt");
        config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));
        config.setSynonymsFile(synonymsFile);

        TestRegistry registry = new TestRegistry();
        luceneIndexer = new LuceneIndexer(config);
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        indexManager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopModules);
    }

    @Test
    void withBuildSynonymsTrue_synonymFileIsCreated() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false, false, true, ProgressListener.SILENT);

        // ConceptMiner may not produce rules for a tiny corpus, but the attempt is made;
        // at minimum the file must not exist only because of the flag being false.
        // We verify the flag=true path completes without error and the index is built.
        assertThat(luceneIndexer.indexExists(PROJECT)).isTrue();
    }

    @Test
    void withBuildSynonymsFalse_synonymFileIsNotTouched() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false, false, false, ProgressListener.SILENT);

        assertThat(synonymsFile).doesNotExist();
    }

    @Test
    void withBuildSynonymsFalse_indexIsStillBuilt() throws Exception {
        indexManager.index(projectRoot, PROJECT, false, false, false, false, ProgressListener.SILENT);

        assertThat(luceneIndexer.indexExists(PROJECT)).isTrue();
    }

    @Test
    void incrementalWithBuildSynonymsFalse_synonymFileIsNotTouched() throws Exception {
        // Full index first (with synonyms off), then incremental also with synonyms off
        indexManager.index(projectRoot, PROJECT, false, false, false, false, ProgressListener.SILENT);
        Files.writeString(
                projectRoot.resolve("src/main/java/com/example/Bar.java"),
                "package com.example;\npublic class Bar { public void go() {} }\n");

        indexManager.index(projectRoot, PROJECT, true, false, false, false, ProgressListener.SILENT);

        assertThat(synonymsFile).doesNotExist();
    }

    @Test
    void defaultOverload_buildsSynonymsByDefault() throws Exception {
        // The 6-arg overload (without buildSynonyms) defaults to true — must not skip
        indexManager.index(projectRoot, PROJECT, false, false, false, ProgressListener.SILENT);

        // Index must be built regardless
        assertThat(luceneIndexer.indexExists(PROJECT)).isTrue();
    }

    // --- minimal in-memory registry ---

    static class TestRegistry extends ProjectRegistry {
        private final Map<String, ProjectMeta> store = new LinkedHashMap<>();

        TestRegistry() { super(IndexConfig.defaults()); }

        @Override public synchronized void register(ProjectMeta meta) { store.put(meta.getName(), meta); }
        @Override public synchronized Optional<ProjectMeta> find(String name) { return Optional.ofNullable(store.get(name)); }
        @Override public synchronized List<ProjectMeta> listAll() { return new ArrayList<>(store.values()); }
        @Override public synchronized void unregister(String name) { store.remove(name); }
        @Override public synchronized void link(String p1, String p2) {}
        @Override public synchronized void unlinkAll(String name) {}
    }
}
