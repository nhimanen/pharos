package com.pharos.config;

import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.MavenPomReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Guards that Maven test runs never touch the developer's real ~/.pharos. */
class TestIsolationTest {

    @TempDir
    Path tempDir;

    @Test
    void defaultBaseIsNotTheRealHomeUnderMaven() {
        Path realHome = Path.of(System.getProperty("user.home"), ".pharos").toAbsolutePath().normalize();
        Path base = IndexConfig.DEFAULT_BASE.toAbsolutePath().normalize();
        assertThat(base.startsWith(realHome)).as("DEFAULT_BASE=%s", base).isFalse();
        assertThat(base.toString()).contains("target");
    }

    @Test
    void synonymMiningDuringTestsStaysInTestHome() throws Exception {
        Path root = tempDir.resolve("project");
        Files.createDirectories(root.resolve("src/main/java/com/example"));
        Files.writeString(root.resolve("src/main/java/com/example/Greeter.java"),
                "package com.example;\npublic class Greeter {\n"
                + "  /** Says hello */ public String hello() { return \"hello\"; }\n}");

        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tempDir.resolve("indexes"));
        ProjectRegistry registry = new ProjectRegistry(config) {
            @Override public synchronized void register(ProjectMeta m) {}
            @Override public synchronized Optional<ProjectMeta> find(String n) { return Optional.empty(); }
            @Override public synchronized List<ProjectMeta> listAll() { return List.of(); }
            @Override public synchronized void link(String a, String b) {}
            @Override public synchronized void unregister(String n) {}
        };
        ModuleGraphBuilder noop = new ModuleGraphBuilder(registry) {
            @Override public synchronized List<String> incorporate(Path r, ProjectMeta m,
                    MavenPomReader.PomInfo p) { return List.of(); }
        };
        ProjectIndexManager mgr = new ProjectIndexManager(config, new LuceneIndexer(config),
                registry, new NoOpEmbeddingProvider(), noop);
        mgr.index(root, "isolation-test", false, false, false, true, ProgressListener.SILENT);

        Path synonyms = config.getSynonymsFile().toAbsolutePath().normalize();
        assertThat(synonyms.startsWith(IndexConfig.DEFAULT_BASE.toAbsolutePath().normalize())).isTrue();
        assertThat(synonyms.startsWith(
                Path.of(System.getProperty("user.home"), ".pharos").toAbsolutePath().normalize())).isFalse();
    }
}
