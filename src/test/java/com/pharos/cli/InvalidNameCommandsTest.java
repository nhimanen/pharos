package com.pharos.cli;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.EmbeddingCacheBackfiller;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.MultiModelEmbedder;
import com.pharos.indexer.ProjectIndexManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InvalidNameCommandsTest {

    @TempDir Path tmp;
    private IndexConfig config;
    private LuceneIndexer lucene;
    private ProjectRegistry registry;

    @BeforeEach
    void setUp() {
        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        lucene = new LuceneIndexer(config);
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        registry.register(new ProjectMeta(".", tmp.toString(), tmp.resolve("indexes").toString()));
        registry.register(new ProjectMeta("good", tmp.toString(), tmp.resolve("indexes/good").toString()));
    }

    private String capture(Runnable r) {
        var buf = new ByteArrayOutputStream();
        var oo = System.out; var oe = System.err;
        System.setOut(new PrintStream(buf, true)); System.setErr(new PrintStream(buf, true));
        try { r.run(); } finally { System.setOut(oo); System.setErr(oe); }
        return buf.toString();
    }

    @Test
    void embedAllSkipsInvalidNames() {
        List<String> seen = new ArrayList<>();
        MultiModelEmbedder e = new MultiModelEmbedder(config, lucene, registry) {
            @Override public int embed(String p, String m, boolean f) { seen.add(p); return 0; }
        };
        String out = capture(() -> new CommandLine(new EmbedCommand(e, registry))
                .execute("--all", "--model", "m"));
        assertThat(seen).containsExactly("good");
        assertThat(out).contains("Skipping invalid project name '.'");
    }

    @Test
    void backfillAllCommandSkipsInvalidNames() {
        String out = capture(() -> new CommandLine(new BackfillEmbeddingCacheCommand(
                new EmbeddingCacheBackfiller(config, lucene, registry))).execute("--all"));
        assertThat(out).contains("Skipping invalid project name '.'").doesNotContain("Failed for '.'");
    }

    @Test
    void indexingAPathEndingInDotRegistersTheRealDirectoryName() throws Exception {
        Path src = Paths.get("src/test/resources/test-projects/sample-app");
        Path root = tmp.resolve("my-real-proj");
        try (var walk = Files.walk(src)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path t = root.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        ModuleGraphBuilder noop = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path r, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) { return List.of(); }
        };
        var mgr = new ProjectIndexManager(config, lucene, registry, new NoOpEmbeddingProvider(), noop);
        int rc = new CommandLine(new IndexCommand(mgr, null, registry, config))
                .execute(root.toString() + "/.", "--no-embed", "--no-progress", "--no-synonyms");
        assertThat(rc).isZero();
        assertThat(registry.find("my-real-proj")).isPresent();
        assertThat(tmp.resolve("indexes/my-real-proj")).isDirectory();
        assertThat(registry.find("..")).isEmpty();
        assertThat(registry.listAll().stream().map(ProjectMeta::getName)).doesNotContain("..");
    }
}
