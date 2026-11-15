package com.pharos.cluster;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublishHookTest {

    private static final String PROJECT = "sample-app";

    @TempDir Path tmp;

    private ProjectRegistry registry;
    private final List<LocalSnapshot> calls = new ArrayList<>();
    private Path source;

    @BeforeEach
    void copySource() throws IOException {
        source = tmp.resolve("src-copy");
        Path from = Path.of("src/test/resources/test-projects/sample-app");
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path dest = source.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(dest);
                else Files.copy(p, dest);
            }
        }
    }

    private ProjectIndexManager manager(IndexPublisher publisher) {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        LuceneIndexer lucene = new LuceneIndexer(config);
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(
                    Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        return new ProjectIndexManager(config, lucene, registry,
                List.of(EmbeddingProvider.create(config)), noopModules,
                List.of(new JavaCodeParser()), publisher);
    }

    private IndexPublisher recording() {
        return snapshot -> {
            calls.add(snapshot);
            return Optional.empty();
        };
    }

    private void assertOneCall() {
        assertThat(calls).hasSize(1);
        LocalSnapshot s = calls.get(0);
        assertThat(s.shard().value()).isEqualTo(PROJECT);
        assertThat(s.meta().getName()).isEqualTo(PROJECT);
        try (Stream<Path> files = Files.list(s.shardDir().resolve("lucene"))) {
            assertThat(files.map(p -> p.getFileName().toString()))
                    .anyMatch(n -> n.startsWith("segments_"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void fullIndexPublishesOnce() throws Exception {
        manager(recording()).index(source, PROJECT, false, false, false, false, ProgressListener.SILENT);
        assertOneCall();
    }

    @Test
    void incrementalIndexPublishesOnce() throws Exception {
        ProjectIndexManager m = manager(recording());
        m.index(source, PROJECT, false, false, false, false, ProgressListener.SILENT);
        calls.clear();
        Path file;
        try (Stream<Path> s = Files.walk(source)) {
            file = s.filter(p -> p.toString().endsWith(".java")).findFirst().orElseThrow();
        }
        Files.writeString(file, Files.readString(file) + "\n// touched\n");
        Files.setLastModifiedTime(file,
                java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() + 5000));
        m.index(source, PROJECT, true, false, false, false, ProgressListener.SILENT);
        assertOneCall();
    }

    @Test
    void publishFailureKeepsLocalIndex() {
        ProjectIndexManager m = manager(snapshot -> {
            throw new PublishException("boom");
        });
        assertThatThrownBy(() -> m.index(source, PROJECT, false, false, false, false,
                ProgressListener.SILENT))
                .isInstanceOf(PublishException.class);
        assertThat(registry.find(PROJECT)).isPresent();
    }
}
