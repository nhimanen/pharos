package com.pharos.cluster;

import com.pharos.cli.IndexCommand;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SourceOptionsTest {

    @TempDir Path tmp;

    @Test
    void singleNodeRefusesSourceOptions() throws Exception {
        IndexConfig all = IndexConfig.defaults();
        all.setIndexDir(tmp.resolve("indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        try (ReplicationBackend backend = Backends.create(all);
             LuceneIndexer lucene = new LuceneIndexer(all, backend.localCache());
             ClusterRuntime runtime = new ClusterRuntime(all, backend, registry, lucene)) {
            PrintStream old = System.err;
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            System.setErr(new PrintStream(err));
            int exit;
            try {
                exit = new CommandLine(new IndexCommand(null, null, registry, all, runtime))
                        .execute(tmp.toString(), "--source-path", tmp.toString());
            } finally {
                System.setErr(old);
            }
            assertThat(exit).isEqualTo(2);
            assertThat(err.toString()).contains("needs a cluster object-store backend");
        }
    }

    @Test
    void writerRecordsSourceInCatalogAfterIndexing() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture c = new ClusterFixture(tmp, store, store, "test-bucket")) {
            Path project = c.source.resolve(ClusterFixture.JAVA);
            IndexCommand cmd = new IndexCommand(c.writerManager,
                    new com.pharos.graph.CrossProjectLinker(c.writerConfig, c.writerRegistry),
                    c.writerRegistry, c.writerConfig, c.writerRuntime);
            int exit = new CommandLine(cmd).execute(project.toString(), "--project", ClusterFixture.JAVA,
                    "--single", "--no-embed", "--no-synonyms", "--no-progress",
                    "--source-git", "https://example.org/r.git", "--ref", "main");
            assertThat(exit).isZero();
            assertThat(c.writerRuntime.sourceCatalog().orElseThrow().entries()).singleElement().satisfies(e -> {
                assertThat(e.id()).isEqualTo(new ShardId(ClusterFixture.JAVA));
                assertThat(e.source()).isEqualTo(Optional.of(ShardSource.git("https://example.org/r.git", "main")));
            });
        }
    }

    @Test
    void sourceForDiscoveredSubprojectIsRefused() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture c = new ClusterFixture(tmp, store, store, "test-bucket")) {
            Path repo = tmp.resolve("repo");
            java.nio.file.Files.createDirectories(repo.resolve("service"));
            java.nio.file.Files.writeString(repo.resolve("service/pom.xml"), "<project/>");
            IndexCommand cmd = new IndexCommand(c.writerManager,
                    new com.pharos.graph.CrossProjectLinker(c.writerConfig, c.writerRegistry),
                    c.writerRegistry, c.writerConfig, c.writerRuntime);
            PrintStream old = System.err;
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            System.setErr(new PrintStream(err));
            int exit;
            try {
                exit = new CommandLine(cmd).execute(repo.toString(), "--no-embed", "--no-synonyms", "--no-progress",
                        "--source-git", "https://example.org/r.git", "--ref", "main");
            } finally {
                System.setErr(old);
            }
            assertThat(exit).isEqualTo(2);
            assertThat(err.toString()).contains("sources for subprojects are not supported yet");
            assertThat(c.writerRuntime.sourceCatalog().orElseThrow().entries()).isEmpty();
            assertThat(c.writerRegistry.find("service")).isEmpty();
        }
    }
}
