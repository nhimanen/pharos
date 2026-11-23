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
                // --no-embed is recorded with the source: work-queue writers index the shard without embeddings too
                assertThat(e.source()).isEqualTo(Optional.of(ShardSource.git("https://example.org/r.git", "main")
                        .withEmbed(false)));
            });
        }
    }

    @Test
    void publishRecordsANoEmbedSource() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture c = new ClusterFixture(tmp, store, store, "test-bucket")) {
            Path project = c.source.resolve(ClusterFixture.JAVA);
            IndexCommand index = new IndexCommand(c.writerManager,
                    new com.pharos.graph.CrossProjectLinker(c.writerConfig, c.writerRegistry),
                    c.writerRegistry, c.writerConfig, c.writerRuntime);
            assertThat(new CommandLine(index).execute(project.toString(), "--project", ClusterFixture.JAVA,
                    "--single", "--no-embed", "--no-synonyms", "--no-progress")).isZero();
            com.pharos.cli.PublishCommand publish = new com.pharos.cli.PublishCommand(c.writerConfig,
                    c.writerRegistry, c.writerRuntime);
            assertThat(new CommandLine(publish).execute(ClusterFixture.JAVA, "--no-embed")).as("no source").isEqualTo(2);
            publish = new com.pharos.cli.PublishCommand(c.writerConfig, c.writerRegistry, c.writerRuntime);
            assertThat(new CommandLine(publish).execute(ClusterFixture.JAVA, "--no-embed",
                    "--source-git", "https://example.org/r.git", "--ref", "main")).isZero();
            assertThat(c.writerRuntime.sourceCatalog().orElseThrow().entries()).singleElement().satisfies(e ->
                    assertThat(e.source()).isEqualTo(Optional.of(ShardSource.git("https://example.org/r.git", "main")
                            .withEmbed(false))));
            publish = new com.pharos.cli.PublishCommand(c.writerConfig, c.writerRegistry, c.writerRuntime);
            assertThat(new CommandLine(publish).execute(ClusterFixture.JAVA,
                    "--source-git", "https://example.org/r.git", "--ref", "main")).isZero();
            assertThat(c.writerRuntime.sourceCatalog().orElseThrow().entries().get(0).source().orElseThrow().embed())
                    .as("without the flag the source embeds").isTrue();
        }
    }

    /** The operator's registration Job: no path, on a work-queue writer, with no writer running. */
    @Test
    void workQueueWriterRegistersASourceWithoutAPath() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture c = new ClusterFixture(tmp, store, store, "test-bucket")) {
            c.writerConfig.getCluster().getWriter().setWorkQueue(true);
            assertThat(c.writerRuntime.isWorkQueueWriter()).isTrue();
            java.util.function.Function<String[], Integer> run = args -> new CommandLine(new IndexCommand(
                    c.writerManager, new com.pharos.graph.CrossProjectLinker(c.writerConfig, c.writerRegistry),
                    c.writerRegistry, c.writerConfig, c.writerRuntime)).execute(args);

            assertThat(run.apply(new String[]{"--project", "lucene", "--source-git", "https://example.org/l.git",
                    "--ref", "main", "--no-embed"})).isZero();
            assertThat(c.writerRuntime.sourceCatalog().orElseThrow().entries()).singleElement().satisfies(e -> {
                assertThat(e.id()).isEqualTo(new ShardId("lucene"));
                assertThat(e.source()).contains(ShardSource.git("https://example.org/l.git", "main").withEmbed(false));
            });
            assertThat(c.writerRegistry.find("lucene")).as("nothing indexed here").isEmpty();

            // a new ref replaces the recorded source
            assertThat(run.apply(new String[]{"--project", "lucene", "--source-git", "https://example.org/l.git",
                    "--ref", "v2"})).isZero();
            assertThat(c.writerRuntime.sourceCatalog().orElseThrow().entries()).singleElement().satisfies(e ->
                    assertThat(e.source()).contains(ShardSource.git("https://example.org/l.git", "v2")));

            // unusable: no project, no source, an invalid name, a url with a password
            assertThat(run.apply(new String[]{"--source-git", "https://example.org/l.git", "--ref", "main"})).isEqualTo(2);
            assertThat(run.apply(new String[]{"--project", "x"})).isEqualTo(2);
            assertThat(run.apply(new String[]{"--project", "bad name", "--source-git", "https://example.org/l.git",
                    "--ref", "main"})).isEqualTo(2);
            assertThat(run.apply(new String[]{"--project", "y", "--source-git", "https://u:pw@example.org/l.git",
                    "--ref", "main"})).isEqualTo(2);
            assertThat(c.writerRuntime.sourceCatalog().orElseThrow().shards()).containsExactly(new ShardId("lucene"));
        }
    }

    /** The operator's removal Job: only the catalog entry goes, on a work-queue writer too. */
    @Test
    void removeFromCatalogDropsOnlyTheEntry() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture c = new ClusterFixture(tmp, store, store, "test-bucket")) {
            c.writerConfig.getCluster().getWriter().setWorkQueue(true);
            var catalog = c.writerRuntime.sourceCatalog().orElseThrow();
            catalog.register(ShardId.ofProject("a"), Optional.of(ShardSource.git("https://example.org/a.git", "main")));
            catalog.register(ShardId.ofProject("b"), Optional.of(ShardSource.git("https://example.org/b.git", "main")));
            java.util.function.Function<String[], Integer> run = args -> new CommandLine(
                    new com.pharos.cli.RemoveIndexCommand(c.writerRegistry, c.writerManager, c.writerConfig,
                            c.writerRuntime)).execute(args);

            assertThat(run.apply(new String[]{"a", "--from-catalog"})).isZero();
            assertThat(catalog.shards()).containsExactly(new ShardId("b"));
            assertThat(run.apply(new String[]{"a", "--from-catalog"})).as("already gone").isZero();
            assertThat(run.apply(new String[]{"--all", "--from-catalog"})).isEqualTo(2);
            assertThat(run.apply(new String[]{"bad name", "--from-catalog"})).isEqualTo(2);
            assertThat(new CommandLine(new com.pharos.cli.RemoveIndexCommand(c.writerRegistry, c.writerManager,
                    c.writerConfig)).execute("b", "--from-catalog")).as("no runtime wired").isEqualTo(2);
            assertThat(catalog.shards()).containsExactly(new ShardId("b"));
        }
    }

    /** Integration carry I5: the registration Job's new ref reaches every shard of a split project. */
    @Test
    void registrationUpdatesEveryShardOfASplitProject() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture c = new ClusterFixture(tmp, store, store, "test-bucket")) {
            c.writerConfig.getCluster().getWriter().setWorkQueue(true);
            var catalog = c.writerRuntime.sourceCatalog().orElseThrow();
            ShardSource main = ShardSource.git("https://example.org/a.git", "main");
            ShardId s01 = new ShardId("a~ring=s01");
            ShardId s02 = new ShardId("a~ring=s02");
            int v = catalog.addSpec(Optional.of("a"), com.pharos.cluster.sharding.ShardSpec.parse(
                    "[project, ring(path, shards=2)]"));
            catalog.putProject("a", v, java.util.List.of(s01, s02));
            catalog.register(s01, Optional.of(main)); // the copies a reshard records
            catalog.register(s02, Optional.of(main));

            assertThat(new CommandLine(new IndexCommand(c.writerManager,
                    new com.pharos.graph.CrossProjectLinker(c.writerConfig, c.writerRegistry), c.writerRegistry,
                    c.writerConfig, c.writerRuntime)).execute("--project", "a", "--source-git",
                    "https://example.org/a.git", "--ref", "v2")).isZero();
            ShardSource v2 = ShardSource.git("https://example.org/a.git", "v2");
            assertThat(catalog.entries()).containsExactlyInAnyOrder(new ShardEntry(s01, Optional.of(v2)),
                    new ShardEntry(s02, Optional.of(v2)), new ShardEntry(ShardId.ofProject("a"), Optional.of(v2)));
        }
    }

    /** Integration carry I4: the removal Job on a split project removes every shard of it at once. */
    @Test
    void removeFromCatalogRemovesEveryShardOfASplitProject() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ClusterFixture c = new ClusterFixture(tmp, store, store, "test-bucket")) {
            c.writerConfig.getCluster().getWriter().setWorkQueue(true);
            var catalog = c.writerRuntime.sourceCatalog().orElseThrow();
            catalog.register(ShardId.ofProject("a"), Optional.of(ShardSource.git("https://example.org/a.git", "main")));
            catalog.register(ShardId.ofProject("b"), Optional.of(ShardSource.git("https://example.org/b.git", "main")));
            int v = catalog.addSpec(Optional.of("a"), com.pharos.cluster.sharding.ShardSpec.parse(
                    "[project, ring(path, shards=2)]"));
            catalog.putProject("a", v, java.util.List.of(new ShardId("a~ring=s01"), new ShardId("a~ring=s02")));
            assertThat(catalog.projectShards().isSplit("a")).isTrue();

            assertThat(new CommandLine(new com.pharos.cli.RemoveIndexCommand(c.writerRegistry, c.writerManager,
                    c.writerConfig, c.writerRuntime)).execute("a", "--from-catalog")).isZero();
            assertThat(catalog.shards()).containsExactly(new ShardId("b"));
            assertThat(catalog.projectShards().projects()).containsExactly("b");
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
