package com.pharos.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cli.IndexCommand;
import com.pharos.cli.PublishCommand;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.mcp.McpToolRegistry;
import com.pharos.parser.JavaCodeParser;
import com.pharos.search.SearchEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.IntSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RoleGuardTest {

    private static final String GUARD = "this node has role reader; run indexing on a writer node";

    @TempDir Path tmp;

    @Test
    void readerRefusesIndexing() throws Exception {
        IndexConfig config = ClusterFixture.config(tmp.resolve("reader"), NodeRole.READER, "b");
        InMemoryObjectStore store = new InMemoryObjectStore();
        try (ReplicationBackend backend = TestBackends.objectStore(config, store)) {
            ProjectRegistry registry = ClusterRuntime.registryFor(config);
            LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
            ProjectIndexManager manager = new ProjectIndexManager(config, lucene, registry,
                    List.of(EmbeddingProvider.create(config)), ClusterFixture.noopModules(registry),
                    List.of(new JavaCodeParser()), backend.publisher());
            IndexCommand cmd = new IndexCommand(manager, new CrossProjectLinker(config, registry), registry, config);

            String[] err = new String[1];
            int exit = captureErr(err, () -> new CommandLine(cmd).execute(
                    "src/test/resources/test-projects/sample-app", "--no-embed", "--no-synonyms", "--no-progress"));

            assertThat(exit).isNotZero();
            assertThat(err[0]).contains(GUARD);
            assertThat(config.getCluster().getCacheDir()).doesNotExist();
            assertThat(config.getIndexDir()).doesNotExist();
            assertThat(store.list("")).isEmpty();
            lucene.close();
        }
    }

    @Test
    void writerAndAllMayIndex() {
        IndexConfig writer = ClusterFixture.config(tmp.resolve("w"), NodeRole.WRITER, "b");
        IndexConfig all = IndexConfig.defaults();
        assertThatCode(() -> RoleGuard.requireIndexing(writer)).doesNotThrowAnyException();
        assertThatCode(() -> RoleGuard.requireIndexing(all)).doesNotThrowAnyException();
        IndexConfig reader = ClusterFixture.config(tmp.resolve("r"), NodeRole.READER, "b");
        assertThatThrownBy(() -> RoleGuard.requireIndexing(reader))
                .isInstanceOf(IllegalStateException.class).hasMessage(GUARD);
    }

    @Test
    void publishRequiresWriterRole() throws Exception {
        IndexConfig all = IndexConfig.defaults();
        all.setIndexDir(tmp.resolve("indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        try (ReplicationBackend backend = Backends.create(all);
             LuceneIndexer lucene = new LuceneIndexer(all, backend.localCache());
             ClusterRuntime runtime = new ClusterRuntime(all, backend, registry, lucene)) {
            String[] err = new String[1];
            int exit = captureErr(err, () -> new CommandLine(
                    new PublishCommand(all, registry, runtime)).execute("nonexistent"));
            assertThat(exit).isEqualTo(2);
            assertThat(err[0]).contains("requires role writer");
        }
    }

    /**
     * Final review I2: on a work-queue writer the work loop owns the local indexes; upgrade-indexes,
     * remove-index and embed are refused like index and publish, and touch nothing.
     */
    @Test
    void workQueueWriterRefusesUpgradeRemoveAndEmbed() throws Exception {
        IndexConfig config = ClusterFixture.config(tmp.resolve("w"), NodeRole.WRITER, "b");
        config.getCluster().getWriter().setWorkQueue(true);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        ProjectIndexManager manager = new ProjectIndexManager(config, new LuceneIndexer(config), registry,
                List.of(EmbeddingProvider.create(config)), ClusterFixture.noopModules(registry),
                List.of(new JavaCodeParser()));
        String[] err = new String[1];

        int upgrade = captureErr(err, () -> new CommandLine(
                new com.pharos.cli.UpgradeIndexesCommand(manager, registry, config)).execute("--yes"));
        assertThat(upgrade).isEqualTo(5);
        assertThat(err[0]).contains("Not running 'pharos upgrade-indexes'", "work-queue mode",
                "restart the writers on the new release");

        int remove = captureErr(err, () -> new CommandLine(
                new com.pharos.cli.RemoveIndexCommand(registry, manager, config)).execute("--all"));
        assertThat(remove).isEqualTo(2);
        assertThat(err[0]).contains("Not running 'pharos remove-index'", "work-queue mode");

        int embed = captureErr(err, () -> new CommandLine(
                new com.pharos.cli.EmbedCommand(null, registry, config)).execute("--all", "--model", "m"));
        assertThat(embed).isEqualTo(2);
        assertThat(err[0]).contains("Not running 'pharos embed'", "work-queue mode");

        // a writer that indexes by hand still may
        config.getCluster().getWriter().setWorkQueue(false);
        assertThatCode(() -> RoleGuard.requireNoWorkQueue(config, "x")).doesNotThrowAnyException();
        assertThatCode(() -> RoleGuard.requireNoWorkQueue(IndexConfig.defaults(), "x")).doesNotThrowAnyException();
    }

    @Test
    void readerAnswersModuleToolsNotAvailable() throws Exception {
        IndexConfig config = ClusterFixture.config(tmp.resolve("reader"), NodeRole.READER, "b");
        try (ReplicationBackend backend = TestBackends.objectStore(config, new InMemoryObjectStore());
             LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache())) {
            ProjectRegistry registry = ClusterRuntime.registryFor(config);
            SearchEngine engine = new SearchEngine(lucene, EmbeddingProvider.create(config), registry);
            McpToolRegistry tools = new McpToolRegistry(engine, registry, ClusterFixture.noopModules(registry),
                    new ModuleBoundaryAnalyzer(registry, engine), NodeRole.READER);
            ObjectMapper json = new ObjectMapper();
            for (String tool : List.of("list_modules", "get_module_deps", "find_module_path", "get_module_boundary")) {
                assertThat(tools.call(tool, json.readTree("{\"module\":\"x\",\"from\":\"a\",\"to\":\"b\",\"project\":\"p\"}")))
                        .as(tool).contains("not available on reader nodes yet");
            }
        }
    }

    private static int captureErr(String[] err, IntSupplier run) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream old = System.err;
        try {
            System.setErr(new PrintStream(buf, true, StandardCharsets.UTF_8));
            return run.getAsInt();
        } finally {
            System.setErr(old);
            err[0] = buf.toString(StandardCharsets.UTF_8);
        }
    }

    @Test
    void sourceReaderCanBeDisabled() throws Exception {
        Path f = Files.writeString(tmp.resolve("A.java"), "line1\nline2\n");
        try {
            assertThat(com.pharos.search.SourceReader.readRange(f.toString(), 1, 2)).isEqualTo("line1\nline2");
            com.pharos.search.SourceReader.setEnabled(false);
            assertThat(com.pharos.search.SourceReader.readRange(f.toString(), 1, 2)).isNull();
        } finally {
            com.pharos.search.SourceReader.setEnabled(true);
        }
    }
}
