package com.pharos.cluster;

import com.pharos.cli.IndexCommand;
import com.pharos.cli.PublishCommand;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SourceReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static org.assertj.core.api.Assertions.assertThat;

/** Writer → object store → reader, in one JVM, wired like {@code Main} wires the two roles. */
class ClusterRoundTripTest {

    @TempDir Path tmp;

    private ClusterFixture cluster;

    @AfterEach
    void tearDown() throws IOException {
        SourceReader.setEnabled(true); // JVM-wide toggle
        if (cluster != null) cluster.close();
    }

    private ClusterFixture inMemoryCluster() throws IOException {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        return cluster;
    }

    @Test
    void readerServesSameResultsAsWriter() throws Exception {
        ClusterFixture c = inMemoryCluster();
        c.indexBoth();
        c.readerRuntime.startServing();

        // A writer reads class bodies from disk; a reader never does. Compare like with like.
        SourceReader.setEnabled(false);
        String writer = SingleNodeGoldenTest.snapshot(c.writerEngine, c.writerRegistry);
        String reader = SingleNodeGoldenTest.snapshot(c.readerEngine, c.readerRegistry);
        assertThat(reader).isEqualTo(writer);
        // callers/shortestPath resolved through the reader registry's generation dir
        assertThat(reader).contains("callers|com.example.model.User#getName()")
                .contains("com.example.service.GreetingService#greet(User)");
        assertThat(c.readerRegistry.find(JAVA).orElseThrow().getIndexPath())
                .startsWith(c.readerConfig.getCluster().getCacheDir().toString());
    }

    @Test
    void newGenerationVisibleAfterOnePoll() throws Exception {
        ClusterFixture c = inMemoryCluster();
        c.index(JAVA, false);
        c.readerRuntime.startServing();
        assertThat(keywordIds(c.readerEngine, "tripleTheValue")).isEmpty();

        Path calc = c.source.resolve(JAVA).resolve("src/main/java/com/example/Calculator.java");
        Files.writeString(calc, Files.readString(calc).replace("class Calculator {",
                "class Calculator {\n    public int tripleTheValue(int x) { return 3 * x; }\n"));
        Files.setLastModifiedTime(calc, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        c.index(JAVA, true);

        c.readerRuntime.syncNow();
        assertThat(keywordIds(c.readerEngine, "tripleTheValue"))
                .contains("sample-app:com.example.Calculator#tripleTheValue(int)");
    }

    @Test
    void readerIgnoresStaleSourceFile() throws Exception {
        ClusterFixture c = inMemoryCluster();
        c.index(JAVA, false);
        c.readerRuntime.startServing();
        ClusterRuntime.applySourceReadPolicy(c.readerConfig); // as Main does for a reader

        // The reader resolves the indexed absolute path to the writer's source dir; that file
        // now holds unrelated text which must never reach a reader's answer.
        Path greeting = c.source.resolve(JAVA).resolve("src/main/java/com/example/service/GreetingService.java");
        // Long enough that the indexed line range exists in the stale file.
        Files.writeString(greeting, "// StaleUnrelatedText, not what was indexed\n".repeat(200));

        SearchEngine.ClassContext ctx = c.readerEngine.getClassContext("com.example.service.GreetingService");
        assertThat(ctx).isNotNull();
        assertThat(ctx.body()).doesNotContain("StaleUnrelatedText").contains("public String greet(User user)");
    }

    /** An in-memory store whose file uploads fail while {@code down} is set. */
    private static final class FlakyStore extends InMemoryObjectStore {
        volatile boolean down = true;

        @Override
        public String put(String key, Path source) throws IOException {
            if (down) throw new IOException("connection refused (injected)");
            return super.put(key, source);
        }
    }

    @Test
    void publishFailureExitCode3() throws Exception {
        FlakyStore store = new FlakyStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        ClusterFixture c = cluster;
        IndexCommand cmd = new IndexCommand(c.writerManager,
                new CrossProjectLinker(c.writerConfig, c.writerRegistry), c.writerRegistry, c.writerConfig);

        Captured r = capture(() -> new CommandLine(cmd).execute(c.source.resolve(JAVA).toString(),
                "--project", JAVA, "--single", "--no-embed", "--no-synonyms", "--no-progress"));

        assertThat(r.exit).isEqualTo(3);
        assertThat(r.err).contains("pharos publish sample-app");
        assertThat(c.writerRegistry.find(JAVA)).isPresent();
        assertThat(c.writerManager.indexExists(JAVA)).isTrue();
        assertThat(store.get("pharos/shards/sample-app/LATEST")).isEmpty();
    }

    @Test
    void publishCommandRetriesAfterPublishFailure() throws Exception {
        FlakyStore store = new FlakyStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        ClusterFixture c = cluster;
        assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> c.index(JAVA, false)))
                .isInstanceOf(PublishException.class);

        store.down = false;
        Captured r = capture(() -> new CommandLine(
                new PublishCommand(c.writerConfig, c.writerRegistry, c.writerRuntime)).execute(JAVA));
        assertThat(r.exit).as(r.err).isEqualTo(0);
        assertThat(r.out).contains("published sample-app generation 1");

        c.readerRuntime.startServing();
        assertThat(c.readerRuntime.served()).containsEntry(new ShardId(JAVA), Generation.FIRST);
        assertThat(keywordIds(c.readerEngine, "Calculator")).isNotEmpty();
    }

    private record Captured(int exit, String out, String err) {}

    private static Captured capture(java.util.function.IntSupplier run) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        PrintStream oldOut = System.out;
        PrintStream oldErr = System.err;
        int exit;
        try {
            System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
            System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
            exit = run.getAsInt();
        } finally {
            System.setOut(oldOut);
            System.setErr(oldErr);
        }
        return new Captured(exit, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private static List<String> keywordIds(SearchEngine engine, String query) throws IOException {
        return engine.search(new SearchRequest(query, SearchRequest.SearchType.KEYWORD, JAVA, null, 10,
                "text", null, null, 0)).stream().map(SearchResult::id).toList();
    }
}
