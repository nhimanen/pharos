package com.pharos.search.distributed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.ClusterFixture;
import com.pharos.cluster.internal.InternalApi;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.mcp.McpToolRegistry;
import com.pharos.graph.ModuleBoundaryAnalyzer;
import com.pharos.search.FstQueryClassifier;
import com.pharos.search.SearchEngine;
import com.pharos.search.pipeline.NoOpCrossEncoder;
import io.javalin.Javalin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One reader node in the test JVM, wired as {@code WebCommand} wires a serving reader: its runtime
 * (heartbeats into the shared bucket, syncs its pinned shards), a search engine made a coordinator by
 * {@code ClusterRuntime#internalApi(SearchEngine)}, and the {@code /internal/*} endpoints on an
 * ephemeral port (its advertise URL). Every {@code /internal} call it receives is recorded.
 */
final class TestReaderNode implements AutoCloseable {

    /** One {@code /internal} call this node answered: path, request body, status, response body. */
    record Call(String node, String path, String request, int status, String response) {
        JsonNode requestJson() { return json(request); }
        JsonNode responseJson() { return json(response); }
        String shard() { return requestJson().path("shard").asText(); }

        private static JsonNode json(String s) {
            try {
                return MAPPER.readTree(s == null || s.isEmpty() ? "{}" : s);
            } catch (Exception e) {
                return MAPPER.createObjectNode();
            }
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    final String id;
    final ClusterFixture.ReaderParts parts;
    final SearchEngine engine;
    final Path configBase;
    final List<Call> calls = Collections.synchronizedList(new ArrayList<>());
    private final Javalin app;
    private boolean killed;

    TestReaderNode(Path home, ObjectStore store, String bucket, String id, List<String> pin,
                   EmbeddingProvider embedder, String synonyms) throws Exception {
        this.id = id;
        this.parts = ClusterFixture.reader(home, store, bucket, id, pin);
        this.configBase = Files.createDirectories(home.resolve("config"));
        if (synonyms != null) Files.writeString(configBase.resolve("synonyms.txt"), synonyms);
        this.engine = new SearchEngine(parts.lucene(), embedder, parts.registry(), new NoOpCrossEncoder(),
                new FstQueryClassifier(), configBase);
        InternalApi api = parts.runtime().internalApi(engine);
        app = Javalin.create(c -> c.http.maxRequestSize = InternalApi.MAX_BODY_BYTES);
        app.after("/internal/*", ctx -> calls.add(new Call(id, ctx.path(), ctx.body(), ctx.statusCode(),
                ctx.result())));
        api.mount(app);
        app.start(0);
        parts.config().getCluster().setAdvertiseUrl("http://localhost:" + app.port());
        parts.runtime().startServing();
    }

    /** The node stops answering (connection refused) while its heartbeat stays live: dead, not drained. */
    void kill() {
        if (!killed) app.stop();
        killed = true;
    }

    /** Waits until this node's live-reader list has {@code n} readers. */
    void awaitLive(int n) throws InterruptedException {
        long deadline = System.nanoTime() + 20_000_000_000L;
        while (parts.runtime().liveReaders().size() < n) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(id + " sees " + parts.runtime().liveReaders().size() + " live readers, wanted " + n);
            }
            Thread.sleep(100);
        }
    }

    /** Waits until {@code readers} live readers (in this node's view) announce {@code shard}. */
    void awaitShard(String shard, int readers) throws InterruptedException {
        long deadline = System.nanoTime() + 20_000_000_000L;
        while (parts.runtime().liveReaders().stream()
                .filter(l -> l.heartbeat().shards() != null && l.heartbeat().shards().contains(shard)).count() < readers) {
            if (System.nanoTime() > deadline) throw new AssertionError(id + " sees fewer than " + readers + " readers of " + shard);
            Thread.sleep(100);
        }
    }

    McpToolRegistry mcp() {
        return new McpToolRegistry(engine, parts.registry(), ClusterFixture.noopModules(parts.registry()),
                new ModuleBoundaryAnalyzer(parts.registry(), engine), com.pharos.cluster.NodeRole.READER);
    }

    List<Call> drainCalls() {
        synchronized (calls) {
            List<Call> out = new ArrayList<>(calls);
            calls.clear();
            return out;
        }
    }

    @Override
    public void close() throws Exception {
        kill();
        parts.runtime().close();
        parts.lucene().close();
    }
}
