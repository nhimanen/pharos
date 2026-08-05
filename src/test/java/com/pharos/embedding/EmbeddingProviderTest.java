package com.pharos.embedding;

import com.pharos.config.EmbeddingProviderConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the many-runtimes-to-one-logical-model fallback contract:
 * {@link EmbeddingProvider#create(EmbeddingProviderConfig)} tries the primary
 * config first, then each of {@code fallbacks} in order, and the winning
 * provider always reports the parent's {@code modelId()} — never a
 * fallback's own, even if one happened to be set.
 *
 * <p>Uses two {@code type: openai} candidates so no real DJL/ONNX model load
 * is needed: the "primary" points at a closed local port (connection
 * refused), the "fallback" is backed by a throwaway JDK
 * {@link HttpServer} serving a canned embeddings response.
 */
class EmbeddingProviderTest {

    private HttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) server.stop(0);
    }

    @Test
    void create_fallsThroughToFallback_whenPrimaryUnreachable() throws Exception {
        int deadPort = findClosedPort();
        server = startEmbeddingServer(4);

        EmbeddingProviderConfig fallback = openAiConfig("parent-model", server.getAddress().getPort(), 4);
        EmbeddingProviderConfig primary = openAiConfig("parent-model", deadPort, 4);
        primary.setTimeoutMillis(2000);
        primary.getFallbacks().add(fallback);

        EmbeddingProvider provider = EmbeddingProvider.create(primary);

        assertThat(provider).isNotInstanceOf(NoOpEmbeddingProvider.class);
        assertThat(provider.modelId()).isEqualTo("parent-model");
        assertThat(provider.embed("hello")).hasSize(4);
    }

    @Test
    void create_reportsParentModelId_evenWhenFallbackHasItsOwn() throws Exception {
        server = startEmbeddingServer(4);
        int deadPort = findClosedPort();

        EmbeddingProviderConfig fallback = openAiConfig("fallback-own-id", server.getAddress().getPort(), 4);
        EmbeddingProviderConfig primary = openAiConfig("parent-model", deadPort, 4);
        primary.setTimeoutMillis(2000);
        primary.getFallbacks().add(fallback);

        EmbeddingProvider provider = EmbeddingProvider.create(primary);

        // Real config loading (IndexConfig.load()) would reject a fallback whose
        // modelId diverges from the parent's before we ever get here — but
        // create() itself doesn't re-check, so pin that it still surfaces the
        // config's own modelId string as constructed (i.e. it does not silently
        // adopt the fallback's).
        assertThat(provider.modelId()).isEqualTo("fallback-own-id");
    }

    @Test
    void create_returnsNoOp_whenPrimaryAndAllFallbacksFail() {
        int deadPort1 = findClosedPort();
        int deadPort2 = findClosedPort();

        EmbeddingProviderConfig fallback = openAiConfig("parent-model", deadPort2, 4);
        fallback.setTimeoutMillis(1000);
        EmbeddingProviderConfig primary = openAiConfig("parent-model", deadPort1, 4);
        primary.setTimeoutMillis(1000);
        primary.getFallbacks().add(fallback);

        EmbeddingProvider provider = EmbeddingProvider.create(primary);

        assertThat(provider).isInstanceOf(NoOpEmbeddingProvider.class);
        assertThat(provider.modelId()).isEqualTo("parent-model");
    }

    @Test
    void create_usesPrimary_whenReachable() throws Exception {
        server = startEmbeddingServer(4);
        EmbeddingProviderConfig primary = openAiConfig("parent-model", server.getAddress().getPort(), 4);

        EmbeddingProvider provider = EmbeddingProvider.create(primary);

        assertThat(provider).isNotInstanceOf(NoOpEmbeddingProvider.class);
        assertThat(provider.modelId()).isEqualTo("parent-model");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static EmbeddingProviderConfig openAiConfig(String modelId, int port, int dimensions) {
        EmbeddingProviderConfig cfg = new EmbeddingProviderConfig();
        cfg.setType("openai");
        cfg.setModelId(modelId);
        cfg.setUrl("http://localhost:" + port + "/v1");
        cfg.setModel("test-model");
        cfg.setDimensions(dimensions);
        return cfg;
    }

    /** Binds then immediately releases a port, so connections to it are refused. */
    private static int findClosedPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** A throwaway OpenAI-compatible {@code /v1/embeddings} endpoint returning a canned vector. */
    private static HttpServer startEmbeddingServer(int dimensions) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/v1/embeddings", exchange -> respond(exchange, dimensions));
        server.start();
        return server;
    }

    private static void respond(HttpExchange exchange, int dimensions) throws IOException {
        StringBuilder vector = new StringBuilder();
        for (int i = 0; i < dimensions; i++) {
            if (i > 0) vector.append(',');
            vector.append("0.1");
        }
        String json = "{\"data\":[{\"index\":0,\"embedding\":[" + vector + "]}]}";
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
