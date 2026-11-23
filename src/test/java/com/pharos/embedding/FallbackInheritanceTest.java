package com.pharos.embedding;

import com.pharos.config.EmbeddingProviderConfig;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class FallbackInheritanceTest {

    @Test
    void createdFallbackRuntime_appliesTheParentsPrefixes() throws Exception {
        List<String> bodies = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/embeddings", ex -> {
            bodies.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] out = "{\"data\":[{\"index\":0,\"embedding\":[1,0,0,0]}]}".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        server.start();
        try {
            EmbeddingProviderConfig parent = new EmbeddingProviderConfig();
            parent.setType("openai");
            parent.setModelId("m");
            parent.setUrl("http://127.0.0.1:1/v1");   // unreachable primary
            parent.setModel("m");
            parent.setDimensions(4);
            parent.setQueryPrefix("Q: ");
            parent.setDocumentPrefix("D: ");
            EmbeddingProviderConfig fb = new EmbeddingProviderConfig();
            fb.setType("openai");
            fb.setUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/v1");
            fb.setModel("m");
            parent.getFallbacks().add(fb);

            EmbeddingProvider p = EmbeddingProvider.create(parent);
            assertThat(p.isAvailable()).isTrue();
            assertThat(p).isInstanceOf(OpenAiHttpEmbeddingProvider.class);
            bodies.clear();
            p.embedQuery("find");
            p.embedDocument("chunk");
            assertThat(bodies.get(0)).contains("Q: find");
            assertThat(bodies.get(1)).contains("D: chunk");
        } finally {
            server.stop(0);
        }
    }
}
