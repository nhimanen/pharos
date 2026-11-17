package com.pharos.web;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class WebServerHealthTest {

    @Test
    void healthReportsWarmingWith503UntilReady() {
        WebServer server = new WebServer(null, null, null, null, null);
        assertThat(server.health()).isEqualTo(Map.of("status", "ok", "ready", true));
        assertThat(server.healthStatus()).isEqualTo(200);

        AtomicBoolean ready = new AtomicBoolean(false);
        server.setReadiness(ready::get);
        assertThat(server.health()).isEqualTo(Map.of("status", "warming", "ready", false));
        assertThat(server.healthStatus()).as("load balancers route on the status code").isEqualTo(503);
        ready.set(true);
        assertThat(server.health()).isEqualTo(Map.of("status", "ok", "ready", true));
        assertThat(server.healthStatus()).isEqualTo(200);
    }

    @Test
    void healthAnswers503OverHttpWhileWarming() throws Exception {
        WebServer server = new WebServer(null, null, null, null, null);
        AtomicBoolean ready = new AtomicBoolean(false);
        server.setReadiness(ready::get);
        io.javalin.Javalin app = server.start(0);
        try {
            java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
            java.net.URI uri = java.net.URI.create("http://127.0.0.1:" + app.port() + "/health");
            var warming = http.send(java.net.http.HttpRequest.newBuilder(uri).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(warming.statusCode()).isEqualTo(503);
            assertThat(warming.body()).contains("\"status\":\"warming\"");
            ready.set(true);
            var ok = http.send(java.net.http.HttpRequest.newBuilder(uri).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(ok.statusCode()).isEqualTo(200);
            assertThat(ok.body()).contains("\"status\":\"ok\"");
        } finally {
            app.stop();
        }
    }

    @Test
    void healthCarriesReaderCacheOnlyWhenThereIsOne() {
        WebServer server = new WebServer(null, null, null, null, null);
        assertThat(server.health()).doesNotContainKey("readerCache");
        server.setReaderCacheStats(() -> null);
        assertThat(server.health()).doesNotContainKey("readerCache");
        Map<String, Object> cache = Map.of("mode", "lazy", "hits", 5L, "hitRate", 1.0);
        server.setReaderCacheStats(() -> cache);
        assertThat(server.health()).containsEntry("readerCache", cache).containsEntry("ready", true);
    }
}
