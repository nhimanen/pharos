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
    void healthCarriesTheWriterDiskWithoutFailingReadiness() {
        WebServer server = new WebServer(null, null, null, null, null);
        assertThat(server.health()).doesNotContainKey("writerDisk");
        Map<String, Object> low = new java.util.LinkedHashMap<>();
        low.put("freeBytes", 1L << 30);
        low.put("totalBytes", 100L << 30);
        low.put("low", true);
        server.setWriterDisk(() -> low);
        assertThat(server.health()).containsEntry("writerDisk", low).containsEntry("status", "ok")
                .containsEntry("ready", true);
        assertThat(server.healthStatus()).as("low disk is informational, not a readiness failure").isEqualTo(200);
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

    /**
     * Phase 2 follow-up M3: a ready reader with a degraded shard (its served generation lost objects
     * on the bucket) stays routable: 200 with {@code "status":"degraded"} and the shards, so routers
     * keep sending it the queries for its other shards; only warming and draining answer 503.
     */
    @Test
    void aDegradedShardKeepsTheNodeAt200AndNamesTheShard() throws Exception {
        WebServer server = new WebServer(null, null, null, null, null);
        java.util.concurrent.atomic.AtomicReference<String> reason = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<java.util.List<String>> shards =
                new java.util.concurrent.atomic.AtomicReference<>(java.util.List.of());
        server.setDegradation(reason::get);
        server.setDegradedShards(shards::get);
        java.util.concurrent.atomic.AtomicInteger readinessReads = new java.util.concurrent.atomic.AtomicInteger();
        server.setReadiness(() -> {
            readinessReads.incrementAndGet();
            return true;
        });
        io.javalin.Javalin app = server.start(0);
        try {
            java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
            java.net.URI uri = java.net.URI.create("http://127.0.0.1:" + app.port() + "/health");
            var ok = http.send(java.net.http.HttpRequest.newBuilder(uri).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(ok.statusCode()).isEqualTo(200);
            assertThat(ok.body()).doesNotContain("degradedShards");
            assertThat(readinessReads.get()).as("one readiness read per request").isEqualTo(1);

            reason.set("shard java: generation 7 was garbage-collected on the bucket");
            shards.set(java.util.List.of("java"));
            var degraded = http.send(java.net.http.HttpRequest.newBuilder(uri).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(degraded.statusCode()).as("routable for its other shards").isEqualTo(200);
            assertThat(degraded.body()).contains("\"status\":\"degraded\"", "\"degradedShards\":[\"java\"]",
                    "\"reason\":\"shard java: generation 7");
            assertThat(server.healthStatus()).isEqualTo(200);

            reason.set(null);
            shards.set(java.util.List.of());
            var again = http.send(java.net.http.HttpRequest.newBuilder(uri).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(again.statusCode()).isEqualTo(200);
            assertThat(again.body()).contains("\"status\":\"ok\"").doesNotContain("reason", "degradedShards");
        } finally {
            app.stop();
        }
    }

    @Test
    void readersPayloadListsLiveReaders() {
        WebServer server = new WebServer(null, null, null, null, null);
        server.setLiveReaders(() -> java.util.List.of(new com.pharos.cluster.membership.ReaderRegistry.LiveReader(
                new com.pharos.cluster.membership.ReaderHeartbeat(
                        "r1", "http://r1:7171", java.util.List.of("p1"), true, null, 0, 0, 1L), 7)));
        @SuppressWarnings("unchecked")
        var rows = (java.util.List<Map<String, Object>>) server.readersPayload().get("readers");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)).containsEntry("node", "r1").containsEntry("advertiseUrl", "http://r1:7171")
                .containsEntry("shards", java.util.List.of("p1")).containsEntry("draining", true)
                .containsEntry("ageSeconds", 7L);
    }
}
