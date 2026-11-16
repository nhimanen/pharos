package com.pharos.web;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class WebServerHealthTest {

    @Test
    void healthReportsSyncingUntilReady() {
        WebServer server = new WebServer(null, null, null, null, null);
        assertThat(server.health()).isEqualTo(Map.of("status", "ok", "ready", true));

        AtomicBoolean ready = new AtomicBoolean(false);
        server.setReadiness(ready::get);
        assertThat(server.health()).isEqualTo(Map.of("status", "syncing", "ready", false));
        ready.set(true);
        assertThat(server.health()).isEqualTo(Map.of("status", "ok", "ready", true));
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
