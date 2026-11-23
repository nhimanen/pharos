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
}
