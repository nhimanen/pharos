package com.pharos.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cli.IndexStatusCommand;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.membership.ReaderRegistry;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterTokenSecrecyTest {

    private static final String SECRET = "s3cr3t-cluster-token";

    private static ClusterConfig readerCfg() {
        ClusterConfig c = new ClusterConfig();
        c.setRole(NodeRole.READER);
        c.setBackend(com.pharos.cluster.BackendKind.OBJECT_STORE);
        c.getObjectStore().setBucket("b");
        c.setAdvertiseUrl("http://r:7171");
        c.setToken(SECRET);
        c.validate();
        return c;
    }

    @Test
    void saveThenLoadKeepsToken(@TempDir Path tmp) throws Exception {
        IndexConfig cfg = IndexConfig.defaults();
        cfg.setCluster(readerCfg());
        Path file = tmp.resolve("config.json"); // never the shared test home: later tests load it
        cfg.save(file);
        IndexConfig back = new ObjectMapper().findAndRegisterModules()
                .readValue(file.toFile(), IndexConfig.class);
        assertThat(back.getCluster().getToken()).isEqualTo(SECRET);
    }

    @Test
    void tokenNeverInAnyOutput(@TempDir Path tmp) throws Exception {
        ClusterConfig c = readerCfg();
        assertThat(c.toString()).doesNotContain(SECRET);
        assertThat(String.valueOf(c) + " " + IndexConfig.defaults().getCluster()).doesNotContain(SECRET);

        WebServer server = new WebServer(null, null, null, null, null);
        ObjectMapper m = new ObjectMapper();
        assertThat(m.writeValueAsString(server.health())).doesNotContain(SECRET);
        server.setReaderCacheStats(() -> java.util.Map.of("mode", "lazy"));
        server.setLiveReaders(() -> List.of(new ReaderRegistry.LiveReader(
                new ReaderHeartbeat("r1", c.getAdvertiseUrl(), List.of("p"), false, null, 0, 0, 1L), 1)));
        assertThat(m.writeValueAsString(server.health())).doesNotContain(SECRET);
        assertThat(m.writeValueAsString(server.readersPayload())).doesNotContain(SECRET);
        assertThat(m.writeValueAsString(new ReaderHeartbeat("r1", c.getAdvertiseUrl(), List.of(), false, null, 0, 0, 1L)))
                .doesNotContain(SECRET);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream old = System.out;
        System.setOut(new PrintStream(out, true));
        try {
            assertThat(new CommandLine(new IndexStatusCommand(ProjectRegistry.at(tmp.resolve("registry.json")),
                    NodeRole.READER)).execute("--json")).isZero();
        } finally {
            System.setOut(old);
        }
        assertThat(out.toString()).doesNotContain(SECRET);
    }

    /**
     * Phase 5 I2: {@code /health} shows peer addresses (discovery spec, writer and peer URLs) only with
     * the cluster token, compared in constant time; status and readiness are the same in both views.
     */
    @Test
    void healthShowsPeerAddressesOnlyWithTheToken(@TempDir Path tmp) throws Exception {
        WebServer server = new WebServer(null, null, null, null, null);
        server.setPeerViews(member -> member
                ? java.util.Map.of("discovery", "seeds:http://writer.internal:7171",
                        "writers", List.of(java.util.Map.of("node", "w1", "url", "http://writer.internal:7171")))
                : java.util.Map.of("writers", List.of(java.util.Map.of("node", "w1"))));
        server.setClusterToken(com.pharos.cluster.internal.InternalApi.tokenMatcher(SECRET));
        server.setReadiness(() -> false); // warming: 503 in both views
        io.javalin.Javalin app = server.start(0, tmp.resolve("web"));
        try {
            java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
            java.net.URI uri = java.net.URI.create("http://127.0.0.1:" + app.port() + "/health");
            java.util.function.Function<String, java.net.http.HttpResponse<String>> get = tok -> {
                var b = java.net.http.HttpRequest.newBuilder(uri);
                if (tok != null) b.header(com.pharos.cluster.internal.InternalApi.TOKEN_HEADER, tok);
                try {
                    return http.send(b.build(), java.net.http.HttpResponse.BodyHandlers.ofString());
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            };
            for (String tok : new String[]{null, "", "wrong", SECRET.substring(1), SECRET + "x"}) {
                var r = get.apply(tok);
                assertThat(r.statusCode()).as("token %s", tok).isEqualTo(503);
                assertThat(r.body()).as("token %s", tok).contains("\"w1\"", "\"warming\"")
                        .doesNotContain("writer.internal").doesNotContain("discovery");
            }
            var member = get.apply(SECRET);
            assertThat(member.statusCode()).isEqualTo(503);
            assertThat(member.body()).contains("writer.internal", "discovery", "\"warming\"").doesNotContain(SECRET);
        } finally {
            app.stop();
        }
    }

    /** Task 5: {@code /cluster/status} (and its events) never carry the token or store credentials. */
    @Test
    void statusNeverCarriesTokenOrCredentials(@TempDir Path tmp) throws Exception {
        IndexConfig cfg = IndexConfig.defaults();
        cfg.setIndexDir(tmp.resolve("indexes"));
        ClusterConfig c = readerCfg();
        c.setCacheDir(tmp.resolve("cache"));
        c.getObjectStore().setAccessKeyEnv("PHAROS_TEST_ACCESS_KEY_ENV");
        c.getObjectStore().setSecretKeyEnv("PHAROS_TEST_SECRET_KEY_ENV");
        c.getObjectStore().setEndpoint("http://minio.internal:9000");
        c.getStatus().setWebhookUrl("http://hooks.internal/notify?key=hook-secret-value");
        c.validate();
        cfg.setCluster(c);
        var store = new com.pharos.cluster.objectstore.InMemoryObjectStore();
        var layout = new com.pharos.cluster.objectstore.ObjectStoreLayout(c.getObjectStore().getPrefix());
        new com.pharos.cluster.objectstore.ObjectStoreCatalog(store, layout)
                .register(new com.pharos.cluster.ShardId("p"));
        ObjectMapper m = new ObjectMapper();
        store.put(layout.readerKey("r1"), m.writeValueAsBytes(new ReaderHeartbeat("r1", c.getAdvertiseUrl(),
                List.of("p"), false, java.util.Map.of("mode", "lazy"), 0, 1, 1L)));
        store.put(layout.writerKey("w1"), m.writeValueAsBytes(new com.pharos.cluster.writer.WriterHeartbeat("w1", true,
                List.of("p"), List.of(), java.util.Map.of(), null, 1L, false)));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        try (var backend = com.pharos.cluster.objectstore.TestBackends.objectStore(cfg, store);
             var lucene = new com.pharos.indexer.LuceneIndexer(cfg, backend.localCache());
             var runtime = new com.pharos.cluster.ClusterRuntime(cfg, backend, registry, lucene)) {
            String json = new String(runtime.statusService().currentJson(), java.nio.charset.StandardCharsets.UTF_8);
            assertThat(json).contains("\"r1\"").contains("\"w1\"");
            assertThat(json).as("the public view has no reader address").doesNotContain("advertiseUrl")
                    .doesNotContain(c.getAdvertiseUrl());

            // over HTTP: the address only with the right token (R12)
            WebServer server = new WebServer(null, null, null, null, null);
            server.setClusterStatus(runtime.statusService());
            server.setClusterToken(runtime.clusterTokenMatcher());
            io.javalin.Javalin app = server.start(0, tmp.resolve("web"));
            try {
                java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
                java.net.URI uri = java.net.URI.create("http://127.0.0.1:" + app.port() + "/cluster/status");
                java.util.function.Function<String, String> get = tok -> {
                    var b = java.net.http.HttpRequest.newBuilder(uri);
                    if (tok != null) b.header(com.pharos.cluster.internal.InternalApi.TOKEN_HEADER, tok);
                    try {
                        var r = http.send(b.build(), java.net.http.HttpResponse.BodyHandlers.ofString());
                        assertThat(r.statusCode()).isEqualTo(200);
                        return r.body();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                };
                assertThat(get.apply(null)).as("no token").doesNotContain("advertiseUrl");
                assertThat(get.apply("wrong-token")).as("a wrong token").doesNotContain("advertiseUrl");
                assertThat(get.apply(SECRET.substring(0, SECRET.length() - 1))).as("a prefix of the token")
                        .doesNotContain("advertiseUrl");
                String member = get.apply(SECRET);
                assertThat(member).as("the cluster token").contains("\"advertiseUrl\":\"" + c.getAdvertiseUrl() + "\"");
                for (String body : List.of(get.apply(null), member)) {
                    assertThat(body).doesNotContain(SECRET).doesNotContain("hook-secret-value");
                }
            } finally {
                app.stop();
            }
            for (String secret : List.of(SECRET, "hook-secret-value", "PHAROS_TEST_ACCESS_KEY_ENV",
                    "PHAROS_TEST_SECRET_KEY_ENV", "minio.internal")) {
                assertThat(json).as(secret).doesNotContain(secret);
            }
            String state = new String(store.get(layout.statusStateKey()).orElseThrow().data(),
                    java.nio.charset.StandardCharsets.UTF_8);
            assertThat(state).doesNotContain(SECRET).doesNotContain("hook-secret-value");
        }
    }
}
