package com.pharos.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cli.IndexStatusCommand;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The operator-facing surfaces of the reader block cache stats (index-status, /health, ./pharos). */
class WarmStatsSurfaceTest {

    static final List<String> FIELDS = List.of("mode", "cacheMaxBytes", "bytesOnDisk", "hits", "misses", "hitRate",
            "bytesFetched", "evictions", "leasesOpen");

    @TempDir Path tmp;
    private final PrintStream origOut = System.out;

    @AfterEach
    void restore() { System.setOut(origOut); }

    private static Map<String, Object> sample() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mode", "lazy");
        m.put("cacheMaxBytes", 8L << 30);
        m.put("bytesOnDisk", 1288490189L);
        m.put("hits", 973L);
        m.put("misses", 27L);
        m.put("hitRate", 0.973);
        m.put("bytesFetched", 5000L);
        m.put("evictions", 41L);
        m.put("leasesOpen", 0L);
        m.put("shared", true);
        return m;
    }

    private String run(IndexStatusCommand cmd, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out, true));
        assertThat(new CommandLine(cmd).execute(args)).isZero();
        System.setOut(origOut);
        return out.toString();
    }

    private IndexStatusCommand status(Map<String, Object> cache) {
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        return new IndexStatusCommand(registry, NodeRole.READER, () -> cache);
    }

    @Test
    void indexStatusJsonCarriesReaderCacheOnReaders() throws Exception {
        JsonNode j = new ObjectMapper().readTree(run(status(sample()), "--json"));
        JsonNode rc = j.get("readerCache");
        assertThat(rc).isNotNull();
        List<String> names = new java.util.ArrayList<>();
        rc.fieldNames().forEachRemaining(names::add);
        assertThat(names).containsAll(FIELDS);
        assertThat(rc.get("hits").asLong()).isEqualTo(973);
        assertThat(rc.get("hitRate").asDouble()).isEqualTo(0.973);
        assertThat(rc.get("mode").asText()).isEqualTo("lazy");
    }

    @Test
    void indexStatusJsonHasNoReaderCacheOffLazyReaders() throws Exception {
        assertThat(new ObjectMapper().readTree(run(status(null), "--json")).has("readerCache")).isFalse();
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        assertThat(new ObjectMapper().readTree(run(new IndexStatusCommand(registry), "--json"))
                .has("readerCache")).isFalse();
        assertThat(run(new IndexStatusCommand(registry))).doesNotContain("reader cache");
    }

    @Test
    void indexStatusTextPrintsOneReaderCacheLine() {
        assertThat(run(status(sample())).lines().filter(l -> l.startsWith("reader cache:")))
                .containsExactly("reader cache: 1.2 GiB / 8.0 GiB, hit rate 97.3%, 41 evictions");
    }

    @Test
    void runtimeReportsReaderCacheOnEveryReaderAndNotOnRoleAll() throws Exception {
        for (NodeRole role : List.of(NodeRole.ALL, NodeRole.READER)) {
            IndexConfig config = ClusterFixture.config(tmp.resolve(role.json()), role, "test-bucket");
            config.getCluster().getReader().setMode(ClusterConfig.ReaderConfig.FULL);
            ReplicationBackend backend = com.pharos.cluster.objectstore.TestBackends.objectStore(config,
                    new com.pharos.cluster.objectstore.InMemoryObjectStore());
            ProjectRegistry registry = ClusterRuntime.registryFor(config);
            try (LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
                 ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene)) {
                Map<String, Object> rc = runtime.readerCacheStats();
                if (role == NodeRole.ALL) {
                    assertThat(rc).as("role all").isNull();
                    continue;
                }
                assertThat(rc).containsEntry("mode", "full").containsEntry("cacheMaxBytes", 0L)
                        .containsEntry("bytesOnDisk", 0L).containsEntry("hits", 0L).containsEntry("misses", 0L)
                        .containsEntry("bytesFetched", 0L).containsEntry("evictions", 0L)
                        .containsEntry("leasesOpen", 0L).containsEntry("shared", false)
                        .containsEntry("source", "process").containsKey("hitRate");
                assertThat(ClusterRuntime.readerCacheLine(rc)).isEqualTo("reader cache: full mode (no block cache)");
            }
        }
    }

    @Test
    void textLineSaysNaBeforeAnyLookup() {
        Map<String, Object> rc = sample();
        rc.put("hits", 0L);
        rc.put("misses", 0L);
        rc.put("hitRate", 1.0);
        assertThat(ClusterRuntime.readerCacheLine(rc)).isEqualTo("reader cache: 1.2 GiB / 8.0 GiB, hit rate n/a, 41 evictions");
    }

    private String python(String body, String... args) throws Exception {
        String py = "import importlib.machinery as m, importlib.util as u, json, sys, threading, http.server\n"
                + "l = m.SourceFileLoader('pharos_cli', sys.argv[1]); s = u.spec_from_loader('pharos_cli', l)\n"
                + "mod = u.module_from_spec(s); l.exec_module(mod)\n" + body;
        List<String> cmd = new java.util.ArrayList<>(List.of("python3", "-I", "-B", "-c", py,
                Path.of("pharos").toAbsolutePath().toString()));
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        assertThat(p.waitFor()).as(out).isZero();
        return out.strip();
    }

    /** ./pharos prints the same line from the JSON, checked against a stub (no JVM, no daemon). */
    @Test
    void pythonClientPrintsTheReaderCacheLine() throws Exception {
        Path stub = tmp.resolve("stub.json");
        Files.writeString(stub, new ObjectMapper().writeValueAsString(sample()));
        String body = "print(mod.reader_cache_line(json.load(open(sys.argv[2]))))\n"
                + "print(mod.reader_cache_line({'mode':'lazy','bytesOnDisk':0,'cacheMaxBytes':1<<30,'hits':0,'misses':0,'hitRate':1.0,'evictions':0}))\n"
                + "print(mod.reader_cache_line({'mode':'full'}))\n";
        assertThat(python(body, stub.toString()).lines().toList()).containsExactly(
                "reader cache: 1.2 GiB / 8.0 GiB, hit rate 97.3%, 41 evictions",
                "reader cache: 0 B / 1.0 GiB, hit rate n/a, 0 evictions",
                "reader cache: full mode (no block cache)");
    }

    /** index-status --json through ./pharos: the daemon's /health readerCache replaces the process's. */
    @Test
    void pythonClientSubstitutesTheDaemonsReaderCacheInJson() throws Exception {
        Map<String, Object> proc = sample();
        proc.put("source", "process");
        Map<String, Object> daemon = sample();
        daemon.put("hits", 5L);
        daemon.put("source", "process"); // what the daemon's own JVM reports
        Path procJson = tmp.resolve("proc.json");
        Files.writeString(procJson, new ObjectMapper().writeValueAsString(Map.of("role", "reader",
                "readerCache", proc, "projects", List.of())));
        Path health = tmp.resolve("health.json");
        Files.writeString(health, new ObjectMapper().writeValueAsString(Map.of("status", "ok", "readerCache", daemon)));
        String body = "doc = open(sys.argv[2]).read(); body = open(sys.argv[3]).read()\n"
                + "class H(http.server.BaseHTTPRequestHandler):\n"
                + "    def do_GET(self):\n"
                + "        self.send_response(200); self.end_headers(); self.wfile.write(body.encode())\n"
                + "    def log_message(self, *a): pass\n"
                + "srv = http.server.HTTPServer(('127.0.0.1', 0), H)\n"
                + "threading.Thread(target=srv.serve_forever, daemon=True).start()\n"
                + "mod.BASE_URL = 'http://127.0.0.1:%d' % srv.server_port\n"
                + "out = json.loads(mod.index_status_json(doc))\n"
                + "print(json.dumps(out['readerCache']['source']), out['readerCache']['hits'])\n"
                + "mod.BASE_URL = 'http://127.0.0.1:1'\n"
                + "out = json.loads(mod.index_status_json(doc))\n"
                + "print(json.dumps(out['readerCache']['source']), out['readerCache']['hits'])\n";
        assertThat(python(body, procJson.toString(), health.toString()).lines().toList())
                .containsExactly("\"daemon\" 5", "\"process\" 973");
    }

    /**
     * A reader daemon answers 503 {"status":"warming"} until its first sync and models are done: the
     * client counts it as running (no second daemon is started) but not ready, and still reads its
     * readerCache.
     */
    @Test
    void pythonClientTreatsAWarmingDaemonAsRunningButNotReady() throws Exception {
        String body = "state = {'code': 503}\n"
                + "class H(http.server.BaseHTTPRequestHandler):\n"
                + "    def do_GET(self):\n"
                + "        c = state['code']; self.send_response(c); self.end_headers()\n"
                + "        self.wfile.write(json.dumps({'status': 'warming' if c == 503 else 'ok', 'ready': c == 200,\n"
                + "                                     'readerCache': {'hits': c}}).encode())\n"
                + "    def log_message(self, *a): pass\n"
                + "srv = http.server.HTTPServer(('127.0.0.1', 0), H)\n"
                + "threading.Thread(target=srv.serve_forever, daemon=True).start()\n"
                + "mod.BASE_URL = 'http://127.0.0.1:%d' % srv.server_port\n"
                + "print(mod._is_daemon_running(), mod._daemon_ready(), mod._daemon_reader_cache()['hits'])\n"
                + "state['code'] = 200\n"
                + "print(mod._is_daemon_running(), mod._daemon_ready(), mod._daemon_reader_cache()['hits'])\n"
                + "mod.BASE_URL = 'http://127.0.0.1:1'\n"
                + "print(mod._is_daemon_running(), mod._daemon_ready())\n";
        assertThat(python(body).lines().toList())
                .containsExactly("True False 503", "True True 200", "False False");
    }

    /**
     * Phase 5 I3: on a peer-pull writer with cluster.tls.keystore, ./pharos talks to the daemon's
     * plain-HTTP loopback listener (cluster.tls.localPort, default web port + 1).
     */
    @Test
    void pythonClientUsesTheLoopbackListenerOnATlsWriter() throws Exception {
        Path plain = tmp.resolve("plain.json");
        Files.writeString(plain, "{\"cluster\": {\"role\": \"writer\"}}");
        Path tls = tmp.resolve("tls.json");
        Files.writeString(tls, "{\"cluster\": {\"tls\": {\"keystore\": \"/k.p12\", \"passwordEnv\": \"P\"}}}");
        Path tlsPort = tmp.resolve("tls-port.json");
        Files.writeString(tlsPort, "{\"cluster\": {\"tls\": {\"keystore\": \"/k.p12\", \"localPort\": 9443}}}");
        String body = "from pathlib import Path\n"
                + "for f in sys.argv[2:]: print(mod._local_daemon_url(Path(f), 7171))\n";
        assertThat(python(body, plain.toString(), tls.toString(), tlsPort.toString(),
                tmp.resolve("absent.json").toString()).lines().toList())
                .containsExactly("http://localhost:7171", "http://127.0.0.1:7172", "http://127.0.0.1:9443",
                        "http://localhost:7171");
    }
}
