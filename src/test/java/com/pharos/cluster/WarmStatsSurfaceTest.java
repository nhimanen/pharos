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
    void runtimeReportsNoReaderCacheOffLazyReaders() throws Exception {
        for (NodeRole role : List.of(NodeRole.ALL, NodeRole.READER)) {
            IndexConfig config = ClusterFixture.config(tmp.resolve(role.json()), role, "test-bucket");
            config.getCluster().getReader().setMode(ClusterConfig.ReaderConfig.FULL);
            ReplicationBackend backend = com.pharos.cluster.objectstore.TestBackends.objectStore(config,
                    new com.pharos.cluster.objectstore.InMemoryObjectStore());
            ProjectRegistry registry = ClusterRuntime.registryFor(config);
            try (LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
                 ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene)) {
                assertThat(runtime.readerCacheStats()).as(role + " in full mode").isNull();
            }
        }
    }

    /** ./pharos prints the same line from the JSON, checked against a stub (no JVM, no daemon). */
    @Test
    void pythonClientPrintsTheReaderCacheLine() throws Exception {
        Path script = Path.of("pharos").toAbsolutePath();
        String json = new ObjectMapper().writeValueAsString(sample());
        Path stub = tmp.resolve("stub.json");
        Files.writeString(stub, json);
        String py = "import importlib.machinery as m, importlib.util as u, json, sys\n"
                + "l = m.SourceFileLoader('pharos_cli', sys.argv[1]); s = u.spec_from_loader('pharos_cli', l)\n"
                + "mod = u.module_from_spec(s); l.exec_module(mod)\n"
                + "print(mod.reader_cache_line(json.load(open(sys.argv[2]))))\n";
        Process p = new ProcessBuilder("python3", "-I", "-B", "-c", py, script.toString(), stub.toString())
                .redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        assertThat(p.waitFor()).as(out).isZero();
        assertThat(out.strip()).isEqualTo("reader cache: 1.2 GiB / 8.0 GiB, hit rate 97.3%, 41 evictions");
    }
}
