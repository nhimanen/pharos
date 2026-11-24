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
}
