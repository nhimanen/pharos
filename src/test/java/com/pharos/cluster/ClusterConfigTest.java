package com.pharos.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class ClusterConfigTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private ClusterConfig parse(String json) throws Exception {
        ClusterConfig c = mapper.readValue(json, IndexConfig.class).getCluster();
        c.validate();
        return c;
    }

    @Test
    void defaultsAreSingleNode() throws Exception {
        ClusterConfig c = parse("{\"verbose\": true}");
        assertThat(c.getRole()).isEqualTo(NodeRole.ALL);
        assertThat(c.getBackend()).isEqualTo(BackendKind.LOCAL);
        assertThat(c.getNodeId()).isNotBlank();
        assertThat(c.getRetentionGenerations()).isEqualTo(3);
    }

    @Test
    void readerNeedsObjectStore() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"local\"}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.backend");
    }

    @Test
    void allNeedsLocal() {
        assertThatThrownBy(() -> parse(
                "{\"cluster\":{\"role\":\"all\",\"backend\":\"object-store\",\"objectStore\":{\"bucket\":\"b\"}}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.backend");
    }

    @Test
    void objectStoreNeedsBucket() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"writer\",\"backend\":\"object-store\"}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.objectStore.bucket");
    }

    @Test
    void retentionAtLeastTwo() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"retentionGenerations\":1}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retentionGenerations");
    }

    @Test
    void parsesFullObjectStoreConfig() throws Exception {
        // Task 11's README example must match this JSON.
        String json = """
                {"cluster": {
                  "role": "writer",
                  "backend": "object-store",
                  "nodeId": "node-a",
                  "pollIntervalSeconds": 5,
                  "cacheDir": "/var/cache/pharos",
                  "retentionGenerations": 4,
                  "retentionMinutes": 120,
                  "objectStore": {
                    "endpoint": "http://localhost:9000",
                    "region": "eu-west-1",
                    "bucket": "pharos-bucket",
                    "prefix": "indexes",
                    "pathStyle": false,
                    "accessKeyEnv": "PHAROS_ACCESS_KEY",
                    "secretKeyEnv": "PHAROS_SECRET_KEY"
                  }}}
                """;
        ClusterConfig c = parse(json);
        assertThat(c.getRole()).isEqualTo(NodeRole.WRITER);
        assertThat(c.getBackend()).isEqualTo(BackendKind.OBJECT_STORE);
        assertThat(c.getNodeId()).isEqualTo("node-a");
        assertThat(c.getPollIntervalSeconds()).isEqualTo(5);
        assertThat(c.getCacheDir()).isEqualTo(Path.of("/var/cache/pharos"));
        assertThat(c.getRetentionGenerations()).isEqualTo(4);
        assertThat(c.getRetentionMinutes()).isEqualTo(120);
        ClusterConfig.ObjectStoreConfig o = c.getObjectStore();
        assertThat(o.getEndpoint()).isEqualTo("http://localhost:9000");
        assertThat(o.getRegion()).isEqualTo("eu-west-1");
        assertThat(o.getBucket()).isEqualTo("pharos-bucket");
        assertThat(o.getPrefix()).isEqualTo("indexes");
        assertThat(o.isPathStyle()).isFalse();
        assertThat(o.getAccessKeyEnv()).isEqualTo("PHAROS_ACCESS_KEY");
        assertThat(o.getSecretKeyEnv()).isEqualTo("PHAROS_SECRET_KEY");
    }

    @Test
    void serializedConfigRoundTripsClusterSection() throws Exception {
        String out = mapper.writeValueAsString(IndexConfig.defaults());
        IndexConfig back = mapper.readValue(out, IndexConfig.class);
        assertThat(back.getCluster().getRole()).isEqualTo(NodeRole.ALL);
        assertThat(back.getCluster().getBackend()).isEqualTo(BackendKind.LOCAL);
    }
}
