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
        assertThat(c.getNodeId()).as("not resolved for single node").isNull();
        assertThat(c.getRetentionGenerations()).isEqualTo(3);
    }

    @Test
    void readerNeedsObjectStore() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"local\"}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.backend");
    }

    @Test
    void writerNeedsObjectStore() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"writer\"}}"))
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

    // ── cluster.reader (Phase 2: lazy reader generations) ─────────────────────────────────

    @Test
    void readerDefaults() throws Exception {
        ClusterConfig.ReaderConfig r = parse("{\"verbose\": true}").getReader();
        assertThat(r.getMode()).isEqualTo("lazy");
        assertThat(r.isLazy()).isTrue();
        assertThat(r.getCacheMaxBytes()).isEqualTo(8L << 30);
        assertThat(r.getBlockSizeBytes()).isEqualTo(8 << 20);
        assertThat(r.getEagerMaxBytes()).isEqualTo(1L << 20);
        assertThat(r.getPrefetchExtensions()).containsExactly("tip", "tmd", "tim", "vex", "vem");
        assertThat(r.getPrefetchTimeoutSeconds()).isEqualTo(30);
        assertThat(r.getMaxConcurrentFetches()).isEqualTo(16);
    }

    @Test
    void parsesReaderSection() throws Exception {
        ClusterConfig.ReaderConfig r = parse("""
                {"cluster": {"reader": {"mode": "full", "cacheMaxBytes": 1073741824, "blockSizeBytes": 65536,
                  "eagerMaxBytes": 0, "prefetchExtensions": ["tip"], "prefetchTimeoutSeconds": 5,
                  "maxConcurrentFetches": 4}}}
                """).getReader();
        assertThat(r.getMode()).isEqualTo("full");
        assertThat(r.isLazy()).isFalse();
        assertThat(r.getCacheMaxBytes()).isEqualTo(1L << 30);
        assertThat(r.getBlockSizeBytes()).isEqualTo(64 * 1024);
        assertThat(r.getEagerMaxBytes()).isZero();
        assertThat(r.getPrefetchExtensions()).containsExactly("tip");
        assertThat(r.getPrefetchTimeoutSeconds()).isEqualTo(5);
        assertThat(r.getMaxConcurrentFetches()).isEqualTo(4);
    }

    @Test
    void blockSizeMustBeAPowerOfTwoInRange() {
        for (long bad : new long[]{100_000, 32 * 1024, 128L << 20, 0, -65536}) {
            assertThatThrownBy(() -> parse("{\"cluster\":{\"reader\":{\"blockSizeBytes\":" + bad + "}}}"))
                    .as("blockSizeBytes %d", bad)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("cluster.reader.blockSizeBytes");
        }
    }

    @Test
    void cacheMustHoldFourBlocks() throws Exception {
        assertThatThrownBy(() -> parse(
                "{\"cluster\":{\"reader\":{\"blockSizeBytes\":1048576,\"cacheMaxBytes\":4194303}}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.reader.cacheMaxBytes");
        parse("{\"cluster\":{\"reader\":{\"blockSizeBytes\":1048576,\"cacheMaxBytes\":4194304}}}");
    }

    @Test
    void unknownReaderModeIsRejected() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"reader\":{\"mode\":\"eager\"}}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.reader.mode")
                .hasMessageContaining("eager");
    }

    @Test
    void otherReaderLimitsNameTheirField() {
        for (String[] c : new String[][]{
                {"eagerMaxBytes", "-1"}, {"prefetchTimeoutSeconds", "-1"}, {"maxConcurrentFetches", "0"},
                {"prefetchExtensions", "null"}}) {
            assertThatThrownBy(() -> parse("{\"cluster\":{\"reader\":{\"" + c[0] + "\":" + c[1] + "}}}"))
                    .as(c[0])
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("cluster.reader." + c[0]);
        }
    }

    @Test
    void privateCacheCapDefaultsToAnEighthOfTheCacheWithinBounds() throws Exception {
        ClusterConfig.ReaderConfig r = parse("{\"verbose\": true}").getReader();
        assertThat(r.getPrivateCacheMaxBytes()).as("0 = derived").isZero();
        assertThat(r.effectivePrivateCacheMaxBytes()).isEqualTo(1L << 30); // 8 GiB / 8
        r = parse("{\"cluster\":{\"reader\":{\"blockSizeBytes\":1048576,\"cacheMaxBytes\":8388608}}}").getReader();
        assertThat(r.effectivePrivateCacheMaxBytes()).as("at least four blocks").isEqualTo(4L << 20);
        r = parse("{\"cluster\":{\"reader\":{\"blockSizeBytes\":1048576,\"cacheMaxBytes\":4194304}}}").getReader();
        assertThat(r.effectivePrivateCacheMaxBytes()).as("never above the cache cap").isEqualTo(4L << 20);
        r = parse("{\"cluster\":{\"reader\":{\"privateCacheMaxBytes\":33554432}}}").getReader();
        assertThat(r.effectivePrivateCacheMaxBytes()).isEqualTo(32L << 20);
        assertThatThrownBy(() -> parse("{\"cluster\":{\"reader\":{\"privateCacheMaxBytes\":1048576}}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.reader.privateCacheMaxBytes");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"reader\":{\"privateCacheMaxBytes\":-1}}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.reader.privateCacheMaxBytes");
    }

    @Test
    void objectStoreReadTimeoutDefaultsAndIsValidated() throws Exception {
        ClusterConfig c = parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\","
                + "\"objectStore\":{\"bucket\":\"b\"}}}");
        assertThat(c.getObjectStore().getReadTimeoutSeconds()).isEqualTo(10);
        assertThat(c.getObjectStore().readAttemptTimeout()).isEqualTo(java.time.Duration.ofSeconds(10));
        assertThat(c.getObjectStore().readTotalTimeout()).isEqualTo(java.time.Duration.ofSeconds(30));
        for (int bad : new int[]{0, -1, 3601}) {
            assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\","
                    + "\"objectStore\":{\"bucket\":\"b\",\"readTimeoutSeconds\":" + bad + "}}}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("cluster.objectStore.readTimeoutSeconds");
        }
        ClusterConfig five = parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\","
                + "\"objectStore\":{\"bucket\":\"b\",\"readTimeoutSeconds\":5}}}");
        assertThat(five.getObjectStore().readTotalTimeout()).isEqualTo(java.time.Duration.ofSeconds(15));
    }

    private static final String READER = "{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\","
            + "\"objectStore\":{\"bucket\":\"b\"}%s}}";

    @Test
    void readerNeedsAdvertiseUrl() {
        assertThatThrownBy(() -> parse(READER.formatted(",\"token\":\"t\"")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.advertiseUrl");
    }

    @Test
    void readerNeedsToken() {
        assertThatThrownBy(() -> parse(READER.formatted(",\"advertiseUrl\":\"http://r:7171\"")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.token");
    }

    @Test
    void readerMembershipDefaultsAndShards() throws Exception {
        ClusterConfig c = parse(READER.formatted(
                ",\"advertiseUrl\":\"http://r:7171\",\"token\":\"t\",\"reader\":{\"shards\":[\"a\",\"b-1\"]}"));
        assertThat(c.getHeartbeatSeconds()).isEqualTo(10);
        assertThat(c.getReaderTtlSeconds()).isEqualTo(30);
        assertThat(c.getReader().getShards()).containsExactly("a", "b-1");
    }

    @Test
    void readerShardsMustBeValidShardIds() {
        assertThatThrownBy(() -> parse(READER.formatted(
                ",\"advertiseUrl\":\"http://r:7171\",\"token\":\"t\",\"reader\":{\"shards\":[\"ok\",\"../x\"]}")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.reader.shards");
    }

    @Test
    void ttlMustExceedHeartbeat() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"heartbeatSeconds\":30,\"readerTtlSeconds\":30}}"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.readerTtlSeconds");
    }

    @Test
    void retireGraceSecondsDefaultsAndValidates() {
        ClusterConfig c = new ClusterConfig();
        org.assertj.core.api.Assertions.assertThat(c.getRetireGraceSeconds()).isEqualTo(120);
        c.setRetireGraceSeconds(0);
        org.assertj.core.api.Assertions.assertThatThrownBy(c::validate)
                .hasMessageContaining("cluster.retireGraceSeconds");
    }
}
