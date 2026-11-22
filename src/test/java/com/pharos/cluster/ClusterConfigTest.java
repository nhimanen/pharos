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
        ClusterConfig c = parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\",\"advertiseUrl\":\"http://r:7171\",\"token\":\"t\","
                + "\"objectStore\":{\"bucket\":\"b\"}}}");
        assertThat(c.getObjectStore().getReadTimeoutSeconds()).isEqualTo(10);
        assertThat(c.getObjectStore().readAttemptTimeout()).isEqualTo(java.time.Duration.ofSeconds(10));
        assertThat(c.getObjectStore().readTotalTimeout()).isEqualTo(java.time.Duration.ofSeconds(30));
        for (int bad : new int[]{0, -1, 3601}) {
            assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\",\"advertiseUrl\":\"http://r:7171\",\"token\":\"t\","
                    + "\"objectStore\":{\"bucket\":\"b\",\"readTimeoutSeconds\":" + bad + "}}}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("cluster.objectStore.readTimeoutSeconds");
        }
        ClusterConfig five = parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\",\"advertiseUrl\":\"http://r:7171\",\"token\":\"t\","
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

    @Test
    void retireGraceMustBeTwicePollInterval() {
        ClusterConfig c = new ClusterConfig();
        c.setPollIntervalSeconds(60);
        c.setRetireGraceSeconds(119);
        org.assertj.core.api.Assertions.assertThatThrownBy(c::validate)
                .hasMessageContaining("cluster.retireGraceSeconds").hasMessageContaining("cluster.pollIntervalSeconds");
        c.setRetireGraceSeconds(120);
        c.validate();
    }

    @Test
    void retireGraceMustCoverFanoutTimeout() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"retireGraceSeconds\":20,\"fanoutTimeoutMillis\":25000}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.retireGraceSeconds")
                .hasMessageContaining("cluster.fanoutTimeoutMillis");
        assertThatCode(() -> parse("{\"cluster\":{\"retireGraceSeconds\":25,\"fanoutTimeoutMillis\":25000}}"))
                .doesNotThrowAnyException();
    }

    @Test
    void fanoutTimeoutsHaveDefaultsAndMustBePositive() throws Exception {
        ClusterConfig c = parse("{}");
        assertThat(c.getFanoutTimeoutMillis()).isEqualTo(5000);
        assertThat(c.getConnectTimeoutMillis()).isEqualTo(1000);
        assertThatThrownBy(() -> parse("{\"cluster\":{\"connectTimeoutMillis\":0}}"))
                .hasMessageContaining("cluster.connectTimeoutMillis");
    }

    @Test
    void advertiseUrlMustBeHttpOrHttps() {
        String reader = "{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\",\"objectStore\":{\"bucket\":\"b\"},"
                + "\"token\":\"t\",\"advertiseUrl\":\"%s\"}}";
        for (String bad : new String[]{"r:7171", "ftp://r:7171", "file:///x", "http://", "not a url"}) {
            assertThatThrownBy(() -> parse(reader.formatted(bad)))
                    .as(bad).isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.advertiseUrl");
        }
        assertThatCode(() -> parse(reader.formatted("https://r.example:7171"))).doesNotThrowAnyException();
    }

    @Test
    void queryTimeoutDefaultsAndBounds() throws Exception {
        ClusterConfig c = parse("{}");
        assertThat(c.effectiveQueryTimeoutMillis()).isEqualTo(20_000);
        assertThat(c.getMaxCatalogAgeSeconds()).isEqualTo(600);
        assertThat(parse("{\"cluster\":{\"retireGraceSeconds\":25,\"fanoutTimeoutMillis\":25000}}")
                .effectiveQueryTimeoutMillis()).as("unset: raised to the fan-out timeout").isEqualTo(25_000);
        assertThatThrownBy(() -> parse("{\"cluster\":{\"queryTimeoutMillis\":4000}}"))
                .hasMessageContaining("cluster.queryTimeoutMillis").hasMessageContaining("cluster.fanoutTimeoutMillis");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"queryTimeoutMillis\":60000}}"))
                .hasMessageContaining("cluster.queryTimeoutMillis").hasMessageContaining("60000 ms");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"maxCatalogAgeSeconds\":0}}"))
                .hasMessageContaining("cluster.maxCatalogAgeSeconds");
        assertThat(parse("{\"cluster\":{\"queryTimeoutMillis\":5000}}").effectiveQueryTimeoutMillis()).isEqualTo(5000);
    }

    @Test
    void drainSecondsDefaultsAndBounds() throws Exception {
        assertThat(parse("{}").getDrainSeconds()).isEqualTo(20);
        assertThat(parse("{\"cluster\":{\"drainSeconds\":0}}").getDrainSeconds()).isZero();
        assertThatThrownBy(() -> parse("{\"cluster\":{\"drainSeconds\":-1}}")).hasMessageContaining("cluster.drainSeconds");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"drainSeconds\":3601}}")).hasMessageContaining("cluster.drainSeconds");
    }

    @Test
    void writerSectionDefaultsAndValidation() throws Exception {
        ClusterConfig.WriterConfig w = parse("{}").getWriter();
        assertThat(w.getPollSeconds()).isEqualTo(60);
        assertThat(w.isWorkQueue()).as("work-queue mode is the writer default").isTrue();
        assertThat(parse("{\"cluster\":{\"writer\":{\"workQueue\":false}}}").getWriter().isWorkQueue()).isFalse();
        assertThat(w.getWorkDir()).isEqualTo(IndexConfig.DEFAULT_BASE.resolve("work"));
        w = parse("{\"cluster\":{\"writer\":{\"workDir\":\"/w\",\"pollSeconds\":5,\"workQueue\":true}}}").getWriter();
        assertThat(w.getWorkDir()).isEqualTo(Path.of("/w"));
        assertThat(w.getPollSeconds()).isEqualTo(5);
        assertThat(w.isWorkQueue()).isTrue();
        assertThatThrownBy(() -> parse("{\"cluster\":{\"writer\":{\"pollSeconds\":0}}}"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.writer.pollSeconds");
    }

    @Test
    void writerLeaseTtlDefaultsAndValidation() throws Exception {
        ClusterConfig.WriterConfig w = parse("{}").getWriter();
        assertThat(w.getLeaseTtlSeconds()).isEqualTo(30);
        assertThat(w.leaseRenewInterval()).isEqualTo(java.time.Duration.ofSeconds(10));
        w = parse("{\"cluster\":{\"writer\":{\"leaseTtlSeconds\":5}}}").getWriter();
        assertThat(w.leaseTtl()).isEqualTo(java.time.Duration.ofSeconds(5));
        assertThat(w.leaseRenewInterval()).isEqualTo(java.time.Duration.ofNanos(1_666_666_666));
        assertThatThrownBy(() -> parse("{\"cluster\":{\"writer\":{\"leaseTtlSeconds\":4}}}"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.writer.leaseTtlSeconds");
    }

    /** Task 5: {@code cluster.status} defaults, shortened windows, and validation. */
    @Test
    void statusTargetsWindowsAndWebhook() throws Exception {
        ClusterConfig d = parse("{}");
        assertThat(d.getStatus().getTargets().getWarmP95Millis()).isEqualTo(500);
        assertThat(d.getStatus().getTargets().getQpsPerReader()).isEqualTo(50.0);
        assertThat(d.getStatus().getTargets().getBacklog()).isEqualTo(600);
        assertThat(d.getStatus().getWindows().getAddReadersSeconds()).isEqualTo(300);
        assertThat(d.getStatus().getWindows().getRemoveReadersSeconds()).isEqualTo(1800);
        assertThat(d.getStatus().getEventsKeepAliveSeconds()).isEqualTo(15);
        assertThat(d.getStatus().getWebhookUrl()).isNull();

        ClusterConfig c = parse("{\"cluster\":{\"status\":{\"targets\":{\"warmP95Millis\":250,\"qpsPerReader\":5,"
                + "\"backlog\":30},\"windows\":{\"addReadersSeconds\":20,\"clearGraceSeconds\":0},"
                + "\"webhookUrl\":\"http://127.0.0.1:9/hook\"}}}");
        var st = com.pharos.cluster.status.StatusSettings.of(c.getStatus());
        assertThat(st.warmP95Millis()).isEqualTo(250);
        assertThat(st.qpsPerReader()).isEqualTo(5.0);
        assertThat(st.backlogSeconds()).isEqualTo(30);
        assertThat(st.window(com.pharos.cluster.status.Proposals.ADD_READERS)).isEqualTo(20);
        assertThat(st.clearGraceSeconds()).isZero();

        assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"targets\":{\"qpsPerReader\":0}}}}"))
                .hasMessageContaining("cluster.status.targets.qpsPerReader");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"windows\":{\"reshardSeconds\":-1}}}}"))
                .hasMessageContaining("cluster.status.windows");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"webhookUrl\":\"ftp://x/y?k=secret\"}}}"))
                .hasMessageContaining("cluster.status.webhookUrl").satisfies(e -> assertThat(e.getMessage())
                        .doesNotContain("secret"));
        assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"eventsKeepAliveSeconds\":0}}}"))
                .hasMessageContaining("eventsKeepAliveSeconds");
    }

    @Test
    void peerPullBackendForcesWorkQueueOffAndNeedsAToken() throws Exception {
        String writer = "{\"cluster\":{\"role\":\"writer\",\"backend\":\"peer-pull\",\"token\":\"t\"";
        ClusterConfig c = parse(writer + "}}");
        assertThat(c.getBackend()).isEqualTo(BackendKind.PEER_PULL);
        assertThat(c.getWriter().isWorkQueue()).as("forced off in peer mode (P5-R6)").isFalse();
        assertThat(c.getWriter().getWorkQueue()).as("nothing configured").isNull();
        assertThat(parse(writer + ",\"writer\":{\"workQueue\":false}}}").getWriter().isWorkQueue()).isFalse();
        // the backend key after the writer section: same result
        assertThat(parse("{\"cluster\":{\"writer\":{},\"role\":\"writer\",\"token\":\"t\",\"backend\":\"peer-pull\"}}")
                .getWriter().isWorkQueue()).isFalse();
        assertThatThrownBy(() -> parse(writer + ",\"writer\":{\"workQueue\":true}}}"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.writer.workQueue")
                .hasMessageContaining("peer-pull");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"writer\",\"backend\":\"peer-pull\"}}"))
                .hasMessageContaining("cluster.token");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"all\",\"backend\":\"peer-pull\"}}"))
                .hasMessageContaining("'local'");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"writer\"}}"))
                .hasMessageContaining("'object-store' or 'peer-pull'");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"backend\":\"bucketless\"}}"))
                .hasStackTraceContaining("peer-pull");
        // object-store keeps the work-queue default
        assertThat(parse("{\"cluster\":{\"role\":\"writer\",\"backend\":\"object-store\","
                + "\"objectStore\":{\"bucket\":\"b\"}}}").getWriter().isWorkQueue()).isTrue();
    }

    @Test
    void workQueueIsSavedOnlyWhenConfigured() throws Exception {
        IndexConfig cfg = mapper.readValue("{\"cluster\":{\"role\":\"writer\",\"backend\":\"peer-pull\","
                + "\"token\":\"t\"}}", IndexConfig.class);
        cfg.getCluster().validate();
        String saved = mapper.writeValueAsString(cfg);
        assertThat(saved).doesNotContain("workQueue");
        // a forced-off writer saved and loaded again is still valid
        ClusterConfig back = mapper.readValue(saved, IndexConfig.class).getCluster();
        back.validate();
        assertThat(back.getWriter().isWorkQueue()).isFalse();
        cfg.getCluster().getWriter().setWorkQueue(false);
        assertThat(mapper.writeValueAsString(cfg)).contains("\"workQueue\":false");
    }

    @Test
    void peersAndTlsDefaultsAndValidation() throws Exception {
        ClusterConfig c = parse("{}");
        assertThat(c.getPeers().getRefreshSeconds()).isEqualTo(15);
        assertThat(c.getPeers().getMaxTransfersPerWriter()).isEqualTo(8);
        assertThat(c.getPeers().getMaxTransfersPerReader()).isEqualTo(4);
        assertThat(c.getPeers().getLongPollSeconds()).isEqualTo(30);
        assertThat(c.getPeers().getMaxLongPolls()).isEqualTo(4096);
        assertThat(c.getPeers().getStoreDir()).isEqualTo(IndexConfig.DEFAULT_BASE.resolve("peer-store"));
        assertThat(c.getTls()).isNull();
        assertThat(c.tlsServer()).isFalse();
        assertThatThrownBy(() -> parse("{\"cluster\":{\"peers\":{\"maxTransfersPerWriter\":0}}}"))
                .hasMessageContaining("cluster.peers.maxTransfersPerWriter");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"peers\":{\"maxTransfersPerReader\":0}}}"))
                .hasMessageContaining("cluster.peers.maxTransfersPerReader");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"peers\":{\"longPollSeconds\":301}}}"))
                .hasMessageContaining("cluster.peers.longPollSeconds");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"peers\":{\"maxLongPolls\":0}}}"))
                .hasMessageContaining("cluster.peers.maxLongPolls");
        String peerWriter = "\"role\":\"writer\",\"backend\":\"peer-pull\",\"token\":\"t\",";
        assertThatThrownBy(() -> parse("{\"cluster\":{" + peerWriter + "\"tls\":{\"keystore\":\"/k.p12\"}}}"))
                .hasMessageContaining("cluster.tls.passwordEnv");
        ClusterConfig tls = parse("{\"cluster\":{" + peerWriter
                + "\"tls\":{\"keystore\":\"/k.p12\",\"passwordEnv\":\"PATH\"}}}");
        assertThat(tls.tlsServer()).isTrue();
        assertThat(tls.getTls().getKeystoreType()).isEqualTo("PKCS12");
        assertThat(new String(tls.getTls().password())).isEqualTo(System.getenv("PATH"));
        assertThat(tls.getTls().localPortFor(7171)).as("I3: the loopback plain port, default web port + 1").isEqualTo(7172);
        assertThat(parse("{\"cluster\":{" + peerWriter + "\"tls\":{\"keystore\":\"/k.p12\",\"passwordEnv\":\"PATH\","
                + "\"localPort\":9000}}}").getTls().localPortFor(7171)).isEqualTo(9000);
        assertThatThrownBy(() -> parse("{\"cluster\":{" + peerWriter + "\"tls\":{\"keystore\":\"/k.p12\","
                + "\"passwordEnv\":\"PATH\",\"localPort\":70000}}}")).hasMessageContaining("cluster.tls.localPort");
        ClusterConfig unset = parse("{\"cluster\":{" + peerWriter + "\"tls\":{\"truststore\":\"/t.p12\","
                + "\"passwordEnv\":\"PHAROS_TEST_SURELY_UNSET_VAR\"}}}");
        assertThat(unset.tlsServer()).as("a truststore alone is client-side only").isFalse();
        assertThatThrownBy(() -> unset.getTls().password()).hasMessageContaining("PHAROS_TEST_SURELY_UNSET_VAR");
        // P5-R7: TLS only on peer-pull writers for now
        String keystore = "\"tls\":{\"keystore\":\"/k.p12\",\"passwordEnv\":\"PATH\"}";
        for (String other : new String[]{"",
                "\"role\":\"writer\",\"backend\":\"object-store\",\"objectStore\":{\"bucket\":\"b\"},",
                "\"role\":\"reader\",\"backend\":\"object-store\",\"objectStore\":{\"bucket\":\"b\"},"
                        + "\"advertiseUrl\":\"http://r:1\",\"token\":\"t\",",
                "\"role\":\"reader\",\"backend\":\"peer-pull\",\"advertiseUrl\":\"http://r:1\",\"token\":\"t\","
                        + "\"peers\":{\"discovery\":\"dns:w\"},"}) {
            assertThatThrownBy(() -> parse("{\"cluster\":{" + other + keystore + "}}")).as(other)
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.tls is not supported")
                    .hasMessageContaining("only peer-pull writers");
        }
        assertThatThrownBy(() -> parse("{\"cluster\":{\"tls\":{\"truststore\":\"/t.p12\",\"passwordEnv\":\"PATH\"}}}"))
                .hasMessageContaining("cluster.tls is not supported");
        assertThat(parse("{\"cluster\":{\"tls\":{}}}").tlsServer()).as("an empty section configures nothing").isFalse();
    }

    /** Phase 5 Task 2: a peer-pull reader needs discovery and may hold a truststore (P5-R7 carry). */
    @Test
    void peerReaderNeedsDiscoveryAndMayHoldATruststore() throws Exception {
        String peerReader = "\"role\":\"reader\",\"backend\":\"peer-pull\",\"advertiseUrl\":\"http://r:1\",\"token\":\"t\",";
        assertThatThrownBy(() -> parse("{\"cluster\":{" + peerReader.substring(0, peerReader.length() - 1) + "}}"))
                .hasMessageContaining("cluster.peers.discovery is required");
        ClusterConfig dns = parse("{\"cluster\":{" + peerReader + "\"peers\":{\"discovery\":\"dns:writers.ns.svc:7070\"}}}");
        assertThat(dns.getPeers().getDiscovery()).isEqualTo("dns:writers.ns.svc:7070");
        parse("{\"cluster\":{" + peerReader + "\"peers\":{\"discovery\":\"seeds:http://w1:7070,https://w2\"}}}");
        for (String bad : new String[]{"dns:", "dns:bad_name", "dns:w:0", "dns:w:x", "seeds:", "seeds:ftp://w",
                "seeds:http://u:p@w", "w1:7070"}) {
            assertThatThrownBy(() -> parse("{\"cluster\":{" + peerReader + "\"peers\":{\"discovery\":\"" + bad + "\"}}}"))
                    .as(bad).isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.peers.discovery");
        }
        String trust = "\"tls\":{\"truststore\":\"/t.p12\",\"passwordEnv\":\"PATH\"}";
        ClusterConfig reader = parse("{\"cluster\":{" + peerReader + "\"peers\":{\"discovery\":\"dns:w\"}," + trust + "}}");
        assertThat(reader.getTls().getTruststore()).hasToString("/t.p12");
        assertThat(reader.tlsServer()).as("a reader never listens with TLS").isFalse();
        assertThatThrownBy(() -> parse("{\"cluster\":{" + peerReader + "\"peers\":{\"discovery\":\"dns:w\"},"
                + "\"tls\":{\"keystore\":\"/k.p12\",\"truststore\":\"/t.p12\",\"passwordEnv\":\"PATH\"}}}"))
                .hasMessageContaining("cluster.tls is not supported").hasMessageContaining("cluster.tls.keystore");
        // P5-R8: with a truststore every seed must be https
        assertThatThrownBy(() -> parse("{\"cluster\":{" + peerReader + "\"peers\":{\"discovery\":"
                + "\"seeds:https://w1,http://w2\"}," + trust + "}}"))
                .hasMessageContaining("every seed must be https://");
        ClusterConfig httpsSeeds = parse("{\"cluster\":{" + peerReader + "\"peers\":{\"discovery\":"
                + "\"seeds:https://w1\",\"allowHosts\":[\"w2\"]}," + trust + "}}");
        assertThat(httpsSeeds.getPeers().getAllowHosts()).containsExactly("w2");
        // object-store readers and coordinators are unchanged: no truststore there yet
        assertThatThrownBy(() -> parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\","
                + "\"objectStore\":{\"bucket\":\"b\"},\"advertiseUrl\":\"http://r:1\",\"token\":\"t\"," + trust + "}}"))
                .hasMessageContaining("cluster.tls is not supported");
    }

    // ── cluster.tokenEnv and the scale thresholds (operator Task 1) ───────────────────────────

    /** An env var no test machine sets. */
    private static final String UNSET_ENV = "PHAROS_TEST_TOKEN_ENV_THAT_IS_NEVER_SET_7F3A";

    @Test
    void tokenFromEnv() throws Exception {
        String home = System.getenv("PATH"); // set on every machine and CI runner (the value stands in for a token)
        assertThat(home).isNotBlank();
        ClusterConfig c = parse("{\"cluster\":{\"role\":\"reader\",\"backend\":\"object-store\","
                + "\"objectStore\":{\"bucket\":\"b\"},\"advertiseUrl\":\"http://r:7171\",\"tokenEnv\":\"PATH\"}}");
        assertThat(c.getTokenEnv()).isEqualTo("PATH");
        assertThat(c.effectiveToken()).as("read from the env var").isEqualTo(home);
        assertThat(c.getToken()).as("the configured token stays unset").isNull();
        assertThat(c.toString()).contains("tokenEnv=PATH").doesNotContain(home);
        // the token read from env is never written back into config.json
        String saved = mapper.writeValueAsString(mapper.readValue("{\"cluster\":{\"tokenEnv\":\"PATH\"}}",
                IndexConfig.class));
        assertThat(saved).contains("\"tokenEnv\":\"PATH\"").doesNotContain(home);

        // a literal token still works as before
        assertThat(parse("{\"cluster\":{\"token\":\"t\"}}").effectiveToken()).isEqualTo("t");
        assertThat(parse("{}").effectiveToken()).isNull();

        // an env var that is not set: refused, naming the variable (never a value)
        assertThatThrownBy(() -> parse("{\"cluster\":{\"tokenEnv\":\"" + UNSET_ENV + "\"}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.tokenEnv").hasMessageContaining(UNSET_ENV);
        assertThatThrownBy(() -> parse("{\"cluster\":{\"tokenEnv\":\" \"}}"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.tokenEnv");
    }

    @Test
    void tokenAndTokenEnvTogetherIsInvalid() {
        assertThatThrownBy(() -> parse("{\"cluster\":{\"token\":\"s3cret\",\"tokenEnv\":\"PATH\"}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.token").hasMessageContaining("cluster.tokenEnv")
                .hasMessageContaining("mutually exclusive")
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("s3cret"));
    }

    @Test
    void statusScaleThresholds() throws Exception {
        ClusterConfig d = parse("{}");
        assertThat(d.getStatus().getRemoveReadersLoad()).isEqualTo(0.2);
        assertThat(d.getStatus().getWriters().getBacklogShards()).isEqualTo(1);
        assertThat(d.getStatus().getWriters().getOldestPendingSeconds()).isEqualTo(300);
        assertThat(d.getStatus().getWriters().getIdleSeconds()).as("unset: windows.removeWritersSeconds applies").isNull();
        var ds = com.pharos.cluster.status.StatusSettings.of(d.getStatus());
        assertThat(ds.window(com.pharos.cluster.status.Proposals.REMOVE_WRITERS)).isEqualTo(1800);

        ClusterConfig c = parse("{\"cluster\":{\"status\":{\"removeReadersLoad\":0.35,"
                + "\"writers\":{\"backlogShards\":2,\"oldestPendingSeconds\":120,\"idleSeconds\":900}}}}");
        var st = com.pharos.cluster.status.StatusSettings.of(c.getStatus());
        assertThat(st.removeReadersLoad()).isEqualTo(0.35);
        assertThat(st.backlogShards()).isEqualTo(2);
        assertThat(st.oldestPendingSeconds()).isEqualTo(120);
        assertThat(st.window(com.pharos.cluster.status.Proposals.REMOVE_WRITERS)).isEqualTo(900);
        // the older windows.removeWritersSeconds still applies when writers.idleSeconds is not set
        var legacy = com.pharos.cluster.status.StatusSettings.of(parse(
                "{\"cluster\":{\"status\":{\"windows\":{\"removeWritersSeconds\":60}}}}").getStatus());
        assertThat(legacy.window(com.pharos.cluster.status.Proposals.REMOVE_WRITERS)).isEqualTo(60);

        // writers.idleSeconds and windows.removeWritersSeconds set the same window: different values are refused
        assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"windows\":{\"removeWritersSeconds\":60},"
                + "\"writers\":{\"idleSeconds\":900}}}}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster.status.writers.idleSeconds")
                .hasMessageContaining("cluster.status.windows.removeWritersSeconds");
        var same = com.pharos.cluster.status.StatusSettings.of(parse("{\"cluster\":{\"status\":{\"windows\":"
                + "{\"removeWritersSeconds\":900},\"writers\":{\"idleSeconds\":900}}}}").getStatus());
        assertThat(same.window(com.pharos.cluster.status.Proposals.REMOVE_WRITERS)).as("equal: allowed").isEqualTo(900);
        // the default (1800) is not a setting: idleSeconds alone, saved and reloaded, stays valid
        String saved = mapper.writeValueAsString(mapper.readValue(
                "{\"cluster\":{\"status\":{\"writers\":{\"idleSeconds\":900}}}}", IndexConfig.class));
        assertThat(saved).doesNotContain("removeWritersSeconds");
        assertThat(com.pharos.cluster.status.StatusSettings.of(parse(saved).getStatus())
                .window(com.pharos.cluster.status.Proposals.REMOVE_WRITERS)).isEqualTo(900);
        String savedLegacy = mapper.writeValueAsString(mapper.readValue(
                "{\"cluster\":{\"status\":{\"windows\":{\"removeWritersSeconds\":60}}}}", IndexConfig.class));
        assertThat(com.pharos.cluster.status.StatusSettings.of(parse(savedLegacy).getStatus())
                .window(com.pharos.cluster.status.Proposals.REMOVE_WRITERS)).as("an explicit value round-trips")
                .isEqualTo(60);

        for (String bad : new String[]{"0", "1", "-0.1", "1.5"}) {
            assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"removeReadersLoad\":" + bad + "}}}"))
                    .as(bad).hasMessageContaining("cluster.status.removeReadersLoad");
        }
        assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"writers\":{\"backlogShards\":0}}}}"))
                .hasMessageContaining("cluster.status.writers.backlogShards");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"writers\":{\"oldestPendingSeconds\":-1}}}}"))
                .hasMessageContaining("cluster.status.writers.oldestPendingSeconds");
        assertThatThrownBy(() -> parse("{\"cluster\":{\"status\":{\"writers\":{\"idleSeconds\":-1}}}}"))
                .hasMessageContaining("cluster.status.writers.idleSeconds");
    }
}
