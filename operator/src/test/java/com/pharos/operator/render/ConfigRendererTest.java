package com.pharos.operator.render;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.pharos.cluster.BackendKind;
import com.pharos.cluster.NodeRole;
import com.pharos.config.ClusterConfig;
import com.pharos.operator.Defaults;
import com.pharos.operator.TestClusters;
import com.pharos.operator.api.PharosCluster;
import com.pharos.operator.api.PharosClusterSpec;
import com.pharos.operator.api.PharosClusterSpec.Autoscaling;
import com.pharos.operator.api.PharosClusterSpec.Triggers;
import com.pharos.operator.render.ConfigRenderer.Role;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigRendererTest {

    private static final JsonNodeFactory F = JsonNodeFactory.instance;

    private static ConfigRenderer.Result render(PharosCluster c) {
        return ConfigRenderer.render(c);
    }

    private static ClusterConfig config(PharosCluster c, Role role) {
        ConfigRenderer.Result r = render(c);
        assertThat(r.errors()).isEmpty();
        return r.get(role).config();
    }

    /** The rendered file read back the way pharos reads config.json (a plain ObjectMapper, no operator modules). */
    private static ClusterConfig asPharosReadsIt(PharosCluster c, Role role) throws Exception {
        JsonNode file = new ObjectMapper().readTree(render(c).get(role).json());
        return new ObjectMapper().configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false).treeToValue(file.get("cluster"), ClusterConfig.class);
    }

    private static PharosCluster withTriggers(Triggers t) {
        PharosCluster base = TestClusters.minio();
        PharosClusterSpec s = base.getSpec();
        Autoscaling a = new Autoscaling(true, null, null, null, t, null);
        return TestClusters.withSpec(base, new PharosClusterSpec(s.version(), s.storage(), s.embeddings(), s.projects(),
                s.writers(), s.readers(), s.router(), a, s.pharosConfig()));
    }

    private static PharosCluster withPharosConfig(Map<String, JsonNode> extra) {
        PharosCluster base = TestClusters.minio();
        return TestClusters.withSpec(base, TestClusters.withPharosConfig(base.getSpec(), extra));
    }

    @Test
    void triggersMapOneToOneOntoStatusConfig() throws Exception {
        Triggers t = new Triggers(
                new Triggers.ReaderTriggers(new Triggers.ReaderScaleUp(400, 30, 240),
                        new Triggers.ReaderScaleDown(25, 1200)),
                new Triggers.WriterTriggers(new Triggers.WriterScaleUp(3, 500, 90),
                        new Triggers.WriterScaleDown(2400)));
        for (Role role : Role.values()) {
            ClusterConfig.StatusConfig s = asPharosReadsIt(withTriggers(t), role).getStatus();
            assertThat(s.getTargets().getWarmP95Millis()).isEqualTo(400);
            assertThat(s.getTargets().getQpsPerReader()).isEqualTo(30.0);
            assertThat(s.getWindows().getAddReadersSeconds()).isEqualTo(240);
            assertThat(s.getRemoveReadersLoad()).isEqualTo(0.25);
            assertThat(s.getWindows().getRemoveReadersSeconds()).isEqualTo(1200);
            assertThat(s.getWriters().getBacklogShards()).isEqualTo(3);
            assertThat(s.getWriters().getOldestPendingSeconds()).isEqualTo(500);
            assertThat(s.getWindows().getAddWritersSeconds()).isEqualTo(90);
            assertThat(s.getWriters().getIdleSeconds()).isEqualTo(2400L);
            assertThat(s.effectiveRemoveWritersSeconds()).isEqualTo(2400);
            assertThat(s.getWindows().getRemoveWritersSeconds()).isNull(); // one key per trigger, no conflict
        }
        // Unset triggers keep pharos's defaults, except qpsPerReader: the operator's default is rendered explicitly.
        ClusterConfig.StatusConfig defaults = asPharosReadsIt(withTriggers(null), Role.READER).getStatus();
        ClusterConfig.StatusConfig pharos = new ClusterConfig.StatusConfig();
        assertThat(defaults.getTargets().getQpsPerReader()).isEqualTo(Defaults.QPS_PER_READER);
        assertThat(defaults.getTargets().getWarmP95Millis()).isEqualTo(pharos.getTargets().getWarmP95Millis());
        assertThat(defaults.getRemoveReadersLoad()).isEqualTo(pharos.getRemoveReadersLoad());
        assertThat(defaults.getWriters().getIdleSeconds()).isNull();
    }

    @Test
    void writerConfigHasWorkQueueAndSharedWorkDir() throws Exception {
        ClusterConfig w = asPharosReadsIt(TestClusters.minio(), Role.WRITER);
        assertThat(w.getRole()).isEqualTo(NodeRole.WRITER);
        assertThat(w.getBackend()).isEqualTo(BackendKind.OBJECT_STORE);
        assertThat(w.getWriter().getWorkQueue()).isTrue();
        assertThat(w.getWriter().getWorkDir()).isEqualTo(Path.of("/data/work"));
        assertThat(w.getNodeId()).isEqualTo("$(POD_NAME)");
        assertThat(w.getAdvertiseUrl()).isEqualTo("http://$(POD_NAME).demo-writers-hl.pharos.svc:7070");
        assertThat(w.getTokenEnv()).isEqualTo("PHAROS_TOKEN");
        assertThat(w.getToken()).isNull();
        assertThat(w.getObjectStore().getEndpoint()).isEqualTo("http://demo-minio.pharos.svc:9000");
        assertThat(w.getObjectStore().getBucket()).isEqualTo("pharos");
        assertThat(w.getObjectStore().getAccessKeyEnv()).isEqualTo("PHAROS_S3_ACCESS_KEY");
        assertThat(w.getObjectStore().getSecretKeyEnv()).isEqualTo("PHAROS_S3_SECRET_KEY");
        // No path in the file comes from the operator's own home directory.
        assertThat(render(TestClusters.minio()).get(Role.WRITER).json()).doesNotContain(System.getProperty("user.home"));
    }

    @Test
    void readerConfigHasModeAndCacheCap() throws Exception {
        ClusterConfig r = asPharosReadsIt(TestClusters.minio(), Role.READER); // readers: mode lazy, cacheVolume 20Gi
        assertThat(r.getRole()).isEqualTo(NodeRole.READER);
        assertThat(r.getReader().getMode()).isEqualTo("lazy");
        assertThat(r.getReader().getCacheMaxBytes()).isEqualTo(20L * 1024 * 1024 * 1024 * 3 / 4);
        assertThat(r.getCacheDir()).isEqualTo(Path.of("/data/cache"));
        assertThat(r.getAdvertiseUrl()).isEqualTo("http://$(POD_NAME).demo-readers-hl.pharos.svc:7070");
        assertThat(r.getNodeId()).isEqualTo("$(POD_NAME)");

        PharosCluster base = TestClusters.minio();
        PharosClusterSpec s = base.getSpec();
        PharosCluster full = TestClusters.withSpec(base, new PharosClusterSpec(s.version(), s.storage(), s.embeddings(),
                s.projects(), s.writers(), new PharosClusterSpec.RoleSpec(2, 8, null, null, null, "full"), s.router(),
                s.autoscaling(), s.pharosConfig()));
        ClusterConfig f = config(full, Role.READER);
        assertThat(f.getReader().getMode()).isEqualTo("full");
        assertThat(f.getReader().getCacheMaxBytes()).isEqualTo(ConfigRenderer.cacheMaxBytes(Defaults.CACHE_VOLUME));
    }

    @Test
    void routerConfigListsReadersFromTheBucketWithTheToken() {
        ClusterConfig r = config(TestClusters.minio(), Role.ROUTER);
        assertThat(r.getBackend()).isEqualTo(BackendKind.OBJECT_STORE);
        assertThat(r.getTokenEnv()).isEqualTo("PHAROS_TOKEN");
        assertThat(r.getNodeId()).isNull();
        assertThat(r.getAdvertiseUrl()).isNull();
    }

    @Test
    void s3ConfigUsesTheBucketAndOnlyNamesTheKeyEnv() {
        ClusterConfig w = config(TestClusters.s3(), Role.WRITER);
        assertThat(w.getObjectStore().getEndpoint()).isEqualTo("https://s3.eu-north-1.amazonaws.com");
        assertThat(w.getObjectStore().getBucket()).isEqualTo("my-pharos");
        assertThat(w.getObjectStore().getRegion()).isEqualTo("eu-north-1");
        assertThat(w.getObjectStore().isPathStyle()).isFalse();
        assertThat(w.getObjectStore().getAccessKeyEnv()).isEqualTo("PHAROS_S3_ACCESS_KEY");
    }

    @ParameterizedTest
    @EnumSource(Role.class)
    void renderingIsDeterministic(Role role) {
        assertThat(render(TestClusters.minio()).get(role).json()).isEqualTo(render(TestClusters.minio()).get(role).json());
    }

    @Test
    void pharosConfigIsMergedIntoEveryRole() throws Exception {
        Map<String, JsonNode> extra = new LinkedHashMap<>();
        extra.put("drainSeconds", F.numberNode(45));
        extra.put("writer.pollSeconds", F.numberNode(15));
        extra.put("status", F.objectNode().set("targets", F.objectNode().put("backlog", 900)));
        for (Role role : Role.values()) {
            ClusterConfig c = asPharosReadsIt(withPharosConfig(extra), role);
            assertThat(c.getDrainSeconds()).isEqualTo(45);
            assertThat(c.getWriter().getPollSeconds()).isEqualTo(15);
            assertThat(c.getStatus().getTargets().getBacklog()).isEqualTo(900);
            // the operator's keys next to them are untouched
            assertThat(c.getStatus().getTargets().getQpsPerReader()).isEqualTo(50.0);
        }
        assertThat(config(withPharosConfig(extra), Role.WRITER).getWriter().getWorkQueue()).isTrue();
    }

    @Test
    void operatorOwnedKeysInPharosConfigAreRefusedByName() {
        Map<String, JsonNode> extra = new LinkedHashMap<>();
        extra.put("role", F.textNode("all"));
        extra.put("objectStore", F.objectNode().put("prefix", "x"));
        extra.put("writer", F.objectNode().put("workQueue", false).put("pollSeconds", 5));
        extra.put("tokenEnv", F.textNode("OTHER"));
        extra.put("status.targets.qpsPerReader", F.numberNode(5));
        extra.put("status.writers.idleSeconds", F.numberNode(5));
        extra.put("nodeId", F.textNode("x"));
        ConfigRenderer.Result r = render(withPharosConfig(extra));
        assertThat(r.errors()).containsExactly("spec.pharosConfig: keys owned by the operator: nodeId, objectStore.prefix,"
                + " role, status.targets.qpsPerReader, status.writers.idleSeconds, tokenEnv, writer.workQueue");
        assertThat(r.roles()).isEmpty();
    }

    @Test
    void everyOwnedKeyIsRefused() {
        for (String key : List.of("role", "backend", "objectStore.bucket", "token", "tokenEnv", "nodeId", "advertiseUrl",
                "writer.workQueue", "writer.workDir", "status.targets.warmP95Millis", "status.targets.qpsPerReader",
                "status.windows.addReadersSeconds", "status.removeReadersLoad", "status.windows.removeReadersSeconds",
                "status.writers.backlogShards", "status.writers.oldestPendingSeconds",
                "status.windows.addWritersSeconds", "status.writers.idleSeconds", "status.windows.removeWritersSeconds")) {
            assertThat(render(withPharosConfig(Map.of(key, F.textNode("1")))).errors())
                    .as(key).singleElement().asString().startsWith("spec.pharosConfig: keys owned by the operator: " + key);
        }
    }

    @Test
    void unknownPharosConfigKeysAreRefused() {
        ConfigRenderer.Result r = render(withPharosConfig(Map.of("drainSecnds", F.numberNode(5),
                "reader", F.objectNode().put("blockSize", 1))));
        assertThat(r.errors()).containsExactly(
                "spec.pharosConfig: unknown keys (not in pharos's cluster config): drainSecnds, reader.blockSize");
    }

    @Test
    void pharosConfigOfTheWrongTypeNamesTheFieldOnly() {
        ConfigRenderer.Result r = render(withPharosConfig(Map.of("drainSeconds", F.textNode("s3cr3t-looking-value"))));
        assertThat(r.errors()).singleElement().asString()
                .isEqualTo("spec.pharosConfig.drainSeconds: has the wrong type")
                .doesNotContain("s3cr3t");
    }

    @Test
    void mergedConfigIsValidatedWithClusterConfig() {
        ConfigRenderer.Result r = render(withPharosConfig(Map.of("drainSeconds", F.numberNode(99999))));
        assertThat(r.errors()).contains("cluster config (writer): cluster.drainSeconds must be between 0 and 3600, got 99999");
        assertThat(r.roles()).isEmpty();
    }

    @Test
    void emptyPharosConfigChangesNothing() {
        assertThat(render(withPharosConfig(Map.of("peers", F.objectNode()))).get(Role.WRITER).json())
                .isEqualTo(render(TestClusters.minio()).get(Role.WRITER).json());
    }

    /**
     * Integration carry I6: Phase 6's keys pass through spec.pharosConfig: cluster.sharding.* (by default and
     * per project, also a project name with a dot), cluster.reader.previousGenerationGraceSeconds and
     * cluster.writer.reshardSyncTimeoutSeconds, and the config pharos reads back carries them.
     */
    @Test
    void phase6KeysPassThroughPharosConfig() throws Exception {
        Map<String, JsonNode> extra = new LinkedHashMap<>();
        extra.put("sharding.default", F.textNode("[project, module]"));
        extra.put("sharding", F.objectNode().set("projects", F.objectNode()
                .put("lucene", "[project, ring(path, shards=4)]").put("my.lib", "[project, ring(path, shards=2)]")));
        extra.put("reader.previousGenerationGraceSeconds", F.numberNode(45));
        extra.put("writer.reshardSyncTimeoutSeconds", F.numberNode(300));
        ConfigRenderer.Result r = render(withPharosConfig(extra));
        assertThat(r.errors()).isEmpty();
        for (Role role : Role.values()) {
            ClusterConfig c = asPharosReadsIt(withPharosConfig(extra), role);
            assertThat(c.getSharding().defaultSpec().toString()).as(role.id()).isEqualTo("[project, module]");
            assertThat(c.getSharding().specFor("lucene").toString()).isEqualTo("[project, ring(path, shards=4, vnodes=512)]");
            assertThat(c.getSharding().specFor("my.lib").toString()).isEqualTo("[project, ring(path, shards=2, vnodes=512)]");
            assertThat(c.getReader().getPreviousGenerationGraceSeconds()).isEqualTo(45);
            assertThat(c.previousGenerationGrace()).isEqualTo(java.time.Duration.ofSeconds(45));
            assertThat(c.getWriter().getReshardSyncTimeoutSeconds()).isEqualTo(300);
        }
        // an invalid spec is pharos's own validation error
        assertThat(render(withPharosConfig(Map.of("sharding.default", F.textNode("[project, hash(path) % 8]")))).errors())
                .first().asString().startsWith("cluster config (writer): cluster.sharding.default: invalid shard spec");
    }

    /**
     * Integration carry I6: the previous-generation grace defaults to max(30 s, 3 x catalog cache + 2 x
     * heartbeatSeconds). The rendered config leaves it unset, so a pod computes the same default pharos's
     * validation does, for the heartbeat the spec sets; never a value fixed at the operator's default heartbeat.
     */
    @Test
    void previousGenerationGraceDefaultFollowsTheHeartbeat() throws Exception {
        for (Role role : Role.values()) {
            JsonNode file = new ObjectMapper().readTree(render(TestClusters.minio()).get(role).json());
            assertThat(file.path("cluster").path("reader").has("previousGenerationGraceSeconds")).as(role.id()).isFalse();
            ClusterConfig d = asPharosReadsIt(TestClusters.minio(), role);
            assertThat(d.previousGenerationGrace()).isEqualTo(new ClusterConfig().previousGenerationGrace());
            ClusterConfig slow = asPharosReadsIt(withPharosConfig(Map.of("heartbeatSeconds", F.numberNode(20))), role);
            assertThat(slow.previousGenerationGrace()).as("3 x 5 s + 2 x 20 s").isEqualTo(java.time.Duration.ofSeconds(55));
        }
    }
}
