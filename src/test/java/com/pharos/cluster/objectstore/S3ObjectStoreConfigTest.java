package com.pharos.cluster.objectstore;

import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.cluster.BackendKind;
import com.pharos.cluster.NodeRole;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ObjectStoreConfigTest {

    private static ClusterConfig.ObjectStoreConfig cfg(String akEnv, String skEnv) {
        ClusterConfig.ObjectStoreConfig c = new ClusterConfig.ObjectStoreConfig();
        c.setBucket("b");
        c.setEndpoint("http://minio.example:9000");
        c.setAccessKeyEnv(akEnv);
        c.setSecretKeyEnv(skEnv);
        return c;
    }

    @Test
    void namedButUnsetVariableFailsFastNamingIt() {
        assertThatThrownBy(() -> S3ObjectStore.credentialsFor(cfg("MY_AK", "MY_SK"), Map.of("MY_SK", "s")::get))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("MY_AK");
        assertThatThrownBy(() -> S3ObjectStore.credentialsFor(cfg("MY_AK", "MY_SK"),
                Map.of("MY_AK", "a", "MY_SK", "")::get))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("MY_SK");
    }

    @Test
    void onlyOneVariableConfiguredFails() {
        assertThatThrownBy(() -> S3ObjectStore.credentialsFor(cfg("MY_AK", null), Map.of("MY_AK", "a")::get))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("secretKeyEnv");
    }

    @Test
    void setVariablesGiveStaticCredentialsAndNoneGivesDefaultChain() {
        assertThat(S3ObjectStore.credentialsFor(cfg("MY_AK", "MY_SK"), Map.of("MY_AK", "a", "MY_SK", "s")::get))
                .isInstanceOf(StaticCredentialsProvider.class);
        assertThat(S3ObjectStore.credentialsFor(cfg(null, null), Map.<String, String>of()::get))
                .isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void endpointDescriptionNamesEndpoint() {
        assertThat(S3ObjectStore.describeEndpoint(cfg(null, null))).contains("http://minio.example:9000");
        ClusterConfig.ObjectStoreConfig aws = cfg(null, null);
        aws.setEndpoint(null);
        assertThat(S3ObjectStore.describeEndpoint(aws)).contains("default AWS endpoint");
    }

    @Test
    void backendValidatesBeforeBuildingTheClient() {
        IndexConfig config = IndexConfig.defaults();
        ClusterConfig c = config.getCluster();
        c.setRole(NodeRole.WRITER);
        c.setBackend(BackendKind.OBJECT_STORE);
        c.setRetentionMinutes(-1);
        c.getObjectStore().setBucket("b");
        // would fail on credentials if the client were built first
        c.getObjectStore().setAccessKeyEnv("PHAROS_TEST_SURELY_UNSET_AK");
        c.getObjectStore().setSecretKeyEnv("PHAROS_TEST_SURELY_UNSET_SK");
        assertThatThrownBy(() -> new ObjectStoreBackend(config))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("retentionMinutes");
        c.setRetentionMinutes(60);
        assertThatThrownBy(() -> new ObjectStoreBackend(config))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("PHAROS_TEST_SURELY_UNSET_AK");
    }
}
