package com.pharos.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterConfigValidationTest {

    private static final String WRITER = "\"role\":\"writer\",\"backend\":\"object-store\","
            + "\"objectStore\":{\"bucket\":\"b\"}";

    private final ObjectMapper mapper = new ObjectMapper();
    private final Supplier<String> originalResolver = ClusterConfig.hostnameResolver;

    private final java.util.function.UnaryOperator<String> originalEnv = ClusterConfig.envReader;

    @AfterEach
    void restoreResolver() {
        ClusterConfig.hostnameResolver = originalResolver;
        ClusterConfig.envReader = originalEnv;
    }

    private ClusterConfig parse(String clusterJson) throws Exception {
        ClusterConfig c = mapper.readValue("{\"cluster\":{" + clusterJson + "}}", IndexConfig.class).getCluster();
        c.validate();
        return c;
    }

    @Test
    void hostnameIsNotResolvedOnConstructionOrSingleNodeUse() throws Exception {
        AtomicInteger lookups = new AtomicInteger();
        ClusterConfig.hostnameResolver = () -> {
            lookups.incrementAndGet();
            return "host-x";
        };
        ClusterConfig c = parse("\"role\":\"all\"");
        IndexConfig.defaults();
        mapper.writeValueAsString(IndexConfig.defaults());
        assertThat(lookups).hasValue(0);
        assertThat(c.getNodeId()).isNull();

        ClusterConfig writer = parse(WRITER);
        assertThat(writer.effectiveNodeId()).isEqualTo("host-x");
        assertThat(writer.effectiveNodeId()).isEqualTo("host-x");
        assertThat(lookups).as("looked up once, on first use").hasValue(1);
    }

    @Test
    void configuredNodeIdNeverResolvesHostname() throws Exception {
        ClusterConfig.hostnameResolver = () -> {
            throw new AssertionError("must not resolve");
        };
        assertThat(parse(WRITER + ",\"nodeId\":\"node-a\"").effectiveNodeId()).isEqualTo("node-a");
    }

    @Test
    void blankNodeIdRejected() {
        assertThatThrownBy(() -> parse(WRITER + ",\"nodeId\":\"  \""))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.nodeId");
    }

    @Test
    void nonPositivePollIntervalRejected() {
        assertThatThrownBy(() -> parse("\"pollIntervalSeconds\":0"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("pollIntervalSeconds");
        assertThatThrownBy(() -> parse("\"pollIntervalSeconds\":-5"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("pollIntervalSeconds");
    }

    @Test
    void negativeRetentionMinutesRejected() throws Exception {
        assertThatThrownBy(() -> parse("\"retentionMinutes\":-1"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("retentionMinutes");
        assertThat(parse("\"retentionMinutes\":0").getRetentionMinutes()).isZero();
    }

    @Test
    void retentionGenerationsBelowTwoRejected() {
        assertThatThrownBy(() -> parse("\"retentionGenerations\":1"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("retentionGenerations");
    }

    @Test
    void unsafePrefixRejected() throws Exception {
        for (String prefix : new String[]{"/pharos", "a/../b", "..", "a/./b"}) {
            assertThatThrownBy(() -> parse("\"role\":\"writer\",\"backend\":\"object-store\","
                    + "\"objectStore\":{\"bucket\":\"b\",\"prefix\":\"" + prefix + "\"}}"))
                    .as(prefix)
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("cluster.objectStore.prefix");
        }
        assertThat(parse("\"role\":\"writer\",\"backend\":\"object-store\","
                + "\"objectStore\":{\"bucket\":\"b\",\"prefix\":\"team/pharos.v1\"}").getObjectStore().getPrefix())
                .isEqualTo("team/pharos.v1");
    }

    /** A Secret made with {@code kubectl create secret --from-file} keeps the file's trailing newline. */
    @Test
    void tokenEnvValueIsStrippedOfSurroundingWhitespace() throws Exception {
        ClusterConfig.envReader = name -> "PHAROS_TOKEN".equals(name) ? "  s3cret-token\n" : null;
        ClusterConfig c = parse("\"tokenEnv\":\"PHAROS_TOKEN\"");
        assertThat(c.effectiveToken()).isEqualTo("s3cret-token");

        ClusterConfig.envReader = name -> "\r\n\t";
        assertThatThrownBy(() -> parse("\"tokenEnv\":\"PHAROS_TOKEN\""))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("PHAROS_TOKEN")
                .hasMessageContaining("not set (or empty)");
    }

    @Test
    void tokenEnvValueWithInnerWhitespaceOrControlCharactersIsRefused() {
        for (String bad : new String[]{"s3cret token", "s3cret\ntoken", "s3cret\ttoken", "s3cret\u0000token",
                "s3cret\u00a0token", "s3cret\u007ftoken"}) {
            ClusterConfig.envReader = name -> bad;
            assertThatThrownBy(() -> parse("\"tokenEnv\":\"PHAROS_TOKEN\""))
                    .as(bad.replace("\n", "\\n"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("cluster.tokenEnv names PHAROS_TOKEN")
                    .hasMessageContaining("whitespace or control characters")
                    .satisfies(e -> assertThat(e.getMessage()).doesNotContain("s3cret"));
        }
    }
}
