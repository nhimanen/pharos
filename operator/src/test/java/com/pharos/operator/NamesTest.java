package com.pharos.operator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NamesTest {

    private static String hash(String raw) {
        return Names.sha1Hex(raw).substring(0, 6);
    }

    @Test
    void k8sNameLowercasesMapsTrimsAndAppendsTheHashOfTheRawName() {
        assertThat(Names.k8sName("lucene")).isEqualTo("lucene-" + hash("lucene"));
        assertThat(Names.k8sName("My_Project.v2")).isEqualTo("my-project-v2-" + hash("My_Project.v2"));
        assertThat(Names.k8sName("_a.")).isEqualTo("a-" + hash("_a."));
        // names that map to the same text still differ
        assertThat(Names.k8sName("a.b")).isNotEqualTo(Names.k8sName("a_b")).startsWith("a-b-");
    }

    @Test
    void k8sNameTruncatesToFitAndKeepsTheHash() {
        String raw = "x".repeat(200);
        assertThat(Names.k8sName(raw)).hasSize(Names.MAX_NAME).endsWith("-" + hash(raw));
        assertThat(Names.k8sName("abc-def", 10)).isEqualTo("abc-" + hash("abc-def")); // no dangling '-'
        assertThat(Names.k8sName("abcdef", 6)).isEqualTo(hash("abcdef"));
        assertThatThrownBy(() -> Names.k8sName("a", 5)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void projectJobNamesFitWithTheLongestClusterName() {
        String cluster = "c".repeat(Names.MAX_CLUSTER_NAME);
        String name = Names.withProject(cluster + "-register-", "Some.Very_Long-project-name", "-1a2b3c");
        assertThat(name).hasSizeLessThanOrEqualTo(Names.MAX_NAME).matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?")
                .startsWith(cluster + "-register-").endsWith("-1a2b3c");
        assertThat(Names.withProject("demo-remove-", "lucene", null)).isEqualTo("demo-remove-lucene-" + hash("lucene"));
    }

    @Test
    void clusterObjectNames() {
        assertThat(Names.token("demo")).isEqualTo("demo-token");
        assertThat(Names.config("demo", "writer")).isEqualTo("demo-config-writer");
        assertThat(Names.headless("demo", "reader")).isEqualTo("demo-readers-hl");
        assertThat(Names.minio("demo")).isEqualTo("demo-minio");
        assertThat(Names.serviceHost("demo-minio", "ns")).isEqualTo("demo-minio.ns.svc");
    }
}
