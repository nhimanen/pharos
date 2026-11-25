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
    void projectObjectNames() {
        assertThat(Names.registerJob("demo", "lucene", "1a2b3c")).isEqualTo("demo-register-lucene-" + hash("lucene") + "-1a2b3c");
        assertThat(Names.removeJob("demo", "lucene")).isEqualTo("demo-remove-lucene-" + hash("lucene"));
        assertThat(Names.gitCredentialsVolume("My.Repo")).isEqualTo("git-my-repo-" + hash("My.Repo"));
        String cluster = "c".repeat(Names.MAX_CLUSTER_NAME);
        String project = "Some.Very_Long-project-name-that-goes-on";
        for (String n : java.util.List.of(Names.registerJob(cluster, project, "1a2b3c"), Names.removeJob(cluster, project),
                Names.gitCredentialsVolume(project.repeat(3)))) {
            assertThat(n).hasSizeLessThanOrEqualTo(Names.MAX_NAME).matches("[a-z0-9]([-a-z0-9]*[a-z0-9])?");
        }
    }

    @Test
    void preflightJobNames() {
        assertThat(Names.preflightJob("demo", "1.7.0")).isEqualTo("demo-preflight-1.7.0");
        assertThat(Names.preflightJob("demo", "1.7.0-rc.1")).isEqualTo("demo-preflight-1.7.0-rc.1");
        // a tag that is no valid name part, or too long, still gets its own name
        assertThat(Names.preflightJob("demo", "1.7.0_RC1")).isEqualTo("demo-preflight-1-7-0-rc1-" + hash("1.7.0_RC1"));
        String cluster = "c".repeat(Names.MAX_CLUSTER_NAME);
        for (String v : java.util.List.of("1.7.0", "1.10.10-rc.10", "x".repeat(128), "A_".repeat(60))) {
            assertThat(Names.preflightJob(cluster, v)).hasSizeLessThanOrEqualTo(Names.MAX_NAME)
                    .matches("[a-z0-9]([-a-z0-9.]*[a-z0-9])?");
        }
        assertThat(Names.preflightJob(cluster, "x".repeat(128))).isNotEqualTo(Names.preflightJob(cluster, "x".repeat(127)));
        assertThat(Names.preflightPostJob("demo", "1.6.0")).isEqualTo("demo-preflight-1.6.0-post");
        for (String v : java.util.List.of("1.7.0", "1.10.10-rc.10", "x".repeat(128), "A_".repeat(60))) {
            assertThat(Names.preflightPostJob(cluster, v)).hasSizeLessThanOrEqualTo(Names.MAX_NAME).endsWith("-post")
                    .matches("[a-z0-9]([-a-z0-9.]*[a-z0-9])?").isNotEqualTo(Names.preflightJob(cluster, v));
        }
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
