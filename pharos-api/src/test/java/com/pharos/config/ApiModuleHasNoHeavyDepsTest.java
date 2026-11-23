package com.pharos.config;

import com.pharos.cluster.status.ClusterStatus;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The operator depends on {@code pharos-api}; this pins that the module stays light. Its test
 * classpath is its whole resolved dependency tree (plus JUnit and AssertJ), so nothing from
 * Lucene, DJL, ONNX Runtime or Javalin may appear on it, and their classes must not load.
 */
class ApiModuleHasNoHeavyDepsTest {

    /** Maven repository path fragments of the forbidden groups (groupId dots become slashes). */
    private static final List<String> FORBIDDEN_GROUPS = List.of(
            "org/apache/lucene/", "ai/djl/", "com/microsoft/onnxruntime/", "io/javalin/");

    /** One class of each forbidden dependency. */
    private static final List<String> FORBIDDEN_CLASSES = List.of(
            "org.apache.lucene.index.IndexWriter", "ai.djl.Model", "ai.onnxruntime.OrtEnvironment",
            "io.javalin.Javalin");

    private static List<String> testClasspath() {
        // surefire puts the full test classpath here (java.class.path may be a manifest-only booter jar)
        String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        return Arrays.stream(cp.split(File.pathSeparator))
                .map(e -> e.replace(File.separatorChar, '/'))
                .toList();
    }

    @Test
    void dependencyTreeHasNoLuceneOnnxDjlOrJavalin() {
        List<String> cp = testClasspath();
        // not vacuous: the classpath really is the resolved tree (Jackson is the module's one dependency)
        assertThat(cp).anyMatch(e -> e.contains("com/fasterxml/jackson/core/jackson-databind/"));
        for (String group : FORBIDDEN_GROUPS) {
            assertThat(cp).as("pharos-api classpath entries under %s", group).noneMatch(e -> e.contains(group));
        }
        assertThat(cp).as("pharos-api must not see the pharos application").noneMatch(e -> e.endsWith("/target/classes")
                && !e.contains("pharos-api/"));
    }

    @Test
    void heavyClassesDoNotLoad() {
        for (String name : FORBIDDEN_CLASSES) {
            assertThatThrownBy(() -> Class.forName(name, false, getClass().getClassLoader()))
                    .as(name).isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test
    void configAndStatusWorkWithoutTheRuntime() throws Exception {
        new ClusterConfig().validate(); // the single-node default is valid

        String json = """
                {"mode":"cluster","asOf":"2026-11-22T00:00:00Z",
                 "layout":{"summary":{"shards":1,"writers":1,"readers":0},"shards":[],"writers":[],"readers":[]},
                 "backlogSummary":{"shardsPending":0},"health":[],
                 "proposals":[{"id":"x","kind":"add-readers","target":"readers","reason":"r","metrics":{},
                               "action":"start 2 readers","delta":2,"since":"2026-11-22T00:00:00Z","future":1}]}""";
        ClusterStatus s = ClusterStatus.MAPPER.readValue(json.getBytes(StandardCharsets.UTF_8), ClusterStatus.class);
        assertThat(s.proposals()).singleElement().satisfies(p -> {
            assertThat(p.delta()).isEqualTo(2);
            assertThat(p.metrics()).isEqualTo(Map.of());
        });
        assertThat(s.layout().summary().writers()).isEqualTo(1);
    }
}
