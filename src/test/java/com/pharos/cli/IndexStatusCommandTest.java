package com.pharos.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IndexVersions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class IndexStatusCommandTest {

    @TempDir Path tmp;

    private final PrintStream origOut = System.out;
    private ByteArrayOutputStream out;
    private ProjectRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out, true));
        registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        addProject("a", true);
        addProject("b", false);
    }

    @AfterEach
    void restore() { System.setOut(origOut); }

    private void addProject(String name, boolean stamped) throws Exception {
        Path root = Files.createDirectories(tmp.resolve("src-" + name));
        Path idx = Files.createDirectories(tmp.resolve("indexes").resolve(name));
        Files.writeString(idx.resolve("segments_1"), "x".repeat(100));
        if (stamped) IndexFormat.write(idx);
        registry.register(new ProjectMeta(name, root.toString(), idx.toString()));
    }

    @Test
    void jsonListsStatusPerProject() throws Exception {
        int rc = new CommandLine(new IndexStatusCommand(registry)).execute("--json");
        assertThat(rc).isZero();
        JsonNode j = new ObjectMapper().readTree(out.toString());
        assertThat(j.get("requiredFormat").asInt()).isEqualTo(IndexVersions.INDEX_FORMAT);
        assertThat(j.get("minReadableFormat").asInt()).isEqualTo(IndexFormat.MIN_READABLE_FORMAT);
        assertThat(j.get("pharosVersion").asText()).isNotBlank();
        Map<String, JsonNode> byName = new TreeMap<>();
        j.get("projects").forEach(p -> byName.put(p.get("name").asText(), p));
        assertThat(byName.get("a").get("status").asText()).isEqualTo("ok");
        assertThat(byName.get("b").get("status").asText()).isEqualTo("upgrade-available");
        assertThat(byName.get("b").get("format").asInt()).isZero();
        assertThat(byName.get("a").get("indexBytes").asLong()).isGreaterThan(0);
        assertThat(byName.get("a").get("rootExists").asBoolean()).isTrue();
        for (String k : new String[]{"pharosVersion", "rootPath", "lastIndexed"}) {
            assertThat(byName.get("a").has(k)).isTrue();
        }
    }

    @Test
    void tableOutputMentionsProjects() {
        assertThat(new CommandLine(new IndexStatusCommand(registry)).execute()).isZero();
        assertThat(out.toString()).contains("a", "b", "upgrade-available");
    }

    @Test
    void entryWithoutIndexPathIsMissingNotAnError() throws Exception {
        registry.register(new ProjectMeta("nopath", tmp.toString(), null));
        assertThat(new CommandLine(new IndexStatusCommand(registry)).execute("--json")).isZero();
        JsonNode j = new ObjectMapper().readTree(out.toString());
        Map<String, JsonNode> byName = new TreeMap<>();
        j.get("projects").forEach(p -> byName.put(p.get("name").asText(), p));
        assertThat(byName.get("nopath").get("status").asText()).isEqualTo("missing");
        assertThat(byName.get("nopath").get("indexBytes").asLong()).isZero();
        assertThat(new CommandLine(new IndexStatusCommand(registry)).execute()).isZero();
        assertThat(new CommandLine(new ProjectsCommand(registry)).execute()).isZero();
        assertThat(out.toString()).contains("nopath");
    }

    @Test
    void writesNothing() throws Exception {
        Map<String, Long> before = snapshot();
        new CommandLine(new IndexStatusCommand(registry)).execute("--json");
        new CommandLine(new IndexStatusCommand(registry)).execute();
        assertThat(snapshot()).isEqualTo(before);
    }

    private Map<String, Long> snapshot() throws Exception {
        Map<String, Long> m = new TreeMap<>();
        try (Stream<Path> s = Files.walk(tmp)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                m.put(p.toString(), Files.getLastModifiedTime(p).toMillis());
            }
        }
        return m;
    }

    @Test
    void invalidNameReportedAsInvalidWithoutCrashing() throws Exception {
        registry.register(new ProjectMeta(".", tmp.toString(), tmp.resolve("indexes").toString()));
        var buf = new java.io.ByteArrayOutputStream();
        var old = System.out;
        System.setOut(new java.io.PrintStream(buf, true));
        int rc;
        try { rc = new picocli.CommandLine(new IndexStatusCommand(registry)).execute("--json"); }
        finally { System.setOut(old); }
        assertThat(rc).isEqualTo(0);
        assertThat(buf.toString()).contains("\"name\" : \".\"").contains("\"status\" : \"invalid\"")
                .contains("\"indexBytes\" : 0");
    }

    @Test
    void projectsCommandHandlesInvalidEntry() throws Exception {
        registry.register(new ProjectMeta(".", tmp.toString(), tmp.resolve("indexes").toString()));
        var buf = new java.io.ByteArrayOutputStream();
        var old = System.out;
        System.setOut(new java.io.PrintStream(buf, true));
        try { assertThat(new picocli.CommandLine(new ProjectsCommand(registry)).execute()).isZero(); }
        finally { System.setOut(old); }
        assertThat(buf.toString()).contains("invalid name");
    }

    @Test
    void jsonReportsTheNodeRole() throws Exception {
        assertThat(new CommandLine(new IndexStatusCommand(registry)).execute("--json")).isZero();
        assertThat(new ObjectMapper().readTree(out.toString()).get("role").asText()).isEqualTo("all");
        out.reset();
        assertThat(new CommandLine(new IndexStatusCommand(registry, com.pharos.cluster.NodeRole.READER))
                .execute("--json")).isZero();
        assertThat(new ObjectMapper().readTree(out.toString()).get("role").asText()).isEqualTo("reader");
    }
}
