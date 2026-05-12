package com.pharos.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class PyprojectReaderTest {

    @TempDir Path tempDir;
    private final PyprojectReader reader = new PyprojectReader();

    // ── isPythonProject ───────────────────────────────────────────────────────

    @Test
    void isPythonProject_detectsPyprojectToml() throws Exception {
        Files.writeString(tempDir.resolve("pyproject.toml"), "[project]\nname = \"app\"\n");
        assertThat(PyprojectReader.isPythonProject(tempDir)).isTrue();
    }

    @Test
    void isPythonProject_detectsSetupPy() throws Exception {
        Files.writeString(tempDir.resolve("setup.py"), "from setuptools import setup");
        assertThat(PyprojectReader.isPythonProject(tempDir)).isTrue();
    }

    @Test
    void isPythonProject_falseForEmpty() {
        assertThat(PyprojectReader.isPythonProject(tempDir)).isFalse();
    }

    // ── pyproject.toml parsing ────────────────────────────────────────────────

    @Test
    void read_extractsNameAndVersion() throws Exception {
        Files.writeString(tempDir.resolve("pyproject.toml"),
                "[project]\n" +
                "name = \"greedy-wand\"\n" +
                "version = \"0.1.0\"\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId()).isEqualTo("greedy_wand"); // hyphens → underscores
        assertThat(info.coordinates().version()).isEqualTo("0.1.0");
        assertThat(info.coordinates().groupId()).isEqualTo("python");
    }

    @Test
    void read_extractsDependencies() throws Exception {
        Files.writeString(tempDir.resolve("pyproject.toml"),
                "[project]\n" +
                "name = \"myapp\"\n" +
                "version = \"1.0\"\n" +
                "dependencies = [\n" +
                "    \"fastapi>=0.115\",\n" +
                "    \"uvicorn[standard]>=0.30\",\n" +
                "    \"httpx>=0.27\",\n" +
                "]\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::artifactId)
                .containsExactlyInAnyOrder("fastapi", "uvicorn", "httpx");
    }

    @Test
    void read_moduleKeyIsGroupColonArtifact() throws Exception {
        Files.writeString(tempDir.resolve("pyproject.toml"),
                "[project]\nname = \"my-tool\"\nversion = \"2.0\"\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().moduleKey()).isEqualTo("python:my_tool");
    }

    @Test
    void read_unknownVersionWhenMissing() throws Exception {
        Files.writeString(tempDir.resolve("pyproject.toml"), "[project]\nname = \"app\"\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().version()).isEqualTo("unknown");
    }

    // ── setup.py fallback ─────────────────────────────────────────────────────

    @Test
    void read_fallsBackToSetupPy() throws Exception {
        Files.writeString(tempDir.resolve("setup.py"),
                "from setuptools import setup\n" +
                "setup(name='my-pkg', version='3.0')\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId()).isEqualTo("my_pkg");
        assertThat(info.coordinates().version()).isEqualTo("3.0");
    }

    // ── Empty ─────────────────────────────────────────────────────────────────

    @Test
    void read_returnsEmptyForNonPythonDir() {
        assertThat(reader.read(tempDir)).isEmpty();
    }
}
