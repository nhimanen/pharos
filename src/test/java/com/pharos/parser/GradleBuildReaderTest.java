package com.pharos.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class GradleBuildReaderTest {

    @TempDir Path tempDir;
    private final GradleBuildReader reader = new GradleBuildReader();

    // ── isGradleProject ──────────────────────────────────────────────────────

    @Test
    void isGradleProject_detectsSettingsGradle() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle"), "rootProject.name = 'myapp'");
        assertThat(GradleBuildReader.isGradleProject(tempDir)).isTrue();
    }

    @Test
    void isGradleProject_detectsBuildGradle() throws Exception {
        Files.writeString(tempDir.resolve("build.gradle"), "apply plugin: 'java'");
        assertThat(GradleBuildReader.isGradleProject(tempDir)).isTrue();
    }

    @Test
    void isGradleProject_falseForPlainDir() {
        assertThat(GradleBuildReader.isGradleProject(tempDir)).isFalse();
    }

    // ── Name extraction ───────────────────────────────────────────────────────

    @Test
    void read_extractsNameFromSettingsGradle() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle"),
                "rootProject.name = \"lucene-root\"");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId()).isEqualTo("lucene-root");
    }

    @Test
    void read_extractsNameWithSingleQuotes() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle"),
                "rootProject.name = 'solr-root'");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId()).isEqualTo("solr-root");
    }

    @Test
    void read_fallsBackToDirNameWhenNoSettingsGradle() throws Exception {
        Files.writeString(tempDir.resolve("build.gradle"), "apply plugin: 'java'");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId())
                .isEqualTo(tempDir.getFileName().toString());
    }

    // ── Group extraction ──────────────────────────────────────────────────────

    @Test
    void read_extractsGroupFromGradleProperties() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle"), "rootProject.name = 'myapp'");
        Files.writeString(tempDir.resolve("gradle.properties"), "group=org.example\nversion=2.0\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().groupId()).isEqualTo("org.example");
        assertThat(info.coordinates().version()).isEqualTo("2.0");
        assertThat(info.coordinates().moduleKey()).isEqualTo("org.example:myapp");
    }

    @Test
    void read_extractsGroupFromBuildGradle() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle"), "rootProject.name = 'myapp'");
        Files.writeString(tempDir.resolve("build.gradle"), "group = 'com.example'\nversion = '1.5'\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().groupId()).isEqualTo("com.example");
        assertThat(info.coordinates().version()).isEqualTo("1.5");
    }

    @Test
    void read_usesNameAsGroupWhenNoGroupDeclared() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle"), "rootProject.name = 'myapp'");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().groupId()).isEqualTo("myapp");
    }

    // ── Dependency extraction ─────────────────────────────────────────────────

    @Test
    void read_extractsDependenciesFromBuildGradle() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle"), "rootProject.name = 'app'");
        Files.writeString(tempDir.resolve("build.gradle"),
                "dependencies {\n" +
                "    implementation 'org.apache.lucene:lucene-core:9.0.0'\n" +
                "    api 'com.google.guava:guava:32.0.0-jre'\n" +
                "}\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::moduleKey)
                .containsExactlyInAnyOrder("org.apache.lucene:lucene-core", "com.google.guava:guava");
    }

    // ── Empty / not-found ─────────────────────────────────────────────────────

    @Test
    void read_returnsEmptyForNonGradleDir() {
        assertThat(reader.read(tempDir)).isEmpty();
    }

    @Test
    void read_moduleKeyExcludesVersion() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle"), "rootProject.name = 'myapp'");
        Files.writeString(tempDir.resolve("gradle.properties"), "group=org.example\nversion=3.0\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().moduleKey()).isEqualTo("org.example:myapp");
        assertThat(info.coordinates().version()).isEqualTo("3.0");
    }
}
