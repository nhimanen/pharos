package com.pharos.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class CMakeReaderTest {

    @TempDir Path tempDir;
    private final CMakeReader reader = new CMakeReader();

    @Test
    void isCMakeProject_detectsCMakeLists() throws Exception {
        Files.writeString(tempDir.resolve("CMakeLists.txt"), "cmake_minimum_required(VERSION 3.20)\nproject(mylib CXX)\n");
        assertThat(CMakeReader.isCMakeProject(tempDir)).isTrue();
    }

    @Test
    void isCMakeProject_falseForEmptyDir() {
        assertThat(CMakeReader.isCMakeProject(tempDir)).isFalse();
    }

    @Test
    void read_extractsProjectName() throws Exception {
        Files.writeString(tempDir.resolve("CMakeLists.txt"),
                "cmake_minimum_required(VERSION 3.20)\nproject(vespa CXX C)\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId()).isEqualTo("vespa");
        assertThat(info.coordinates().groupId()).isEqualTo("cmake");
    }

    @Test
    void read_extractsVersionFromVersionClause() throws Exception {
        Files.writeString(tempDir.resolve("CMakeLists.txt"),
                "project(mylib VERSION 1.2.3 CXX)\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().version()).isEqualTo("1.2.3");
        assertThat(info.coordinates().artifactId()).isEqualTo("mylib");
    }

    @Test
    void read_unknownVersionWhenAbsent() throws Exception {
        Files.writeString(tempDir.resolve("CMakeLists.txt"), "project(foo CXX)\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().version()).isEqualTo("unknown");
    }

    @Test
    void read_projectNameIsLowercased() throws Exception {
        Files.writeString(tempDir.resolve("CMakeLists.txt"), "project(MyLib CXX)\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId()).isEqualTo("mylib");
    }

    @Test
    void read_moduleKeyIsCmakeColonName() throws Exception {
        Files.writeString(tempDir.resolve("CMakeLists.txt"), "project(vespa CXX C)\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().moduleKey()).isEqualTo("cmake:vespa");
    }

    @Test
    void read_returnsEmptyWhenNoCMakeListsTxt() {
        assertThat(reader.read(tempDir)).isEmpty();
    }

    @Test
    void read_returnsEmptyWhenNoProjectCommand() throws Exception {
        Files.writeString(tempDir.resolve("CMakeLists.txt"), "cmake_minimum_required(VERSION 3.20)\n");
        assertThat(reader.read(tempDir)).isEmpty();
    }
}
