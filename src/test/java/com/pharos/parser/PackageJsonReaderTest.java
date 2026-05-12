package com.pharos.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class PackageJsonReaderTest {

    @TempDir Path tempDir;
    private final PackageJsonReader reader = new PackageJsonReader();

    @Test
    void isNodeProject_detectsPackageJson() throws Exception {
        Files.writeString(tempDir.resolve("package.json"), "{\"name\":\"app\"}");
        assertThat(PackageJsonReader.isNodeProject(tempDir)).isTrue();
    }

    @Test
    void isNodeProject_falseForEmptyDir() {
        assertThat(PackageJsonReader.isNodeProject(tempDir)).isFalse();
    }

    @Test
    void read_extractsNameAndVersion() throws Exception {
        Files.writeString(tempDir.resolve("package.json"),
                "{\"name\":\"my-app\",\"version\":\"1.2.3\"}");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId()).isEqualTo("my-app");
        assertThat(info.coordinates().version()).isEqualTo("1.2.3");
        assertThat(info.coordinates().groupId()).isEqualTo("npm");
        assertThat(info.coordinates().moduleKey()).isEqualTo("npm:my-app");
    }

    @Test
    void read_handlesScopedPackageName() throws Exception {
        Files.writeString(tempDir.resolve("package.json"),
                "{\"name\":\"@myorg/my-lib\",\"version\":\"2.0.0\"}");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().groupId()).isEqualTo("myorg");
        assertThat(info.coordinates().artifactId()).isEqualTo("my-lib");
        assertThat(info.coordinates().moduleKey()).isEqualTo("myorg:my-lib");
    }

    @Test
    void read_extractsDependencies() throws Exception {
        Files.writeString(tempDir.resolve("package.json"),
                "{\"name\":\"app\",\"version\":\"1.0\"," +
                "\"dependencies\":{\"react\":\"^18\",\"axios\":\"^1.6\"}}");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::artifactId)
                .containsExactlyInAnyOrder("react", "axios");
    }

    @Test
    void read_unknownVersionWhenMissing() throws Exception {
        Files.writeString(tempDir.resolve("package.json"), "{\"name\":\"app\"}");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().version()).isEqualTo("unknown");
    }

    @Test
    void read_fallsBackToDirNameWhenNoName() throws Exception {
        Files.writeString(tempDir.resolve("package.json"), "{}");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().artifactId())
                .isEqualTo(tempDir.getFileName().toString());
    }

    @Test
    void read_returnsEmptyForNonNodeDir() {
        assertThat(reader.read(tempDir)).isEmpty();
    }
}
