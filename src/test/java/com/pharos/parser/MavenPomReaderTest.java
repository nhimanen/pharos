package com.pharos.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class MavenPomReaderTest {

    @TempDir
    Path tempDir;

    private final MavenPomReader reader = new MavenPomReader();

    @Test
    void read_simpleCoordinates() throws Exception {
        Path pom = writePom(
                "<project>" +
                "  <groupId>com.example</groupId>" +
                "  <artifactId>my-app</artifactId>" +
                "  <version>1.0.0</version>" +
                "</project>");

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.coordinates().groupId()).isEqualTo("com.example");
        assertThat(info.coordinates().artifactId()).isEqualTo("my-app");
        assertThat(info.coordinates().version()).isEqualTo("1.0.0");
        assertThat(info.coordinates().moduleKey()).isEqualTo("com.example:my-app");
        assertThat(info.coordinates().gav()).isEqualTo("com.example:my-app:1.0.0");
    }

    @Test
    void read_inheritsGroupIdFromParent() throws Exception {
        Path pom = writePom(
                "<project>" +
                "  <parent>" +
                "    <groupId>com.parent</groupId>" +
                "    <version>2.0.0</version>" +
                "  </parent>" +
                "  <artifactId>child-module</artifactId>" +
                "</project>");

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.coordinates().groupId()).isEqualTo("com.parent");
        assertThat(info.coordinates().version()).isEqualTo("2.0.0");
        assertThat(info.coordinates().artifactId()).isEqualTo("child-module");
    }

    @Test
    void read_ownGroupIdTakesPrecedenceOverParent() throws Exception {
        Path pom = writePom(
                "<project>" +
                "  <parent><groupId>com.parent</groupId><version>1.0</version></parent>" +
                "  <groupId>com.own</groupId>" +
                "  <artifactId>my-module</artifactId>" +
                "  <version>2.0</version>" +
                "</project>");

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.coordinates().groupId()).isEqualTo("com.own");
        assertThat(info.coordinates().version()).isEqualTo("2.0");
    }

    @Test
    void read_parsesDependencies_withScopeAndVersion() throws Exception {
        Path pom = writePom(
                "<project>" +
                "  <groupId>com.example</groupId>" +
                "  <artifactId>app</artifactId>" +
                "  <version>1.0</version>" +
                "  <dependencies>" +
                "    <dependency>" +
                "      <groupId>org.junit</groupId>" +
                "      <artifactId>junit-core</artifactId>" +
                "      <version>5.0</version>" +
                "      <scope>test</scope>" +
                "    </dependency>" +
                "    <dependency>" +
                "      <groupId>com.google</groupId>" +
                "      <artifactId>guava</artifactId>" +
                "      <version>33.0</version>" +
                "    </dependency>" +
                "  </dependencies>" +
                "</project>");

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.dependencies()).hasSize(2);

        MavenPomReader.MavenDependency junit = info.dependencies().get(0);
        assertThat(junit.groupId()).isEqualTo("org.junit");
        assertThat(junit.artifactId()).isEqualTo("junit-core");
        assertThat(junit.version()).isEqualTo("5.0");
        assertThat(junit.scope()).isEqualTo("test");
        assertThat(junit.effectiveScope()).isEqualTo("test");
        assertThat(junit.moduleKey()).isEqualTo("org.junit:junit-core");

        MavenPomReader.MavenDependency guava = info.dependencies().get(1);
        assertThat(guava.groupId()).isEqualTo("com.google");
        assertThat(guava.scope()).isNull();
        assertThat(guava.effectiveScope()).isEqualTo("compile"); // null scope → compile
    }

    @Test
    void read_skipsDependencyManagementEntries() throws Exception {
        Path pom = writePom(
                "<project>" +
                "  <groupId>com.example</groupId><artifactId>app</artifactId><version>1.0</version>" +
                "  <dependencyManagement>" +
                "    <dependencies>" +
                "      <dependency>" +
                "        <groupId>com.managed</groupId>" +
                "        <artifactId>managed-lib</artifactId>" +
                "        <version>1.0</version>" +
                "      </dependency>" +
                "    </dependencies>" +
                "  </dependencyManagement>" +
                "  <dependencies>" +
                "    <dependency>" +
                "      <groupId>com.real</groupId>" +
                "      <artifactId>real-lib</artifactId>" +
                "      <version>2.0</version>" +
                "    </dependency>" +
                "  </dependencies>" +
                "</project>");

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.dependencies()).hasSize(1);
        assertThat(info.dependencies().get(0).groupId()).isEqualTo("com.real");
    }

    @Test
    void read_parsesMultiModuleSubmodules() throws Exception {
        Path pom = writePom(
                "<project>" +
                "  <groupId>com.example</groupId><artifactId>parent</artifactId><version>1.0</version>" +
                "  <modules>" +
                "    <module>module-a</module>" +
                "    <module>module-b</module>" +
                "  </modules>" +
                "</project>");

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.isMultiModule()).isTrue();
        assertThat(info.modules()).containsExactly("module-a", "module-b");
    }

    @Test
    void read_noDependencies_returnsEmptyList() throws Exception {
        Path pom = writePom(
                "<project>" +
                "  <groupId>com.example</groupId><artifactId>lib</artifactId><version>1.0</version>" +
                "</project>");

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.dependencies()).isEmpty();
        assertThat(info.isMultiModule()).isFalse();
    }

    @Test
    void findPom_findsExistingPomXml() throws Exception {
        Path pom = tempDir.resolve("pom.xml");
        Files.writeString(pom, "<?xml version=\"1.0\"?><project/>");

        assertThat(MavenPomReader.findPom(tempDir)).contains(pom);
    }

    @Test
    void findPom_returnsEmptyWhenNoPomXml() {
        assertThat(MavenPomReader.findPom(tempDir)).isEmpty();
    }

    @Test
    void read_returnsEmptyOnMalformedXml() throws Exception {
        Path pom = tempDir.resolve("pom.xml");
        Files.writeString(pom, "this is not xml <<invalid>>");

        assertThat(reader.read(pom)).isEmpty();
    }

    @Test
    void read_usesUnknownForMissingCoordinates() throws Exception {
        Path pom = writePom("<project/>");

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.coordinates().groupId()).isEqualTo("unknown");
        assertThat(info.coordinates().artifactId()).isEqualTo("unknown");
        assertThat(info.coordinates().version()).isEqualTo("unknown");
    }

    @Test
    void read_realSampleAppPom() throws Exception {
        // Test against the actual sample-app pom.xml in test resources
        Path projectRoot = Path.of(
                getClass().getClassLoader().getResource("test-projects/sample-app").toURI());
        Path pom = MavenPomReader.findPom(projectRoot).orElseThrow();

        MavenPomReader.PomInfo info = reader.read(pom).orElseThrow();

        assertThat(info.coordinates().groupId()).isEqualTo("com.example");
        assertThat(info.coordinates().artifactId()).isEqualTo("sample-app");
        assertThat(info.coordinates().version()).isEqualTo("1.0.0");
        // Should find commons-lang3 and slf4j-api as compile deps, junit as test dep
        assertThat(info.dependencies()).anyMatch(d -> "commons-lang3".equals(d.artifactId()));
        assertThat(info.dependencies()).anyMatch(d ->
                "junit-jupiter".equals(d.artifactId()) && "test".equals(d.scope()));
    }

    // --- helper ---

    private Path writePom(String body) throws Exception {
        Path pom = tempDir.resolve("pom.xml");
        Files.writeString(pom, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + body);
        return pom;
    }
}
