package com.pharos.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class GoModReaderTest {

    private static final Path SAMPLE_APP = sampleAppPath();

    @Test
    void isGoProject_detectsGoMod() {
        assertThat(GoModReader.isGoProject(SAMPLE_APP)).isTrue();
    }

    @Test
    void isGoProject_falseWithoutMarker(@TempDir Path dir) {
        assertThat(GoModReader.isGoProject(dir)).isFalse();
    }

    @Test
    void isGoProject_detectsGoWork(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("go.work"), "go 1.22\n\nuse ./svc\n");
        assertThat(GoModReader.isGoProject(dir)).isTrue();
    }

    @Test
    void modulePathBecomesTheArtifactIdUnderTheGoNamespace() {
        var coords = read(SAMPLE_APP).coordinates();
        assertThat(coords.groupId()).isEqualTo("go");
        assertThat(coords.artifactId()).isEqualTo("github.com/acme/sample");
        assertThat(coords.moduleKey()).isEqualTo("go:github.com/acme/sample");
    }

    @Test
    void goDirectiveIsNotUsedAsTheModuleVersion() {
        // Go module versions live in VCS tags, not in go.mod; `go 1.22` is a language version.
        assertThat(read(SAMPLE_APP).coordinates().version()).isEqualTo("unknown");
    }

    @Test
    void blockAndSingleLineRequiresAreBothCollected() {
        assertThat(read(SAMPLE_APP).dependencies())
                .extracting(d -> d.artifactId() + "@" + d.version())
                .containsExactlyInAnyOrder(
                        "github.com/google/uuid@v1.6.0",
                        "golang.org/x/sync@v0.7.0",
                        "github.com/pkg/errors@v0.9.1");
    }

    @Test
    void indirectRequirementsGetRuntimeScope() {
        assertThat(read(SAMPLE_APP).dependencies())
                .filteredOn(d -> d.artifactId().equals("golang.org/x/sync"))
                .singleElement()
                .extracting(d -> d.effectiveScope()).isEqualTo("runtime");
    }

    @Test
    void directRequirementsGetCompileScope() {
        assertThat(read(SAMPLE_APP).dependencies())
                .filteredOn(d -> d.artifactId().equals("github.com/google/uuid"))
                .singleElement()
                .extracting(d -> d.effectiveScope()).isEqualTo("compile");
    }

    @Test
    void localReplaceTargetsAreRecordedAsModules() {
        assertThat(read(SAMPLE_APP).modules()).containsExactly("./shared");
    }

    @Test
    void remoteReplaceTargetsAreNotRecordedAsModules(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("go.mod"), """
                module example.com/x

                go 1.22

                replace example.com/a => example.com/b v1.2.3
                replace example.com/c => ../c
                """);
        assertThat(read(dir).modules()).containsExactly("../c");
    }

    @Test
    void requiresInsideCommentsAndExcludeBlocksAreIgnored(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("go.mod"), """
                module example.com/x

                go 1.22

                require (
                	example.com/keep v1.0.0
                	// example.com/commented v9.9.9
                )

                exclude (
                	example.com/excluded v0.0.1
                )
                """);
        assertThat(read(dir).dependencies())
                .extracting(d -> d.artifactId()).containsExactly("example.com/keep");
    }

    @Test
    void goWorkUseEntriesWidenTheModuleList(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("go.mod"), "module example.com/root\n\ngo 1.22\n");
        Files.writeString(dir.resolve("go.work"), """
                go 1.22

                use (
                	./svc/api
                	./svc/worker
                )
                """);
        assertThat(read(dir).modules()).containsExactly("./svc/api", "./svc/worker");
    }

    @Test
    void workspaceWithoutGoModUsesTheDirectoryNameAsCoordinates(@TempDir Path dir) throws IOException {
        Path root = dir.resolve("monorepo");
        Files.createDirectories(root);
        Files.writeString(root.resolve("go.work"), "go 1.22\n\nuse ./svc\n");

        var info = read(root);
        assertThat(info.coordinates().artifactId()).isEqualTo("monorepo");
        assertThat(info.modules()).containsExactly("./svc");
    }

    @Test
    void missingGoModReturnsEmpty(@TempDir Path dir) {
        assertThat(new GoModReader().read(dir)).isEmpty();
    }

    @Test
    void goModWithoutModuleDirectiveReturnsEmpty(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("go.mod"), "go 1.22\n");
        assertThat(new GoModReader().read(dir)).isEmpty();
    }

    @Test
    void findModuleRoot_walksUpFromANestedPackage() {
        Optional<Path> root = GoModReader.findModuleRoot(SAMPLE_APP.resolve("service"));
        assertThat(root).contains(SAMPLE_APP);
    }

    @Test
    void findModuleRoot_emptyWhenNoModuleAbove(@TempDir Path dir) throws IOException {
        Path nested = dir.resolve("a/b");
        Files.createDirectories(nested);
        assertThat(GoModReader.findModuleRoot(nested)).isEmpty();
    }

    @Test
    void readModulePath_readsTheModuleDirective() {
        assertThat(GoModReader.readModulePath(SAMPLE_APP.resolve("go.mod")))
                .contains("github.com/acme/sample");
    }

    private static MavenPomReader.PomInfo read(Path dir) {
        Optional<MavenPomReader.PomInfo> info = new GoModReader().read(dir);
        assertThat(info).as("go.mod at %s", dir).isPresent();
        return info.get();
    }

    private static Path sampleAppPath() {
        try {
            var url = GoModReaderTest.class.getClassLoader()
                    .getResource("test-projects/sample-go-app");
            if (url == null) throw new IllegalStateException("sample-go-app fixture not on classpath");
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
