package com.pharos.indexer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class ProjectDiscoveryTest {

    @TempDir Path workspace;

    // ── isProjectRoot ──────────────────────────────────────────────────────────

    @Test
    void isProjectRoot_maven() throws Exception {
        Files.createFile(workspace.resolve("pom.xml"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_gradle() throws Exception {
        Files.createFile(workspace.resolve("build.gradle"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_python_pyproject() throws Exception {
        Files.createFile(workspace.resolve("pyproject.toml"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_python_setuppy() throws Exception {
        Files.createFile(workspace.resolve("setup.py"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_node() throws Exception {
        Files.createFile(workspace.resolve("package.json"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_rust() throws Exception {
        Files.createFile(workspace.resolve("Cargo.toml"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_go() throws Exception {
        Files.createFile(workspace.resolve("go.mod"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_dotnet_csproj() throws Exception {
        Files.createFile(workspace.resolve("MyApp.csproj"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_dotnet_sln() throws Exception {
        Files.createFile(workspace.resolve("Solution.sln"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_php_composer() throws Exception {
        Files.createFile(workspace.resolve("composer.json"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isTrue();
    }

    @Test
    void isProjectRoot_emptyDir_returnsFalse() {
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isFalse();
    }

    @Test
    void isProjectRoot_onlySourceFiles_returnsFalse() throws Exception {
        Files.createFile(workspace.resolve("Main.java"));
        Files.createFile(workspace.resolve("README.md"));
        assertThat(ProjectDiscovery.isProjectRoot(workspace)).isFalse();
    }

    // ── discover: root IS a project ───────────────────────────────────────────

    @Test
    void discover_rootIsProject_returnsSingleEntry() throws Exception {
        Files.createFile(workspace.resolve("pom.xml"));

        List<ProjectDiscovery.DiscoveredProject> result = ProjectDiscovery.discover(workspace, 3);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).path()).isEqualTo(workspace);
    }

    // ── discover: workspace with multiple sub-projects ────────────────────────

    @Test
    void discover_multipleSubProjects() throws Exception {
        Path a = Files.createDirectory(workspace.resolve("service-a"));
        Path b = Files.createDirectory(workspace.resolve("service-b"));
        Path c = Files.createDirectory(workspace.resolve("service-c"));
        Files.createFile(a.resolve("pom.xml"));
        Files.createFile(b.resolve("build.gradle"));
        Files.createFile(c.resolve("pyproject.toml"));

        List<ProjectDiscovery.DiscoveredProject> result = ProjectDiscovery.discover(workspace, 3);

        assertThat(result).hasSize(3);
        assertThat(result.stream().map(p -> p.path().getFileName().toString()))
                .containsExactlyInAnyOrder("service-a", "service-b", "service-c");
    }

    @Test
    void discover_mixedWorkspace_nonProjectDirsIgnored() throws Exception {
        Path proj = Files.createDirectory(workspace.resolve("my-app"));
        Files.createDirectory(workspace.resolve("docs"));         // no marker
        Files.createDirectory(workspace.resolve("scripts"));      // no marker
        Files.createFile(proj.resolve("Cargo.toml"));

        List<ProjectDiscovery.DiscoveredProject> result = ProjectDiscovery.discover(workspace, 3);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).path()).isEqualTo(proj);
    }

    // ── discover: skip noise directories ──────────────────────────────────────

    @Test
    void discover_skipsNodeModules() throws Exception {
        Path nm = Files.createDirectory(workspace.resolve("node_modules"));
        Path inner = Files.createDirectory(nm.resolve("some-pkg"));
        Files.createFile(inner.resolve("package.json"));  // should be ignored

        Path real = Files.createDirectory(workspace.resolve("my-app"));
        Files.createFile(real.resolve("package.json"));

        List<ProjectDiscovery.DiscoveredProject> result = ProjectDiscovery.discover(workspace, 3);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).path()).isEqualTo(real);
    }

    @Test
    void discover_skipsDotGitDir() throws Exception {
        Path git = Files.createDirectory(workspace.resolve(".git"));
        Path hooks = Files.createDirectory(git.resolve("hooks"));
        Files.createFile(hooks.resolve("pom.xml")); // nonsense but should be skipped

        Path real = Files.createDirectory(workspace.resolve("app"));
        Files.createFile(real.resolve("go.mod"));

        List<ProjectDiscovery.DiscoveredProject> result = ProjectDiscovery.discover(workspace, 3);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).path()).isEqualTo(real);
    }

    @Test
    void discover_skipsTargetAndBuildDirs() throws Exception {
        Path target = Files.createDirectory(workspace.resolve("target"));
        Files.createFile(target.resolve("pom.xml")); // should be ignored

        Path proj = Files.createDirectory(workspace.resolve("my-lib"));
        Files.createFile(proj.resolve("pom.xml"));

        List<ProjectDiscovery.DiscoveredProject> result = ProjectDiscovery.discover(workspace, 3);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).path()).isEqualTo(proj);
    }

    // ── discover: no descend into found projects ───────────────────────────────

    @Test
    void discover_doesNotDescendIntoProjRoot() throws Exception {
        // Maven multi-module: root pom.xml + child pom.xml should yield only root
        Files.createFile(workspace.resolve("pom.xml"));
        Path child = Files.createDirectory(workspace.resolve("module-a"));
        Files.createFile(child.resolve("pom.xml"));

        List<ProjectDiscovery.DiscoveredProject> result = ProjectDiscovery.discover(workspace, 3);

        // Root itself is the project — returned as single entry, not descending
        assertThat(result).hasSize(1);
        assertThat(result.get(0).path()).isEqualTo(workspace);
    }

    // ── discover: depth limit ─────────────────────────────────────────────────

    @Test
    void discover_depthLimitRespected() throws Exception {
        // Project at depth 2 should be found with maxDepth=2 but not maxDepth=1
        Path level1 = Files.createDirectory(workspace.resolve("level1"));
        Path level2 = Files.createDirectory(level1.resolve("level2"));
        Files.createFile(level2.resolve("pom.xml"));

        assertThat(ProjectDiscovery.discover(workspace, 2)).hasSize(1);
        assertThat(ProjectDiscovery.discover(workspace, 1)).isEmpty();
    }

    // ── discover: empty workspace ─────────────────────────────────────────────

    @Test
    void discover_emptyWorkspace_returnsEmpty() throws Exception {
        assertThat(ProjectDiscovery.discover(workspace, 3)).isEmpty();
    }

    // ── deriveProjectName ─────────────────────────────────────────────────────

    @Test
    void deriveProjectName_plain() {
        assertThat(ProjectDiscovery.deriveProjectName(Path.of("/workspace/my-service")))
                .isEqualTo("my-service");
    }

    @Test
    void deriveProjectName_stripsVersionSuffix() {
        assertThat(ProjectDiscovery.deriveProjectName(Path.of("/opt/lucene-9.10.0")))
                .isEqualTo("lucene");
        assertThat(ProjectDiscovery.deriveProjectName(Path.of("/opt/spring-framework-6.1")))
                .isEqualTo("spring-framework");
    }

    @Test
    void deriveProjectName_noVersionSuffix_unchanged() {
        assertThat(ProjectDiscovery.deriveProjectName(Path.of("/repos/my-app")))
                .isEqualTo("my-app");
    }
}
