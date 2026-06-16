package com.pharos.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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

    // ── Version catalog parsing (gradle/libs.versions.toml) ───────────────────

    @Test
    void parseVersionCatalog_inlineStringForm() throws Exception {
        writeCatalog("[libraries]\nyeast = \"com.example:acme:1.0.0\"\n");

        Map<String, String> catalog = reader.parseVersionCatalog(tempDir);
        assertThat(catalog).containsEntry("yeast", "com.example:acme:1.0.0");
    }

    @Test
    void parseVersionCatalog_inlineTableWithLiteralVersion() throws Exception {
        writeCatalog("[libraries]\n" +
                "spring-boot = { module = \"org.springframework.boot:spring-boot-starter\", version = \"3.2.0\" }\n");

        Map<String, String> catalog = reader.parseVersionCatalog(tempDir);
        assertThat(catalog).containsEntry(
                "spring-boot",
                "org.springframework.boot:spring-boot-starter:3.2.0");
    }

    @Test
    void parseVersionCatalog_inlineTableWithVersionRef() throws Exception {
        writeCatalog("[versions]\njackson = \"2.21.2\"\n\n" +
                "[libraries]\n" +
                "jackson-bom = { module = \"com.fasterxml.jackson:jackson-bom\", version.ref = \"jackson\" }\n");

        Map<String, String> catalog = reader.parseVersionCatalog(tempDir);
        assertThat(catalog).containsEntry(
                "jackson-bom",
                "com.fasterxml.jackson:jackson-bom:2.21.2");
    }

    @Test
    void parseVersionCatalog_versionRefMissing_leavesVersionUnknown() throws Exception {
        // version.ref points to a [versions] entry that doesn't exist — the alias
        // should still appear so the module-graph edge can be recorded.
        writeCatalog("[libraries]\norphan = { module = \"g:a\", version.ref = \"nope\" }\n");

        Map<String, String> catalog = reader.parseVersionCatalog(tempDir);
        assertThat(catalog).containsEntry("orphan", "g:a:unknown");
    }

    @Test
    void parseVersionCatalog_commentsAndBlankLinesIgnored() throws Exception {
        writeCatalog("# top-level comment\n" +
                "[versions]\n" +
                "# version comment\n" +
                "spring = \"6.2.0\"\n\n" +
                "[libraries]\n" +
                "# library comment\n" +
                "spring-core = { module = \"org.springframework:spring-core\", version.ref = \"spring\" }\n");

        Map<String, String> catalog = reader.parseVersionCatalog(tempDir);
        assertThat(catalog).containsEntry("spring-core", "org.springframework:spring-core:6.2.0");
    }

    @Test
    void parseVersionCatalog_missingFile_returnsEmpty() {
        // No gradle/libs.versions.toml under tempDir
        assertThat(reader.parseVersionCatalog(tempDir)).isEmpty();
    }

    @Test
    void parseVersionCatalog_ignoresOtherSections() throws Exception {
        // Only [libraries] is parsed; [plugins] and [bundles] are skipped.
        writeCatalog("[plugins]\n" +
                "kotlin-jvm = { id = \"org.jetbrains.kotlin.jvm\", version = \"1.9.0\" }\n\n" +
                "[bundles]\n" +
                "test = [\"junit\", \"mockito\"]\n\n" +
                "[libraries]\n" +
                "yeast = \"com.example:acme:1.0.0\"\n");

        Map<String, String> catalog = reader.parseVersionCatalog(tempDir);
        assertThat(catalog).containsOnlyKeys("yeast");
    }

    // ── read() — catalog & project refs end-to-end ────────────────────────────

    @Test
    void read_resolvesCatalogReferencesIntoDependencies() throws Exception {
        writeCatalog("[versions]\njackson = \"2.18.0\"\n\n" +
                "[libraries]\n" +
                "yeast       = \"com.example:acme:1.0.0\"\n" +
                "jackson-bom = { module = \"com.fasterxml.jackson:jackson-bom\", version.ref = \"jackson\" }\n");
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"my-app\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"),
                "group = \"com.example\"\nversion = \"1.0.0\"\n\n" +
                "dependencies {\n" +
                "    implementation(libs.yeast)\n" +
                "    implementation(libs.jackson.bom)\n" +
                "}\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.coordinates().moduleKey()).isEqualTo("com.example:my-app");
        assertThat(info.dependencies())
                .extracting(d -> d.groupId() + ":" + d.artifactId() + ":" + d.version())
                .containsExactlyInAnyOrder(
                        "com.example:acme:1.0.0",
                        "com.fasterxml.jackson:jackson-bom:2.18.0");
    }

    @Test
    void read_recognizesProjectRefsAsLocalDeps() throws Exception {
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"my-app\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"),
                "group = \"com.example\"\nversion = \"1.0.0\"\n\n" +
                "dependencies {\n" +
                "    implementation(project(\":backend\"))\n" +
                "    implementation(project(\":frontend\"))\n" +
                "}\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::artifactId)
                .containsExactlyInAnyOrder("backend", "frontend");
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::version)
                .containsOnly("LOCAL");
    }

    @Test
    void read_mixedStyles_allExtracted() throws Exception {
        // Literal coordinates + catalog ref + project ref — all in one build.gradle.kts.
        writeCatalog("[libraries]\nyeast = \"com.example:acme:1.0.0\"\n");
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"my-app\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"),
                "group = \"com.example\"\nversion = \"1.0.0\"\n\n" +
                "dependencies {\n" +
                "    implementation(\"org.apache.commons:commons-lang3:3.14.0\")\n" +
                "    implementation(libs.yeast)\n" +
                "    implementation(project(\":sub\"))\n" +
                "}\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::artifactId)
                .containsExactlyInAnyOrder("commons-lang3", "yeast", "sub");
    }

    @Test
    void read_catalogRefWithoutCatalogFile_isIgnoredQuietly() throws Exception {
        // libs.foo reference but no gradle/libs.versions.toml — the catalog ref is
        // silently dropped and literal coords still come through.
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"my-app\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"),
                "group = \"com.example\"\nversion = \"1.0.0\"\n\n" +
                "dependencies {\n" +
                "    implementation(libs.missing.lib)\n" +
                "    implementation(\"g:a:1.0\")\n" +
                "}\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::artifactId)
                .containsExactly("a");
    }

    // ── parseBundles ──────────────────────────────────────────────────────────

    @Test
    void parseBundles_inlineArrayForm() throws Exception {
        writeCatalog("[bundles]\ntest = [\"junit\", \"mockito\"]\n");

        Map<String, List<String>> bundles = reader.parseBundles(tempDir);
        assertThat(bundles).containsKey("test");
        assertThat(bundles.get("test")).containsExactly("junit", "mockito");
    }

    @Test
    void parseBundles_multiLineForm() throws Exception {
        // Real-world catalogs format bundles across many lines.
        writeCatalog("[bundles]\n" +
                "core = [\n" +
                "    \"spring-boot\",\n" +
                "    \"yeast\",\n" +
                "    \"jackson-bom\",\n" +
                "]\n");

        Map<String, List<String>> bundles = reader.parseBundles(tempDir);
        assertThat(bundles).containsKey("core");
        assertThat(bundles.get("core")).containsExactly("spring-boot", "yeast", "jackson-bom");
    }

    @Test
    void parseBundles_missingFile_returnsEmpty() {
        assertThat(reader.parseBundles(tempDir)).isEmpty();
    }

    @Test
    void parseBundles_noBundlesSection_returnsEmpty() throws Exception {
        writeCatalog("[libraries]\nyeast = \"com.example:acme:1.0.0\"\n");
        assertThat(reader.parseBundles(tempDir)).isEmpty();
    }

    @Test
    void read_expandsBundleReferenceIntoMemberDeps() throws Exception {
        // Bundle membership should produce one dep per constituent library.
        writeCatalog("[libraries]\n" +
                "yeast       = \"com.example:acme:1.0.0\"\n" +
                "spring-core = { module = \"org.springframework:spring-core\", version = \"6.2.0\" }\n\n" +
                "[bundles]\n" +
                "core = [\"yeast\", \"spring-core\"]\n");
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"my-app\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"),
                "group = \"com.example\"\nversion = \"1.0.0\"\n\n" +
                "dependencies {\n" +
                "    implementation(libs.bundles.core)\n" +
                "}\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(d -> d.groupId() + ":" + d.artifactId() + ":" + d.version())
                .containsExactlyInAnyOrder(
                        "com.example:acme:1.0.0",
                        "org.springframework:spring-core:6.2.0");
    }

    @Test
    void read_bundleRefWithUnknownBundle_isIgnoredQuietly() throws Exception {
        writeCatalog("[libraries]\nyeast = \"com.example:acme:1.0.0\"\n");
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"my-app\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"),
                "group = \"com.example\"\nversion = \"1.0\"\n" +
                "dependencies { implementation(libs.bundles.missing) }\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies()).isEmpty();
    }

    // ── Nested build-file scanning ────────────────────────────────────────────

    @Test
    void read_walksSubProjectsAndAggregatesDeps() throws Exception {
        // Multi-module Gradle layout: root build file with NO real deps, but the
        // real deps live in nested sub-project build files. Pharos should aggregate
        // them all into the parent project node.
        writeCatalog("[libraries]\nyeast = \"com.example:acme:1.0.0\"\n");
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"workspace\"");
        // Root build file: only project metadata, no implementation lines.
        Files.writeString(tempDir.resolve("build.gradle.kts"),
                "group = \"com.example\"\nversion = \"1.0.0\"\n");

        // Sub-project with a real catalog ref.
        Path backend = tempDir.resolve("backend");
        Files.createDirectories(backend);
        Files.writeString(backend.resolve("build.gradle.kts"),
                "dependencies { implementation(libs.yeast) }\n");

        // Deeply nested sub-project with a literal coord.
        Path deeplyNested = tempDir.resolve("modules").resolve("solver");
        Files.createDirectories(deeplyNested);
        Files.writeString(deeplyNested.resolve("build.gradle.kts"),
                "dependencies { implementation(\"org.apache.commons:commons-lang3:3.14.0\") }\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::artifactId)
                .containsExactlyInAnyOrder("yeast", "commons-lang3");
    }

    @Test
    void read_dedupesSameDepDeclaredInMultipleSubprojects() throws Exception {
        // Two sub-projects each declare implementation(libs.yeast). We should record
        // ONE yeast dep, not two — the module graph keys on group:artifact.
        writeCatalog("[libraries]\nyeast = \"com.example:acme:1.0.0\"\n");
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"workspace\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"), "group = \"com.example\"\n");

        Path a = tempDir.resolve("module-a"); Files.createDirectories(a);
        Files.writeString(a.resolve("build.gradle.kts"),
                "dependencies { implementation(libs.yeast) }\n");
        Path b = tempDir.resolve("module-b"); Files.createDirectories(b);
        Files.writeString(b.resolve("build.gradle.kts"),
                "dependencies { implementation(libs.yeast) }\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .filteredOn(d -> "yeast".equals(d.artifactId()))
                .hasSize(1);
    }

    @Test
    void read_skipsBuildSrcDirectory() throws Exception {
        // buildSrc holds Gradle convention-plugin code — its deps are plugin
        // tooling, not application deps. They must not leak into the project graph.
        writeCatalog("[libraries]\n" +
                "yeast        = \"com.example:acme:1.0.0\"\n" +
                "kotlin-plugin = \"org.jetbrains.kotlin:kotlin-gradle-plugin:1.9.0\"\n");
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"workspace\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"), "group = \"com.example\"\n");

        // buildSrc with a plugin dep — should NOT appear in results.
        Path buildSrc = tempDir.resolve("buildSrc");
        Files.createDirectories(buildSrc);
        Files.writeString(buildSrc.resolve("build.gradle.kts"),
                "dependencies { implementation(libs.kotlin.plugin) }\n");

        // Real sub-project with yeast — should appear.
        Path real = tempDir.resolve("backend");
        Files.createDirectories(real);
        Files.writeString(real.resolve("build.gradle.kts"),
                "dependencies { implementation(libs.yeast) }\n");

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies())
                .extracting(MavenPomReader.MavenDependency::artifactId)
                .containsExactly("yeast")
                .doesNotContain("kotlin-gradle-plugin");
    }

    @Test
    void read_skipsNoiseDirectories() throws Exception {
        // build/, .gradle/, node_modules/ — these contain generated artefacts that
        // would inflate the scan time and produce spurious deps.
        Files.writeString(tempDir.resolve("settings.gradle.kts"), "rootProject.name = \"workspace\"");
        Files.writeString(tempDir.resolve("build.gradle.kts"), "group = \"com.example\"\n");

        for (String noise : List.of("build", ".gradle", "node_modules", "target", ".git")) {
            Path d = tempDir.resolve(noise).resolve("nested");
            Files.createDirectories(d);
            Files.writeString(d.resolve("build.gradle.kts"),
                    "dependencies { implementation(\"should:not:appear\") }\n");
        }

        MavenPomReader.PomInfo info = reader.read(tempDir).orElseThrow();
        assertThat(info.dependencies()).isEmpty();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private void writeCatalog(String contents) throws java.io.IOException {
        Path gradleDir = tempDir.resolve("gradle");
        Files.createDirectories(gradleDir);
        Files.writeString(gradleDir.resolve("libs.versions.toml"), contents);
    }
}
