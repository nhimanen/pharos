package com.pharos.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads Gradle project metadata from {@code settings.gradle}, {@code gradle.properties},
 * and {@code build.gradle} / {@code build.gradle.kts}.
 *
 * Extracts enough information to register the project in the module dependency graph:
 * coordinates (group:artifact:version) and declared dependencies.
 * Does NOT evaluate dynamic versions, catalog aliases, or complex version logic.
 */
public class GradleBuildReader {

    private static final Logger log = LoggerFactory.getLogger(GradleBuildReader.class);

    private static final Pattern RE_ROOT_NAME    = Pattern.compile("rootProject\\.name\\s*=\\s*[\"']([^\"']+)[\"']");
    private static final Pattern RE_GROUP        = Pattern.compile("(?m)^group\\s*=\\s*[\"']([^\"']+)[\"']");
    private static final Pattern RE_VERSION_PROP = Pattern.compile("(?m)^version\\s*=\\s*[\"']?([\\w.\\-]+)[\"']?");
    // Groovy/Kotlin dependency notations: implementation("g:a:v") or implementation 'g:a:v'
    private static final Pattern RE_DEP         = Pattern.compile(
            "(?:implementation|api|compile|runtimeOnly|testImplementation)\\s*[\"'(]([A-Za-z][\\w.\\-]+:[\\w.\\-]+:[\\w.\\-]+)[\"')]");

    /** Returns true if the given project root looks like a Gradle project. */
    public static boolean isGradleProject(Path projectRoot) {
        return Files.exists(projectRoot.resolve("settings.gradle"))
                || Files.exists(projectRoot.resolve("settings.gradle.kts"))
                || Files.exists(projectRoot.resolve("build.gradle"))
                || Files.exists(projectRoot.resolve("build.gradle.kts"));
    }

    /**
     * Attempts to parse Gradle project metadata from {@code projectRoot}.
     * Returns empty on any failure or if no Gradle files are found.
     */
    public Optional<MavenPomReader.PomInfo> read(Path projectRoot) {
        if (!isGradleProject(projectRoot)) return Optional.empty();
        try {
            String name    = extractName(projectRoot);
            String group   = extractGroup(projectRoot);
            String version = extractVersion(projectRoot);

            if (name == null) name = projectRoot.getFileName().toString();
            if (group == null) group = name; // use name as group when unknown
            if (version == null) version = "unknown";

            String moduleKey = group + ":" + name;
            log.debug("Gradle project detected: {} = {}:{}", projectRoot, group, name);

            List<MavenPomReader.MavenDependency> deps = extractDeps(projectRoot);
            MavenPomReader.MavenCoordinates coords =
                    new MavenPomReader.MavenCoordinates(group, name, version);
            return Optional.of(new MavenPomReader.PomInfo(coords, deps, List.of()));

        } catch (Exception e) {
            log.debug("Failed to read Gradle project at {}: {}", projectRoot, e.getMessage());
            return Optional.empty();
        }
    }

    private String extractName(Path root) throws IOException {
        for (String filename : List.of("settings.gradle", "settings.gradle.kts")) {
            Path f = root.resolve(filename);
            if (Files.exists(f)) {
                String text = Files.readString(f);
                Matcher m = RE_ROOT_NAME.matcher(text);
                if (m.find()) return m.group(1);
            }
        }
        return null;
    }

    private String extractGroup(Path root) throws IOException {
        // Check gradle.properties first
        Path props = root.resolve("gradle.properties");
        if (Files.exists(props)) {
            for (String line : Files.readAllLines(props)) {
                if (line.startsWith("group=") || line.startsWith("group =")) {
                    return line.substring(line.indexOf('=') + 1).trim();
                }
            }
        }
        // Fall back to build.gradle
        for (String filename : List.of("build.gradle", "build.gradle.kts")) {
            Path f = root.resolve(filename);
            if (Files.exists(f)) {
                Matcher m = RE_GROUP.matcher(Files.readString(f));
                if (m.find()) return m.group(1);
            }
        }
        return null;
    }

    private String extractVersion(Path root) throws IOException {
        Path props = root.resolve("gradle.properties");
        if (Files.exists(props)) {
            for (String line : Files.readAllLines(props)) {
                if (line.startsWith("version=") || line.startsWith("version =")) {
                    return line.substring(line.indexOf('=') + 1).trim();
                }
            }
        }
        for (String filename : List.of("build.gradle", "build.gradle.kts")) {
            Path f = root.resolve(filename);
            if (Files.exists(f)) {
                Matcher m = RE_VERSION_PROP.matcher(Files.readString(f));
                if (m.find()) return m.group(1);
            }
        }
        return null;
    }

    private List<MavenPomReader.MavenDependency> extractDeps(Path root) throws IOException {
        List<MavenPomReader.MavenDependency> deps = new ArrayList<>();
        for (String filename : List.of("build.gradle", "build.gradle.kts")) {
            Path f = root.resolve(filename);
            if (!Files.exists(f)) continue;
            Matcher m = RE_DEP.matcher(Files.readString(f));
            while (m.find()) {
                String[] parts = m.group(1).split(":");
                if (parts.length >= 2) {
                    deps.add(new MavenPomReader.MavenDependency(
                            parts[0], parts[1],
                            parts.length >= 3 ? parts[2] : null,
                            "compile"));
                }
            }
        }
        return deps;
    }
}
