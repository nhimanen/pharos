package com.pharos.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/**
 * Discovers project roots within a directory tree by recognising build-system
 * and package-manager marker files.
 *
 * <p>Supported ecosystems and their markers:
 * <ul>
 *   <li>Java/Maven    — {@code pom.xml}
 *   <li>Java/Gradle   — {@code build.gradle}, {@code build.gradle.kts},
 *                       {@code settings.gradle}, {@code settings.gradle.kts}
 *   <li>Scala/sbt     — {@code build.sbt}
 *   <li>Python        — {@code pyproject.toml}, {@code setup.py}, {@code setup.cfg},
 *                       {@code Pipfile}, {@code requirements.txt}
 *   <li>Node.js/TS    — {@code package.json}
 *   <li>Rust          — {@code Cargo.toml}
 *   <li>Go            — {@code go.mod}
 *   <li>Ruby          — {@code Gemfile}
 *   <li>.NET/C#       — {@code *.csproj}, {@code *.sln}, {@code *.fsproj}, {@code *.vbproj}
 *   <li>PHP           — {@code composer.json}
 *   <li>Elixir        — {@code mix.exs}
 *   <li>Haskell       — {@code package.yaml}, {@code *.cabal}
 *   <li>Swift/Apple   — {@code Package.swift}
 *   <li>Clojure       — {@code project.clj}, {@code deps.edn}
 * </ul>
 *
 * <p>Discovery stops descending into a subtree once a project root is found,
 * preventing false positives from nested module directories (e.g. Maven
 * multi-module or Gradle subprojects) being treated as independent projects
 * unless {@code --nested} mode is requested.
 */
public class ProjectDiscovery {

    private static final Logger log = LoggerFactory.getLogger(ProjectDiscovery.class);

    /** Exact-match marker file names (case-sensitive). */
    private static final Set<String> EXACT_MARKERS = Set.of(
            // Java / JVM
            "pom.xml",
            "build.gradle", "build.gradle.kts",
            "settings.gradle", "settings.gradle.kts",
            "build.sbt",
            // Python
            "pyproject.toml", "setup.py", "setup.cfg", "Pipfile", "requirements.txt",
            // Node / TypeScript / JavaScript
            "package.json",
            // Rust
            "Cargo.toml",
            // Go
            "go.mod",
            // Ruby
            "Gemfile",
            // PHP
            "composer.json",
            // Elixir / Erlang
            "mix.exs",
            // Swift / Apple platforms
            "Package.swift",
            // Clojure
            "project.clj", "deps.edn",
            // Haskell
            "package.yaml"
    );

    /** Glob-style suffix markers (matched against file name only). */
    private static final List<String> SUFFIX_MARKERS = List.of(
            ".csproj", ".fsproj", ".vbproj", ".sln",   // .NET
            ".cabal"                                     // Haskell
    );

    /** Directories that are never project roots and should be skipped entirely. */
    private static final Set<String> SKIP_DIRS = Set.of(
            ".git", ".hg", ".svn",
            "node_modules", ".yarn",
            "target", "build", "dist", "out", ".gradle",
            "__pycache__", ".tox", ".venv", "venv", ".env",
            ".idea", ".vscode",
            "vendor",          // Go / PHP
            ".cargo"           // Rust cache
    );

    private ProjectDiscovery() {}

    /**
     * Discovers project roots directly under {@code root}.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>If {@code root} itself is a project root, return it as a single-element list.
     *   <li>Otherwise walk immediate children; for each sub-directory that is a project
     *       root, add it and do not descend further into it.
     *   <li>Continue recursing into non-project sub-directories up to {@code maxDepth}.
     * </ol>
     *
     * @param root     directory to search (must exist and be a directory)
     * @param maxDepth maximum depth below {@code root} to recurse (2–3 is usually enough)
     * @return discovered project roots, sorted by path
     */
    public static List<DiscoveredProject> discover(Path root, int maxDepth) throws IOException {
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Not a directory: " + root);
        }

        // If root itself is a project root, treat it as a single project
        if (isProjectRoot(root)) {
            return List.of(new DiscoveredProject(root, deriveProjectName(root)));
        }

        List<DiscoveredProject> found = new ArrayList<>();
        collectProjects(root, root, maxDepth, found);
        found.sort(Comparator.comparing(p -> p.path().toString()));
        return Collections.unmodifiableList(found);
    }

    /** Returns true if {@code dir} itself is a project root. */
    public static boolean isProjectRoot(Path dir) {
        if (!Files.isDirectory(dir)) return false;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry) && matchesMarker(entry.getFileName().toString())) {
                    return true;
                }
            }
        } catch (IOException e) {
            log.debug("Cannot read directory {}: {}", dir, e.getMessage());
        }
        return false;
    }

    private static void collectProjects(Path searchRoot, Path dir,
                                        int remainingDepth,
                                        List<DiscoveredProject> found) throws IOException {
        if (remainingDepth <= 0) return;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (!Files.isDirectory(entry)) continue;
                String name = entry.getFileName().toString();
                if (SKIP_DIRS.contains(name) || name.startsWith(".")) continue;

                if (isProjectRoot(entry)) {
                    found.add(new DiscoveredProject(entry, deriveProjectName(entry)));
                    log.debug("Discovered project: {}", entry);
                    // Do not recurse into discovered project roots
                } else {
                    collectProjects(searchRoot, entry, remainingDepth - 1, found);
                }
            }
        } catch (IOException e) {
            log.debug("Cannot list {}: {}", dir, e.getMessage());
        }
    }

    private static boolean matchesMarker(String fileName) {
        if (EXACT_MARKERS.contains(fileName)) return true;
        for (String suffix : SUFFIX_MARKERS) {
            if (fileName.endsWith(suffix)) return true;
        }
        return false;
    }

    /**
     * Derives a human-readable project name from its root directory.
     * Uses the directory name, stripping common version-suffix patterns like "-1.0" or "_main".
     */
    static String deriveProjectName(Path projectRoot) {
        String name = projectRoot.getFileName().toString();
        // Strip trailing version numbers: my-lib-1.2.3 → my-lib
        name = name.replaceAll("-\\d+(\\.\\d+)*$", "");
        return name;
    }

    /**
     * A discovered project with its root path and derived name.
     */
    public record DiscoveredProject(Path path, String name) {}
}
