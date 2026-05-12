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
 * Reads Python project metadata from {@code pyproject.toml} or {@code setup.py}.
 *
 * Uses line-by-line text parsing — no external TOML library required.
 * Extracts project name, version, and declared dependencies so Python projects
 * can be registered in the module dependency graph.
 */
public class PyprojectReader {

    private static final Logger log = LoggerFactory.getLogger(PyprojectReader.class);

    private static final String GROUP_PREFIX = "python"; // synthetic group for Python packages

    // Patterns for pyproject.toml [project] section
    private static final Pattern RE_NAME    = Pattern.compile("^name\\s*=\\s*[\"']([^\"']+)[\"']");
    private static final Pattern RE_VERSION = Pattern.compile("^version\\s*=\\s*[\"']([^\"']+)[\"']");
    // Dependency entries: "package>=1.0" or "package" — extract the package name part
    private static final Pattern RE_DEP     = Pattern.compile("^[\"']?([A-Za-z][\\w.\\-]+)([>=<!,;\"' \\[].*)?$");

    // Fallback: setup.py / setup.cfg
    private static final Pattern RE_SETUP_NAME    = Pattern.compile("name\\s*=\\s*[\"']([^\"']+)[\"']");
    private static final Pattern RE_SETUP_VERSION = Pattern.compile("version\\s*=\\s*[\"']([^\"']+)[\"']");

    public static boolean isPythonProject(Path projectRoot) {
        return Files.exists(projectRoot.resolve("pyproject.toml"))
                || Files.exists(projectRoot.resolve("setup.py"))
                || Files.exists(projectRoot.resolve("setup.cfg"));
    }

    public Optional<MavenPomReader.PomInfo> read(Path projectRoot) {
        try {
            // Try pyproject.toml first
            Path pyproject = projectRoot.resolve("pyproject.toml");
            if (Files.exists(pyproject)) {
                return parsePyproject(pyproject);
            }
            // Fallback: setup.py
            Path setupPy = projectRoot.resolve("setup.py");
            if (Files.exists(setupPy)) {
                return parseSetupPy(setupPy, projectRoot);
            }
            // Fallback: setup.cfg
            Path setupCfg = projectRoot.resolve("setup.cfg");
            if (Files.exists(setupCfg)) {
                return parseSetupCfg(setupCfg);
            }
            return Optional.empty();
        } catch (Exception e) {
            log.debug("Failed to read Python project at {}: {}", projectRoot, e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<MavenPomReader.PomInfo> parsePyproject(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file);
        String name = null, version = null;
        List<MavenPomReader.MavenDependency> deps = new ArrayList<>();
        boolean inProject = false, inDeps = false;

        for (String raw : lines) {
            String line = raw.strip();
            if (line.startsWith("[")) {
                inProject = line.equals("[project]") || line.equals("[tool.poetry]");
                inDeps = false;
                continue;
            }
            if (!inProject) continue;

            if (line.startsWith("dependencies")) {
                inDeps = true;
                continue;
            }
            if (inDeps) {
                if (line.startsWith("[") || (line.startsWith("[") && line.contains("]"))) {
                    inDeps = false;
                } else if (line.startsWith("]")) {
                    inDeps = false;
                } else if (!line.isEmpty() && !line.startsWith("#")) {
                    // Strip list markers and quotes
                    String dep = line.replaceAll("^[\"',\\[\\]]+|[\"',\\[\\]]+$", "").strip();
                    Matcher m = RE_DEP.matcher(dep);
                    if (m.matches() && !dep.isEmpty()) {
                        String pkg = m.group(1).replace("-", "_");
                        deps.add(new MavenPomReader.MavenDependency(
                                GROUP_PREFIX, pkg, null, "runtime"));
                    }
                }
                continue;
            }

            if (name == null) {
                Matcher m = RE_NAME.matcher(line);
                if (m.find()) { name = m.group(1); continue; }
            }
            if (version == null) {
                Matcher m = RE_VERSION.matcher(line);
                if (m.find()) { version = m.group(1); }
            }
        }

        if (name == null) return Optional.empty();
        String artifactId = name.replace("-", "_");
        log.debug("Python project detected: {}:{}", GROUP_PREFIX, artifactId);
        return Optional.of(new MavenPomReader.PomInfo(
                new MavenPomReader.MavenCoordinates(GROUP_PREFIX, artifactId, version != null ? version : "unknown"),
                deps, List.of()));
    }

    private Optional<MavenPomReader.PomInfo> parseSetupPy(Path file, Path projectRoot) throws IOException {
        String text = Files.readString(file);
        String name = firstMatch(RE_SETUP_NAME, text);
        String version = firstMatch(RE_SETUP_VERSION, text);
        if (name == null) name = projectRoot.getFileName().toString();
        String artifactId = name.replace("-", "_");
        return Optional.of(new MavenPomReader.PomInfo(
                new MavenPomReader.MavenCoordinates(GROUP_PREFIX, artifactId, version != null ? version : "unknown"),
                List.of(), List.of()));
    }

    private Optional<MavenPomReader.PomInfo> parseSetupCfg(Path file) throws IOException {
        String name = null, version = null;
        for (String line : Files.readAllLines(file)) {
            String s = line.strip();
            if (name == null && s.startsWith("name")) {
                name = s.substring(s.indexOf('=') + 1).strip();
            } else if (version == null && s.startsWith("version")) {
                version = s.substring(s.indexOf('=') + 1).strip();
            }
        }
        if (name == null) return Optional.empty();
        String artifactId = name.replace("-", "_");
        return Optional.of(new MavenPomReader.PomInfo(
                new MavenPomReader.MavenCoordinates(GROUP_PREFIX, artifactId, version != null ? version : "unknown"),
                List.of(), List.of()));
    }

    private static String firstMatch(Pattern p, String text) {
        Matcher m = p.matcher(text);
        return m.find() ? m.group(1) : null;
    }
}
