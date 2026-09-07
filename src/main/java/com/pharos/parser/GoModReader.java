package com.pharos.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads Go module metadata from {@code go.mod} (and {@code go.work}, when present).
 *
 * <p>Mirrors the shape of {@link MavenPomReader} / {@link PyprojectReader} /
 * {@link PackageJsonReader}: a static {@code isGoProject} probe plus a
 * {@code read} that returns the universal {@link MavenPomReader.PomInfo} currency
 * consumed by {@code ModuleGraphBuilder}.
 *
 * <p>Mapping onto Maven coordinates:
 * <ul>
 *   <li>{@code groupId}    → the literal {@code "go"} (module-key namespace, matching
 *       the {@code python:} / {@code npm:} / {@code cmake:} convention)</li>
 *   <li>{@code artifactId} → the full module path, e.g. {@code github.com/acme/api}</li>
 *   <li>{@code version}    → {@code "unknown"}; Go module versions live in VCS tags,
 *       not in {@code go.mod}. The {@code go} directive is a <em>language</em> version
 *       and is deliberately not used here.</li>
 *   <li>{@code dependencies} → {@code require} directives. Direct requirements get
 *       scope {@code compile}; those marked {@code // indirect} get {@code runtime}.</li>
 *   <li>{@code modules} → filesystem-local sibling modules discovered from
 *       {@code replace ... => ./path} directives and {@code go.work} {@code use}
 *       entries. These are the strongest available signal for cross-project linking
 *       within a monorepo.</li>
 * </ul>
 *
 * <p>Limitations (by design): {@code exclude} and {@code retract} directives are
 * ignored, and {@code replace} targets that point at another <em>module path</em>
 * (rather than a local directory) are not rewritten into the dependency list.
 */
public class GoModReader {

    private static final Logger log = LoggerFactory.getLogger(GoModReader.class);

    /** Module-key namespace, matching {@code python:} / {@code npm:} / {@code cmake:}. */
    public static final String GROUP_PREFIX = "go";

    private static final String UNKNOWN_VERSION = "unknown";

    /** {@code module github.com/acme/api} — quoted form is legal but rare. */
    private static final Pattern RE_MODULE =
            Pattern.compile("^module\\s+[\"']?([^\"'\\s]+)[\"']?");

    /** {@code go 1.22} / {@code go 1.22.3}. */
    private static final Pattern RE_GO_VERSION = Pattern.compile("^go\\s+([\\d.]+)");

    /** A {@code require} entry: {@code <path> <version>}, optionally {@code // indirect}. */
    private static final Pattern RE_REQUIRE_ENTRY =
            Pattern.compile("^([^\\s()]+)\\s+(v[^\\s]+)");

    /** {@code replace old[ vX] => new[ vY]} — group 1 = target (may be a local path). */
    private static final Pattern RE_REPLACE_TARGET =
            Pattern.compile("=>\\s*([^\\s]+)");

    /** A {@code use} entry inside go.work: {@code ./svc/api} or {@code use ./svc/api}. */
    private static final Pattern RE_USE_ENTRY =
            Pattern.compile("^(?:use\\s+)?([./][^\\s()]*)");

    // ---------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------

    /** True when {@code projectRoot} directly contains a {@code go.mod} or {@code go.work}. */
    public static boolean isGoProject(Path projectRoot) {
        return Files.exists(projectRoot.resolve("go.mod"))
                || Files.exists(projectRoot.resolve("go.work"));
    }

    /**
     * Walks up from {@code startDir} looking for the nearest enclosing {@code go.mod}.
     * Used by {@link GoCodeParser} to turn a source directory into a Go import path.
     *
     * @return the directory containing {@code go.mod}, or empty if none up to the filesystem root
     */
    public static Optional<Path> findModuleRoot(Path startDir) {
        Path dir = startDir;
        while (dir != null) {
            if (Files.exists(dir.resolve("go.mod"))) return Optional.of(dir);
            dir = dir.getParent();
        }
        return Optional.empty();
    }

    /** Reads just the {@code module} directive from a {@code go.mod} file. */
    public static Optional<String> readModulePath(Path goModFile) {
        try {
            for (String raw : Files.readAllLines(goModFile, StandardCharsets.UTF_8)) {
                Matcher m = RE_MODULE.matcher(raw.strip());
                if (m.find()) return Optional.of(m.group(1));
            }
        } catch (IOException e) {
            log.debug("Failed to read {}: {}", goModFile, e.getMessage());
        }
        return Optional.empty();
    }

    /**
     * Parses {@code <projectRoot>/go.mod} into the shared {@link MavenPomReader.PomInfo}
     * shape. Falls back to {@code go.work} when only a workspace file is present.
     */
    public Optional<MavenPomReader.PomInfo> read(Path projectRoot) {
        try {
            Path goMod = projectRoot.resolve("go.mod");
            if (Files.exists(goMod)) {
                return parseGoMod(goMod, projectRoot);
            }
            Path goWork = projectRoot.resolve("go.work");
            if (Files.exists(goWork)) {
                return parseGoWorkOnly(goWork, projectRoot);
            }
            return Optional.empty();
        } catch (Exception e) {
            log.debug("Failed to read Go project at {}: {}", projectRoot, e.getMessage());
            return Optional.empty();
        }
    }

    // ---------------------------------------------------------------------------
    // go.mod parsing
    // ---------------------------------------------------------------------------

    private Optional<MavenPomReader.PomInfo> parseGoMod(Path goMod, Path projectRoot)
            throws IOException {
        List<String> lines = Files.readAllLines(goMod, StandardCharsets.UTF_8);

        String modulePath = null;
        String goVersion = null;
        List<MavenPomReader.MavenDependency> deps = new ArrayList<>();
        List<String> localModules = new ArrayList<>();

        // Which parenthesised block we are inside, if any.
        String block = null;

        for (String raw : lines) {
            String line = stripComment(raw).strip();
            String withComment = raw.strip();
            if (line.isEmpty()) continue;

            // --- close a block ---
            if (block != null && line.startsWith(")")) {
                block = null;
                continue;
            }

            // --- open a block: "require (", "replace (", "exclude (", "retract (" ---
            if (block == null) {
                String directive = directiveOf(line);
                if (directive != null && line.endsWith("(")) {
                    block = directive;
                    continue;
                }
            }

            if (block != null) {
                switch (block) {
                    case "require" -> addRequire(deps, line, withComment);
                    case "replace" -> addLocalReplace(localModules, line);
                    default -> { /* exclude / retract — ignored by design */ }
                }
                continue;
            }

            // --- single-line directives ---
            if (modulePath == null) {
                Matcher m = RE_MODULE.matcher(line);
                if (m.find()) { modulePath = m.group(1); continue; }
            }
            if (goVersion == null) {
                Matcher m = RE_GO_VERSION.matcher(line);
                if (m.find()) { goVersion = m.group(1); continue; }
            }
            if (line.startsWith("require ")) {
                addRequire(deps, line.substring("require ".length()).strip(), withComment);
                continue;
            }
            if (line.startsWith("replace ")) {
                addLocalReplace(localModules, line);
            }
        }

        if (modulePath == null) {
            log.debug("{} has no module directive", goMod);
            return Optional.empty();
        }

        // A go.work alongside go.mod widens the set of local modules.
        Path goWork = projectRoot.resolve("go.work");
        if (Files.exists(goWork)) {
            collectWorkspaceUses(goWork, localModules);
        }

        log.debug("Go module detected: {}:{} (go {})", GROUP_PREFIX, modulePath,
                goVersion != null ? goVersion : "?");
        return Optional.of(new MavenPomReader.PomInfo(
                new MavenPomReader.MavenCoordinates(GROUP_PREFIX, modulePath, UNKNOWN_VERSION),
                deps,
                List.copyOf(localModules)));
    }

    /**
     * A workspace root with no {@code go.mod} of its own. The workspace directory name
     * stands in as the module path, and each {@code use} entry becomes a local module.
     */
    private Optional<MavenPomReader.PomInfo> parseGoWorkOnly(Path goWork, Path projectRoot)
            throws IOException {
        List<String> localModules = new ArrayList<>();
        collectWorkspaceUses(goWork, localModules);
        if (localModules.isEmpty()) return Optional.empty();

        Path fileName = projectRoot.getFileName();
        String name = fileName != null ? fileName.toString() : "workspace";
        log.debug("Go workspace detected at {} with {} module(s)", projectRoot, localModules.size());
        return Optional.of(new MavenPomReader.PomInfo(
                new MavenPomReader.MavenCoordinates(GROUP_PREFIX, name, UNKNOWN_VERSION),
                List.of(),
                List.copyOf(localModules)));
    }

    private void collectWorkspaceUses(Path goWork, List<String> localModules) throws IOException {
        boolean inUseBlock = false;
        for (String raw : Files.readAllLines(goWork, StandardCharsets.UTF_8)) {
            String line = stripComment(raw).strip();
            if (line.isEmpty()) continue;

            if (inUseBlock) {
                if (line.startsWith(")")) { inUseBlock = false; continue; }
                addUse(localModules, line);
                continue;
            }
            if (line.startsWith("use") && line.endsWith("(")) { inUseBlock = true; continue; }
            if (line.startsWith("use ")) addUse(localModules, line);
        }
    }

    // ---------------------------------------------------------------------------
    // Entry helpers
    // ---------------------------------------------------------------------------

    private void addRequire(List<MavenPomReader.MavenDependency> deps,
                            String entry, String rawWithComment) {
        Matcher m = RE_REQUIRE_ENTRY.matcher(entry);
        if (!m.find()) return;
        boolean indirect = rawWithComment.contains("// indirect");
        deps.add(new MavenPomReader.MavenDependency(
                GROUP_PREFIX, m.group(1), m.group(2), indirect ? "runtime" : "compile"));
    }

    /** Records only {@code replace} targets that are filesystem-local. */
    private void addLocalReplace(List<String> localModules, String line) {
        Matcher m = RE_REPLACE_TARGET.matcher(line);
        if (!m.find()) return;
        String target = m.group(1);
        if (target.startsWith("./") || target.startsWith("../")
                || target.equals(".") || target.equals("..")) {
            if (!localModules.contains(target)) localModules.add(target);
        }
    }

    private void addUse(List<String> localModules, String line) {
        Matcher m = RE_USE_ENTRY.matcher(line.startsWith("use ")
                ? line.substring("use ".length()).strip() : line);
        if (!m.find()) return;
        String target = m.group(1);
        if (!localModules.contains(target)) localModules.add(target);
    }

    private static String directiveOf(String line) {
        for (String d : List.of("require", "replace", "exclude", "retract")) {
            if (line.equals(d + " (") || line.startsWith(d + " (")) return d;
        }
        return null;
    }

    /** Strips a trailing {@code // ...} comment, leaving directive text only. */
    private static String stripComment(String line) {
        int idx = line.indexOf("//");
        return idx >= 0 ? line.substring(0, idx) : line;
    }
}
