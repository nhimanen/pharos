package com.pharos.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads Gradle project metadata from {@code settings.gradle}, {@code gradle.properties},
 * {@code build.gradle} / {@code build.gradle.kts}, and the version catalog at
 * {@code gradle/libs.versions.toml}.
 *
 * <p>Extracts enough information to register the project in the module dependency graph:
 * coordinates (group:artifact:version) and declared dependencies. Recognized dependency
 * declaration styles:
 * <ul>
 *   <li>Literal coordinates: {@code implementation("g:a:v")} / {@code 'g:a:v'}</li>
 *   <li>Version-catalog refs: {@code implementation(libs.spring.boot.starter)} — looked up
 *       in {@code gradle/libs.versions.toml} (aliases use {@code -}, refs use {@code .}).</li>
 *   <li>Project refs: {@code implementation(project(":sub-module"))} — emitted as a
 *       local-module edge with version "LOCAL".</li>
 *   <li>Bundle refs: {@code implementation(libs.bundles.foo)} — expanded into the
 *       constituent libraries declared in {@code [bundles]}.</li>
 * </ul>
 *
 * <p>For multi-module Gradle projects, every {@code build.gradle*} file under the
 * project root is scanned (skipping {@code buildSrc/}, build/output dirs, and dotfiles)
 * and deps are aggregated into a single set keyed by group:artifact.
 *
 * <p>Custom DSL functions (e.g. {@code kernel("backend:cmt")}), interpolated variables
 * ({@code "g:a:$ver"}), and dynamic version logic are NOT evaluated — those would
 * require running Gradle itself.
 */
public class GradleBuildReader {

    private static final Logger log = LoggerFactory.getLogger(GradleBuildReader.class);

    private static final Pattern RE_ROOT_NAME    = Pattern.compile("rootProject\\.name\\s*=\\s*[\"']([^\"']+)[\"']");
    private static final Pattern RE_GROUP        = Pattern.compile("(?m)^group\\s*=\\s*[\"']([^\"']+)[\"']");
    private static final Pattern RE_VERSION_PROP = Pattern.compile("(?m)^version\\s*=\\s*[\"']?([\\w.\\-]+)[\"']?");
    // Groovy/Kotlin literal-coordinate dependency. Handles all four shapes:
    //   implementation "g:a:v"           (Groovy double quotes)
    //   implementation 'g:a:v'           (Groovy single quotes)
    //   implementation("g:a:v")          (Kotlin DSL — most common modern form)
    //   implementation('g:a:v')          (Groovy with explicit parens)
    // Constrained to 3-part Maven coords (no $ interpolation, no .ref tokens) so we
    // don't falsely match catalog refs that happen to start with the same configurations.
    private static final Pattern RE_DEP_LITERAL = Pattern.compile(
            "(?:implementation|api|compile|runtimeOnly|testImplementation)" +
            // First char must be a letter (excludes catalog refs and stray numbers);
            // remaining chars in the groupId part are optional so single-letter groups
            // like 'g:a:1.0' (common in test fixtures) still match.
            "\\s*\\(?\\s*[\"']([A-Za-z][\\w.\\-]*:[\\w.\\-]+:[\\w.\\-]+)[\"']\\s*\\)?");
    // Catalog-reference dependency: implementation(libs.spring.boot.starter)
    private static final Pattern RE_DEP_CATALOG = Pattern.compile(
            "(?:implementation|api|compile|runtimeOnly|testImplementation)\\s*\\(\\s*libs\\.([\\w.]+)");
    // Local module reference: implementation(project(":sub-module")) or 'project(":..")'
    private static final Pattern RE_DEP_PROJECT = Pattern.compile(
            "(?:implementation|api|compile|runtimeOnly|testImplementation)\\s*\\(\\s*project\\s*\\(\\s*[\"']:([\\w.\\-]+)[\"']\\s*\\)");

    // libs.versions.toml — minimal parser scoped to [versions] and [libraries] tables.
    private static final Pattern RE_TOML_SECTION = Pattern.compile("^\\s*\\[(\\w+)\\]\\s*$");
    // name = "value" — used for both versions entries and the inline-string library form.
    private static final Pattern RE_TOML_KV_STRING = Pattern.compile(
            "^\\s*([\\w.\\-]+)\\s*=\\s*\"([^\"]+)\"\\s*$");
    // name = { module = "g:a", version = "1.0" }      | version may be inline "..."
    // name = { module = "g:a", version.ref = "alias" } | or a ref to [versions]
    private static final Pattern RE_TOML_LIB_TABLE = Pattern.compile(
            "^\\s*([\\w.\\-]+)\\s*=\\s*\\{[^}]*module\\s*=\\s*\"([^\"]+)\"[^}]*}\\s*$");
    private static final Pattern RE_TOML_INLINE_VERSION     = Pattern.compile("\\bversion\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern RE_TOML_INLINE_VERSION_REF = Pattern.compile("\\bversion\\.ref\\s*=\\s*\"([^\"]+)\"");
    // Inline-array bundle: name = ["a", "b", "c"]
    private static final Pattern RE_TOML_BUNDLE_INLINE = Pattern.compile(
            "^\\s*([\\w.\\-]+)\\s*=\\s*\\[(.+)\\]\\s*$");
    // Start of a multi-line bundle array: name = [   (may have items after [)
    private static final Pattern RE_TOML_BUNDLE_START = Pattern.compile(
            "^\\s*([\\w.\\-]+)\\s*=\\s*\\[(.*)$");
    // Quoted string item inside an array
    private static final Pattern RE_TOML_QUOTED_STRING = Pattern.compile("\"([^\"]+)\"");

    /**
     * Directories we should never descend into when scanning for build files.
     * {@code buildSrc} is excluded because it contains Gradle convention-plugin code,
     * not application dependencies — including its deps would conflate plugin
     * tooling with the project's actual runtime/compile dependencies.
     */
    private static final Set<String> SKIP_DIRS = Set.of(
            ".git", ".hg", ".svn",
            "buildSrc",
            "node_modules", "vendor",
            "target", "build", "out", "dist", ".gradle",
            "logs", "log", "tmp", ".cache",
            ".idea", ".vscode"
    );

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
        // Catalog and bundles are project-wide — parsed once from the single
        // gradle/libs.versions.toml at the project root, used by every nested build file.
        Map<String, String>       catalog = parseVersionCatalog(root);
        Map<String, List<String>> bundles = parseBundles(root);

        // Walk the project tree so multi-module Gradle projects (where the root
        // build.gradle.kts often has zero deps and real deps live in sub-projects)
        // get their full dependency picture captured. Deduplicated by group:artifact —
        // if the same dep appears in multiple sub-projects, we keep the first seen.
        LinkedHashMap<String, MavenPomReader.MavenDependency> deduped = new LinkedHashMap<>();
        for (Path buildFile : findBuildFiles(root)) {
            String text;
            try { text = Files.readString(buildFile); }
            catch (IOException e) {
                log.debug("Failed to read {}: {}", buildFile, e.getMessage());
                continue;
            }
            extractDepsFromText(text, catalog, bundles, deduped);
        }
        return new ArrayList<>(deduped.values());
    }

    /**
     * Scans a single build-script text for all recognized dependency styles and adds
     * them to {@code sink}, keyed by {@code groupId:artifactId} so duplicates from
     * multiple build files coalesce.
     */
    private void extractDepsFromText(String text,
                                     Map<String, String> catalog,
                                     Map<String, List<String>> bundles,
                                     LinkedHashMap<String, MavenPomReader.MavenDependency> sink) {
        // 1) Literal-coordinate deps: implementation("g:a:v")
        Matcher m = RE_DEP_LITERAL.matcher(text);
        while (m.find()) {
            String[] parts = m.group(1).split(":");
            if (parts.length >= 2) addDep(sink, parts[0], parts[1],
                    parts.length >= 3 ? parts[2] : null);
        }

        // 2) Version-catalog and bundle refs: implementation(libs.x.y.z)
        if (!catalog.isEmpty() || !bundles.isEmpty()) {
            m = RE_DEP_CATALOG.matcher(text);
            while (m.find()) {
                String raw = m.group(1);
                if (raw.startsWith("bundles.")) {
                    // Bundle reference — expand into constituent libraries.
                    String bundleAlias = raw.substring("bundles.".length()).replace('.', '-');
                    List<String> members = bundles.get(bundleAlias);
                    if (members == null) {
                        log.debug("Bundle ref 'libs.bundles.{}' not found", bundleAlias);
                        continue;
                    }
                    for (String memberAlias : members) {
                        String coords = catalog.get(memberAlias);
                        if (coords == null) continue;
                        String[] parts = coords.split(":");
                        if (parts.length >= 2) addDep(sink, parts[0], parts[1],
                                parts.length >= 3 ? parts[2] : null);
                    }
                } else {
                    String alias = raw.replace('.', '-');
                    String coords = catalog.get(alias);
                    if (coords == null) {
                        log.debug("Catalog ref 'libs.{}' (alias '{}') not found",
                                raw, alias);
                        continue;
                    }
                    String[] parts = coords.split(":");
                    if (parts.length >= 2) addDep(sink, parts[0], parts[1],
                            parts.length >= 3 ? parts[2] : null);
                }
            }
        }

        // 3) Local project refs: implementation(project(":sub-module"))
        //    Emit a synthetic "LOCAL" edge so multi-module graphs are connected.
        m = RE_DEP_PROJECT.matcher(text);
        while (m.find()) {
            addDep(sink, "", m.group(1), "LOCAL");
        }
    }

    private static void addDep(LinkedHashMap<String, MavenPomReader.MavenDependency> sink,
                                String groupId, String artifactId, String version) {
        String key = groupId + ":" + artifactId;
        sink.putIfAbsent(key, new MavenPomReader.MavenDependency(
                groupId, artifactId, version, "compile"));
    }

    /**
     * Walks the project tree under {@code root} for {@code build.gradle} / {@code .kts}
     * files, skipping noise directories ({@code buildSrc}, {@code build}, {@code .git},
     * etc. — see {@link #SKIP_DIRS}).
     */
    private List<Path> findBuildFiles(Path root) throws IOException {
        List<Path> files = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                Path name = dir.getFileName();
                if (name == null) return FileVisitResult.CONTINUE;
                String n = name.toString();
                // Skip noise dirs and any dotfile dir (handles arbitrary .* folders).
                if (SKIP_DIRS.contains(n) || (n.startsWith(".") && !dir.equals(root))) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String n = file.getFileName().toString();
                if ("build.gradle".equals(n) || "build.gradle.kts".equals(n)) {
                    files.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return files;
    }

    /**
     * Parses {@code gradle/libs.versions.toml} into an alias → "group:artifact:version"
     * map. Returns an empty map if the catalog file is missing or malformed.
     *
     * <p>Supports the two standard inline forms:
     * <pre>
     *   yeast       = "fi.relex:yeast:5.14.0"
     *   spring-boot = { module = "org.springframework.boot:spring-boot-starter", version = "3.2.0" }
     *   jackson-bom = { module = "com.fasterxml.jackson:jackson-bom", version.ref = "jackson" }
     * </pre>
     * Multi-line table values, bundles, and plugins are not parsed.
     */
    Map<String, String> parseVersionCatalog(Path projectRoot) {
        Path toml = projectRoot.resolve("gradle").resolve("libs.versions.toml");
        if (!Files.exists(toml)) return Map.of();

        Map<String, String> versions  = new HashMap<>();
        Map<String, String> moduleKey = new HashMap<>(); // alias -> "g:a"
        Map<String, String> inlineVer = new HashMap<>(); // alias -> literal version
        Map<String, String> refVer    = new HashMap<>(); // alias -> version-ref name
        String section = null;

        try {
            for (String raw : Files.readAllLines(toml)) {
                // Strip line comments (TOML uses #). Leave # inside strings alone — we
                // can get away with a naive strip because aliases/coords don't contain #.
                int hash = raw.indexOf('#');
                String line = (hash >= 0 ? raw.substring(0, hash) : raw).strip();
                if (line.isEmpty()) continue;

                Matcher sec = RE_TOML_SECTION.matcher(line);
                if (sec.matches()) { section = sec.group(1); continue; }

                if ("versions".equals(section)) {
                    Matcher m = RE_TOML_KV_STRING.matcher(line);
                    if (m.matches()) versions.put(m.group(1), m.group(2));
                } else if ("libraries".equals(section)) {
                    // Inline string form: yeast = "g:a:v"
                    Matcher m = RE_TOML_KV_STRING.matcher(line);
                    if (m.matches() && m.group(2).chars().filter(c -> c == ':').count() == 2) {
                        String[] parts = m.group(2).split(":");
                        moduleKey.put(m.group(1), parts[0] + ":" + parts[1]);
                        inlineVer.put(m.group(1), parts[2]);
                        continue;
                    }
                    // Inline table form: name = { module = "g:a", version[.ref] = "..." }
                    Matcher t = RE_TOML_LIB_TABLE.matcher(line);
                    if (t.matches()) {
                        String alias = t.group(1);
                        moduleKey.put(alias, t.group(2));
                        Matcher vRef = RE_TOML_INLINE_VERSION_REF.matcher(line);
                        if (vRef.find()) {
                            refVer.put(alias, vRef.group(1));
                        } else {
                            Matcher v = RE_TOML_INLINE_VERSION.matcher(line);
                            if (v.find()) inlineVer.put(alias, v.group(1));
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.debug("Failed to read {}: {}", toml, e.getMessage());
            return Map.of();
        }

        // Resolve each alias to its final coordinate string
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, String> e : moduleKey.entrySet()) {
            String alias = e.getKey();
            String ver = inlineVer.get(alias);
            if (ver == null) {
                String refName = refVer.get(alias);
                if (refName != null) ver = versions.get(refName);
            }
            out.put(alias, e.getValue() + ":" + (ver != null ? ver : "unknown"));
        }
        log.debug("Gradle version catalog at {}: {} libraries", toml, out.size());
        return out;
    }

    /**
     * Parses the {@code [bundles]} section of {@code gradle/libs.versions.toml} into an
     * {@code alias → [library-alias…]} map. Bundles are sets of library aliases that
     * Gradle expands when you write {@code implementation(libs.bundles.foo)}.
     *
     * <p>Supports both inline-array form:
     * <pre>  test = ["junit", "mockito"]</pre>
     * and the multi-line form:
     * <pre>  core = [
     *      "spring-boot",
     *      "yeast",
     *  ]</pre>
     *
     * <p>Returns an empty map if the catalog is absent or has no {@code [bundles]} table.
     */
    Map<String, List<String>> parseBundles(Path projectRoot) {
        Path toml = projectRoot.resolve("gradle").resolve("libs.versions.toml");
        if (!Files.exists(toml)) return Map.of();

        Map<String, List<String>> bundles = new HashMap<>();
        String section = null;
        // Non-null only while we're accumulating items of a multi-line bundle array.
        String currentName = null;
        List<String> currentItems = null;

        try {
            for (String raw : Files.readAllLines(toml)) {
                int hash = raw.indexOf('#');
                String line = (hash >= 0 ? raw.substring(0, hash) : raw).strip();
                if (line.isEmpty()) continue;

                Matcher sec = RE_TOML_SECTION.matcher(line);
                if (sec.matches()) {
                    // Section change closes any in-progress multi-line bundle (malformed
                    // input but we don't want to lose what we accumulated).
                    if (currentName != null) bundles.put(currentName, currentItems);
                    section = sec.group(1);
                    currentName = null;
                    continue;
                }
                if (!"bundles".equals(section)) continue;

                if (currentName != null) {
                    // Inside a multi-line array — accumulate items until we see ']'.
                    Matcher items = RE_TOML_QUOTED_STRING.matcher(line);
                    while (items.find()) currentItems.add(items.group(1));
                    if (line.contains("]")) {
                        bundles.put(currentName, currentItems);
                        currentName = null;
                    }
                    continue;
                }

                // Try inline form first: name = ["a", "b"]
                Matcher inline = RE_TOML_BUNDLE_INLINE.matcher(line);
                if (inline.matches()) {
                    List<String> items = new ArrayList<>();
                    Matcher q = RE_TOML_QUOTED_STRING.matcher(inline.group(2));
                    while (q.find()) items.add(q.group(1));
                    bundles.put(inline.group(1), items);
                    continue;
                }
                // Multi-line start: name = [   (may already contain items after the [)
                Matcher start = RE_TOML_BUNDLE_START.matcher(line);
                if (start.matches()) {
                    currentName = start.group(1);
                    currentItems = new ArrayList<>();
                    Matcher q = RE_TOML_QUOTED_STRING.matcher(start.group(2));
                    while (q.find()) currentItems.add(q.group(1));
                }
            }
            // EOF inside an unterminated bundle — record what we got.
            if (currentName != null) bundles.put(currentName, currentItems);
        } catch (IOException e) {
            log.debug("Failed to read {} (bundles): {}", toml, e.getMessage());
            return Map.of();
        }

        return bundles;
    }
}
