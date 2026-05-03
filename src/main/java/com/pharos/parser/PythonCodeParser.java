package com.pharos.parser;

import com.pharos.parser.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Python source code parser using Python's built-in {@code ast} module.
 *
 * Spawns {@code python3 python-extractor.py} as a subprocess, which walks the
 * source tree and emits a JSON array describing every class and function found.
 * The extractor script is bundled as a classpath resource and extracted to a
 * temp file on first use (cached for the JVM lifetime).
 *
 * Call references are all unresolved — Python's dynamic dispatch cannot be
 * statically resolved, mirroring how external Java library calls are handled.
 *
 * FQN format mirrors Java:
 *   top-level function in {@code utils/helpers.py} → {@code utils.helpers#my_func()}
 *   method on class in same file → {@code utils.helpers.MyClass#my_method()}
 */
public class PythonCodeParser implements CodeParser {

    private static final Logger log = LoggerFactory.getLogger(PythonCodeParser.class);
    private static final List<String> EXTENSIONS = List.of(".py");
    private static final int SUBPROCESS_TIMEOUT_SECONDS = 120;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Cached path to the extracted extractor script (written once per JVM run). */
    private static volatile Path extractorScript;

    @Override
    public List<String> supportedExtensions() {
        return EXTENSIONS;
    }

    @Override
    public ParsedFile parseFile(Path file, String projectName) throws IOException {
        Path root = file.getParent() != null ? file.getParent() : file;
        String json = runExtractor("--file", file.toAbsolutePath().toString());
        if (json == null) return emptyFile(file);
        List<ParsedFile> files = mapJson(json, projectName, root);
        return files.isEmpty() ? emptyFile(file) : files.get(0);
    }

    @Override
    public ParsedProject parseProject(Path projectRoot, String projectName) throws IOException {
        log.info("Indexing Python project '{}' from {}", projectName, projectRoot);
        String json = runExtractor("--root", projectRoot.toAbsolutePath().toString());
        if (json == null) {
            log.warn("python3 extractor returned no output for '{}' — skipping", projectName);
            return new ParsedProject(projectName, projectRoot.toString(), List.of());
        }
        List<ParsedFile> files = mapJson(json, projectName, projectRoot);
        log.info("Parsed {} Python file(s) in project '{}'", files.size(), projectName);
        return new ParsedProject(projectName, projectRoot.toString(), files);
    }

    // -------------------------------------------------------------------------
    // JSON → model mapping
    // -------------------------------------------------------------------------

    private List<ParsedFile> mapJson(String json, String projectName, Path root) throws IOException {
        JsonNode array;
        try {
            array = MAPPER.readTree(json);
        } catch (Exception e) {
            log.warn("Failed to parse extractor JSON: {}", e.getMessage());
            return List.of();
        }
        if (!array.isArray()) return List.of();

        List<ParsedFile> results = new ArrayList<>();
        for (JsonNode fileNode : array) {
            results.add(mapFile(fileNode, projectName));
        }
        return results;
    }

    private ParsedFile mapFile(JsonNode fileNode, String projectName) {
        String filePath = fileNode.path("file").asText();
        String pkg      = fileNode.path("package").asText("");

        List<ParsedClass> classes   = new ArrayList<>();
        List<ParsedMethod> methods  = new ArrayList<>();

        // --- Explicit class declarations ---
        for (JsonNode cls : fileNode.path("classes")) {
            classes.add(mapClass(cls, projectName, pkg, filePath));
        }

        // Module pseudo-class: created for any top-level function (class_name == null)
        // Use the last segment of the package as a module pseudo-class name.
        String moduleName = pkg.contains(".") ? pkg.substring(pkg.lastIndexOf('.') + 1) : pkg;
        if (moduleName.isEmpty()) moduleName = "module";
        String moduleQualified = pkg.isEmpty() ? moduleName : pkg;

        boolean hasTopLevelFunctions = false;
        for (JsonNode fn : fileNode.path("functions")) {
            if (fn.path("class_name").isNull() || fn.path("class_name").isMissingNode()) {
                hasTopLevelFunctions = true;
                break;
            }
        }

        ParsedClass modulePseudoClass = null;
        if (hasTopLevelFunctions) {
            modulePseudoClass = new ParsedClass(
                    projectName, pkg, moduleName, moduleQualified,
                    "module", null, List.of(), List.of(),
                    "public", false, false, null, filePath, 1, 1);
            classes.add(modulePseudoClass);
        }

        // --- Functions / methods ---
        for (JsonNode fn : fileNode.path("functions")) {
            String className = fn.path("class_name").isNull() ? null : fn.path("class_name").asText(null);

            ParsedClass containingClass;
            if (className == null) {
                containingClass = modulePseudoClass;
            } else {
                // Find the declared class
                final String cn = className;
                containingClass = classes.stream()
                        .filter(c -> c.className().equals(cn))
                        .findFirst()
                        .orElseGet(() -> {
                            String q = pkg.isEmpty() ? cn : pkg + "." + cn;
                            return new ParsedClass(projectName, pkg, cn, q,
                                    "class", null, List.of(), List.of(),
                                    "public", false, false, null, filePath, 0, 0);
                        });
            }

            if (containingClass == null) continue;
            methods.add(mapMethod(fn, projectName, filePath, containingClass));
        }

        return new ParsedFile(filePath, pkg, List.of(), classes, methods);
    }

    private ParsedClass mapClass(JsonNode cls, String projectName, String pkg, String filePath) {
        String name      = cls.path("name").asText();
        String qualified = cls.path("qualified").asText(pkg.isEmpty() ? name : pkg + "." + name);
        List<String> bases = jsonStringList(cls.path("bases"));
        List<String> decorators = jsonStringList(cls.path("decorators"));
        String docstring = cls.path("docstring").isNull() ? null : cls.path("docstring").asText(null);
        int start = cls.path("start").asInt(0);
        int end   = cls.path("end").asInt(0);

        return new ParsedClass(
                projectName, pkg, name, qualified,
                "class",
                bases.isEmpty() ? null : bases.get(0),  // superclass = first base
                bases.size() > 1 ? bases.subList(1, bases.size()) : List.of(), // rest = interfaces
                decorators,
                "public", false, false,
                docstring, filePath, start, end);
    }

    private ParsedMethod mapMethod(JsonNode fn, String projectName, String filePath,
                                    ParsedClass containingClass) {
        String name         = fn.path("name").asText();
        boolean isCtor      = fn.path("is_constructor").asBoolean(false);
        List<String> params = jsonStringList(fn.path("params"));
        List<String> decs   = jsonStringList(fn.path("decorators"));
        String docstring    = fn.path("docstring").isNull() ? null : fn.path("docstring").asText(null);
        String body         = fn.path("body").asText("");
        int start           = fn.path("start").asInt(0);
        int end             = fn.path("end").asInt(0);

        // Remove "self" / "cls" from displayed params
        List<String> paramTypes = params.stream()
                .filter(p -> !p.equals("self") && !p.equals("cls"))
                .toList();

        // Python: empty param list in FQN (dynamically typed)
        String id = ParsedMethod.buildId(projectName, containingClass.qualifiedClassName(), name, List.of());
        String callerFqn = containingClass.qualifiedClassName() + "#" + name + "()";

        // Build signature string
        String allParams = String.join(", ", params);
        String sig = (decs.isEmpty() ? "" : "@" + String.join(" @", decs) + " ")
                + "def " + name + "(" + allParams + ")";

        // Calls → all unresolved (Python dynamic dispatch)
        List<CallReference> calls = new ArrayList<>();
        for (JsonNode callNode : fn.path("calls")) {
            calls.add(CallReference.unresolved(callerFqn, callNode.asText(), start));
        }

        return new ParsedMethod(
                id, projectName,
                containingClass.packageName(),
                containingClass.className(),
                containingClass.qualifiedClassName(),
                name, sig,
                isCtor ? containingClass.className() : "object", // return type
                paramTypes, paramTypes,  // param types = param names (no type info)
                body, docstring, decs,
                "public",
                decs.contains("staticmethod"), isCtor, false, false,
                List.of(), calls,
                filePath, start, end);
    }

    // -------------------------------------------------------------------------
    // Subprocess invocation
    // -------------------------------------------------------------------------

    /**
     * Runs the extractor with the given arguments and returns stdout as a String.
     * Returns null on failure (python3 not found, non-zero exit, timeout).
     */
    private String runExtractor(String... extraArgs) {
        Path script = getExtractorScript();
        if (script == null) return null;

        List<String> cmd = new ArrayList<>();
        cmd.add("python3");
        cmd.add(script.toAbsolutePath().toString());
        cmd.addAll(List.of(extraArgs));

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(false);
            Process proc = pb.start();

            // Read stdout in a background thread to avoid blocking
            final StringBuilder out = new StringBuilder();
            Thread reader = new Thread(() -> {
                try (var s = proc.getInputStream()) {
                    out.append(new String(s.readAllBytes()));
                } catch (IOException e) { /* ignore */ }
            });
            reader.start();

            boolean finished = proc.waitFor(SUBPROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            reader.join(5_000);

            if (!finished) {
                proc.destroyForcibly();
                log.warn("python-extractor timed out after {}s", SUBPROCESS_TIMEOUT_SECONDS);
                return null;
            }
            if (proc.exitValue() != 0) {
                try (var err = proc.getErrorStream()) {
                    log.warn("python-extractor exited {} — stderr: {}",
                            proc.exitValue(), new String(err.readAllBytes()).trim());
                } catch (IOException ignored) {}
                return null;
            }

            return out.toString();
        } catch (IOException e) {
            log.warn("Could not launch python3: {} — Python indexing skipped", e.getMessage());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Extracts {@code python-extractor.py} from the classpath to a temp file and
     * returns its path. Result is cached — the file is only written once per JVM run.
     */
    static Path getExtractorScript() {
        if (extractorScript != null) return extractorScript;
        synchronized (PythonCodeParser.class) {
            if (extractorScript != null) return extractorScript;
            try (InputStream in = PythonCodeParser.class.getClassLoader()
                    .getResourceAsStream("python-extractor.py")) {
                if (in == null) {
                    LoggerFactory.getLogger(PythonCodeParser.class)
                            .error("python-extractor.py not found on classpath");
                    return null;
                }
                Path tmp = Files.createTempFile("pharos-python-extractor-", ".py");
                tmp.toFile().deleteOnExit();
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                extractorScript = tmp;
                return tmp;
            } catch (IOException e) {
                LoggerFactory.getLogger(PythonCodeParser.class)
                        .error("Failed to extract python-extractor.py: {}", e.getMessage());
                return null;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Source root detection
    // -------------------------------------------------------------------------

    /**
     * Detects a Python project's source root.
     * Checks {@code src/} first (common layout), then falls back to projectRoot.
     */
    public static Path detectSourceRoot(Path projectRoot) {
        Path src = projectRoot.resolve("src");
        if (Files.isDirectory(src)) return src;
        return projectRoot;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static List<String> jsonStringList(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) return List.of();
        List<String> result = new ArrayList<>();
        for (JsonNode item : node) result.add(item.asText());
        return result;
    }

    private static ParsedFile emptyFile(Path file) {
        return new ParsedFile(file.toAbsolutePath().toString(), "", List.of(), List.of(), List.of());
    }
}
