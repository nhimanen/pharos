package com.pharos.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base for language parsers that delegate analysis to an external script
 * (e.g. Python's {@code ast} module, Node.js's parser APIs).
 *
 * <p>The script is bundled as a classpath resource, extracted to a temp file on
 * first use, and invoked as a subprocess with {@code --root <dir>} or
 * {@code --file <path>}. The script must emit a JSON array to stdout using the
 * shared schema:
 * <pre>
 * [
 *   {
 *     "file": "/abs/path/source.ext",
 *     "package": "pkg.sub",
 *     "classes": [ { "name":"Foo","qualified":"pkg.Foo","bases":[],"decorators":[],"docstring":null,"start":1,"end":50 } ],
 *     "functions": [ { "name":"bar","class_name":"Foo","params":["x"],"decorators":[],"docstring":null,
 *                      "body":"...","calls":["helper"],"start":5,"end":20,"is_constructor":false } ]
 *   }
 * ]
 * </pre>
 *
 * <p>Subclasses must implement {@link #scriptResourceName()}, {@link #runtimeCommand()},
 * and {@link #supportedExtensions()}. They may override the protected hook methods to
 * customize signature formatting and parameter filtering.
 */
public abstract class ScriptBasedCodeParser implements CodeParser {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int SUBPROCESS_TIMEOUT_SECONDS = 120;

    /** One cached temp-file path per classpath resource name. */
    private static final Map<String, Path> SCRIPT_CACHE = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Abstract contract
    // -------------------------------------------------------------------------

    /** Classpath resource name of the extractor script (e.g. {@code "python-extractor.py"}). */
    protected abstract String scriptResourceName();

    /**
     * Command used to invoke the script, excluding the script path itself.
     * E.g. {@code List.of("python3")} or {@code List.of("node")}.
     */
    protected abstract List<String> runtimeCommand();

    // -------------------------------------------------------------------------
    // Overridable hooks
    // -------------------------------------------------------------------------

    /**
     * Filters the raw parameter list before building the signature and FQN.
     * Default: return as-is. Python overrides to strip "self"/"cls".
     */
    protected List<String> filterParams(List<String> params) {
        return params;
    }

    /**
     * Builds the human-readable signature string shown in search results.
     *
     * @param name       method/function name
     * @param allParams  raw parameter list (including receiver if any)
     * @param decorators decorator/annotation names
     */
    protected String buildSignatureString(String name, List<String> allParams, List<String> decorators) {
        return (decorators.isEmpty() ? "" : "@" + String.join(" @", decorators) + " ")
                + name + "(" + String.join(", ", allParams) + ")";
    }

    /** Return type string used for non-constructor methods. Default: "object". */
    protected String defaultReturnType() {
        return "object";
    }

    // -------------------------------------------------------------------------
    // CodeParser implementation
    // -------------------------------------------------------------------------

    @Override
    public ParsedFile parseFile(Path file, String projectName) throws IOException {
        String json = runExtractor("--file", file.toAbsolutePath().toString());
        if (json == null) return emptyFile(file);
        List<ParsedFile> files = mapJson(json, projectName);
        return files.isEmpty() ? emptyFile(file) : files.get(0);
    }

    @Override
    public ParsedProject parseProject(Path projectRoot, String projectName) throws IOException {
        log.info("Indexing {} project '{}' from {}", getClass().getSimpleName(), projectName, projectRoot);
        String json = runExtractor("--root", projectRoot.toAbsolutePath().toString());
        if (json == null) {
            log.warn("{} extractor returned no output for '{}' — skipping",
                    scriptResourceName(), projectName);
            return new ParsedProject(projectName, projectRoot.toString(), List.of());
        }
        List<ParsedFile> files = mapJson(json, projectName);
        log.info("Parsed {} file(s) in project '{}'", files.size(), projectName);
        return new ParsedProject(projectName, projectRoot.toString(), files);
    }

    // -------------------------------------------------------------------------
    // JSON → model mapping
    // -------------------------------------------------------------------------

    private List<ParsedFile> mapJson(String json, String projectName) throws IOException {
        JsonNode array;
        try {
            array = MAPPER.readTree(json);
        } catch (Exception e) {
            log.warn("Failed to parse extractor JSON from {}: {}", scriptResourceName(), e.getMessage());
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

        List<ParsedClass>  classes = new ArrayList<>();
        List<ParsedMethod> methods = new ArrayList<>();

        // Explicit class declarations
        for (JsonNode cls : fileNode.path("classes")) {
            classes.add(mapClass(cls, projectName, pkg, filePath));
        }

        // Module pseudo-class for top-level functions
        boolean hasTopLevel = false;
        for (JsonNode fn : fileNode.path("functions")) {
            if (fn.path("class_name").isNull() || fn.path("class_name").isMissingNode()) {
                hasTopLevel = true;
                break;
            }
        }

        String moduleName = pkg.contains(".") ? pkg.substring(pkg.lastIndexOf('.') + 1) : pkg;
        if (moduleName.isEmpty()) moduleName = "module";
        String moduleQualified = pkg.isEmpty() ? moduleName : pkg;

        ParsedClass modulePseudoClass = null;
        if (hasTopLevel) {
            modulePseudoClass = new ParsedClass(
                    projectName, pkg, moduleName, moduleQualified,
                    "module", null, List.of(), List.of(),
                    "public", false, false, null, filePath, 1, 1);
            classes.add(modulePseudoClass);
        }

        for (JsonNode fn : fileNode.path("functions")) {
            String className = fn.path("class_name").isNull() ? null
                    : fn.path("class_name").asText(null);

            ParsedClass containingClass;
            if (className == null) {
                containingClass = modulePseudoClass;
            } else {
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
        List<String> bases      = jsonStringList(cls.path("bases"));
        List<String> decorators = jsonStringList(cls.path("decorators"));
        String docstring = cls.path("docstring").isNull() ? null : cls.path("docstring").asText(null);
        int start = cls.path("start").asInt(0);
        int end   = cls.path("end").asInt(0);

        return new ParsedClass(
                projectName, pkg, name, qualified,
                "class",
                bases.isEmpty() ? null : bases.get(0),
                bases.size() > 1 ? bases.subList(1, bases.size()) : List.of(),
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

        List<String> displayParams = filterParams(params);
        String id        = ParsedMethod.buildId(projectName, containingClass.qualifiedClassName(),
                name, List.of());
        String callerFqn = containingClass.qualifiedClassName() + "#" + name + "()";
        String sig       = buildSignatureString(name, params, decs);
        String returnType = isCtor ? containingClass.className() : defaultReturnType();

        List<CallReference> calls = new ArrayList<>();
        for (JsonNode callNode : fn.path("calls")) {
            calls.add(CallReference.unresolved(callerFqn, callNode.asText(), null, 0, start));
        }

        return new ParsedMethod(
                id, projectName,
                containingClass.packageName(),
                containingClass.className(),
                containingClass.qualifiedClassName(),
                name, sig, returnType,
                displayParams, displayParams,
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
     * Returns null on failure (runtime not found, non-zero exit, timeout).
     */
    protected String runExtractor(String... extraArgs) {
        Path script = resolveScript();
        if (script == null) return null;

        List<String> cmd = new ArrayList<>(runtimeCommand());
        cmd.add(script.toAbsolutePath().toString());
        cmd.addAll(List.of(extraArgs));

        try {
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(false);
            Process proc = pb.start();

            final StringBuilder out = new StringBuilder();
            Thread reader = new Thread(() -> {
                try (var s = proc.getInputStream()) {
                    out.append(new String(s.readAllBytes()));
                } catch (IOException e) { /* ignore */ }
            });
            reader.setDaemon(true);
            reader.start();

            boolean finished = proc.waitFor(SUBPROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            reader.join(5_000);

            if (!finished) {
                proc.destroyForcibly();
                log.warn("{} extractor timed out after {}s", scriptResourceName(), SUBPROCESS_TIMEOUT_SECONDS);
                return null;
            }
            if (proc.exitValue() != 0) {
                try (var err = proc.getErrorStream()) {
                    log.warn("{} extractor exited {} — stderr: {}",
                            scriptResourceName(), proc.exitValue(),
                            new String(err.readAllBytes()).trim());
                } catch (IOException ignored) {}
                return null;
            }
            return out.toString();
        } catch (IOException e) {
            log.warn("Could not launch {}: {} — {} indexing skipped",
                    runtimeCommand().get(0), e.getMessage(), scriptResourceName());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Extracts the script from the classpath to a temp file (cached per resource name).
     * Returns null if the resource is missing or extraction fails.
     */
    protected Path resolveScript() {
        String resourceName = scriptResourceName();
        Path cached = SCRIPT_CACHE.get(resourceName);
        if (cached != null) return cached;

        synchronized (SCRIPT_CACHE) {
            cached = SCRIPT_CACHE.get(resourceName);
            if (cached != null) return cached;
            try (InputStream in = getClass().getClassLoader().getResourceAsStream(resourceName)) {
                if (in == null) {
                    log.error("{} not found on classpath", resourceName);
                    return null;
                }
                String suffix = resourceName.contains(".") ?
                        resourceName.substring(resourceName.lastIndexOf('.')) : ".tmp";
                Path tmp = Files.createTempFile("pharos-extractor-", suffix);
                tmp.toFile().deleteOnExit();
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                SCRIPT_CACHE.put(resourceName, tmp);
                return tmp;
            } catch (IOException e) {
                log.error("Failed to extract {}: {}", resourceName, e.getMessage());
                return null;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    protected static List<String> jsonStringList(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) return List.of();
        List<String> result = new ArrayList<>();
        for (JsonNode item : node) result.add(item.asText());
        return result;
    }

    protected static ParsedFile emptyFile(Path file) {
        return new ParsedFile(file.toAbsolutePath().toString(), "", List.of(), List.of(), List.of());
    }
}
