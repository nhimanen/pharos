package com.pharos.parser;

import com.pharos.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Language-agnostic code parser driven by a {@link LanguageProfile}.
 *
 * <p>Extracts classes, methods, and call references using regex patterns defined in the
 * profile. No external runtime is required — this is purely a JVM operation. Quality is
 * lower than a full AST parser but far better than treating source code as plain text.
 *
 * <p>Extraction strategy (per file):
 * <ol>
 *   <li>Detect package/module name from the first matching line.</li>
 *   <li>Collect import lines.</li>
 *   <li>Find all class/struct/trait declarations (line number → name).</li>
 *   <li>Find all method/function declarations; associate each with the most recently
 *       declared class at a lower line number.</li>
 *   <li>Extract method body using the profile's {@link LanguageProfile.BodyStyle}.</li>
 *   <li>Extract call references from the body (identifier followed by {@code (}).</li>
 * </ol>
 *
 * <p>Methods that precede any class declaration are grouped into a module pseudo-class
 * named after the file stem.
 */
public class RegexCodeParser implements CodeParser {

    private static final Logger log = LoggerFactory.getLogger(RegexCodeParser.class);

    private static final int MAX_BODY_LINES = 300;
    private static final Pattern CALL_PATTERN = Pattern.compile("\\b(\\w+)\\s*\\(");
    private static final Set<String> CALL_EXCLUSIONS = Set.of(
            "if", "for", "while", "do", "switch", "case", "catch", "new", "return",
            "throw", "match", "when", "let", "fun", "fn", "def", "defp", "class",
            "struct", "enum", "type", "import", "use", "where", "with", "try",
            "yield", "await", "async", "module", "namespace", "package", "super",
            "this", "self", "print", "println", "printf", "sprintf", "require"
    );

    private final LanguageProfile profile;

    public RegexCodeParser(LanguageProfile profile) {
        this.profile = profile;
    }

    @Override
    public List<String> supportedExtensions() {
        return profile.extensions();
    }

    // -------------------------------------------------------------------------
    // CodeParser implementation
    // -------------------------------------------------------------------------

    @Override
    public ParsedFile parseFile(Path file, String projectName) throws IOException {
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // Fall back to Latin-1 if UTF-8 decode fails
            lines = Files.readAllLines(file, StandardCharsets.ISO_8859_1);
        }
        return parseLines(lines, file.toAbsolutePath(), projectName);
    }

    @Override
    public ParsedProject parseProject(Path projectRoot, String projectName) throws IOException {
        log.info("Indexing {} project '{}' from {}", profile.language(), projectName, projectRoot);
        Set<String> exts = new HashSet<>(profile.extensions());
        List<Path> files = new ArrayList<>();

        Files.walkFileTree(projectRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String name = dir.getFileName() != null ? dir.getFileName().toString() : "";
                // Skip common noise directories
                if (name.startsWith(".") || name.equals("target") || name.equals("build")
                        || name.equals("node_modules") || name.equals("vendor")
                        || name.equals("dist") || name.equals("out")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                if (exts.stream().anyMatch(name::endsWith)) {
                    files.add(file);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }
        });

        List<ParsedFile> parsed = new ArrayList<>();
        for (Path f : files) {
            try {
                parsed.add(parseFile(f, projectName));
            } catch (Exception e) {
                log.debug("Failed to parse {}: {}", f, e.getMessage());
            }
        }
        log.info("Parsed {} {} file(s) in project '{}'",
                parsed.size(), profile.language(), projectName);
        return new ParsedProject(projectName, projectRoot.toString(), parsed);
    }

    // -------------------------------------------------------------------------
    // Per-file parsing
    // -------------------------------------------------------------------------

    private ParsedFile parseLines(List<String> lines, Path filePath, String projectName) {
        String absPath = filePath.toAbsolutePath().toString();

        String packageName = detectPackage(lines);
        List<String> imports = collectImports(lines);

        // --- Pass 1: collect class declarations ---
        record ClassDecl(int line, ParsedClass cls) {}
        List<ClassDecl> classDecls = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            String name = matchClassName(lines.get(i));
            if (name != null) {
                String qualified = packageName.isEmpty() ? name : packageName + "." + name;
                classDecls.add(new ClassDecl(i, new ParsedClass(
                        projectName, packageName, name, qualified,
                        "class", null, List.of(), List.of(),
                        "public", false, false, null, absPath, i + 1, i + 1)));
            }
        }

        // Module pseudo-class for top-level functions (populated lazily)
        String stem = fileStem(filePath);
        String moduleQualified = packageName.isEmpty() ? stem : packageName + "." + stem;
        ParsedClass modulePseudoClass = new ParsedClass(
                projectName, packageName, stem, moduleQualified,
                "module", null, List.of(), List.of(),
                "public", false, false, null, absPath, 1, 1);

        // --- Pass 2: collect method declarations ---
        List<ParsedMethod> methods = new ArrayList<>();
        boolean modulePseudoClassUsed = false;

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            Matcher m = profile.methodPattern().matcher(line);
            if (!m.find()) continue;

            String methodName = firstNonNull(m, 1, 2);
            if (methodName == null || isKeyword(methodName)) continue;

            String rawParams = groupSafe(m, profile.methodPattern().pattern().contains("group2") ? 2
                    : m.groupCount() >= 2 ? 2 : -1);

            // Determine enclosing class: last class declared at or before this line
            ParsedClass containingClass = modulePseudoClass;
            for (ClassDecl cd : classDecls) {
                if (cd.line() <= i) containingClass = cd.cls();
                else break;
            }
            if (containingClass == modulePseudoClass) {
                modulePseudoClassUsed = true;
            }

            String body = extractBody(lines, i);
            List<String> params = parseParams(rawParams);
            List<CallReference> calls = extractCalls(body,
                    containingClass.qualifiedClassName() + "#" + methodName + "()");

            String id = ParsedMethod.buildId(projectName,
                    containingClass.qualifiedClassName(), methodName, List.of());
            String sig = methodName + "(" + String.join(", ", params) + ")";

            methods.add(new ParsedMethod(
                    id, projectName,
                    containingClass.packageName(),
                    containingClass.className(),
                    containingClass.qualifiedClassName(),
                    methodName, sig, "object",
                    params, params,
                    body.length() > 4000 ? body.substring(0, 4000) : body,
                    null, List.of(), "public",
                    false, false, false, false,
                    List.of(), calls,
                    absPath, i + 1, endLine(lines, i, body)));
        }

        // Assemble class list (only include module pseudo-class if it has methods)
        List<ParsedClass> classes = new ArrayList<>();
        if (modulePseudoClassUsed) classes.add(modulePseudoClass);
        classDecls.forEach(cd -> classes.add(cd.cls()));

        return new ParsedFile(absPath, packageName, imports, classes, methods);
    }

    // -------------------------------------------------------------------------
    // Package and import extraction
    // -------------------------------------------------------------------------

    private String detectPackage(List<String> lines) {
        if (profile.packagePattern() == null) return "";
        for (String line : lines) {
            Matcher m = profile.packagePattern().matcher(line);
            if (m.find()) return m.group(1).trim();
        }
        return "";
    }

    private List<String> collectImports(List<String> lines) {
        if (profile.importPattern() == null) return List.of();
        List<String> imports = new ArrayList<>();
        for (String line : lines) {
            Matcher m = profile.importPattern().matcher(line);
            if (m.find()) imports.add(m.group(1).trim());
        }
        return imports;
    }

    // -------------------------------------------------------------------------
    // Class name matching
    // -------------------------------------------------------------------------

    private String matchClassName(String line) {
        Matcher m = profile.classPattern().matcher(line);
        if (!m.find()) return null;
        // Try group 1 then group 2 (some patterns have two alternatives)
        return firstNonNull(m, 1, 2);
    }

    // -------------------------------------------------------------------------
    // Body extraction
    // -------------------------------------------------------------------------

    private String extractBody(List<String> lines, int startLine) {
        return switch (profile.bodyStyle()) {
            case BRACES  -> extractBracedBody(lines, startLine);
            case DO_END  -> extractDoEndBody(lines, startLine);
            case PARENS  -> extractParenBody(lines, startLine);
            case INDENT  -> extractIndentBody(lines, startLine);
        };
    }

    private String extractBracedBody(List<String> lines, int startLine) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        boolean opened = false;
        for (int i = startLine; i < Math.min(lines.size(), startLine + MAX_BODY_LINES); i++) {
            String line = lines.get(i);
            sb.append(line).append('\n');
            for (char c : line.toCharArray()) {
                if (c == '{') { depth++; opened = true; }
                else if (c == '}') depth--;
            }
            if (opened && depth == 0) break;
        }
        return sb.toString();
    }

    private String extractDoEndBody(List<String> lines, int startLine) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        boolean opened = false;
        Pattern doPattern  = Pattern.compile("\\bdo\\b");
        Pattern endPattern = Pattern.compile("^\\s*end\\b");
        for (int i = startLine; i < Math.min(lines.size(), startLine + MAX_BODY_LINES); i++) {
            String line = lines.get(i);
            sb.append(line).append('\n');
            String trimmed = line.trim();
            // Skip comment lines
            if (trimmed.startsWith(profile.lineCommentPrefix())) continue;
            if (doPattern.matcher(line).find()) { depth++; opened = true; }
            if (endPattern.matcher(line).find() && opened) {
                depth--;
                if (depth <= 0) break;
            }
        }
        return sb.toString();
    }

    private String extractParenBody(List<String> lines, int startLine) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        boolean opened = false;
        for (int i = startLine; i < Math.min(lines.size(), startLine + MAX_BODY_LINES); i++) {
            String line = lines.get(i);
            sb.append(line).append('\n');
            for (char c : line.toCharArray()) {
                if (c == '(') { depth++; opened = true; }
                else if (c == ')') depth--;
            }
            if (opened && depth == 0) break;
        }
        return sb.toString();
    }

    private String extractIndentBody(List<String> lines, int startLine) {
        StringBuilder sb = new StringBuilder();
        int baseIndent = indentation(lines.get(startLine));
        sb.append(lines.get(startLine)).append('\n');
        for (int i = startLine + 1; i < Math.min(lines.size(), startLine + MAX_BODY_LINES); i++) {
            String line = lines.get(i);
            if (line.isBlank()) { sb.append('\n'); continue; }
            if (indentation(line) <= baseIndent) break;
            sb.append(line).append('\n');
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // Call extraction
    // -------------------------------------------------------------------------

    private List<CallReference> extractCalls(String body, String callerFqn) {
        Set<String> seen = new LinkedHashSet<>();
        Matcher m = CALL_PATTERN.matcher(body);
        while (m.find()) {
            String name = m.group(1);
            if (!CALL_EXCLUSIONS.contains(name) && !name.equals(name.toUpperCase())
                    && name.length() > 1) {
                seen.add(name);
            }
        }
        return seen.stream()
                .map(name -> CallReference.unresolved(callerFqn, name, null, 0, 0))
                .collect(Collectors.toList());
    }

    // -------------------------------------------------------------------------
    // Parameter parsing
    // -------------------------------------------------------------------------

    private static List<String> parseParams(String rawParams) {
        if (rawParams == null || rawParams.isBlank()) return List.of();
        // Split on top-level commas
        List<String> result = new ArrayList<>();
        int depth = 0;
        StringBuilder cur = new StringBuilder();
        for (char c : rawParams.toCharArray()) {
            if (c == '(' || c == '[' || c == '<') depth++;
            else if (c == ')' || c == ']' || c == '>') depth--;
            else if (c == ',' && depth == 0) {
                String param = extractParamName(cur.toString().trim());
                if (!param.isEmpty()) result.add(param);
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        String last = extractParamName(cur.toString().trim());
        if (!last.isEmpty()) result.add(last);
        return result;
    }

    /**
     * Extracts the parameter name from a potentially type-annotated parameter.
     * E.g. "String name" → "name", "name: String" → "name", "&self" → "self", "x" → "x".
     */
    private static String extractParamName(String param) {
        if (param.isEmpty()) return "";
        // Remove leading & (Rust)
        param = param.replaceAll("^[&*]+", "").trim();
        // Kotlin/Swift style: "name: Type" → take part before ':'
        if (param.contains(":")) return param.substring(0, param.indexOf(':')).trim();
        // Java/C# style: "Type name" → take last word
        String[] parts = param.split("\\s+");
        return parts[parts.length - 1].replaceAll("[^\\w]", "");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String fileStem(Path path) {
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    private static int indentation(String line) {
        int count = 0;
        for (char c : line.toCharArray()) {
            if (c == ' ') count++;
            else if (c == '\t') count += 4;
            else break;
        }
        return count;
    }

    /** Returns group {@code g1} if non-null/non-empty, else group {@code g2}, else null. */
    private static String firstNonNull(Matcher m, int g1, int g2) {
        try {
            String v = m.group(g1);
            if (v != null && !v.isBlank()) return v.trim();
        } catch (IndexOutOfBoundsException ignored) {}
        try {
            String v = m.group(g2);
            if (v != null && !v.isBlank()) return v.trim();
        } catch (IndexOutOfBoundsException ignored) {}
        return null;
    }

    /** Returns the string captured by group {@code g}, or null if the group doesn't exist. */
    private static String groupSafe(Matcher m, int g) {
        if (g < 1 || g > m.groupCount()) return null;
        try { return m.group(g); } catch (IndexOutOfBoundsException e) { return null; }
    }

    private static boolean isKeyword(String name) {
        return CALL_EXCLUSIONS.contains(name);
    }

    /** Approximate end line based on body line count. */
    private static int endLine(List<String> lines, int startLine, String body) {
        long bodyLines = body.chars().filter(c -> c == '\n').count();
        return (int) Math.min(startLine + 1 + bodyLines, lines.size());
    }
}
