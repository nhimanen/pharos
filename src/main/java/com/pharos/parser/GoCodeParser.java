package com.pharos.parser;

import com.pharos.parser.model.CallReference;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedMethod;
import com.pharos.parser.model.ParsedProject;
import com.pharos.parser.model.ParsedRelationships;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Go source parser implemented entirely in Java — no {@code go} toolchain required.
 *
 * <p>Unlike {@link PythonCodeParser} / {@link JsCodeParser} / {@link TerraformCodeParser},
 * which shell out to an interpreter, this parser needs no runtime on PATH. That matters
 * because a Go extractor would need the (compiled) Go toolchain, which is frequently
 * absent on indexing hosts and in CI — and {@link ScriptBasedCodeParser} degrades to an
 * <em>empty</em> result when its runtime is missing, so Go files would be silently dropped.
 *
 * <h2>Why not {@link RegexCodeParser}?</h2>
 * The generic regex tier attaches each function to "the most recently declared type above
 * it", which is wrong for Go: methods are bound by their <em>receiver</em>
 * ({@code func (s *Server) Handle(...)}), and a package's types and methods are routinely
 * split across files. The regex tier also emits only unresolved call references, and
 * {@link com.pharos.graph.CallGraphBuilder} turns <em>only resolved</em> references into
 * edges — so it produces no call graph at all.
 *
 * <h2>Call graph resolution</h2>
 * Go has no function overloading, so a method's identity is exactly
 * {@code (receiver type, name)} and a package function's is {@code (package, name)}.
 * Parameter types are therefore omitted from {@link ParsedMethod#fqn()} (paramTypes is
 * empty), which makes name-based resolution <em>exact</em> rather than a heuristic. Full
 * type information is preserved in {@link ParsedMethod#signature()} (indexed at 2x boost)
 * and in {@link ParsedMethod#paramNames()}.
 *
 * <p>Four resolution rules produce resolved edges, all of them exact:
 * <ol>
 *   <li>{@code s.doWork()} where {@code s} is the enclosing method's receiver →
 *       the receiver type's method.</li>
 *   <li>{@code helper()} → a package-level function in the same Go package.</li>
 *   <li>{@code store.Get()} where {@code store} is an imported package belonging to this
 *       project → that package's function.</li>
 *   <li>{@code v := &Server{}} / {@code v := NewServer()} then {@code v.Handle()} →
 *       the inferred type's method.</li>
 * </ol>
 * Everything else stays unresolved for {@code CrossProjectLinker} to pick up later; no
 * guessed edges are emitted.
 *
 * <h2>Structure mapping</h2>
 * <ul>
 *   <li>{@code packageName} is the Go <em>import path</em> ({@code github.com/acme/api/store}),
 *       derived from the enclosing {@code go.mod} via {@link GoModReader}. Without a
 *       {@code go.mod} it falls back to the {@code package} clause.</li>
 *   <li>{@code type X struct}/{@code interface}/alias → {@link ParsedClass}
 *       (kind {@code class} / {@code interface}).</li>
 *   <li>Embedded types → {@code superclass} (first embedded struct) and {@code interfaces}
 *       (the rest), feeding inherits/implements relationships.</li>
 *   <li>Struct fields → {@link ParsedRelationships.FieldDecl}.</li>
 *   <li>Interface method sets → abstract {@link ParsedMethod}s on the interface.</li>
 *   <li>Package-level functions → a per-file {@code module} pseudo-class. Per-file rather
 *       than per-package because class documents are keyed
 *       {@code project:qualifiedClassName} and would otherwise collide across a package's
 *       files.</li>
 *   <li>Exported (capitalised) identifiers → {@code public}, unexported → {@code private}.</li>
 * </ul>
 *
 * <p>Comments and string literals (including backtick raw strings and rune literals) are
 * masked before any brace matching, so braces inside strings, struct tags, and comments
 * cannot corrupt body extents.
 */
public class GoCodeParser implements CodeParser {

    private static final Logger log = LoggerFactory.getLogger(GoCodeParser.class);

    private static final List<String> EXTENSIONS = List.of(".go");

    /** Body text stored per method, matching the cap used by the other parsers. */
    private static final int MAX_BODY_CHARS = 4000;

    /** Directories never walked: toolchain-ignored, vendored, or build output. */
    private static final Set<String> SKIP_DIRS = Set.of(
            "vendor", "testdata", "node_modules", "target", "build", "dist", "out");

    // --- declaration patterns (matched against the literal-masked view) ---
    private static final Pattern RE_PACKAGE = Pattern.compile("^\\s*package\\s+(\\w+)");
    private static final Pattern RE_GROUP_OPEN =
            Pattern.compile("^\\s*(type|import|var|const)\\s*\\($");
    /** An import spec: optional alias or {@code .}/{@code _}, then a quoted path. */
    private static final Pattern RE_IMPORT_SPEC =
            Pattern.compile("^\\s*(?:(\\w+|\\.|_)\\s+)?\"([^\"]+)\"");
    /** A call site: optional single-identifier receiver, then the callee. */
    private static final Pattern RE_CALL =
            Pattern.compile("(?:(\\w+)\\s*\\.\\s*)?\\b(\\w+)\\s*\\(");
    /** {@code v := &Type{} } / {@code v := Type{} } / {@code v := pkg.NewType(...)}. */
    private static final Pattern RE_LOCAL_BIND = Pattern.compile(
            "\\b(\\w+)\\s*:=\\s*(?:&\\s*)?(?:(\\w+)\\s*\\{|(?:\\w+\\s*\\.\\s*)?New(\\w+)\\s*\\()");

    /** Go keywords that can be followed by {@code (} but are never calls. */
    private static final Set<String> NOT_CALLS = Set.of(
            "if", "for", "switch", "select", "case", "return", "go", "defer", "range",
            "func", "chan", "map", "struct", "interface", "type", "var", "const",
            "package", "import", "else", "goto", "break", "continue", "fallthrough",
            // builtins
            "append", "cap", "clear", "close", "complex", "copy", "delete", "imag",
            "len", "make", "max", "min", "new", "panic", "print", "println", "real",
            "recover",
            // predeclared types, i.e. conversions rather than calls
            "bool", "byte", "rune", "string", "error", "any", "uintptr",
            "int", "int8", "int16", "int32", "int64",
            "uint", "uint8", "uint16", "uint32", "uint64",
            "float32", "float64", "complex64", "complex128");

    /** Type keywords whose {@code {} } belongs to a type literal, not a function body. */
    private static final Set<String> TYPE_LITERAL_KEYWORDS = Set.of("struct", "interface");

    private final int parseThreads;

    /** Source directory → Go import path. Resolved once per directory. */
    private final Map<Path, String> importPathCache = new ConcurrentHashMap<>();

    /**
     * Structural detail captured while parsing and consumed by
     * {@link #buildRelationships}, which the {@link CodeParser} contract runs immediately
     * after {@link #parseFiles}. Caching it here keeps the relationship pass from having
     * to re-read and re-parse every source file.
     */
    private final Map<String, List<GoField>> fieldsByType = new ConcurrentHashMap<>();

    /** Method fqn → declared parameter types, for the {@code takes} edges. */
    private final Map<String, List<String>> paramTypesByMethod = new ConcurrentHashMap<>();

    public GoCodeParser() { this(1); }

    public GoCodeParser(int parseThreads) {
        this.parseThreads = Math.max(1, parseThreads);
    }

    @Override
    public List<String> supportedExtensions() {
        return EXTENSIONS;
    }

    // =========================================================================
    // CodeParser implementation
    // =========================================================================

    /**
     * Parses one file in isolation. Call resolution is limited to what a single file can
     * prove (its own receivers, its own package-level functions); use
     * {@link #parseFiles} for project-wide resolution.
     */
    @Override
    public ParsedFile parseFile(Path file, String projectName) throws IOException {
        GoAst ast = parseAst(file);
        if (ast == null) return emptyFile(file);
        Symbols symbols = new Symbols();
        symbols.add(ast, projectName);
        return toParsedFile(ast, projectName, symbols);
    }

    @Override
    public ParsedProject parseFiles(List<Path> files, Path projectRoot, String projectName) {
        fieldsByType.clear();
        paramTypesByMethod.clear();

        // Phase 1 — build every file's AST and a project-wide symbol table.
        List<GoAst> asts = new ArrayList<>(files.size());
        Symbols symbols = new Symbols();
        int errors = 0;
        for (Path file : files) {
            try {
                GoAst ast = parseAst(file);
                if (ast == null) { errors++; continue; }
                asts.add(ast);
                symbols.add(ast, projectName);
            } catch (Exception e) {
                log.debug("Parse error in {}: {}", file, e.getMessage());
                errors++;
            }
        }

        // Phase 2 — map to the shared model, resolving calls against the symbol table.
        List<ParsedFile> parsed = new ArrayList<>(asts.size());
        for (GoAst ast : asts) {
            try {
                parsed.add(toParsedFile(ast, projectName, symbols));
            } catch (Exception e) {
                log.debug("Mapping error in {}: {}", ast.file(), e.getMessage());
                errors++;
            }
        }
        log.info("GoCodeParser: parsed {} file(s) ({} error(s)) in project '{}'",
                parsed.size(), errors, projectName);
        return new ParsedProject(projectName, projectRoot.toString(), parsed);
    }

    @Override
    public ParsedProject parseProject(Path projectRoot, String projectName) throws IOException {
        log.info("Indexing Go project '{}' from {}", projectName, projectRoot);
        return parseFiles(collectGoFiles(projectRoot), projectRoot, projectName);
    }

    private List<Path> collectGoFiles(Path projectRoot) throws IOException {
        List<Path> files = new ArrayList<>();
        Files.walkFileTree(projectRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                Path name = dir.getFileName();
                String n = name != null ? name.toString() : "";
                if (n.startsWith(".") || SKIP_DIRS.contains(n)) return FileVisitResult.SKIP_SUBTREE;
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (file.getFileName().toString().endsWith(".go")) files.add(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }
        });
        return files;
    }

    // =========================================================================
    // Intermediate representation
    // =========================================================================

    private record GoParam(String name, String type) {}

    private record GoField(String name, String type) {}

    private record GoFunc(
            String name,
            String recvVar,          // null for package-level functions
            String recvType,         // receiver's bare type name, null for package-level
            String recvExpr,         // receiver type as written ("*Server"), null if none
            String typeParams,       // generics as written ("[T, U any]"), "" if none
            List<GoParam> params,
            String results,          // "", "error", "(int, error)"
            String doc,              // null when undocumented
            String body,             // raw text, "" for bodyless declarations
            String maskedBody,       // literal-masked body, used for call scanning
            int startLine,
            int endLine,
            boolean abstractDecl     // interface method or bodyless declaration
    ) {}

    private record GoType(
            String name,
            String kind,             // "class" (struct/alias) or "interface"
            List<String> embedded,
            List<GoField> fields,
            List<GoFunc> ifaceMethods,
            String doc,
            int startLine,
            int endLine
    ) {}

    private record GoAst(
            Path file,
            String packageName,      // Go import path, or the package clause as fallback
            String packageClause,
            List<String> imports,
            Map<String, String> importAliases,   // local name → import path
            List<GoType> types,
            List<GoFunc> funcs
    ) {}

    /** Project-wide symbol table used to resolve call references exactly. */
    private static final class Symbols {
        /** import path → function name → owning qualified class (the file's pseudo-class). */
        final Map<String, Map<String, String>> pkgFuncOwners = new HashMap<>();
        /** qualified type name → declared method names. */
        final Map<String, Set<String>> typeMethods = new HashMap<>();
        /** import path → simple type name → qualified type name. */
        final Map<String, Map<String, String>> pkgTypes = new HashMap<>();

        void add(GoAst ast, String projectName) {
            String pkg = ast.packageName();
            String pseudo = pseudoClassQualifiedName(ast);

            for (GoType t : ast.types()) {
                pkgTypes.computeIfAbsent(pkg, k -> new HashMap<>())
                        .put(t.name(), qualify(pkg, t.name()));
                Set<String> methods = typeMethods.computeIfAbsent(
                        qualify(pkg, t.name()), k -> new LinkedHashSet<>());
                for (GoFunc m : t.ifaceMethods()) methods.add(m.name());
            }
            for (GoFunc f : ast.funcs()) {
                if (f.recvType() == null) {
                    pkgFuncOwners.computeIfAbsent(pkg, k -> new HashMap<>())
                            .put(f.name(), pseudo);
                } else {
                    typeMethods.computeIfAbsent(qualify(pkg, f.recvType()),
                            k -> new LinkedHashSet<>()).add(f.name());
                }
            }
        }
    }

    // =========================================================================
    // Parsing: source text → GoAst
    // =========================================================================

    private GoAst parseAst(Path file) throws IOException {
        String src;
        try {
            src = Files.readString(file, StandardCharsets.UTF_8);
        } catch (Exception e) {
            src = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
        }

        char[] mask = maskLiterals(src);
        String masked = new String(mask);
        String[] raw = src.split("\n", -1);
        String[] msk = masked.split("\n", -1);
        int[] lineStart = lineStarts(src);

        String packageClause = "";
        List<String> imports = new ArrayList<>();
        Map<String, String> aliases = new LinkedHashMap<>();
        List<GoType> types = new ArrayList<>();
        List<GoFunc> funcs = new ArrayList<>();

        int braceDepth = 0;
        int parenDepth = 0;
        String groupKind = null;   // non-null while inside a `type (` / `import (` group

        for (int i = 0; i < msk.length; i++) {
            int startBrace = braceDepth;
            int startParen = parenDepth;
            String line = msk[i];
            String trimmed = line.strip();

            boolean topLevel = startBrace == 0 && startParen == 0;

            if (topLevel && !trimmed.isEmpty()) {
                Matcher pm = RE_PACKAGE.matcher(line);
                if (packageClause.isEmpty() && pm.find()) {
                    packageClause = pm.group(1);
                    braceDepth += net(line, '{', '}');
                    parenDepth += net(line, '(', ')');
                    continue;
                }
                Matcher gm = RE_GROUP_OPEN.matcher(trimmed);
                if (gm.matches()) {
                    groupKind = gm.group(1);
                    parenDepth += net(line, '(', ')');
                    braceDepth += net(line, '{', '}');
                    continue;
                }
                if (trimmed.startsWith("import ")) {
                    addImport(aliases, imports, raw[i], line);
                    braceDepth += net(line, '{', '}');
                    parenDepth += net(line, '(', ')');
                    continue;
                }
                if (trimmed.startsWith("type ")) {
                    int consumed = parseTypeDecl(
                            trimmed.substring(trimmed.indexOf("type ") + 5),
                            i, raw, msk, mask, lineStart, types);
                    if (consumed >= i) {
                        // Re-scan the consumed span so depths stay accurate.
                        for (int k = i; k <= consumed && k < msk.length; k++) {
                            braceDepth += net(msk[k], '{', '}');
                            parenDepth += net(msk[k], '(', ')');
                        }
                        i = consumed;
                        continue;
                    }
                }
                if (trimmed.startsWith("func ") || trimmed.equals("func")) {
                    int funcStart = lineStart[i] + line.indexOf("func");
                    GoFunc fn = parseFunc(mask, src, raw, lineStart, funcStart, i);
                    if (fn != null) {
                        funcs.add(fn);
                        int last = fn.endLine() - 1;
                        for (int k = i; k <= last && k < msk.length; k++) {
                            braceDepth += net(msk[k], '{', '}');
                            parenDepth += net(msk[k], '(', ')');
                        }
                        i = Math.max(i, last);
                        continue;
                    }
                }
            }

            // Entries inside a `type (...)` / `import (...)` group.
            if (groupKind != null && startParen == 1 && startBrace == 0 && !trimmed.isEmpty()
                    && !trimmed.startsWith(")")) {
                if (groupKind.equals("import")) {
                    addImport(aliases, imports, raw[i], line);
                } else if (groupKind.equals("type")) {
                    int consumed = parseTypeDecl(trimmed, i, raw, msk, mask, lineStart, types);
                    if (consumed > i) {
                        for (int k = i; k <= consumed && k < msk.length; k++) {
                            braceDepth += net(msk[k], '{', '}');
                            parenDepth += net(msk[k], '(', ')');
                        }
                        i = consumed;
                        continue;
                    }
                }
            }

            braceDepth += net(line, '{', '}');
            parenDepth += net(line, '(', ')');
            if (parenDepth <= 0) { parenDepth = Math.max(0, parenDepth); groupKind = null; }
            if (braceDepth < 0) braceDepth = 0;
        }

        if (packageClause.isEmpty()) {
            log.debug("{} has no package clause — skipping", file);
            return null;
        }

        String importPath = importPathFor(file);
        String pkgName = importPath.isEmpty() ? packageClause : importPath;

        return new GoAst(file.toAbsolutePath(), pkgName, packageClause,
                List.copyOf(imports), aliases, types, funcs);
    }

    private void addImport(Map<String, String> aliases, List<String> imports,
                           String rawLine, String maskedLine) {
        // The masked view proves this is code (a commented-out import masks to blank),
        // but the path itself must be read from the raw line.
        if (maskedLine.strip().isEmpty()) return;
        Matcher m = RE_IMPORT_SPEC.matcher(
                rawLine.strip().startsWith("import ")
                        ? rawLine.strip().substring("import ".length())
                        : rawLine);
        if (!m.find()) return;
        String alias = m.group(1);
        String path = m.group(2);
        if (path.isBlank()) return;
        imports.add(path);
        String local = (alias != null && !alias.equals("_") && !alias.equals("."))
                ? alias
                : lastSegment(path);
        aliases.put(local, path);
    }

    /**
     * Parses one type spec, which may be a single-line alias or a multi-line
     * struct/interface. {@code spec} is the text after {@code type } (or the raw entry
     * inside a {@code type (...)} group).
     *
     * @return index of the last line consumed, or {@code -1} when the text is not a type spec
     */
    private int parseTypeDecl(String spec, int lineIdx, String[] raw, String[] msk,
                              char[] mask, int[] lineStart, List<GoType> out) {
        Matcher nm = Pattern.compile("^(\\w+)").matcher(spec);
        if (!nm.find()) return -1;
        String name = nm.group(1);

        String rest = spec.substring(nm.end()).strip();
        // Drop type parameters: type List[T any] struct {...}
        if (rest.startsWith("[")) {
            int close = matchInText(rest, 0, '[', ']');
            if (close > 0) rest = rest.substring(close + 1).strip();
        }
        if (rest.startsWith("=")) rest = rest.substring(1).strip();   // alias

        String doc = docCommentAbove(raw, lineIdx);
        boolean isInterface = rest.startsWith("interface");
        boolean isStruct = rest.startsWith("struct");

        if (!isInterface && !isStruct) {
            // Named type / alias: `type UserID string`, `type Handler func(int) error`
            out.add(new GoType(name, "class", List.of(), List.of(), List.of(),
                    doc, lineIdx + 1, lineIdx + 1));
            return lineIdx;
        }

        // Locate the body brace for the struct/interface within the declaration line.
        int lineOffset = lineStart[lineIdx];
        int braceIdx = msk[lineIdx].indexOf('{', Math.max(0, msk[lineIdx].indexOf(name)));
        if (braceIdx < 0) {
            out.add(new GoType(name, isInterface ? "interface" : "class",
                    List.of(), List.of(), List.of(), doc, lineIdx + 1, lineIdx + 1));
            return lineIdx;
        }
        int open = lineOffset + braceIdx;
        int close = matchBracket(mask, open);
        if (close < 0) {
            out.add(new GoType(name, isInterface ? "interface" : "class",
                    List.of(), List.of(), List.of(), doc, lineIdx + 1, lineIdx + 1));
            return lineIdx;
        }
        int endLineIdx = lineOf(lineStart, close);

        List<String> embedded = new ArrayList<>();
        List<GoField> fields = new ArrayList<>();
        List<GoFunc> ifaceMethods = new ArrayList<>();

        if (isInterface) {
            parseInterfaceBody(mask, raw, msk, lineStart, open, close, embedded, ifaceMethods);
        } else {
            parseStructBody(raw, msk, lineStart, open, close, embedded, fields);
        }

        out.add(new GoType(name, isInterface ? "interface" : "class",
                embedded, fields, ifaceMethods, doc, lineIdx + 1, endLineIdx + 1));
        return endLineIdx;
    }

    /** Collects fields and embedded types at depth 1 of a struct body. */
    private void parseStructBody(String[] raw, String[] msk, int[] lineStart,
                                 int open, int close,
                                 List<String> embedded, List<GoField> fields) {
        int first = lineOf(lineStart, open);
        int last = lineOf(lineStart, close);
        int depth = 0;
        List<String> pendingNames = new ArrayList<>();

        for (int i = first; i <= last && i < msk.length; i++) {
            String maskedLine = msk[i];
            int from = (i == first) ? open - lineStart[i] + 1 : 0;
            int to = (i == last) ? close - lineStart[i] : maskedLine.length();
            if (from > maskedLine.length()) from = maskedLine.length();
            if (to > maskedLine.length()) to = maskedLine.length();
            if (from > to) continue;

            String segment = maskedLine.substring(from, to);
            if (depth == 0) {
                // Struct tags are backtick literals; masked content is blank but the
                // delimiters survive, so cutting at the first backtick drops the tag.
                int tick = segment.indexOf('`');
                String decl = (tick >= 0 ? segment.substring(0, tick) : segment).strip();
                if (!decl.isEmpty()) {
                    String rawSeg = safeSubstring(raw[i], from, Math.min(to, raw[i].length()));
                    int rawTick = rawSeg.indexOf('`');
                    String rawDecl = (rawTick >= 0 ? rawSeg.substring(0, rawTick) : rawSeg).strip();
                    collectStructEntry(rawDecl, pendingNames, embedded, fields);
                }
            }
            depth += net(segment, '{', '}');
            if (depth < 0) depth = 0;
        }
    }

    /**
     * Handles the three struct entry shapes: {@code Name Type}, a grouped
     * {@code A, B Type}, and an embedded {@code Type} / {@code *pkg.Type}.
     */
    private void collectStructEntry(String decl, List<String> pendingNames,
                                    List<String> embedded, List<GoField> fields) {
        for (String entry : splitTopLevel(decl, ';')) {
            String e = entry.strip();
            if (e.isEmpty()) continue;

            List<String> parts = splitTopLevel(e, ',');
            // Trailing group member carries the shared type: "A, B int"
            String lastPart = parts.get(parts.size() - 1).strip();
            int sp = firstTopLevelSpace(lastPart);

            if (sp < 0) {
                if (parts.size() == 1) {
                    // No type on this line — either an embedded type or a pending name.
                    String tok = lastPart;
                    if (looksLikeEmbedded(tok)) {
                        embedded.add(stripPointer(tok));
                    } else {
                        pendingNames.add(tok);
                    }
                } else {
                    for (String p : parts) pendingNames.add(p.strip());
                }
                continue;
            }

            String type = lastPart.substring(sp).strip();
            List<String> names = new ArrayList<>(pendingNames);
            pendingNames.clear();
            for (int i = 0; i < parts.size() - 1; i++) names.add(parts.get(i).strip());
            names.add(lastPart.substring(0, sp).strip());
            for (String n : names) {
                if (!n.isEmpty() && !type.isEmpty()) fields.add(new GoField(n, type));
            }
        }
    }

    /** An entry with no type is embedded when it reads as a type reference, not a name. */
    private static boolean looksLikeEmbedded(String tok) {
        return tok.startsWith("*") || tok.contains(".") || isCapitalized(tok);
    }

    /** Collects the method set and embedded interfaces at depth 1 of an interface body. */
    private void parseInterfaceBody(char[] mask, String[] raw, String[] msk, int[] lineStart,
                                    int open, int close,
                                    List<String> embedded, List<GoFunc> methods) {
        int first = lineOf(lineStart, open);
        int last = lineOf(lineStart, close);
        int depth = 0;

        for (int i = first; i <= last && i < msk.length; i++) {
            String maskedLine = msk[i];
            int from = (i == first) ? open - lineStart[i] + 1 : 0;
            int to = (i == last) ? close - lineStart[i] : maskedLine.length();
            if (from > maskedLine.length()) from = maskedLine.length();
            if (to > maskedLine.length()) to = maskedLine.length();
            if (from > to) continue;

            String segment = maskedLine.substring(from, to);
            if (depth == 0) {
                String decl = segment.strip();
                // Type-constraint elements (`~int | string`) are not a method set.
                if (!decl.isEmpty() && !decl.contains("|") && !decl.contains("~")) {
                    int paren = decl.indexOf('(');
                    if (paren > 0) {
                        String name = decl.substring(0, paren).strip();
                        if (name.matches("\\w+")) {
                            int openAbs = lineStart[i] + from + segment.indexOf('(');
                            int closeAbs = matchBracket(mask, openAbs);
                            String paramText = "";
                            int resultsFrom = openAbs + 1;
                            if (closeAbs > openAbs) {
                                paramText = safeRange(raw, lineStart, openAbs + 1, closeAbs);
                                resultsFrom = closeAbs + 1;
                            }
                            int resEnd = Math.min(lineStart[i] + to,
                                    resultsFrom + 200);
                            String results = safeRange(raw, lineStart, resultsFrom,
                                    Math.max(resultsFrom, resEnd)).strip();
                            methods.add(new GoFunc(name, null, null, null, "",
                                    parseParams(paramText), results,
                                    docCommentAbove(raw, i), "", "",
                                    i + 1, i + 1, true));
                        }
                    } else if (looksLikeEmbedded(decl) && decl.matches("[*\\w.]+")) {
                        embedded.add(stripPointer(decl));
                    }
                }
            }
            depth += net(segment, '{', '}');
            if (depth < 0) depth = 0;
        }
    }

    /**
     * Parses a {@code func} declaration starting at {@code funcOffset}.
     *
     * <p>Handles receivers, type parameters, multi-line parameter lists, composite result
     * types ({@code struct{...}} / {@code interface{}}), and bodyless declarations.
     */
    private GoFunc parseFunc(char[] mask, String src, String[] raw, int[] lineStart,
                             int funcOffset, int lineIdx) {
        int n = mask.length;
        int p = funcOffset + 4;   // past "func"
        p = skipSpace(mask, p);
        if (p >= n) return null;

        String recvVar = null;
        String recvType = null;
        String recvExpr = null;

        // Optional receiver: func (s *Server) Name(...)
        if (mask[p] == '(') {
            int rc = matchBracket(mask, p);
            if (rc < 0) return null;
            // Read from src, not mask: the receiver is plain code, and src keeps it verbatim.
            String recv = src.substring(p + 1, rc).strip();
            if (!recv.isEmpty()) {
                int sp = firstTopLevelSpace(recv);
                String typeExpr;
                if (sp < 0) {
                    typeExpr = recv;
                } else {
                    recvVar = recv.substring(0, sp).strip();
                    typeExpr = recv.substring(sp).strip();
                }
                // recvType is the bare name used for symbol lookup; recvExpr keeps the
                // pointer/generic form so the signature reads as it does in the source.
                recvExpr = typeExpr;
                recvType = simpleTypeName(typeExpr);
                if ("_".equals(recvVar)) recvVar = null;
            }
            p = skipSpace(mask, rc + 1);
        }

        // Function name
        int nameStart = p;
        while (p < n && (Character.isLetterOrDigit(mask[p]) || mask[p] == '_')) p++;
        if (p == nameStart) return null;
        String name = new String(mask, nameStart, p - nameStart);
        p = skipSpace(mask, p);

        // Optional type parameters: func Map[T, U any](...)
        String typeParams = "";
        if (p < n && mask[p] == '[') {
            int tc = matchBracket(mask, p);
            if (tc < 0) return null;
            typeParams = src.substring(p, tc + 1);
            p = skipSpace(mask, tc + 1);
        }

        if (p >= n || mask[p] != '(') return null;
        int paramClose = matchBracket(mask, p);
        if (paramClose < 0) return null;
        String paramText = src.substring(p + 1, paramClose);

        // Results and body brace. Go's semicolon insertion puts the body `{` on the same
        // line as the end of the signature, which bounds the search.
        int bodyBrace = findBodyBrace(mask, paramClose + 1, lineStart);
        int endOfSigLine = endOfLine(mask, paramClose + 1);
        String results;
        String body = "";
        String maskedBody = "";
        int endLine;
        boolean abstractDecl;

        if (bodyBrace > 0) {
            results = src.substring(paramClose + 1, bodyBrace).strip();
            int bodyClose = matchBracket(mask, bodyBrace);
            if (bodyClose < 0) bodyClose = Math.min(n - 1, endOfSigLine);
            body = src.substring(bodyBrace, Math.min(bodyClose + 1, src.length()));
            maskedBody = new String(mask, bodyBrace,
                    Math.min(bodyClose + 1, n) - bodyBrace);
            endLine = lineOf(lineStart, bodyClose) + 1;
            abstractDecl = false;
        } else {
            results = src.substring(paramClose + 1, Math.min(endOfSigLine, src.length())).strip();
            endLine = lineOf(lineStart, Math.min(endOfSigLine, n - 1)) + 1;
            abstractDecl = true;
        }

        return new GoFunc(name, recvVar, recvType, recvExpr, typeParams,
                parseParams(paramText),
                normalizeResults(results), docCommentAbove(raw, lineIdx),
                body, maskedBody, lineIdx + 1, Math.max(endLine, lineIdx + 1), abstractDecl);
    }

    /**
     * Finds the {@code {} that opens a function body, skipping braces that belong to a
     * composite result type. Returns -1 for a bodyless declaration.
     */
    private int findBodyBrace(char[] mask, int from, int[] lineStart) {
        int n = mask.length;
        int depth = 0;
        int limitLine = lineOf(lineStart, Math.min(from, n - 1));
        for (int i = from; i < n; i++) {
            char c = mask[i];
            if (c == '\n') {
                if (depth == 0 && lineOf(lineStart, i) >= limitLine) return -1;
                continue;
            }
            if (c == '(' || c == '[') { depth++; continue; }
            if (c == ')' || c == ']') { depth--; continue; }
            if (c == '{') {
                if (depth > 0) { depth++; continue; }
                if (precededByTypeKeyword(mask, i)) {
                    int close = matchBracket(mask, i);
                    if (close < 0) return -1;
                    i = close;
                    limitLine = lineOf(lineStart, close);
                    continue;
                }
                return i;
            }
            if (c == '}') { depth--; }
        }
        return -1;
    }

    /** True when the {@code {} at {@code idx} closes a {@code struct}/{@code interface} type. */
    private static boolean precededByTypeKeyword(char[] mask, int idx) {
        int i = idx - 1;
        while (i >= 0 && Character.isWhitespace(mask[i])) i--;
        int end = i + 1;
        while (i >= 0 && (Character.isLetterOrDigit(mask[i]) || mask[i] == '_')) i--;
        if (end <= i + 1) return false;
        return TYPE_LITERAL_KEYWORDS.contains(new String(mask, i + 1, end - i - 1));
    }

    /** Normalises a result list: drops redundant parens around a single result. */
    private static String normalizeResults(String results) {
        String r = results.strip();
        if (r.startsWith("(") && r.endsWith(")")) {
            String inner = r.substring(1, r.length() - 1).strip();
            if (!inner.contains(",")) return inner;
        }
        return r;
    }

    /**
     * Parses a Go parameter list.
     *
     * <p>Applies Go's all-or-nothing naming rule: if no entry has an explicit
     * {@code name Type} form, every entry is an unnamed type ({@code (int, error)});
     * otherwise bare identifiers are names sharing the next explicit type
     * ({@code a, b int}).
     */
    private static List<GoParam> parseParams(String text) {
        String t = text == null ? "" : text.strip();
        if (t.isEmpty()) return List.of();

        List<String> groups = splitTopLevel(t, ',');
        boolean anyNamed = groups.stream()
                .anyMatch(g -> firstTopLevelSpace(g.strip()) > 0);

        List<GoParam> out = new ArrayList<>();
        if (!anyNamed) {
            for (String g : groups) {
                String type = g.strip();
                if (!type.isEmpty()) out.add(new GoParam("", type));
            }
            return out;
        }

        List<String> pending = new ArrayList<>();
        for (String g : groups) {
            String e = g.strip();
            if (e.isEmpty()) continue;
            int sp = firstTopLevelSpace(e);
            if (sp < 0) {
                pending.add(e);
                continue;
            }
            String type = e.substring(sp).strip();
            pending.add(e.substring(0, sp).strip());
            for (String nm : pending) {
                if (!nm.isEmpty()) out.add(new GoParam(nm, type));
            }
            pending.clear();
        }
        // Trailing names with no type (malformed source) — keep them, type unknown.
        for (String nm : pending) if (!nm.isEmpty()) out.add(new GoParam(nm, ""));
        return out;
    }

    // =========================================================================
    // Mapping: GoAst → ParsedFile
    // =========================================================================

    private ParsedFile toParsedFile(GoAst ast, String projectName, Symbols symbols) {
        String pkg = ast.packageName();
        String absPath = ast.file().toString();

        List<ParsedClass> classes = new ArrayList<>();
        List<ParsedMethod> methods = new ArrayList<>();
        Map<String, ParsedClass> byName = new LinkedHashMap<>();

        // --- declared types ---
        for (GoType t : ast.types()) {
            String qualified = qualify(pkg, t.name());
            String superclass = null;
            List<String> interfaces = new ArrayList<>();
            if (!t.embedded().isEmpty()) {
                if (t.kind().equals("interface")) {
                    interfaces.addAll(t.embedded());
                } else {
                    superclass = t.embedded().get(0);
                    interfaces.addAll(t.embedded().subList(1, t.embedded().size()));
                }
            }
            ParsedClass cls = new ParsedClass(
                    projectName, pkg, t.name(), qualified,
                    t.kind(), superclass, List.copyOf(interfaces), List.of(),
                    accessOf(t.name()),
                    t.kind().equals("interface"), false,
                    t.doc(), absPath, t.startLine(), t.endLine());
            classes.add(cls);
            byName.put(t.name(), cls);
            fieldsByType.put(qualified, t.fields());

            for (GoFunc m : t.ifaceMethods()) {
                methods.add(toParsedMethod(m, projectName, absPath, cls, ast, symbols));
            }
        }

        // --- per-file pseudo-class for package-level functions ---
        boolean hasTopLevel = ast.funcs().stream().anyMatch(f -> f.recvType() == null);
        ParsedClass pseudo = null;
        if (hasTopLevel) {
            String pseudoName = pseudoClassSimpleName(ast);
            pseudo = new ParsedClass(
                    projectName, pkg, pseudoName, pseudoClassQualifiedName(ast),
                    "module", null, List.of(), List.of(),
                    "public", false, false, null, absPath, 1, 1);
            classes.add(pseudo);
        }

        // --- functions and methods ---
        for (GoFunc f : ast.funcs()) {
            ParsedClass owner;
            if (f.recvType() == null) {
                owner = pseudo;
            } else {
                owner = byName.get(f.recvType());
                if (owner == null) {
                    // Receiver type declared in another file of the same package.
                    String q = qualify(pkg, f.recvType());
                    owner = new ParsedClass(projectName, pkg, f.recvType(), q,
                            "class", null, List.of(), List.of(),
                            accessOf(f.recvType()), false, false, null, absPath, 0, 0);
                    byName.put(f.recvType(), owner);
                }
            }
            if (owner == null) continue;
            methods.add(toParsedMethod(f, projectName, absPath, owner, ast, symbols));
        }

        return new ParsedFile(absPath, pkg, ast.imports(), classes, methods);
    }

    private ParsedMethod toParsedMethod(GoFunc f, String projectName, String absPath,
                                        ParsedClass owner, GoAst ast, Symbols symbols) {
        List<String> paramNames = f.params().stream()
                .map(p -> p.name().isEmpty() ? p.type() : p.name())
                .toList();

        // paramTypes is deliberately empty: Go has no overloading, so identity is
        // (receiver, name). This keeps fqn() == the id and makes call resolution exact.
        String id = ParsedMethod.buildId(projectName, owner.qualifiedClassName(),
                f.name(), List.of());
        String selfFqn = owner.qualifiedClassName() + "#" + f.name() + "()";
        paramTypesByMethod.put(selfFqn,
                f.params().stream().map(GoParam::type).filter(s -> !s.isEmpty()).toList());

        List<CallReference> calls = f.maskedBody().isEmpty()
                ? List.of()
                : extractCalls(f, selfFqn, ast, symbols);

        String body = f.body().length() > MAX_BODY_CHARS
                ? f.body().substring(0, MAX_BODY_CHARS)
                : f.body();

        return new ParsedMethod(
                id, projectName,
                owner.packageName(),
                owner.className(),
                owner.qualifiedClassName(),
                f.name(),
                buildSignature(f),
                f.results().isEmpty() ? "void" : f.results(),
                List.of(),
                paramNames,
                body,
                f.doc(),
                List.of(),
                accessOf(f.name()),
                // A package-level func is the closest Go analogue of a static method;
                // an interface's method set is not static.
                "module".equals(owner.kind()),
                false,
                f.abstractDecl(),
                false,
                List.of(),
                calls,
                absPath, f.startLine(), f.endLine());
    }

    /** Renders the Go declaration as written: {@code func (s *Server) Handle(w W) error}. */
    private static String buildSignature(GoFunc f) {
        StringBuilder sb = new StringBuilder("func ");
        if (f.recvType() != null) {
            sb.append('(');
            if (f.recvVar() != null) sb.append(f.recvVar()).append(' ');
            sb.append(f.recvExpr() != null ? f.recvExpr() : f.recvType()).append(") ");
        }
        sb.append(f.name()).append(f.typeParams()).append('(');
        List<String> parts = new ArrayList<>();
        for (GoParam p : f.params()) {
            parts.add(p.name().isEmpty() ? p.type() : p.name() + " " + p.type());
        }
        sb.append(String.join(", ", parts)).append(')');
        if (!f.results().isEmpty()) sb.append(' ').append(f.results());
        return sb.toString();
    }

    // =========================================================================
    // Call extraction and resolution
    // =========================================================================

    private List<CallReference> extractCalls(GoFunc f, String callerFqn,
                                             GoAst ast, Symbols symbols) {
        String masked = f.maskedBody();
        String pkg = ast.packageName();

        // Local variable type inference: `v := &Server{}` / `v := NewServer(...)`.
        Map<String, String> localTypes = new HashMap<>();
        Matcher bind = RE_LOCAL_BIND.matcher(masked);
        while (bind.find()) {
            String var = bind.group(1);
            String type = bind.group(2) != null ? bind.group(2) : bind.group(3);
            if (type != null && !type.isEmpty()) localTypes.putIfAbsent(var, type);
        }

        Map<String, CallReference> seen = new LinkedHashMap<>();
        Matcher m = RE_CALL.matcher(masked);
        while (m.find()) {
            String prefix = m.group(1);
            String callee = m.group(2);
            if (NOT_CALLS.contains(callee)) continue;
            if (prefix != null && NOT_CALLS.contains(prefix)) continue;

            int line = f.startLine() + countNewlines(masked, m.start());
            int argCount = countArgs(masked, m.end() - 1);

            String resolvedOwner = null;
            String receiverType = null;

            if (prefix == null) {
                // Package-level function in this package.
                Map<String, String> owners = symbols.pkgFuncOwners.get(pkg);
                if (owners != null) resolvedOwner = owners.get(callee);
            } else if (prefix.equals(f.recvVar()) && f.recvType() != null) {
                receiverType = f.recvType();
                resolvedOwner = ownerIfDeclaredMethod(symbols, pkg, f.recvType(), callee);
            } else if (localTypes.containsKey(prefix)) {
                receiverType = localTypes.get(prefix);
                resolvedOwner = ownerIfDeclaredMethod(symbols, pkg, receiverType, callee);
            } else if (ast.importAliases().containsKey(prefix)) {
                String importPath = ast.importAliases().get(prefix);
                Map<String, String> owners = symbols.pkgFuncOwners.get(importPath);
                if (owners != null) resolvedOwner = owners.get(callee);
            }

            CallReference ref = (resolvedOwner != null)
                    ? CallReference.resolved(callerFqn,
                            resolvedOwner + "#" + callee + "()", argCount, line)
                    : CallReference.unresolved(callerFqn, callee, receiverType, argCount, line);
            seen.putIfAbsent(ref.calleeFqn(), ref);
        }
        return List.copyOf(seen.values());
    }

    /** Returns the qualified type name when it really declares {@code method}, else null. */
    private static String ownerIfDeclaredMethod(Symbols symbols, String pkg,
                                                String typeName, String method) {
        String qualified = qualify(pkg, typeName);
        Set<String> declared = symbols.typeMethods.get(qualified);
        return (declared != null && declared.contains(method)) ? qualified : null;
    }

    /** Counts top-level arguments starting from the call's open paren. */
    private static int countArgs(String masked, int openParen) {
        if (openParen < 0 || openParen >= masked.length() || masked.charAt(openParen) != '(') {
            return 0;
        }
        int depth = 0;
        int count = 0;
        boolean sawContent = false;
        for (int i = openParen; i < masked.length(); i++) {
            char c = masked.charAt(i);
            if (c == '(' || c == '[' || c == '{') { depth++; continue; }
            if (c == ')' || c == ']' || c == '}') {
                depth--;
                if (depth == 0) return sawContent ? count + 1 : 0;
                continue;
            }
            if (depth == 1) {
                if (c == ',') count++;
                else if (!Character.isWhitespace(c)) sawContent = true;
            }
        }
        return sawContent ? count + 1 : 0;
    }

    // =========================================================================
    // Relationships
    // =========================================================================

    /**
     * Emits embedding (inherits/implements), struct fields, and — restricted to types
     * declared inside this project — parameter and result type edges.
     */
    @Override
    public ParsedRelationships buildRelationships(ParsedProject project) {
        List<ParsedRelationships.TypeEdge> inherits = new ArrayList<>();
        List<ParsedRelationships.TypeEdge> implementsEdges = new ArrayList<>();
        List<ParsedRelationships.FieldDecl> fields = new ArrayList<>();
        List<ParsedRelationships.TypeEdge> returns = new ArrayList<>();
        List<ParsedRelationships.TypeEdge> takes = new ArrayList<>();

        // Simple type name → qualified name, per package, for resolving embedded refs.
        Map<String, Map<String, String>> declared = new HashMap<>();
        for (ParsedFile file : project.files()) {
            for (ParsedClass cls : file.classes()) {
                if ("module".equals(cls.kind())) continue;
                declared.computeIfAbsent(cls.packageName(), k -> new HashMap<>())
                        .put(cls.className(), cls.qualifiedClassName());
            }
        }

        for (ParsedFile file : project.files()) {
            for (ParsedClass cls : file.classes()) {
                String from = cls.qualifiedClassName();
                if (cls.superclass() != null && !cls.superclass().isBlank()) {
                    inherits.add(new ParsedRelationships.TypeEdge(
                            from, resolveTypeRef(declared, cls.packageName(), cls.superclass())));
                }
                for (String iface : cls.interfaces()) {
                    if (!iface.isBlank()) {
                        implementsEdges.add(new ParsedRelationships.TypeEdge(
                                from, resolveTypeRef(declared, cls.packageName(), iface)));
                    }
                }
            }
        }

        for (ParsedFile file : project.files()) {
            for (ParsedClass cls : file.classes()) {
                if (!"class".equals(cls.kind())) continue;
                String q = cls.qualifiedClassName();
                for (GoField gf : structFieldsOf(file, cls)) {
                    fields.add(new ParsedRelationships.FieldDecl(
                            q + "#" + gf.name(), q, gf.type(), accessOf(gf.name())));
                }
            }
            // Package qualifiers in type expressions ("model.User") resolve through the
            // file's own import list.
            Map<String, String> imports = new HashMap<>();
            for (String imp : file.imports()) imports.put(lastSegment(imp), imp);

            for (ParsedMethod m : file.methods()) {
                String rq = resolveDeclaredType(declared, imports, m.packageName(), m.returnType());
                if (rq != null) returns.add(new ParsedRelationships.TypeEdge(m.fqn(), rq));

                for (String pt : paramTypesByMethod.getOrDefault(m.fqn(), List.of())) {
                    String pq = resolveDeclaredType(declared, imports, m.packageName(), pt);
                    if (pq != null) takes.add(new ParsedRelationships.TypeEdge(m.fqn(), pq));
                }
            }
        }

        return new ParsedRelationships(project.projectName(),
                inherits, implementsEdges, fields,
                List.of(), List.of(),
                returns, takes, List.of());
    }

    /**
     * Struct fields for {@code cls}, served from the parse-time cache. Falls back to
     * re-reading the file only when the cache has no entry at all — which happens when
     * {@code buildRelationships} is handed a project this instance did not parse.
     */
    private List<GoField> structFieldsOf(ParsedFile file, ParsedClass cls) {
        List<GoField> cached = fieldsByType.get(cls.qualifiedClassName());
        if (cached != null) return cached;
        try {
            GoAst ast = parseAst(Path.of(file.filePath()));
            if (ast == null) return List.of();
            for (GoType t : ast.types()) {
                if (t.name().equals(cls.className())) return t.fields();
            }
        } catch (Exception e) {
            log.debug("Could not re-read fields for {}: {}", cls.qualifiedClassName(), e.getMessage());
        }
        return List.of();
    }

    private static String resolveTypeRef(Map<String, Map<String, String>> declared,
                                         String pkg, String ref) {
        String simple = simpleTypeName(ref);
        String q = lookup(declared, pkg, simple);
        return q != null ? q : ref;
    }

    private static String lookup(Map<String, Map<String, String>> declared,
                                 String pkg, String simple) {
        Map<String, String> inPkg = declared.get(pkg);
        return inPkg != null ? inPkg.get(simple) : null;
    }

    /**
     * Resolves a Go type expression to a type declared inside this project, or null.
     *
     * <p>Peels pointers, slices, ellipses and channel direction, drops type arguments,
     * and resolves a package qualifier ({@code model.User}) through {@code imports}.
     * Returns null for anything not declared in the project — builtins, third-party
     * types, type parameters, and anonymous composite types.
     */
    private static String resolveDeclaredType(Map<String, Map<String, String>> declared,
                                              Map<String, String> imports,
                                              String pkg, String typeExpr) {
        if (typeExpr == null) return null;
        String t = typeExpr.strip();
        if (t.isEmpty() || t.equals("void") || t.contains(",")) return null;
        t = t.replaceAll("^(?:[*\\[\\]&]|\\.\\.\\.|<-|chan\\s+|\\s)+", "").strip();
        int br = t.indexOf('[');
        if (br > 0) t = t.substring(0, br);
        if (t.isEmpty()) return null;

        int dot = t.lastIndexOf('.');
        if (dot >= 0) {
            String path = imports.get(t.substring(0, dot));
            return path == null ? null : lookup(declared, path, t.substring(dot + 1));
        }
        return t.matches("\\w+") ? lookup(declared, pkg, t) : null;
    }

    // =========================================================================
    // Literal masking and text utilities
    // =========================================================================

    /**
     * Returns a copy of {@code src} in which comment bodies and literal contents are
     * replaced by spaces, preserving length, newlines, and the literal delimiters.
     *
     * <p>Every brace-matching and pattern-matching operation runs against this view, so
     * a {@code }} inside a string, struct tag, comment, or rune literal cannot be
     * mistaken for real syntax.
     */
    static char[] maskLiterals(String src) {
        char[] out = src.toCharArray();
        int n = out.length;
        int i = 0;
        while (i < n) {
            char c = out[i];
            if (c == '/' && i + 1 < n && out[i + 1] == '/') {
                while (i < n && out[i] != '\n') out[i++] = ' ';
            } else if (c == '/' && i + 1 < n && out[i + 1] == '*') {
                out[i++] = ' ';
                out[i++] = ' ';
                while (i < n) {
                    if (out[i] == '*' && i + 1 < n && out[i + 1] == '/') {
                        out[i++] = ' ';
                        out[i++] = ' ';
                        break;
                    }
                    if (out[i] != '\n') out[i] = ' ';
                    i++;
                }
            } else if (c == '"') {
                i++;
                while (i < n && out[i] != '"' && out[i] != '\n') {
                    if (out[i] == '\\' && i + 1 < n) {
                        out[i] = ' ';
                        if (out[i + 1] != '\n') out[i + 1] = ' ';
                        i += 2;
                        continue;
                    }
                    out[i++] = ' ';
                }
                if (i < n && out[i] == '"') i++;
            } else if (c == '`') {
                i++;
                while (i < n && out[i] != '`') {
                    if (out[i] != '\n') out[i] = ' ';
                    i++;
                }
                if (i < n) i++;
            } else if (c == '\'') {
                i++;
                while (i < n && out[i] != '\'' && out[i] != '\n') {
                    if (out[i] == '\\' && i + 1 < n) {
                        out[i] = ' ';
                        if (out[i + 1] != '\n') out[i + 1] = ' ';
                        i += 2;
                        continue;
                    }
                    out[i++] = ' ';
                }
                if (i < n && out[i] == '\'') i++;
            } else {
                i++;
            }
        }
        return out;
    }

    /** Index of the bracket matching the one at {@code open}, or -1. */
    static int matchBracket(char[] m, int open) {
        if (open < 0 || open >= m.length) return -1;
        char o = m[open];
        char c = switch (o) {
            case '(' -> ')';
            case '[' -> ']';
            case '{' -> '}';
            default -> 0;
        };
        if (c == 0) return -1;
        int depth = 0;
        for (int i = open; i < m.length; i++) {
            if (m[i] == o) depth++;
            else if (m[i] == c && --depth == 0) return i;
        }
        return -1;
    }

    /** Same as {@link #matchBracket} but over a String and an explicit pair. */
    private static int matchInText(String s, int open, char o, char c) {
        int depth = 0;
        for (int i = open; i < s.length(); i++) {
            if (s.charAt(i) == o) depth++;
            else if (s.charAt(i) == c && --depth == 0) return i;
        }
        return -1;
    }

    /** Splits on {@code sep} at bracket depth zero. */
    private static List<String> splitTopLevel(String s, char sep) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        StringBuilder cur = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c == '(' || c == '[' || c == '{') depth++;
            else if (c == ')' || c == ']' || c == '}') depth--;
            if (c == sep && depth == 0) {
                out.add(cur.toString());
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        out.add(cur.toString());
        return out;
    }

    /**
     * Index of the first whitespace run at bracket depth zero — the boundary between a
     * Go identifier and its type. Returns -1 when the text is a single token, and skips
     * the leading keyword of {@code func}/{@code chan}/{@code map}-style types.
     */
    private static int firstTopLevelSpace(String s) {
        String t = s.strip();
        if (t.isEmpty()) return -1;
        int depth = 0;
        int i = 0;
        // A leading type keyword means the whole text is a type, not "name Type".
        int wordEnd = 0;
        while (wordEnd < t.length()
                && (Character.isLetterOrDigit(t.charAt(wordEnd)) || t.charAt(wordEnd) == '_')) {
            wordEnd++;
        }
        String firstWord = t.substring(0, wordEnd);
        if (firstWord.equals("func") || firstWord.equals("chan") || firstWord.equals("map")
                || firstWord.equals("struct") || firstWord.equals("interface")) {
            return -1;
        }
        if (wordEnd == 0) return -1;   // starts with *, [, etc. — a bare type
        for (i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            if (c == '(' || c == '[' || c == '{') depth++;
            else if (c == ')' || c == ']' || c == '}') depth--;
            else if (depth == 0 && Character.isWhitespace(c)) return i;
        }
        return -1;
    }

    /** {@code *pkg.List[T]} → {@code List}. */
    private static String simpleTypeName(String typeExpr) {
        String t = typeExpr.strip();
        t = t.replaceAll("^[*&\\s]+", "");
        int br = t.indexOf('[');
        if (br > 0) t = t.substring(0, br);
        int dot = t.lastIndexOf('.');
        if (dot >= 0) t = t.substring(dot + 1);
        return t.strip();
    }

    private static String stripPointer(String s) {
        return s.strip().replaceAll("^[*&\\s]+", "");
    }

    /**
     * Collects the Go doc comment directly above {@code lineIdx} — contiguous
     * {@code //} lines, or a {@code /* ... *}{@code /} block, with no blank line between
     * the comment and the declaration.
     */
    private static String docCommentAbove(String[] raw, int lineIdx) {
        List<String> collected = new ArrayList<>();
        int i = lineIdx - 1;

        if (i >= 0 && raw[i].strip().endsWith("*/")) {
            List<String> block = new ArrayList<>();
            while (i >= 0) {
                String line = raw[i].strip();
                block.add(0, line);
                if (line.startsWith("/*")) break;
                i--;
            }
            if (i < 0) return null;
            String joined = String.join("\n", block);
            joined = joined.replaceFirst("^/\\*+", "").replaceFirst("\\*+/$", "");
            String cleaned = joined.replaceAll("(?m)^\\s*\\*?\\s?", "").strip();
            return cleaned.isEmpty() ? null : cleaned;
        }

        while (i >= 0) {
            String line = raw[i].strip();
            if (!line.startsWith("//")) break;
            // Build directives (//go:generate, //go:build) are not documentation.
            String text = line.substring(2);
            if (text.startsWith("go:") || text.startsWith("nolint")) { i--; continue; }
            collected.add(0, text.startsWith(" ") ? text.substring(1) : text);
            i--;
        }
        if (collected.isEmpty()) return null;
        String doc = String.join("\n", collected).strip();
        return doc.isEmpty() ? null : doc;
    }

    private static int[] lineStarts(String src) {
        List<Integer> starts = new ArrayList<>();
        starts.add(0);
        for (int i = 0; i < src.length(); i++) {
            if (src.charAt(i) == '\n') starts.add(i + 1);
        }
        int[] out = new int[starts.size()];
        for (int i = 0; i < out.length; i++) out[i] = starts.get(i);
        return out;
    }

    /** Zero-based line index containing {@code offset}. */
    private static int lineOf(int[] lineStart, int offset) {
        int lo = 0;
        int hi = lineStart.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (lineStart[mid] <= offset) lo = mid;
            else hi = mid - 1;
        }
        return lo;
    }

    private static int skipSpace(char[] m, int i) {
        while (i < m.length && Character.isWhitespace(m[i])) i++;
        return i;
    }

    private static int endOfLine(char[] m, int from) {
        int i = Math.max(0, from);
        while (i < m.length && m[i] != '\n') i++;
        return i;
    }

    private static int net(String s, char open, char close) {
        int d = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == open) d++;
            else if (c == close) d--;
        }
        return d;
    }

    private static int countNewlines(String s, int upTo) {
        int c = 0;
        for (int i = 0; i < Math.min(upTo, s.length()); i++) {
            if (s.charAt(i) == '\n') c++;
        }
        return c;
    }

    private static String safeSubstring(String s, int from, int to) {
        int f = Math.max(0, Math.min(from, s.length()));
        int t = Math.max(f, Math.min(to, s.length()));
        return s.substring(f, t);
    }

    /** Reconstructs raw text spanning {@code [from, to)} from the raw line array. */
    private static String safeRange(String[] raw, int[] lineStart, int from, int to) {
        if (to <= from) return "";
        int firstLine = lineOf(lineStart, from);
        int lastLine = lineOf(lineStart, Math.max(from, to - 1));
        StringBuilder sb = new StringBuilder();
        for (int i = firstLine; i <= lastLine && i < raw.length; i++) {
            int s = (i == firstLine) ? from - lineStart[i] : 0;
            int e = (i == lastLine) ? to - lineStart[i] : raw[i].length();
            sb.append(safeSubstring(raw[i], s, e));
            if (i != lastLine) sb.append(' ');
        }
        return sb.toString();
    }

    // =========================================================================
    // Naming
    // =========================================================================

    /** Go import path for a file's directory, from the enclosing {@code go.mod}. */
    private String importPathFor(Path file) {
        Path dir = file.toAbsolutePath().getParent();
        if (dir == null) return "";
        return importPathCache.computeIfAbsent(dir, d -> {
            Optional<Path> rootOpt = GoModReader.findModuleRoot(d);
            if (rootOpt.isEmpty()) return "";
            Path root = rootOpt.get();
            Optional<String> modOpt = GoModReader.readModulePath(root.resolve("go.mod"));
            if (modOpt.isEmpty()) return "";
            String rel = root.relativize(d).toString().replace(File.separatorChar, '/');
            return rel.isEmpty() ? modOpt.get() : modOpt.get() + "/" + rel;
        });
    }

    private static String qualify(String pkg, String name) {
        return pkg.isEmpty() ? name : pkg + "." + name;
    }

    /**
     * Name of the per-file pseudo-class holding package-level functions. Uses the file
     * stem, disambiguated if the file also declares a type of that exact name.
     */
    private static String pseudoClassSimpleName(GoAst ast) {
        String stem = fileStem(ast.file());
        boolean clash = ast.types().stream().anyMatch(t -> t.name().equals(stem));
        return clash ? stem + "_pkg" : stem;
    }

    private static String pseudoClassQualifiedName(GoAst ast) {
        return qualify(ast.packageName(), pseudoClassSimpleName(ast));
    }

    private static String fileStem(Path path) {
        String name = path.getFileName().toString();
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    private static String lastSegment(String importPath) {
        int slash = importPath.lastIndexOf('/');
        return slash >= 0 ? importPath.substring(slash + 1) : importPath;
    }

    /** Go exportedness: a capitalised identifier is exported. */
    private static String accessOf(String name) {
        return isCapitalized(name) ? "public" : "private";
    }

    private static boolean isCapitalized(String s) {
        return s != null && !s.isEmpty() && Character.isUpperCase(s.charAt(0));
    }

    private static ParsedFile emptyFile(Path file) {
        return new ParsedFile(file.toAbsolutePath().toString(), "",
                List.of(), List.of(), List.of());
    }
}
