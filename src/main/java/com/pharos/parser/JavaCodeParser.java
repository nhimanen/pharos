package com.pharos.parser;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.expr.*;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.JavaParserTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.JarTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;
import com.pharos.parser.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Java source code parser using JavaParser with symbol resolution.
 *
 * Mirrors mcp_server_code_extractor's approach:
 * - AST-based (not regex): understands code structure
 * - Extracts rich metadata per method/class (location, parent, params, javadoc, calls)
 * - Handles symbol resolution via CombinedTypeSolver (JDK + source + jars)
 * - Graceful fallback for unresolvable symbols (external deps)
 */
public class JavaCodeParser implements CodeParser {

    private static final Logger log = LoggerFactory.getLogger(JavaCodeParser.class);
    private static final List<String> EXTENSIONS = List.of(".java");

    private final List<Path> additionalSourceRoots;
    private final List<Path> jarPaths;
    private final int parseThreads;

    /**
     * Cache of (sourceRoot → [java21Parser, rawParser]) pairs, keyed by source root path.
     * Avoids creating a new CombinedTypeSolver + JavaParserTypeSolver on every parseFile() call
     * during incremental indexing where the same source root is used for all dirty files.
     * The pair is stored as a two-element array: index 0 = JAVA_21, index 1 = RAW.
     */
    private final java.util.concurrent.ConcurrentHashMap<Path, JavaParser[]> parserCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    public JavaCodeParser() {
        this(List.of(), List.of(), 1);
    }

    public JavaCodeParser(List<Path> additionalSourceRoots, List<Path> jarPaths) {
        this(additionalSourceRoots, jarPaths, 1);
    }

    public JavaCodeParser(List<Path> additionalSourceRoots, List<Path> jarPaths, int parseThreads) {
        this.additionalSourceRoots = additionalSourceRoots;
        this.jarPaths = jarPaths;
        this.parseThreads = Math.max(1, parseThreads);
    }

    /**
     * Creates a fully configured {@link JavaParser} instance for the given source root.
     * Each call produces an independent parser with its own {@link CombinedTypeSolver} —
     * safe to use from a dedicated thread without sharing state with other threads.
     */
    private JavaParser createParser(Path sourceRoot) {
        return createParser(sourceRoot, ParserConfiguration.LanguageLevel.JAVA_21);
    }

    /**
     * Creates a parser with the given language level.
     *
     * Two-pass strategy used by {@link #doParse}:
     * <ol>
     *   <li>JAVA_21 — recognises {@code yield} as a keyword (Java 14+), handles all modern
     *       switch expression syntax. Fails on files that use {@code _} as an identifier
     *       because JavaParser treats {@code _} as reserved since JAVA_9.</li>
     *   <li>RAW — no reserved keywords beyond the Java 1.0 set, so {@code _} is a valid
     *       identifier. Does NOT treat {@code yield} as a keyword, but the token is still
     *       parseable as an identifier so these files degrade gracefully (body extraction
     *       still works even if switch-expression semantics aren't fully modelled).</li>
     * </ol>
     */
    private JavaParser createParser(Path sourceRoot, ParserConfiguration.LanguageLevel level) {
        CombinedTypeSolver solver = new CombinedTypeSolver();
        // JDK types — false = don't try to resolve user classes via reflection
        solver.add(new ReflectionTypeSolver(false));
        if (Files.exists(sourceRoot)) {
            solver.add(new JavaParserTypeSolver(sourceRoot));
        }
        for (Path root : additionalSourceRoots) {
            if (Files.exists(root)) {
                solver.add(new JavaParserTypeSolver(root));
            }
        }
        for (Path jar : jarPaths) {
            if (Files.exists(jar)) {
                try {
                    solver.add(new JarTypeSolver(jar));
                } catch (IOException e) {
                    log.warn("Could not load jar for type solving: {}", jar);
                }
            }
        }
        ParserConfiguration cfg = new ParserConfiguration()
                .setSymbolResolver(new JavaSymbolSolver(solver))
                .setLanguageLevel(level)
                .setAttributeComments(true);
        return new JavaParser(cfg);
    }


    @Override
    public ParsedFile parseFile(Path file, String projectName) throws IOException {
        Path sourceRoot = findSourceRoot(file);
        // Reuse cached parsers for this source root — avoids rebuilding CombinedTypeSolver
        // (which re-loads JDK reflection + JavaParserTypeSolver caches) on every dirty file.
        JavaParser[] pair = parserCache.computeIfAbsent(sourceRoot, root -> new JavaParser[]{
                createParser(root, ParserConfiguration.LanguageLevel.JAVA_21),
                createParser(root, ParserConfiguration.LanguageLevel.RAW)
        });
        return doParseWithFallback(pair[0], pair[1], file, projectName);
    }

    @Override
    public ParsedProject parseProject(Path projectRoot, String projectName) throws IOException {
        Path sourceRoot = detectSourceRoot(projectRoot);
        log.info("Indexing project '{}' from {} (source root: {}, parseThreads: {})",
                projectName, projectRoot, sourceRoot, parseThreads);

        // Collect all .java files first so we can distribute them across threads
        List<Path> javaFiles = new ArrayList<>();
        Files.walkFileTree(sourceRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (file.toString().endsWith(".java")) javaFiles.add(file);
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                log.debug("Cannot read {}: {}", file, exc.getMessage());
                return FileVisitResult.CONTINUE;
            }
        });

        List<ParsedFile> parsedFiles = new CopyOnWriteArrayList<>();
        AtomicInteger fileCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        if (parseThreads <= 1) {
            // Single-threaded path — create parsers once and reuse across all files
            JavaParser java21Parser = createParser(sourceRoot, ParserConfiguration.LanguageLevel.JAVA_21);
            JavaParser rawParser    = createParser(sourceRoot, ParserConfiguration.LanguageLevel.RAW);
            for (Path file : javaFiles) {
                try {
                    parsedFiles.add(doParseWithFallback(java21Parser, rawParser, file, projectName));
                    fileCount.incrementAndGet();
                } catch (Throwable e) {
                    log.warn("Parse error in {}: {}", file, e.getMessage());
                    errorCount.incrementAndGet();
                }
            }
        } else {
            // Multi-threaded path — each thread gets its own parsers via ThreadLocal.
            // JavaParserTypeSolver has internal mutable caches, so per-thread instances
            // are required to avoid data races.
            ThreadLocal<JavaParser> threadParserJava21 = ThreadLocal.withInitial(
                    () -> createParser(sourceRoot, ParserConfiguration.LanguageLevel.JAVA_21));
            ThreadLocal<JavaParser> threadParserRaw = ThreadLocal.withInitial(
                    () -> createParser(sourceRoot, ParserConfiguration.LanguageLevel.RAW));

            ExecutorService pool = Executors.newFixedThreadPool(parseThreads,
                    r -> { Thread t = new Thread(r, "java-parser"); t.setDaemon(true); return t; });
            try {
                List<Future<ParsedFile>> futures = new ArrayList<>(javaFiles.size());
                for (Path file : javaFiles) {
                    futures.add(pool.submit(() -> {
                        try {
                            ParsedFile pf = doParseWithFallback(
                                    threadParserJava21.get(), threadParserRaw.get(), file, projectName);
                            fileCount.incrementAndGet();
                            return pf;
                        } catch (Exception e) {
                            log.debug("Parse error in {}: {}", file, e.getMessage());
                            errorCount.incrementAndGet();
                            return null;
                        }
                    }));
                }
                for (Future<ParsedFile> f : futures) {
                    try {
                        ParsedFile pf = f.get();
                        if (pf != null) parsedFiles.add(pf);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Parsing interrupted", e);
                    } catch (ExecutionException e) {
                        log.debug("Unexpected parse failure: {}", e.getCause().getMessage());
                        errorCount.incrementAndGet();
                    }
                }
            } finally {
                pool.shutdown();
            }
        }

        log.info("Parsed {} files ({} errors) in project '{}'", fileCount.get(), errorCount.get(), projectName);
        return new ParsedProject(projectName, projectRoot.toString(), parsedFiles);
    }

    /**
     * Two-pass parse: try JAVA_21 first (supports {@code yield} in switch expressions),
     * fall back to RAW if it fails (RAW allows {@code _} as an identifier, which JAVA_21
     * rejects because JavaParser reserves {@code _} since JAVA_9).
     */
    private ParsedFile doParseWithFallback(Path sourceRoot, Path file, String projectName)
            throws IOException {
        JavaParser java21 = createParser(sourceRoot, ParserConfiguration.LanguageLevel.JAVA_21);
        JavaParser raw    = createParser(sourceRoot, ParserConfiguration.LanguageLevel.RAW);
        return doParseWithFallback(java21, raw, file, projectName);
    }

    private ParsedFile doParseWithFallback(JavaParser java21Parser, JavaParser rawParser,
                                            Path file, String projectName) throws IOException {
        ParseResult<CompilationUnit> result = java21Parser.parse(file);
        if (result.isSuccessful() && result.getResult().isPresent()) {
            return buildParsedFile(result.getResult().get(), file, projectName);
        }
        // JAVA_21 failed — retry with RAW (handles _ as identifier and other pre-14 patterns)
        log.debug("JAVA_21 parse failed for {}, retrying with RAW: {}", file,
                result.getProblems().stream().map(Object::toString)
                        .collect(java.util.stream.Collectors.joining("; ")));
        ParseResult<CompilationUnit> rawResult = rawParser.parse(file);
        if (!rawResult.isSuccessful() || rawResult.getResult().isEmpty()) {
            throw new IOException("Parse failed for " + file + ": " +
                    rawResult.getProblems().stream().map(Object::toString)
                            .collect(java.util.stream.Collectors.joining("; ")));
        }
        return buildParsedFile(rawResult.getResult().get(), file, projectName);
    }

    private ParsedFile doParse(JavaParser parser, Path file, String projectName) throws IOException {
        ParseResult<CompilationUnit> result = parser.parse(file);
        if (!result.isSuccessful() || result.getResult().isEmpty()) {
            throw new IOException("Parse failed for " + file + ": " +
                    result.getProblems().stream().map(Object::toString)
                            .collect(java.util.stream.Collectors.joining("; ")));
        }
        return buildParsedFile(result.getResult().get(), file, projectName);
    }

    private ParsedFile buildParsedFile(CompilationUnit cu, Path file, String projectName) {
        String packageName = cu.getPackageDeclaration()
                .map(p -> p.getNameAsString())
                .orElse("");

        List<String> imports = cu.getImports().stream()
                .map(i -> i.getNameAsString())
                .toList();

        String filePath = file.toAbsolutePath().toString();

        // First pass: collect all class declarations
        List<ParsedClass> classes = new ArrayList<>();
        ClassVisitor classVisitor = new ClassVisitor(projectName, packageName, filePath);
        classVisitor.visit(cu, classes);

        // Second pass: for each class, collect its methods using that class as context
        List<ParsedMethod> methods = new ArrayList<>();
        for (TypeDeclaration<?> type : cu.getTypes()) {
            visitTypeForMethods(type, classes, packageName, projectName, filePath, methods);
        }

        return new ParsedFile(filePath, packageName, imports, classes, methods);
    }

    private void visitTypeForMethods(TypeDeclaration<?> type, List<ParsedClass> classes,
                                      String packageName, String projectName, String filePath,
                                      List<ParsedMethod> methods) {
        ParsedClass ctx = findClassContext(classes, type, packageName);
        if (ctx == null) return;

        // Use accept() to dispatch via double-dispatch (bypasses wildcard type restriction)
        MethodVisitor methodVisitor = new MethodVisitor(projectName, filePath, ctx);
        type.accept(methodVisitor, methods);

        // Recurse into nested types
        type.getMembers().stream()
                .filter(m -> m instanceof TypeDeclaration)
                .map(m -> (TypeDeclaration<?>) m)
                .forEach(nested -> visitTypeForMethods(nested, classes, packageName, projectName, filePath, methods));
    }

    private ParsedClass findClassContext(List<ParsedClass> classes, TypeDeclaration<?> type, String packageName) {
        String simpleName = type.getNameAsString();
        return classes.stream()
                .filter(c -> c.className().equals(simpleName))
                .findFirst()
                .orElseGet(() -> {
                    // Fallback: create minimal context
                    String qualified = packageName.isEmpty() ? simpleName : packageName + "." + simpleName;
                    return new com.pharos.parser.model.ParsedClass(
                            "", packageName, simpleName, qualified,
                            "class", null, List.of(), List.of(),
                            "package-private", false, false, null, "", 0, 0);
                });
    }

    /** Detect the Java source root for a project (tries common Maven/Gradle layouts). */
    public static Path detectSourceRoot(Path projectRoot) {
        // Maven and Gradle both use src/main/java
        Path mavenMain = projectRoot.resolve("src/main/java");
        if (Files.exists(mavenMain)) return mavenMain;

        // Try src/ for simpler project layouts
        Path src = projectRoot.resolve("src");
        if (Files.exists(src)) return src;

        // Fall back to project root
        return projectRoot;
    }

    /** Find source root for a single file by walking up to find a package-consistent root. */
    private Path findSourceRoot(Path file) {
        Path parent = file.getParent();
        if (parent == null) return file;
        // Walk up — for a file in src/main/java/com/example/Foo.java, return src/main/java
        // Simple heuristic: find the directory that is NOT a Java package name
        Path current = parent;
        while (current != null) {
            String dirName = current.getFileName() != null ? current.getFileName().toString() : "";
            if (dirName.equals("java") || dirName.equals("src") || dirName.isEmpty()) {
                return current;
            }
            current = current.getParent();
        }
        return parent;
    }

    /**
     * Parse a pre-collected list of Java files, reusing the same parser instances across
     * the entire batch.  This avoids recreating {@link CombinedTypeSolver} (which re-loads
     * JDK reflection caches) for every file when called from the shared-manifest path.
     */
    @Override
    public ParsedProject parseFiles(List<Path> files, Path projectRoot,
                                     String projectName) throws IOException {
        if (files.isEmpty()) {
            return new ParsedProject(projectName, projectRoot.toString(), List.of());
        }
        Path sourceRoot = detectSourceRoot(projectRoot);
        List<ParsedFile> results = new CopyOnWriteArrayList<>();
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicInteger fileCount = new AtomicInteger(0);

        if (parseThreads <= 1) {
            JavaParser java21Parser = createParser(sourceRoot, ParserConfiguration.LanguageLevel.JAVA_21);
            JavaParser rawParser    = createParser(sourceRoot, ParserConfiguration.LanguageLevel.RAW);
            for (Path file : files) {
                try {
                    results.add(doParseWithFallback(java21Parser, rawParser, file, projectName));
                    fileCount.incrementAndGet();
                } catch (Throwable e) {
                    log.warn("Parse error in {}: {}", file, e.getMessage());
                    errorCount.incrementAndGet();
                }
            }
        } else {
            ThreadLocal<JavaParser> threadParserJava21 = ThreadLocal.withInitial(
                    () -> createParser(sourceRoot, ParserConfiguration.LanguageLevel.JAVA_21));
            ThreadLocal<JavaParser> threadParserRaw = ThreadLocal.withInitial(
                    () -> createParser(sourceRoot, ParserConfiguration.LanguageLevel.RAW));
            ExecutorService pool = Executors.newFixedThreadPool(parseThreads,
                    r -> { Thread t = new Thread(r, "java-parser"); t.setDaemon(true); return t; });
            try {
                List<Future<ParsedFile>> futures = new ArrayList<>(files.size());
                for (Path file : files) {
                    futures.add(pool.submit(() -> {
                        try {
                            ParsedFile pf = doParseWithFallback(
                                    threadParserJava21.get(), threadParserRaw.get(), file, projectName);
                            fileCount.incrementAndGet();
                            return pf;
                        } catch (Exception e) {
                            log.debug("Parse error in {}: {}", file, e.getMessage());
                            errorCount.incrementAndGet();
                            return null;
                        }
                    }));
                }
                for (Future<ParsedFile> f : futures) {
                    try {
                        ParsedFile pf = f.get();
                        if (pf != null) results.add(pf);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Parsing interrupted", e);
                    } catch (ExecutionException e) {
                        log.debug("Unexpected parse failure: {}", e.getCause().getMessage());
                        errorCount.incrementAndGet();
                    }
                }
            } finally {
                pool.shutdown();
            }
        }
        log.info("parseFiles: {} files ({} errors) in project '{}'",
                fileCount.get(), errorCount.get(), projectName);
        return new ParsedProject(projectName, projectRoot.toString(), results);
    }

    @Override
    public List<String> supportedExtensions() {
        return EXTENSIONS;
    }

    // -------------------------------------------------------------------------
    // Relationship extraction
    // -------------------------------------------------------------------------

    private static final Set<String> PRIMITIVE_TYPES = Set.of(
            "void", "boolean", "byte", "char", "short", "int", "long", "float", "double");

    private static final Set<String> JAVA_LANG_SKIP = Set.of(
            "String", "Object", "Number", "Boolean", "Integer", "Long", "Double", "Float",
            "Short", "Byte", "Character", "Void", "Enum", "Record",
            "Override", "Deprecated", "SuppressWarnings", "FunctionalInterface", "SafeVarargs");

    @Override
    public ParsedRelationships buildRelationships(ParsedProject project) {
        // Build project-wide lookup: simpleName → qualifiedName
        Map<String, String> simpleToQual = new HashMap<>();
        for (ParsedClass cls : project.allClasses()) {
            simpleToQual.putIfAbsent(cls.className(), cls.qualifiedClassName());
        }

        List<ParsedRelationships.TypeEdge>   inherits         = new ArrayList<>();
        List<ParsedRelationships.TypeEdge>   implementsEdges  = new ArrayList<>();
        List<ParsedRelationships.TypeEdge>   returns          = new ArrayList<>();
        List<ParsedRelationships.TypeEdge>   takes            = new ArrayList<>();
        List<ParsedRelationships.TypeEdge>   annotatedBy      = new ArrayList<>();
        List<ParsedRelationships.FieldDecl>  fields           = new ArrayList<>();
        List<ParsedRelationships.FieldAccess> reads           = new ArrayList<>();
        List<ParsedRelationships.FieldAccess> writes          = new ArrayList<>();

        for (ParsedFile file : project.files()) {
            // Build per-file import map: simpleName → qualifiedName
            Map<String, String> importMap = buildImportMap(file.imports());

            for (ParsedClass cls : file.classes()) {
                String clsFqn = cls.qualifiedClassName();

                // Inheritance
                if (cls.superclass() != null && !cls.superclass().isBlank()) {
                    String superFqn = resolve(cls.superclass(), clsFqn, simpleToQual, importMap);
                    if (superFqn != null) inherits.add(new ParsedRelationships.TypeEdge(clsFqn, superFqn));
                }
                for (String iface : cls.interfaces()) {
                    String ifaceFqn = resolve(iface, clsFqn, simpleToQual, importMap);
                    if (ifaceFqn != null) implementsEdges.add(new ParsedRelationships.TypeEdge(clsFqn, ifaceFqn));
                }

                // Class annotations
                for (String ann : cls.annotations()) {
                    if (!JAVA_LANG_SKIP.contains(ann)) {
                        String annFqn = resolve(ann, clsFqn, simpleToQual, importMap);
                        annotatedBy.add(new ParsedRelationships.TypeEdge(clsFqn,
                                annFqn != null ? annFqn : ann));
                    }
                }
            }

            for (ParsedMethod method : file.methods()) {
                String methodFqn = method.fqn();

                // Return type
                String rt = method.returnType();
                if (rt != null && !PRIMITIVE_TYPES.contains(rt) && !rt.isBlank() && !rt.equals("void")) {
                    String base = stripGenerics(rt);
                    if (!JAVA_LANG_SKIP.contains(base)) {
                        String rtFqn = resolve(base, method.qualifiedClassName(), simpleToQual, importMap);
                        returns.add(new ParsedRelationships.TypeEdge(methodFqn,
                                rtFqn != null ? rtFqn : base));
                    }
                }

                // Parameter types
                for (String pt : method.paramTypes()) {
                    String base = stripGenerics(pt);
                    if (!base.isBlank() && !PRIMITIVE_TYPES.contains(base) && !JAVA_LANG_SKIP.contains(base)) {
                        String ptFqn = resolve(base, method.qualifiedClassName(), simpleToQual, importMap);
                        takes.add(new ParsedRelationships.TypeEdge(methodFqn,
                                ptFqn != null ? ptFqn : base));
                    }
                }

                // Method annotations
                for (String ann : method.annotations()) {
                    if (!JAVA_LANG_SKIP.contains(ann)) {
                        String annFqn = resolve(ann, method.qualifiedClassName(), simpleToQual, importMap);
                        annotatedBy.add(new ParsedRelationships.TypeEdge(methodFqn,
                                annFqn != null ? annFqn : ann));
                    }
                }
            }

            // Field declarations and accesses — targeted re-parse of this file's AST
            extractFieldData(file, simpleToQual, importMap, fields, reads, writes);
        }

        return new ParsedRelationships(project.projectName(),
                inherits, implementsEdges, fields, reads, writes, returns, takes, annotatedBy);
    }

    /** Re-parses a file (reusing cached solver) to collect field declarations and accesses. */
    private void extractFieldData(ParsedFile file,
                                   Map<String, String> simpleToQual,
                                   Map<String, String> importMap,
                                   List<ParsedRelationships.FieldDecl>  fields,
                                   List<ParsedRelationships.FieldAccess> reads,
                                   List<ParsedRelationships.FieldAccess> writes) {
        Path filePath = Path.of(file.filePath());
        if (!Files.exists(filePath)) return;
        try {
            Path sourceRoot = findSourceRoot(filePath);
            JavaParser[] parsers = parserCache.computeIfAbsent(sourceRoot, root -> new JavaParser[]{
                    createParser(root, ParserConfiguration.LanguageLevel.JAVA_21),
                    createParser(root, ParserConfiguration.LanguageLevel.RAW)
            });
            ParseResult<CompilationUnit> result = parsers[0].parse(filePath);
            if (!result.isSuccessful()) result = parsers[1].parse(filePath);
            if (!result.isSuccessful() || result.getResult().isEmpty()) return;
            CompilationUnit cu = result.getResult().get();

            // Field declarations — visit every class/interface in the file
            cu.findAll(ClassOrInterfaceDeclaration.class).forEach(classDecl -> {
                String classQual = buildQualifiedNameFromCu(file.packageName(), classDecl);
                for (FieldDeclaration fd : classDecl.getFields()) {
                    String access = fd.getAccessSpecifier().asString().toLowerCase();
                    if (access.isEmpty()) access = "package-private";
                    String fieldType = fd.getVariable(0).getTypeAsString();
                    for (VariableDeclarator vd : fd.getVariables()) {
                        String fieldFqn = classQual + "#" + vd.getNameAsString();
                        fields.add(new ParsedRelationships.FieldDecl(
                                fieldFqn, classQual, stripGenerics(fieldType), access));
                    }
                }
            });

            // Field accesses — only explicit this.field and super.field (reliable without full resolution)
            cu.findAll(MethodDeclaration.class).forEach(md -> {
                md.getBody().ifPresent(body -> {
                    String ownerQual = md.findAncestor(ClassOrInterfaceDeclaration.class)
                            .map(c -> buildQualifiedNameFromCu(file.packageName(), c))
                            .orElse(null);
                    if (ownerQual == null) return;

                    List<String> paramNames = md.getParameters().stream()
                            .map(p -> p.getNameAsString()).toList();
                    String methodFqn = ownerQual + "#" + md.getNameAsString() + "(" +
                            md.getParameters().stream().map(p -> p.getTypeAsString())
                                    .collect(java.util.stream.Collectors.joining(",")) + ")";

                    collectFieldAccesses(body, methodFqn, ownerQual, paramNames, reads, writes);
                });
            });
            // Also handle constructors
            cu.findAll(ConstructorDeclaration.class).forEach(cd -> {
                String ownerQual = cd.findAncestor(ClassOrInterfaceDeclaration.class)
                        .map(c -> buildQualifiedNameFromCu(file.packageName(), c))
                        .orElse(null);
                if (ownerQual == null) return;
                List<String> paramNames = cd.getParameters().stream()
                        .map(p -> p.getNameAsString()).toList();
                String methodFqn = ownerQual + "#<init>(" +
                        cd.getParameters().stream().map(p -> p.getTypeAsString())
                                .collect(java.util.stream.Collectors.joining(",")) + ")";
                collectFieldAccesses(cd.getBody(), methodFqn, ownerQual, paramNames, reads, writes);
            });

        } catch (Exception e) {
            log.debug("Field extraction failed for {}: {}", file.filePath(), e.getMessage());
        }
    }

    private void collectFieldAccesses(BlockStmt body, String methodFqn, String ownerQual,
                                       List<String> paramNames,
                                       List<ParsedRelationships.FieldAccess> reads,
                                       List<ParsedRelationships.FieldAccess> writes) {
        // Track local variable declarations to exclude them from implicit field access
        Set<String> localVars = new HashSet<>(paramNames);
        body.findAll(VariableDeclarator.class).forEach(vd -> localVars.add(vd.getNameAsString()));

        // Explicit this.field / super.field accesses
        body.findAll(FieldAccessExpr.class).forEach(fae -> {
            Expression scope = fae.getScope();
            if (!(scope instanceof ThisExpr) && !(scope instanceof SuperExpr)) return;
            String fieldFqn = ownerQual + "#" + fae.getNameAsString();
            boolean isWrite = isAssignmentTarget(fae);
            (isWrite ? writes : reads).add(new ParsedRelationships.FieldAccess(methodFqn, fieldFqn));
        });

        // Implicit field access: bare name that is NOT a local/param variable
        body.findAll(NameExpr.class).forEach(ne -> {
            String name = ne.getNameAsString();
            if (name.length() <= 1 || localVars.contains(name)) return;
            // Try symbol resolution — only adds if JavaParser confirms it's a field
            try {
                var resolved = ne.resolve();
                if (!resolved.isField()) return;
                String declClass = resolved.asField().declaringType().getQualifiedName();
                String fieldFqn = declClass + "#" + name;
                boolean isWrite = isAssignmentTarget(ne);
                (isWrite ? writes : reads).add(new ParsedRelationships.FieldAccess(methodFqn, fieldFqn));
            } catch (Exception ignored) {}
        });
    }

    private static boolean isAssignmentTarget(Expression expr) {
        var parent = expr.getParentNode().orElse(null);
        return parent instanceof AssignExpr ae && ae.getTarget() == expr;
    }

    private static String buildQualifiedNameFromCu(String packageName, ClassOrInterfaceDeclaration n) {
        List<String> parts = new ArrayList<>();
        parts.add(n.getNameAsString());
        com.github.javaparser.ast.Node parent = n.getParentNode().orElse(null);
        while (parent instanceof ClassOrInterfaceDeclaration enclosing) {
            parts.add(0, enclosing.getNameAsString());
            parent = enclosing.getParentNode().orElse(null);
        }
        String classPath = String.join(".", parts);
        return packageName == null || packageName.isEmpty() ? classPath : packageName + "." + classPath;
    }

    private static Map<String, String> buildImportMap(List<String> imports) {
        Map<String, String> map = new HashMap<>();
        for (String imp : imports) {
            // "import com.example.Foo" → "Foo" → "com.example.Foo"
            String stripped = imp.trim()
                    .replaceFirst("^import\\s+(static\\s+)?", "")
                    .replaceFirst(";$", "").trim();
            if (stripped.endsWith(".*") || stripped.isEmpty()) continue;
            int dot = stripped.lastIndexOf('.');
            if (dot >= 0) map.put(stripped.substring(dot + 1), stripped);
        }
        return map;
    }

    /** Strip generic parameters and array brackets: {@code List<String>} → {@code List}. */
    private static String stripGenerics(String type) {
        if (type == null) return "";
        int lt = type.indexOf('<');
        String base = lt >= 0 ? type.substring(0, lt) : type;
        return base.replace("[]", "").replace("...", "").trim();
    }

    /**
     * Resolve a simple or partially-qualified type name to a project-known FQN.
     * Resolution order: exact project class match → import map → return null.
     */
    private static String resolve(String name, String contextClass,
                                   Map<String, String> simpleToQual,
                                   Map<String, String> importMap) {
        if (name == null || name.isBlank()) return null;
        String stripped = stripGenerics(name);
        // Already qualified (contains a dot and not a primitive)
        if (stripped.contains(".")) return stripped;
        // Import map
        String fromImport = importMap.get(stripped);
        if (fromImport != null) return fromImport;
        // Project class map
        String fromProject = simpleToQual.get(stripped);
        if (fromProject != null) return fromProject;
        return null; // unresolvable
    }
}
