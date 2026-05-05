package com.pharos.parser;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.*;
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
                .setLanguageLevel(ParserConfiguration.LanguageLevel.RAW)
                .setAttributeComments(true);
        return new JavaParser(cfg);
    }


    @Override
    public ParsedFile parseFile(Path file, String projectName) throws IOException {
        // Single-file incremental path — create a fresh parser instance (not shared)
        Path sourceRoot = findSourceRoot(file);
        JavaParser parser = createParser(sourceRoot);
        return doParse(parser, file, projectName);
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
            // Single-threaded path — one parser for all files
            JavaParser parser = createParser(sourceRoot);
            for (Path file : javaFiles) {
                try {
                    parsedFiles.add(doParse(parser, file, projectName));
                    fileCount.incrementAndGet();
                } catch (Exception e) {
                    log.debug("Parse error in {}: {}", file, e.getMessage());
                    errorCount.incrementAndGet();
                }
            }
        } else {
            // Multi-threaded path — each thread gets its own JavaParser via ThreadLocal.
            // JavaParserTypeSolver has internal mutable caches, so per-thread instances
            // are required to avoid data races.
            ThreadLocal<JavaParser> threadParser = ThreadLocal.withInitial(() -> createParser(sourceRoot));

            ExecutorService pool = Executors.newFixedThreadPool(parseThreads,
                    r -> { Thread t = new Thread(r, "java-parser"); t.setDaemon(true); return t; });
            try {
                List<Future<ParsedFile>> futures = new ArrayList<>(javaFiles.size());
                for (Path file : javaFiles) {
                    futures.add(pool.submit(() -> {
                        try {
                            ParsedFile pf = doParse(threadParser.get(), file, projectName);
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

    private ParsedFile doParse(JavaParser parser, Path file, String projectName) throws IOException {
        ParseResult<CompilationUnit> result = parser.parse(file);
        if (!result.isSuccessful() || result.getResult().isEmpty()) {
            throw new IOException("Parse failed for " + file + ": " +
                    result.getProblems().stream().map(Object::toString)
                            .collect(java.util.stream.Collectors.joining("; ")));
        }
        CompilationUnit cu = result.getResult().get();

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

    @Override
    public List<String> supportedExtensions() {
        return EXTENSIONS;
    }
}
