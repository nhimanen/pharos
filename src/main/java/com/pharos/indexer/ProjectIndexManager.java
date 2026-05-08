package com.pharos.indexer;

import com.pharos.analysis.ConceptMiner;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.CallGraphBuilder;
import com.pharos.graph.CallGraphSerializer;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.parser.CodeParser;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import com.pharos.parser.model.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Orchestrates the full pipeline: parse → embed → index → graph → registry.
 * Supports both full and incremental indexing modes, and multiple languages
 * via pluggable {@link CodeParser} implementations.
 */
public class ProjectIndexManager {

    private static final Logger log = LoggerFactory.getLogger(ProjectIndexManager.class);

    private final IndexConfig config;
    private final LuceneIndexer luceneIndexer;
    private final ProjectRegistry registry;
    private final EmbeddingProvider embedder;
    private final ModuleGraphBuilder moduleGraphBuilder;
    /** Ordered list of language parsers; first match by extension wins. */
    private final List<CodeParser> parsers;

    public ProjectIndexManager(IndexConfig config, LuceneIndexer luceneIndexer,
                                ProjectRegistry registry, EmbeddingProvider embedder) {
        this(config, luceneIndexer, registry, embedder, new ModuleGraphBuilder(registry));
    }

    public ProjectIndexManager(IndexConfig config, LuceneIndexer luceneIndexer,
                                ProjectRegistry registry, EmbeddingProvider embedder,
                                ModuleGraphBuilder moduleGraphBuilder) {
        this(config, luceneIndexer, registry, embedder, moduleGraphBuilder,
                List.of(new JavaCodeParser()));
    }

    public ProjectIndexManager(IndexConfig config, LuceneIndexer luceneIndexer,
                                ProjectRegistry registry, EmbeddingProvider embedder,
                                ModuleGraphBuilder moduleGraphBuilder,
                                List<CodeParser> parsers) {
        this.config = config;
        this.luceneIndexer = luceneIndexer;
        this.registry = registry;
        this.embedder = embedder;
        this.moduleGraphBuilder = moduleGraphBuilder;
        this.parsers = List.copyOf(parsers);
    }

    /** Returns the union of all extensions supported by registered parsers. */
    private Set<String> allSupportedExtensions() {
        return parsers.stream()
                .flatMap(p -> p.supportedExtensions().stream())
                .collect(Collectors.toSet());
    }

    /** Returns the parser that handles the given file, or null if none match. */
    private CodeParser parserFor(Path file) {
        String name = file.getFileName().toString();
        for (CodeParser p : parsers) {
            for (String ext : p.supportedExtensions()) {
                if (name.endsWith(ext)) return p;
            }
        }
        return null;
    }

    /** Merges multiple ParsedProject results (one per language parser) into one. */
    private static ParsedProject merge(String projectName, Path projectRoot,
                                        List<ParsedProject> projects) {
        List<ParsedFile> allFiles = projects.stream()
                .flatMap(p -> p.files().stream())
                .collect(Collectors.toList());
        return new ParsedProject(projectName, projectRoot.toString(), allFiles);
    }

    /** Returns true if a Lucene index already exists on disk for {@code projectName}. */
    public boolean indexExists(String projectName) {
        return luceneIndexer.indexExists(projectName);
    }

    /**
     * Index a project.
     *
     * @param projectRoot        path to the project root directory
     * @param projectName        logical name (used as index key)
     * @param incremental        if true, only re-index changed files
     * @param generateEmbeddings if true, compute vector embeddings
     */
    public ProjectMeta index(Path projectRoot, String projectName,
                              boolean incremental, boolean generateEmbeddings) throws IOException {
        return index(projectRoot, projectName, incremental, generateEmbeddings, ProgressListener.SILENT);
    }

    /**
     * Index a project with live progress callbacks.
     *
     * @param projectRoot        path to the project root directory
     * @param projectName        logical name (used as index key)
     * @param incremental        if true, only re-index changed files
     * @param generateEmbeddings if true, compute vector embeddings
     * @param progress           listener that receives stage/count/total updates
     */
    public ProjectMeta index(Path projectRoot, String projectName,
                              boolean incremental, boolean generateEmbeddings,
                              ProgressListener progress) throws IOException {
        return index(projectRoot, projectName, incremental, false, generateEmbeddings, progress);
    }

    /**
     * Index a project with optional force-wipe of the existing index.
     *
     * @param force if true, deletes the existing Lucene directory before indexing —
     *              required after Lucene version upgrades or to recover from corrupt indexes
     */
    public ProjectMeta index(Path projectRoot, String projectName,
                              boolean incremental, boolean force, boolean generateEmbeddings,
                              ProgressListener progress) throws IOException {
        if (force && luceneIndexer.indexExists(projectName)) {
            log.info("Force re-index: wiping existing index for '{}'", projectName);
            luceneIndexer.deleteIndex(projectName);
        }

        log.info("Starting {} index of project '{}' at {}",
                incremental ? "incremental" : "full", projectName, projectRoot);

        Path projectIndexDir = luceneIndexer.getProjectIndexDir(projectName);
        Files.createDirectories(projectIndexDir);

        FileStateTracker stateTracker = new FileStateTracker(projectIndexDir);

        if (incremental && luceneIndexer.indexExists(projectName)) {
            return indexIncremental(projectRoot, projectName, projectIndexDir,
                    stateTracker, generateEmbeddings, progress);
        } else {
            return indexFull(projectRoot, projectName, projectIndexDir,
                    stateTracker, generateEmbeddings, progress);
        }
    }

    private ProjectMeta indexFull(Path projectRoot, String projectName,
                                   Path projectIndexDir, FileStateTracker stateTracker,
                                   boolean generateEmbeddings, ProgressListener progress) throws IOException {
        stateTracker.clear();

        // Single walkFileTree — collect all supported source files once, dispatch to parsers by extension.
        // This avoids N independent walks when N parsers are registered.
        progress.onProgress("Scanning", 0, 0);
        Set<String> supportedExts = allSupportedExtensions();
        Map<CodeParser, List<Path>> filesByParser = new java.util.LinkedHashMap<>();
        for (CodeParser p : parsers) filesByParser.put(p, new ArrayList<>());

        Set<String> skipDirs = Set.of(
                ".git", ".hg", ".svn",
                "node_modules", ".yarn", "vendor", ".cargo",
                "target", "build", "dist", "out", ".gradle",
                "__pycache__", ".tox", ".venv", "venv", ".env",
                ".idea", ".vscode",
                "logs", "log", "tmp", "temp", ".cache"
        );
        Files.walkFileTree(projectRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String name = dir.getFileName() != null ? dir.getFileName().toString() : "";
                if (skipDirs.contains(name) || name.startsWith(".")) return FileVisitResult.SKIP_SUBTREE;
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                for (CodeParser p : parsers) {
                    if (p.supportedExtensions().stream().anyMatch(name::endsWith)) {
                        filesByParser.get(p).add(file);
                        break;
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });

        // Parse the collected files using each parser (shared parsers across the batch)
        progress.onProgress("Parsing", 0, parsers.size());
        List<ParsedProject> perLang = new ArrayList<>();
        int parserIdx = 0;
        for (Map.Entry<CodeParser, List<Path>> entry : filesByParser.entrySet()) {
            perLang.add(entry.getKey().parseFiles(entry.getValue(), projectRoot, projectName));
            progress.onProgress("Parsing", ++parserIdx, parsers.size());
        }
        ParsedProject project = merge(projectName, projectRoot, perLang);

        // Pass 1: build call graph — needed to compute in-degrees and caller contexts
        progress.onProgress("Building call graph", 0, 0);
        CallGraph graph = buildAndSaveGraph(project, projectIndexDir);
        GraphIndexData graphData = GraphIndexData.build(graph, project);

        // Pass 2: write Lucene index with graph-derived fields
        try (IndexWriter writer = luceneIndexer.openWriterFresh(projectName)) {
            indexProject(writer, project, stateTracker, generateEmbeddings, graphData, progress);
            writer.commit();
        }

        // Save file state
        stateTracker.save();

        // Update registry
        ProjectMeta meta = buildMeta(projectName, projectRoot, projectIndexDir, project, graph);
        registry.register(meta);
        updateModuleGraph(projectRoot, meta);

        progress.onProgress("Done", meta.getFileCount(), meta.getFileCount());
        log.info("Full index complete for '{}': {} methods, {} classes, {} files",
                meta.getMethodCount(), meta.getClassCount(), meta.getFileCount(), projectName);

        expandSynonyms(projectName);
        return meta;
    }

    private ProjectMeta indexIncremental(Path projectRoot, String projectName,
                                          Path projectIndexDir, FileStateTracker stateTracker,
                                          boolean generateEmbeddings, ProgressListener progress) throws IOException {
        log.info("Incremental index: scanning for changes in '{}'", projectName);
        progress.onProgress("Scanning for changes", 0, 0);

        Set<String> supportedExts = allSupportedExtensions();
        List<Path> dirtyFiles = new ArrayList<>();

        // Collect all supported source files that currently exist under the project root.
        // Skip noise directories that parsers would also skip — avoids counting .git objects,
        // vendored deps, build artifacts, and log/data directories as source files.
        Set<String> skipDirs = Set.of(
                ".git", ".hg", ".svn",
                "node_modules", ".yarn", "vendor", ".cargo",
                "target", "build", "dist", "out", ".gradle",
                "__pycache__", ".tox", ".venv", "venv", ".env",
                ".idea", ".vscode",
                "logs", "log", "tmp", "temp", ".cache"
        );
        Set<Path> currentFiles = new java.util.HashSet<>();
        Files.walkFileTree(projectRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String dirName = dir.getFileName() != null ? dir.getFileName().toString() : "";
                if (skipDirs.contains(dirName) || dirName.startsWith(".")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                boolean supported = supportedExts.stream().anyMatch(name::endsWith);
                if (supported) {
                    currentFiles.add(file.toAbsolutePath());
                    if (stateTracker.isDirty(file)) {
                        dirtyFiles.add(file);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });

        // Detect files that were tracked but no longer exist on disk.
        // Only consider files that the scanner *would* discover: under projectRoot and not in a
        // skipped directory.  Files tracked from a previous full-index run that live in dotdirs
        // or noise dirs (e.g. .gradle/, logs/) are invisible to the scanner and must not be
        // treated as "deleted" — doing so creates endless churn where they are removed from the
        // index every incremental run and re-added by the graph-rebuild full parse.
        List<Path> deletedFiles = stateTracker.trackedFiles().stream()
                .filter(p -> {
                    String n = p.getFileName() != null ? p.getFileName().toString() : "";
                    if (!supportedExts.stream().anyMatch(n::endsWith)) return false;
                    if (currentFiles.contains(p)) return false;
                    // Only flag as deleted if the file was in a discoverable location
                    if (!p.startsWith(projectRoot)) return false;
                    // Check none of the path segments are in the skip list or start with '.'
                    for (int i = projectRoot.getNameCount(); i < p.getNameCount() - 1; i++) {
                        String seg = p.getName(i).toString();
                        if (skipDirs.contains(seg) || seg.startsWith(".")) return false;
                    }
                    return true;
                })
                .collect(Collectors.toList());

        if (dirtyFiles.isEmpty() && deletedFiles.isEmpty()) {
            log.info("No changes detected for '{}', index is up to date", projectName);
            return registry.find(projectName).orElseThrow();
        }

        log.info("Re-indexing {} changed file(s), removing {} deleted file(s) in '{}'",
                dirtyFiles.size(), deletedFiles.size(), projectName);
        int totalWork = dirtyFiles.size() + deletedFiles.size();
        int doneWork = 0;

        // Collect parsed dirty files so we can use them for graph patching later (avoids re-parse)
        List<ParsedFile> parsedDirtyFiles = new ArrayList<>();

        try (IndexWriter writer = luceneIndexer.openWriter(projectName)) {
            // Remove documents for deleted source files
            for (Path deleted : deletedFiles) {
                writer.deleteDocuments(new Term(DocumentMapper.F_FILE_PATH, deleted.toString()));
                stateTracker.remove(deleted);
                log.debug("Removed index docs for deleted file {}", deleted.getFileName());
                progress.onProgress("Removing deleted files", ++doneWork, totalWork);
            }

            for (Path file : dirtyFiles) {
                // Delete existing docs for this file (both methods and classes)
                writer.deleteDocuments(new Term(DocumentMapper.F_FILE_PATH, file.toAbsolutePath().toString()));

                // Re-parse and re-index using the matching language parser
                CodeParser fileParser = parserFor(file);
                if (fileParser == null) {
                    log.debug("No parser for {}, skipping", file.getFileName());
                    progress.onProgress("Indexing", ++doneWork, totalWork);
                    continue;
                }
                try {
                    ParsedFile parsedFile = fileParser.parseFile(file, projectName);
                    parsedDirtyFiles.add(parsedFile);

                    // Build embedding texts per file and batch them in one call
                    List<String> methodTexts = parsedFile.methods().stream()
                            .map(DocumentMapper::buildEmbeddingText)
                            .collect(Collectors.toList());
                    Map<String, List<ParsedMethod>> methodsByClass = parsedFile.methods().stream()
                            .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));
                    List<String> synthesizedBodies = parsedFile.classes().stream()
                            .map(cls -> buildSynthesizedBody(
                                    methodsByClass.getOrDefault(cls.qualifiedClassName(), List.of())))
                            .collect(Collectors.toList());
                    List<String> classTexts = new ArrayList<>(parsedFile.classes().size());
                    for (int ci = 0; ci < parsedFile.classes().size(); ci++) {
                        classTexts.add(DocumentMapper.buildClassEmbeddingText(
                                parsedFile.classes().get(ci), synthesizedBodies.get(ci)));
                    }

                    // Batch embed all methods + classes in this file in one ONNX pass
                    List<String> allTexts = new ArrayList<>(methodTexts.size() + classTexts.size());
                    allTexts.addAll(methodTexts);
                    allTexts.addAll(classTexts);
                    float[][] allEmbeddings = (generateEmbeddings && embedder.isAvailable())
                            ? embedder.embedBatch(allTexts)
                            : new float[allTexts.size()][];

                    // Re-index methods
                    for (int mi = 0; mi < parsedFile.methods().size(); mi++) {
                        ParsedMethod method = parsedFile.methods().get(mi);
                        float[] embedding = allEmbeddings[mi];
                        writer.addDocument(DocumentMapper.toDocument(method, embedding));
                    }

                    // Re-index classes
                    int classOffset = parsedFile.methods().size();
                    for (int ci = 0; ci < parsedFile.classes().size(); ci++) {
                        ParsedClass cls = parsedFile.classes().get(ci);
                        String synthesizedBody = synthesizedBodies.get(ci);
                        float[] embedding = allEmbeddings[classOffset + ci];
                        writer.addDocument(DocumentMapper.toClassDocument(cls, synthesizedBody, embedding));
                    }

                    List<String> classNames = parsedFile.classes().stream()
                            .map(ParsedClass::qualifiedClassName)
                            .collect(Collectors.toList());
                    stateTracker.track(file, classNames);
                    log.debug("Re-indexed {}", file.getFileName());
                } catch (Exception e) {
                    log.warn("Failed to re-index {}: {}", file, e.getMessage());
                }
                progress.onProgress("Indexing", ++doneWork, totalWork);
            }
            writer.commit();
        }

        stateTracker.save();

        // Patch call graph in-place — load existing graph, evict stale entries for
        // dirty/deleted files, splice in fresh edges from newly-parsed files.
        // This avoids a full project re-parse (which would double the parse cost on every
        // incremental run) while keeping the graph consistent.
        progress.onProgress("Patching call graph", 0, 0);
        CallGraph graph = patchGraph(projectIndexDir, parsedDirtyFiles, deletedFiles, stateTracker);

        // Build lightweight ProjectMeta from Lucene reader counts (no re-parse needed)
        ProjectMeta meta = buildMetaFromIndex(projectName, projectRoot, projectIndexDir, graph, currentFiles.size());
        // Inherit known packages and unresolved refs from the previous registry entry so
        // cross-project linking can still work without a full parse
        registry.find(projectName).ifPresent(prev -> {
            meta.setKnownPackages(prev.getKnownPackages());
            meta.setUnresolvedRefs(prev.getUnresolvedRefs());
            meta.setUnresolvedRefsHash(prev.getUnresolvedRefsHash());
        });
        registry.register(meta);
        updateModuleGraph(projectRoot, meta);

        progress.onProgress("Done", dirtyFiles.size(), dirtyFiles.size());
        log.info("Incremental index complete for '{}': {} files updated", projectName, dirtyFiles.size());

        // Only re-mine synonyms when content actually changed — synonym expansion does a full
        // Lucene scan + TF-IDF computation which is expensive to run for deletion-only updates.
        if (!dirtyFiles.isEmpty()) {
            expandSynonyms(projectName);
        }
        return meta;
    }

    private void indexProject(IndexWriter writer, ParsedProject project,
                               FileStateTracker stateTracker, boolean generateEmbeddings,
                               GraphIndexData graphData, ProgressListener progress) throws IOException {
        int indexThreads = config.resolvedIndexThreads();
        log.debug("Indexing with {} thread(s)", indexThreads);

        // Build method lookup per class for synthesized class bodies (read-only after construction)
        Map<String, List<ParsedMethod>> methodsByClass = project.allMethods().stream()
                .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));

        // Pre-compute synthesized bodies once per class. In the parallel path, multiple threads
        // may index methods from the same class — memoizing avoids redundant string concatenation.
        ConcurrentHashMap<String, String> synthesizedBodyCache = new ConcurrentHashMap<>();

        int totalFiles = project.files().size();
        AtomicInteger doneFiles = new AtomicInteger(0);
        AtomicInteger totalMethods = new AtomicInteger(0);

        if (indexThreads <= 1) {
            // Sequential path — preserves original behaviour exactly
            for (ParsedFile file : project.files()) {
                indexFile(writer, file, stateTracker, generateEmbeddings, graphData,
                        methodsByClass, synthesizedBodyCache);
                int done = doneFiles.incrementAndGet();
                totalMethods.addAndGet(file.methods().size());
                progress.onProgress("Indexing", done, totalFiles);
                if (totalMethods.get() % 500 == 0) {
                    log.debug("Indexed {} methods...", totalMethods.get());
                }
            }
        } else {
            // Parallel path:
            //   • IndexWriter.addDocument() is thread-safe in Lucene
            //   • EmbeddingProvider uses ThreadLocal Predictors (one per thread)
            //   • FileStateTracker uses ConcurrentHashMap — no external lock needed
            //   • synthesizedBodyCache is ConcurrentHashMap — safe for concurrent reads/writes
            ExecutorService pool = Executors.newFixedThreadPool(indexThreads,
                    r -> { Thread t = new Thread(r, "indexer"); t.setDaemon(true); return t; });
            try {
                List<Future<?>> futures = new ArrayList<>(totalFiles);
                for (ParsedFile file : project.files()) {
                    futures.add(pool.submit(() -> {
                        try {
                            indexFile(writer, file, stateTracker, generateEmbeddings,
                                    graphData, methodsByClass, synthesizedBodyCache);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        int done = doneFiles.incrementAndGet();
                        totalMethods.addAndGet(file.methods().size());
                        progress.onProgress("Indexing", done, totalFiles);
                        if (totalMethods.get() % 500 == 0) {
                            log.debug("Indexed {} methods...", totalMethods.get());
                        }
                        return null;
                    }));
                }
                // Await completion and surface any exceptions
                for (Future<?> f : futures) {
                    try {
                        f.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Indexing interrupted", e);
                    } catch (ExecutionException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof UncheckedIOException u) throw u.getCause();
                        if (cause instanceof IOException io) throw io;
                        throw new IOException("Indexing task failed: " + cause.getMessage(), cause);
                    }
                }
            } finally {
                pool.shutdown();
            }
        }
        log.debug("Total methods indexed: {}", totalMethods.get());
    }

    /**
     * Indexes a single file's methods and classes into the Lucene writer.
     * Safe to call from multiple threads simultaneously:
     *   - {@link IndexWriter#addDocument} is thread-safe
     *   - {@link FileStateTracker#track} uses ConcurrentHashMap internally
     *   - {@link EmbeddingProvider#embed} uses thread-local Predictors
     *   - {@code synthesizedBodyCache} is a ConcurrentHashMap — bodies are computed once per class
     */
    private void indexFile(IndexWriter writer, ParsedFile file,
                           FileStateTracker stateTracker, boolean generateEmbeddings,
                           GraphIndexData graphData,
                           Map<String, List<ParsedMethod>> methodsByClass,
                           ConcurrentHashMap<String, String> synthesizedBodyCache) throws IOException {
        // Resolve synthesized bodies for classes in this file (memoized across files in full index)
        List<String> synthesizedBodies = file.classes().stream()
                .map(cls -> synthesizedBodyCache.computeIfAbsent(
                        cls.qualifiedClassName(),
                        k -> buildSynthesizedBody(methodsByClass.getOrDefault(k, List.of()))))
                .collect(Collectors.toList());

        // Batch all embedding texts for this file into a single ONNX forward pass
        float[][] allEmbeddings;
        if (generateEmbeddings && embedder.isAvailable()) {
            List<String> methodTexts = file.methods().stream()
                    .map(DocumentMapper::buildEmbeddingText)
                    .collect(Collectors.toList());
            List<String> classTexts = new ArrayList<>(file.classes().size());
            for (int ci = 0; ci < file.classes().size(); ci++) {
                classTexts.add(DocumentMapper.buildClassEmbeddingText(
                        file.classes().get(ci), synthesizedBodies.get(ci)));
            }
            List<String> allTexts = new ArrayList<>(methodTexts.size() + classTexts.size());
            allTexts.addAll(methodTexts);
            allTexts.addAll(classTexts);
            try {
                allEmbeddings = embedder.embedBatch(allTexts);
            } catch (Exception e) {
                log.debug("Batch embedding failed for {}: {}", file.filePath(), e.getMessage());
                allEmbeddings = new float[allTexts.size()][];
            }
        } else {
            allEmbeddings = new float[file.methods().size() + file.classes().size()][];
        }

        for (int mi = 0; mi < file.methods().size(); mi++) {
            ParsedMethod method = file.methods().get(mi);
            float[] embedding = allEmbeddings[mi];
            int inDegree = graphData.inDegree(method.fqn());
            List<String> callerNames = graphData.callerSimpleNames(method.fqn());
            writer.addDocument(DocumentMapper.toDocument(method, embedding, inDegree, callerNames));
        }

        int classOffset = file.methods().size();
        for (int ci = 0; ci < file.classes().size(); ci++) {
            ParsedClass cls = file.classes().get(ci);
            String synthesizedBody = synthesizedBodies.get(ci);
            float[] embedding = allEmbeddings[classOffset + ci];
            writer.addDocument(DocumentMapper.toClassDocument(cls, synthesizedBody, embedding));
        }

        List<String> classNames = file.classes().stream()
                .map(ParsedClass::qualifiedClassName)
                .collect(Collectors.toList());
        stateTracker.track(Path.of(file.filePath()), classNames);
    }

    /**
     * Builds the synthesized body text for a class document.
     * Concatenates method signatures and javadocs so BM25 has the vocabulary of what the class does.
     */
    private static String buildSynthesizedBody(List<ParsedMethod> methods) {
        StringBuilder sb = new StringBuilder();
        for (ParsedMethod m : methods) {
            sb.append(m.signature()).append("\n");
            if (m.javadoc() != null && !m.javadoc().isBlank()) {
                sb.append("  // ").append(m.javadoc().replaceAll("\\s+", " ").trim()).append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * Auto-detects pom.xml at {@code projectRoot}, parses it, and incorporates the
     * project into the module-level dependency graph.
     * For each newly auto-linked project, also triggers cross-project call graph linking.
     */
    private void updateModuleGraph(Path projectRoot, ProjectMeta meta) {
        MavenPomReader reader = new MavenPomReader();
        MavenPomReader.findPom(projectRoot)
                .flatMap(reader::read)
                .ifPresentOrElse(
                        pomInfo -> {
                            try {
                                List<String> autoLinked =
                                        moduleGraphBuilder.incorporate(projectRoot, meta, pomInfo);
                                // Auto-link call graphs for newly-matched projects
                                CrossProjectLinker linker =
                                        new CrossProjectLinker(config, registry);
                                for (String linkedProject : autoLinked) {
                                    if (meta.getName().equals(linkedProject)) continue;
                                    try {
                                        registry.link(meta.getName(), linkedProject);

                                        // Cross-project link guard (#7): skip expensive graph
                                        // linking when neither project's unresolved refs have
                                        // changed since the last time we linked them.
                                        boolean skip = false;
                                        int currentHash = meta.getUnresolvedRefsHash();
                                        Optional<ProjectMeta> prevMeta = registry.find(meta.getName());
                                        if (currentHash != 0 && prevMeta.isPresent()
                                                && prevMeta.get().getUnresolvedRefsHash() == currentHash) {
                                            Optional<ProjectMeta> linkedMeta = registry.find(linkedProject);
                                            if (linkedMeta.isPresent()) {
                                                int prevLinkedHash = linkedMeta.get().getUnresolvedRefsHash();
                                                // We don't have the "at link time" hash here, so we
                                                // only skip if both sides have the same hash as stored.
                                                // A zero hash means "not computed yet" (pre-existing index).
                                                skip = prevLinkedHash != 0
                                                        && currentHash == prevMeta.get().getUnresolvedRefsHash();
                                            }
                                        }

                                        if (skip) {
                                            log.debug("Cross-project link guard: skipping '{}' <-> '{}' (refs unchanged)",
                                                    meta.getName(), linkedProject);
                                        } else {
                                            linker.buildCrossProjectGraph(meta.getName(), linkedProject);
                                            log.info("Auto-linked call graphs: '{}' <-> '{}'",
                                                    meta.getName(), linkedProject);
                                        }
                                    } catch (Exception e) {
                                        log.warn("Auto call-graph link failed for {} <-> {}: {}",
                                                meta.getName(), linkedProject, e.getMessage());
                                    }
                                }
                            } catch (Exception e) {
                                log.warn("Module graph update failed for '{}': {}",
                                        meta.getName(), e.getMessage());
                            }
                        },
                        () -> log.debug("No pom.xml found at {}, skipping module graph", projectRoot)
                );
    }

    /**
     * Patches the persisted call graph for an incremental run:
     * <ol>
     *   <li>Load the existing {@code graph.graphml} (falls back to empty graph if absent).</li>
     *   <li>Remove all vertices/edges for dirty and deleted files using the class names
     *       recorded in {@link FileStateTracker} (deleted files) or from the freshly-parsed
     *       {@link ParsedFile} objects (dirty files).</li>
     *   <li>Splice in new vertices/edges for each freshly-parsed dirty file.</li>
     *   <li>Save and return the updated graph.</li>
     * </ol>
     * This avoids a full project re-parse just to rebuild the graph on every incremental run.
     */
    private CallGraph patchGraph(Path projectIndexDir,
                                  List<ParsedFile> parsedDirtyFiles,
                                  List<Path> deletedFiles,
                                  FileStateTracker stateTracker) {
        Path graphFile = projectIndexDir.resolve("graph.graphml");
        CallGraphSerializer serializer = new CallGraphSerializer();
        CallGraph graph;
        try {
            graph = serializer.load(graphFile);
            log.debug("Loaded call graph for patching: {} nodes, {} edges",
                    graph.nodeCount(), graph.edgeCount());
        } catch (Exception e) {
            log.warn("Could not load existing graph for patching, starting fresh: {}", e.getMessage());
            graph = new CallGraph();
        }

        // Evict stale nodes for deleted files using class names from the state tracker
        for (Path deleted : deletedFiles) {
            List<String> classNames = stateTracker.getTrackedClassNames(deleted);
            if (!classNames.isEmpty()) {
                graph.removeMethodsFromClasses(classNames);
                log.debug("Patched graph: removed {} class(es) for deleted {}", classNames.size(), deleted.getFileName());
            }
        }

        // Evict stale nodes for dirty files using their freshly-parsed class names,
        // then splice in the new nodes and edges from the fresh parse.
        CallGraphBuilder builder = new CallGraphBuilder();
        for (ParsedFile pf : parsedDirtyFiles) {
            List<String> classNames = pf.classes().stream()
                    .map(ParsedClass::qualifiedClassName)
                    .collect(Collectors.toList());
            graph.removeMethodsFromClasses(classNames);
            builder.buildFile(graph, pf);
        }

        log.debug("Patched call graph: {} nodes, {} edges after update", graph.nodeCount(), graph.edgeCount());

        try {
            serializer.save(graph, graphFile);
        } catch (Exception e) {
            log.warn("Failed to save patched call graph: {}", e.getMessage());
        }
        return graph;
    }

    /**
     * Builds a lightweight {@link ProjectMeta} for incremental runs where we have
     * the graph and scanner file count but did not re-parse the full project.
     * Method/class counts are read from the existing Lucene index.
     */
    private ProjectMeta buildMetaFromIndex(String projectName, Path projectRoot,
                                            Path projectIndexDir, CallGraph graph,
                                            int fileCount) {
        ProjectMeta meta = new ProjectMeta(
                projectName,
                projectRoot.toAbsolutePath().toString(),
                projectIndexDir.toAbsolutePath().toString()
        );
        meta.setLastIndexed(Instant.now());
        meta.setFileCount(fileCount);

        // Count methods and classes from the Lucene index using TermQuery
        try {
            org.apache.lucene.index.IndexReader reader = luceneIndexer.openReader(projectName);
            org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);
            int methods = searcher.count(new org.apache.lucene.search.TermQuery(
                    new Term(DocumentMapper.F_DOC_TYPE, "method")));
            int classes = searcher.count(new org.apache.lucene.search.TermQuery(
                    new Term(DocumentMapper.F_DOC_TYPE, "class")));
            meta.setMethodCount(methods);
            meta.setClassCount(classes);
        } catch (Exception e) {
            log.debug("Could not read method/class counts from index: {}", e.getMessage());
        }

        return meta;
    }

    private CallGraph buildAndSaveGraph(ParsedProject project, Path projectIndexDir) {
        CallGraph graph = new CallGraph();
        CallGraphBuilder builder = new CallGraphBuilder();
        builder.build(graph, project);

        try {
            CallGraphSerializer serializer = new CallGraphSerializer();
            serializer.save(graph, projectIndexDir.resolve("graph.graphml"));
            log.debug("Call graph saved: {} nodes, {} edges",
                    graph.nodeCount(), graph.edgeCount());
        } catch (Exception e) {
            log.warn("Failed to save call graph: {}", e.getMessage());
        }
        return graph;
    }

    private ProjectMeta buildMeta(String projectName, Path projectRoot, Path projectIndexDir,
                                   ParsedProject project, CallGraph graph) {
        ProjectMeta meta = new ProjectMeta(
                projectName,
                projectRoot.toAbsolutePath().toString(),
                projectIndexDir.toAbsolutePath().toString()
        );
        meta.setLastIndexed(Instant.now());
        meta.setMethodCount(project.allMethods().size());
        meta.setClassCount(project.allClasses().size());
        meta.setFileCount(project.files().size());
        meta.setKnownPackages(new ArrayList<>(project.knownPackages()));

        // Save unresolved refs for later cross-project linking.
        // We join each call reference with its file's import list so the linker can
        // infer which package the receiver type belongs to.
        List<ProjectMeta.UnresolvedRef> unresolvedRefs = new ArrayList<>();
        for (var file : project.files()) {
            List<String> imports = file.imports() != null ? file.imports() : List.of();
            for (var method : file.methods()) {
                for (var ref : method.calledMethods()) {
                    if (!ref.resolved()) {
                        unresolvedRefs.add(ProjectMeta.UnresolvedRef.from(ref, imports));
                    }
                }
            }
        }
        meta.setUnresolvedRefs(unresolvedRefs);
        meta.setUnresolvedRefsHash(computeRefsHash(unresolvedRefs));

        // Persist refs to a separate file so registry.json stays lean (#3)
        registry.saveRefs(projectName, projectIndexDir.toAbsolutePath().toString(), unresolvedRefs);

        return meta;
    }

    private static int computeRefsHash(List<ProjectMeta.UnresolvedRef> refs) {
        if (refs == null || refs.isEmpty()) return 0;
        // Stable hash: combine callerFqn + calleeMethodName + receiverType for each ref.
        // Order-sensitive — list order is deterministic (file walk + method ordering).
        int h = 1;
        for (ProjectMeta.UnresolvedRef r : refs) {
            h = 31 * h + (r.callerFqn    != null ? r.callerFqn.hashCode()          : 0);
            h = 31 * h + (r.calleeMethodName != null ? r.calleeMethodName.hashCode() : 0);
            h = 31 * h + (r.receiverTypeName != null ? r.receiverTypeName.hashCode() : 0);
        }
        // Never return 0 (reserved for "not computed")
        return h == 0 ? 1 : h;
    }

    /**
     * Mines discriminative TF-IDF terms from the freshly indexed project and
     * appends new synonym rules to {@code ~/.pharos/synonyms.txt}.
     *
     * <p>Runs after every full or incremental index.  Errors are logged but
     * never propagate — synonym expansion is best-effort and must not break
     * the primary indexing pipeline.
     */
    private void expandSynonyms(String projectName) {
        Path synonymFile = IndexConfig.DEFAULT_BASE.resolve("synonyms.txt");
        try {
            org.apache.lucene.index.IndexReader reader =
                    luceneIndexer.openMultiReader(List.of(projectName));
            ConceptMiner miner = new ConceptMiner(10, 2, 30, 0.05);
            int added = miner.appendNewSynonyms(reader, synonymFile, projectName);
            if (added > 0) {
                log.info("Synonym expansion: appended {} new rules to {}", added, synonymFile);
            } else {
                log.debug("Synonym expansion: no new rules for '{}'", projectName);
            }
        } catch (Exception e) {
            log.warn("Synonym expansion failed for '{}' (non-fatal): {}", projectName, e.getMessage());
        }
    }

    /**
     * Pre-computed per-method graph data used during two-pass indexing.
     * Built once from the call graph, then consulted for every method document.
     */
    static class GraphIndexData {
        // fqn → number of callers (in-degree in the call graph)
        private final java.util.Map<String, Integer> inDegrees;
        // fqn → simple method names of callers (for callerContext text field)
        private final java.util.Map<String, List<String>> callerNames;

        private GraphIndexData(java.util.Map<String, Integer> inDegrees,
                               java.util.Map<String, List<String>> callerNames) {
            this.inDegrees = inDegrees;
            this.callerNames = callerNames;
        }

        static GraphIndexData build(CallGraph graph, ParsedProject project) {
            List<ParsedMethod> allMethods = project.allMethods();
            java.util.Map<String, Integer> deg = new java.util.HashMap<>(allMethods.size() * 2);
            java.util.Map<String, List<String>> names = new java.util.HashMap<>(allMethods.size() * 2);

            // Single pass: compute in-degree and caller simple names together
            for (ParsedMethod method : allMethods) {
                String fqn = method.fqn();
                java.util.Set<String> callers = graph.getCallers(fqn);
                deg.put(fqn, callers.size());
                List<String> simpleCallerNames = callers.stream()
                        .map(callerFqn -> {
                            int hash = callerFqn.indexOf('#');
                            if (hash < 0) return callerFqn;
                            int paren = callerFqn.indexOf('(', hash);
                            return paren > 0
                                    ? callerFqn.substring(hash + 1, paren)
                                    : callerFqn.substring(hash + 1);
                        })
                        .distinct()
                        .collect(Collectors.toList());
                names.put(fqn, simpleCallerNames);
            }
            return new GraphIndexData(deg, names);
        }

        int inDegree(String fqn) {
            return inDegrees.getOrDefault(fqn, 0);
        }

        List<String> callerSimpleNames(String fqn) {
            return callerNames.getOrDefault(fqn, List.of());
        }
    }
}
