package com.pharos.indexer;

import com.pharos.analysis.ConceptMiner;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.CallGraphBuilder;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.graph.ModuleGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.parser.CodeParser;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.CMakeReader;
import com.pharos.parser.GradleBuildReader;
import com.pharos.parser.MavenPomReader;
import com.pharos.parser.PackageJsonReader;
import com.pharos.parser.PyprojectReader;
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
    /** Number of embedding texts per ONNX forward pass during global batching. */
    private static final int EMBED_CHUNK_SIZE = 32;

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
     * Removes all data associated with a project:
     * <ol>
     *   <li>Downgrades the module-graph node from INDEXED → EXTERNAL (preserves dep edges)</li>
     *   <li>Deletes the cross-project stamp so the next link rebuilds the cross graph</li>
     *   <li>Closes and deletes the Lucene index</li>
     *   <li>Deletes the full project index directory (call graph, file-state, etc.)</li>
     *   <li>Strips auto-mined synonym rules for this project from {@code synonyms.txt}</li>
     *   <li>Removes {@code linkedProjects} back-references from other registry entries</li>
     *   <li>Unregisters the project from the registry</li>
     * </ol>
     * Safe to call when the project is not registered or has no index on disk — each step
     * is a no-op when the resource does not exist.
     */
    public void removeProject(String projectName) throws IOException {
        // 1. Downgrade module-graph node INDEXED → EXTERNAL (preserves dep edges)
        try (ModuleGraph moduleGraph = moduleGraphBuilder.open()) {
            moduleGraph.findByProjectName(projectName)
                    .ifPresent(node -> moduleGraph.downgradeToExternal(node.moduleKey()));
        } catch (Exception e) {
            log.warn("Could not update module graph for '{}': {}", projectName, e.getMessage());
        }

        // 2. Invalidate cross-project graph so the next link rebuilds it
        Files.deleteIfExists(IndexConfig.DEFAULT_BASE.resolve("cross-project.stamp"));

        // 3. Close cached reader and delete the Lucene segment directory
        luceneIndexer.deleteIndex(projectName);

        // 4. Delete the full project index directory (callgraph, file-state, etc.)
        Path projectDir = luceneIndexer.getProjectIndexDir(projectName);
        if (Files.exists(projectDir)) {
            try (var stream = Files.walk(projectDir)) {
                stream.sorted(Comparator.reverseOrder())
                      .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
            }
        }

        // 5. Strip auto-mined synonym rules for this project from synonyms.txt
        Path synonymFile = config.getSynonymsFile();
        try {
            int removed = ConceptMiner.removeProjectSynonyms(synonymFile, projectName);
            if (removed > 0) log.info("Removed {} synonym lines for '{}'", removed, projectName);
        } catch (Exception e) {
            log.warn("Could not clean synonyms for '{}': {}", projectName, e.getMessage());
        }

        // 6. Remove back-references in other projects' linkedProjects lists
        registry.unlinkAll(projectName);

        // 7. Remove from registry
        registry.unregister(projectName);

        log.info("Removed all data for project '{}'", projectName);
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
     * @param force          if true, deletes the existing Lucene directory before indexing —
     *                       required after Lucene version upgrades or to recover from corrupt indexes
     * @param buildSynonyms  if true, mine and append synonym rules after indexing
     */
    public ProjectMeta index(Path projectRoot, String projectName,
                              boolean incremental, boolean force, boolean generateEmbeddings,
                              ProgressListener progress) throws IOException {
        return index(projectRoot, projectName, incremental, force, generateEmbeddings, true, progress);
    }

    public ProjectMeta index(Path projectRoot, String projectName,
                              boolean incremental, boolean force, boolean generateEmbeddings,
                              boolean buildSynonyms, ProgressListener progress) throws IOException {
        if (force) {
            log.info("Force re-index: removing all project data for '{}'", projectName);
            removeProject(projectName);
        }

        log.info("Starting {} index of project '{}' at {}",
                incremental ? "incremental" : "full", projectName, projectRoot);

        Path projectIndexDir = luceneIndexer.getProjectIndexDir(projectName);
        Files.createDirectories(projectIndexDir);

        FileStateTracker stateTracker = new FileStateTracker(projectIndexDir);

        if (incremental && luceneIndexer.indexExists(projectName)) {
            return indexIncremental(projectRoot, projectName, projectIndexDir,
                    stateTracker, generateEmbeddings, buildSynonyms, progress);
        } else {
            return indexFull(projectRoot, projectName, projectIndexDir,
                    stateTracker, generateEmbeddings, buildSynonyms, progress);
        }
    }

    private ProjectMeta indexFull(Path projectRoot, String projectName,
                                   Path projectIndexDir, FileStateTracker stateTracker,
                                   boolean generateEmbeddings, boolean buildSynonyms,
                                   ProgressListener progress) throws IOException {
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
        GraphIndexData graphData = buildAndSaveGraph(project, projectIndexDir);

        // Pass 2: write Lucene index with graph-derived fields
        try (IndexWriter writer = luceneIndexer.openWriterFresh(projectName)) {
            indexProject(writer, project, stateTracker, generateEmbeddings, graphData, progress);
            writer.commit();
        }

        // Save file state
        stateTracker.save();

        // Update registry
        ProjectMeta meta = buildMeta(projectName, projectRoot, projectIndexDir, project);
        registry.register(meta);
        updateModuleGraph(projectRoot, meta);

        progress.onProgress("Done", meta.getFileCount(), meta.getFileCount());
        log.info("Full index complete for '{}': {} methods, {} classes, {} files",
                meta.getMethodCount(), meta.getClassCount(), meta.getFileCount(), projectName);

        if (buildSynonyms) expandSynonyms(projectName);
        return meta;
    }

    private ProjectMeta indexIncremental(Path projectRoot, String projectName,
                                          Path projectIndexDir, FileStateTracker stateTracker,
                                          boolean generateEmbeddings, boolean buildSynonyms,
                                          ProgressListener progress) throws IOException {
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

            // Pass 1: delete stale docs and parse all dirty files.
            // Collect embedding texts per file (no ONNX yet) so we can embed in one global batch.
            boolean doEmbed = generateEmbeddings && embedder.isAvailable();
            List<String> allIncrementalTexts = doEmbed ? new ArrayList<>() : List.of();
            List<Integer> fileTextStarts = new ArrayList<>();     // offset in allIncrementalTexts per parsed file
            List<List<String>> parsedSynthBodies = new ArrayList<>();

            for (Path file : dirtyFiles) {
                writer.deleteDocuments(new Term(DocumentMapper.F_FILE_PATH, file.toAbsolutePath().toString()));
                CodeParser fileParser = parserFor(file);
                if (fileParser == null) {
                    log.debug("No parser for {}, skipping", file.getFileName());
                    continue;
                }
                try {
                    ParsedFile parsedFile = fileParser.parseFile(file, projectName);
                    parsedDirtyFiles.add(parsedFile);

                    Map<String, List<ParsedMethod>> methodsByClass = parsedFile.methods().stream()
                            .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));
                    List<String> synthBodies = parsedFile.classes().stream()
                            .map(cls -> buildSynthesizedBody(
                                    methodsByClass.getOrDefault(cls.qualifiedClassName(), List.of())))
                            .collect(Collectors.toList());
                    parsedSynthBodies.add(synthBodies);

                    if (doEmbed) {
                        fileTextStarts.add(allIncrementalTexts.size());
                        for (ParsedMethod m : parsedFile.methods()) {
                            allIncrementalTexts.add(DocumentMapper.buildEmbeddingText(m));
                        }
                        for (int ci = 0; ci < parsedFile.classes().size(); ci++) {
                            allIncrementalTexts.add(DocumentMapper.buildClassEmbeddingText(
                                    parsedFile.classes().get(ci), synthBodies.get(ci)));
                        }
                    } else {
                        fileTextStarts.add(0);
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse {}: {}", file, e.getMessage());
                }
            }

            // Pass 2: embed all dirty-file texts in one global chunked ONNX pass.
            final float[][] incrementalEmbeddings;
            if (doEmbed && !allIncrementalTexts.isEmpty()) {
                int totalChunks = (allIncrementalTexts.size() + EMBED_CHUNK_SIZE - 1) / EMBED_CHUNK_SIZE;
                incrementalEmbeddings = embedder.embedChunked(allIncrementalTexts, EMBED_CHUNK_SIZE,
                        done -> progress.onProgress("Embedding", done, totalChunks));
            } else {
                incrementalEmbeddings = new float[0][];
            }

            // Pass 3: write Lucene documents using pre-computed embeddings.
            for (int i = 0; i < parsedDirtyFiles.size(); i++) {
                ParsedFile parsedFile = parsedDirtyFiles.get(i);
                List<String> synthBodies = parsedSynthBodies.get(i);
                int offset = fileTextStarts.get(i);

                for (int mi = 0; mi < parsedFile.methods().size(); mi++) {
                    ParsedMethod method = parsedFile.methods().get(mi);
                    float[] embedding = (offset + mi < incrementalEmbeddings.length)
                            ? incrementalEmbeddings[offset + mi] : null;
                    writer.addDocument(DocumentMapper.toDocument(method, embedding));
                }
                int classOffset = parsedFile.methods().size();
                for (int ci = 0; ci < parsedFile.classes().size(); ci++) {
                    ParsedClass cls = parsedFile.classes().get(ci);
                    float[] embedding = (offset + classOffset + ci < incrementalEmbeddings.length)
                            ? incrementalEmbeddings[offset + classOffset + ci] : null;
                    writer.addDocument(DocumentMapper.toClassDocument(cls, synthBodies.get(ci), embedding));
                }

                List<String> classNames = parsedFile.classes().stream()
                        .map(ParsedClass::qualifiedClassName)
                        .collect(Collectors.toList());
                stateTracker.track(Path.of(parsedFile.filePath()), classNames);
                log.debug("Re-indexed {}", parsedFile.filePath());
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
        patchGraph(projectIndexDir, parsedDirtyFiles, deletedFiles, stateTracker);

        // Build lightweight ProjectMeta from Lucene reader counts (no re-parse needed)
        ProjectMeta meta = buildMetaFromIndex(projectName, projectRoot, projectIndexDir, currentFiles.size());
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
        if (buildSynonyms && !dirtyFiles.isEmpty()) {
            expandSynonyms(projectName);
        }
        return meta;
    }

    private void indexProject(IndexWriter writer, ParsedProject project,
                               FileStateTracker stateTracker, boolean generateEmbeddings,
                               GraphIndexData graphData, ProgressListener progress) throws IOException {
        int indexThreads = config.resolvedIndexThreads();
        List<ParsedFile> files = project.files();
        int totalFiles = files.size();
        log.debug("Indexing with {} thread(s)", indexThreads);

        // Build method lookup per class for synthesized class bodies (read-only after construction)
        Map<String, List<ParsedMethod>> methodsByClass = project.allMethods().stream()
                .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));
        ConcurrentHashMap<String, String> synthesizedBodyCache = new ConcurrentHashMap<>();

        // Phase 1: collect synthesized bodies and embedding texts for every file without
        // touching ONNX.  Per-file texts occupy a contiguous slice of allTexts:
        //   methods at [fileOffsets[fi], fileOffsets[fi] + file.methods().size())
        //   classes  at [fileOffsets[fi] + file.methods().size(), fileOffsets[fi+1])
        boolean doEmbed = generateEmbeddings && embedder.isAvailable();
        List<String> allTexts = doEmbed ? new ArrayList<>() : List.of();
        int[] fileOffsets = new int[totalFiles];
        List<List<String>> fileSynthBodies = new ArrayList<>(totalFiles);

        for (int fi = 0; fi < totalFiles; fi++) {
            ParsedFile file = files.get(fi);
            List<String> synthBodies = file.classes().stream()
                    .map(cls -> synthesizedBodyCache.computeIfAbsent(
                            cls.qualifiedClassName(),
                            k -> buildSynthesizedBody(methodsByClass.getOrDefault(k, List.of()))))
                    .collect(Collectors.toList());
            fileSynthBodies.add(synthBodies);

            if (doEmbed) {
                fileOffsets[fi] = allTexts.size();
                for (ParsedMethod m : file.methods()) {
                    allTexts.add(DocumentMapper.buildEmbeddingText(m));
                }
                for (int ci = 0; ci < file.classes().size(); ci++) {
                    allTexts.add(DocumentMapper.buildClassEmbeddingText(
                            file.classes().get(ci), synthBodies.get(ci)));
                }
            }
        }

        // Phase 2: single global ONNX pass with length-sorted chunking.
        // Large batches amortise JNI + session overhead; sorting groups similar-length sequences
        // together so padding waste within each chunk is minimised.
        final float[][] globalEmbeddings;
        if (doEmbed && !allTexts.isEmpty()) {
            int totalChunks = (allTexts.size() + EMBED_CHUNK_SIZE - 1) / EMBED_CHUNK_SIZE;
            globalEmbeddings = embedder.embedChunked(allTexts, EMBED_CHUNK_SIZE,
                    done -> progress.onProgress("Embedding", done, totalChunks));
        } else {
            globalEmbeddings = new float[0][];
        }

        // Phase 3: write Lucene documents — no ONNX here, safe to parallelise freely.
        AtomicInteger doneFiles = new AtomicInteger(0);
        AtomicInteger totalMethods = new AtomicInteger(0);

        if (indexThreads <= 1) {
            for (int fi = 0; fi < totalFiles; fi++) {
                ParsedFile file = files.get(fi);
                writeFileDocs(writer, file, stateTracker, graphData,
                        fileSynthBodies.get(fi), globalEmbeddings, fileOffsets[fi]);
                int done = doneFiles.incrementAndGet();
                totalMethods.addAndGet(file.methods().size());
                progress.onProgress("Indexing", done, totalFiles);
                if (totalMethods.get() % 500 == 0) {
                    log.debug("Indexed {} methods...", totalMethods.get());
                }
            }
        } else {
            // IndexWriter.addDocument() is thread-safe; embeddings are pre-computed read-only
            // arrays; FileStateTracker uses ConcurrentHashMap — the parallel write is safe.
            ExecutorService pool = Executors.newFixedThreadPool(indexThreads,
                    r -> { Thread t = new Thread(r, "indexer"); t.setDaemon(true); return t; });
            try {
                List<Future<?>> futures = new ArrayList<>(totalFiles);
                for (int fi = 0; fi < totalFiles; fi++) {
                    final int fiFinal = fi;
                    futures.add(pool.submit(() -> {
                        try {
                            ParsedFile file = files.get(fiFinal);
                            writeFileDocs(writer, file, stateTracker, graphData,
                                    fileSynthBodies.get(fiFinal), globalEmbeddings, fileOffsets[fiFinal]);
                            int done = doneFiles.incrementAndGet();
                            totalMethods.addAndGet(file.methods().size());
                            progress.onProgress("Indexing", done, totalFiles);
                            if (totalMethods.get() % 500 == 0) {
                                log.debug("Indexed {} methods...", totalMethods.get());
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        return null;
                    }));
                }
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
     * Writes Lucene documents for a single file's methods and classes using pre-computed embeddings.
     * Safe to call from multiple threads: IndexWriter.addDocument and FileStateTracker are thread-safe.
     *
     * @param globalEmbeddings flat array of all embeddings computed in Phase 2 (may be empty)
     * @param embeddingOffset  index of this file's first embedding in {@code globalEmbeddings}
     */
    private void writeFileDocs(IndexWriter writer, ParsedFile file,
                               FileStateTracker stateTracker,
                               GraphIndexData graphData,
                               List<String> synthesizedBodies,
                               float[][] globalEmbeddings,
                               int embeddingOffset) throws IOException {
        for (int mi = 0; mi < file.methods().size(); mi++) {
            ParsedMethod method = file.methods().get(mi);
            float[] embedding = (embeddingOffset + mi < globalEmbeddings.length)
                    ? globalEmbeddings[embeddingOffset + mi] : null;
            int inDegree = graphData.inDegree(method.fqn());
            List<String> callerNames = graphData.callerSimpleNames(method.fqn());
            writer.addDocument(DocumentMapper.toDocument(method, embedding, inDegree, callerNames));
        }

        int classOffset = file.methods().size();
        for (int ci = 0; ci < file.classes().size(); ci++) {
            ParsedClass cls = file.classes().get(ci);
            float[] embedding = (embeddingOffset + classOffset + ci < globalEmbeddings.length)
                    ? globalEmbeddings[embeddingOffset + classOffset + ci] : null;
            writer.addDocument(DocumentMapper.toClassDocument(cls, synthesizedBodies.get(ci), embedding));
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
     * Auto-detects the project's build system, parses it, and incorporates the
     * project into the module-level dependency graph.
     *
     * Detection order (first match wins):
     *   1. Maven  — pom.xml
     *   2. Gradle — settings.gradle / build.gradle
     *   3. Python — pyproject.toml / setup.py / setup.cfg
     *   4. CMake  — CMakeLists.txt
     *
     * For each newly auto-linked project, also triggers cross-project call graph linking.
     */
    private void updateModuleGraph(Path projectRoot, ProjectMeta meta) {
        Optional<MavenPomReader.PomInfo> pomInfoOpt =
                MavenPomReader.findPom(projectRoot).flatMap(new MavenPomReader()::read);

        if (pomInfoOpt.isEmpty() && GradleBuildReader.isGradleProject(projectRoot)) {
            pomInfoOpt = new GradleBuildReader().read(projectRoot);
        }
        if (pomInfoOpt.isEmpty() && PyprojectReader.isPythonProject(projectRoot)) {
            pomInfoOpt = new PyprojectReader().read(projectRoot);
        }
        if (pomInfoOpt.isEmpty() && PackageJsonReader.isNodeProject(projectRoot)) {
            pomInfoOpt = new PackageJsonReader().read(projectRoot);
        }
        if (pomInfoOpt.isEmpty() && CMakeReader.isCMakeProject(projectRoot)) {
            pomInfoOpt = new CMakeReader().read(projectRoot);
        }

        pomInfoOpt.ifPresentOrElse(
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
     *   <li>Open the existing ArcadeDB call graph (created if absent).</li>
     *   <li>Evict all vertices for dirty and deleted files' class prefixes.</li>
     *   <li>Splice in new vertices/edges for each freshly-parsed dirty file.</li>
     * </ol>
     * This avoids a full project re-parse just to rebuild the graph on every incremental run.
     */
    private void patchGraph(Path projectIndexDir,
                             List<ParsedFile> parsedDirtyFiles,
                             List<Path> deletedFiles,
                             FileStateTracker stateTracker) {
        Path dbDir = projectIndexDir.resolve("callgraph.arcadedb");
        try (CallGraph graph = CallGraph.open(dbDir)) {
            log.debug("Opened call graph for patching: {} methods", graph.methodCount());

            // Collect class prefixes to evict (deleted files + dirty files)
            List<String> toEvict = new ArrayList<>();
            for (Path deleted : deletedFiles) {
                toEvict.addAll(stateTracker.getTrackedClassNames(deleted));
            }
            for (ParsedFile pf : parsedDirtyFiles) {
                pf.classes().forEach(c -> toEvict.add(c.qualifiedClassName()));
            }
            graph.evictClasses(toEvict);

            // Splice in fresh vertices/edges for dirty files
            CallGraphBuilder builder = new CallGraphBuilder();
            for (ParsedFile pf : parsedDirtyFiles) {
                builder.buildFile(graph, pf);
            }

            CrossProjectLinker.touchStamp(projectIndexDir.resolve("graph.stamp"));
            log.debug("Patched call graph: {} methods after update", graph.methodCount());
        } catch (Exception e) {
            log.warn("Failed to patch call graph: {}", e.getMessage());
        }
    }

    /**
     * Builds a lightweight {@link ProjectMeta} for incremental runs where we did not
     * re-parse the full project. Method/class counts are read from the existing Lucene index.
     */
    private ProjectMeta buildMetaFromIndex(String projectName, Path projectRoot,
                                            Path projectIndexDir,
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

    private GraphIndexData buildAndSaveGraph(ParsedProject project, Path projectIndexDir) {
        Path dbDir = projectIndexDir.resolve("callgraph.arcadedb");
        try (CallGraph graph = CallGraph.open(dbDir)) {
            graph.clear();
            new CallGraphBuilder().build(graph, project);
            CrossProjectLinker.touchStamp(projectIndexDir.resolve("graph.stamp"));
            log.debug("Call graph built: {} methods, {} calls",
                    graph.methodCount(), graph.callCount());
            return GraphIndexData.build(graph, project);
        } catch (Exception e) {
            log.warn("Failed to build call graph: {}", e.getMessage());
            return GraphIndexData.empty();
        }
    }

    private ProjectMeta buildMeta(String projectName, Path projectRoot, Path projectIndexDir,
                                   ParsedProject project) {
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
        Path synonymFile = config.getSynonymsFile();
        try {
            // Remove stale rules for this project first so every reindex applies
            // current filters (fanout cap, test-class filter, redundancy filter).
            int removed = ConceptMiner.removeProjectSynonyms(synonymFile, projectName);
            if (removed > 0)
                log.debug("Synonym expansion: removed {} stale rules for '{}'", removed, projectName);

            org.apache.lucene.index.IndexReader reader =
                    luceneIndexer.openMultiReader(List.of(projectName));
            ConceptMiner miner = new ConceptMiner();
            int added = miner.appendNewSynonyms(reader, synonymFile, projectName);
            if (added > 0) {
                log.info("Synonym expansion: wrote {} rules for '{}'", added, projectName);
            } else {
                log.debug("Synonym expansion: no rules generated for '{}'", projectName);
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
                java.util.Set<String> callers = graph.callers(fqn);
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

        static GraphIndexData empty() {
            return new GraphIndexData(new java.util.HashMap<>(), new java.util.HashMap<>());
        }

        int inDegree(String fqn) {
            return inDegrees.getOrDefault(fqn, 0);
        }

        List<String> callerSimpleNames(String fqn) {
            return callerNames.getOrDefault(fqn, List.of());
        }
    }
}
