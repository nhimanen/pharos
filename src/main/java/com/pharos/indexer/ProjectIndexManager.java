package com.pharos.indexer;

import com.pharos.analysis.ConceptMiner;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.CallGraph;
import com.pharos.graph.CallGraphBuilder;
import com.pharos.graph.KnowledgeGraphBuilder;
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
    private static final int EMBED_CHUNK_SIZE = 8;

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
        // Single walkFileTree — collect all supported source files once, dispatch to parsers by extension.
        // This avoids N independent walks when N parsers are registered.
        // Check dirty status against the pre-existing state before clearing it, so synonym mining
        // can be skipped when no source files have actually changed.
        progress.onProgress("Scanning", 0, 0);
        Set<String> supportedExts = allSupportedExtensions();
        Map<CodeParser, List<Path>> filesByParser = new java.util.LinkedHashMap<>();
        for (CodeParser p : parsers) filesByParser.put(p, new ArrayList<>());

        // Treat a brand-new project (no prior state) as always changed.
        boolean[] anyChanges = { stateTracker.trackedFiles().isEmpty() };

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
                        if (!anyChanges[0] && stateTracker.isDirty(file)) anyChanges[0] = true;
                        break;
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });

        // Also treat removed files as a change.
        if (!anyChanges[0]) {
            Set<Path> currentFiles = filesByParser.values().stream()
                    .flatMap(List::stream)
                    .collect(java.util.stream.Collectors.toSet());
            for (Path tracked : stateTracker.trackedFiles()) {
                if (!currentFiles.contains(tracked)) { anyChanges[0] = true; break; }
            }
        }

        stateTracker.clear();

        // Parse the collected files using each parser (shared parsers across the batch)
        progress.onProgress("Parsing", 0, parsers.size());
        List<ParsedProject> perLang = new ArrayList<>();
        int parserIdx = 0;
        for (Map.Entry<CodeParser, List<Path>> entry : filesByParser.entrySet()) {
            perLang.add(entry.getKey().parseFiles(entry.getValue(), projectRoot, projectName));
            progress.onProgress("Parsing", ++parserIdx, parsers.size());
        }
        ParsedProject project = merge(projectName, projectRoot, perLang);

        // Build knowledge-graph relationships from parsed data (best-effort, per parser).
        // Catches Throwable (including OutOfMemoryError) so a large project cannot silently
        // kill the indexing thread when the symbol resolver runs out of heap.
        List<com.pharos.parser.model.ParsedRelationships> allRelationships = new ArrayList<>();
        int relIdx = 0;
        for (Map.Entry<CodeParser, List<Path>> entry : filesByParser.entrySet()) {
            try {
                allRelationships.add(entry.getKey().buildRelationships(project));
            } catch (Throwable e) {
                log.warn("buildRelationships failed for parser {} ({}): {}",
                        entry.getKey().getClass().getSimpleName(),
                        e.getClass().getSimpleName(), e.getMessage());
            }
            progress.onProgress("Extracting relationships", ++relIdx, parsers.size());
        }
        com.pharos.parser.model.ParsedRelationships relationships = mergeRelationships(projectName, allRelationships);

        // Pass 1: build call graph + knowledge graph — needed to compute in-degrees and caller contexts
        progress.onProgress("Building call graph", 0, 0);
        GraphIndexData graphData = buildAndSaveGraph(project, relationships, projectIndexDir);

        // Release relationships and intermediate lists before embedding starts.
        // For large projects the relationship lists (returns, takes, etc.) can be
        // tens of MBs; holding them through embedding causes OOM under heap pressure.
        relationships = null;
        allRelationships = null;

        // Pass 2: write Lucene index with graph-derived fields
        try (IndexWriter writer = luceneIndexer.openWriterFresh(projectName)) {
            indexProject(writer, project, stateTracker, generateEmbeddings, graphData, progress, projectIndexDir);
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

        if (buildSynonyms && anyChanges[0]) expandSynonyms(projectName);
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

            // Pass 1: delete stale docs and parse all dirty files; spool embedding texts to disk.
            boolean doEmbed = generateEmbeddings && embedder.isAvailable();
            int dims = embedder.dimensions();
            List<Integer> fileSlotStarts = new ArrayList<>();  // slot offset per parsed file
            List<List<String>> parsedSynthBodies = new ArrayList<>();

            Path spoolPath = projectIndexDir.resolve("embed-incr.spool");
            Path cachePath = projectIndexDir.resolve("embed-incr.cache");
            try {
                int totalSlots = 0;
                try (DataOutputStream spool = doEmbed
                        ? new DataOutputStream(new BufferedOutputStream(
                                Files.newOutputStream(spoolPath), 1 << 16))
                        : null) {
                    for (Path file : dirtyFiles) {
                        writer.deleteDocuments(new Term(DocumentMapper.F_FILE_PATH,
                                file.toAbsolutePath().toString()));
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
                                            cls, methodsByClass.getOrDefault(cls.qualifiedClassName(), List.of())))
                                    .collect(Collectors.toList());
                            parsedSynthBodies.add(synthBodies);

                            fileSlotStarts.add(totalSlots);
                            if (doEmbed) {
                                for (ParsedMethod m : parsedFile.methods()) {
                                    spoolWrite(spool, DocumentMapper.buildEmbeddingText(m));
                                    totalSlots++;
                                }
                                for (int ci = 0; ci < parsedFile.classes().size(); ci++) {
                                    spoolWrite(spool, DocumentMapper.buildClassEmbeddingText(
                                            parsedFile.classes().get(ci), synthBodies.get(ci)));
                                    totalSlots++;
                                }
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse {}: {}", file, e.getMessage());
                        }
                    }
                }

                // Pass 2: stream spool through ONNX in chunks, write embedding cache.
                if (doEmbed && totalSlots > 0) {
                    int totalChunks = (totalSlots + EMBED_CHUNK_SIZE - 1) / EMBED_CHUNK_SIZE;
                    try (DataInputStream spool = new DataInputStream(new BufferedInputStream(
                                 Files.newInputStream(spoolPath), 1 << 16));
                         DataOutputStream cache = new DataOutputStream(new BufferedOutputStream(
                                 Files.newOutputStream(cachePath), 1 << 16))) {
                        int chunksDone = 0;
                        int slot = 0;
                        while (slot < totalSlots) {
                            int end = Math.min(slot + EMBED_CHUNK_SIZE, totalSlots);
                            List<String> chunk = new ArrayList<>(end - slot);
                            for (int i = slot; i < end; i++) chunk.add(spoolRead(spool));
                            float[][] embeddings = embedder.embedBatch(chunk);
                            for (int i = 0; i < embeddings.length; i++) cacheWrite(cache, embeddings[i], dims);
                            slot = end;
                            progress.onProgress("Embedding", ++chunksDone, totalChunks);
                        }
                    }
                }

                // Pass 3: write Lucene documents reading embeddings from cache.
                FileChannel cacheChannel = (doEmbed && totalSlots > 0)
                        ? FileChannel.open(cachePath, StandardOpenOption.READ) : null;
                try {
                    for (int i = 0; i < parsedDirtyFiles.size(); i++) {
                        ParsedFile parsedFile = parsedDirtyFiles.get(i);
                        List<String> synthBodies = parsedSynthBodies.get(i);
                        int slotCount = parsedFile.methods().size() + parsedFile.classes().size();
                        float[][] embs = readCachedEmbeddings(cacheChannel, fileSlotStarts.get(i), slotCount, dims);

                        for (int mi = 0; mi < parsedFile.methods().size(); mi++) {
                            writer.addDocument(DocumentMapper.toDocument(
                                    parsedFile.methods().get(mi), embs[mi]));
                        }
                        int classOff = parsedFile.methods().size();
                        for (int ci = 0; ci < parsedFile.classes().size(); ci++) {
                            writer.addDocument(DocumentMapper.toClassDocument(
                                    parsedFile.classes().get(ci), synthBodies.get(ci), embs[classOff + ci]));
                        }

                        List<String> classNames = parsedFile.classes().stream()
                                .map(ParsedClass::qualifiedClassName).collect(Collectors.toList());
                        stateTracker.track(Path.of(parsedFile.filePath()), classNames);
                        log.debug("Re-indexed {}", parsedFile.filePath());
                        progress.onProgress("Indexing", ++doneWork, totalWork);
                    }
                } finally {
                    if (cacheChannel != null) cacheChannel.close();
                }
                writer.commit();
            } finally {
                Files.deleteIfExists(spoolPath);
                Files.deleteIfExists(cachePath);
            }
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

    /**
     * Indexes a parsed project into Lucene using a streaming spool/cache pattern to
     * bound heap usage regardless of project size.
     *
     * <p>Three sequential phases:
     * <ol>
     *   <li>Spool — write every embedding text to {@code embed.spool} (length-prefixed bytes);
     *       only {@code int[] fileOffsets} kept in RAM.</li>
     *   <li>Embed — stream spool in chunks of {@value #EMBED_CHUNK_SIZE}, batch-embed each chunk,
     *       write float arrays to {@code embed.cache} (fixed-size slots seekable by index).</li>
     *   <li>Write — read embeddings from cache per file (one range-read), write Lucene docs;
     *       safe to parallelise because {@link FileChannel} reads are thread-safe.</li>
     * </ol>
     * Both temp files are deleted in a {@code finally} block even on error or OOM.
     */
    private void indexProject(IndexWriter writer, ParsedProject project,
                               FileStateTracker stateTracker, boolean generateEmbeddings,
                               GraphIndexData graphData, ProgressListener progress,
                               Path projectIndexDir) throws IOException {
        int indexThreads = config.resolvedIndexThreads();
        List<ParsedFile> files = project.files();
        int totalFiles = files.size();
        int dims = embedder.dimensions();
        log.debug("Indexing {} files with {} thread(s)", totalFiles, indexThreads);

        Map<String, List<ParsedMethod>> methodsByClass = project.allMethods().stream()
                .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));
        ConcurrentHashMap<String, String> synthesizedBodyCache = new ConcurrentHashMap<>();

        boolean doEmbed = generateEmbeddings && embedder.isAvailable();
        int[] fileOffsets = new int[totalFiles];
        List<List<String>> fileSynthBodies = new ArrayList<>(totalFiles);

        // Per-file, per-document chunk lists — kept in memory so Phase 3 knows
        // how many embeddings to read per document.
        // fileMethodChunks[fi][mi] = List<Chunk> for method mi (1 chunk for short, N for long)
        // fileClassChunks[fi][ci]  = List<Chunk> for class ci (header + group chunks)
        Chunker chunker = new DefaultChunker();
        List<List<List<Chunk>>> fileMethodChunks = new ArrayList<>(totalFiles);
        List<List<List<Chunk>>> fileClassChunks  = new ArrayList<>(totalFiles);

        Path spoolPath = projectIndexDir.resolve("embed.spool");
        Path cachePath = projectIndexDir.resolve("embed.cache");
        try {
            // Phase 1: build chunk lists and spool all embedding texts to disk.
            // Methods always contribute 1 slot; classes contribute N slots (one per chunk).
            int totalSlots = 0;
            if (doEmbed) {
                Files.createDirectories(projectIndexDir);
                try (DataOutputStream spool = new DataOutputStream(new BufferedOutputStream(
                        Files.newOutputStream(spoolPath), 1 << 16))) {
                    for (int fi = 0; fi < totalFiles; fi++) {
                        ParsedFile file = files.get(fi);
                        List<String> synthBodies = file.classes().stream()
                                .map(cls -> synthesizedBodyCache.computeIfAbsent(
                                        cls.qualifiedClassName(),
                                        k -> buildSynthesizedBody(cls, methodsByClass.getOrDefault(k, List.of()))))
                                .collect(Collectors.toList());
                        fileSynthBodies.add(synthBodies);
                        fileOffsets[fi] = totalSlots;

                        // Methods: 1 chunk for short, N for long (multi-chunk).
                        // After spooling the text we strip it from the Chunk — only startLine/endLine
                        // are needed in Phase 3. Dropping the text strings now frees ~80 MB for
                        // large projects (lucene: 50k methods × ~800 chars each) before embedding.
                        // Preload source lines once per file — lazy body reads (body==null) use this
                        // list instead of reopening the file N times for the same file.
                        java.util.List<String> fileLines = preloadSourceLines(file.filePath());
                        List<List<Chunk>> methodChunksPerFile = new ArrayList<>(file.methods().size());
                        for (ParsedMethod m : file.methods()) {
                            List<Chunk> mChunks = chunker.chunkMethodWithLines(m, true, fileLines);
                            List<Chunk> stripped = new ArrayList<>(mChunks.size());
                            for (Chunk c : mChunks) {
                                spoolWrite(spool, c.text());
                                totalSlots++;
                                stripped.add(new Chunk(null, c.startLine(), c.endLine())); // text not needed after spool
                            }
                            methodChunksPerFile.add(stripped);
                        }
                        fileMethodChunks.add(methodChunksPerFile);

                        // Classes: 1-N chunks each
                        List<List<Chunk>> classChunksList = new ArrayList<>(file.classes().size());
                        for (int ci = 0; ci < file.classes().size(); ci++) {
                            ParsedClass cls = file.classes().get(ci);
                            List<ParsedMethod> clsMethods = methodsByClass.getOrDefault(
                                    cls.qualifiedClassName(), List.of());
                            List<Chunk> clsChunks = "document".equals(cls.kind())
                                    ? chunker.chunkDocument(cls, synthBodies.get(ci))
                                    : chunker.chunkClass(cls, synthBodies.get(ci), clsMethods);
                            List<Chunk> stripped = new ArrayList<>(clsChunks.size());
                            for (Chunk c : clsChunks) {
                                spoolWrite(spool, c.text());
                                totalSlots++;
                                stripped.add(new Chunk(null, c.startLine(), c.endLine()));
                            }
                            classChunksList.add(stripped);
                        }
                        fileClassChunks.add(classChunksList);
                    }
                }
            } else {
                for (int fi = 0; fi < totalFiles; fi++) {
                    ParsedFile file = files.get(fi);
                    List<String> synthBodies = file.classes().stream()
                            .map(cls -> synthesizedBodyCache.computeIfAbsent(
                                    cls.qualifiedClassName(),
                                    k -> buildSynthesizedBody(cls, methodsByClass.getOrDefault(k, List.of()))))
                            .collect(Collectors.toList());
                    fileSynthBodies.add(synthBodies);
                    fileMethodChunks.add(List.of()); // empty = no-embed fallback
                    fileClassChunks.add(List.of());
                }
            }

            // Phase 2: stream spool through ONNX in chunks, write fixed-size embedding cache.
            // Peak heap: one chunk of texts + one chunk of embeddings (~200 KB for chunk size 32).
            if (doEmbed && totalSlots > 0) {
                int totalChunks = (totalSlots + EMBED_CHUNK_SIZE - 1) / EMBED_CHUNK_SIZE;
                try (DataInputStream spool = new DataInputStream(new BufferedInputStream(
                             Files.newInputStream(spoolPath), 1 << 16));
                     DataOutputStream cache = new DataOutputStream(new BufferedOutputStream(
                             Files.newOutputStream(cachePath), 1 << 16))) {
                    int chunksDone = 0;
                    int slot = 0;
                    while (slot < totalSlots) {
                        int end = Math.min(slot + EMBED_CHUNK_SIZE, totalSlots);
                        List<String> chunk = new ArrayList<>(end - slot);
                        for (int i = slot; i < end; i++) chunk.add(spoolRead(spool));
                        float[][] embeddings = embedder.embedBatch(chunk);
                        for (int i = 0; i < embeddings.length; i++) cacheWrite(cache, embeddings[i], dims);
                        slot = end;
                        progress.onProgress("Embedding", ++chunksDone, totalChunks);
                    }
                }
            }

            // Phase 3: write Lucene documents. Each file reads its embedding range from the
            // cache in one shot; FileChannel is thread-safe so the parallel path works as-is.
            FileChannel cacheChannel = (doEmbed && totalSlots > 0)
                    ? FileChannel.open(cachePath, StandardOpenOption.READ) : null;
            try {
                AtomicInteger doneFiles = new AtomicInteger(0);
                AtomicInteger totalMethods = new AtomicInteger(0);

                if (indexThreads <= 1) {
                    for (int fi = 0; fi < totalFiles; fi++) {
                        ParsedFile file = files.get(fi);
                        int slotCount = fileSlotCount(file, fileMethodChunks.get(fi), fileClassChunks.get(fi));
                        float[][] embs = readCachedEmbeddings(cacheChannel, fileOffsets[fi], slotCount, dims);
                        writeFileDocs(writer, file, stateTracker, graphData, fileSynthBodies.get(fi),
                                embs, fileMethodChunks.get(fi), fileClassChunks.get(fi));
                        int done = doneFiles.incrementAndGet();
                        totalMethods.addAndGet(file.methods().size());
                        progress.onProgress("Indexing", done, totalFiles);
                        if (totalMethods.get() % 500 == 0) log.debug("Indexed {} methods...", totalMethods.get());
                    }
                } else {
                    ExecutorService pool = Executors.newFixedThreadPool(indexThreads,
                            r -> { Thread t = new Thread(r, "indexer"); t.setDaemon(true); return t; });
                    try {
                        List<Future<?>> futures = new ArrayList<>(totalFiles);
                        for (int fi = 0; fi < totalFiles; fi++) {
                            final int fiFinal = fi;
                            futures.add(pool.submit(() -> {
                                try {
                                    ParsedFile file = files.get(fiFinal);
                                    int slotCount = fileSlotCount(file, fileMethodChunks.get(fiFinal), fileClassChunks.get(fiFinal));
                                    float[][] embs = readCachedEmbeddings(
                                            cacheChannel, fileOffsets[fiFinal], slotCount, dims);
                                    writeFileDocs(writer, file, stateTracker, graphData,
                                            fileSynthBodies.get(fiFinal), embs,
                                            fileMethodChunks.get(fiFinal), fileClassChunks.get(fiFinal));
                                    int done = doneFiles.incrementAndGet();
                                    totalMethods.addAndGet(file.methods().size());
                                    progress.onProgress("Indexing", done, totalFiles);
                                    if (totalMethods.get() % 500 == 0)
                                        log.debug("Indexed {} methods...", totalMethods.get());
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
            } finally {
                if (cacheChannel != null) cacheChannel.close();
            }
        } finally {
            Files.deleteIfExists(spoolPath);
            Files.deleteIfExists(cachePath);
        }
    }

    // ── Spool / cache helpers ──────────────────────────────────────────────────

    /** Writes one text slot: [4-byte length][UTF-8 bytes]. Length 0 signals null/blank. */
    private static void spoolWrite(DataOutputStream out, String text) throws IOException {
        if (text == null || text.isBlank()) {
            out.writeInt(0);
        } else {
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    /** Reads one text slot from the spool. Returns null for zero-length entries. */
    private static String spoolRead(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == 0) return null;
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Writes one embedding slot to the cache (fixed size: {@code dims} floats).
     * A null embedding is written as NaN in the first position (sentinel) + zeros.
     */
    private static void cacheWrite(DataOutputStream out, float[] embedding, int dims) throws IOException {
        if (embedding != null) {
            for (float f : embedding) out.writeFloat(f);
        } else {
            out.writeFloat(Float.NaN);
            for (int i = 1; i < dims; i++) out.writeFloat(0f);
        }
    }

    /**
     * Reads {@code count} embedding slots starting at {@code startSlot} from the cache file.
     * Uses a single range read for efficiency. Returns null slots where the NaN sentinel is set.
     * Thread-safe: uses absolute-position {@link FileChannel#read(ByteBuffer, long)}.
     */
    private static float[][] readCachedEmbeddings(FileChannel cache, int startSlot, int count, int dims)
            throws IOException {
        float[][] result = new float[count][];
        if (cache == null || count == 0) return result;
        int totalBytes = count * dims * 4;
        ByteBuffer buf = ByteBuffer.allocate(totalBytes);
        long pos = (long) startSlot * dims * 4;
        while (buf.hasRemaining()) {
            int n = cache.read(buf, pos + buf.position());
            if (n < 0) break;
        }
        buf.flip();
        for (int i = 0; i < count; i++) {
            float first = buf.getFloat();
            if (Float.isNaN(first)) {
                buf.position(buf.position() + (dims - 1) * 4);
            } else {
                float[] emb = new float[dims];
                emb[0] = first;
                for (int d = 1; d < dims; d++) emb[d] = buf.getFloat();
                result[i] = emb;
            }
        }
        return result;
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
                               List<List<Chunk>> methodChunkMeta,
                               List<List<Chunk>> classChunkMeta) throws IOException {
        int slot = 0;

        // Preload source lines once per file — methods with lazy bodies (body==null) all share this.
        java.util.List<String> fileLines = preloadSourceLines(file.filePath());

        // Methods — 1 or more chunks each (long methods get N chunks)
        for (int mi = 0; mi < file.methods().size(); mi++) {
            ParsedMethod method = file.methods().get(mi);
            int inDegree     = graphData.inDegree(method.fqn());
            List<String> callerNames = graphData.callerSimpleNames(method.fqn());
            int n = (!methodChunkMeta.isEmpty() && mi < methodChunkMeta.size())
                    ? methodChunkMeta.get(mi).size() : 0;
            if (n > 0 && slot + n <= globalEmbeddings.length) {
                float[][] chunkEmbs = java.util.Arrays.copyOfRange(globalEmbeddings, slot, slot + n);
                List<Chunk> mChunkList = methodChunkMeta.get(mi);
                int[][] ranges = mChunkList.stream()
                        .map(c -> new int[]{c.startLine(), c.endLine()})
                        .toArray(int[][]::new);
                writer.addDocument(DocumentMapper.toDocumentMultiVec(method, chunkEmbs, ranges, inDegree, callerNames, fileLines));
                slot += n;
            } else {
                writer.addDocument(DocumentMapper.toDocument(method, (float[]) null, inDegree, callerNames, fileLines));
            }
        }

        // Classes — N chunks each
        for (int ci = 0; ci < file.classes().size(); ci++) {
            ParsedClass cls = file.classes().get(ci);
            int n = (!classChunkMeta.isEmpty() && ci < classChunkMeta.size())
                    ? classChunkMeta.get(ci).size() : 0;
            if (n > 0 && slot + n <= globalEmbeddings.length) {
                float[][] chunkEmbs = java.util.Arrays.copyOfRange(globalEmbeddings, slot, slot + n);
                List<Chunk> cChunkList = classChunkMeta.get(ci);
                int[][] ranges = cChunkList.stream()
                        .map(c -> new int[]{c.startLine(), c.endLine()})
                        .toArray(int[][]::new);
                writer.addDocument(DocumentMapper.toClassDocumentMultiVec(cls, synthesizedBodies.get(ci), chunkEmbs, ranges));
                slot += n;
            } else {
                writer.addDocument(DocumentMapper.toClassDocument(cls, synthesizedBodies.get(ci), (float[]) null));
            }
        }

        List<String> classNames = file.classes().stream()
                .map(ParsedClass::qualifiedClassName)
                .collect(Collectors.toList());
        stateTracker.track(Path.of(file.filePath()), classNames);
    }

    /**
     * Reads all source lines for a file into memory once, so lazy body reads
     * ({@link ParsedMethod#body()} == null) for all methods in that file share a single list.
     * Returns an empty list on I/O failure; callers handle null bodies gracefully.
     */
    private static java.util.List<String> preloadSourceLines(String filePath) {
        try {
            return java.nio.file.Files.readAllLines(java.nio.file.Path.of(filePath));
        } catch (Exception e) {
            return List.of();
        }
    }

    /** Total embedding slots for a file = sum(methodChunks) + sum(classChunks). */
    private static int fileSlotCount(ParsedFile file, List<List<Chunk>> methodChunkMeta,
                                     List<List<Chunk>> classChunkMeta) {
        int n = 0;
        if (methodChunkMeta != null && !methodChunkMeta.isEmpty()) {
            for (List<Chunk> mc : methodChunkMeta) n += mc.size();
        } else {
            n += file.methods().size(); // fallback: 1 per method
        }
        if (classChunkMeta != null && !classChunkMeta.isEmpty()) {
            for (List<Chunk> cc : classChunkMeta) n += cc.size();
        } else {
            n += file.classes().size();
        }
        return n;
    }

    /**
     * Returns the synthesized body for a class.
     * For document-kind classes (non-code files indexed by GenericFileParser) the body is
     * the raw file content so chunkers and BM25 see the actual text.
     * For code classes it is the concatenation of method signatures and javadocs.
     */
    private static String buildSynthesizedBody(ParsedClass cls, List<ParsedMethod> methods) {
        if ("document".equals(cls.kind())) {
            return DocumentMapper.readBodyFromFile(cls.filePath(), cls.startLine(), cls.endLine());
        }
        return buildSynthesizedBody(methods);
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

            // Rebuild knowledge-graph edges for dirty files
            if (!parsedDirtyFiles.isEmpty()) {
                ParsedProject miniProject = new com.pharos.parser.model.ParsedProject(
                        "incremental", "", parsedDirtyFiles);
                com.pharos.parser.model.ParsedRelationships rel =
                        parsers.stream()
                               .filter(p -> p.supportedExtensions().contains(".java"))
                               .findFirst()
                               .map(p -> { try { return p.buildRelationships(miniProject); } catch (Throwable e2) { return com.pharos.parser.model.ParsedRelationships.empty("incremental"); } })
                               .orElse(com.pharos.parser.model.ParsedRelationships.empty("incremental"));
                new com.pharos.graph.KnowledgeGraphBuilder().build(graph, rel);
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

    private GraphIndexData buildAndSaveGraph(ParsedProject project,
                                              com.pharos.parser.model.ParsedRelationships relationships,
                                              Path projectIndexDir) {
        Path dbDir = projectIndexDir.resolve("callgraph.arcadedb");
        try (CallGraph graph = CallGraph.open(dbDir)) {
            graph.clear();
            new CallGraphBuilder().build(graph, project);
            new KnowledgeGraphBuilder().build(graph, relationships);
            CrossProjectLinker.touchStamp(projectIndexDir.resolve("graph.stamp"));
            log.debug("Call graph built: {} methods, {} calls",
                    graph.methodCount(), graph.callCount());
            return GraphIndexData.build(graph, project);
        } catch (Throwable e) {
            // Catch Throwable (including OutOfMemoryError) so a large project cannot
            // propagate through the ExecutorService and kill other queued projects.
            log.warn("Failed to build call graph for '{}' ({}): {}",
                    project.projectName(), e.getClass().getSimpleName(), e.getMessage());
            return GraphIndexData.empty();
        }
    }

    private static com.pharos.parser.model.ParsedRelationships mergeRelationships(
            String projectName, List<com.pharos.parser.model.ParsedRelationships> all) {
        List<com.pharos.parser.model.ParsedRelationships.TypeEdge>   inherits   = new ArrayList<>();
        List<com.pharos.parser.model.ParsedRelationships.TypeEdge>   implEdges  = new ArrayList<>();
        List<com.pharos.parser.model.ParsedRelationships.FieldDecl>  fields     = new ArrayList<>();
        List<com.pharos.parser.model.ParsedRelationships.FieldAccess> reads     = new ArrayList<>();
        List<com.pharos.parser.model.ParsedRelationships.FieldAccess> writes    = new ArrayList<>();
        List<com.pharos.parser.model.ParsedRelationships.TypeEdge>   returns    = new ArrayList<>();
        List<com.pharos.parser.model.ParsedRelationships.TypeEdge>   takes      = new ArrayList<>();
        List<com.pharos.parser.model.ParsedRelationships.TypeEdge>   annotated  = new ArrayList<>();
        for (var r : all) {
            inherits.addAll(r.inherits());
            implEdges.addAll(r.implementsEdges());
            fields.addAll(r.fields());
            reads.addAll(r.reads());
            writes.addAll(r.writes());
            returns.addAll(r.returns());
            takes.addAll(r.takes());
            annotated.addAll(r.annotatedBy());
        }
        return new com.pharos.parser.model.ParsedRelationships(
                projectName, inherits, implEdges, fields, reads, writes, returns, takes, annotated);
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
