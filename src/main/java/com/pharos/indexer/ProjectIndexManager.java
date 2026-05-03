package com.pharos.indexer;

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
import org.apache.lucene.document.Document;
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

        // Parse entire project with all registered language parsers, then merge
        progress.onProgress("Parsing", 0, parsers.size());
        List<ParsedProject> perLang = new ArrayList<>();
        for (int i = 0; i < parsers.size(); i++) {
            CodeParser parser = parsers.get(i);
            perLang.add(parser.parseProject(projectRoot, projectName));
            progress.onProgress("Parsing", i + 1, parsers.size());
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
        return meta;
    }

    private ProjectMeta indexIncremental(Path projectRoot, String projectName,
                                          Path projectIndexDir, FileStateTracker stateTracker,
                                          boolean generateEmbeddings, ProgressListener progress) throws IOException {
        log.info("Incremental index: scanning for changes in '{}'", projectName);
        progress.onProgress("Scanning for changes", 0, 0);

        Set<String> supportedExts = allSupportedExtensions();
        List<Path> dirtyFiles = new ArrayList<>();

        // Collect all supported source files that currently exist under the project root
        Set<Path> currentFiles = new java.util.HashSet<>();
        Files.walkFileTree(projectRoot, new SimpleFileVisitor<>() {
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

        // Detect files that were tracked but no longer exist on disk
        List<Path> deletedFiles = stateTracker.trackedFiles().stream()
                .filter(p -> {
                    String n = p.getFileName() != null ? p.getFileName().toString() : "";
                    return supportedExts.stream().anyMatch(n::endsWith) && !currentFiles.contains(p);
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

                    // Re-index methods
                    for (ParsedMethod method : parsedFile.methods()) {
                        float[] embedding = generateEmbeddings && embedder.isAvailable()
                                ? embedder.embed(DocumentMapper.buildEmbeddingText(method))
                                : null;
                        Document doc = DocumentMapper.toDocument(method, embedding);
                        writer.addDocument(doc);
                    }

                    // Re-index classes
                    Map<String, List<ParsedMethod>> methodsByClass = parsedFile.methods().stream()
                            .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));
                    for (ParsedClass cls : parsedFile.classes()) {
                        List<ParsedMethod> methods = methodsByClass.getOrDefault(
                                cls.qualifiedClassName(), List.of());
                        String synthesizedBody = buildSynthesizedBody(methods);
                        float[] embedding = generateEmbeddings && embedder.isAvailable()
                                ? embedder.embed(DocumentMapper.buildClassEmbeddingText(cls, synthesizedBody))
                                : null;
                        writer.addDocument(DocumentMapper.toClassDocument(cls, synthesizedBody, embedding));
                    }

                    stateTracker.track(file);
                    log.debug("Re-indexed {}", file.getFileName());
                } catch (Exception e) {
                    log.warn("Failed to re-index {}: {}", file, e.getMessage());
                }
                progress.onProgress("Indexing", ++doneWork, totalWork);
            }
            writer.commit();
        }

        stateTracker.save();

        // Rebuild graph (full rebuild — graph is fast relative to parsing)
        progress.onProgress("Building call graph", 0, 0);
        List<ParsedProject> perLang = new ArrayList<>();
        for (CodeParser p : parsers) {
            perLang.add(p.parseProject(projectRoot, projectName));
        }
        ParsedProject fullProject = merge(projectName, projectRoot, perLang);
        CallGraph graph = buildAndSaveGraph(fullProject, projectIndexDir);

        ProjectMeta meta = buildMeta(projectName, projectRoot, projectIndexDir, fullProject, graph);
        registry.register(meta);
        updateModuleGraph(projectRoot, meta);

        progress.onProgress("Done", dirtyFiles.size(), dirtyFiles.size());
        log.info("Incremental index complete for '{}': {} files updated", projectName, dirtyFiles.size());
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

        int totalFiles = project.files().size();
        AtomicInteger doneFiles = new AtomicInteger(0);
        AtomicInteger totalMethods = new AtomicInteger(0);

        if (indexThreads <= 1) {
            // Sequential path — preserves original behaviour exactly
            for (ParsedFile file : project.files()) {
                indexFile(writer, file, stateTracker, generateEmbeddings, graphData, methodsByClass);
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
            //   • FileStateTracker.track() is synchronized below (HashMap, not thread-safe)
            ExecutorService pool = Executors.newFixedThreadPool(indexThreads,
                    r -> { Thread t = new Thread(r, "indexer"); t.setDaemon(true); return t; });
            try {
                List<Future<?>> futures = new ArrayList<>(totalFiles);
                for (ParsedFile file : project.files()) {
                    futures.add(pool.submit(() -> {
                        try {
                            indexFile(writer, file, stateTracker, generateEmbeddings,
                                    graphData, methodsByClass);
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
     *   - {@link FileStateTracker#track} is synchronized here
     *   - {@link EmbeddingProvider#embed} uses thread-local Predictors
     */
    private void indexFile(IndexWriter writer, ParsedFile file,
                           FileStateTracker stateTracker, boolean generateEmbeddings,
                           GraphIndexData graphData,
                           Map<String, List<ParsedMethod>> methodsByClass) throws IOException {
        for (ParsedMethod method : file.methods()) {
            float[] embedding = null;
            if (generateEmbeddings && embedder.isAvailable()) {
                try {
                    embedding = embedder.embed(DocumentMapper.buildEmbeddingText(method));
                } catch (Exception e) {
                    log.debug("Embedding failed for {}: {}", method.id(), e.getMessage());
                }
            }
            int inDegree = graphData.inDegree(method.fqn());
            List<String> callerNames = graphData.callerSimpleNames(method.fqn());
            writer.addDocument(DocumentMapper.toDocument(method, embedding, inDegree, callerNames));
        }

        for (ParsedClass cls : file.classes()) {
            List<ParsedMethod> methods = methodsByClass.getOrDefault(cls.qualifiedClassName(), List.of());
            String synthesizedBody = buildSynthesizedBody(methods);
            float[] embedding = null;
            if (generateEmbeddings && embedder.isAvailable()) {
                try {
                    embedding = embedder.embed(DocumentMapper.buildClassEmbeddingText(cls, synthesizedBody));
                } catch (Exception e) {
                    log.debug("Class embedding failed for {}: {}", cls.qualifiedClassName(), e.getMessage());
                }
            }
            writer.addDocument(DocumentMapper.toClassDocument(cls, synthesizedBody, embedding));
        }

        synchronized (stateTracker) {
            stateTracker.track(Path.of(file.filePath()));
        }
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
                                        linker.buildCrossProjectGraph(
                                                meta.getName(), linkedProject);
                                        log.info("Auto-linked call graphs: '{}' <-> '{}'",
                                                meta.getName(), linkedProject);
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

        // Save unresolved refs for later cross-project linking
        List<ProjectMeta.UnresolvedRef> unresolvedRefs = project.unresolvedCalls().stream()
                .map(ProjectMeta.UnresolvedRef::from)
                .collect(Collectors.toList());
        meta.setUnresolvedRefs(unresolvedRefs);

        return meta;
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
            java.util.Map<String, Integer> deg = new java.util.HashMap<>();
            java.util.Map<String, List<String>> names = new java.util.HashMap<>();

            for (ParsedMethod method : project.allMethods()) {
                String fqn = method.fqn();
                java.util.Set<String> callers = graph.getCallers(fqn);
                deg.put(fqn, callers.size());

                // Extract simple method name (strip class + params) from each caller FQN
                List<String> simpleCallerNames = callers.stream()
                        .map(callerFqn -> {
                            int hash = callerFqn.indexOf('#');
                            int paren = callerFqn.indexOf('(', hash);
                            if (hash < 0) return callerFqn;
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
