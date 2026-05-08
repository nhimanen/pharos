package com.pharos.indexer;

import com.pharos.analysis.SynonymProvider;
import com.pharos.config.IndexConfig;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;

/**
 * Manages Lucene index directories — opening writers and readers per project,
 * and a MultiReader spanning all projects for cross-project search.
 */
public class LuceneIndexer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LuceneIndexer.class);

    private final IndexConfig config;
    private final Map<String, DirectoryReader> openReaders = new HashMap<>();

    public LuceneIndexer(IndexConfig config) {
        this.config = config;
    }

    /** Open an IndexWriter for a project (CREATE_OR_APPEND mode). */
    public IndexWriter openWriter(String projectName) throws IOException {
        Path luceneDir = getLucenePath(projectName);
        Files.createDirectories(luceneDir);
        NIOFSDirectory dir = new NIOFSDirectory(luceneDir);
        IndexWriterConfig iwc = new IndexWriterConfig(buildAnalyzer());
        iwc.setSimilarity(new BM25Similarity());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        iwc.setRAMBufferSizeMB(128);
        return new IndexWriter(dir, iwc);
    }

    /** Open an IndexWriter for a project, deleting all existing documents first (full re-index). */
    public IndexWriter openWriterFresh(String projectName) throws IOException {
        Path luceneDir = getLucenePath(projectName);
        Files.createDirectories(luceneDir);
        NIOFSDirectory dir = new NIOFSDirectory(luceneDir);
        IndexWriterConfig iwc = new IndexWriterConfig(buildAnalyzer());
        iwc.setSimilarity(new BM25Similarity());
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(128);
        return new IndexWriter(dir, iwc);
    }

    /** Open a DirectoryReader for a single project. Caches and refreshes automatically. */
    public DirectoryReader openReader(String projectName) throws IOException {
        Path luceneDir = getLucenePath(projectName);
        if (!Files.exists(luceneDir) || !indexExists(projectName)) {
            throw new IOException("No valid index found for project: " + projectName
                    + " (run 'pharos index' first)");
        }
        DirectoryReader cached = openReaders.get(projectName);
        if (cached != null) {
            DirectoryReader refreshed = DirectoryReader.openIfChanged(cached);
            if (refreshed != null) {
                cached.close();
                openReaders.put(projectName, refreshed);
                return refreshed;
            }
            return cached;
        }
        DirectoryReader reader = DirectoryReader.open(new NIOFSDirectory(luceneDir));
        openReaders.put(projectName, reader);
        return reader;
    }

    /**
     * Open a MultiReader spanning the given projects.
     * Use for cross-project search. The returned reader must be closed by the caller
     * (but it shares the underlying DirectoryReaders from the cache).
     */
    public IndexReader openMultiReader(List<String> projectNames) throws IOException {
        if (projectNames.isEmpty()) {
            throw new IllegalArgumentException("No projects specified");
        }
        if (projectNames.size() == 1) {
            return openReader(projectNames.get(0));
        }
        IndexReader[] readers = new IndexReader[projectNames.size()];
        for (int i = 0; i < projectNames.size(); i++) {
            readers[i] = openReader(projectNames.get(i));
        }
        return new MultiReader(readers);
    }

    /** Returns true if a valid (committed) Lucene index exists for the given project. */
    public boolean indexExists(String projectName) {
        Path luceneDir = getLucenePath(projectName);
        if (!Files.exists(luceneDir)) return false;
        try (NIOFSDirectory dir = new NIOFSDirectory(luceneDir)) {
            return DirectoryReader.indexExists(dir);
        } catch (IOException e) {
            return false;
        }
    }

    public Path getLucenePath(String projectName) {
        return config.getIndexDir().resolve(projectName).resolve("lucene");
    }

    public Path getProjectIndexDir(String projectName) {
        return config.getIndexDir().resolve(projectName);
    }

    /**
     * Deletes the Lucene index directory for a project and evicts any cached reader.
     * Called by force re-index to clear incompatible or corrupt index state.
     */
    public void deleteIndex(String projectName) throws IOException {
        DirectoryReader cached = openReaders.remove(projectName);
        if (cached != null) {
            try { cached.close(); } catch (IOException ignored) {}
        }
        Path luceneDir = getLucenePath(projectName);
        if (Files.exists(luceneDir)) {
            try (var stream = Files.walk(luceneDir)) {
                stream.sorted(Comparator.reverseOrder())
                      .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
            }
            log.info("Deleted index directory for '{}'", projectName);
        }
    }

    /**
     * Builds the per-field analyzer used at <b>index time</b>:
     * - StandardAnalyzer for full-text fields (body, javadoc, signature, methodName, className)
     * - KeywordAnalyzer (default) for exact-match fields (id, project, filePath, etc.)
     *
     * <p>No synonyms are applied at index time — synonyms are query-only (see
     * {@link #buildQueryAnalyzer}) so the synonym file can be updated without re-indexing.
     */
    public static Analyzer buildAnalyzer() {
        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        StandardAnalyzer std = new StandardAnalyzer();
        fieldAnalyzers.put(DocumentMapper.F_BODY, std);
        fieldAnalyzers.put(DocumentMapper.F_JAVADOC, std);
        fieldAnalyzers.put(DocumentMapper.F_SIGNATURE, std);
        fieldAnalyzers.put(DocumentMapper.F_METHOD_NAME, std);
        fieldAnalyzers.put(DocumentMapper.F_CLASS_NAME, std);
        fieldAnalyzers.put(DocumentMapper.F_QUALIFIED_CLASS, std);
        fieldAnalyzers.put(DocumentMapper.F_ANNOTATIONS, std);
        return new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), fieldAnalyzers);
    }

    /**
     * Builds the per-field analyzer used at <b>query time</b>.
     *
     * <p>Identical to {@link #buildAnalyzer} for exact-match fields.
     * For full-text fields, wraps the standard tokenizer with a
     * {@link SynonymGraphFilter} loaded from {@code ~/.pharos/synonyms.txt}
     * when that file exists — enabling query-time vocabulary bridging without
     * re-indexing.
     *
     * <p>If the synonym file is absent or fails to parse, falls back to
     * {@link #buildAnalyzer} transparently.
     *
     * @param configBase the Pharos config directory (typically {@code ~/.pharos})
     */
    public static Analyzer buildQueryAnalyzer(Path configBase) {
        Path synonymFile = configBase.resolve("synonyms.txt");
        SynonymProvider provider = new SynonymProvider(synonymFile);
        if (!provider.isAvailable()) {
            return buildAnalyzer();
        }

        SynonymMap synonymMap;
        try {
            synonymMap = provider.load(true);
        } catch (IOException | ParseException e) {
            log.warn("Failed to load synonyms from {} — falling back to standard analyzer: {}",
                    synonymFile, e.getMessage());
            return buildAnalyzer();
        }

        // Build a synonym-aware analyzer for each full-text field.
        // We construct it inline with an anonymous Analyzer subclass so that
        // each field gets its own TokenStreamComponents (Lucene requires this).
        final SynonymMap finalMap = synonymMap;
        Analyzer synonymAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                StandardTokenizer tokenizer = new StandardTokenizer();
                TokenStream stream = new LowerCaseFilter(tokenizer);
                stream = new SynonymGraphFilter(stream, finalMap, true);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        for (String field : List.of(
                DocumentMapper.F_BODY, DocumentMapper.F_JAVADOC, DocumentMapper.F_SIGNATURE,
                DocumentMapper.F_METHOD_NAME, DocumentMapper.F_CLASS_NAME,
                DocumentMapper.F_QUALIFIED_CLASS, DocumentMapper.F_ANNOTATIONS,
                DocumentMapper.F_CALLER_CONTEXT)) {
            fieldAnalyzers.put(field, synonymAnalyzer);
        }
        log.info("Query analyzer loaded with synonym expansion from {}", synonymFile);
        return new PerFieldAnalyzerWrapper(new KeywordAnalyzer(), fieldAnalyzers);
    }

    @Override
    public void close() {
        for (Map.Entry<String, DirectoryReader> entry : openReaders.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.warn("Error closing reader for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        openReaders.clear();
    }
}
