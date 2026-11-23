package com.pharos.indexer;

import com.pharos.analysis.SynonymProvider;
import com.pharos.cluster.LocalCache;
import com.pharos.cluster.LocalFsCache;
import com.pharos.cluster.ShardId;
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
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages Lucene index directories — opening writers and readers per project,
 * and a MultiReader spanning all projects for cross-project search.
 */
public class LuceneIndexer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LuceneIndexer.class);

    /** How long a reader replaced by a shard-path change stays open for in-flight searches. */
    public static final Duration RETIRE_DELAY = com.pharos.config.ClusterConfig.RETIRE_DELAY;

    /** A cached reader and the lucene directory it was opened on. */
    private record CachedReader(Path path, DirectoryReader reader) {}

    private final IndexConfig config;
    /** Guarded by {@code this}. */
    private final Map<String, CachedReader> openReaders = new HashMap<>();
    /**
     * Ruling P6-R19: per shard, the reader of its retained previous generation ({@link LocalCache#retainedDir});
     * released ({@link #releasePrevious}) when the cache ends the retention. Guarded by {@code this}.
     */
    private final Map<String, CachedReader> previousReaders = new HashMap<>();
    /** Readers swapped out by a path change, awaiting their delayed close; guarded by {@code this}. */
    private final Set<DirectoryReader> retired = new HashSet<>();
    /** Lazily created single daemon thread closing retired readers; guarded by {@code this}. */
    private ScheduledExecutorService retirer;
    private final Duration retireDelay;

    private final LocalCache cache;
    private volatile LuceneDirectoryFactory directoryFactory = LuceneDirectoryFactory.DEFAULT;

    public LuceneIndexer(IndexConfig config) {
        this(config, new LocalFsCache(config));
    }

    public LuceneIndexer(IndexConfig config, LocalCache cache) {
        this(config, cache, RETIRE_DELAY);
    }

    /** Package-private: a shorter retire delay for tests. */
    LuceneIndexer(IndexConfig config, LocalCache cache, Duration retireDelay) {
        this.config = config;
        this.cache = cache;
        this.retireDelay = retireDelay;
        if (cache instanceof com.pharos.cluster.ReaderLocalCache rc) rc.onRelease(this::releasePrevious);
    }

    /**
     * Open an IndexWriter for a project (CREATE_OR_APPEND mode).
     *
     * <p>The writer does not commit on close: callers must {@code commit()} explicitly once their
     * work is complete. Closing without a commit (e.g. on an exception) discards everything since
     * the last commit, so a failed run never publishes half-done changes.
     */
    public IndexWriter openWriter(String projectName) throws IOException {
        ProjectNames.resolveProjectDir(config.getIndexDir(), projectName);
        return openWriterAt(getLucenePath(projectName), false);
    }

    /**
     * Open an IndexWriter for a project, deleting all existing documents first (full re-index).
     *
     * <p>Like {@link #openWriter}, it does not commit on close: until the caller commits, readers
     * keep seeing the previous commit, and closing without a commit (a failed run) leaves that
     * previous commit in place instead of publishing a partial or empty index.
     */
    public IndexWriter openWriterFresh(String projectName) throws IOException {
        ProjectNames.resolveProjectDir(config.getIndexDir(), projectName);
        return openWriterAt(getLucenePath(projectName), true);
    }

    /**
     * An IndexWriter on one shard's Lucene dir of a split project ({@link LocalShardLayout#luceneDir}),
     * with the semantics of {@link #openWriterFresh} ({@code fresh}) or {@link #openWriter}.
     */
    public IndexWriter openShardWriter(String projectName, ShardId shard, boolean fresh) throws IOException {
        ProjectNames.resolveProjectDir(config.getIndexDir(), projectName);
        return openWriterAt(getShardLucenePath(projectName, shard), fresh);
    }

    private IndexWriter openWriterAt(Path luceneDir, boolean fresh) throws IOException {
        Files.createDirectories(luceneDir);
        Directory dir = openDirectory(luceneDir);
        IndexWriterConfig iwc = new IndexWriterConfig(buildAnalyzer());
        iwc.setSimilarity(new BM25Similarity());
        iwc.setOpenMode(fresh ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        iwc.setCommitOnClose(false);
        iwc.setRAMBufferSizeMB(128);
        return new IndexWriter(dir, iwc);
    }

    /**
     * Open a DirectoryReader for a single project. Caches and refreshes automatically.
     *
     * <p>When the project's lucene path changed since the cached reader was opened (a reader
     * node activated a new generation directory), a fresh reader is opened on the new path and
     * published first; the old one is retired and closed after {@link #RETIRE_DELAY}, so searches
     * that obtained it just before the swap can finish.
     */
    public synchronized DirectoryReader openReader(String projectName) throws IOException {
        IndexFormat.check(projectName, getProjectIndexDir(projectName));
        if (localLayout(projectName).isPresent()) {
            throw new IOException("Project " + projectName + " is split into shards locally; read it with"
                    + " openMultiReader");
        }
        return openCached(projectName, getLucenePath(projectName), projectName);
    }

    /**
     * The cached reader under {@code key} on {@code luceneDir}, refreshed or reopened; caller holds
     * {@code this}.
     */
    private DirectoryReader openCached(String key, Path luceneDir, String projectName) throws IOException {
        if (!Files.exists(luceneDir) || !indexExists(luceneDir)) {
            throw new IOException("No valid index found for project: " + projectName
                    + " (run 'pharos index' first)");
        }
        CachedReader cached = openReaders.get(key);
        if (cached != null && cached.path().equals(luceneDir)) {
            DirectoryReader refreshed = DirectoryReader.openIfChanged(cached.reader());
            if (refreshed != null) {
                cached.reader().close();
                openReaders.put(key, new CachedReader(luceneDir, refreshed));
                return refreshed;
            }
            return cached.reader();
        }
        DirectoryReader reader = DirectoryReader.open(openDirectory(luceneDir));
        openReaders.put(key, new CachedReader(luceneDir, reader));
        if (cached != null) {
            Path oldDir = cached.path().getParent();
            long oldGen = generationOf(oldDir);
            if (oldGen >= 0 && cache.retainedDir(key, oldGen).filter(d -> d.equals(oldDir)).isPresent()) {
                // P6-R19: the generation stays answerable for its grace; the reader with it
                log.info("Project '{}' moved to {}; keeping the reader on {} for the previous generation's grace",
                        projectName, luceneDir, cached.path());
                CachedReader before = previousReaders.put(key, cached);
                if (before != null && before.reader() != cached.reader()) retire(before.reader());
            } else {
                log.info("Project '{}' moved to {}; retiring the reader on {}", projectName, luceneDir, cached.path());
                retire(cached.reader());
            }
        }
        return reader;
    }

    /** {@code gen-N} → N, else -1. */
    private static long generationOf(Path dir) {
        if (dir == null || dir.getFileName() == null) return -1;
        var m = java.util.regex.Pattern.compile("gen-(\\d{1,18})").matcher(dir.getFileName().toString());
        return m.matches() ? Long.parseLong(m.group(1)) : -1;
    }

    /**
     * Ruling P6-R19: the reader of {@code projectName}'s retained previous generation {@code generation}
     * ({@link LocalCache#retainedDir}) with its lucene dir; empty when the node does not answer that
     * generation (any more). The active generation is {@link #openPinnedReader}'s.
     */
    public synchronized Optional<PinnedReader> openPinnedPreviousReader(String projectName, long generation)
            throws IOException {
        Optional<Path> dir = cache.retainedDir(projectName, generation);
        if (dir.isEmpty()) return Optional.empty();
        Path luceneDir = dir.get().resolve("lucene");
        CachedReader cached = previousReaders.get(projectName);
        if (cached != null && cached.path().equals(luceneDir)) {
            return Optional.of(new PinnedReader(luceneDir, cached.reader()));
        }
        if (!Files.exists(luceneDir) || !indexExists(luceneDir)) return Optional.empty();
        DirectoryReader reader = DirectoryReader.open(openDirectory(luceneDir));
        previousReaders.put(projectName, new CachedReader(luceneDir, reader));
        if (cached != null) retire(cached.reader());
        return Optional.of(new PinnedReader(luceneDir, reader));
    }

    /** The shards whose retained previous generation has an open reader here (P6-R19). */
    public synchronized Set<String> previousGenerationReaders() {
        return Set.copyOf(previousReaders.keySet());
    }

    /** Ends the retention of {@code projectName}'s previous generation: its reader is retired (closed later). */
    public synchronized void releasePrevious(String projectName) {
        CachedReader cached = previousReaders.remove(projectName);
        if (cached != null) retire(cached.reader());
    }

    /**
     * A project's reader together with the lucene directory it was opened on, taken atomically: a
     * reader node's generation dir ({@code .../gen-N/lucene}) names the generation the reader reads.
     */
    public record PinnedReader(Path luceneDir, DirectoryReader reader) {}

    /**
     * {@link #openReader}, also returning the directory the returned reader reads, under the same
     * lock, so an activation in between cannot pair a reader with another generation's path.
     */
    public synchronized PinnedReader openPinnedReader(String projectName) throws IOException {
        DirectoryReader reader = openReader(projectName);
        return new PinnedReader(openReaders.get(projectName).path(), reader);
    }

    /** Schedules {@code old} to be closed after the retire delay; caller holds {@code this}. */
    private void retire(DirectoryReader old) {
        retired.add(old);
        if (retirer == null) {
            retirer = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "pharos-reader-retire");
                t.setDaemon(true);
                return t;
            });
        }
        retirer.schedule(() -> closeRetired(old), retireDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void closeRetired(DirectoryReader old) {
        synchronized (this) {
            if (!retired.remove(old)) return; // already closed by close()
        }
        try {
            old.close();
        } catch (IOException e) {
            log.warn("Error closing retired reader: {}", e.getMessage());
        }
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
        List<IndexReader> readers = new ArrayList<>();
        for (String project : projectNames) readers.addAll(openProjectReaders(project));
        if (readers.size() == 1) return readers.getFirst();
        return new MultiReader(readers.toArray(IndexReader[]::new));
    }

    /**
     * The readers of one project: its one reader, or, for a project split into shards locally, one per
     * shard (cached like a project's), so a {@link MultiReader} over them sees the whole project with
     * global term statistics.
     */
    synchronized List<IndexReader> openProjectReaders(String projectName) throws IOException {
        Optional<LocalShardLayout.Layout> layout = localLayout(projectName);
        if (layout.isEmpty()) return List.of(openReader(projectName));
        IndexFormat.check(projectName, getProjectIndexDir(projectName));
        if (!indexExists(projectName)) {
            throw new IOException("No valid index found for project: " + projectName
                    + " (run 'pharos index' first)");
        }
        List<IndexReader> out = new ArrayList<>();
        for (ShardId shard : layout.get().shards()) {
            out.add(openCached(shardKey(projectName, shard), getShardLucenePath(projectName, shard), projectName));
        }
        return out;
    }

    /** The reader-cache key of one shard of a split project; '#' never occurs in a project name. */
    private static String shardKey(String projectName, ShardId shard) {
        return projectName + "#" + shard.pathSegment();
    }

    /** Returns true if a valid (committed) Lucene index exists for the given project. */
    public boolean indexExists(String projectName) {
        Optional<LocalShardLayout.Layout> layout = localLayout(projectName);
        if (layout.isEmpty()) return indexExists(getLucenePath(projectName));
        if (layout.get().shards().isEmpty()) return false;
        for (ShardId shard : layout.get().shards()) {
            if (!indexExists(getShardLucenePath(projectName, shard))) return false;
        }
        return true;
    }

    /**
     * The local shard layout of the project ({@link LocalShardLayout}); empty for a {@code [project]}
     * index, whose Lucene dir is {@link #getLucenePath}.
     */
    public Optional<LocalShardLayout.Layout> localLayout(String projectName) {
        return LocalShardLayout.read(getProjectIndexDir(projectName));
    }

    /** The Lucene dir of one shard of a split project. */
    public Path getShardLucenePath(String projectName, ShardId shard) {
        return LocalShardLayout.luceneDir(getProjectIndexDir(projectName), shard);
    }

    /**
     * After a full run committed the project under {@code shards} (empty: the {@code [project]} layout):
     * removes the Lucene dirs of any other layout (the {@code lucene/} dir of a project now split, the
     * shard dirs of one no longer split or no longer listed) and evicts their cached readers.
     */
    public synchronized void dropOtherLayouts(String projectName, List<ShardId> shards) throws IOException {
        Path projectDir = getProjectIndexDir(projectName);
        java.util.Set<Path> keep = new java.util.HashSet<>();
        for (ShardId s : shards) keep.add(getShardLucenePath(projectName, s).getParent());
        if (!shards.isEmpty()) {
            evict(projectName);
            deleteTree(getLucenePath(projectName));
        }
        String prefix = projectName + "#";
        for (String key : new ArrayList<>(openReaders.keySet())) {
            if (key.startsWith(prefix) && !keep.contains(openReaders.get(key).path().getParent())) evict(key);
        }
        Path shardsDir = projectDir.resolve(LocalShardLayout.SHARDS_DIR);
        if (Files.isDirectory(shardsDir)) {
            try (var dirs = Files.list(shardsDir)) {
                for (Path d : dirs.toList()) if (!keep.contains(d)) deleteTree(d);
            }
            if (shards.isEmpty()) deleteTree(shardsDir);
        }
    }

    private void evict(String key) {
        CachedReader cached = openReaders.remove(key);
        if (cached != null) retire(cached.reader());
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var stream = Files.walk(dir)) {
            for (Path p : stream.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
        }
    }

    /**
     * The live document count of the project's latest commit, read from its segment infos only (no
     * reader is opened, and the cached readers are not refreshed); -1 when there is no index or it
     * can't be read. For {@code /cluster/status} on a single node.
     */
    public long committedDocCount(String projectName) {
        Optional<LocalShardLayout.Layout> layout = localLayout(projectName);
        if (layout.isPresent()) {
            if (!indexExists(projectName)) return -1;
            long total = 0;
            for (ShardId shard : layout.get().shards()) {
                long n = committedDocCount(getShardLucenePath(projectName, shard));
                if (n < 0) return -1;
                total += n;
            }
            return total;
        }
        return committedDocCount(getLucenePath(projectName));
    }

    private long committedDocCount(Path luceneDir) {
        if (!indexExists(luceneDir)) return -1;
        try (Directory dir = openDirectory(luceneDir)) {
            org.apache.lucene.index.SegmentInfos infos = org.apache.lucene.index.SegmentInfos.readLatestCommit(dir);
            long n = 0;
            for (org.apache.lucene.index.SegmentCommitInfo si : infos) n += si.info.maxDoc() - si.getDelCount();
            return n;
        } catch (IOException | RuntimeException e) {
            return -1;
        }
    }

    private boolean indexExists(Path luceneDir) {
        if (!Files.exists(luceneDir)) return false;
        try {
            return directoryFactory.indexExists(luceneDir);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Opens {@code luceneDir} through the installed {@link LuceneDirectoryFactory}; by default
     * {@link LuceneDirectoryFactory#DEFAULT}, Lucene's platform default ({@code MMapDirectory} on
     * 64-bit JVMs). Mapped pages count as shared, reclaimable file-backed RSS, not anonymous memory.
     */
    Directory openDirectory(Path luceneDir) throws IOException {
        return directoryFactory.open(luceneDir);
    }

    /**
     * Replaces the factory every directory is opened with from now on (readers already open keep
     * theirs). Reader nodes in lazy mode install one that serves remote generations.
     */
    public void setDirectoryFactory(LuceneDirectoryFactory factory) {
        this.directoryFactory = Objects.requireNonNull(factory, "factory");
    }

    /** The installed directory factory. */
    public LuceneDirectoryFactory directoryFactory() {
        return directoryFactory;
    }

    /** The configured index root (parent of every project dir). */
    public Path getIndexDir() {
        return config.getIndexDir();
    }

    public Path getLucenePath(String projectName) {
        return getProjectIndexDir(projectName).resolve("lucene");
    }

    public Path getProjectIndexDir(String projectName) {
        return cache.shardDir(projectName);
    }

    /** Where this indexer finds the shard dirs it serves. */
    public LocalCache localCache() {
        return cache;
    }

    /**
     * Deletes the Lucene index directory for a project and evicts any cached reader.
     * Called by force re-index to clear incompatible or corrupt index state.
     */
    public synchronized void deleteIndex(String projectName) throws IOException {
        // Refuse before touching anything: a name like "." resolves to the index root itself.
        ProjectNames.resolveProjectDir(config.getIndexDir(), projectName);
        String prefix = projectName + "#";
        for (String key : new ArrayList<>(openReaders.keySet())) {
            if (!key.equals(projectName) && !key.startsWith(prefix)) continue;
            CachedReader cached = openReaders.remove(key);
            try { cached.reader().close(); } catch (IOException ignored) {}
        }
        Path projectDir = getProjectIndexDir(projectName);
        deleteTree(projectDir.resolve(LocalShardLayout.SHARDS_DIR));
        Files.deleteIfExists(projectDir.resolve(LocalShardLayout.FILE_NAME));
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
     * Opens and immediately closes a reader on {@code dir}, so a lazy reader generation pays its
     * open cost (codec headers, footers, segment metadata) before it is activated. The reader is
     * never cached or used to serve queries; serving readers come only from {@link #openReader}.
     * Kept here so that this class stays the only place that opens Lucene readers.
     */
    public static void warmOpen(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            reader.maxDoc();
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

    /** Closes cached readers and, immediately, any retired ones still awaiting their delay. */
    @Override
    public synchronized void close() {
        for (Map.Entry<String, CachedReader> entry : openReaders.entrySet()) {
            try {
                entry.getValue().reader().close();
            } catch (IOException e) {
                log.warn("Error closing reader for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        openReaders.clear();
        for (CachedReader r : previousReaders.values()) {
            try {
                r.reader().close();
            } catch (IOException e) {
                log.warn("Error closing a previous generation's reader: {}", e.getMessage());
            }
        }
        previousReaders.clear();
        for (DirectoryReader r : retired) {
            try {
                r.close();
            } catch (IOException e) {
                log.warn("Error closing retired reader: {}", e.getMessage());
            }
        }
        retired.clear();
        if (retirer != null) {
            retirer.shutdownNow();
            retirer = null;
        }
    }
}
