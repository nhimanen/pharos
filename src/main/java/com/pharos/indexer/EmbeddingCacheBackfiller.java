package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedMethod;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Populates {@link PersistentEmbeddingCache} from existing Lucene index data without
 * calling the ONNX embedding model.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>Reads every stored {@code vectorEmbedding} float[] from the Lucene HNSW index
 *       using {@link FloatVectorValues}, keyed by document ID (FQN or qualified class name).</li>
 *   <li>Groups documents by source file and skips any file whose SHA-256 has changed
 *       since the last index run (would produce different chunk texts).</li>
 *   <li>Re-parses each eligible file and re-runs the chunker to get the current chunk
 *       texts — the same texts that would be produced by a fresh full index.</li>
 *   <li>For single-chunk documents where the stored chunk count still matches:
 *       stores {@code SHA-256(chunkText) → storedVector} in the cache.</li>
 * </ol>
 *
 * <h3>Limitations</h3>
 * Multi-chunk documents are skipped: the stored {@code F_VECTOR} is the mean-pooled
 * representative, not an individual chunk embedding, so per-chunk cache entries cannot
 * be reconstructed.  Multi-chunk methods are rare (very long bodies) — they will be
 * re-embedded on the next full index and then cached normally.
 *
 * Doc-scoped files ({@code scope=docs}) are intentionally excluded: they need fresh
 * embeddings after chunking changes and must not be backfilled.
 */
public class EmbeddingCacheBackfiller {

    private static final Logger log = LoggerFactory.getLogger(EmbeddingCacheBackfiller.class);

    private final IndexConfig config;
    private final LuceneIndexer luceneIndexer;
    private final ProjectRegistry registry;
    private final JavaCodeParser parser;
    private final Chunker chunker;

    public EmbeddingCacheBackfiller(IndexConfig config, LuceneIndexer luceneIndexer,
                                     ProjectRegistry registry) {
        this.config = config;
        this.luceneIndexer = luceneIndexer;
        this.registry = registry;
        this.parser = new JavaCodeParser();
        this.chunker = new DefaultChunker();
    }

    /**
     * Backfills the embedding cache for the given project.
     *
     * @return number of cache entries written
     */
    public int backfill(String projectName) throws IOException {
        registry.find(projectName)
                .orElseThrow(() -> new IllegalArgumentException("Unknown project: " + projectName));

        Path projectIndexDir = luceneIndexer.getProjectIndexDir(projectName);
        String modelFp = IndexVersions.modelFingerprint(config);
        int dims = config.getEmbeddingDimensions();
        if (dims <= 0) {
            System.err.println("embeddingDimensions not configured in ~/.pharos/config.json — cannot backfill.");
            return 0;
        }

        PersistentEmbeddingCache cache = new PersistentEmbeddingCache(projectIndexDir, modelFp, dims);
        FileStateTracker tracker = new FileStateTracker(projectIndexDir);

        // ── Phase 1: collect single-chunk Java docs from the Lucene HNSW index ─────
        // key = F_ID (FQN for methods, qualifiedClassName for classes)
        Map<String, float[]>  idToVector   = new LinkedHashMap<>();
        Map<String, Integer>  idToChunks   = new LinkedHashMap<>();
        Map<String, String>   idToFilePath = new LinkedHashMap<>();

        IndexReader reader = luceneIndexer.openReader(projectName);
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            FloatVectorValues fvv = leaf.getFloatVectorValues(DocumentMapper.F_VECTOR);
            if (fvv == null) continue;
            StoredFields sf = leaf.storedFields();

            KnnVectorValues.DocIndexIterator iter = fvv.iterator();
            int docId;
            while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                float[] vec = fvv.vectorValue(iter.index()).clone(); // clone: backing array is reused
                Document doc = sf.document(docId);
                String id       = doc.get(DocumentMapper.F_ID);
                String filePath = doc.get(DocumentMapper.F_FILE_PATH);
                String scope    = doc.get(DocumentMapper.F_SCOPE);
                if (id == null || filePath == null) continue;
                if ("docs".equals(scope)) continue; // will be re-embedded, not backfilled

                BytesRef rangesRef = doc.getBinaryValue(DocumentMapper.F_CHUNK_LINE_RANGES);
                int chunkCount = (rangesRef != null)
                        ? DocumentMapper.decodeLineRanges(rangesRef.bytes).length
                        : 1;
                if (chunkCount != 1) continue; // multi-chunk: representative vector ≠ per-chunk vector

                idToVector.put(id, vec);
                idToChunks.put(id, chunkCount);
                idToFilePath.put(id, filePath);
            }
        }

        log.info("Found {} single-chunk Java docs to consider for backfill in '{}'",
                idToVector.size(), projectName);

        // ── Phase 2: re-parse non-dirty files and map SHA-256(chunkText) → vector ──
        Map<String, List<String>> fileToIds = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : idToFilePath.entrySet()) {
            fileToIds.computeIfAbsent(e.getValue(), k -> new ArrayList<>()).add(e.getKey());
        }

        int added = 0, skippedDirty = 0, skippedParseFail = 0, skippedMismatch = 0;

        for (Map.Entry<String, List<String>> entry : fileToIds.entrySet()) {
            Path file = Path.of(entry.getKey());
            if (!Files.exists(file)) continue;
            if (tracker.isDirty(file)) { skippedDirty++; continue; }

            ParsedFile parsedFile;
            try {
                parsedFile = parser.parseFile(file, projectName);
            } catch (Exception e) {
                log.debug("Parse failed for {}: {}", file.getFileName(), e.getMessage());
                skippedParseFail++;
                continue;
            }

            List<String> lines = preloadLines(entry.getKey());
            Map<String, ParsedMethod> byFqn = parsedFile.methods().stream()
                    .collect(Collectors.toMap(ParsedMethod::fqn, m -> m, (a, b) -> a));
            Map<String, ParsedClass> byQcn  = parsedFile.classes().stream()
                    .collect(Collectors.toMap(ParsedClass::qualifiedClassName, c -> c, (a, b) -> a));
            Map<String, List<ParsedMethod>> methodsByClass = parsedFile.methods().stream()
                    .collect(Collectors.groupingBy(ParsedMethod::qualifiedClassName));

            for (String id : entry.getValue()) {
                float[] vec = idToVector.get(id);
                List<Chunk> chunks = null;

                if (byFqn.containsKey(id)) {
                    chunks = chunker.chunkMethodWithLines(byFqn.get(id), true, lines);
                } else if (byQcn.containsKey(id)) {
                    ParsedClass cls = byQcn.get(id);
                    List<ParsedMethod> clsMethods =
                            methodsByClass.getOrDefault(cls.qualifiedClassName(), List.of());
                    String synthBody = synthesizedBody(cls, clsMethods);
                    chunks = "document".equals(cls.kind())
                            ? chunker.chunkDocument(cls, synthBody)
                            : chunker.chunkClass(cls, synthBody, clsMethods);
                }

                if (chunks == null || chunks.size() != 1) {
                    // Chunk count changed — source or chunker drifted, skip to avoid wrong mapping
                    if (chunks != null && chunks.size() != 1) skippedMismatch++;
                    continue;
                }

                cache.put(PersistentEmbeddingCache.sha256Hex(chunks.get(0).text()), vec);
                added++;
            }
        }

        cache.save();
        System.out.printf("Backfill complete: %d entries added (%d dirty-file skips, " +
                          "%d parse errors, %d chunk-count mismatches)%n",
                added, skippedDirty, skippedParseFail, skippedMismatch);
        return added;
    }

    private static List<String> preloadLines(String filePath) {
        try { return Files.readAllLines(Path.of(filePath)); }
        catch (Exception e) { return List.of(); }
    }

    private static String synthesizedBody(ParsedClass cls, List<ParsedMethod> methods) {
        if ("document".equals(cls.kind())) {
            return DocumentMapper.readBodyFromFile(cls.filePath(), cls.startLine(), cls.endLine(), null);
        }
        StringBuilder sb = new StringBuilder();
        for (ParsedMethod m : methods) {
            sb.append(m.signature()).append("\n");
            if (m.javadoc() != null && !m.javadoc().isBlank()) {
                sb.append("  // ").append(m.javadoc().replaceAll("\\s+", " ").trim()).append("\n");
            }
        }
        return sb.toString();
    }
}
