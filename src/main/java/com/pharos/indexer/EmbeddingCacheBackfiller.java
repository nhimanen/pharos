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
     * Backfills the embedding cache for every registered project.
     *
     * @return total cache entries written across all projects
     */
    public int backfillAll() throws IOException {
        var projects = registry.listAll();
        if (projects.isEmpty()) {
            System.out.println("No indexed projects found.");
            return 0;
        }
        int total = 0;
        int failed = 0;
        for (var meta : projects) {
            System.out.printf("%n=== Backfilling '%s' ===%n", meta.getName());
            try {
                total += backfill(meta.getName());
            } catch (Exception e) {
                System.err.printf("Failed for '%s': %s%n", meta.getName(), e.getMessage());
                failed++;
            }
        }
        System.out.printf("%nDone: %d project(s)%s, %d total entries added.%n",
                projects.size(), failed > 0 ? " (" + failed + " failed)" : "", total);
        return total;
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

        // Pre-build the set of non-dirty Java files so Phase 1 can skip them eagerly,
        // avoiding loading vectors into memory for files we can never process.
        Set<String> eligibleFiles = new java.util.HashSet<>();
        for (Path p : tracker.trackedFiles()) {
            if (p.toString().endsWith(".java") && Files.exists(p) && !tracker.isDirty(p)) {
                eligibleFiles.add(p.toString());
            }
        }

        // ── Phase 1: collect single-chunk Java docs from the Lucene HNSW index ─────
        // Vectors are grouped by file path so Phase 2 can process and release one file
        // at a time without holding the entire project's float[][] in memory at once.
        // key = filePath → list of (id, vec) pairs
        Map<String, List<String>>  fileToIds     = new LinkedHashMap<>();
        Map<String, float[]>       idToVector    = new LinkedHashMap<>();

        int totalVectors = 0, skippedDocs = 0, skippedMultiChunk = 0, skippedNotEligible = 0;
        IndexReader reader = luceneIndexer.openReader(projectName);
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            FloatVectorValues fvv = leaf.getFloatVectorValues(DocumentMapper.F_VECTOR);
            if (fvv == null) continue;
            StoredFields sf = leaf.storedFields();

            KnnVectorValues.DocIndexIterator iter = fvv.iterator();
            int docId;
            while ((docId = iter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                totalVectors++;
                Document doc = sf.document(docId);
                String rawId    = doc.get(DocumentMapper.F_ID);
                String filePath = doc.get(DocumentMapper.F_FILE_PATH);
                String scope    = doc.get(DocumentMapper.F_SCOPE);
                if (rawId == null || filePath == null) continue;
                // F_ID is stored as "projectName:fqn" — strip the prefix so we can
                // look up against ParsedMethod.fqn() / ParsedClass.qualifiedClassName()
                int colon = rawId.indexOf(':');
                String id = colon >= 0 ? rawId.substring(colon + 1) : rawId;
                if ("docs".equals(scope)) { skippedDocs++; continue; }
                // Only backfill Java files — we only have a Java chunker
                if (!filePath.endsWith(".java")) { skippedNotEligible++; continue; }
                // Skip dirty / missing files early to avoid loading their vectors
                if (!eligibleFiles.contains(filePath)) { skippedNotEligible++; continue; }

                BytesRef rangesRef = doc.getBinaryValue(DocumentMapper.F_CHUNK_LINE_RANGES);
                int chunkCount = 1;
                if (rangesRef != null) {
                    byte[] rangeBytes = java.util.Arrays.copyOfRange(
                            rangesRef.bytes, rangesRef.offset, rangesRef.offset + rangesRef.length);
                    chunkCount = DocumentMapper.decodeLineRanges(rangeBytes).length;
                }
                if (chunkCount != 1) { skippedMultiChunk++; continue; }

                // Clone only after passing all filters — avoids cloning vectors we'll discard
                float[] vec = fvv.vectorValue(iter.index()).clone();
                idToVector.put(id, vec);
                fileToIds.computeIfAbsent(filePath, k -> new ArrayList<>()).add(id);
            }
        }

        System.out.printf("Phase 1: %d total vectors — %d docs-scope, %d multi-chunk, " +
                          "%d non-Java/dirty skipped, %d candidates%n",
                totalVectors, skippedDocs, skippedMultiChunk, skippedNotEligible, idToVector.size());

        if (idToVector.isEmpty()) {
            if (totalVectors == 0) {
                System.out.println("  → No embedded documents found. " +
                        "Was this project last indexed with --embed?");
            }
            cache.save();
            return 0;
        }

        // ── Phase 2: re-parse one file at a time, release vectors after use ──────────
        // Processing and immediately nulling out each file's vectors keeps peak heap at
        // roughly (largest-file vector count × dims × 4) rather than the whole project.
        int added = 0, skippedParseFail = 0, skippedMismatch = 0;

        for (Map.Entry<String, List<String>> entry : fileToIds.entrySet()) {
            Path file = Path.of(entry.getKey());

            ParsedFile parsedFile;
            try {
                parsedFile = parser.parseFile(file, projectName);
            } catch (Exception e) {
                log.debug("Parse failed for {}: {}", file.getFileName(), e.getMessage());
                skippedParseFail++;
                entry.getValue().forEach(idToVector::remove); // release vectors
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
                float[] vec = idToVector.remove(id); // remove immediately — releases reference
                if (vec == null) continue;

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
                    if (chunks != null) skippedMismatch++;
                    continue;
                }

                cache.put(PersistentEmbeddingCache.sha256Hex(chunks.get(0).text()), vec);
                added++;
            }
        }

        cache.save();
        System.out.printf("Backfill complete: %d entries added " +
                          "(%d parse errors, %d chunk-count mismatches)%n",
                added, skippedParseFail, skippedMismatch);
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
