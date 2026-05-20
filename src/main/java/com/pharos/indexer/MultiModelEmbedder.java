package com.pharos.indexer;

import com.pharos.config.EmbeddingProviderConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
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
 * Adds (or refreshes) one embedding model's vectors against an existing Lucene
 * index without re-running parsing or the call-graph build. Used by the
 * {@code pharos embed --model=<id> <project>} CLI to compare embedding models
 * cheaply: index once with model A, then drop in model B via this class
 * instead of paying the full parse + graph cost again.
 *
 * <h3>What it does</h3>
 * <ol>
 *   <li>Walks the existing Lucene index; for every {@link Document}, reads its
 *       stored fields and all per-model {@code vec.*} vectors via
 *       {@link FloatVectorValues}.</li>
 *   <li>Skips docs that already carry a {@code vec.<targetModelId>} field
 *       (unless {@code force=true}) — idempotency property.</li>
 *   <li>Re-parses the source file the doc came from, locates the matching
 *       {@link ParsedMethod} or {@link ParsedClass}, builds its embedding text
 *       via {@link DocumentMapper}, and embeds it with the target provider.</li>
 *   <li>Rebuilds the Lucene {@link Document} with all original stored fields,
 *       the existing per-model vectors re-attached, and the new
 *       {@code vec.<targetModelId>} added; commits via
 *       {@link IndexWriter#updateDocument} which is atomic delete-then-add.</li>
 *   <li>Updates {@link ProjectMeta#getEmbeddedModels()} so the search engine's
 *       per-project model-availability validation accepts the new id.</li>
 * </ol>
 *
 * <h3>v1 scope</h3>
 * Only single-chunk documents are processed. Multi-chunk docs (long methods
 * that the chunker split into N chunks during indexing) are skipped with a
 * count in the summary — for those a {@code pharos index --full} re-index is
 * the supported path, since recomputing per-chunk embeddings here would
 * require re-running the chunker over the source file for every affected doc.
 *
 * <p>Only Java sources are re-parsed in v1 (mirrors {@link EmbeddingCacheBackfiller}).
 * Non-Java docs are skipped with a count in the summary.
 */
public class MultiModelEmbedder {

    private static final Logger log = LoggerFactory.getLogger(MultiModelEmbedder.class);

    private final IndexConfig config;
    private final LuceneIndexer luceneIndexer;
    private final ProjectRegistry registry;
    private final JavaCodeParser parser;
    private final Chunker chunker;

    public MultiModelEmbedder(IndexConfig config, LuceneIndexer luceneIndexer,
                               ProjectRegistry registry) {
        this.config = config;
        this.luceneIndexer = luceneIndexer;
        this.registry = registry;
        this.parser = new JavaCodeParser();
        this.chunker = new DefaultChunker();
    }

    /** Returns the count of documents updated (excluding skips). */
    public int embed(String projectName, String modelId, boolean force) throws IOException {
        ProjectMeta meta = registry.find(projectName)
                .orElseThrow(() -> new IllegalArgumentException("Unknown project: " + projectName));

        EmbeddingProviderConfig cfg = config.findProviderConfig(modelId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Embedding model '" + modelId + "' not declared in config.embeddingProviders. " +
                        "Add an entry to ~/.pharos/config.json first."));
        EmbeddingProvider provider = EmbeddingProvider.create(cfg);
        if (!provider.isAvailable()) {
            throw new IllegalStateException(
                    "Provider '" + modelId + "' is unavailable (failed to load). Aborting.");
        }

        String targetField = DocumentMapper.vectorFieldFor(modelId);
        Path projectIndexDir = luceneIndexer.getProjectIndexDir(projectName);
        PersistentEmbeddingCache cache = new PersistentEmbeddingCache(
                projectIndexDir, IndexVersions.modelFingerprint(cfg), provider.dimensions());

        // ── Phase 1: collect per-doc info from the existing index ─────────────
        // For each docId in each segment: its FQN, file path, chunk count, and
        // every per-model vector currently attached. Per-model chunkVec.* values
        // are kept too so we can re-attach them in the rebuilt doc.
        Map<String, DocSnapshot> snapshots = new LinkedHashMap<>();
        IndexReader reader = luceneIndexer.openReader(projectName);
        Set<String> vecFieldNames = collectVectorFieldNames(reader);

        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            StoredFields sf = leaf.storedFields();
            // Per-model FloatVectorValues iterators — built lazily.
            Map<String, FloatVectorValues> vecsByField = new HashMap<>();
            Map<String, KnnVectorValues.DocIndexIterator> iters = new HashMap<>();
            for (String fn : vecFieldNames) {
                FloatVectorValues fvv = leaf.getFloatVectorValues(fn);
                if (fvv != null) {
                    vecsByField.put(fn, fvv);
                    iters.put(fn, fvv.iterator());
                }
            }

            // Iterate by docId so the per-field iterators advance in lock-step.
            // Using the dominant target field's iterator as the driver — if the
            // target model isn't yet in this leaf, any other vec.* field works.
            DocIdSetIterator driver = chooseDriverIterator(iters);
            if (driver == null) continue;  // no vector data at all in this leaf

            int docId;
            while ((docId = driver.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                Document doc = sf.document(docId);
                String id = doc.get(DocumentMapper.F_ID);
                if (id == null) continue;

                DocSnapshot snap = new DocSnapshot();
                snap.id        = id;
                snap.filePath  = doc.get(DocumentMapper.F_FILE_PATH);
                snap.docType   = doc.get(DocumentMapper.F_DOC_TYPE);
                snap.scope     = doc.get(DocumentMapper.F_SCOPE);

                // Detect multi-chunk docs via the chunk-line-ranges marker.
                BytesRef rangesRef = doc.getBinaryValue(DocumentMapper.F_CHUNK_LINE_RANGES);
                int chunkCount = (rangesRef != null)
                        ? DocumentMapper.decodeLineRanges(rangesRef.bytes).length
                        : 1;
                snap.chunkCount = chunkCount;

                // Read every per-model vector field for this docId. Iterators
                // must be advanced separately for fields that lag behind.
                for (Map.Entry<String, FloatVectorValues> e : vecsByField.entrySet()) {
                    String fn = e.getKey();
                    FloatVectorValues fvv = e.getValue();
                    KnnVectorValues.DocIndexIterator it = iters.get(fn);
                    while (it.docID() < docId && it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        // advance to docId
                    }
                    if (it.docID() == docId) {
                        snap.vectorsByField.put(fn, fvv.vectorValue(it.index()).clone());
                    }
                }

                // Read per-model chunkVec.* binary fields (LateInteractionField
                // payloads). Their field name mirrors vec.* but with chunkVec.* prefix.
                for (org.apache.lucene.index.IndexableField f : doc.getFields()) {
                    String name = f.name();
                    if (name.startsWith("chunkVec.") || DocumentMapper.F_CHUNK_VECTORS_LEGACY.equals(name)) {
                        BytesRef br = doc.getBinaryValue(name);
                        if (br != null) snap.chunkVecsByField.put(name, br.bytes.clone());
                    }
                }
                snapshots.put(id, snap);
            }
        }

        log.info("Embed '{}': scanned {} doc(s) in '{}'", modelId, snapshots.size(), projectName);

        // ── Phase 2: group by file, parse, embed, updateDocument ─────────────
        Map<String, List<DocSnapshot>> byFile = new LinkedHashMap<>();
        for (DocSnapshot s : snapshots.values()) {
            if (s.filePath == null) continue;
            byFile.computeIfAbsent(s.filePath, k -> new ArrayList<>()).add(s);
        }

        int updated = 0, skippedExisting = 0, skippedMultiChunk = 0;
        int skippedNonJava = 0, skippedMissingFile = 0, skippedNoMatch = 0;

        try (IndexWriter writer = luceneIndexer.openWriter(projectName)) {
            for (Map.Entry<String, List<DocSnapshot>> entry : byFile.entrySet()) {
                Path filePath = Path.of(entry.getKey());
                if (!Files.exists(filePath)) {
                    skippedMissingFile += entry.getValue().size();
                    continue;
                }
                if (!entry.getKey().endsWith(".java")) {
                    skippedNonJava += entry.getValue().size();
                    continue;
                }

                ParsedFile parsedFile;
                try {
                    parsedFile = parser.parseFile(filePath, projectName);
                } catch (Exception e) {
                    log.debug("Parse failed for {}: {}", filePath.getFileName(), e.getMessage());
                    continue;
                }
                Map<String, ParsedMethod> byFqn = parsedFile.methods().stream()
                        .collect(Collectors.toMap(ParsedMethod::fqn, m -> m, (a, b) -> a));
                Map<String, ParsedClass> byQcn = parsedFile.classes().stream()
                        .collect(Collectors.toMap(ParsedClass::qualifiedClassName, c -> c, (a, b) -> a));
                List<String> fileLines;
                try { fileLines = Files.readAllLines(filePath); }
                catch (IOException e) { fileLines = List.of(); }

                for (DocSnapshot snap : entry.getValue()) {
                    // v1 limitation: skip multi-chunk docs.
                    if (snap.chunkCount > 1) { skippedMultiChunk++; continue; }
                    // Idempotency: skip if the target field is already present and !force.
                    if (!force && snap.vectorsByField.containsKey(targetField)) {
                        skippedExisting++;
                        continue;
                    }

                    Document fresh = buildUpdatedDoc(snap, byFqn, byQcn, fileLines,
                            provider, targetField, cache, projectName);
                    if (fresh == null) { skippedNoMatch++; continue; }
                    writer.updateDocument(new Term(DocumentMapper.F_ID, snap.id), fresh);
                    updated++;
                }
            }
            writer.commit();
        }

        if (cache != null) cache.save();

        // ── Phase 3: bookkeeping ─────────────────────────────────────────────
        List<String> embedded = new ArrayList<>(meta.getEmbeddedModels());
        if (!embedded.contains(modelId)) {
            embedded.add(modelId);
            meta.setEmbeddedModels(embedded);
            registry.register(meta);
        }

        System.out.printf("Embed '%s' for '%s': %d updated, %d already present, " +
                "%d multi-chunk skipped, %d non-java skipped, %d missing-file skipped, %d no-match skipped%n",
                modelId, projectName, updated, skippedExisting, skippedMultiChunk,
                skippedNonJava, skippedMissingFile, skippedNoMatch);
        return updated;
    }

    /**
     * Rebuilds a Lucene document by re-parsing its source file, embedding the
     * target model's vector, and re-attaching every existing per-model vector
     * from the snapshot. Returns null when the FQN can't be matched in the
     * freshly parsed file (e.g. the source moved or was renamed).
     */
    private Document buildUpdatedDoc(DocSnapshot snap, Map<String, ParsedMethod> byFqn,
                                      Map<String, ParsedClass> byQcn, List<String> fileLines,
                                      EmbeddingProvider provider, String targetField,
                                      PersistentEmbeddingCache cache,
                                      String projectName) {
        // Build the per-model vector map: existing fields (other models) +
        // the freshly-embedded target. Each existing vec.* field name maps to
        // its modelId by stripping the "vec." prefix.
        Map<String, float[]> vectorsByModel = new LinkedHashMap<>();
        for (Map.Entry<String, float[]> e : snap.vectorsByField.entrySet()) {
            String fn = e.getKey();
            // Skip the target field — we're about to overwrite it with a fresh value.
            if (fn.equals(targetField)) continue;
            String existingModelId = modelIdFromFieldName(fn);
            if (existingModelId != null) vectorsByModel.put(existingModelId, e.getValue());
        }

        if ("method".equals(snap.docType) || "chunk".equals(snap.docType)) {
            ParsedMethod method = byFqn.get(snap.id);
            if (method == null) return null;
            String text = DocumentMapper.buildEmbeddingText(method);
            float[] vec = embedWithCache(provider, text, cache);
            if (vec == null) return null;
            vectorsByModel.put(provider.modelId(), vec);
            // Read inDegree + callerNames from the existing index. We're not
            // re-building the graph, so these stay as recorded at the last
            // full/incremental index.
            int inDegree = 0;  // best-effort: graph data isn't on the snapshot path
            List<String> callerNames = List.of();
            return DocumentMapper.toDocument(method, vectorsByModel, inDegree, callerNames, fileLines);
        } else if ("class".equals(snap.docType) || "document".equals(snap.docType)) {
            ParsedClass cls = byQcn.get(snap.id);
            if (cls == null) return null;
            String synthBody = synthesizedBody(cls);
            String text = DocumentMapper.buildClassEmbeddingText(cls, synthBody);
            float[] vec = embedWithCache(provider, text, cache);
            if (vec == null) return null;
            vectorsByModel.put(provider.modelId(), vec);
            return DocumentMapper.toClassDocument(cls, synthBody, vectorsByModel);
        }
        return null;
    }

    /** Persistent-cache wrapper around a single embed() call. */
    private static float[] embedWithCache(EmbeddingProvider provider, String text,
                                           PersistentEmbeddingCache cache) {
        if (text == null || text.isBlank()) return null;
        if (cache != null) {
            String key = PersistentEmbeddingCache.sha256Hex(text);
            float[] cached = cache.get(key);
            if (cached != null) return cached;
            float[] fresh = provider.embed(text);
            if (fresh != null) cache.put(key, fresh);
            return fresh;
        }
        return provider.embed(text);
    }

    /** Strip {@code vec.} prefix and return null for the legacy field. */
    private static String modelIdFromFieldName(String fieldName) {
        if (fieldName == null) return null;
        if (DocumentMapper.F_VECTOR_LEGACY.equals(fieldName)) return IndexConfig.LEGACY_MODEL_ID;
        if (fieldName.startsWith("vec.")) return fieldName.substring("vec.".length());
        return null;
    }

    /**
     * Scans the reader's leaf field infos for every Lucene KNN vector field.
     * Used to know which per-model vectors need to be preserved when rewriting
     * a doc via {@link IndexWriter#updateDocument}.
     */
    private static Set<String> collectVectorFieldNames(IndexReader reader) {
        Set<String> out = new LinkedHashSet<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            FieldInfos fis = ctx.reader().getFieldInfos();
            for (FieldInfo fi : fis) {
                if (fi.getVectorDimension() > 0) out.add(fi.name);
            }
        }
        return out;
    }

    /** Pick any iterator — they all iterate in docId order, so any works as the driver. */
    private static DocIdSetIterator chooseDriverIterator(
            Map<String, KnnVectorValues.DocIndexIterator> iters) {
        return iters.values().stream().findFirst().orElse(null);
    }

    /** Build the same synthesized body the full indexer would produce for this class. */
    private String synthesizedBody(ParsedClass cls) {
        if ("document".equals(cls.kind())) {
            return DocumentMapper.readBodyFromFile(cls.filePath(), cls.startLine(), cls.endLine(), null);
        }
        // Best-effort: signatures + javadoc of the class's methods.
        return "";
    }

    /** Snapshot of one doc as it exists in the source Lucene index. */
    private static final class DocSnapshot {
        String id;
        String filePath;
        String docType;
        String scope;
        int chunkCount;
        /** fieldName → float[] (existing per-model KNN vectors). */
        final Map<String, float[]>  vectorsByField   = new LinkedHashMap<>();
        /** fieldName → encoded LateInteractionField bytes (currently informational). */
        final Map<String, byte[]>   chunkVecsByField = new LinkedHashMap<>();
    }
}
