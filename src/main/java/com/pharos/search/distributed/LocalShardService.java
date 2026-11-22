package com.pharos.search.distributed;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.CalleesRequest;
import com.pharos.cluster.internal.CalleesResponse;
import com.pharos.cluster.internal.GraphRequest;
import com.pharos.cluster.internal.GraphResponse;
import com.pharos.cluster.internal.ModelMismatchException;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.cluster.internal.ShardSearchRequest;
import com.pharos.cluster.internal.ShardSearchResponse;
import com.pharos.cluster.internal.ShardSearchService;
import com.pharos.cluster.internal.SnippetsRequest;
import com.pharos.cluster.internal.SnippetsResponse;
import com.pharos.cluster.internal.SynonymsMismatchException;
import com.pharos.cluster.internal.TermStatsRequest;
import com.pharos.cluster.internal.TermStatsResponse;
import com.pharos.graph.GraphView;
import com.pharos.graph.GraphViews;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.Snippet;
import com.pharos.search.SnippetResolver;
import com.pharos.search.VectorSearchStrategy;
import com.pharos.search.pipeline.UnifiedRetrievalStage;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * The shard side of fan-out, over the shards this node holds: raw retrieval with the coordinator's
 * global statistics, term statistics, snippets, callees and graph lookups. It answers the
 * {@code /internal/*} endpoints ({@link ShardSearchService}) and, in process, a coordinator's own
 * local shards, so both run the same code.
 *
 * <p><b>Generation pinning.</b> Every operation first {@link #pin pins} the shard: the reader and
 * the generation dir it reads are taken together under {@link LuceneIndexer}'s lock, and everything
 * the operation reads (term statistics, raw search, stored fields, snippets, callees, the call graph)
 * comes from that one pair. The response states the pinned generation; {@code InternalApi} and the
 * coordinator refuse it when it is not the generation the query pinned. A replaced reader stays open
 * for {@link LuceneIndexer#RETIRE_DELAY}, far longer than a query.
 *
 * <p>A request analysed with other synonyms ({@code synonymsHash} differs from this node's) is
 * refused with {@link SynonymsMismatchException}.
 */
public final class LocalShardService implements ShardSearchService {

    private static final Pattern GEN_DIR = Pattern.compile("gen-(\\d{1,18})");

    /**
     * One shard pinned: the generation and the reader of that generation, holding a reference on the
     * reader until {@link #close} (a swap retires the reader; it is closed only once released).
     * {@code project} is the project whose documents the generation holds (their ids are
     * {@code <project>:<fqn>}), {@code projectGeneration} the index run of a split project's shard
     * (null otherwise); both from the generation's manifest.
     */
    public record Pin(ShardId shard, long generation, IndexReader reader, Path shardDir, String project,
                      Long projectGeneration) implements AutoCloseable {
        public Pin(ShardId shard, long generation, IndexReader reader, Path shardDir) {
            this(shard, generation, reader, shardDir, shard.value(), null);
        }

        @Override
        public void close() throws IOException {
            reader.decRef();
        }
    }

    /** What a generation dir's manifest says about its documents; see {@link Pin}. */
    private record GenInfo(String project, Long projectGeneration) {}

    /** Per generation dir (immutable once materialised): read once, its manifest is small. */
    private final Map<Path, GenInfo> genInfo = new java.util.concurrent.ConcurrentHashMap<>();

    /** What a node knows of the search embedding model it serves vectors for. */
    public record Model(String modelId, int dims, String fingerprint) {}

    private final LuceneIndexer lucene;
    private final Predicate<String> serves;
    private final KeywordSearchStrategy keyword;
    private final VectorSearchStrategy vector;
    private final UnifiedRetrievalStage unified;
    private final ProjectRegistry registry;
    private final Model model;

    /**
     * @param serves   whether this node holds a shard (on readers: an active generation)
     * @param registry the node's project registry: which embedding models each shard has vectors for
     * @param model    this node's search model ({@code fingerprint} may be null: not compared)
     */
    public LocalShardService(LuceneIndexer lucene, Predicate<String> serves, KeywordSearchStrategy keyword,
                             VectorSearchStrategy vector, UnifiedRetrievalStage unified, ProjectRegistry registry,
                             Model model) {
        this.lucene = lucene;
        this.serves = serves;
        this.keyword = keyword;
        this.vector = vector;
        this.unified = unified;
        this.registry = registry;
        this.model = model;
    }

    /** This node's search model. */
    public Model model() {
        return model;
    }

    public KeywordSearchStrategy keyword() {
        return keyword;
    }

    /** The synonyms this node analyses queries with ({@link KeywordSearchStrategy#synonymsHash}). */
    public String synonymsHash() {
        return keyword.synonymsHash();
    }

    @Override
    public OptionalLong servedGeneration(ShardId shard) {
        if (!serves.test(shard.value())) return OptionalLong.empty();
        return OptionalLong.of(generationOf(lucene.getProjectIndexDir(shard.value())));
    }

    /** Pins the shard's current reader and its generation, holding a reference: close the pin. */
    public Pin pin(ShardId shard) throws IOException {
        for (int attempt = 0; attempt < 3; attempt++) {
            LuceneIndexer.PinnedReader p = lucene.openPinnedReader(shard.value());
            if (!p.reader().tryIncRef()) continue; // closed between the open and here: take the new one
            Path shardDir = p.luceneDir().getParent();
            GenInfo info = genInfo(shard, shardDir);
            return new Pin(shard, generationOf(shardDir), p.reader(), shardDir, info.project(), info.projectGeneration());
        }
        throw new IOException("the reader of shard " + shard + " kept closing while being pinned");
    }

    /**
     * The project and index run of a generation dir, from its {@code manifest.json} (a reader's
     * generation); a dir without one (a single node's project dir) holds the documents of the project
     * named like the shard. The project is the writer's: the one in the documents' ids.
     */
    private GenInfo genInfo(ShardId shard, Path shardDir) {
        if (shardDir == null) return new GenInfo(shard.value(), null);
        GenInfo cached = genInfo.get(shardDir);
        if (cached != null) return cached;
        GenInfo info = new GenInfo(shard.value(), null);
        Path manifest = shardDir.resolve("manifest.json");
        if (java.nio.file.Files.isRegularFile(manifest)) {
            try {
                com.pharos.cluster.objectstore.ShardManifest m = com.pharos.cluster.objectstore.ShardManifest.read(
                        java.nio.file.Files.readAllBytes(manifest));
                String project = m.projectMeta() != null && m.projectMeta().getName() != null
                        ? m.projectMeta().getName() : shard.value();
                info = new GenInfo(project, m.projectGeneration());
            } catch (IOException | RuntimeException e) {
                return info; // not cached: read again next time
            }
        }
        if (genInfo.size() > 4096) genInfo.clear(); // generation dirs come and go; never grows without bound
        genInfo.put(shardDir, info);
        return info;
    }

    /**
     * Refuses a vector request this shard cannot answer with the coordinator's model, as a single
     * node refuses a vector search on a project without vectors of its model.
     */
    void checkModel(ShardId shard, String modelId, int dims, String fingerprint) {
        if (modelId == null) return;
        ProjectMeta meta = registry == null ? null : registry.find(shard.value()).orElse(null);
        if (meta != null) {
            List<String> embedded = meta.getEmbeddedModels();
            boolean has = embedded.contains(modelId)
                    || (embedded.isEmpty() && IndexConfig.LEGACY_MODEL_ID.equals(modelId));
            if (!has) {
                throw new ModelMismatchException(ModelMismatchException.Reason.NO_VECTORS,
                        "no vectors for model " + modelId + " (has: "
                        + (embedded.isEmpty() ? "<legacy / none recorded>" : String.join(", ", embedded)) + ")");
            }
        }
        if (model == null) return;
        if (!modelId.equals(model.modelId())) {
            throw new ModelMismatchException("this reader searches with model " + model.modelId() + ", not " + modelId);
        }
        if (dims > 0 && model.dims() > 0 && dims != model.dims()) {
            throw new ModelMismatchException("model " + modelId + " has " + model.dims() + " dimensions here, "
                    + dims + " at the coordinator");
        }
        if (fingerprint != null && model.fingerprint() != null && !fingerprint.equals(model.fingerprint())) {
            throw new ModelMismatchException("model " + modelId + " is configured differently here (fingerprint "
                    + model.fingerprint() + " vs " + fingerprint + ")");
        }
    }

    /** {@code gen-N} → N; 0 for a shard dir that is not a generation dir (a single node's project dir). */
    static long generationOf(Path shardDir) {
        if (shardDir == null || shardDir.getFileName() == null) return 0;
        var m = GEN_DIR.matcher(shardDir.getFileName().toString());
        return m.matches() ? Long.parseLong(m.group(1)) : 0;
    }

    private void checkSynonyms(String hash) {
        if (hash != null && !hash.equals(synonymsHash())) {
            throw new SynonymsMismatchException("synonyms hash " + hash + " differs from this reader's "
                    + synonymsHash());
        }
    }

    private static boolean other(Long wanted, Pin pin) {
        return wanted != null && wanted != pin.generation();
    }

    // ── term statistics ───────────────────────────────────────────────────────

    @Override
    public TermStatsResponse termStats(TermStatsRequest request) throws IOException {
        checkSynonyms(request.synonymsHash());
        List<Term> terms = new ArrayList<>();
        if (request.terms() != null) {
            for (TermStatsRequest.Term t : request.terms()) terms.add(new Term(t.field(), t.text()));
        }
        List<String> fields = request.fields() == null ? List.of() : request.fields();
        try (Pin pin = pin(new ShardId(request.shard()))) {
            return new TermStatsResponse(pin.generation(), termStats(pin, fields, terms), pin.projectGeneration());
        }
    }

    /** The pinned shard's statistics of {@code fields} and {@code terms}. */
    public GlobalStats termStats(Pin pin, Collection<String> fields, Collection<Term> terms) throws IOException {
        return GlobalStats.of(new IndexSearcher(pin.reader()), fields, terms);
    }

    // ── raw retrieval ─────────────────────────────────────────────────────────

    @Override
    public ShardSearchResponse searchRaw(ShardSearchRequest request) throws IOException {
        checkSynonyms(request.synonymsHash());
        if (request.queryVector() != null && !"keyword".equals(request.retriever())) {
            checkModel(new ShardId(request.shard()), request.modelId(), request.dims(), request.modelFingerprint());
        }
        try (Pin pin = pin(new ShardId(request.shard()))) {
            if (other(request.generation(), pin)) return new ShardSearchResponse(pin.generation(), 1, List.of());
            return search(pin, request.retriever(), request.request(), request.queryVector(), request.globalStats(),
                    request.selected(), request.selectedKnn(), request.chunkVectors());
        }
    }

    /**
     * Raw candidates of one retrieval stage over the pinned shard, scored with {@code stats} (null:
     * the shard's own statistics). A fresh searcher per call: stages set their similarity on it.
     */
    public ShardSearchResponse search(Pin pin, String retriever, SearchRequest req, float[] queryVector,
                                      GlobalStats stats) throws IOException {
        return search(pin, retriever, req, queryVector, stats, null, null, false);
    }

    /** As above, with the two-round retrievers' selections (see {@code ShardSearchRequest}). */
    public ShardSearchResponse search(Pin pin, String retriever, SearchRequest req, float[] queryVector,
                                      GlobalStats stats, List<ScoredId> selected, List<ScoredId> selectedKnn,
                                      boolean chunkVectors) throws IOException {
        if (req != null && req.hasKeyFilter()) {
            com.pharos.search.KeyFiltersUnsupportedException.check(pin.project(), pin.shardDir());
        }
        IndexSearcher searcher = stats == null ? new IndexSearcher(pin.reader())
                : new GlobalStatsSearcher(pin.reader(), stats);
        List<RawCandidate> raw;
        String intent = null;
        switch (retriever == null ? "" : retriever) {
            case "vector-knn" -> {
                if (queryVector == null) return ShardSearchResponse.recall(pin.generation(), List.of(), List.of(), false);
                return ShardSearchResponse.recall(pin.generation(),
                        scoredIds(searcher, vector.firstPass(searcher, req, queryVector)), List.of(),
                        vector.hasChunkVectors(pin.reader()));
            }
            case "unified-recall" -> {
                List<ScoredId> bm25 = scoredIds(searcher, unified.bm25Recall(searcher, req));
                List<ScoredId> knn = queryVector == null ? List.of()
                        : scoredIds(searcher, unified.knnRecall(searcher, req, queryVector));
                return ShardSearchResponse.recall(pin.generation(), bm25, knn, false);
            }
            case "vector-rescore" -> {
                ScoreDoc[] sel = select(searcher, selected);
                raw = vector.finish(searcher, new TopDocs(new org.apache.lucene.search.TotalHits(sel.length,
                        org.apache.lucene.search.TotalHits.Relation.EQUAL_TO), sel), queryVector, req.limit(), chunkVectors);
            }
            case "unified-rescore" -> {
                searcher.setSimilarity(new org.apache.lucene.search.similarities.BM25Similarity());
                UnifiedRetrievalStage.RawRetrieval r = queryVector == null
                        ? unified.bm25Only(searcher, req, select(searcher, selected))
                        : unified.rescore(searcher, req, queryVector, select(searcher, selected), select(searcher, selectedKnn));
                raw = r.candidates();
                intent = r.shapingIntent();
            }
            case "keyword" -> raw = keyword.retrieveRaw(searcher, req);
            case "vector" -> raw = queryVector == null ? List.of() : vector.retrieveRaw(searcher, req, queryVector);
            case "unified" -> {
                UnifiedRetrievalStage.RawRetrieval r = unified.retrieveRaw(searcher, req, queryVector, null);
                raw = r.candidates();
                intent = r.shapingIntent();
            }
            default -> throw new IllegalArgumentException("unknown retriever '" + retriever + "'");
        }
        int maxInDegree = 1;
        for (RawCandidate c : raw) maxInDegree = Math.max(maxInDegree, c.inDegree());
        return new ShardSearchResponse(pin.generation(), maxInDegree, raw, intent);
    }

    /** The stable ids and scores of {@code hits}, in order. */
    private static List<ScoredId> scoredIds(IndexSearcher searcher, TopDocs hits) throws IOException {
        StoredFields stored = searcher.storedFields();
        List<ScoredId> out = new ArrayList<>(hits.scoreDocs.length);
        for (ScoreDoc sd : hits.scoreDocs) {
            out.add(new ScoredId(stored.document(sd.doc, Set.of(DocumentMapper.F_ID)).get(DocumentMapper.F_ID),
                    sd.doc, sd.score));
        }
        return out;
    }

    /**
     * The selected documents of this shard as score docs, in the given order with the given scores:
     * by doc id (stable within the pinned generation), each checked against its stored id.
     */
    private static ScoreDoc[] select(IndexSearcher searcher, List<ScoredId> ids) throws IOException {
        if (ids == null || ids.isEmpty()) return new ScoreDoc[0];
        StoredFields stored = searcher.storedFields();
        int maxDoc = searcher.getIndexReader().maxDoc();
        List<ScoreDoc> out = new ArrayList<>(ids.size());
        for (ScoredId i : ids) {
            if (i.doc() < 0 || i.doc() >= maxDoc
                    || !i.id().equals(stored.document(i.doc(), Set.of(DocumentMapper.F_ID)).get(DocumentMapper.F_ID))) {
                throw new IllegalStateException("selected document " + i.doc() + " is not '" + i.id()
                        + "' in this generation");
            }
            out.add(new ScoreDoc(i.doc(), i.score()));
        }
        return out.toArray(new ScoreDoc[0]);
    }

    // ── snippets ──────────────────────────────────────────────────────────────

    @Override
    public SnippetsResponse snippets(SnippetsRequest request) throws IOException {
        try (Pin pin = pin(new ShardId(request.shard()))) {
            if (other(request.generation(), pin)) return new SnippetsResponse(pin.generation(), Map.of());
            return new SnippetsResponse(pin.generation(),
                    snippets(pin, request.ids(), request.query(), request.queryVector()));
        }
    }

    /**
     * Snippet position hints for result {@code ids} of the pinned shard: the vector hint (best chunk)
     * when {@code queryVector} is given, else the keyword hint for {@code query}. Ids without a hint
     * are left out.
     */
    public Map<String, Snippet> snippets(Pin pin, List<String> ids, String query, float[] queryVector)
            throws IOException {
        SnippetResolver resolver = queryVector != null
                ? vector.buildVectorResolver(pin.reader(), queryVector)
                : keyword.buildKeywordResolver(keyword.buildQuery(SearchRequest.keyword(query == null ? "" : query, null, 10)));
        Map<String, Snippet> out = new LinkedHashMap<>();
        if (ids == null) return out;
        Map<String, SearchResult> docs = docsById(pin, ids);
        for (String id : ids) {
            SearchResult r = docs.get(id);
            if (r == null) continue;
            Snippet s = resolver.resolve(r);
            if (s != null) out.put(id, s);
        }
        return out;
    }

    // ── callees ───────────────────────────────────────────────────────────────

    @Override
    public CalleesResponse callees(CalleesRequest request) throws IOException {
        try (Pin pin = pin(new ShardId(request.shard()))) {
            if (other(request.generation(), pin)) return new CalleesResponse(pin.generation(), Map.of());
            return new CalleesResponse(pin.generation(), callees(pin, request.fqns()));
        }
    }

    /** The called methods (stored {@code calledMethods}) of each {@code fqn} found on the pinned shard. */
    public Map<String, List<String>> callees(Pin pin, List<String> fqns) throws IOException {
        Map<String, List<String>> out = new LinkedHashMap<>();
        if (fqns == null) return out;
        IndexSearcher searcher = new IndexSearcher(pin.reader());
        StoredFields stored = searcher.storedFields();
        for (String fqn : fqns) {
            TopDocs hits = searcher.search(new TermQuery(new Term(DocumentMapper.F_ID, pin.project() + ":" + fqn)), 1);
            if (hits.scoreDocs.length == 0) continue;
            Document doc = stored.document(hits.scoreDocs[0].doc);
            out.put(fqn, List.of(doc.getValues(DocumentMapper.F_CALLED_METHODS)));
        }
        return out;
    }

    // ── graph ─────────────────────────────────────────────────────────────────

    @Override
    public GraphResponse graph(GraphRequest request) throws IOException {
        try (Pin pin = pin(new ShardId(request.shard()))) {
            if (other(request.generation(), pin)) return new GraphResponse(pin.generation(), null, null, null);
            return graph(pin, request);
        }
    }

    /** One {@link GraphRequest} operation over the pinned shard (its call graph is the pinned generation's). */
    public GraphResponse graph(Pin pin, GraphRequest request) throws IOException {
        List<String> fqns = request.fqns() == null ? List.of() : request.fqns();
        String op = request.op() == null ? "" : request.op();
        return switch (op) {
            case "docs" -> new GraphResponse(pin.generation(), null, docsByFqn(pin, fqns), null);
            case "called" -> new GraphResponse(pin.generation(), callees(pin, fqns), null, null);
            case "callers" -> {
                if (fqns.isEmpty()) yield new GraphResponse(pin.generation(), null, null, null);
                IndexSearcher searcher = request.globalStats() == null ? new IndexSearcher(pin.reader())
                        : new GlobalStatsSearcher(pin.reader(), request.globalStats());
                TopDocs hits = searcher.search(new TermQuery(new Term(DocumentMapper.F_CALLED_METHODS, fqns.get(0))),
                        Math.max(1, request.limit()));
                yield new GraphResponse(pin.generation(), null, null,
                        KeywordSearchStrategy.rawCandidates(searcher, hits, "graph"));
            }
            case "neighbors", "usages", "path" -> {
                try (GraphView g = GraphViews.open(pin.project(), pin.shardDir())) {
                    yield new GraphResponse(pin.generation(), graphLists(g, op, fqns, request), null, null);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException("closing the call graph of " + pin.shard() + " failed", e);
                }
            }
            default -> throw new IllegalArgumentException("unknown graph op '" + op + "'");
        };
    }

    private static Map<String, List<String>> graphLists(GraphView g, String op, List<String> fqns, GraphRequest r) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        switch (op) {
            case "neighbors" -> {
                String dir = r.direction() == null ? "both" : r.direction();
                for (String fqn : fqns) {
                    Set<String> n = new LinkedHashSet<>();
                    if (!"callees".equals(dir)) n.addAll(g.callers(fqn));
                    if (!"callers".equals(dir)) n.addAll(g.callees(fqn));
                    out.put(fqn, List.copyOf(n));
                }
            }
            case "usages" -> {
                if (fqns.isEmpty()) break;
                String fqn = fqns.get(0);
                String kind = r.kind() == null ? "all" : r.kind();
                boolean all = "all".equals(kind);
                if (all || "callers".equals(kind)) out.put("callers", List.copyOf(g.callers(fqn)));
                if (all || "subclasses".equals(kind)) {
                    out.put("subclasses", List.copyOf(g.directSubclasses(fqn)));
                    out.put("superTypes", List.copyOf(g.directSuperTypes(fqn)));
                }
                if (all || "field_readers".equals(kind)) out.put("fieldReaders", List.copyOf(g.fieldReaders(fqn)));
                if (all || "field_writers".equals(kind)) out.put("fieldWriters", List.copyOf(g.fieldWriters(fqn)));
                if (all || "annotated".equals(kind)) out.put("annotatedWith", List.copyOf(g.annotatedWith(fqn)));
                if (all || "type_refs".equals(kind)) {
                    out.put("methodsReturning", List.copyOf(g.methodsReturning(fqn)));
                    out.put("methodsTaking", List.copyOf(g.methodsTaking(fqn)));
                }
            }
            case "path" -> {
                if (!fqns.isEmpty() && r.to() != null) out.put("path", List.copyOf(g.shortestPath(fqns.get(0), r.to())));
            }
            default -> { }
        }
        return out;
    }

    /** The documents of bare {@code fqns} on the pinned shard, keyed by fqn (score 1, type "exact"). */
    public Map<String, SearchResult> docsByFqn(Pin pin, List<String> fqns) throws IOException {
        List<String> ids = new ArrayList<>(fqns.size());
        for (String fqn : fqns) ids.add(pin.project() + ":" + fqn);
        Map<String, SearchResult> byId = docsById(pin, ids);
        Map<String, SearchResult> out = new LinkedHashMap<>();
        for (String fqn : fqns) {
            SearchResult r = byId.get(pin.project() + ":" + fqn);
            if (r != null) out.put(fqn, r);
        }
        return out;
    }

    private static Map<String, SearchResult> docsById(Pin pin, List<String> ids) throws IOException {
        Map<String, SearchResult> out = new LinkedHashMap<>();
        if (ids.isEmpty()) return out;
        List<BytesRef> terms = new ArrayList<>(ids.size());
        for (String id : new LinkedHashSet<>(ids)) terms.add(new BytesRef(id));
        IndexSearcher searcher = new IndexSearcher(pin.reader());
        TopDocs hits = searcher.search(new TermInSetQuery(DocumentMapper.F_ID, terms), Math.max(1, terms.size()));
        StoredFields stored = searcher.storedFields();
        for (ScoreDoc sd : hits.scoreDocs) {
            SearchResult r = KeywordSearchStrategy.docToResult(stored.document(sd.doc), 1.0f, "exact");
            out.putIfAbsent(r.id(), r);
        }
        return out;
    }
}
