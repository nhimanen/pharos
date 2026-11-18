package com.pharos.search.distributed;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.internal.CalleesRequest;
import com.pharos.cluster.internal.CalleesResponse;
import com.pharos.cluster.internal.GraphRequest;
import com.pharos.cluster.internal.GraphResponse;
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

    /** One shard pinned for one operation: the generation and the reader of that generation. */
    public record Pin(ShardId shard, long generation, IndexReader reader, Path shardDir) {}

    private final LuceneIndexer lucene;
    private final Predicate<String> serves;
    private final KeywordSearchStrategy keyword;
    private final VectorSearchStrategy vector;
    private final UnifiedRetrievalStage unified;

    /**
     * @param serves whether this node holds a shard (on readers: an active generation)
     */
    public LocalShardService(LuceneIndexer lucene, Predicate<String> serves, KeywordSearchStrategy keyword,
                             VectorSearchStrategy vector, UnifiedRetrievalStage unified) {
        this.lucene = lucene;
        this.serves = serves;
        this.keyword = keyword;
        this.vector = vector;
        this.unified = unified;
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

    /** Pins the shard's current reader and its generation. */
    public Pin pin(ShardId shard) throws IOException {
        LuceneIndexer.PinnedReader p = lucene.openPinnedReader(shard.value());
        Path shardDir = p.luceneDir().getParent();
        return new Pin(shard, generationOf(shardDir), p.reader(), shardDir);
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
        Pin pin = pin(new ShardId(request.shard()));
        List<Term> terms = new ArrayList<>();
        if (request.terms() != null) {
            for (TermStatsRequest.Term t : request.terms()) terms.add(new Term(t.field(), t.text()));
        }
        List<String> fields = request.fields() == null ? List.of() : request.fields();
        return new TermStatsResponse(pin.generation(), termStats(pin, fields, terms));
    }

    /** The pinned shard's statistics of {@code fields} and {@code terms}. */
    public GlobalStats termStats(Pin pin, Collection<String> fields, Collection<Term> terms) throws IOException {
        return GlobalStats.of(new IndexSearcher(pin.reader()), fields, terms);
    }

    // ── raw retrieval ─────────────────────────────────────────────────────────

    @Override
    public ShardSearchResponse searchRaw(ShardSearchRequest request) throws IOException {
        checkSynonyms(request.synonymsHash());
        Pin pin = pin(new ShardId(request.shard()));
        if (other(request.generation(), pin)) return new ShardSearchResponse(pin.generation(), 1, List.of());
        return search(pin, request.retriever(), request.request(), request.queryVector(), request.globalStats());
    }

    /**
     * Raw candidates of one retrieval stage over the pinned shard, scored with {@code stats} (null:
     * the shard's own statistics). A fresh searcher per call: stages set their similarity on it.
     */
    public ShardSearchResponse search(Pin pin, String retriever, SearchRequest req, float[] queryVector,
                                      GlobalStats stats) throws IOException {
        IndexSearcher searcher = stats == null ? new IndexSearcher(pin.reader())
                : new GlobalStatsSearcher(pin.reader(), stats);
        List<RawCandidate> raw;
        String intent = null;
        switch (retriever == null ? "" : retriever) {
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

    // ── snippets ──────────────────────────────────────────────────────────────

    @Override
    public SnippetsResponse snippets(SnippetsRequest request) throws IOException {
        Pin pin = pin(new ShardId(request.shard()));
        if (other(request.generation(), pin)) return new SnippetsResponse(pin.generation(), Map.of());
        return new SnippetsResponse(pin.generation(),
                snippets(pin, request.ids(), request.query(), request.queryVector()));
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
        Pin pin = pin(new ShardId(request.shard()));
        if (other(request.generation(), pin)) return new CalleesResponse(pin.generation(), Map.of());
        return new CalleesResponse(pin.generation(), callees(pin, request.fqns()));
    }

    /** The called methods (stored {@code calledMethods}) of each {@code fqn} found on the pinned shard. */
    public Map<String, List<String>> callees(Pin pin, List<String> fqns) throws IOException {
        Map<String, List<String>> out = new LinkedHashMap<>();
        if (fqns == null) return out;
        IndexSearcher searcher = new IndexSearcher(pin.reader());
        StoredFields stored = searcher.storedFields();
        for (String fqn : fqns) {
            TopDocs hits = searcher.search(new TermQuery(new Term(DocumentMapper.F_ID, pin.shard().value() + ":" + fqn)), 1);
            if (hits.scoreDocs.length == 0) continue;
            Document doc = stored.document(hits.scoreDocs[0].doc);
            out.put(fqn, List.of(doc.getValues(DocumentMapper.F_CALLED_METHODS)));
        }
        return out;
    }

    // ── graph ─────────────────────────────────────────────────────────────────

    @Override
    public GraphResponse graph(GraphRequest request) throws IOException {
        Pin pin = pin(new ShardId(request.shard()));
        if (other(request.generation(), pin)) return new GraphResponse(pin.generation(), null, null, null);
        return graph(pin, request);
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
                IndexSearcher searcher = new IndexSearcher(pin.reader());
                TopDocs hits = searcher.search(new TermQuery(new Term(DocumentMapper.F_CALLED_METHODS, fqns.get(0))),
                        Math.max(1, request.limit()));
                yield new GraphResponse(pin.generation(), null, null,
                        KeywordSearchStrategy.rawCandidates(searcher, hits, "graph"));
            }
            case "neighbors", "usages", "path" -> {
                try (GraphView g = GraphViews.open(pin.shard().value(), pin.shardDir())) {
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
        for (String fqn : fqns) ids.add(pin.shard().value() + ":" + fqn);
        Map<String, SearchResult> byId = docsById(pin, ids);
        Map<String, SearchResult> out = new LinkedHashMap<>();
        for (String fqn : fqns) {
            SearchResult r = byId.get(pin.shard().value() + ":" + fqn);
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
