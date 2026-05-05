package com.pharos.search;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Facade for all search operations. Routes to keyword, vector, or hybrid strategy
 * and handles multi-project index opening.
 */
public class SearchEngine {

    private static final Logger log = LoggerFactory.getLogger(SearchEngine.class);

    private final LuceneIndexer luceneIndexer;
    private final EmbeddingProvider embedder;
    private final ProjectRegistry registry;
    private final KeywordSearchStrategy keywordStrategy;
    private final HybridSearchStrategy hybridStrategy;
    private final VectorSearchStrategy vectorStrategy;

    public SearchEngine(LuceneIndexer luceneIndexer, EmbeddingProvider embedder,
                        ProjectRegistry registry) {
        this.luceneIndexer = luceneIndexer;
        this.embedder = embedder;
        this.registry = registry;
        this.keywordStrategy = new KeywordSearchStrategy();
        this.hybridStrategy = new HybridSearchStrategy(embedder);
        this.vectorStrategy = new VectorSearchStrategy(embedder);
    }

    public List<SearchResult> search(SearchRequest req) throws IOException {
        return search(req, false);
    }

    /**
     * Search with optional neighborhood expansion.
     *
     * When {@code expand} is true, appends callee methods of the top-3 primary
     * results as additional "related" entries (score = parent_score * 0.5).
     * Deduplicates against primary results by document ID.
     */
    public List<SearchResult> search(SearchRequest req, boolean expand) throws IOException {
        return searchWithTrace(req, expand).results();
    }

    /**
     * Search and return results together with a detailed timing trace.
     * Use this from {@link com.pharos.cli.SearchCommand} when {@code --debug} is set.
     */
    public SearchResponse searchWithTrace(SearchRequest req, boolean expand) throws IOException {
        SearchTrace trace = new SearchTrace();
        trace.start();

        List<String> projects = resolveProjects(req);
        if (projects.isEmpty()) {
            log.warn("No indexed projects found. Run 'pharos index <path>' first.");
            return new SearchResponse(List.of(), trace);
        }

        IndexReader reader = luceneIndexer.openMultiReader(projects);

        List<SearchResult> primary;
        switch (req.type()) {
            case KEYWORD -> {
                long t = System.currentTimeMillis();
                primary = keywordStrategy.search(reader, req);
                trace.record("keyword search", t);
            }
            case VECTOR -> {
                long tEmbed = System.currentTimeMillis();
                // Embedding happens inside VectorSearchStrategy; we measure the whole call
                primary = vectorStrategy.search(reader, req);
                trace.record("vector search (incl. embed)", tEmbed);
            }
            case HYBRID -> {
                long tKw = System.currentTimeMillis();
                List<SearchResult> kwResults = keywordStrategy.search(reader, req);
                trace.record("keyword search", tKw);

                if (!embedder.isAvailable()) {
                    primary = kwResults;
                    trace.record("vector search skipped (no embedder)", System.currentTimeMillis());
                } else {
                    long tVec = System.currentTimeMillis();
                    List<SearchResult> vecResults = vectorStrategy.search(reader, req);
                    trace.record("vector search (incl. embed)", tVec);
                    long tRrf = System.currentTimeMillis();
                    primary = hybridStrategy.fuse(kwResults, vecResults, req.limit(), req.query());
                    trace.record("rrf fusion", tRrf);
                }
            }
            default -> primary = List.of();
        }

        if (!expand || primary.isEmpty()) {
            return new SearchResponse(primary, trace);
        }

        long tExpand = System.currentTimeMillis();
        List<SearchResult> expanded = expandNeighborhood(primary, req.project());
        trace.record("neighborhood expansion", tExpand);
        return new SearchResponse(expanded, trace);
    }

    /**
     * Expands primary results by fetching callee documents for the top-3 hits.
     * Related results get score = parent_score * 0.5, searchType = "related".
     * Already-seen IDs are skipped to avoid duplicates.
     */
    private List<SearchResult> expandNeighborhood(List<SearchResult> primary, String project) throws IOException {
        Set<String> seen = new LinkedHashSet<>();
        for (SearchResult r : primary) seen.add(r.id());

        List<SearchResult> expanded = new ArrayList<>(primary);

        // Expand callees of top-3 results
        int expandLimit = Math.min(3, primary.size());
        for (int i = 0; i < expandLimit; i++) {
            SearchResult parent = primary.get(i);
            // Extract the FQN from the id: id format is "project:fqn"
            String id = parent.id();
            int colon = id.indexOf(':');
            if (colon < 0) continue;
            String fqn = id.substring(colon + 1);

            List<SearchResult> callees = findCallees(fqn, project);
            for (SearchResult callee : callees) {
                if (!seen.contains(callee.id())) {
                    seen.add(callee.id());
                    expanded.add(new SearchResult(
                            callee.id(), callee.project(), callee.packageName(),
                            callee.className(), callee.qualifiedClassName(), callee.methodName(),
                            callee.signature(), callee.returnType(), callee.body(), callee.javadoc(),
                            callee.accessModifier(), callee.filePath(),
                            callee.startLine(), callee.endLine(),
                            parent.score() * 0.5f, "related", callee.docType()
                    ));
                }
            }
        }
        return expanded;
    }

    /** Look up a method by its exact FQN (e.g., "com.example.MyClass#myMethod(String,int)"). */
    public SearchResult getMethodByFqn(String fqn) throws IOException {
        List<String> projects = registry.listAll().stream()
                .map(ProjectMeta::getName)
                .collect(Collectors.toList());
        if (projects.isEmpty()) return null;

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Search across all projects by matching the FQN suffix of the id field
        // id format: "projectName:fqn" — search all projects
        for (String project : projects) {
            String id = project + ":" + fqn;
            TopDocs hits = searcher.search(new TermQuery(new Term(DocumentMapper.F_ID, id)), 1);
            if (hits.totalHits.value() > 0) {
                StoredFields storedFields = searcher.storedFields();
                Document doc = storedFields.document(hits.scoreDocs[0].doc);
                return KeywordSearchStrategy.docToResult(doc, 1.0f, "exact");
            }
        }
        return null;
    }

    /** Find all methods that call the given FQN (reverse lookup via calledMethods field). */
    public List<SearchResult> findCallers(String calleeFqn, String project) throws IOException {
        SearchRequest req = SearchRequest.keyword("", project, 100);
        List<String> projects = resolveProjects(req);
        if (projects.isEmpty()) return List.of();

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);

        TermQuery calleeQuery = new TermQuery(new Term(DocumentMapper.F_CALLED_METHODS, calleeFqn));
        TopDocs hits = searcher.search(calleeQuery, 100);
        return KeywordSearchStrategy.toResults(searcher, hits, "graph");
    }

    /** Find all methods that the given FQN calls (forward lookup via calledMethods field). */
    public List<SearchResult> findCallees(String callerFqn, String project) throws IOException {
        SearchResult caller = getMethodByFqn(callerFqn);
        if (caller == null) return List.of();

        // The calledMethods field stores FQNs; we look them up directly
        // For now return the caller with its callees via a direct document lookup
        List<String> projects = resolveProjects(SearchRequest.keyword("", project, 100));
        if (projects.isEmpty()) return List.of();

        IndexReader reader = luceneIndexer.openMultiReader(projects);
        IndexSearcher searcher = new IndexSearcher(reader);
        StoredFields storedFields = searcher.storedFields();

        // Find the caller document to get its calledMethods values
        for (String proj : projects) {
            String id = proj + ":" + callerFqn;
            TopDocs hits = searcher.search(new TermQuery(new Term(DocumentMapper.F_ID, id)), 1);
            if (hits.totalHits.value() > 0) {
                Document doc = storedFields.document(hits.scoreDocs[0].doc);
                String[] calledFqns = doc.getValues(DocumentMapper.F_CALLED_METHODS);
                if (calledFqns.length == 0) return List.of();

                // Look up each callee
                return java.util.Arrays.stream(calledFqns)
                        .map(calleeFqn -> {
                            try { return getMethodByFqn(calleeFqn); } catch (IOException e) { return null; }
                        })
                        .filter(r -> r != null)
                        .collect(Collectors.toList());
            }
        }
        return List.of();
    }

    private List<String> resolveProjects(SearchRequest req) {
        if (req.project() != null && !req.project().isEmpty()) {
            return List.of(req.project());
        }
        if (req.projects() != null && !req.projects().isEmpty()) {
            return req.projects();
        }
        // All indexed projects
        return registry.listAll().stream()
                .map(ProjectMeta::getName)
                .filter(name -> luceneIndexer.indexExists(name))
                .collect(Collectors.toList());
    }
}
