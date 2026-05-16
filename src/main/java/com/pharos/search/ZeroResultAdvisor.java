package com.pharos.search;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.config.ProjectRegistry;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Generates actionable hints when a search returns zero results.
 *
 * <p>Three hint strategies, each independently computed and independently nullable:
 * <ol>
 *   <li><b>Fuzzy matches</b> — Lucene {@link FuzzyQuery} (edit distance ≤ 2) on
 *       {@code methodName} and {@code className} fields.  Catches typos such as
 *       {@code "ConnectionPoll"} → {@code "ConnectionPool"}.</li>
 *   <li><b>Token matches</b> — individual query tokens tested one at a time.  If
 *       {@code "JWT token refresh logic"} returns nothing but {@code "JWT"} and
 *       {@code "token"} each match alone, the agent learns the combination is the issue.</li>
 *   <li><b>Filter note</b> — when active filters (project, scope, docType) may have
 *       excluded otherwise-matching results, the same query is re-run filter-free and
 *       a short explanation is returned if results appear.</li>
 * </ol>
 *
 * <p>All strategies are lazy — only executed when the primary search returns 0 results.
 */
public class ZeroResultAdvisor {

    private static final Logger log = LoggerFactory.getLogger(ZeroResultAdvisor.class);

    private static final int MAX_FUZZY_HITS  = 5;
    private static final int MAX_FUZZY_EDITS = 2;
    private static final int MIN_TERM_LENGTH = 3;

    private final LuceneIndexer         luceneIndexer;
    private final ProjectRegistry       registry;
    private final KeywordSearchStrategy keywordStrategy;

    public ZeroResultAdvisor(LuceneIndexer luceneIndexer, ProjectRegistry registry,
                              KeywordSearchStrategy keywordStrategy) {
        this.luceneIndexer   = luceneIndexer;
        this.registry        = registry;
        this.keywordStrategy = keywordStrategy;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /** A fuzzy identifier suggestion with its Lucene edit distance. */
    public record FuzzyMatch(String fqn, String label, int editDistance) {}

    /**
     * Hints returned when a search has zero results.
     * Any field may be empty / null when no useful hint was found for that strategy.
     */
    public record Suggestions(
            List<FuzzyMatch> fuzzyMatches,   // top fuzzy identifier matches
            List<String>     tokenMatches,   // query tokens that do match individually
            String           filterNote      // explanation when filters excluded results
    ) {
        public boolean isEmpty() {
            return fuzzyMatches.isEmpty() && tokenMatches.isEmpty() && filterNote == null;
        }
    }

    /**
     * Computes hints for a failed search.
     *
     * @param req       the original request that returned 0 results
     * @param projects  the project scope already resolved by the caller
     */
    public Suggestions advise(SearchRequest req, List<String> projects) {
        if (projects.isEmpty()) return empty();
        try {
            IndexReader reader  = luceneIndexer.openMultiReader(projects);
            IndexSearcher searcher = new IndexSearcher(reader);

            List<FuzzyMatch> fuzzy       = findFuzzyMatches(searcher, req.query());
            List<String>     tokenHits   = findTokenMatches(searcher, req);
            String           filterNote  = findFilterNote(searcher, req);

            return new Suggestions(fuzzy, tokenHits, filterNote);
        } catch (Exception e) {
            log.debug("ZeroResultAdvisor failed: {}", e.getMessage());
            return empty();
        }
    }

    // -------------------------------------------------------------------------
    // Strategy 1: fuzzy identifier matching
    // -------------------------------------------------------------------------

    private List<FuzzyMatch> findFuzzyMatches(IndexSearcher searcher, String query) throws IOException {
        // Use the first camelCase token as the fuzzy probe (full phrase fuzzy is too slow)
        String probe = query.trim().split("[\\s#.(]")[0];
        if (probe.length() < MIN_TERM_LENGTH) return List.of();
        String lower = probe.toLowerCase();

        BooleanQuery.Builder b = new BooleanQuery.Builder();
        b.add(new FuzzyQuery(new Term(DocumentMapper.F_METHOD_NAME, lower), MAX_FUZZY_EDITS),
                BooleanClause.Occur.SHOULD);
        b.add(new FuzzyQuery(new Term(DocumentMapper.F_CLASS_NAME, lower), MAX_FUZZY_EDITS),
                BooleanClause.Occur.SHOULD);

        TopDocs hits = searcher.search(b.build(), MAX_FUZZY_HITS);
        if (hits.scoreDocs.length == 0) return List.of();

        StoredFields storedFields = searcher.storedFields();
        List<FuzzyMatch> results = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();

        for (ScoreDoc sd : hits.scoreDocs) {
            var doc    = storedFields.document(sd.doc);
            String id  = doc.get(DocumentMapper.F_ID);
            if (id == null || !seen.add(id)) continue;

            // Reconstruct label for display
            String qualClass  = doc.get(DocumentMapper.F_QUALIFIED_CLASS);
            String methodName = doc.get(DocumentMapper.F_METHOD_NAME);
            String docType    = doc.get(DocumentMapper.F_DOC_TYPE);

            String label = ("class".equals(docType) || qualClass != null && methodName == null)
                    ? qualClass
                    : (qualClass != null && methodName != null ? qualClass + "#" + methodName : id);

            // Estimate edit distance by comparing probe against stored field values
            int dist = editDistance(lower,
                    (methodName != null ? methodName : "").toLowerCase());
            int distCls = editDistance(lower,
                    (doc.get(DocumentMapper.F_CLASS_NAME) != null
                            ? doc.get(DocumentMapper.F_CLASS_NAME) : "").toLowerCase());
            int bestDist = Math.min(dist, distCls);

            results.add(new FuzzyMatch(id.substring(id.indexOf(':') + 1), label, bestDist));
        }

        results.sort(Comparator.comparingInt(FuzzyMatch::editDistance));
        return results;
    }

    // -------------------------------------------------------------------------
    // Strategy 2: per-token match check
    // -------------------------------------------------------------------------

    private List<String> findTokenMatches(IndexSearcher searcher, SearchRequest req)
            throws IOException {
        List<String> tokens = keywordStrategy.analyzeTerms(req.query());
        if (tokens.size() <= 1) return List.of(); // single token — no value in per-token breakdown

        List<String> matching = new ArrayList<>();
        for (String token : tokens) {
            if (token.length() < MIN_TERM_LENGTH) continue;
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.add(new TermQuery(new Term(DocumentMapper.F_METHOD_NAME, token)), BooleanClause.Occur.SHOULD);
            b.add(new TermQuery(new Term(DocumentMapper.F_CLASS_NAME,  token)), BooleanClause.Occur.SHOULD);
            b.add(new TermQuery(new Term(DocumentMapper.F_BODY,        token)), BooleanClause.Occur.SHOULD);
            b.add(new TermQuery(new Term(DocumentMapper.F_JAVADOC,     token)), BooleanClause.Occur.SHOULD);
            if (searcher.search(b.build(), 1).totalHits.value() > 0) matching.add(token);
        }
        return matching;
    }

    // -------------------------------------------------------------------------
    // Strategy 3: filter exclusion check
    // -------------------------------------------------------------------------

    private String findFilterNote(IndexSearcher searcher, SearchRequest req) throws IOException {
        boolean hasProject = req.project()  != null && !req.project().isBlank();
        boolean hasScope   = req.scope()    != null && !req.scope().isBlank();
        boolean hasDocType = req.docType()  != null && !req.docType().isBlank();
        if (!hasProject && !hasScope && !hasDocType) return null;

        // Build a minimal BM25 query without any filters
        SearchRequest bare = new SearchRequest(req.query(), SearchRequest.SearchType.KEYWORD,
                null, null, 1, "text", null, null, 0);
        Query bareQuery = keywordStrategy.buildQuery(bare);
        if (bareQuery == null) return null;

        long hits = searcher.search(bareQuery, 1).totalHits.value();
        if (hits == 0) return null;

        List<String> activeFilters = new ArrayList<>();
        if (hasProject) activeFilters.add("project=" + req.project());
        if (hasScope)   activeFilters.add("scope=" + req.scope());
        if (hasDocType) activeFilters.add("doc_type=" + req.docType());
        return hits + " result(s) exist without the active filter(s): "
                + String.join(", ", activeFilters);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Suggestions empty() {
        return new Suggestions(List.of(), List.of(), null);
    }

    /** Simple iterative Levenshtein edit distance, capped at MAX_FUZZY_EDITS+1. */
    static int editDistance(String a, String b) {
        if (a.isEmpty()) return b.length();
        if (b.isEmpty()) return a.length();
        int[] prev = new int[b.length() + 1];
        int[] curr = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) prev[j] = j;
        for (int i = 1; i <= a.length(); i++) {
            curr[0] = i;
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                curr[j] = Math.min(Math.min(prev[j] + 1, curr[j - 1] + 1), prev[j - 1] + cost);
            }
            int[] tmp = prev; prev = curr; curr = tmp;
        }
        return prev[b.length()];
    }
}
