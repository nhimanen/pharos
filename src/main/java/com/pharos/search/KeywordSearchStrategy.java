package com.pharos.search;

import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * BM25 keyword search using MultiFieldQueryParser with field-level boosting.
 *
 * Field boost hierarchy:
 * - methodName: 3x  (direct method name match is most relevant)
 * - javadoc: 2x     (documentation describes intent)
 * - signature: 2x   (type information is highly relevant)
 * - className: 1.5x (class context)
 * - body: 1x        (implementation details)
 */
public class KeywordSearchStrategy {

    private static final Logger log = LoggerFactory.getLogger(KeywordSearchStrategy.class);

    /**
     * Weight applied to the graph-derived boost on top of BM25 score.
     * Final score = bm25 * (1 + GRAPH_BOOST_WEIGHT * log(1+inDeg)/log(1+maxInDeg))
     * At 0.3, a method called by 100 others (log boost ≈ 1.0) gets ~30% score lift
     * over a method never called. Keeps relevance dominant while surfacing hub methods.
     */
    private static final float GRAPH_BOOST_WEIGHT = 0.3f;

    private static final String[] SEARCH_FIELDS = {
            DocumentMapper.F_METHOD_NAME,
            DocumentMapper.F_JAVADOC,
            DocumentMapper.F_SIGNATURE,
            DocumentMapper.F_CLASS_NAME,
            DocumentMapper.F_BODY,
            DocumentMapper.F_ANNOTATIONS,
            DocumentMapper.F_CALLER_CONTEXT
    };

    private static final Map<String, Float> FIELD_BOOSTS = new HashMap<>();
    static {
        FIELD_BOOSTS.put(DocumentMapper.F_METHOD_NAME, 3.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_JAVADOC, 2.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_SIGNATURE, 2.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_CLASS_NAME, 1.5f);
        FIELD_BOOSTS.put(DocumentMapper.F_BODY, 1.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_ANNOTATIONS, 1.0f);
        FIELD_BOOSTS.put(DocumentMapper.F_CALLER_CONTEXT, 1.5f);
    }

    private final Analyzer analyzer;

    public KeywordSearchStrategy() {
        this.analyzer = LuceneIndexer.buildAnalyzer();
    }

    public List<SearchResult> search(IndexReader reader, SearchRequest req) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new org.apache.lucene.search.similarities.BM25Similarity());

        Query query;
        try {
            MultiFieldQueryParser parser = new MultiFieldQueryParser(SEARCH_FIELDS, analyzer, FIELD_BOOSTS);
            parser.setDefaultOperator(org.apache.lucene.queryparser.classic.QueryParser.Operator.OR);
            // Escape special chars that might cause parse errors
            String escaped = MultiFieldQueryParser.escape(req.query());
            query = parser.parse(escaped);
        } catch (ParseException e) {
            log.warn("Query parse error: {}", e.getMessage());
            return List.of();
        }

        // Optional project + docType filters
        BooleanQuery.Builder filtered = new BooleanQuery.Builder().add(query, BooleanClause.Occur.MUST);
        boolean hasFilter = false;
        if (req.project() != null && !req.project().isEmpty()) {
            filtered.add(new TermQuery(new Term(DocumentMapper.F_PROJECT, req.project())),
                    BooleanClause.Occur.FILTER);
            hasFilter = true;
        }
        if (req.docType() != null && !req.docType().isEmpty()) {
            filtered.add(new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, req.docType())),
                    BooleanClause.Occur.FILTER);
            hasFilter = true;
        }
        if (hasFilter) query = filtered.build();

        TopDocs hits = searcher.search(query, req.limit());
        return toResults(searcher, hits, "keyword");
    }

    static List<SearchResult> toResults(IndexSearcher searcher, TopDocs hits, String type) throws IOException {
        if (hits.scoreDocs.length == 0) return List.of();

        StoredFields storedFields = searcher.storedFields();

        // First pass: load all docs + find max in-degree for normalization
        record RawHit(Document doc, float rawScore) {}
        List<RawHit> rawHits = new ArrayList<>(hits.scoreDocs.length);
        int maxInDegree = 1; // avoid division by zero
        for (ScoreDoc sd : hits.scoreDocs) {
            Document doc = storedFields.document(sd.doc);
            int inDeg = getInt(doc, DocumentMapper.F_IN_DEGREE);
            if (inDeg > maxInDegree) maxInDegree = inDeg;
            rawHits.add(new RawHit(doc, sd.score));
        }

        // Second pass: apply log-normalized in-degree boost and re-sort
        final int maxDeg = maxInDegree;
        return rawHits.stream()
                .map(hit -> {
                    int inDeg = getInt(hit.doc(), DocumentMapper.F_IN_DEGREE);
                    float normBoost = (float) (Math.log1p(inDeg) / Math.log1p(maxDeg));
                    float boostedScore = hit.rawScore() * (1.0f + GRAPH_BOOST_WEIGHT * normBoost);
                    return docToResult(hit.doc(), boostedScore, type);
                })
                .sorted(Comparator.comparingDouble(SearchResult::score).reversed())
                .collect(Collectors.toList());
    }

    static SearchResult docToResult(Document doc, float score, String searchType) {
        String docType = doc.get(DocumentMapper.F_DOC_TYPE);
        if (docType == null) docType = "method"; // backwards compat with pre-docType indexes
        return new SearchResult(
                doc.get(DocumentMapper.F_ID),
                doc.get(DocumentMapper.F_PROJECT),
                doc.get(DocumentMapper.F_PACKAGE),
                doc.get(DocumentMapper.F_CLASS_NAME),
                doc.get(DocumentMapper.F_QUALIFIED_CLASS),
                doc.get(DocumentMapper.F_METHOD_NAME),
                doc.get(DocumentMapper.F_SIGNATURE),
                doc.get(DocumentMapper.F_RETURN_TYPE),
                doc.get(DocumentMapper.F_BODY),
                doc.get(DocumentMapper.F_JAVADOC),
                doc.get(DocumentMapper.F_ACCESS),
                doc.get(DocumentMapper.F_FILE_PATH),
                getInt(doc, DocumentMapper.F_START_LINE),
                getInt(doc, DocumentMapper.F_END_LINE),
                score,
                searchType,
                docType
        );
    }

    private static int getInt(Document doc, String field) {
        String val = doc.get(field);
        if (val == null) return 0;
        try { return Integer.parseInt(val); } catch (NumberFormatException e) { return 0; }
    }
}
