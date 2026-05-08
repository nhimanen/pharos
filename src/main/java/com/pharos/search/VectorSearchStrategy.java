package com.pharos.search;

import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.DocumentMapper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Semantic vector search using Lucene's built-in HNSW KNN index.
 * Requires vector embeddings to have been generated at index time.
 */
public class VectorSearchStrategy {

    private static final Logger log = LoggerFactory.getLogger(VectorSearchStrategy.class);

    private final EmbeddingProvider embedder;

    public VectorSearchStrategy(EmbeddingProvider embedder) {
        this.embedder = embedder;
    }

    public List<SearchResult> search(IndexReader reader, SearchRequest req) throws IOException {
        if (!embedder.isAvailable()) {
            log.warn("Vector search requested but no embedding provider configured. " +
                    "Run with --no-embed=false and set embeddingModelUrl in config.");
            return List.of();
        }

        float[] queryVector = embedder.embed(req.query());
        if (queryVector == null) {
            log.warn("Embedding returned null for query: {}", req.query());
            return List.of();
        }

        // Build combined filter for project + docType (KnnFloatVectorQuery accepts a single filter query)
        Query vectorQuery;
        Query filter = buildFilter(req);
        if (filter != null) {
            vectorQuery = new KnnFloatVectorQuery(DocumentMapper.F_VECTOR, queryVector, req.limit(), filter);
        } else {
            vectorQuery = new KnnFloatVectorQuery(DocumentMapper.F_VECTOR, queryVector, req.limit());
        }

        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(vectorQuery, req.limit());
        return KeywordSearchStrategy.toResults(searcher, hits, "vector");
    }

    private static Query buildFilter(SearchRequest req) {
        boolean hasProject = req.project() != null && !req.project().isEmpty();
        boolean hasDocType = req.docType() != null && !req.docType().isEmpty();
        if (!hasProject && !hasDocType) return null;
        if (hasProject && !hasDocType) {
            return new TermQuery(new Term(DocumentMapper.F_PROJECT, req.project()));
        }
        if (!hasProject) {
            return new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, req.docType()));
        }
        return new BooleanQuery.Builder()
                .add(new TermQuery(new Term(DocumentMapper.F_PROJECT, req.project())), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term(DocumentMapper.F_DOC_TYPE, req.docType())), BooleanClause.Occur.FILTER)
                .build();
    }
}
