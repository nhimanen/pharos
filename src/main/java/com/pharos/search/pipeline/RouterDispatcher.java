package com.pharos.search.pipeline;

import com.pharos.search.QueryClassification;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A retrieval stage that dispatches to one of several child {@link SearchPipeline}s
 * based on the {@link QueryClassification} attached to the request by the pipeline's
 * {@link com.pharos.search.QueryRouter}.
 *
 * <p>This is the implementation of the {@code auto} pipeline: the router classifies
 * the query once (at the parent {@link SearchPipeline} level), and the dispatcher
 * forwards execution to the appropriate child pipeline — e.g. keyword for exact
 * identifiers, hybrid for natural-language queries.
 *
 * <p>Child pipelines should NOT have their own router (to avoid double-classification).
 * The classification propagated by the parent is reused by the child.
 *
 * <pre>{@code
 * SearchPipeline auto = SearchPipeline.builder()
 *     .router(new FstQueryClassifier())
 *     .retriever(new RouterDispatcher(
 *         Map.of(SearchType.KEYWORD, keywordPipeline,
 *                SearchType.HYBRID,  hybridPipeline),
 *         SearchType.HYBRID))   // default when no match
 *     .build();
 * }</pre>
 */
public final class RouterDispatcher implements RetrievalStage {

    private final Map<SearchRequest.SearchType, SearchPipeline> children;
    private final SearchRequest.SearchType defaultType;

    public RouterDispatcher(Map<SearchRequest.SearchType, SearchPipeline> children,
                            SearchRequest.SearchType defaultType) {
        this.children    = Map.copyOf(children);
        this.defaultType = defaultType;
    }

    @Override
    public List<SearchResult> retrieve(IndexReader reader, SearchRequest req,
                                       SearchTrace trace) throws IOException {
        QueryClassification c = req.classification();
        SearchRequest.SearchType target = (c != null) ? c.type() : defaultType;
        SearchPipeline child = children.getOrDefault(target, children.get(defaultType));
        if (child == null) child = children.values().iterator().next(); // last resort
        return child.execute(reader, req, trace);
    }
}
