package com.pharos.search.pipeline;

import com.pharos.search.QueryClassification;
import com.pharos.search.QueryRouter;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A composable search pipeline:
 * <pre>
 *   [QueryRouter] → retrievers → [pre-merge filters] → [merger] → rerankers
 * </pre>
 *
 * <p>When a {@link QueryRouter} is configured it runs first, classifying the query
 * and producing a {@link QueryClassification} that is attached to the
 * {@link SearchRequest} before any stage sees it.  Stages that are aware of the
 * classification (e.g. {@link UnifiedRetrievalStage}, {@link RouterDispatcher}) can
 * read {@link SearchRequest#classification()}; stages that do not care ignore it.
 *
 * <p>Build with {@link Builder}:
 * <pre>{@code
 * SearchPipeline pipeline = SearchPipeline.builder()
 *     .router(new FstQueryClassifier())          // classify once, propagate to all stages
 *     .retriever(new UnifiedRetrievalStage(...)) // reads classification for adaptive weights
 *     .build();
 *
 * // auto pipeline — router selects keyword vs hybrid child pipeline per query:
 * SearchPipeline auto = SearchPipeline.builder()
 *     .router(new FstQueryClassifier())
 *     .retriever(new RouterDispatcher(Map.of(
 *         SearchType.KEYWORD, keywordPipeline,
 *         SearchType.HYBRID,  hybridPipeline), SearchType.HYBRID))
 *     .build();
 * }</pre>
 */
public final class SearchPipeline {

    private final QueryRouter router;           // optional — null = skip classification
    private final List<RetrievalStage> retrievers;
    private final List<RerankStage> premergeFilters;
    private final MergeStage merger;
    private final List<RerankStage> rerankers;
    private final int oversampleFactor;

    private SearchPipeline(Builder b) {
        this.router          = b.router;
        this.retrievers      = List.copyOf(b.retrievers);
        this.premergeFilters = List.copyOf(b.premergeFilters);
        this.merger          = b.merger;
        this.rerankers       = List.copyOf(b.rerankers);
        this.oversampleFactor = b.oversampleFactor;
    }

    public List<SearchResult> execute(IndexReader reader, SearchRequest req,
                                      SearchTrace trace) throws IOException {
        // ── Step 0: QueryRouter — classify once, propagate to all stages ──────
        if (router != null && req.classification() == null) {
            QueryClassification classification = router.classify(req.query());
            req = req.withClassification(classification);
        }

        // ── Oversample fetch request ──────────────────────────────────────────
        int effectiveFactor = req.oversampleFactor() > 0 ? req.oversampleFactor() : oversampleFactor;
        SearchRequest fetchReq = effectiveFactor > 1
                ? new SearchRequest(req.query(), req.type(), req.project(), req.projects(),
                                    req.limit() * effectiveFactor, req.outputFormat(),
                                    req.docType(), req.scope(), 0, req.classification())
                : req;

        // ── Retrieval ─────────────────────────────────────────────────────────
        List<List<SearchResult>> candidates = new ArrayList<>(retrievers.size());
        for (RetrievalStage r : retrievers) {
            List<SearchResult> list = r.retrieve(reader, fetchReq, trace);
            for (RerankStage pre : premergeFilters) {
                list = pre.rerank(list, fetchReq, reader, list.size(), trace);
            }
            candidates.add(list);
        }

        // ── Merge ─────────────────────────────────────────────────────────────
        List<SearchResult> merged;
        if (retrievers.size() == 1) {
            merged = candidates.get(0);
        } else {
            merged = merger.merge(candidates, req.limit(), req.query(), trace);
        }

        // ── Rerank ────────────────────────────────────────────────────────────
        List<SearchResult> current = merged;
        for (RerankStage rr : rerankers) {
            current = rr.rerank(current, req, reader, req.limit(), trace);
        }
        return current;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private QueryRouter router;
        private final List<RetrievalStage> retrievers = new ArrayList<>();
        private final List<RerankStage> premergeFilters = new ArrayList<>();
        private MergeStage merger;
        private final List<RerankStage> rerankers = new ArrayList<>();
        private int oversampleFactor = 1;

        /** Sets the query router that classifies each query before retrieval. */
        public Builder router(QueryRouter r) {
            this.router = r;
            return this;
        }

        public Builder retriever(RetrievalStage r) {
            retrievers.add(r);
            return this;
        }

        /** Applied to each retriever's result list independently, before the merge step. */
        public Builder premerge(RerankStage r) {
            premergeFilters.add(r);
            return this;
        }

        public Builder merger(MergeStage m) {
            this.merger = m;
            return this;
        }

        public Builder reranker(RerankStage r) {
            rerankers.add(r);
            return this;
        }

        /**
         * Multiply each retriever's fetch limit by this factor before retrieval.
         * Factor of 1 (default) disables oversampling.
         */
        public Builder oversample(int factor) {
            if (factor < 1) throw new IllegalArgumentException("oversampleFactor must be >= 1");
            this.oversampleFactor = factor;
            return this;
        }

        public SearchPipeline build() {
            if (retrievers.isEmpty()) throw new IllegalStateException("At least one retriever required");
            if (retrievers.size() > 1 && merger == null)
                throw new IllegalStateException("Multi-retriever pipeline requires a merger");
            return new SearchPipeline(this);
        }
    }
}
