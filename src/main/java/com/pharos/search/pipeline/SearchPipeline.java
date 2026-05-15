package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A composable search pipeline: one or more retrievers → optional merger → zero or more rerankers.
 *
 * <p>Build with {@link Builder}:
 * <pre>{@code
 * SearchPipeline pipeline = SearchPipeline.builder()
 *     .retriever(keywordStage)
 *     .retriever(vectorStage)
 *     .merger(new BordaMerger())
 *     .reranker(new CrossEncoderReranker(encoder))
 *     .build();
 * }</pre>
 */
public final class SearchPipeline {

    private final List<RetrievalStage> retrievers;
    private final MergeStage merger;
    private final List<RerankStage> rerankers;

    private SearchPipeline(Builder b) {
        this.retrievers = List.copyOf(b.retrievers);
        this.merger = b.merger;
        this.rerankers = List.copyOf(b.rerankers);
    }

    public List<SearchResult> execute(IndexReader reader, SearchRequest req,
                                      SearchTrace trace) throws IOException {
        List<List<SearchResult>> candidates = new ArrayList<>(retrievers.size());
        for (RetrievalStage r : retrievers) {
            candidates.add(r.retrieve(reader, req, trace));
        }

        List<SearchResult> merged;
        if (retrievers.size() == 1) {
            merged = candidates.get(0);
        } else {
            merged = merger.merge(candidates, req.limit(), req.query(), trace);
        }

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
        private final List<RetrievalStage> retrievers = new ArrayList<>();
        private MergeStage merger;
        private final List<RerankStage> rerankers = new ArrayList<>();

        public Builder retriever(RetrievalStage r) {
            retrievers.add(r);
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

        public SearchPipeline build() {
            if (retrievers.isEmpty()) throw new IllegalStateException("At least one retriever required");
            if (retrievers.size() > 1 && merger == null)
                throw new IllegalStateException("Multi-retriever pipeline requires a merger");
            return new SearchPipeline(this);
        }
    }
}
