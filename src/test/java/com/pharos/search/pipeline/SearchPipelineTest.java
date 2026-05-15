package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SearchPipelineTest {

    private static SearchResult result(String id, float score) {
        return new SearchResult(id, "proj", "pkg", "Cls", "pkg.Cls", "m", "void m()",
                "void", "", "", "public", "/C.java", 1, 5, score, "keyword", "method");
    }

    private static SearchRequest req(String query) {
        return new SearchRequest(query, SearchRequest.SearchType.KEYWORD, null, null,
                10, "text", null, null);
    }

    @Test
    void singleRetrieverNoReranker() throws Exception {
        SearchResult r = result("proj:a", 1.0f);
        RetrievalStage retriever = (reader, req, trace) -> List.of(r);

        SearchPipeline pipeline = SearchPipeline.builder().retriever(retriever).build();
        List<SearchResult> results = pipeline.execute(null, req("query"), null);

        assertThat(results).containsExactly(r);
    }

    @Test
    void twoRetrieversWithMerger() throws Exception {
        SearchResult a = result("proj:a", 1f);
        SearchResult b = result("proj:b", 0.5f);
        RetrievalStage r1 = (reader, req, trace) -> List.of(a);
        RetrievalStage r2 = (reader, req, trace) -> List.of(b);
        MergeStage merger = (lists, limit, query, trace) -> List.of(a, b);

        SearchPipeline pipeline = SearchPipeline.builder()
                .retriever(r1).retriever(r2).merger(merger).build();
        List<SearchResult> results = pipeline.execute(null, req("query"), null);

        assertThat(results).containsExactly(a, b);
    }

    @Test
    void rerankersAppliedInOrder() throws Exception {
        SearchResult original = result("proj:a", 1f);
        SearchResult afterFirst  = result("proj:a", 2f);
        SearchResult afterSecond = result("proj:a", 3f);

        RetrievalStage retriever = (reader, req, trace) -> List.of(original);
        RerankStage first  = (results, req, reader, limit, trace) -> List.of(afterFirst);
        RerankStage second = (results, req, reader, limit, trace) -> {
            assertThat(results).containsExactly(afterFirst);
            return List.of(afterSecond);
        };

        SearchPipeline pipeline = SearchPipeline.builder()
                .retriever(retriever).reranker(first).reranker(second).build();
        List<SearchResult> results = pipeline.execute(null, req("q"), null);

        assertThat(results).containsExactly(afterSecond);
    }

    @Test
    void traceReceivesSpans() throws Exception {
        RetrievalStage retriever = (reader, req, trace) -> {
            if (trace != null) trace.record("test-span", System.currentTimeMillis());
            return List.of();
        };
        SearchPipeline pipeline = SearchPipeline.builder().retriever(retriever).build();
        SearchTrace trace = new SearchTrace();
        pipeline.execute(null, req("q"), trace);

        assertThat(trace.spans()).hasSize(1);
        assertThat(trace.spans().get(0).name()).isEqualTo("test-span");
    }

    @Test
    void builderRejectsEmptyRetrievers() {
        assertThatThrownBy(() -> SearchPipeline.builder().build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void builderRejectsMultiRetrieverWithoutMerger() {
        RetrievalStage r = (reader, req, trace) -> List.of();
        assertThatThrownBy(() -> SearchPipeline.builder().retriever(r).retriever(r).build())
                .isInstanceOf(IllegalStateException.class);
    }
}
