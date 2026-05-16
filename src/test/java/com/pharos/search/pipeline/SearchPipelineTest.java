package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SearchPipelineTest {

    private static SearchResult result(String id, float score) {
        return result(id, score, "method");
    }

    private static SearchResult result(String id, float score, String docType) {
        return new SearchResult(id, "proj", "pkg", "Cls", "pkg.Cls", "m", "void m()",
                "void", "", "", "public", "/C.java", 1, 5, score, "keyword", docType);
    }

    private static SearchRequest req(String query) {
        return new SearchRequest(query, SearchRequest.SearchType.KEYWORD, null, null,
                10, "text", null, null, 0);
    }

    private static SearchRequest reqWithOversample(String query, int factor) {
        return new SearchRequest(query, SearchRequest.SearchType.KEYWORD, null, null,
                10, "text", null, null, factor);
    }

    // ── existing pipeline tests ───────────────────────────────────────────────

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
        SearchResult original   = result("proj:a", 1f);
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

    // ── premerge ─────────────────────────────────────────────────────────────

    @Test
    void premergeAppliedToEachRetrieverListIndependently() throws Exception {
        SearchResult a = result("proj:a", 1f);
        SearchResult b = result("proj:b", 1f);
        List<List<SearchResult>> seenInputs = new ArrayList<>();

        RetrievalStage r1 = (reader, req, trace) -> List.of(a);
        RetrievalStage r2 = (reader, req, trace) -> List.of(b);
        MergeStage merger = (lists, limit, query, trace) -> {
            lists.forEach(l -> seenInputs.add(new ArrayList<>(l)));
            return List.of(a, b);
        };
        RerankStage premerge = (results, req, reader, limit, trace) -> {
            // Mark each result to prove premerge ran
            return results.stream()
                    .map(r -> new SearchResult(r.id() + "-pre", r.project(), r.packageName(),
                            r.className(), r.qualifiedClassName(), r.methodName(), r.signature(),
                            r.returnType(), r.body(), r.javadoc(), r.accessModifier(),
                            r.filePath(), r.startLine(), r.endLine(), r.score(), r.searchType(), r.docType()))
                    .toList();
        };

        SearchPipeline pipeline = SearchPipeline.builder()
                .retriever(r1).retriever(r2).premerge(premerge).merger(merger).build();
        pipeline.execute(null, req("q"), null);

        // Both retriever lists were transformed before reaching the merger
        assertThat(seenInputs).hasSize(2);
        assertThat(seenInputs.get(0).get(0).id()).isEqualTo("proj:a-pre");
        assertThat(seenInputs.get(1).get(0).id()).isEqualTo("proj:b-pre");
    }

    @Test
    void premergeReceivesFullListNotTruncated() throws Exception {
        List<SearchResult> bigList = new ArrayList<>();
        for (int i = 0; i < 30; i++) bigList.add(result("proj:" + i, i));

        List<Integer> preMergeSizes = new ArrayList<>();
        RetrievalStage retriever = (reader, req, trace) -> bigList;
        RerankStage premerge = (results, req, reader, limit, trace) -> {
            preMergeSizes.add(results.size());
            return results;
        };

        SearchPipeline pipeline = SearchPipeline.builder()
                .retriever(retriever).premerge(premerge).build();
        pipeline.execute(null, req("q"), null);  // req has limit=10

        // premerge should see all 30, not capped at req.limit()=10
        assertThat(preMergeSizes.get(0)).isEqualTo(30);
    }

    // ── oversampling ──────────────────────────────────────────────────────────

    @Test
    void oversampleFactorMultipliesRetrieverFetchLimit() throws Exception {
        List<Integer> seenLimits = new ArrayList<>();
        RetrievalStage retriever = (reader, req, trace) -> {
            seenLimits.add(req.limit());
            return List.of();
        };

        SearchPipeline pipeline = SearchPipeline.builder()
                .retriever(retriever).oversample(3).build();
        pipeline.execute(null, req("q"), null);  // req.limit() = 10

        assertThat(seenLimits).containsExactly(30);
    }

    @Test
    void oversampleFactorFromRequestOverridesPipelineDefault() throws Exception {
        List<Integer> seenLimits = new ArrayList<>();
        RetrievalStage retriever = (reader, req, trace) -> {
            seenLimits.add(req.limit());
            return List.of();
        };

        SearchPipeline pipeline = SearchPipeline.builder()
                .retriever(retriever).oversample(2).build();  // pipeline says 2×
        pipeline.execute(null, reqWithOversample("q", 5), null);  // request says 5×

        assertThat(seenLimits).containsExactly(50);  // 10 × 5 wins
    }

    @Test
    void oversampleOneIsNoOp() throws Exception {
        List<Integer> seenLimits = new ArrayList<>();
        RetrievalStage retriever = (reader, req, trace) -> {
            seenLimits.add(req.limit());
            return List.of();
        };

        SearchPipeline pipeline = SearchPipeline.builder()
                .retriever(retriever).oversample(1).build();
        pipeline.execute(null, req("q"), null);

        assertThat(seenLimits).containsExactly(10);  // unchanged
    }

    @Test
    void oversampleBuilderRejectsZeroOrNegative() {
        assertThatThrownBy(() -> SearchPipeline.builder()
                .retriever((r, req, t) -> List.of()).oversample(0).build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
