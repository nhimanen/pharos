package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DiversityRerankerTest {

    private static SearchResult result(String id, float score, String docType) {
        return new SearchResult(id, "proj", "pkg", "Cls", "pkg.Cls", "m", "void m()",
                "void", "", "", "public", "/C.java", 1, 5, score, "hybrid", docType);
    }

    private static SearchRequest req() {
        return new SearchRequest("q", SearchRequest.SearchType.HYBRID_DIVERSE, null, null,
                10, "text", null, null, 0);
    }

    @Test
    void emptyInputReturnedUnchanged() throws Exception {
        DiversityReranker reranker = new DiversityReranker(0.5f);
        assertThat(reranker.rerank(List.of(), req(), null, 10, null)).isEmpty();
    }

    @Test
    void singleDocTypeUnaffectedByPenalty() throws Exception {
        // All same type: scores decay but relative order stays the same
        DiversityReranker reranker = new DiversityReranker(0.5f);
        List<SearchResult> input = List.of(
                result("proj:a", 10f, "method"),
                result("proj:b",  8f, "method"),
                result("proj:c",  6f, "method")
        );
        List<SearchResult> out = reranker.rerank(input, req(), null, 10, null);

        assertThat(out).extracting(SearchResult::id)
                .containsExactly("proj:a", "proj:b", "proj:c");
    }

    @Test
    void interleavesMixedDocTypes() throws Exception {
        // method(10), method(9), class(8) → penalty pushes second method below class
        // Expected: method(10), class(8), method(9×0.5=4.5)
        DiversityReranker reranker = new DiversityReranker(0.5f);
        List<SearchResult> input = List.of(
                result("proj:m1", 10f, "method"),
                result("proj:m2",  9f, "method"),
                result("proj:c1",  8f, "class")
        );
        List<SearchResult> out = reranker.rerank(input, req(), null, 10, null);

        assertThat(out).extracting(SearchResult::id)
                .containsExactly("proj:m1", "proj:c1", "proj:m2");
    }

    @Test
    void penaltyOneIsNoOp() throws Exception {
        DiversityReranker reranker = new DiversityReranker(1.0f);
        List<SearchResult> input = List.of(
                result("proj:m1", 10f, "method"),
                result("proj:m2",  9f, "method"),
                result("proj:c1",  8f, "class")
        );
        List<SearchResult> out = reranker.rerank(input, req(), null, 10, null);

        // penalty=1.0 → no score change → original score order preserved
        assertThat(out).extracting(SearchResult::id)
                .containsExactly("proj:m1", "proj:m2", "proj:c1");
    }

    @Test
    void penaltyZeroKeepsOnlyFirstOfEachType() throws Exception {
        DiversityReranker reranker = new DiversityReranker(0.0f);
        List<SearchResult> input = List.of(
                result("proj:m1", 10f, "method"),
                result("proj:c1",  8f, "class"),
                result("proj:m2",  6f, "method"),
                result("proj:c2",  4f, "class")
        );
        List<SearchResult> out = reranker.rerank(input, req(), null, 10, null);

        // Second occurrence of each type gets score 0, so first of each type leads
        assertThat(out.get(0).id()).isEqualTo("proj:m1");
        assertThat(out.get(1).id()).isEqualTo("proj:c1");
        // m2 and c2 both score 0; they appear last
        assertThat(out.subList(2, 4)).extracting(SearchResult::score)
                .allMatch(s -> s == 0f);
    }

    @Test
    void limitApplied() throws Exception {
        DiversityReranker reranker = new DiversityReranker(0.5f);
        List<SearchResult> input = List.of(
                result("proj:a", 3f, "method"),
                result("proj:b", 2f, "class"),
                result("proj:c", 1f, "chunk")
        );
        List<SearchResult> out = reranker.rerank(input, req(), null, 2, null);

        assertThat(out).hasSize(2);
    }

    @Test
    void nullDocTypeTreatedAsUnknown() throws Exception {
        // Should not throw; null docType falls back to "unknown" key
        DiversityReranker reranker = new DiversityReranker(0.5f);
        SearchResult nullType = new SearchResult("proj:x", "proj", "pkg", "Cls", "pkg.Cls",
                "m", "void m()", "void", "", "", "public", "/C.java", 1, 5, 1f, "hybrid", null);
        List<SearchResult> out = reranker.rerank(List.of(nullType), req(), null, 10, null);

        assertThat(out).hasSize(1);
    }

    @Test
    void constructorRejectsOutOfRangePenalty() {
        assertThatThrownBy(() -> new DiversityReranker(-0.1f))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DiversityReranker(1.1f))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void threeTypesFullyInterleaved() throws Exception {
        // method(9), method(8), class(7), class(6), chunk(5), chunk(4)
        // After penalty=0.5:
        //   method: m1=9, m2=8×0.5=4
        //   class:  c1=7, c2=6×0.5=3
        //   chunk:  k1=5, k2=4×0.5=2
        // Expected order: m1(9), c1(7), k1(5), m2(4), c2(3), k2(2)
        DiversityReranker reranker = new DiversityReranker(0.5f);
        List<SearchResult> input = List.of(
                result("proj:m1", 9f, "method"),
                result("proj:m2", 8f, "method"),
                result("proj:c1", 7f, "class"),
                result("proj:c2", 6f, "class"),
                result("proj:k1", 5f, "chunk"),
                result("proj:k2", 4f, "chunk")
        );
        List<SearchResult> out = reranker.rerank(input, req(), null, 10, null);

        assertThat(out).extracting(SearchResult::id)
                .containsExactly("proj:m1", "proj:c1", "proj:k1", "proj:m2", "proj:c2", "proj:k2");
    }
}
