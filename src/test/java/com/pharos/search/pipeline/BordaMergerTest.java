package com.pharos.search.pipeline;

import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BordaMergerTest {

    private static SearchResult result(String id, float score, String searchType) {
        return new SearchResult(id, "proj", "pkg", "Cls", "pkg.Cls", "method",
                "void method()", "void", "body", "javadoc", "public",
                "/Cls.java", 1, 10, score, searchType, "method");
    }

    private final BordaMerger merger = new BordaMerger();

    @Test
    void agreementBonusApplied() {
        SearchResult a = result("proj:a", 1.0f, "keyword");
        SearchResult b = result("proj:b", 0.9f, "keyword");
        SearchResult c = result("proj:c", 0.5f, "vector");

        List<SearchResult> kw  = List.of(a, b);
        List<SearchResult> vec = List.of(a, c);

        List<SearchResult> merged = merger.merge(List.of(kw, vec), 10, "naturalLanguage", null);

        // "a" appears in both — should rank first with agreement bonus
        assertThat(merged.get(0).id()).isEqualTo("proj:a");
    }

    @Test
    void limitRespected() {
        List<SearchResult> kw  = List.of(result("proj:1", 1f, "k"), result("proj:2", 0.9f, "k"),
                result("proj:3", 0.8f, "k"));
        List<SearchResult> vec = List.of(result("proj:4", 1f, "v"), result("proj:5", 0.9f, "v"),
                result("proj:6", 0.8f, "v"));

        List<SearchResult> merged = merger.merge(List.of(kw, vec), 2, "query", null);
        assertThat(merged).hasSize(2);
    }

    @Test
    void emptyListsHandled() {
        List<SearchResult> kw  = List.of(result("proj:a", 1f, "k"));
        List<SearchResult> vec = List.of();

        List<SearchResult> merged = merger.merge(List.of(kw, vec), 10, "query", null);
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).id()).isEqualTo("proj:a");
    }

    @Test
    void camelCaseQueryFavoursKeywordWeight() {
        // CamelCase → keyword weight 0.8, vector 0.2
        // If keyword has doc "a" at rank 1 and vector has "b" at rank 1 (no overlap),
        // "a" should rank higher than "b" because kw weight > vec weight.
        SearchResult kwTop  = result("proj:a", 1f, "k");
        SearchResult vecTop = result("proj:b", 1f, "v");

        List<SearchResult> merged = merger.merge(
                List.of(List.of(kwTop), List.of(vecTop)), 10, "BooleanQuery", null);

        assertThat(merged.get(0).id()).isEqualTo("proj:a");
    }

    @Test
    void equalWeightForNLQuery() {
        // NL → kw=0.5, vec=0.5; single-element lists → equal Borda points, order is insertion-stable
        SearchResult kw  = result("proj:a", 1f, "k");
        SearchResult vec = result("proj:b", 1f, "v");

        List<SearchResult> merged = merger.merge(
                List.of(List.of(kw), List.of(vec)), 10, "find methods that parse json", null);

        assertThat(merged).hasSize(2);
    }

    @Test
    void searchTypeSetToHybrid() {
        SearchResult a = result("proj:a", 1f, "keyword");
        List<SearchResult> merged = merger.merge(
                List.of(List.of(a), List.of(a)), 10, "query", null);
        assertThat(merged.get(0).searchType()).isEqualTo("hybrid");
    }

    @Test
    void containsCamelCaseDetection() {
        assertThat(BordaMerger.containsCamelCase("BooleanQuery")).isTrue();
        assertThat(BordaMerger.containsCamelCase("HnswGraphBuilder")).isTrue();
        assertThat(BordaMerger.containsCamelCase("HNSW")).isTrue();
        assertThat(BordaMerger.containsCamelCase("find methods that parse")).isFalse();
        assertThat(BordaMerger.containsCamelCase(null)).isFalse();
        assertThat(BordaMerger.containsCamelCase("")).isFalse();
    }

    @Test
    void equalWeightFuseForNLists() {
        SearchResult a = result("proj:a", 1f, "s1");
        SearchResult b = result("proj:b", 1f, "s2");
        SearchResult c = result("proj:c", 1f, "s3");

        List<SearchResult> merged = merger.merge(
                List.of(List.of(a), List.of(b), List.of(c)), 10, "anything", null);

        assertThat(merged).hasSize(3);
    }
}
