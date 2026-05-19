package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
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

    private static SearchRequest req(String query, int limit) {
        return new SearchRequest(query, SearchRequest.SearchType.HYBRID, null, null,
                limit, "text", null, null, 0);
    }

    private final BordaMerger merger = new BordaMerger();

    @Test
    void agreementBonusApplied() {
        SearchResult a = result("proj:a", 1.0f, "keyword");
        SearchResult b = result("proj:b", 0.9f, "keyword");
        SearchResult c = result("proj:c", 0.5f, "vector");

        List<SearchResult> kw  = List.of(a, b);
        List<SearchResult> vec = List.of(a, c);

        List<SearchResult> merged = merger.merge(List.of(kw, vec), req("naturalLanguage", 10), null);
        assertThat(merged.get(0).id()).isEqualTo("proj:a");
    }

    @Test
    void limitRespected() {
        List<SearchResult> kw  = List.of(result("proj:1", 1f, "k"), result("proj:2", 0.9f, "k"),
                result("proj:3", 0.8f, "k"));
        List<SearchResult> vec = List.of(result("proj:4", 1f, "v"), result("proj:5", 0.9f, "v"),
                result("proj:6", 0.8f, "v"));

        List<SearchResult> merged = merger.merge(List.of(kw, vec), req("query", 2), null);
        assertThat(merged).hasSize(2);
    }

    @Test
    void emptyListsHandled() {
        List<SearchResult> kw  = List.of(result("proj:a", 1f, "k"));
        List<SearchResult> vec = List.of();

        List<SearchResult> merged = merger.merge(List.of(kw, vec), req("query", 10), null);
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).id()).isEqualTo("proj:a");
    }

    @Test
    void camelCaseQueryFavoursKeywordWeight() {
        // Falls back to legacy CamelCase detection when no classification — kw 0.8 > vec 0.2
        SearchResult kwTop  = result("proj:a", 1f, "k");
        SearchResult vecTop = result("proj:b", 1f, "v");

        List<SearchResult> merged = merger.merge(
                List.of(List.of(kwTop), List.of(vecTop)), req("BooleanQuery", 10), null);

        assertThat(merged.get(0).id()).isEqualTo("proj:a");
    }

    @Test
    void equalWeightForNLQuery() {
        SearchResult kw  = result("proj:a", 1f, "k");
        SearchResult vec = result("proj:b", 1f, "v");

        List<SearchResult> merged = merger.merge(
                List.of(List.of(kw), List.of(vec)), req("find methods that parse json", 10), null);

        assertThat(merged).hasSize(2);
    }

    @Test
    void searchTypeSetToHybrid() {
        SearchResult a = result("proj:a", 1f, "keyword");
        List<SearchResult> merged = merger.merge(
                List.of(List.of(a), List.of(a)), req("query", 10), null);
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
                List.of(List.of(a), List.of(b), List.of(c)), req("anything", 10), null);

        assertThat(merged).hasSize(3);
    }
}
