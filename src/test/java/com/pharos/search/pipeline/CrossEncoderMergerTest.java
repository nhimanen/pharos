package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CrossEncoderMergerTest {

    private static SearchRequest req(String query, int limit) {
        return new SearchRequest(query, SearchRequest.SearchType.HYBRID, null, null,
                limit, "text", null, null, 0);
    }

    private static SearchResult result(String id, float score, String searchType) {
        return new SearchResult(id, "proj", "pkg", "Cls", "pkg.Cls", "method",
                "void method()", "void", "body", "javadoc", "public",
                "/Cls.java", 1, 10, score, searchType, "method");
    }

    @Test
    void withNoOpEncoderReturnsDeduplicatedPool() {
        CrossEncoderMerger merger = new CrossEncoderMerger(new NoOpCrossEncoder());

        SearchResult a = result("proj:a", 1f, "keyword");
        SearchResult b = result("proj:b", 0.5f, "keyword");
        SearchResult c = result("proj:c", 0.8f, "vector");

        List<SearchResult> merged = merger.merge(
                List.of(List.of(a, b), List.of(c)), req("query", 10), null);

        assertThat(merged).hasSize(3);
        assertThat(merged.stream().map(SearchResult::id).toList())
                .containsExactlyInAnyOrder("proj:a", "proj:b", "proj:c");
    }

    @Test
    void deduplicatesById() {
        CrossEncoderMerger merger = new CrossEncoderMerger(new NoOpCrossEncoder());

        SearchResult a1 = result("proj:a", 1f, "keyword");
        SearchResult a2 = result("proj:a", 0.9f, "vector");  // same id, different score

        List<SearchResult> merged = merger.merge(
                List.of(List.of(a1), List.of(a2)), req("q", 10), null);

        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).searchType()).isEqualTo("keyword"); // first-occurrence wins
    }

    @Test
    void limitRespected() {
        CrossEncoderMerger merger = new CrossEncoderMerger(new NoOpCrossEncoder());

        List<SearchResult> kw = List.of(
                result("proj:a", 1f, "k"), result("proj:b", 0.9f, "k"),
                result("proj:c", 0.8f, "k"));
        List<SearchResult> vec = List.of(result("proj:d", 0.7f, "v"));

        List<SearchResult> merged = merger.merge(List.of(kw, vec), req("q", 2), null);
        assertThat(merged).hasSize(2);
    }

    @Test
    void withRealEncoderSortsByScore() {
        // Deterministic stub encoder: always returns score = index as float (0,1,2...)
        CrossEncoder stubbedEncoder = new CrossEncoder() {
            @Override
            public float[] score(String query, List<String> passages) {
                float[] scores = new float[passages.size()];
                // Give last passage the highest score to verify re-sort
                for (int i = 0; i < scores.length; i++) scores[i] = i;
                return scores;
            }

            @Override
            public boolean isAvailable() { return true; }
        };

        CrossEncoderMerger merger = new CrossEncoderMerger(stubbedEncoder);

        SearchResult a = result("proj:a", 1f, "k"); // will get score 0
        SearchResult b = result("proj:b", 0.5f, "k"); // will get score 1

        List<SearchResult> merged = merger.merge(List.of(List.of(a, b), List.of()), req("q", 10), null);

        // b (score=1) should beat a (score=0)
        assertThat(merged.get(0).id()).isEqualTo("proj:b");
        assertThat(merged.get(0).searchType()).isEqualTo("cross-encoder");
    }

    @Test
    void emptyInputReturnsEmpty() {
        CrossEncoderMerger merger = new CrossEncoderMerger(new NoOpCrossEncoder());
        List<SearchResult> merged = merger.merge(List.of(List.of(), List.of()), req("q", 10), null);
        assertThat(merged).isEmpty();
    }
}
