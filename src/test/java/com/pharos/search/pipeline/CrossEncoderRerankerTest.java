package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CrossEncoderRerankerTest {

    private static SearchResult result(String id, float score) {
        return new SearchResult(id, "proj", "pkg", "Cls", "pkg.Cls", "method",
                "void method()", "void", "body", "javadoc", "public",
                "/Cls.java", 1, 10, score, "hybrid", "method");
    }

    private static SearchRequest req(String query) {
        return new SearchRequest(query, SearchRequest.SearchType.HYBRID_RERANKED, null, null,
                10, "text", null, null, 0);
    }

    @Test
    void withNoOpEncoderReturnInputUnchanged() throws Exception {
        CrossEncoderReranker reranker = new CrossEncoderReranker(new NoOpCrossEncoder());
        List<SearchResult> input = List.of(result("proj:a", 1f), result("proj:b", 0.5f));
        List<SearchResult> out = reranker.rerank(input, req("q"), null, 10, null);
        assertThat(out).isSameAs(input);
    }

    @Test
    void emptyInputReturnedUnchanged() throws Exception {
        CrossEncoderReranker reranker = new CrossEncoderReranker(new NoOpCrossEncoder());
        List<SearchResult> out = reranker.rerank(List.of(), req("q"), null, 10, null);
        assertThat(out).isEmpty();
    }

    @Test
    void resortsByEncoderScore() throws Exception {
        // Encoder assigns score = index, so later-in-list items get higher score
        CrossEncoder ascendingEncoder = new CrossEncoder() {
            @Override
            public float[] score(String query, List<String> passages) {
                float[] scores = new float[passages.size()];
                for (int i = 0; i < scores.length; i++) scores[i] = i;
                return scores;
            }

            @Override
            public boolean isAvailable() { return true; }
        };

        CrossEncoderReranker reranker = new CrossEncoderReranker(ascendingEncoder);

        SearchResult a = result("proj:a", 1.0f); // ranks first by BM25, gets encoder score 0
        SearchResult b = result("proj:b", 0.1f); // ranks second by BM25, gets encoder score 1

        List<SearchResult> out = reranker.rerank(List.of(a, b), req("q"), null, 10, null);

        // Reranker should put b first (encoder score 1 > 0)
        assertThat(out.get(0).id()).isEqualTo("proj:b");
        assertThat(out.get(1).id()).isEqualTo("proj:a");
    }

    @Test
    void limitApplied() throws Exception {
        CrossEncoder alwaysAvailable = new CrossEncoder() {
            @Override
            public float[] score(String query, List<String> passages) {
                return new float[passages.size()];
            }
            @Override
            public boolean isAvailable() { return true; }
        };

        CrossEncoderReranker reranker = new CrossEncoderReranker(alwaysAvailable);
        List<SearchResult> input = List.of(
                result("proj:a", 1f), result("proj:b", 0.9f), result("proj:c", 0.8f));
        List<SearchResult> out = reranker.rerank(input, req("q"), null, 2, null);
        assertThat(out).hasSize(2);
    }

    @Test
    void passageBuildingDoesNotThrowOnNullFields() throws Exception {
        SearchResult nullFields = new SearchResult(
                "proj:x", "proj", null, null, null, null, null, null,
                null, null, "public", null, 0, 0, 1f, "hybrid", "method");

        CrossEncoder alwaysAvailable = new CrossEncoder() {
            @Override
            public float[] score(String query, List<String> passages) {
                return new float[passages.size()];
            }
            @Override
            public boolean isAvailable() { return true; }
        };

        CrossEncoderReranker reranker = new CrossEncoderReranker(alwaysAvailable);
        List<SearchResult> out = reranker.rerank(List.of(nullFields), req("q"), null, 10, null);
        assertThat(out).hasSize(1);
    }
}
