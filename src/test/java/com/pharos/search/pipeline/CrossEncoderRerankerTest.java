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

    private static SearchResult named(String id) {
        return result(id, 0f);
    }

    private static CrossEncoder fixed(float... scores) {
        return new CrossEncoder() {
            @Override public float[] score(String q, List<String> passages) { return scores.clone(); }
            @Override public boolean isAvailable() { return true; }
        };
    }

    @Test
    void exactNameCandidateAtFirstStageRankOneSurvivesLowCrossEncoderScore() throws Exception {
        // first-stage order a,b,c,d,e ; CE hates a (0.0001) and loves the rest, in reverse order
        CrossEncoderReranker reranker = new CrossEncoderReranker(fixed(0.0001f, 0.2f, 0.4f, 0.8f, 0.99f));
        List<SearchResult> in = List.of(named("a"), named("b"), named("c"), named("d"), named("e"));
        List<SearchResult> out = reranker.rerank(in, req("q"), null, 10, null);
        assertThat(out.stream().map(SearchResult::id).toList().indexOf("a")).isLessThan(3);
    }

    @Test
    void scoresAreRrfAndMonotonicWithFusedOrder() throws Exception {
        CrossEncoderReranker reranker = new CrossEncoderReranker(fixed(0.1f, 0.9f, 0.5f));
        List<SearchResult> out = reranker.rerank(
                List.of(named("a"), named("b"), named("c")), req("q"), null, 10, null);
        for (int i = 1; i < out.size(); i++) {
            assertThat(out.get(i - 1).score()).isGreaterThanOrEqualTo(out.get(i).score());
        }
        // a: first 1, ce 3 ; b: first 2, ce 1 ; c: first 3, ce 2
        double b = 0.5 / 62 + 0.5 / 61;
        assertThat(out.get(0).id()).isEqualTo("b");
        assertThat((double) out.get(0).score()).isCloseTo(b, org.assertj.core.data.Offset.offset(1e-6));
        assertThat(out.get(0).score()).isLessThan(0.1f); // no longer a raw CE probability
    }

    @Test
    void tiesAreDeterministicAndPreferCrossEncoderThenFirstStage() throws Exception {
        // a(first1,ce2) vs b(first2,ce1): equal RRF; CE rank breaks the tie -> b first
        CrossEncoderReranker reranker = new CrossEncoderReranker(fixed(0.3f, 0.7f));
        List<SearchResult> in = List.of(named("a"), named("b"));
        List<String> first = reranker.rerank(in, req("q"), null, 10, null).stream().map(SearchResult::id).toList();
        List<String> second = reranker.rerank(in, req("q"), null, 10, null).stream().map(SearchResult::id).toList();
        assertThat(first).containsExactly("b", "a").isEqualTo(second);
    }
}
