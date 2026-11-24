package com.pharos.search.distributed;

import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CandidateShaperTest {

    private static RawCandidate cand(String id, float raw, int inDegree, String filePath, String docType,
                                     String classType, String access) {
        SearchResult r = new SearchResult(id, "p", "pkg", "C", "pkg.C", "m", "m()", "void", "body", "doc",
                access, filePath, 3, 9, raw, "raw", docType == null ? "method" : docType);
        return new RawCandidate(raw, inDegree, filePath, docType, classType, access, r);
    }

    /** One candidate with in-degree 0 (graph factor exactly 1): its shaped score. */
    private static float single(String filePath, String docType, String classType, String access, String intent) {
        List<SearchResult> out = CandidateShaper.shape(
                List.of(cand("x", 10f, 0, filePath, docType, classType, access)), "keyword", intent);
        return out.get(0).score();
    }

    private static final String PROD = "/repo/src/main/java/A.java";

    @Test
    void emptyInputGivesEmptyOutput() {
        assertThat(CandidateShaper.shape(List.of(), "keyword", null)).isEmpty();
    }

    @Test
    void sourcePathPenaltyEveryPath() {
        Map<String, Float> expected = Map.ofEntries(
                Map.entry(PROD, 1.0f),
                Map.entry("/repo/src/test/java/ATest.java", 0.30f),
                Map.entry("/repo/module/test/A.java", 0.30f),
                Map.entry("/repo/benchmark/A.java", 0.25f),
                Map.entry("/repo/jmh/A.java", 0.25f),
                Map.entry("/repo/.github/workflows/ci.json", 0.10f),
                Map.entry("/repo/README.md", 0.10f),
                Map.entry("/repo/notes.txt", 0.10f),
                Map.entry("/repo/run.sh", 0.10f),
                Map.entry("/repo/app.yml", 0.10f),
                Map.entry("C:\\repo\\src\\test\\ATest.java", 0.30f));
        expected.forEach((path, penalty) -> {
            assertThat(CandidateShaper.sourcePathPenalty(path)).as(path).isEqualTo(penalty);
            // method doc, public, no intent: penalty is the only multiplier
            assertThat(single(path, "method", null, "public", null)).as(path).isEqualTo(10f * 1.0f * penalty);
        });
        assertThat(CandidateShaper.sourcePathPenalty(null)).isEqualTo(1.0f);
        assertThat(single(null, "method", null, "public", null)).isEqualTo(10f);
    }

    @Test
    void docTypeBonusExcludesTestClasses() {
        assertThat(CandidateShaper.docTypeBonus("class", PROD)).isEqualTo(1.8f);
        assertThat(CandidateShaper.docTypeBonus("class", null)).isEqualTo(1.8f);
        assertThat(CandidateShaper.docTypeBonus("class", "/repo/src/test/java/ATest.java")).isEqualTo(1.0f);
        assertThat(CandidateShaper.docTypeBonus("class", "/repo/module/test/A.java")).isEqualTo(1.0f);
        assertThat(CandidateShaper.docTypeBonus("method", PROD)).isEqualTo(1.0f);
        assertThat(CandidateShaper.docTypeBonus("chunk", PROD)).isEqualTo(1.0f);
        assertThat(CandidateShaper.docTypeBonus(null, PROD)).isEqualTo(1.0f);

        assertThat(single(PROD, "class", null, "public", null)).isEqualTo(10f * 1.8f);
        // a test class: path penalty, but no class bonus
        assertThat(single("/repo/src/test/java/ATest.java", "class", null, "public", null))
                .isEqualTo(10f * 1.0f * 0.30f * 1.0f);
    }

    @Test
    void classTypeBonusEveryIntentAndClassType() {
        String[] classTypes = {"interface", "abstract", "class", "enum", "record", "annotation", "other"};
        Map<String, float[]> table = Map.of(
                //                 interface abstract class enum record annotation other
                "INTERFACE",      new float[]{3.0f, 1.8f, 0.7f, 1.0f, 1.0f, 1.0f, 1.0f},
                "ABSTRACT",       new float[]{1.8f, 3.0f, 0.7f, 1.0f, 1.0f, 1.0f, 1.0f},
                "ENUM",           new float[]{0.6f, 0.6f, 0.8f, 3.0f, 0.6f, 0.6f, 0.6f},
                "RECORD",         new float[]{0.6f, 0.6f, 0.8f, 0.6f, 3.0f, 0.6f, 0.6f},
                "ANNOTATION",     new float[]{0.9f, 0.6f, 0.7f, 0.6f, 0.6f, 3.0f, 0.6f},
                "IMPLEMENTATION", new float[]{0.4f, 0.7f, 2.5f, 1.5f, 1.5f, 1.0f, 1.0f},
                "BEHAVIORAL",     new float[]{1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f},
                "default",        new float[]{1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f});
        table.forEach((intent, row) -> {
            for (int i = 0; i < classTypes.length; i++) {
                String ct = classTypes[i];
                assertThat(CandidateShaper.classTypeBonus(ct, intent)).as(intent + "×" + ct).isEqualTo(row[i]);
                assertThat(single(PROD, "method", ct, "public", intent)).as(intent + "×" + ct)
                        .isEqualTo(10f * 1.0f * 1.0f * 1.0f * row[i]);
            }
            assertThat(CandidateShaper.classTypeBonus(null, intent)).as(intent + "×null").isEqualTo(1.0f);
        });
        for (String ct : classTypes) {
            assertThat(CandidateShaper.classTypeBonus(ct, null)).as("null×" + ct).isEqualTo(1.0f);
        }
    }

    @Test
    void accessMultiplierAllLevels() {
        assertThat(CandidateShaper.accessMultiplier("public")).isEqualTo(1.00f);
        assertThat(CandidateShaper.accessMultiplier("PUBLIC")).isEqualTo(1.00f);
        assertThat(CandidateShaper.accessMultiplier("protected")).isEqualTo(0.85f);
        assertThat(CandidateShaper.accessMultiplier("private")).isEqualTo(0.50f);
        assertThat(CandidateShaper.accessMultiplier("package-private")).isEqualTo(0.75f);
        assertThat(CandidateShaper.accessMultiplier(null)).isEqualTo(0.75f);
        assertThat(CandidateShaper.accessMultiplier(" ")).isEqualTo(0.75f);

        assertThat(single(PROD, "method", null, "protected", null)).isEqualTo(10f * 0.85f);
        assertThat(single(PROD, "method", null, "private", null)).isEqualTo(10f * 0.50f);
        assertThat(single(PROD, "method", null, null, null)).isEqualTo(10f * 0.75f);
    }

    @Test
    void inDegreeIsNormalisedByTheMaxOverTheGivenList() {
        // max 0: floored at 1, every graph factor is 1 + 0.3 * log1p(0) / log1p(1) = 1
        List<SearchResult> zero = CandidateShaper.shape(List.of(
                cand("a", 4f, 0, PROD, "method", null, "public"),
                cand("b", 2f, 0, PROD, "method", null, "public")), "keyword", null);
        assertThat(zero).extracting(SearchResult::score).containsExactly(4f, 2f);

        // max 1: the in-degree-1 doc gets the full 1.3
        List<SearchResult> one = CandidateShaper.shape(List.of(
                cand("a", 4f, 0, PROD, "method", null, "public"),
                cand("b", 4f, 1, PROD, "method", null, "public")), "keyword", null);
        assertThat(one).extracting(SearchResult::id).containsExactly("b", "a");
        assertThat(one).extracting(SearchResult::score).containsExactly(4f * (1.0f + 0.3f * 1.0f), 4f);

        // max > 1: normalised by log1p(max)
        List<SearchResult> many = CandidateShaper.shape(List.of(
                cand("a", 1f, 1, PROD, "method", null, "public"),
                cand("b", 1f, 9, PROD, "method", null, "public")), "keyword", null);
        float normA = (float) (Math.log1p(1) / Math.log1p(9));
        assertThat(many).extracting(SearchResult::id).containsExactly("b", "a");
        assertThat(many).extracting(SearchResult::score)
                .containsExactly(1f * (1.0f + 0.3f * 1.0f), 1f * (1.0f + 0.3f * normA));

        // the same candidate shaped in a list with a bigger hub boosts less: the max is the list's
        float alone = CandidateShaper.shape(List.of(cand("a", 1f, 1, PROD, "method", null, "public")),
                "keyword", null).get(0).score();
        assertThat(alone).isGreaterThan(many.get(1).score());
    }

    @Test
    void equalBoostedScoresKeepInputOrder() {
        RawCandidate a = cand("a", 2f, 0, PROD, "method", null, "public");
        RawCandidate b = cand("b", 4f, 0, PROD, "method", null, "private");      // 4 * 0.5 = 2
        RawCandidate c = cand("c", 1f, 0, PROD, "class", null, "public");        // ≠ 2: 1.8
        RawCandidate d = cand("d", 2f, 0, PROD, "method", null, "public");
        assertThat(CandidateShaper.shape(List.of(a, b, c, d), "keyword", null))
                .extracting(SearchResult::id).containsExactly("a", "b", "d", "c");
        assertThat(CandidateShaper.shape(List.of(d, c, b, a), "keyword", null))
                .extracting(SearchResult::id).containsExactly("d", "b", "a", "c");
    }

    @Test
    void shapedResultCarriesTheFieldsTypeAndBoostedScore() {
        RawCandidate c = cand("id1", 3f, 0, PROD, "class", "interface", "public");
        SearchResult r = CandidateShaper.shape(List.of(c), "unified", "INTERFACE").get(0);
        assertThat(r.score()).isEqualTo(3f * 1.0f * 1.0f * 1.8f * 3.0f * 1.0f);
        assertThat(r.searchType()).isEqualTo("unified");
        assertThat(r).usingRecursiveComparison().ignoringFields("score", "searchType").isEqualTo(c.result());
        assertThat(Arrays.asList(r.startLine(), r.endLine())).containsExactly(3, 9);
    }
}
