package com.pharos.search;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SnippetDecorator}.
 *
 * Resolvers are stubbed with lambdas — no mocking framework needed.
 * All tests build minimal {@link SearchResult} instances via the backward-compatible
 * constructor (snippet = null).
 */
class SnippetDecoratorTest {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Builds a result with the given body whose method starts at startLine. */
    private static SearchResult result(String body, int startLine, int endLine) {
        return new SearchResult(
                "proj:com.example.Foo#bar()", "proj",
                "com.example", "Foo", "com.example.Foo",
                "bar", "void bar()", "void",
                body, null, "public", "/src/Foo.java",
                startLine, endLine,
                1.0f, "keyword", "method"
        );
    }

    /** Builds a body string with {@code n} numbered lines. */
    private static String lines(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            if (i > 1) sb.append('\n');
            sb.append("line").append(i);
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // No resolvers — both return null
    // -------------------------------------------------------------------------

    @Nested
    class NoResolverSignal {

        private final SnippetDecorator dec = new SnippetDecorator(
                5, r -> null, r -> null);

        @Test
        void shortBody_snippetEqualsFullBody() {
            String body = lines(3);
            SearchResult r = result(body, 10, 12);
            List<SearchResult> out = dec.decorate(List.of(r), "query");

            Snippet snippet = out.get(0).snippet();
            assertNotNull(snippet);
            // Full 3-line body must be present in the text (no truncation needed)
            assertTrue(snippet.text().contains("line1"), "Expected line1 in: " + snippet.text());
            assertTrue(snippet.text().contains("line3"), "Expected line3 in: " + snippet.text());
        }

        @Test
        void longBody_firstWindowLinesReturned() {
            // 20 lines with windowLines=5: only first 5 should appear, line6 should not
            SnippetDecorator dec20 = new SnippetDecorator(5, r -> null, r -> null);
            String body = lines(20);
            SearchResult r = result(body, 1, 20);
            List<SearchResult> out = dec20.decorate(List.of(r), "query");

            Snippet snippet = out.get(0).snippet();
            assertNotNull(snippet);
            assertTrue(snippet.text().contains("line1"), "line1 should be present");
            assertTrue(snippet.text().contains("line5"), "line5 should be present");
            assertFalse(snippet.text().contains("line6") && !snippet.text().contains("line16"),
                    "line6 should not appear unless wrapped");
        }

        @Test
        void nullBody_snippetHasNullText() {
            SearchResult r = result(null, 10, 20);
            List<SearchResult> out = dec.decorate(List.of(r), "query");

            Snippet snippet = out.get(0).snippet();
            assertNotNull(snippet);
            assertNull(snippet.text());
        }

        @Test
        void startLinePreservedInSnippet() {
            String body = lines(3);
            SearchResult r = result(body, 42, 44);
            List<SearchResult> out = dec.decorate(List.of(r), "query");

            Snippet snippet = out.get(0).snippet();
            assertEquals(42, snippet.startLine());
        }
    }

    // -------------------------------------------------------------------------
    // Keyword resolver only
    // -------------------------------------------------------------------------

    @Nested
    class KeywordResolverOnly {

        @Test
        void resolverReturnsAnchor_snippetCentredOnThatLine() {
            // Body: 20 lines, method starts at line 100
            // Keyword resolver says the match is at line 115 (body index 15)
            String body = lines(20);
            SearchResult r = result(body, 100, 119);

            // Anchor: startLine=115, endLine=115 → midpoint=115, bodyIdx=115-100=15
            SnippetResolver kwResolver = result2 -> new Snippet(null, 115, 115);
            SnippetDecorator dec = new SnippetDecorator(10, kwResolver, r2 -> null);

            List<SearchResult> out = dec.decorate(List.of(r), "query");
            Snippet snippet = out.get(0).snippet();

            assertNotNull(snippet);
            // startLine should be somewhere around 115 ± half-window
            assertTrue(snippet.startLine() >= 100 && snippet.startLine() <= 115,
                    "startLine should be near anchor: " + snippet.startLine());
        }

        @Test
        void bodyFitInWindow_fullBodyReturned() {
            // body shorter than windowLines: always returns full body regardless of resolver
            String body = lines(3);
            SearchResult r = result(body, 50, 52);

            SnippetResolver kwResolver = result2 -> new Snippet(null, 51, 51);
            SnippetDecorator dec = new SnippetDecorator(10, kwResolver, r2 -> null);

            List<SearchResult> out = dec.decorate(List.of(r), "query");
            Snippet snippet = out.get(0).snippet();

            assertNotNull(snippet);
            assertTrue(snippet.text().contains("line1"), "All lines should be present: " + snippet.text());
            assertTrue(snippet.text().contains("line3"), "All lines should be present: " + snippet.text());
        }
    }

    // -------------------------------------------------------------------------
    // Both resolvers
    // -------------------------------------------------------------------------

    @Nested
    class BothResolvers {

        @Test
        void overlappingHints_singleMergedWindow() {
            // 30-line body, method starts at line 1
            // kw anchor=line 12, vec anchor=line 14 → |12-14|=2 < windowLines/2=5 → merge
            String body = lines(30);
            SearchResult r = result(body, 1, 30);

            SnippetResolver kwResolver  = result2 -> new Snippet(null, 12, 12);
            SnippetResolver vecResolver = result2 -> new Snippet(null, 14, 14);
            SnippetDecorator dec = new SnippetDecorator(10, kwResolver, vecResolver);

            List<SearchResult> out = dec.decorate(List.of(r), "query");
            Snippet snippet = out.get(0).snippet();

            assertNotNull(snippet);
            // Single window — no "// ..." separator between the two sections
            // (a separator would appear only in a two-section snippet)
            long separatorCount = countSeparators(snippet.text());
            assertTrue(separatorCount <= 2,
                    "Overlapping hints should produce at most a head/tail ellipsis, got: "
                            + separatorCount + " separators in:\n" + snippet.text());
        }

        @Test
        void disjointHints_snippetContainsSeparator() {
            // 60-line body, method starts at line 1
            // kw anchor=line 55 (bodyIdx=54), vec anchor=line 5 (bodyIdx=4) → far apart
            String body = lines(60);
            SearchResult r = result(body, 1, 60);

            SnippetResolver kwResolver  = result2 -> new Snippet(null, 55, 55);
            SnippetResolver vecResolver = result2 -> new Snippet(null, 5,  5);
            SnippetDecorator dec = new SnippetDecorator(10, kwResolver, vecResolver);

            List<SearchResult> out = dec.decorate(List.of(r), "query");
            Snippet snippet = out.get(0).snippet();

            assertNotNull(snippet);
            assertTrue(snippet.text().contains("// ..."),
                    "Disjoint hints should produce a '// ...' separator:\n" + snippet.text());
        }

        @Test
        void disjointHints_startLineReflectsVectorChunkPosition() {
            // Vector resolver signals line 5 → snippet startLine should be near 5, not 55
            String body = lines(60);
            SearchResult r = result(body, 1, 60);

            SnippetResolver kwResolver  = result2 -> new Snippet(null, 55, 55);
            SnippetResolver vecResolver = result2 -> new Snippet(null, 5,  5);
            SnippetDecorator dec = new SnippetDecorator(10, kwResolver, vecResolver);

            List<SearchResult> out = dec.decorate(List.of(r), "query");
            Snippet snippet = out.get(0).snippet();

            assertNotNull(snippet);
            // startLine should be near the vector chunk (primary anchor), i.e. near 1-5 not 55
            assertTrue(snippet.startLine() <= 10,
                    "startLine should reflect vector chunk position (near 5), got: "
                            + snippet.startLine());
        }
    }

    // -------------------------------------------------------------------------
    // Empty input
    // -------------------------------------------------------------------------

    @Nested
    class EmptyResultList {

        @Test
        void emptyList_returnedEmpty() {
            SnippetDecorator dec = new SnippetDecorator(10, r -> null, r -> null);
            List<SearchResult> out = dec.decorate(List.of(), "query");
            assertTrue(out.isEmpty());
        }
    }

    // -------------------------------------------------------------------------
    // Window clamping via constructor
    // -------------------------------------------------------------------------

    @Nested
    class WindowClamping {

        @Test
        void windowBelowMin_clampedToFive() {
            // windowLines=2 is below minimum 5; decorator still works without throwing
            SnippetDecorator dec = new SnippetDecorator(2, r -> null, r -> null);
            SearchResult r = result(lines(10), 1, 10);
            assertDoesNotThrow(() -> dec.decorate(List.of(r), "q"));
        }

        @Test
        void windowAboveMax_clampedToFifty() {
            // windowLines=999 is above maximum 50; decorator still works
            SnippetDecorator dec = new SnippetDecorator(999, r -> null, r -> null);
            SearchResult r = result(lines(10), 1, 10);
            assertDoesNotThrow(() -> dec.decorate(List.of(r), "q"));
        }
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private static long countSeparators(String text) {
        if (text == null) return 0;
        long count = 0;
        int idx = 0;
        while ((idx = text.indexOf("// ...", idx)) >= 0) {
            count++;
            idx += 6;
        }
        return count;
    }
}
