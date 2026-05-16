package com.pharos.search;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Decorator that attaches a source snippet to each {@link SearchResult}.
 *
 * <p>Position resolution is delegated to two lazy {@link SnippetResolver}s:
 * <ul>
 *   <li>{@link #keywordResolver} — uses the Lucene Highlighter against the actual
 *       query to find the best-matching character offset in the body, then maps it
 *       to a source line.  Only called for results in the final ranked set.</li>
 *   <li>{@link #vectorResolver}  — uses argmax cosine over stored chunk vectors to
 *       identify the best-matching chunk by line range.</li>
 * </ul>
 *
 * <p>When both resolvers return a hint the snippets are combined:
 * <ul>
 *   <li>If the hints overlap (within {@code windowLines/2} lines) they are merged
 *       into one centred window.</li>
 *   <li>If they are disjoint, the vector chunk is shown first (semantically most
 *       relevant) followed by {@code // ...} and the keyword hit section.</li>
 * </ul>
 *
 * <p>When neither resolver provides a signal the first {@code windowLines} of the
 * body are returned.
 */
public class SnippetDecorator implements SearchResultDecorator {

    public static final int DEFAULT_WINDOW_LINES = 15;

    private final int            windowLines;
    private final SnippetResolver keywordResolver;
    private final SnippetResolver vectorResolver;

    /** Full constructor. */
    public SnippetDecorator(int windowLines,
                             SnippetResolver keywordResolver,
                             SnippetResolver vectorResolver) {
        this.windowLines     = Math.min(50, Math.max(5, windowLines));
        this.keywordResolver = keywordResolver != null ? keywordResolver : SnippetResolver.none();
        this.vectorResolver  = vectorResolver  != null ? vectorResolver  : SnippetResolver.none();
    }

    /** Backward-compatible constructor for callers that still hold a KeywordSearchStrategy. */
    public SnippetDecorator(KeywordSearchStrategy keywordStrategy, int windowLines) {
        this(windowLines, SnippetResolver.none(), SnippetResolver.none());
    }

    @Override
    public List<SearchResult> decorate(List<SearchResult> results, String query) {
        if (results.isEmpty()) return results;
        return results.stream()
                .map(r -> r.withSnippet(buildSnippet(r)))
                .collect(Collectors.toList());
    }

    private Snippet buildSnippet(SearchResult r) {
        String body = r.body();
        if (body == null || body.isBlank())
            return new Snippet(null, r.startLine(), r.endLine());

        Snippet kwHint  = keywordResolver.resolve(r);
        Snippet vecHint = vectorResolver.resolve(r);

        return combine(body, r.startLine(), r.endLine(), kwHint, vecHint);
    }

    // -------------------------------------------------------------------------
    // Hint combination
    // -------------------------------------------------------------------------

    private Snippet combine(String body, int docStart, int docEnd,
                             Snippet kwHint, Snippet vecHint) {
        String[] lines = body.split("\n", -1);

        if (kwHint == null && vecHint == null) return firstWindow(lines, docStart);

        int kwIdx  = hintToBodyIdx(kwHint,  docStart, lines.length);
        int vecIdx = hintToBodyIdx(vecHint, docStart, lines.length);

        if (kwHint == null)  return centeredWindow(lines, vecIdx, docStart);
        if (vecHint == null) return centeredWindow(lines, kwIdx,  docStart);

        // Both hints present
        if (Math.abs(kwIdx - vecIdx) <= windowLines / 2) {
            // Overlapping — merge around midpoint
            return centeredWindow(lines, (kwIdx + vecIdx) / 2, docStart);
        }
        // Disjoint — vector primary, keyword secondary
        return twoSectionSnippet(lines, vecIdx, kwIdx, docStart);
    }

    /** Returns the anchor line index within the body array from a Snippet hint. */
    private static int hintToBodyIdx(Snippet hint, int docStart, int bodyLen) {
        if (hint == null) return bodyLen / 2;
        int fileAnchor = (hint.startLine() + hint.endLine()) / 2;
        int idx = fileAnchor - docStart;
        return Math.max(0, Math.min(idx, bodyLen - 1));
    }

    // -------------------------------------------------------------------------
    // Window extraction helpers
    // -------------------------------------------------------------------------

    private Snippet firstWindow(String[] lines, int docStart) {
        int to = Math.min(lines.length, windowLines);
        String text = extractLines(lines, 0, to, lines.length);
        return new Snippet(text, docStart, docStart + to - 1);
    }

    private Snippet centeredWindow(String[] lines, int anchor, int docStart) {
        if (lines.length <= windowLines) {
            return new Snippet(String.join("\n", lines), docStart, docStart + lines.length - 1);
        }
        int from = Math.max(0, anchor - windowLines / 2);
        int to   = Math.min(lines.length, from + windowLines);
        from = Math.max(0, to - windowLines);
        String text = extractLines(lines, from, to, lines.length);
        return new Snippet(text, docStart + from, docStart + to - 1);
    }

    private Snippet twoSectionSnippet(String[] lines, int vecAnchor, int kwAnchor, int docStart) {
        int half = Math.max(3, windowLines / 3);

        int vFrom = clamp(vecAnchor - half, 0, lines.length);
        int vTo   = clamp(vFrom + half * 2, 0, lines.length);
        int kFrom = clamp(kwAnchor - half / 2, vTo + 1, lines.length);
        int kTo   = clamp(kFrom + half, 0, lines.length);

        StringBuilder sb = new StringBuilder();
        if (vFrom > 0) sb.append("// ...\n");
        for (int i = vFrom; i < vTo; i++) { if (i > vFrom) sb.append('\n'); sb.append(lines[i]); }
        sb.append("\n// ...\n");
        for (int i = kFrom; i < kTo; i++) { if (i > kFrom) sb.append('\n'); sb.append(lines[i]); }
        if (kTo < lines.length) sb.append("\n// ...");

        return new Snippet(sb.toString(), docStart + vFrom, docStart + kTo - 1);
    }

    private static String extractLines(String[] lines, int from, int to, int total) {
        StringBuilder sb = new StringBuilder();
        if (from > 0) sb.append("// ...\n");
        for (int i = from; i < to; i++) { if (i > from) sb.append('\n'); sb.append(lines[i]); }
        if (to < total) sb.append("\n// ...");
        return sb.toString();
    }

    private static int clamp(int v, int lo, int hi) { return Math.max(lo, Math.min(v, hi)); }
}
