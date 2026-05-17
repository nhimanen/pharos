package com.pharos.indexer;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;

import java.util.*;

/**
 * Heuristic chunker for multi-vector indexing.
 *
 * <h3>Methods</h3>
 * A method that fits within the context window (≤ {@value #MAX_CHARS} chars) becomes
 * one chunk.  If it is longer, the body is split at blank-line boundaries closest to
 * {@value #MAX_CHARS} and each piece becomes its own chunk, all prefixed with the class
 * and method signature for grounding.  Subsequent chunks include a short overlap (first
 * 2 lines of the preceding chunk) to maintain continuity.
 *
 * <h3>Classes</h3>
 * 1-N chunks built from method-name-prefix groups:
 * <ul>
 *   <li>A header chunk with class declaration, javadoc, and ungrouped methods.</li>
 *   <li>One group chunk per camelCase-first-word prefix ({@code get*}, {@code find*}, …)
 *       that contains at least {@value #MIN_GROUP_SIZE} methods.</li>
 * </ul>
 *
 * <h3>Text / documentation</h3>
 * {@link #chunkText} splits arbitrary prose at paragraph or sentence boundaries near
 * the context-window limit.  Each continuation chunk is prefixed with the file header
 * and a short overlap from the previous chunk.
 */
public class DefaultChunker implements Chunker {

    /**
     * Character budget per chunk sent to the embedding model.
     *
     * <p>This is a char-level pre-filter; the embedding provider truncates to its token
     * limit independently ({@code embeddingMaxTokens} in config, default 512 for MiniLM /
     * 8192 for Jina-v2).  Rough conversion: ~4 chars per token for English/Java code.
     *
     * <ul>
     *   <li>Jina-v2-base-code (8 192 tokens): 8 000 chars ≈ 2 000 tokens — deliberately
     *       conservative so each chunk stays focused; uses ~25 % of available context.</li>
     *   <li>all-MiniLM-L6-v2 (512 tokens): 8 000 chars exceeds the window; DJL truncates
     *       silently to 512 tokens before encoding.  Consider lowering to 2 000 chars when
     *       using MiniLM to avoid generating text that is immediately discarded.</li>
     * </ul>
     */
    static final int MAX_CHARS      = 8_000;
    /** Characters reserved for context prefix inside a chunk. */
    private static final int PREFIX_RESERVE = 400;
    /** Minimum methods to form a named group (remainder goes to the header chunk). */
    private static final int MIN_GROUP_SIZE = 2;
    /** Characters of the previous chunk kept as overlap at the start of the next. */
    private static final int OVERLAP_CHARS  = 200;

    // -------------------------------------------------------------------------
    // Chunker interface — methods
    // -------------------------------------------------------------------------

    @Override
    public Chunk chunkMethod(ParsedMethod method) {
        String prefix = buildMethodPrefix(method);
        String body   = resolveBody(method, null);

        int available = MAX_CHARS - prefix.length() - 10; // -10 for closing "}"

        if (body.length() <= available) {
            return new Chunk(prefix + body + "\n}", method.startLine(), method.endLine());
        }

        // Multi-chunk: split body at blank-line boundaries
        return chunkLongMethod(prefix, body, method.startLine());
    }

    private Chunk chunkLongMethod(String prefix, String body, int methodStartLine) {
        List<Chunk> chunks = buildMethodChunks(prefix, body, methodStartLine);
        return chunks.isEmpty() ? new Chunk(prefix, methodStartLine, methodStartLine) : chunks.get(0);
    }

    /**
     * Splits a long method body into multiple chunks using an index-based line scan.
     *
     * <p>Improvements over a naive split-based approach:
     * <ul>
     *   <li>No {@code body.split("\n")} — avoids allocating N String objects for each line;
     *       instead a single {@code int[]} of line-start positions is built from the char array.</li>
     *   <li>No rollback — the split point is determined by a forward scan before any
     *       StringBuilder is touched, so each chunk text is built exactly once.</li>
     *   <li>Prefix pre-appended — the final chunk String is produced by a single
     *       {@code sb.toString()} with no extra {@code prefix + body} concatenation.</li>
     *   <li>Overlap extracted from line indexes — no {@code chunkBody.toString()} pass.</li>
     * </ul>
     */
    private List<Chunk> buildMethodChunks(String prefix, String body, int methodStartLine) {
        int available = MAX_CHARS - prefix.length() - PREFIX_RESERVE;

        // Build line-start position array — one pass over chars, no String allocation per line.
        int[] lineStarts = buildLineStarts(body);
        int lineCount    = lineStarts.length;

        List<Chunk> chunks  = new ArrayList<>(Math.max(1, body.length() / Math.max(1, available) + 1));
        int lineIdx         = 0;
        int overlapFrom     = -1; // start line index of overlap region, -1 = none
        int overlapTo       = -1; // exclusive end

        while (lineIdx < lineCount) {
            int chunkFrom = lineIdx;

            // Overlap header overhead: "// ...\n" + overlap lines + "// ...\n" ≈ 14 + overlapLen
            int overlapLen = overlapFrom >= 0 ? rangeLength(body, lineStarts, overlapFrom, overlapTo) + 14 : 0;

            // Forward scan: find where adding the next line would exceed `available`
            int charsUsed  = overlapLen;
            int lastBlank  = -1;
            int chunkTo    = chunkFrom;
            while (chunkTo < lineCount) {
                int lineLen = lineLen(body, lineStarts, chunkTo);
                if (charsUsed + lineLen + 1 > available) break;
                if (isBlankAt(body, lineStarts, chunkTo)) lastBlank = chunkTo;
                charsUsed += lineLen + 1;
                chunkTo++;
            }

            // Prefer a blank-line boundary in the second half of the chunk
            if (lastBlank > chunkFrom && charsUsed > available / 2) chunkTo = lastBlank + 1;
            if (chunkTo <= chunkFrom) chunkTo = chunkFrom + 1; // always advance

            // Build chunk text in ONE pass — prefix first, then overlap header, then lines
            StringBuilder sb = new StringBuilder(prefix.length() + charsUsed + 20);
            sb.append(prefix);
            if (overlapFrom >= 0) {
                sb.append("// ...\n");
                appendLines(sb, body, lineStarts, overlapFrom, overlapTo);
                sb.append("// ...\n");
            }
            appendLines(sb, body, lineStarts, chunkFrom, chunkTo);
            if (chunkTo < lineCount) sb.append("// ...");

            chunks.add(new Chunk(sb.toString(),
                    methodStartLine + chunkFrom,
                    methodStartLine + Math.min(chunkTo, lineCount) - 1));

            // Overlap for next chunk: first 2 non-empty lines — scan indexes, no String copy
            int[] ov = firstNonEmptyLineRange(body, lineStarts, chunkFrom, chunkTo, 2);
            overlapFrom = ov[0];
            overlapTo   = ov[1];
            lineIdx     = chunkTo;
        }

        return chunks.isEmpty()
                ? List.of(new Chunk(prefix + body.substring(0, Math.min(body.length(), available)),
                          methodStartLine, methodStartLine))
                : chunks;
    }

    // -------------------------------------------------------------------------
    // Line-index helpers (operate on char positions, no String allocation)
    // -------------------------------------------------------------------------

    /** Returns an int[] where entry i is the char offset of line i's start in {@code s}. */
    private static int[] buildLineStarts(String s) {
        if (s.isEmpty()) return new int[]{0};
        int count = 1;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == '\n') count++;
        int[] starts = new int[count];
        int idx = 1;
        for (int i = 0; i < s.length() && idx < count; i++)
            if (s.charAt(i) == '\n') starts[idx++] = i + 1;
        return starts;
    }

    /** Length of line {@code li} (excluding the trailing newline). */
    private static int lineLen(String s, int[] starts, int li) {
        int start = starts[li];
        int end   = li + 1 < starts.length ? starts[li + 1] - 1 : s.length();
        return end - start;
    }

    /** True when line {@code li} contains only whitespace. */
    private static boolean isBlankAt(String s, int[] starts, int li) {
        int start = starts[li];
        int end   = li + 1 < starts.length ? starts[li + 1] - 1 : s.length();
        for (int i = start; i < end; i++) if (!Character.isWhitespace(s.charAt(i))) return false;
        return true;
    }

    /** Total char length of lines [from, to) including their newlines. */
    private static int rangeLength(String s, int[] starts, int from, int to) {
        if (from >= to) return 0;
        int start = starts[from];
        int end   = to < starts.length ? starts[to] : s.length() + 1; // +1 for implicit newline
        return end - start;
    }

    /** Appends lines [from, to) from {@code s} (using precomputed starts) to {@code sb}. */
    private static void appendLines(StringBuilder sb, String s, int[] starts, int from, int to) {
        for (int li = from; li < to; li++) {
            int start = starts[li];
            int end   = li + 1 < starts.length ? starts[li + 1] - 1 : s.length();
            sb.append(s, start, end).append('\n');
        }
    }

    /**
     * Returns [firstNonEmptyLine, exclusiveEnd] covering the first {@code n} non-empty
     * lines in [from, to).  Returns [-1, -1] when none found.
     */
    private static int[] firstNonEmptyLineRange(String s, int[] starts, int from, int to, int n) {
        int count = 0, first = -1, last = -1;
        for (int li = from; li < to && count < n; li++) {
            if (!isBlankAt(s, starts, li)) {
                if (first < 0) first = li;
                last = li;
                count++;
            }
        }
        if (first < 0) return new int[]{-1, -1};
        return new int[]{first, last + 1};
    }

    @Override
    public List<Chunk> chunkMethod(ParsedMethod method, boolean multiChunk) {
        if (!multiChunk) return List.of(chunkMethod(method));
        String prefix = buildMethodPrefix(method);
        String body   = resolveBody(method, null);
        int available = MAX_CHARS - prefix.length() - 10;
        if (body.length() <= available) {
            return List.of(new Chunk(prefix + body + "\n}", method.startLine(), method.endLine()));
        }
        return buildMethodChunks(prefix, body, method.startLine());
    }

    /**
     * Returns the method body — from {@link ParsedMethod#body()} when present,
     * otherwise read from the source file via {@link DocumentMapper#readBodyFromFile}.
     *
     * @param preloadedLines  source lines for the file already in memory, or null to read on demand
     */
    public static String resolveBody(ParsedMethod method, java.util.List<String> preloadedLines) {
        String body = method.body();
        if (body != null) return body;
        return DocumentMapper.readBodyFromFile(
                method.filePath(), method.startLine(), method.endLine(), preloadedLines);
    }

    public List<Chunk> chunkMethodWithLines(ParsedMethod method, boolean multiChunk,
                                             List<String> preloadedLines) {
        String prefix = buildMethodPrefix(method);
        String body   = resolveBody(method, preloadedLines);
        if (!multiChunk) {
            int avail = MAX_CHARS - prefix.length() - 10;
            return body.length() <= avail
                    ? List.of(new Chunk(prefix + body + "\n}", method.startLine(), method.endLine()))
                    : List.of(chunkLongMethod(prefix, body, method.startLine()));
        }
        int available = MAX_CHARS - prefix.length() - 10;
        if (body.length() <= available) {
            return List.of(new Chunk(prefix + body + "\n}", method.startLine(), method.endLine()));
        }
        return buildMethodChunks(prefix, body, method.startLine());
    }

    // -------------------------------------------------------------------------
    // Chunker interface — classes
    // -------------------------------------------------------------------------

    @Override
    public List<Chunk> chunkClass(ParsedClass cls, String synthesizedBody,
                                   List<ParsedMethod> methods) {
        String classPrefix = buildClassPrefix(cls);

        if (methods.isEmpty()) {
            return List.of(new Chunk(classPrefix + truncate(synthesizedBody, MAX_CHARS - classPrefix.length()),
                    cls.startLine(), cls.endLine()));
        }

        Map<String, List<ParsedMethod>> groups = groupByPrefix(methods);

        List<ParsedMethod> headerMethods = new ArrayList<>();
        Map<String, List<ParsedMethod>> namedGroups = new LinkedHashMap<>();
        for (var entry : groups.entrySet()) {
            if (entry.getValue().size() >= MIN_GROUP_SIZE)
                namedGroups.put(entry.getKey(), entry.getValue());
            else
                headerMethods.addAll(entry.getValue());
        }

        if (namedGroups.isEmpty()) {
            return List.of(new Chunk(classPrefix + truncate(synthesizedBody, MAX_CHARS - classPrefix.length()),
                    cls.startLine(), cls.endLine()));
        }

        List<Chunk> chunks = new ArrayList<>();
        String headerText = buildGroupText(classPrefix, "header", headerMethods);
        int headerEnd = headerMethods.isEmpty() ? cls.startLine()
                : headerMethods.stream().mapToInt(ParsedMethod::endLine).max().orElse(cls.startLine());
        chunks.add(new Chunk(headerText, cls.startLine(), headerEnd));

        for (var entry : namedGroups.entrySet()) {
            List<ParsedMethod> group = entry.getValue();
            String groupText  = buildGroupText(classPrefix, entry.getKey() + "*", group);
            int groupStart    = group.stream().mapToInt(ParsedMethod::startLine).min().orElse(cls.startLine());
            int groupEnd      = group.stream().mapToInt(ParsedMethod::endLine).max().orElse(cls.endLine());
            chunks.add(new Chunk(groupText, groupStart, groupEnd));
        }

        return chunks;
    }

    // -------------------------------------------------------------------------
    // Text / documentation chunking
    // -------------------------------------------------------------------------

    /**
     * Splits arbitrary text (markdown, javadoc, plain prose) into context-window-sized
     * chunks using paragraph and sentence boundaries.
     *
     * <p>Each chunk after the first is prefixed with {@code fileHeader} and a short
     * overlap from the previous chunk so the vector is grounded in context.
     *
     * @param fileHeader one-line description of the source (e.g. file path or title)
     * @param content    the full text to split
     * @param startLine  1-based line number where the content begins in the source file
     */
    public List<Chunk> chunkText(String fileHeader, String content, int startLine) {
        String header  = fileHeader != null ? "[" + fileHeader + "]\n" : "";
        int    budget  = MAX_CHARS - header.length() - OVERLAP_CHARS - 20;

        if (content.length() <= budget) {
            return List.of(new Chunk(header + content, startLine,
                    startLine + countLines(content) - 1));
        }

        List<Chunk> chunks = new ArrayList<>();
        int pos = 0;
        String overlap = "";
        int currentLine = startLine;

        while (pos < content.length()) {
            int end = findSplitPoint(content, pos, budget - overlap.length());

            String piece = content.substring(pos, end);
            StringBuilder chunkText = new StringBuilder(header);
            if (!overlap.isEmpty()) {
                chunkText.append("// ...\n").append(overlap).append("// ...\n");
            }
            chunkText.append(piece);

            int lines = countLines(piece);
            chunks.add(new Chunk(chunkText.toString(), currentLine, currentLine + lines - 1));

            overlap     = extractOverlap(piece);
            currentLine += lines;
            pos          = end;
        }

        return chunks;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private static String buildMethodPrefix(ParsedMethod method) {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(method.qualifiedClassName()).append(']').append('\n');
        if (method.javadoc() != null && !method.javadoc().isBlank()) {
            sb.append("/** ").append(truncate(method.javadoc().trim(), 300)).append(" */\n");
        }
        sb.append(method.signature()).append(" {\n");
        return sb.toString();
    }

    private static String buildClassPrefix(ParsedClass cls) {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(cls.qualifiedClassName()).append(']').append('\n');
        if (cls.javadoc() != null && !cls.javadoc().isBlank()) {
            sb.append("/** ").append(truncate(cls.javadoc().trim(), 300)).append(" */\n");
        }
        return sb.toString();
    }

    private static String buildGroupText(String classPrefix, String groupLabel,
                                          List<ParsedMethod> methods) {
        StringBuilder sb = new StringBuilder(classPrefix);
        sb.append("Group: ").append(groupLabel).append('\n');
        int chars = sb.length();
        int limit = MAX_CHARS - PREFIX_RESERVE;
        for (ParsedMethod m : methods) {
            sb.append(m.signature()).append('\n');
            if (m.javadoc() != null && !m.javadoc().isBlank()) {
                String jdoc = "  // " + m.javadoc().trim().replaceAll("\\s+", " ");
                sb.append(jdoc, 0, Math.min(jdoc.length(), 120)).append('\n');
            }
            chars = sb.length();
            if (chars > limit) break;
        }
        return sb.toString();
    }

    private static Map<String, List<ParsedMethod>> groupByPrefix(List<ParsedMethod> methods) {
        Map<String, List<ParsedMethod>> groups = new LinkedHashMap<>();
        for (ParsedMethod m : methods) {
            String prefix = firstWord(m.methodName());
            groups.computeIfAbsent(prefix, k -> new ArrayList<>()).add(m);
        }
        return groups;
    }

    static String firstWord(String name) {
        if (name == null || name.isBlank() || name.startsWith("<")) return "";
        StringBuilder word = new StringBuilder();
        for (char c : name.toCharArray()) {
            if (Character.isUpperCase(c) && !word.isEmpty()) break;
            word.append(Character.toLowerCase(c));
        }
        return word.toString();
    }

    /**
     * Finds the best split point in {@code text} starting from {@code from},
     * preferring blank lines, then sentence endings, then word boundaries.
     */
    private static int findSplitPoint(String text, int from, int budget) {
        int hard = Math.min(from + budget, text.length());
        if (hard >= text.length()) return text.length();

        // 1. Paragraph boundary (blank line) in the second half of the budget
        int half = from + budget / 2;
        int blankLine = text.lastIndexOf("\n\n", hard);
        if (blankLine > half) return blankLine + 2;

        // 2. Single newline
        int newLine = text.lastIndexOf('\n', hard);
        if (newLine > half) return newLine + 1;

        // 3. Sentence boundary (.  !  ?)
        for (int i = hard; i > half; i--) {
            char c = text.charAt(i - 1);
            if ((c == '.' || c == '!' || c == '?') && i < text.length() && text.charAt(i) == ' ') {
                return i + 1;
            }
        }

        // 4. Word boundary
        int space = text.lastIndexOf(' ', hard);
        if (space > half) return space + 1;

        return hard; // hard split as last resort
    }

/** Returns a short overlap string (first sentences up to OVERLAP_CHARS) from the chunk. */
    private static String extractOverlap(String piece) {
        if (piece.length() <= OVERLAP_CHARS) return piece;
        // Find sentence end near OVERLAP_CHARS
        for (int i = OVERLAP_CHARS; i > OVERLAP_CHARS / 2; i--) {
            char c = piece.charAt(i - 1);
            if ((c == '.' || c == '!' || c == '?') &&
                    i < piece.length() && piece.charAt(i) == ' ') {
                return piece.substring(0, i + 1).trim();
            }
        }
        return piece.substring(0, OVERLAP_CHARS).trim();
    }

    private static int countLines(String text) {
        if (text == null || text.isEmpty()) return 1;
        int count = 1;
        for (int i = 0; i < text.length(); i++) if (text.charAt(i) == '\n') count++;
        return count;
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }
}
