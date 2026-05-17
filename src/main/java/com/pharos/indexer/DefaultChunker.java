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
        String body   = method.body() != null ? method.body() : "";

        int available = MAX_CHARS - prefix.length() - 10; // -10 for closing "}"

        if (body.length() <= available) {
            return new Chunk(prefix + body + "\n}", method.startLine(), method.endLine());
        }

        // Multi-chunk: split body at blank-line boundaries
        return chunkLongMethod(prefix, body, method.startLine());
    }

    private Chunk chunkLongMethod(String prefix, String body, int methodStartLine) {
        // Single-chunk path for callers that need List<Chunk> — delegate to the list builder
        return buildMethodChunks(prefix, body, methodStartLine).get(0);
    }

    /**
     * Splits a long method body into multiple chunks, each within the context window.
     * Called by {@link #chunkMethod} when the method exceeds {@value #MAX_CHARS} chars.
     */
    private List<Chunk> buildMethodChunks(String prefix, String body, int methodStartLine) {
        int available = MAX_CHARS - prefix.length() - PREFIX_RESERVE;
        String[] lines = body.split("\n", -1);

        List<Chunk> chunks = new ArrayList<>();
        int lineIdx = 0;
        String overlapLines = "";

        while (lineIdx < lines.length) {
            StringBuilder chunkBody = new StringBuilder();
            if (!overlapLines.isEmpty()) {
                chunkBody.append("// ...\n").append(overlapLines).append("// ...\n");
            }
            int chunkStartIdx = lineIdx;
            int lastBlankIdx  = -1;

            while (lineIdx < lines.length) {
                String line = lines[lineIdx];
                if (chunkBody.length() + line.length() + 1 > available) {
                    // Prefer blank-line split (if one exists in the second half of the chunk)
                    if (lastBlankIdx > chunkStartIdx && chunkBody.length() > available / 2) {
                        // Roll back to last blank line — rebuild chunkBody up to that point
                        chunkBody.setLength(0);
                        if (!overlapLines.isEmpty())
                            chunkBody.append("// ...\n").append(overlapLines).append("// ...\n");
                        for (int i = chunkStartIdx; i <= lastBlankIdx; i++)
                            chunkBody.append(lines[i]).append('\n');
                        lineIdx = lastBlankIdx + 1;
                    }
                    break;
                }
                if (line.isBlank()) lastBlankIdx = lineIdx;
                chunkBody.append(line).append('\n');
                lineIdx++;
            }

            boolean hasMore = lineIdx < lines.length;
            if (hasMore) chunkBody.append("// ...");

            int chunkEndIdx = lineIdx - 1;
            int startLine   = methodStartLine + chunkStartIdx;
            int endLine     = methodStartLine + chunkEndIdx;
            chunks.add(new Chunk(prefix + chunkBody, startLine, endLine));

            // Overlap: first 2 non-empty lines of this chunk's body (after the "..." marker)
            overlapLines = firstNonEmptyLines(chunkBody.toString(), 2);
        }

        return chunks.isEmpty()
                ? List.of(new Chunk(prefix + body.substring(0, Math.min(body.length(), available)), methodStartLine, methodStartLine))
                : chunks;
    }

    @Override
    public List<Chunk> chunkMethod(ParsedMethod method, boolean multiChunk) {
        if (!multiChunk) return List.of(chunkMethod(method));
        String prefix = buildMethodPrefix(method);
        String body   = method.body() != null ? method.body() : "";
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

    /** Extracts the first {@code n} non-empty lines from text for use as overlap. */
    private static String firstNonEmptyLines(String text, int n) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (String line : text.split("\n", -1)) {
            if (!line.isBlank() && !line.startsWith("//")) {
                sb.append(line).append('\n');
                if (++count >= n) break;
            }
        }
        return sb.length() > OVERLAP_CHARS ? sb.substring(0, OVERLAP_CHARS) : sb.toString();
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
