package com.pharos.indexer;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;

import java.util.List;

/**
 * Splits a code or text unit into one or more {@link Chunk}s for multi-vector indexing.
 *
 * <p>Each returned {@link Chunk} carries contextualized text (with class/file header prepended)
 * ready to be sent to the embedding model, plus the source line range it covers.
 */
public interface Chunker {

    /**
     * Produces one or more Chunks for a method.
     *
     * <p>Short methods (body + prefix fits within the context window) yield exactly 1 chunk.
     * Long methods are split at blank-line boundaries near the context window limit;
     * each continuation chunk includes the class + method prefix and a short overlap
     * from the previous chunk for continuity.
     */
    List<Chunk> chunkMethod(ParsedMethod method, boolean multiChunk);

    /**
     * Variant of {@link #chunkMethod(ParsedMethod, boolean)} that accepts preloaded source
     * lines for the method's file.  When {@link ParsedMethod#body()} is null (lazy loading),
     * body text is extracted from {@code preloadedLines} rather than re-reading the file for
     * each method — callers that process multiple methods from the same file should use this.
     */
    default List<Chunk> chunkMethodWithLines(ParsedMethod method, boolean multiChunk,
                                              List<String> preloadedLines) {
        return chunkMethod(method, multiChunk);
    }

    /** Convenience single-chunk overload — equivalent to {@code chunkMethod(method, false)}. */
    default Chunk chunkMethod(ParsedMethod method) {
        List<Chunk> chunks = chunkMethod(method, false);
        return chunks.isEmpty() ? new Chunk("", method.startLine(), method.endLine()) : chunks.get(0);
    }

    /**
     * Splits a class into 1-N chunks.
     *
     * <p>Produces a header chunk (class declaration + javadoc + ungrouped methods) and
     * one group chunk per camelCase-prefix method group that has enough methods to warrant
     * a dedicated chunk.
     *
     * @param cls             the parsed class
     * @param synthesizedBody concatenated method signatures/javadocs (pre-built by DocumentMapper)
     * @param methods         the class's methods, used for grouping by name prefix
     */
    List<Chunk> chunkClass(ParsedClass cls, String synthesizedBody, List<ParsedMethod> methods);

    /**
     * Splits arbitrary text (markdown, prose, documentation) into context-window-sized chunks
     * using paragraph and sentence boundaries.
     *
     * <p>Each continuation chunk is prefixed with {@code fileHeader} and a short overlap
     * from the previous chunk so vectors remain contextually grounded.
     *
     * @param fileHeader  one-line description of the source file (path or title)
     * @param content     the full text to split
     * @param startLine   1-based line number where the content begins in the source file
     */
    List<Chunk> chunkText(String fileHeader, String content, int startLine);
}
