package com.pharos.indexer;

/**
 * A logical sub-block of a class, carrying the text sent to the embedding model
 * (which includes parent-class context for grounding) and its source line range.
 *
 * For methods pharos always produces exactly one Chunk — the full method body
 * with its signature/javadoc as context prefix.  For classes and long text
 * documents the chunker produces 1-N Chunks, one per logical section.
 */
public record Chunk(
        String text,       // contextualized text to embed (class/group prefix + content)
        int    startLine,  // 1-based, inclusive
        int    endLine     // 1-based, inclusive
) {}
