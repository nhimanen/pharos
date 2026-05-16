package com.pharos.search;

/**
 * Lazily resolves the best source position for a {@link SearchResult}'s snippet.
 *
 * <p>Implementations are created during search (capturing query context such as
 * the Lucene {@link org.apache.lucene.search.Query} or an embedding vector) but
 * called only for the results that survive to the final ranked set — not for the
 * full candidate pool.
 *
 * <p>The returned {@link Snippet} carries only line-range information
 * ({@code text == null}); {@link SnippetDecorator} fills in the text by extracting
 * the relevant lines from the stored body.
 */
@FunctionalInterface
public interface SnippetResolver {

    /**
     * Returns a positional hint for {@code result}, or {@code null} when this
     * resolver has no signal for that result (e.g. the document has no stored
     * chunk vectors, or no query terms matched the body).
     *
     * <p>Implementations must be safe to call from multiple threads.
     */
    Snippet resolve(SearchResult result);

    /** No-op resolver — always returns null. */
    static SnippetResolver none() { return r -> null; }
}
