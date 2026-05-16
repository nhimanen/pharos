package com.pharos.search;

import java.util.List;

/**
 * Wraps search results together with timing trace, pipeline metadata,
 * zero-result hints, and lazy snippet resolvers.
 *
 * {@link #keywordResolver} — built from the keyword query used in the search;
 * uses the Lucene Highlighter to find the best-matching character offset.
 *
 * {@link #vectorResolver} — built from the query embedding; finds the
 * best-matching chunk vector via argmax cosine.
 *
 * Both resolvers are called lazily — only for results in the final ranked set,
 * not for the full retrieval candidate pool.
 */
public record SearchResponse(
        List<SearchResult>             results,
        SearchTrace                    trace,
        String                         resolvedType,
        ZeroResultAdvisor.Suggestions  suggestions,
        SnippetResolver                keywordResolver,
        SnippetResolver                vectorResolver
) {
    /** Backward-compatible constructor — no suggestions, no resolvers. */
    public SearchResponse(List<SearchResult> results, SearchTrace trace) {
        this(results, trace, null, null, null, null);
    }

    /** Backward-compatible constructor — no resolvers. */
    public SearchResponse(List<SearchResult> results, SearchTrace trace, String resolvedType) {
        this(results, trace, resolvedType, null, null, null);
    }

    /** Backward-compatible constructor — no resolvers. */
    public SearchResponse(List<SearchResult> results, SearchTrace trace,
                          String resolvedType, ZeroResultAdvisor.Suggestions suggestions) {
        this(results, trace, resolvedType, suggestions, null, null);
    }
}
