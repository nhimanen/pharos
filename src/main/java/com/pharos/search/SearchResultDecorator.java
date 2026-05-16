package com.pharos.search;

import java.util.List;

/**
 * Enriches a list of search results after retrieval and scoring.
 * Decorators are applied post-pipeline (after all merging and reranking).
 *
 * <p>Implementations may add snippets, highlights, cross-references, etc.
 * The decorator receives the original results and the raw query string,
 * and returns a new list (usually the same size, with enriched records).
 */
@FunctionalInterface
public interface SearchResultDecorator {

    List<SearchResult> decorate(List<SearchResult> results, String query);

    /** No-op decorator — returns results unchanged. */
    static SearchResultDecorator identity() {
        return (results, query) -> results;
    }

    /** Chains this decorator with {@code next}, applying both in sequence. */
    default SearchResultDecorator andThen(SearchResultDecorator next) {
        return (results, query) -> next.decorate(this.decorate(results, query), query);
    }
}
