package com.pharos.search;

/**
 * Classifies a raw query string into a {@link SearchRequest.SearchType}.
 *
 * Used when {@code type=auto} to select the best retrieval strategy without
 * requiring the caller to reason about search mechanics.
 */
@FunctionalInterface
public interface QueryClassifier {

    SearchRequest.SearchType classify(String query);
}
