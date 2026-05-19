package com.pharos.search;

/**
 * Classifies a raw query string into a {@link QueryClassification} that carries
 * both the resolved {@link SearchRequest.SearchType} and an optional docType filter.
 *
 * Used when {@code type=auto} to select the best retrieval strategy without
 * requiring the caller to reason about search mechanics.
 */
@FunctionalInterface
public interface QueryClassifier {

    QueryClassification classify(String query);
}
