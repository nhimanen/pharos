package com.pharos.search;

/**
 * Classifies a raw query string into a {@link QueryClassification} that carries
 * the resolved pipeline type, an optional docType filter, and a named intent
 * label used by adaptive stages such as {@code UnifiedRetrievalStage}.
 *
 * <p>The router runs once at the top of a {@link com.pharos.search.pipeline.SearchPipeline}
 * and its result is attached to the {@link SearchRequest} passed to all downstream
 * stages — stages that do not care about the classification simply ignore it.
 */
@FunctionalInterface
public interface QueryRouter {

    QueryClassification classify(String query);
}
