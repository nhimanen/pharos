package com.pharos.search;

import java.util.List;

/**
 * Wraps search results together with an optional timing trace.
 * Returned by {@link SearchEngine#searchWithTrace}.
 */
public record SearchResponse(List<SearchResult> results, SearchTrace trace) {}
