package com.pharos.search;

/**
 * Result of {@link QueryClassifier#classify(String)}.
 *
 * @param type    the resolved search pipeline type
 * @param docType optional Lucene docType filter to inject ("method", "class", or null = no filter)
 */
public record QueryClassification(SearchRequest.SearchType type, String docType) {

    public static QueryClassification of(SearchRequest.SearchType type) {
        return new QueryClassification(type, null);
    }

    public static QueryClassification of(SearchRequest.SearchType type, String docType) {
        return new QueryClassification(type, docType);
    }
}
