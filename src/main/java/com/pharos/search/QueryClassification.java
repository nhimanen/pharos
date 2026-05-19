package com.pharos.search;

/**
 * Result of {@link QueryClassifier#classify(String)}.
 *
 * @param type    the resolved search pipeline type
 * @param docType optional Lucene docType filter to inject ("method", "class", or null = no filter)
 * @param intent  the detected query intent label — used by {@code UnifiedRetrievalStage} to
 *                select the appropriate BM25/vector balance. Values:
 *                {@code BEHAVIORAL}, {@code INTERFACE}, {@code JAVADOC}, {@code CONFIG},
 *                {@code LIFECYCLE}, {@code KEYWORD}, {@code KEYWORD_TECHNICAL}, {@code HYBRID}.
 */
public record QueryClassification(SearchRequest.SearchType type, String docType, String intent) {

    public static QueryClassification of(SearchRequest.SearchType type) {
        return new QueryClassification(type, null, type == SearchRequest.SearchType.KEYWORD ? "KEYWORD" : "HYBRID");
    }

    public static QueryClassification of(SearchRequest.SearchType type, String docType) {
        return new QueryClassification(type, docType, type == SearchRequest.SearchType.KEYWORD ? "KEYWORD" : "HYBRID");
    }

    public static QueryClassification of(SearchRequest.SearchType type, String docType, String intent) {
        return new QueryClassification(type, docType, intent);
    }
}
