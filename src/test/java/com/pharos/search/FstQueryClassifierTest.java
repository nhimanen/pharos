package com.pharos.search;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class FstQueryClassifierTest {

    private static FstQueryClassifier classifier;

    @BeforeAll
    static void setUp() {
        classifier = new FstQueryClassifier();
    }

    // ── FST phrase matches ────────────────────────────────────────────────────

    @Test void behavioral_findCodeThat() {
        var r = classifier.classify("find code that prevents two writers");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.HYBRID);
        assertThat(r.docType()).isNull();
    }

    @Test void interface_withDocTypeFilter() {
        var r = classifier.classify("interface for collecting search results");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.HYBRID);
        assertThat(r.docType()).isEqualTo("class");
    }

    @Test void javadoc_returnsThe() {
        var r = classifier.classify("returns the number of deleted documents in a segment");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
        assertThat(r.intent()).isEqualTo("JAVADOC");
        assertThat(r.docType()).isNull(); // docType filter removed from JAVADOC — too narrow
    }

    @Test void javadoc_methodThat() {
        var r = classifier.classify("method that tracks top scoring documents");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
        assertThat(r.intent()).isEqualTo("JAVADOC");
        assertThat(r.docType()).isNull();
    }

    @Test void config_setMaximum() {
        var r = classifier.classify("set maximum merge segment size megabytes");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
        assertThat(r.docType()).isNull();
    }

    @Test void lifecycle_handleCorrupt() {
        var r = classifier.classify("handle corrupt index exception on startup");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
    }

    @Test void lifecycle_recoverFrom() {
        var r = classifier.classify("recover from merge exception and continue");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
    }

    // ── Heuristic fallback ────────────────────────────────────────────────────

    @Test void heuristic_singleToken() {
        assertThat(classifier.classify("TieredMergePolicy").type())
                .isEqualTo(SearchRequest.SearchType.KEYWORD);
    }

    @Test void heuristic_camelCaseInPhrase() {
        var r = classifier.classify("SolrCloud replica state transition");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
    }

    @Test void heuristic_acronymInPhrase() {
        var r = classifier.classify("HNSW graph neighbor candidate exploration");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
    }

    @Test void heuristic_technicalPhrase_noStopWords() {
        var r = classifier.classify("segment merge candidate selection");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
    }

    @Test void heuristic_naturalLanguage_withStopWords() {
        var r = classifier.classify("how do segments get merged in the background");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.HYBRID);
    }

    @Test void fqn_routesToKeyword() {
        var r = classifier.classify("com.pharos.search.SearchEngine#search(SearchRequest)");
        assertThat(r.type()).isEqualTo(SearchRequest.SearchType.KEYWORD);
    }

    @Test void noDocTypeLeakedForNonInterfaceQuery() {
        var r = classifier.classify("BooleanQuery");
        assertThat(r.docType()).isNull();
    }
}
