package com.pharos.search;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DefaultQueryClassifier#classify(String)} and
 * {@link SearchRequest.SearchType#from(String)}.
 */
class DefaultQueryClassifierTest {

    private final DefaultQueryClassifier classifier = new DefaultQueryClassifier();

    // -------------------------------------------------------------------------
    // Rule 1: no spaces → KEYWORD
    // -------------------------------------------------------------------------

    @Nested
    class NoSpaces {

        @Test
        void singleCamelCaseIdentifier_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("getUserById"));
        }

        @Test
        void PascalCaseIdentifier_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("ConnectionPool"));
        }

        @Test
        void shortMethodName_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("findById"));
        }
    }

    // -------------------------------------------------------------------------
    // Rule 2: contains # → KEYWORD (FQN format)
    // -------------------------------------------------------------------------

    @Nested
    class ContainsHash {

        @Test
        void fqnWithHash_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("MyClass#myMethod"));
        }

        @Test
        void fqnWithHashAndSpaces_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("com.example.MyClass#myMethod"));
        }
    }

    // -------------------------------------------------------------------------
    // Rule 3: starts with @ → KEYWORD
    // -------------------------------------------------------------------------

    @Nested
    class StartsWithAt {

        @Test
        void atTransactional_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("@Transactional"));
        }

        @Test
        void atAnnotationWithSpace_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("@Override methods"));
        }
    }

    // -------------------------------------------------------------------------
    // Rule 4: structural Java keyword → KEYWORD
    // -------------------------------------------------------------------------

    @Nested
    class StructuralKeywords {

        @Test
        void throwsClause_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("throws IOException"));
        }

        @Test
        void implementsClause_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("implements Runnable"));
        }

        @Test
        void extendsClause_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("extends AbstractService"));
        }
    }

    // -------------------------------------------------------------------------
    // Rule 5: stop word → HYBRID
    // -------------------------------------------------------------------------

    @Nested
    class StopWords {

        @Test
        void queryWithThe_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("find the user"));
        }

        @Test
        void queryWithHow_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("how to authenticate"));
        }

        @Test
        void commandWordGet_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("get all users"));
        }

        @Test
        void commandWordFind_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("find authentication"));
        }

        @Test
        void commandWordList_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("list endpoints"));
        }
    }

    // -------------------------------------------------------------------------
    // Rule 6: 4+ tokens → HYBRID
    // -------------------------------------------------------------------------

    @Nested
    class FourOrMoreTokens {

        @Test
        void fourTokenNaturalLanguage_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID,
                    classifier.classify("connection pool initialization logic"));
        }

        @Test
        void fiveTokenPhrase_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID,
                    classifier.classify("retry logic with exponential backoff"));
        }
    }

    // -------------------------------------------------------------------------
    // Rule 7: 2-3 tokens, all tokens have uppercase → KEYWORD
    // -------------------------------------------------------------------------

    @Nested
    class MultiPartIdentifiers {

        @Test
        void twoTokensAllUppercase_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("ConnectionPool Manager"));
        }

        @Test
        void twoTokensMixedCase_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("connection pool"));
        }

        @Test
        void twoTokensAllLowercase_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("retry logic"));
        }

        @Test
        void threeTokensAllUppercase_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("User Session Manager"));
        }
    }

    // -------------------------------------------------------------------------
    // Edge cases: blank / null → HYBRID
    // -------------------------------------------------------------------------

    @Nested
    class BlankAndNull {

        @Test
        void nullQuery_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify(null));
        }

        @Test
        void emptyString_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify(""));
        }

        @Test
        void blankWhitespace_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("   "));
        }
    }

    // -------------------------------------------------------------------------
    // SearchType.from() factory method
    // -------------------------------------------------------------------------

    @Nested
    class SearchTypeFrom {

        @Test
        void fromAuto_returnsAuto() {
            assertEquals(SearchRequest.SearchType.AUTO, SearchRequest.SearchType.from("auto"));
        }

        @Test
        void fromUnified_returnsUnified() {
            assertEquals(SearchRequest.SearchType.UNIFIED, SearchRequest.SearchType.from("unified"));
        }

        @Test
        void fromKeyword_returnsKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, SearchRequest.SearchType.from("keyword"));
        }

        @Test
        void fromVector_returnsVector() {
            assertEquals(SearchRequest.SearchType.VECTOR, SearchRequest.SearchType.from("vector"));
        }

        @Test
        void fromHybrid_returnsHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, SearchRequest.SearchType.from("hybrid"));
        }

        @Test
        void unknownString_defaultsToAuto() {
            assertEquals(SearchRequest.SearchType.AUTO, SearchRequest.SearchType.from("anything_unknown"));
        }

        @Test
        void unknownUppercase_defaultsToAuto() {
            assertEquals(SearchRequest.SearchType.AUTO, SearchRequest.SearchType.from("BOGUS"));
        }

        @Test
        void hybridRerankedDash_returnsHybridReranked() {
            assertEquals(SearchRequest.SearchType.HYBRID_RERANKED,
                    SearchRequest.SearchType.from("hybrid-reranked"));
        }

        @Test
        void hybridDiverse_returnsHybridDiverse() {
            assertEquals(SearchRequest.SearchType.HYBRID_DIVERSE,
                    SearchRequest.SearchType.from("hybrid-diverse"));
        }
    }
}
