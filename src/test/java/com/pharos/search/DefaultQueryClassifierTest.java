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
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("getUserById").type());
        }

        @Test
        void PascalCaseIdentifier_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("ConnectionPool").type());
        }

        @Test
        void shortMethodName_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("findById").type());
        }
    }

    // -------------------------------------------------------------------------
    // Rule 2: contains # → KEYWORD (FQN format)
    // -------------------------------------------------------------------------

    @Nested
    class ContainsHash {

        @Test
        void fqnWithHash_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("MyClass#myMethod").type());
        }

        @Test
        void fqnWithHashAndSpaces_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("com.example.MyClass#myMethod").type());
        }
    }

    // -------------------------------------------------------------------------
    // Rule 3: starts with @ → KEYWORD
    // -------------------------------------------------------------------------

    @Nested
    class StartsWithAt {

        @Test
        void atTransactional_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD, classifier.classify("@Transactional").type());
        }

        @Test
        void atAnnotationWithSpace_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("@Override methods").type());
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
                    classifier.classify("throws IOException").type());
        }

        @Test
        void implementsClause_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("implements Runnable").type());
        }

        @Test
        void extendsClause_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("extends AbstractService").type());
        }
    }

    // -------------------------------------------------------------------------
    // Rule 5: stop word → HYBRID
    // -------------------------------------------------------------------------

    @Nested
    class StopWords {

        @Test
        void queryWithThe_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("find the user").type());
        }

        @Test
        void queryWithHow_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("how to authenticate").type());
        }

        @Test
        void commandWordGet_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("get all users").type());
        }

        @Test
        void commandWordFind_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("find authentication").type());
        }

        @Test
        void commandWordList_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("list endpoints").type());
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
                    classifier.classify("connection pool initialization logic").type());
        }

        @Test
        void fiveTokenPhrase_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID,
                    classifier.classify("retry logic with exponential backoff").type());
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
                    classifier.classify("ConnectionPool Manager").type());
        }

        @Test
        void twoTokensMixedCase_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("connection pool").type());
        }

        @Test
        void twoTokensAllLowercase_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("retry logic").type());
        }

        @Test
        void threeTokensAllUppercase_isKeyword() {
            assertEquals(SearchRequest.SearchType.KEYWORD,
                    classifier.classify("User Session Manager").type());
        }
    }

    // -------------------------------------------------------------------------
    // Edge cases: blank / null → HYBRID
    // -------------------------------------------------------------------------

    @Nested
    class BlankAndNull {

        @Test
        void nullQuery_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify(null).type());
        }

        @Test
        void emptyString_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("").type());
        }

        @Test
        void blankWhitespace_isHybrid() {
            assertEquals(SearchRequest.SearchType.HYBRID, classifier.classify("   ").type());
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
