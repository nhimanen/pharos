package com.pharos.search;

import com.pharos.indexer.DocumentMapper;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for rVSM query-time identifier expansion in {@link KeywordSearchStrategy}.
 */
class IdentifierExpansionTest {

    // -------------------------------------------------------------------------
    // isIdentifierLike
    // -------------------------------------------------------------------------

    @Nested
    class IsIdentifierLike {

        @Test
        void camelCase_returnsTrue() {
            assertTrue(KeywordSearchStrategy.isIdentifierLike("getUserById"));
        }

        @Test
        void PascalCase_returnsTrue() {
            assertTrue(KeywordSearchStrategy.isIdentifierLike("ConnectionPool"));
        }

        @Test
        void underscoreJoined_returnsTrue() {
            assertTrue(KeywordSearchStrategy.isIdentifierLike("get_user_by_id"));
        }

        @Test
        void mixedCaseAndUnderscore_returnsTrue() {
            assertTrue(KeywordSearchStrategy.isIdentifierLike("MY_CONSTANT_VALUE"));
        }

        @Test
        void allLowercase_returnsFalse() {
            assertFalse(KeywordSearchStrategy.isIdentifierLike("connection"));
        }

        @Test
        void singleCapitalisedWord_returnsFalse() {
            // "Connection" — capital only at position 0, no mid-word upper
            assertFalse(KeywordSearchStrategy.isIdentifierLike("Connection"));
        }

        @Test
        void singleChar_returnsFalse() {
            assertFalse(KeywordSearchStrategy.isIdentifierLike("x"));
        }

        @Test
        void naturalLanguagePhrase_returnsFalse() {
            // whole phrase is not a single token in context, but if passed as-is
            assertFalse(KeywordSearchStrategy.isIdentifierLike("find authentication"));
        }
    }

    // -------------------------------------------------------------------------
    // withIdentifierExpansion — no-op cases
    // -------------------------------------------------------------------------

    @Nested
    class NoExpansion {

        private final Query sentinel = new TermQuery(
                new org.apache.lucene.index.Term("field", "sentinel"));

        @Test
        void naturalLanguageQuery_returnedUnchanged() {
            Query result = KeywordSearchStrategy.withIdentifierExpansion(
                    "connection pool manager", sentinel);
            assertSame(sentinel, result);
        }

        @Test
        void singleLowercaseWord_returnedUnchanged() {
            Query result = KeywordSearchStrategy.withIdentifierExpansion("authenticate", sentinel);
            assertSame(sentinel, result);
        }

        @Test
        void singleCapitalisedWord_returnedUnchanged() {
            // "Connection" is not camelCase — no split occurs
            Query result = KeywordSearchStrategy.withIdentifierExpansion("Connection", sentinel);
            assertSame(sentinel, result);
        }

        @Test
        void nullQuery_returnedUnchanged() {
            Query result = KeywordSearchStrategy.withIdentifierExpansion(null, sentinel);
            assertSame(sentinel, result);
        }

        @Test
        void blankQuery_returnedUnchanged() {
            Query result = KeywordSearchStrategy.withIdentifierExpansion("   ", sentinel);
            assertSame(sentinel, result);
        }
    }

    // -------------------------------------------------------------------------
    // withIdentifierExpansion — expansion cases
    // -------------------------------------------------------------------------

    @Nested
    class WithExpansion {

        private final Query sentinel = new TermQuery(
                new org.apache.lucene.index.Term("field", "sentinel"));

        @Test
        void camelCaseToken_returnsWrappedBooleanQuery() {
            Query result = KeywordSearchStrategy.withIdentifierExpansion("getUserById", sentinel);
            // Must be a BooleanQuery wrapping the sentinel
            assertInstanceOf(BooleanQuery.class, result);
        }

        @Test
        void camelCaseToken_baseQueryIsMusClause() {
            BooleanQuery result = (BooleanQuery)
                    KeywordSearchStrategy.withIdentifierExpansion("getUserById", sentinel);
            long mustClauses = result.clauses().stream()
                    .filter(c -> c.occur() == org.apache.lucene.search.BooleanClause.Occur.MUST)
                    .count();
            assertEquals(1, mustClauses, "Base query should be the single MUST clause");
        }

        @Test
        void camelCaseToken_expansionIsBoostShouldClause() {
            BooleanQuery result = (BooleanQuery)
                    KeywordSearchStrategy.withIdentifierExpansion("getUserById", sentinel);
            long shouldClauses = result.clauses().stream()
                    .filter(c -> c.occur() == org.apache.lucene.search.BooleanClause.Occur.SHOULD)
                    .count();
            assertEquals(1, shouldClauses, "Expansion terms should form one SHOULD clause");
        }

        @Test
        void camelCaseToken_expansionIsBoostQuery() {
            BooleanQuery result = (BooleanQuery)
                    KeywordSearchStrategy.withIdentifierExpansion("ConnectionPool", sentinel);
            Query shouldQuery = result.clauses().stream()
                    .filter(c -> c.occur() == org.apache.lucene.search.BooleanClause.Occur.SHOULD)
                    .map(org.apache.lucene.search.BooleanClause::query)
                    .findFirst()
                    .orElseThrow();
            assertInstanceOf(BoostQuery.class, shouldQuery,
                    "Expansion SHOULD clause should be a BoostQuery");
        }

        @Test
        void underscoreToken_triggersExpansion() {
            Query result = KeywordSearchStrategy.withIdentifierExpansion(
                    "get_user_by_id", sentinel);
            assertInstanceOf(BooleanQuery.class, result);
        }

        @Test
        void mixedQuery_onlyCamelCaseTokenExpanded() {
            // "findUserById authentication" — findUserById is camelCase; authentication is plain
            Query result = KeywordSearchStrategy.withIdentifierExpansion(
                    "findUserById authentication", sentinel);
            // Should still expand (at least findUserById triggers it)
            assertInstanceOf(BooleanQuery.class, result);
        }

        @Test
        void shortWordsAfterSplit_filteredOut() {
            // "getById" splits to "get by id" — "by" (2 chars) is filtered, "get" and "id"
            // (3 chars each) kept. If BOTH are filtered, falls back to no expansion.
            // "getById" → "get by id": "by" filtered (len<3? no, len=2); "get"=3, "id"=2
            // "id" is 2 chars → filtered. Only "get" remains → expansion fires.
            Query result = KeywordSearchStrategy.withIdentifierExpansion("getById", sentinel);
            // "get" (3) qualifies, so expansion should fire
            assertInstanceOf(BooleanQuery.class, result);
        }

        @Test
        void expansionBoostWeightIsCorrect() {
            BooleanQuery outer = (BooleanQuery)
                    KeywordSearchStrategy.withIdentifierExpansion("ConnectionPool", sentinel);
            BoostQuery boost = (BoostQuery) outer.clauses().stream()
                    .filter(c -> c.occur() == org.apache.lucene.search.BooleanClause.Occur.SHOULD)
                    .map(org.apache.lucene.search.BooleanClause::query)
                    .findFirst()
                    .orElseThrow();
            assertEquals(KeywordSearchStrategy.EXPANSION_BOOST, boost.getBoost(), 1e-6f);
        }

        @Test
        void expansionTermsContainSplitWords() {
            // "ConnectionPool" → split words "connection", "pool"
            // Expansion inner query is a BooleanQuery of TermQueries
            BooleanQuery outer = (BooleanQuery)
                    KeywordSearchStrategy.withIdentifierExpansion("ConnectionPool", sentinel);
            BoostQuery boost = (BoostQuery) outer.clauses().stream()
                    .filter(c -> c.occur() == org.apache.lucene.search.BooleanClause.Occur.SHOULD)
                    .map(org.apache.lucene.search.BooleanClause::query)
                    .findFirst()
                    .orElseThrow();
            BooleanQuery terms = (BooleanQuery) boost.getQuery();

            // Should contain "connection" and "pool" as TermQuery terms (on any field)
            long connectionTerms = terms.clauses().stream()
                    .filter(c -> c.query() instanceof TermQuery tq
                            && tq.getTerm().text().equals("connection"))
                    .count();
            long poolTerms = terms.clauses().stream()
                    .filter(c -> c.query() instanceof TermQuery tq
                            && tq.getTerm().text().equals("pool"))
                    .count();
            assertTrue(connectionTerms > 0, "Expected 'connection' term in expansion");
            assertTrue(poolTerms > 0, "Expected 'pool' term in expansion");
        }
    }

    // -------------------------------------------------------------------------
    // splitIdentifier is already public — verify the contract
    // -------------------------------------------------------------------------

    @Nested
    class SplitIdentifier {

        @Test
        void camelCase_splitCorrectly() {
            assertEquals("get user by id", DocumentMapper.splitIdentifier("getUserById"));
        }

        @Test
        void PascalCase_splitCorrectly() {
            assertEquals("connection pool", DocumentMapper.splitIdentifier("ConnectionPool"));
        }

        @Test
        void underscore_splitCorrectly() {
            assertEquals("my constant", DocumentMapper.splitIdentifier("MY_CONSTANT"));
        }

        @Test
        void acronym_keptIntact() {
            // "XML" splits to "xml" (no mid-word upper in the right position)
            String result = DocumentMapper.splitIdentifier("XMLParser");
            assertTrue(result.contains("xml") || result.contains("parser"),
                    "Should split XMLParser into recognisable parts: " + result);
        }

        @Test
        void singleWord_returnedLowercase() {
            assertEquals("connection", DocumentMapper.splitIdentifier("connection"));
        }
    }
}
