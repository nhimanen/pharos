package com.pharos.search;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the private static helpers in {@link SearchEngine}:
 * {@code queryHasCamelCase(String)} and {@code adaptiveOversample(SearchType, String)}.
 *
 * Both methods are {@code private static}, so reflection is used to invoke them directly.
 * The logic is pure and has no side effects, making reflection-based testing appropriate here.
 */
class AdaptiveOversampleTest {

    // -------------------------------------------------------------------------
    // queryHasCamelCase
    // -------------------------------------------------------------------------

    @Nested
    class QueryHasCamelCase {

        private static boolean hasCamelCase(String query) throws Exception {
            Method m = SearchEngine.class.getDeclaredMethod("queryHasCamelCase", String.class);
            m.setAccessible(true);
            return (boolean) m.invoke(null, query);
        }

        @Test
        void queryWithUppercase_returnsTrue() throws Exception {
            assertTrue(hasCamelCase("ConnectionPool"));
        }

        @Test
        void camelCaseMethodName_returnsTrue() throws Exception {
            assertTrue(hasCamelCase("getUserById"));
        }

        @Test
        void allLowercase_returnsFalse() throws Exception {
            assertFalse(hasCamelCase("connection pool"));
        }

        @Test
        void nullQuery_returnsFalse() throws Exception {
            assertFalse(hasCamelCase(null));
        }

        @Test
        void emptyString_returnsFalse() throws Exception {
            assertFalse(hasCamelCase(""));
        }

        @Test
        void singleUppercaseLetter_returnsTrue() throws Exception {
            assertTrue(hasCamelCase("A"));
        }

        @Test
        void naturalLanguageNoUppercase_returnsFalse() throws Exception {
            assertFalse(hasCamelCase("find authentication token"));
        }
    }

    // -------------------------------------------------------------------------
    // adaptiveOversample
    // -------------------------------------------------------------------------

    @Nested
    class AdaptiveOversample {

        private static int oversample(SearchRequest.SearchType type, String query) throws Exception {
            Method m = SearchEngine.class.getDeclaredMethod(
                    "adaptiveOversample", SearchRequest.SearchType.class, String.class);
            m.setAccessible(true);
            return (int) m.invoke(null, type, query);
        }

        @Test
        void keywordType_returnsOne() throws Exception {
            assertEquals(1, oversample(SearchRequest.SearchType.KEYWORD, "getUserById"));
        }

        @Test
        void hybridWithCamelCase_returnsOne() throws Exception {
            // CamelCase identifier → BM25 precision is high, no extra vector candidates
            assertEquals(1, oversample(SearchRequest.SearchType.HYBRID, "ConnectionPool"));
        }

        @Test
        void hybridWithNaturalLanguage_returnsThree() throws Exception {
            // Natural-language query → semantic recall matters, use 3× pool
            assertEquals(3, oversample(SearchRequest.SearchType.HYBRID, "connection pool initialization logic"));
        }

        @Test
        void vectorType_returnsZero() throws Exception {
            // VECTOR manages its own oversampling internally
            assertEquals(0, oversample(SearchRequest.SearchType.VECTOR, "getUserById"));
        }

        @Test
        void unifiedType_returnsZero() throws Exception {
            assertEquals(0, oversample(SearchRequest.SearchType.UNIFIED, "some query"));
        }

        @Test
        void hybridReranked_returnsZero() throws Exception {
            assertEquals(0, oversample(SearchRequest.SearchType.HYBRID_RERANKED, "some query"));
        }

        @Test
        void hybridDiverse_returnsZero() throws Exception {
            assertEquals(0, oversample(SearchRequest.SearchType.HYBRID_DIVERSE, "some query"));
        }
    }
}
