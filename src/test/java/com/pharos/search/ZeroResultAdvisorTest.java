package com.pharos.search;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the static {@link ZeroResultAdvisor#editDistance(String, String)} utility.
 *
 * The full {@link ZeroResultAdvisor#advise} method requires a live Lucene index and is
 * therefore excluded from this unit-test suite.
 */
class ZeroResultAdvisorTest {

    // -------------------------------------------------------------------------
    // Identical strings
    // -------------------------------------------------------------------------

    @Nested
    class IdenticalStrings {

        @Test
        void emptyStrings_distanceIsZero() {
            assertEquals(0, ZeroResultAdvisor.editDistance("", ""));
        }

        @Test
        void singleCharSame_distanceIsZero() {
            assertEquals(0, ZeroResultAdvisor.editDistance("a", "a"));
        }

        @Test
        void wordSame_distanceIsZero() {
            assertEquals(0, ZeroResultAdvisor.editDistance("pool", "pool"));
        }

        @Test
        void longIdenticalStrings_distanceIsZero() {
            assertEquals(0, ZeroResultAdvisor.editDistance("connectionPool", "connectionPool"));
        }
    }

    // -------------------------------------------------------------------------
    // One-character differences
    // -------------------------------------------------------------------------

    @Nested
    class OneCharDifference {

        @Test
        void substitution_distanceIsOne() {
            assertEquals(1, ZeroResultAdvisor.editDistance("pool", "poll"));
        }

        @Test
        void insertion_distanceIsOne() {
            assertEquals(1, ZeroResultAdvisor.editDistance("car", "cart"));
        }

        @Test
        void deletion_distanceIsOne() {
            assertEquals(1, ZeroResultAdvisor.editDistance("cart", "car"));
        }
    }

    // -------------------------------------------------------------------------
    // Empty vs non-empty
    // -------------------------------------------------------------------------

    @Nested
    class EmptyVsNonEmpty {

        @Test
        void emptyA_distanceIsLengthOfB() {
            assertEquals(5, ZeroResultAdvisor.editDistance("", "hello"));
        }

        @Test
        void emptyB_distanceIsLengthOfA() {
            assertEquals(5, ZeroResultAdvisor.editDistance("hello", ""));
        }

        @Test
        void emptyA_singleCharB_distanceIsOne() {
            assertEquals(1, ZeroResultAdvisor.editDistance("", "x"));
        }
    }

    // -------------------------------------------------------------------------
    // Completely different strings
    // -------------------------------------------------------------------------

    @Nested
    class CompletelyDifferent {

        @Test
        void noCommonChars_shortStrings_distanceIsLengthOfLonger() {
            // "abc" vs "xyz": three substitutions → distance 3
            assertEquals(3, ZeroResultAdvisor.editDistance("abc", "xyz"));
        }

        @Test
        void differentLengths_distanceAtMostSumOfLengths() {
            int dist = ZeroResultAdvisor.editDistance("foo", "quux");
            assertTrue(dist <= 4, "Distance should be ≤ length of longer string: " + dist);
        }
    }

    // -------------------------------------------------------------------------
    // Symmetry property
    // -------------------------------------------------------------------------

    @Nested
    class Symmetry {

        @Test
        void editDistanceIsSymmetric_shortStrings() {
            int ab = ZeroResultAdvisor.editDistance("pool", "poll");
            int ba = ZeroResultAdvisor.editDistance("poll", "pool");
            assertEquals(ab, ba);
        }

        @Test
        void editDistanceIsSymmetric_differentLengths() {
            int ab = ZeroResultAdvisor.editDistance("ConnectionPool", "ConnectionPoll");
            int ba = ZeroResultAdvisor.editDistance("ConnectionPoll", "ConnectionPool");
            assertEquals(ab, ba);
        }
    }
}
