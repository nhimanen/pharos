package com.pharos.indexer;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DocumentMapper#encodeLineRanges} and
 * {@link DocumentMapper#decodeLineRanges} — the compact binary representation
 * used to store chunk line ranges alongside multi-vector embeddings.
 */
class LineRangesEncodingTest {

    // -------------------------------------------------------------------------
    // Single range round-trip
    // -------------------------------------------------------------------------

    @Nested
    class SingleRange {

        @Test
        void singleRange_decodesCorrectly() {
            int[][] ranges = {{10, 25}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(1, decoded.length);
            assertEquals(10, decoded[0][0]);
            assertEquals(25, decoded[0][1]);
        }

        @Test
        void singleRange_startLinePreserved() {
            int[][] ranges = {{100, 200}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(100, decoded[0][0]);
        }

        @Test
        void singleRange_endLinePreserved() {
            int[][] ranges = {{100, 200}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(200, decoded[0][1]);
        }

        @Test
        void singleRange_countIsOne() {
            int[][] ranges = {{5, 10}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(1, decoded.length);
        }
    }

    // -------------------------------------------------------------------------
    // Multiple ranges round-trip
    // -------------------------------------------------------------------------

    @Nested
    class MultipleRanges {

        @Test
        void threeRanges_countCorrect() {
            int[][] ranges = {{1, 10}, {11, 20}, {21, 30}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(3, decoded.length);
        }

        @Test
        void threeRanges_firstRangePreserved() {
            int[][] ranges = {{1, 10}, {11, 20}, {21, 30}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(1,  decoded[0][0]);
            assertEquals(10, decoded[0][1]);
        }

        @Test
        void threeRanges_middleRangePreserved() {
            int[][] ranges = {{1, 10}, {11, 20}, {21, 30}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(11, decoded[1][0]);
            assertEquals(20, decoded[1][1]);
        }

        @Test
        void threeRanges_lastRangePreserved() {
            int[][] ranges = {{1, 10}, {11, 20}, {21, 30}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(21, decoded[2][0]);
            assertEquals(30, decoded[2][1]);
        }

        @Test
        void largeLineNumbers_roundTrip() {
            int[][] ranges = {{9999, 10050}, {20000, 20100}};
            int[][] decoded = DocumentMapper.decodeLineRanges(DocumentMapper.encodeLineRanges(ranges));
            assertEquals(9999,  decoded[0][0]);
            assertEquals(10050, decoded[0][1]);
            assertEquals(20000, decoded[1][0]);
            assertEquals(20100, decoded[1][1]);
        }
    }

    // -------------------------------------------------------------------------
    // Empty array
    // -------------------------------------------------------------------------

    @Nested
    class EmptyArray {

        @Test
        void emptyArray_encodesAndDecodesToEmpty() {
            int[][] ranges = {};
            byte[] encoded = DocumentMapper.encodeLineRanges(ranges);
            int[][] decoded = DocumentMapper.decodeLineRanges(encoded);
            assertEquals(0, decoded.length);
        }

        @Test
        void emptyArray_encodedBytesHaveCorrectSize() {
            // Format: [N:int32] = 4 bytes for N=0 (no range entries follow)
            int[][] ranges = {};
            byte[] encoded = DocumentMapper.encodeLineRanges(ranges);
            assertEquals(4, encoded.length, "Empty range array should encode to exactly 4 bytes (count field)");
        }
    }

    // -------------------------------------------------------------------------
    // Null / malformed decode input
    // -------------------------------------------------------------------------

    @Nested
    class NullOrShortInput {

        @Test
        void nullInput_decodesToEmptyArray() {
            int[][] decoded = DocumentMapper.decodeLineRanges(null);
            assertNotNull(decoded);
            assertEquals(0, decoded.length);
        }

        @Test
        void tooShortInput_decodesToEmptyArray() {
            // Fewer than 4 bytes cannot hold the count field
            byte[] tooShort = {0, 0};
            int[][] decoded = DocumentMapper.decodeLineRanges(tooShort);
            assertNotNull(decoded);
            assertEquals(0, decoded.length);
        }
    }

    // -------------------------------------------------------------------------
    // Encode output size
    // -------------------------------------------------------------------------

    @Nested
    class EncodedSize {

        @Test
        void singleRange_encodedSizeIsCorrect() {
            // 4 (count) + 1 * 8 (start + end) = 12 bytes
            int[][] ranges = {{1, 5}};
            byte[] encoded = DocumentMapper.encodeLineRanges(ranges);
            assertEquals(12, encoded.length);
        }

        @Test
        void twoRanges_encodedSizeIsCorrect() {
            // 4 (count) + 2 * 8 = 20 bytes
            int[][] ranges = {{1, 5}, {6, 10}};
            byte[] encoded = DocumentMapper.encodeLineRanges(ranges);
            assertEquals(20, encoded.length);
        }
    }
}
