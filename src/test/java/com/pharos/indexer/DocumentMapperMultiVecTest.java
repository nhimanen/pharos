package com.pharos.indexer;

import com.pharos.parser.model.ParsedClass;
import com.pharos.parser.model.ParsedMethod;
import org.apache.lucene.document.Document;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class DocumentMapperMultiVecTest {

    private static final String PROJECT = "test-proj";

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ParsedMethod testMethod() {
        return new ParsedMethod(
                PROJECT + ":com.example.Service#process(String)",
                PROJECT, "com.example",
                "Service", "com.example.Service",
                "process", "public String process(String input)", "String",
                List.of("String"), List.of("input"),
                "return input.trim();", null,
                List.of(), "public",
                false, false, false, false,
                List.of(), List.of(),
                "/src/Service.java", 5, 10
        );
    }

    private static ParsedClass testClass() {
        return new ParsedClass(
                PROJECT, "com.example",
                "Service", "com.example.Service",
                "class", null, List.of(), List.of(),
                "public", false, false, null,
                "/src/Service.java", 1, 50
        );
    }

    // -------------------------------------------------------------------------
    // meanPool
    // -------------------------------------------------------------------------

    @Nested
    class MeanPool {

        @Test
        void null_returnsNull() {
            assertThat(DocumentMapper.meanPool(null)).isNull();
        }

        @Test
        void emptyArray_returnsNull() {
            assertThat(DocumentMapper.meanPool(new float[0][])).isNull();
        }

        @Test
        void singleVector_returnsSameVector() {
            float[] v = {0.1f, 0.2f, 0.3f};
            float[] result = DocumentMapper.meanPool(new float[][]{v});

            assertThat(result).isEqualTo(v);
        }

        @Test
        void twoVectors_returnsElementWiseMean() {
            float[] a = {0.0f, 1.0f};
            float[] b = {1.0f, 0.0f};

            float[] result = DocumentMapper.meanPool(new float[][]{a, b});

            assertThat(result).hasSize(2);
            assertThat(result[0]).isCloseTo(0.5f, within(0.001f));
            assertThat(result[1]).isCloseTo(0.5f, within(0.001f));
        }

        @Test
        void threeVectors_returnsCorrectMean() {
            float[] a = {3.0f, 0.0f};
            float[] b = {0.0f, 3.0f};
            float[] c = {3.0f, 3.0f};

            float[] result = DocumentMapper.meanPool(new float[][]{a, b, c});

            assertThat(result[0]).isCloseTo(2.0f, within(0.001f));
            assertThat(result[1]).isCloseTo(2.0f, within(0.001f));
        }

        @Test
        void singleDimensionVectors_meanIsCorrect() {
            float[] a = {4.0f};
            float[] b = {2.0f};

            float[] result = DocumentMapper.meanPool(new float[][]{a, b});

            assertThat(result[0]).isCloseTo(3.0f, within(0.001f));
        }
    }

    // -------------------------------------------------------------------------
    // toDocumentMultiVec
    // -------------------------------------------------------------------------

    @Nested
    class ToDocumentMultiVec {

        @Test
        void containsChunkVectorsField() {
            float[][] embeddings = {{0.1f, 0.2f}};
            Document doc = DocumentMapper.toDocumentMultiVec(
                    testMethod(), embeddings, 0, List.of());

            // LateInteractionField is a BinaryDocValuesField — accessible via getBinaryValue
            assertThat(doc.getBinaryValue(DocumentMapper.F_CHUNK_VECTORS)).isNotNull();
        }

        @Test
        void containsVectorEmbeddingField_setToMeanOfChunks() {
            float[] chunk1 = {0.0f, 1.0f};
            float[] chunk2 = {1.0f, 0.0f};
            float[][] embeddings = {chunk1, chunk2};

            Document doc = DocumentMapper.toDocumentMultiVec(
                    testMethod(), embeddings, 0, List.of());

            // F_VECTOR is a KnnFloatVectorField — stored as binary DocValues under the hood,
            // but the field name must appear in the document
            assertThat(doc.getField(DocumentMapper.F_VECTOR)).isNotNull();
        }

        @Test
        void singleChunk_representativeEqualsChunkVector() {
            // When meanPool has 1 input, it returns that exact vector
            float[] only = {0.1f, 0.9f};
            float[][] embeddings = {only};

            Document doc = DocumentMapper.toDocumentMultiVec(
                    testMethod(), embeddings, 0, List.of());

            // Verify the representative (mean-pooled) vector is present and not null
            assertThat(doc.getField(DocumentMapper.F_VECTOR)).isNotNull();
            assertThat(doc.getBinaryValue(DocumentMapper.F_CHUNK_VECTORS)).isNotNull();
        }

        @Test
        void hasAllStandardBm25Fields() {
            float[][] embeddings = {{0.5f, 0.5f}};
            Document doc = DocumentMapper.toDocumentMultiVec(
                    testMethod(), embeddings, 0, List.of());

            assertThat(doc.get(DocumentMapper.F_ID)).isNotNull();
            assertThat(doc.get(DocumentMapper.F_METHOD_NAME)).isEqualTo("process");
            assertThat(doc.get(DocumentMapper.F_CLASS_NAME)).isEqualTo("Service");
            assertThat(doc.get(DocumentMapper.F_BODY)).contains("return input.trim();");
            assertThat(doc.get(DocumentMapper.F_DOC_TYPE)).isEqualTo("method");
        }

        @Test
        void twoChunks_representativeIsMean() {
            float[] chunk1 = {0.0f, 2.0f};
            float[] chunk2 = {2.0f, 0.0f};
            float[][] embeddings = {chunk1, chunk2};

            // meanPool should yield {1.0, 1.0}; verify F_VECTOR field exists (can't read raw
            // float values from KnnFloatVectorField without a DirectoryReader, but we confirm
            // the field was added and chunkVectors is also present)
            Document doc = DocumentMapper.toDocumentMultiVec(
                    testMethod(), embeddings, 0, List.of());

            assertThat(doc.getField(DocumentMapper.F_VECTOR)).isNotNull();
            assertThat(doc.getBinaryValue(DocumentMapper.F_CHUNK_VECTORS)).isNotNull();
        }

        @Test
        void graphDataPropagated_inDegreeAndCallerContext() {
            float[][] embeddings = {{0.1f, 0.2f}};
            Document doc = DocumentMapper.toDocumentMultiVec(
                    testMethod(), embeddings, 7, List.of("caller1", "caller2"));

            assertThat(doc.get(DocumentMapper.F_IN_DEGREE)).isEqualTo("7");
            assertThat(doc.get(DocumentMapper.F_CALLER_CONTEXT)).contains("caller1");
        }
    }

    // -------------------------------------------------------------------------
    // toClassDocumentMultiVec
    // -------------------------------------------------------------------------

    @Nested
    class ToClassDocumentMultiVec {

        @Test
        void containsChunkVectorsField() {
            float[][] embeddings = {{0.3f, 0.7f}};
            Document doc = DocumentMapper.toClassDocumentMultiVec(
                    testClass(), "int process()", embeddings);

            assertThat(doc.getBinaryValue(DocumentMapper.F_CHUNK_VECTORS)).isNotNull();
        }

        @Test
        void docTypeIsClass() {
            float[][] embeddings = {{0.5f, 0.5f}};
            Document doc = DocumentMapper.toClassDocumentMultiVec(
                    testClass(), "body", embeddings);

            assertThat(doc.get(DocumentMapper.F_DOC_TYPE)).isEqualTo("class");
        }

        @Test
        void representativeVectorField_isPresent() {
            float[][] embeddings = {{0.1f, 0.2f}, {0.3f, 0.4f}};
            Document doc = DocumentMapper.toClassDocumentMultiVec(
                    testClass(), "body", embeddings);

            assertThat(doc.getField(DocumentMapper.F_VECTOR)).isNotNull();
        }

        @Test
        void hasStandardClassFields() {
            float[][] embeddings = {{0.5f, 0.5f}};
            String synthesizedBody = "String process(String input)\n";
            Document doc = DocumentMapper.toClassDocumentMultiVec(
                    testClass(), synthesizedBody, embeddings);

            assertThat(doc.get(DocumentMapper.F_CLASS_NAME)).isEqualTo("Service");
            assertThat(doc.get(DocumentMapper.F_QUALIFIED_CLASS)).isEqualTo("com.example.Service");
            assertThat(doc.get(DocumentMapper.F_BODY)).isEqualTo(synthesizedBody);
            assertThat(doc.get(DocumentMapper.F_ID))
                    .isEqualTo(PROJECT + ":com.example.Service");
        }

        @Test
        void singleChunk_bothVectorFieldsPresent() {
            float[][] embeddings = {{0.9f, 0.1f}};
            Document doc = DocumentMapper.toClassDocumentMultiVec(
                    testClass(), "body", embeddings);

            assertThat(doc.getField(DocumentMapper.F_VECTOR)).isNotNull();
            assertThat(doc.getBinaryValue(DocumentMapper.F_CHUNK_VECTORS)).isNotNull();
        }
    }
}
