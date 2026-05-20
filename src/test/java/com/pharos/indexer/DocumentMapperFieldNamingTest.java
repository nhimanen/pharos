package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Locks in the modelId → Lucene field-name mapping. Pharos derives every
 * Lucene field name from the {@code modelId} string using a deterministic
 * sanitization rule. Anything that breaks this mapping silently writes vectors
 * into a different field than search reads from, so a unit test pinning the
 * exact transform is worth its weight.
 */
class DocumentMapperFieldNamingTest {

    @Test
    void vectorFieldName_simpleId() {
        assertThat(DocumentMapper.vectorFieldName("jina-code-v2"))
                .isEqualTo("vec.jina-code-v2");
    }

    @Test
    void vectorFieldName_uppercaseLowercased() {
        assertThat(DocumentMapper.vectorFieldName("Qwen3-Embedding-4B"))
                .isEqualTo("vec.qwen3-embedding-4b");
    }

    @Test
    void vectorFieldName_replacesIllegalCharsWithUnderscore() {
        assertThat(DocumentMapper.vectorFieldName("Qwen/Qwen3-Embedding-4B@1024"))
                .isEqualTo("vec.qwen_qwen3-embedding-4b_1024");
    }

    @Test
    void vectorFieldName_collapsesRunsOfUnderscore() {
        assertThat(DocumentMapper.vectorFieldName("foo!!!@#$bar"))
                .isEqualTo("vec.foo_bar");
    }

    @Test
    void vectorFieldName_cappedAt96CharsAfterPrefix() {
        // Sanitized portion (everything after "vec.") capped at 96 chars.
        String longId = "a".repeat(200);
        String fieldName = DocumentMapper.vectorFieldName(longId);
        assertThat(fieldName).startsWith("vec.");
        // 96-char cap is applied to the sanitized payload BEFORE the prefix is added.
        assertThat(fieldName.substring("vec.".length())).hasSize(96);
    }

    @Test
    void vectorFieldName_isDeterministic() {
        String id = "Qwen3-Embedding-4B@1024";
        assertThat(DocumentMapper.vectorFieldName(id))
                .isEqualTo(DocumentMapper.vectorFieldName(id));
    }

    @Test
    void vectorFieldName_throwsOnBlankInput() {
        assertThatThrownBy(() -> DocumentMapper.vectorFieldName(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DocumentMapper.vectorFieldName(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DocumentMapper.vectorFieldName("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void chunkVectorFieldName_usesChunkPrefix() {
        assertThat(DocumentMapper.chunkVectorFieldName("jina-code-v2"))
                .isEqualTo("chunkVec.jina-code-v2");
    }

    @Test
    void vectorFieldFor_legacyId_returnsLegacyFieldName() {
        // LEGACY_MODEL_ID → "vectorEmbedding", the old single-vector field name.
        assertThat(DocumentMapper.vectorFieldFor(IndexConfig.LEGACY_MODEL_ID))
                .isEqualTo(DocumentMapper.F_VECTOR_LEGACY)
                .isEqualTo("vectorEmbedding");
    }

    @Test
    void vectorFieldFor_nonLegacyId_returnsPerModelField() {
        assertThat(DocumentMapper.vectorFieldFor("qwen3-emb-1024"))
                .isEqualTo("vec.qwen3-emb-1024");
    }

    @Test
    void chunkVectorFieldFor_legacyId_returnsLegacyField() {
        assertThat(DocumentMapper.chunkVectorFieldFor(IndexConfig.LEGACY_MODEL_ID))
                .isEqualTo("chunkVectors");
    }

    @Test
    void chunkVectorFieldFor_nonLegacyId_returnsPerModelField() {
        assertThat(DocumentMapper.chunkVectorFieldFor("qwen3-emb-1024"))
                .isEqualTo("chunkVec.qwen3-emb-1024");
    }
}
