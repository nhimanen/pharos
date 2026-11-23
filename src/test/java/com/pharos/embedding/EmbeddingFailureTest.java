package com.pharos.embedding;

import ai.djl.translate.TranslateException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EmbeddingFailureTest {
    @Test
    void translatorFailureThrowsInsteadOfNull() {
        EmbeddingException e = assertThrows(EmbeddingException.class, () ->
                DjlEmbeddingProvider.predictOrThrow("granite", () -> { throw new TranslateException("bad tokenizer"); }));
        assertTrue(e.getMessage().contains("granite"));
        assertTrue(e.getMessage().contains("bad tokenizer"));
        assertInstanceOf(TranslateException.class, e.getCause());
    }

    @Test
    void successPassesThrough() {
        assertArrayEquals(new float[]{1f}, DjlEmbeddingProvider.predictOrThrow("m", () -> new float[]{1f}));
    }
}
