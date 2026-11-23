package com.pharos.embedding;

/** An embedding model failed to produce a vector (tokenizer/translator/inference error). */
public class EmbeddingException extends RuntimeException {
    public EmbeddingException(String message, Throwable cause) {
        super(message, cause);
    }
}
