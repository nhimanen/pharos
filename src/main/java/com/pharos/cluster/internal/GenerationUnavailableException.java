package com.pharos.cluster.internal;

/**
 * Ruling P6-R21: a request for a shard as an older spec version of its project names a generation this reader
 * no longer answers (its retained previous generation is gone). Answered 409 "generation mismatch" with the
 * generation it serves, as for a requested generation it does not serve.
 */
public class GenerationUnavailableException extends RuntimeException {
    private final long servedGeneration;

    public GenerationUnavailableException(String message, long servedGeneration) {
        super(message);
        this.servedGeneration = servedGeneration;
    }

    public long servedGeneration() {
        return servedGeneration;
    }
}
