package com.pharos.cluster.internal;

/** A reader answered 409: it serves another generation than the one requested. */
public class GenerationMismatchException extends RuntimeException {
    private final long servedGeneration;

    public GenerationMismatchException(String node, long servedGeneration) {
        super("reader " + node + " serves generation " + servedGeneration);
        this.servedGeneration = servedGeneration;
    }

    public long servedGeneration() { return servedGeneration; }
}
