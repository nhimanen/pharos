package com.pharos.cluster.internal;

/** A reader answered 409: it serves another generation than the one requested. */
public class GenerationMismatchException extends RuntimeException {
    private final long servedGeneration;
    private final String node;

    public GenerationMismatchException(String node, long servedGeneration) {
        super("reader " + node + " serves generation " + servedGeneration);
        this.servedGeneration = servedGeneration;
        this.node = node;
    }

    /** The reader that answered. */
    public String node() { return node; }

    public long servedGeneration() { return servedGeneration; }
}
