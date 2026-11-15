package com.pharos.search.pipeline;

import java.util.List;
import java.util.function.Supplier;

/**
 * Defers loading the cross-encoder model until a reranking pipeline first uses it. Like
 * {@link com.pharos.embedding.LazyEmbeddingProvider}, {@link #isAvailable()} triggers the load,
 * because pipeline selection relies on it meaning "the model loaded".
 */
public final class LazyCrossEncoder implements CrossEncoder, AutoCloseable {

    private final Supplier<CrossEncoder> factory;
    private volatile CrossEncoder delegate;

    public LazyCrossEncoder(Supplier<CrossEncoder> factory) {
        this.factory = factory;
    }

    public boolean isLoaded() {
        return delegate != null;
    }

    private CrossEncoder delegate() {
        CrossEncoder d = delegate;
        if (d == null) {
            synchronized (this) {
                d = delegate;
                if (d == null) {
                    d = factory.get();
                    if (d == null) d = new NoOpCrossEncoder();
                    delegate = d;
                }
            }
        }
        return d;
    }

    @Override public float[] score(String query, List<String> passages) { return delegate().score(query, passages); }

    @Override public boolean isAvailable() { return delegate().isAvailable(); }

    @Override
    public void close() throws Exception {
        if (delegate instanceof AutoCloseable c) c.close();
    }
}
