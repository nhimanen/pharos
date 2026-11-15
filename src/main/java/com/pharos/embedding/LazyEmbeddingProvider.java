package com.pharos.embedding;

import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/**
 * Defers constructing (and therefore loading) an embedding model until something actually needs
 * it. A local ONNX model costs ~1.1 GB of native memory and several seconds to load, and most
 * pharos commands — graph lookups, {@code index --no-embed}, {@code list}, {@code link}, MCP
 * sessions that only navigate — never embed anything.
 *
 * <p>{@link #modelId()} and {@link #dimensions()} come from configuration and never trigger a
 * load. {@link #isAvailable()} does: availability means "the model loaded", and callers rely on
 * that to fall back (pipeline selection, cache fingerprints, skipping vector fields). Only call
 * it where embeddings are actually about to be used.
 *
 * <p>Thread-safe; the delegate is created at most once.
 */
public final class LazyEmbeddingProvider implements EmbeddingProvider {

    private final String modelId;
    private final int dimensions;
    private final Supplier<EmbeddingProvider> factory;
    private volatile EmbeddingProvider delegate;

    public LazyEmbeddingProvider(String modelId, int dimensions, Supplier<EmbeddingProvider> factory) {
        this.modelId = modelId;
        this.dimensions = dimensions;
        this.factory = factory;
    }

    /** True once the underlying provider has been constructed (diagnostics and tests). */
    public boolean isLoaded() {
        return delegate != null;
    }

    private EmbeddingProvider delegate() {
        EmbeddingProvider d = delegate;
        if (d == null) {
            synchronized (this) {
                d = delegate;
                if (d == null) {
                    d = factory.get();
                    if (d == null) d = new NoOpEmbeddingProvider(modelId);
                    delegate = d;
                }
            }
        }
        return d;
    }

    @Override public String modelId() { return modelId; }

    /** Configured dimensions; does not load the model. */
    @Override public int dimensions() { return dimensions; }

    /** Loads the model if needed and reports whether that succeeded. */
    @Override public boolean isAvailable() { return delegate().isAvailable(); }

    @Override public float[] embed(String text) { return delegate().embed(text); }

    @Override public float[][] embedBatch(List<String> texts) { return delegate().embedBatch(texts); }

    @Override
    public float[][] embedChunked(List<String> texts, int chunkSize, IntConsumer onChunkDone) {
        return delegate().embedChunked(texts, chunkSize, onChunkDone);
    }

    @Override
    public String toString() {
        return "Lazy[" + modelId + (isLoaded() ? ", loaded" : ", not loaded") + "]";
    }
}
