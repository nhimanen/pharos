package com.pharos.search.pipeline;

import com.pharos.embedding.CrossEncoderProvider;

import java.util.List;

/** Bridges the existing {@link CrossEncoderProvider} to the {@link CrossEncoder} interface. */
public final class CrossEncoderProviderAdapter implements CrossEncoder, AutoCloseable {

    private final CrossEncoderProvider provider;

    public CrossEncoderProviderAdapter(CrossEncoderProvider provider) {
        this.provider = provider;
    }

    @Override
    public float[] score(String query, List<String> passages) {
        return provider.scoreBatch(query, passages);
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void close() throws Exception {
        provider.close();
    }
}
