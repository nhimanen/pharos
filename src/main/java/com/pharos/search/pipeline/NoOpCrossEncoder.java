package com.pharos.search.pipeline;

import java.util.List;

public final class NoOpCrossEncoder implements CrossEncoder {

    @Override
    public float[] score(String query, List<String> passages) {
        return new float[passages.size()];
    }

    @Override
    public boolean isAvailable() {
        return false;
    }
}
