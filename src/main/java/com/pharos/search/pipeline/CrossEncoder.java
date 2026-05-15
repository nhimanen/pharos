package com.pharos.search.pipeline;

import java.util.List;

public interface CrossEncoder {
    /**
     * Scores each (query, passage) pair. Returns one score per passage in [0, 1].
     * Higher = more relevant. Same length as {@code passages}.
     */
    float[] score(String query, List<String> passages);

    boolean isAvailable();
}
