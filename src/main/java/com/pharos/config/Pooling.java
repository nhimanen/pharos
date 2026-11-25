package com.pharos.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

/** How a local (DJL) embedding model reduces token hidden states to one vector. */
public enum Pooling {
    /** Attention-masked mean over all tokens (sentence-transformers default; pharos' historic behaviour). */
    MEAN,
    /** The first ({@code [CLS]}) token's hidden state. */
    CLS;

    @JsonValue
    public String json() { return name().toLowerCase(Locale.ROOT); }

    @JsonCreator
    public static Pooling fromJson(String s) {
        if (s == null || s.isBlank()) return MEAN;
        try {
            return valueOf(s.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown pooling '" + s + "' (use 'mean' or 'cls')");
        }
    }
}
