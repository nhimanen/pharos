package com.pharos.search;

/**
 * Thrown when a vector-based search is requested but the configured search
 * embedding model has no corresponding {@code vec.<modelId>} field for one or
 * more of the queried projects.
 *
 * <p>Surfaces as a clear error rather than empty results, so callers can
 * suggest running {@code pharos embed --model=<id> <project>} (or a re-index)
 * instead of silently returning nothing.
 */
public class VectorModelUnavailableException extends RuntimeException {

    public VectorModelUnavailableException(String message) {
        super(message);
    }
}
