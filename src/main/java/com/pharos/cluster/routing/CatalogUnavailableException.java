package com.pharos.cluster.routing;

/** The shard catalog cannot be read right now (HTTP 503). */
public class CatalogUnavailableException extends RuntimeException {
    public CatalogUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
