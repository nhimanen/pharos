package com.pharos.cluster.routing;

/**
 * The shard catalog has not been read successfully yet (the bucket is unreachable since this
 * reader started). Unlike an over-age catalog, a query can still be answered from the shards this
 * reader holds, as before fan-out existed.
 */
public class CatalogNeverReadException extends CatalogUnavailableException {
    public CatalogNeverReadException(String message, Throwable cause) {
        super(message, cause);
    }
}
