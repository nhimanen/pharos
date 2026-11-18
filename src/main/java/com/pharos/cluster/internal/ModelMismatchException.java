package com.pharos.cluster.internal;

/**
 * The shard has no vectors for the coordinator's search embedding model (or a different model
 * fingerprint or dimension count): the reader refuses (409, {@code "error":"model mismatch"}) and
 * the coordinator leaves that shard out of the vector retrieval with a warning.
 */
public class ModelMismatchException extends ShardCallException {
    public ModelMismatchException(String message) { super(message); }
}
