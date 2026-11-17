package com.pharos.cluster.internal;

/** A reader refused a shard call in a way another replica would not fix (401, 404, 400). */
public class ShardCallException extends RuntimeException {
    public ShardCallException(String message) { super(message); }
}
