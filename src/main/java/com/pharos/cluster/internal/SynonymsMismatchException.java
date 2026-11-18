package com.pharos.cluster.internal;

/**
 * Coordinator and reader analyse queries with different {@code synonyms.txt}: the reader refuses
 * (409, {@code "error":"synonyms mismatch"}) and the coordinator treats that replica as unavailable.
 */
public class SynonymsMismatchException extends ShardCallException {
    public SynonymsMismatchException(String message) { super(message); }
}
