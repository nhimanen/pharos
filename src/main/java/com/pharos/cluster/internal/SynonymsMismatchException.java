package com.pharos.cluster.internal;

/**
 * Coordinator and reader analyse queries with different {@code synonyms.txt}: the reader refuses
 * (409, {@code "error":"synonyms mismatch"}, with the hash of its own synonyms as {@code synonymsHash}) and
 * the coordinator treats that replica as unavailable, unless it can analyse the query with the reader's
 * synonyms instead (its previous ones, ruling P6-R19).
 */
public class SynonymsMismatchException extends ShardCallException {
    private final String servedHash;

    public SynonymsMismatchException(String message) {
        this(message, null);
    }

    /** @param servedHash the hash of the synonyms the refusing side analyses with; null when unknown */
    public SynonymsMismatchException(String message, String servedHash) {
        super(message);
        this.servedHash = servedHash;
    }

    /** The hash of the synonyms the refusing reader analyses with; null when it did not say (an older one). */
    public String servedHash() {
        return servedHash;
    }
}
