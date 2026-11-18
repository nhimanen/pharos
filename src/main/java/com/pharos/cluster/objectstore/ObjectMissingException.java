package com.pharos.cluster.objectstore;

import java.nio.file.NoSuchFileException;

/**
 * The object store has no object under {@code key}. Every {@link ObjectStore} throws this type (not
 * a bare {@link NoSuchFileException}) for a missing key, so callers can tell a missing remote object
 * (e.g. a generation garbage-collected on the bucket) from a local file that is gone: a lazy reader
 * degrades a shard only on this type.
 */
public final class ObjectMissingException extends NoSuchFileException {

    private final String key;

    public ObjectMissingException(String key) {
        super(key, null, "object not found: " + key);
        this.key = key;
    }

    /** The missing object's key. */
    public String key() {
        return key;
    }
}
