package com.pharos.cluster.objectstore;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/** Minimal object-storage abstraction used for publishing and fetching index generations. */
public interface ObjectStore extends Closeable {

    /** Reads an object fully into memory; empty when the key does not exist. */
    Optional<StoredObject> get(String key) throws IOException;

    /**
     * Streams an object to {@code target} (via a temp file in the same directory, then moved into
     * place, so a partial target is never left behind).
     *
     * @throws java.nio.file.NoSuchFileException when the key does not exist
     */
    void download(String key, Path target) throws IOException;

    /**
     * Streams exactly {@code length} bytes starting at {@code offset} to {@code target} (temp file in
     * the same directory, then an atomic move; a failure leaves no target behind).
     *
     * @throws java.nio.file.NoSuchFileException when the key does not exist
     * @throws java.io.EOFException when {@code offset + length} exceeds the object size
     * @throws IOException on a short body
     */
    void downloadRange(String key, long offset, long length, Path target) throws IOException;

    /**
     * Opens a stream over exactly {@code length} bytes starting at {@code offset}, for callers that
     * want the bytes without a temp file. Missing keys and ranges past the end fail here, at open.
     * The stream may still end early (a dropped connection): the caller must check that it read
     * exactly {@code length} bytes. A zero-length range returns an empty stream after checking that
     * the object exists and {@code offset} is not past its end.
     *
     * @throws java.nio.file.NoSuchFileException when the key does not exist
     * @throws java.io.EOFException when {@code offset + length} exceeds the object size
     */
    InputStream openRange(String key, long offset, long length) throws IOException;

    /**
     * Validates a {@link #downloadRange} or {@link #openRange} request and returns its exclusive end {@code offset + length}.
     *
     * @throws IllegalArgumentException for a negative offset or length, or when the end overflows a long
     */
    static long checkRange(long offset, long length) {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("negative offset or length: " + offset + "+" + length);
        }
        try {
            return Math.addExact(offset, length);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("range " + offset + "+" + length + " overflows a long");
        }
    }

    /** Uploads a file; returns the new ETag. */
    String put(String key, Path source) throws IOException;

    /** Uploads bytes; returns the new ETag. */
    String put(String key, byte[] data) throws IOException;

    /** Creates the object only if absent. Returns the ETag, or empty if the key already existed. */
    Optional<String> putIfAbsent(String key, byte[] data) throws IOException;

    /** Overwrites only if the current ETag equals {@code etag}. Returns the new ETag, or empty if it changed. */
    Optional<String> putIfMatch(String key, byte[] data, String etag) throws IOException;

    /** Lists all objects whose key starts with {@code prefix}. */
    List<ObjectInfo> list(String prefix) throws IOException;

    /** Deletes an object; an absent key is not an error. */
    void delete(String key) throws IOException;
}
