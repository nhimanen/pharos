package com.pharos.cluster.objectstore;

/** An object's bytes plus the ETag under which it was read. */
public record StoredObject(byte[] data, String etag) {}
