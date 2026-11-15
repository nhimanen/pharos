package com.pharos.cluster.objectstore;

import java.time.Instant;

/** Listing entry for a stored object. */
public record ObjectInfo(String key, long size, Instant lastModified) {}
