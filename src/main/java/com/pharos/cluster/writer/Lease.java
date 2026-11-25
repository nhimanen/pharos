package com.pharos.cluster.writer;

import com.pharos.cluster.ShardId;

import java.time.Instant;

/**
 * A shard lease held by {@code writer}.
 *
 * @param expires the bucket LastModified of the write that produced {@code etag}, plus the TTL
 *                (informational; expiry is always re-judged from the store, never from this field)
 * @param etag    the lease object's ETag after that write; the next renewal is conditioned on it
 */
public record Lease(ShardId shard, String writer, Instant expires, String etag) {}
