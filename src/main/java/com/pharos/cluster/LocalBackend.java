package com.pharos.cluster;

import com.pharos.config.IndexConfig;

/** Single-node backend: no replication, shards live under the index directory. */
public class LocalBackend implements ReplicationBackend {
    private final LocalCache cache;

    public LocalBackend(IndexConfig config) {
        this.cache = new LocalFsCache(config);
    }

    @Override public BackendKind kind() { return BackendKind.LOCAL; }

    @Override public IndexPublisher publisher() { return NoOpPublisher.INSTANCE; }

    @Override public LocalCache localCache() { return cache; }

    @Override public void close() {}
}
