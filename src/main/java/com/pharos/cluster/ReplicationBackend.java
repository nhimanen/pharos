package com.pharos.cluster;

import java.io.Closeable;

/** Where shards are published to and (on readers) cached locally. */
public interface ReplicationBackend extends Closeable {
    BackendKind kind();

    IndexPublisher publisher();

    LocalCache localCache();
}
