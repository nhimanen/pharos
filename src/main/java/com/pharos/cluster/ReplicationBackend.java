package com.pharos.cluster;

import java.io.Closeable;

/** Where shards are published to and (on readers) cached locally. */
public interface ReplicationBackend extends Closeable {
    BackendKind kind();

    IndexPublisher publisher();

    LocalCache localCache();

    /** The published shard list (readers); unsupported by backends without replication. */
    ShardCatalog catalog();

    /** Where readers fetch generations from; unsupported by backends without replication. */
    ReplicaSource source();

    /** Writer state backed up after each publish and restored on takeover; {@link WriterStateStore#NONE} when not kept. */
    WriterStateStore writerState();
}
