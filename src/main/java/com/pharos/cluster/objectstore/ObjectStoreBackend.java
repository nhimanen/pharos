package com.pharos.cluster.objectstore;

import com.pharos.cluster.BackendKind;
import com.pharos.cluster.IndexPublisher;
import com.pharos.cluster.LocalCache;
import com.pharos.cluster.LocalFsCache;
import com.pharos.cluster.NoOpPublisher;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.ReaderLocalCache;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ReplicationBackend;
import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.WriterStateStore;
import com.pharos.cluster.writer.LeaseGuard;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * Object-store replication. Writers index into the regular index directory
 * ({@link LocalFsCache}) and publish each committed run with an {@link ObjectStorePublisher};
 * readers serve generations materialised under {@code cluster.cacheDir} ({@link ReaderLocalCache})
 * and never publish. {@link #close} closes the store's client.
 */
public final class ObjectStoreBackend implements ReplicationBackend {

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final ObjectStoreCatalog catalog;
    private final ObjectStoreReplicaSource source;
    private final LocalCache cache;
    private final IndexPublisher publisher;
    private final WriterStateStore writerState;

    public ObjectStoreBackend(IndexConfig config) {
        // validated before the S3 client is built (arguments evaluate left to right)
        this(validated(config), new S3ObjectStore(config.getCluster().getObjectStore()));
    }

    private static IndexConfig validated(IndexConfig config) {
        config.getCluster().validate();
        return config;
    }

    /** Package-private: injects the store (tests). Takes ownership of {@code store}. */
    ObjectStoreBackend(IndexConfig config, ObjectStore store) {
        this.store = Objects.requireNonNull(store, "store");
        ClusterConfig cfg = config.getCluster();
        cfg.validate();
        ObjectStoreLayout layout = new ObjectStoreLayout(cfg.getObjectStore().getPrefix());
        this.layout = layout;
        this.catalog = new ObjectStoreCatalog(store, layout);
        this.source = new ObjectStoreReplicaSource(store, layout);
        if (cfg.getRole() == NodeRole.READER) {
            this.cache = new ReaderLocalCache(cfg.getCacheDir());
            this.publisher = NoOpPublisher.INSTANCE;
            this.writerState = WriterStateStore.NONE;
        } else {
            this.cache = new LocalFsCache(config);
            GenerationGc gc = new GenerationGc(store, layout, cfg.getRetentionGenerations(),
                    Duration.ofMinutes(cfg.getRetentionMinutes()));
            this.writerState = new ObjectStoreWriterStateStore(store, layout, source, cfg.effectiveNodeId());
            this.publisher = new ObjectStorePublisher(store, layout, catalog, gc, cfg.effectiveNodeId(),
                    config.getSynonymsFile(), LeaseGuard.IMPLICIT, writerState);
        }
    }

    /** The bucket client (shared with the membership registry; closed by {@link #close}). */
    public ObjectStore store() { return store; }

    public ObjectStoreLayout layout() { return layout; }

    @Override public BackendKind kind() { return BackendKind.OBJECT_STORE; }

    @Override public IndexPublisher publisher() { return publisher; }

    @Override public LocalCache localCache() { return cache; }

    @Override public ShardCatalog catalog() { return catalog; }

    /** The catalog with its write side ({@code register}, source entries). */
    public ObjectStoreCatalog objectStoreCatalog() { return catalog; }

    @Override public ReplicaSource source() { return source; }

    @Override public WriterStateStore writerState() { return writerState; }

    @Override
    public void close() throws IOException {
        store.close();
    }
}
