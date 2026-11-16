package com.pharos.cluster;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * The node's role-specific runtime: the replication backend and, on readers, the
 * {@link ReplicaSyncer} that keeps the local cache in step with the object store.
 * {@link #close} stops the syncer and closes the backend (object-store clients); it is idempotent.
 */
public final class ClusterRuntime implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ClusterRuntime.class);

    private final IndexConfig config;
    private final ReplicationBackend backend;
    private final ProjectRegistry registry;
    private final LuceneIndexer luceneIndexer;
    private ReplicaSyncer syncer;
    private boolean closed;

    public ClusterRuntime(IndexConfig config, ReplicationBackend backend, ProjectRegistry registry,
                          LuceneIndexer luceneIndexer) {
        this.config = Objects.requireNonNull(config, "config");
        this.backend = Objects.requireNonNull(backend, "backend");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.luceneIndexer = Objects.requireNonNull(luceneIndexer, "luceneIndexer");
    }

    /**
     * The registry a node serves: on readers {@code <cacheDir>/registry.json}, filled by the
     * syncer; otherwise the regular {@code registry.json}.
     */
    public static ProjectRegistry registryFor(IndexConfig config) {
        if (config.getCluster().getRole() == NodeRole.READER) {
            return ProjectRegistry.at(config.getCluster().getCacheDir().resolve("registry.json"));
        }
        return new ProjectRegistry(config);
    }

    /**
     * Readers never read source files: bodies and snippets come from the index, so an
     * unrelated or stale file at an indexed absolute path cannot leak into answers.
     */
    public static void applySourceReadPolicy(IndexConfig config) {
        SourceReader.setEnabled(config.getCluster().getRole() != NodeRole.READER);
    }

    public IndexPublisher publisher() {
        return backend.publisher();
    }

    /**
     * Readers: syncs once synchronously (so the daemon serves the published shards from its
     * first request), then polls every {@code cluster.pollIntervalSeconds}. No-op on other roles
     * and when already serving.
     *
     * @throws IOException when the shard catalog cannot be read
     */
    public synchronized void startServing() throws IOException {
        if (config.getCluster().getRole() != NodeRole.READER || syncer != null) return;
        if (closed) throw new IllegalStateException("cluster runtime is closed");
        if (!(backend.localCache() instanceof ReaderLocalCache cache)) {
            throw new IllegalStateException("reader role needs a ReaderLocalCache, got " + backend.localCache());
        }
        ReplicaSyncer s = new ReplicaSyncer(backend.catalog(), backend.source(), cache, registry,
                config.getCluster().getCacheDir(), config.getSynonymsFile(),
                Duration.ofSeconds(config.getCluster().getPollIntervalSeconds()));
        try {
            s.syncOnce();
        } catch (IOException | RuntimeException e) {
            s.close();
            throw e;
        }
        syncer = s;
        Map<ShardId, Generation> served = s.served();
        log.info("Reader {} serving {} shard(s): {}", config.getCluster().getNodeId(), served.size(), served);
        warm(served);
        s.start();
    }

    /** Readers: one sync pass now (in addition to the polling). */
    public void syncNow() throws IOException {
        ReplicaSyncer s;
        synchronized (this) {
            s = syncer;
        }
        if (s == null) throw new IllegalStateException("not serving: call startServing() on a reader first");
        s.syncOnce();
    }

    /** Generation served per shard; empty unless this is a serving reader. */
    public synchronized Map<ShardId, Generation> served() {
        return syncer == null ? Map.of() : syncer.served();
    }

    /** Opens the served shards' readers so the first query does not pay for it. */
    private void warm(Map<ShardId, Generation> served) {
        for (ShardId shard : served.keySet()) {
            try {
                luceneIndexer.openReader(shard.value());
            } catch (IOException | RuntimeException e) {
                log.warn("Shard {}: opening the index reader failed: {}", shard, e.toString());
            }
        }
    }

    @Override
    public void close() {
        ReplicaSyncer s;
        synchronized (this) {
            if (closed) return;
            closed = true;
            s = syncer;
            syncer = null;
        }
        if (s != null) s.close();
        try {
            backend.close();
        } catch (IOException | RuntimeException e) {
            log.warn("Closing the {} backend failed: {}", backend.kind().json(), e.toString());
        }
    }
}
