package com.pharos.cluster.peer;

import com.pharos.cluster.BackendKind;
import com.pharos.cluster.Generation;
import com.pharos.cluster.IndexPublisher;
import com.pharos.cluster.LocalCache;
import com.pharos.cluster.LocalFsCache;
import com.pharos.cluster.LocalSnapshot;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ReplicationBackend;
import com.pharos.cluster.ShardCatalog;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.WriterStateStore;
import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.writer.LeaseGuard;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The peer-pull backend: no bucket. A writer indexes into its regular index directory
 * ({@link LocalFsCache}) and publishes each committed run into its local {@link PeerStore}
 * ({@code cluster.peers.storeDir}) with the object-store publisher and GC unchanged: the same
 * protocol ({@code LATEST} CAS as the commit point), the same retention (the last
 * {@code cluster.retentionGenerations} generations plus {@code cluster.retentionMinutes}), the same
 * per-shard synonyms sections. The writer's {@code pharos web} serves that store at
 * {@code /replication/*} ({@link ReplicationEndpoint}).
 *
 * <p>There is no takeover in peer mode, so no writer state is backed up
 * ({@link WriterStateStore#NONE}) and no lease guards the publish ({@link LeaseGuard#IMPLICIT}).
 *
 * <p>Readers pull from writers (Phase 5 Task 2); until then a reader on this backend is refused.
 */
public final class PeerBackend implements ReplicationBackend {

    private final IndexConfig config;
    private final PeerStore peer;
    private final LocalCache cache;
    private final IndexPublisher publisher;

    public PeerBackend(IndexConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        ClusterConfig cfg = config.getCluster();
        cfg.validate();
        if (cfg.getRole() != NodeRole.WRITER) {
            throw new IllegalStateException("cluster.backend 'peer-pull' supports cluster.role 'writer' only in"
                    + " this build: peer-pull readers are not available yet");
        }
        try {
            this.peer = PeerStore.open(cfg.getPeers().getStoreDir());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open cluster.peers.storeDir " + cfg.getPeers().getStoreDir(), e);
        }
        storeMismatch(cfg.getPeers().getStoreDir(), config.getIndexDir(), FILE_STORE).ifPresent(log::warn);
        this.cache = new LocalFsCache(config);
        GenerationGc gc = new GenerationGc(peer.store(), peer.layout(), cfg.getRetentionGenerations(),
                Duration.ofMinutes(cfg.getRetentionMinutes()));
        this.publisher = new SignallingPublisher(new ObjectStorePublisher(peer.store(), peer.layout(),
                peer.catalog(), gc, cfg.effectiveNodeId(), config.getSynonymsFile(), LeaseGuard.IMPLICIT,
                WriterStateStore.NONE), peer.signal());
    }

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PeerBackend.class);

    /** The file store of a path (the nearest existing ancestor); tests inject a fake. */
    @FunctionalInterface
    interface StoreOf {
        Object of(Path p) throws IOException;
    }

    static final StoreOf FILE_STORE = p -> {
        Path q = p.toAbsolutePath();
        while (q != null && !java.nio.file.Files.exists(q)) q = q.getParent();
        return java.nio.file.Files.getFileStore(q == null ? p.toAbsolutePath().getRoot() : q);
    };

    /**
     * The WARN for a store directory on another file store than the index directory: hard links
     * cannot cross file systems, so every published Lucene file is copied and disk use doubles.
     * Empty when they share one, or when either store cannot be told (nothing to go by).
     */
    static Optional<String> storeMismatch(Path storeDir, Path indexDir, StoreOf storeOf) {
        Object store;
        Object index;
        try {
            store = storeOf.of(storeDir);
            index = storeOf.of(indexDir);
        } catch (IOException | RuntimeException e) {
            return Optional.empty();
        }
        if (Objects.equals(store, index)) return Optional.empty();
        return Optional.of("cluster.peers.storeDir " + storeDir + " (file store " + store + ") is not on the file store"
                + " of the index directory " + indexDir + " (" + index + "): published Lucene files will be copied"
                + " instead of hard-linked, which doubles their disk use; put both on one file system");
    }

    /** The writer's published state, served by the replication endpoint. */
    public PeerStore peerStore() { return peer; }

    /** The {@code /replication/*} routes of this writer, sized from {@code cluster.peers}. */
    public ReplicationEndpoint endpoint() {
        ClusterConfig cfg = config.getCluster();
        return new ReplicationEndpoint(peer, cfg.effectiveNodeId(),
                com.pharos.cluster.internal.InternalApi.tokenMatcher(cfg.getToken()),
                cfg.getPeers().getMaxTransfersPerWriter(), cfg.getPeers().getMaxLongPolls());
    }

    @Override public BackendKind kind() { return BackendKind.PEER_PULL; }

    @Override public IndexPublisher publisher() { return publisher; }

    @Override public LocalCache localCache() { return cache; }

    /** This writer's own shards (those it published). */
    @Override public ShardCatalog catalog() { return peer.catalog(); }

    /** This writer's own published generations, read back from its store. */
    @Override public ReplicaSource source() { return peer.source(); }

    @Override public WriterStateStore writerState() { return WriterStateStore.NONE; }

    @Override
    public void close() {
        peer.signal().close();
        peer.store().close();
    }

    /** Publishes, then wakes the {@code latest} long-polls of the shard. */
    static final class SignallingPublisher implements IndexPublisher {
        private final IndexPublisher inner;
        private final GenerationSignal signal;

        SignallingPublisher(IndexPublisher inner, GenerationSignal signal) {
            this.inner = inner;
            this.signal = signal;
        }

        @Override
        public Optional<Generation> publish(LocalSnapshot snapshot) throws IOException {
            Optional<Generation> gen = inner.publish(snapshot);
            gen.ifPresent(g -> signal.published(ShardId.ofProject(snapshot.project()), g));
            return gen;
        }

        @Override
        public void checkPublishable(String project) {
            inner.checkPublishable(project);
        }
    }
}
