package com.pharos.cluster.peer;

import com.pharos.cluster.BackendKind;
import com.pharos.cluster.Generation;
import com.pharos.cluster.IndexPublisher;
import com.pharos.cluster.LocalCache;
import com.pharos.cluster.LocalFsCache;
import com.pharos.cluster.LocalSnapshot;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.NoOpPublisher;
import com.pharos.cluster.ReaderLocalCache;
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
 * <p><b>Readers</b> serve generations materialised under {@code cluster.cacheDir}
 * ({@link ReaderLocalCache}) and never publish. Their catalog is a {@link PeerCatalog} over the
 * writers {@code cluster.peers.discovery} finds, their source a {@link PeerReplicaSource}, both on
 * one {@link PeerHttp} client (with {@code cluster.tls.truststore} when set).
 */
public final class PeerBackend implements ReplicationBackend {

    private final IndexConfig config;
    /** The writer's store; null on a reader. */
    private final PeerStore peer;
    private final LocalCache cache;
    private final IndexPublisher publisher;
    /** Reader side; null on a writer. */
    private final PeerHttp http;
    private final PeerCatalog peerCatalog;
    private final PeerReplicaSource peerSource;

    public PeerBackend(IndexConfig config) {
        this(config, PeerDiscovery.SYSTEM_RESOLVER);
    }

    /** Package-private: injects the DNS resolver (tests). */
    PeerBackend(IndexConfig config, PeerDiscovery.Resolver resolver) {
        this.config = Objects.requireNonNull(config, "config");
        ClusterConfig cfg = config.getCluster();
        cfg.validate();
        if (cfg.getRole() == NodeRole.READER) {
            this.peer = null;
            this.cache = new ReaderLocalCache(cfg.getCacheDir());
            this.publisher = NoOpPublisher.INSTANCE;
            this.http = PeerHttp.of(cfg);
            PeerDiscovery discovery = new PeerDiscovery(PeerDiscovery.parse(cfg.getPeers().getDiscovery()),
                    http.tls() ? "https" : "http", resolver, http, cfg.getPeers().getAllowHosts());
            this.peerCatalog = new PeerCatalog(discovery, http, Duration.ofSeconds(cfg.getPeers().getRefreshSeconds()),
                    System::nanoTime);
            this.peerSource = new PeerReplicaSource(peerCatalog, http,
                    Duration.ofSeconds(cfg.getPeers().getLongPollSeconds()));
            peerSource.setSelf(cfg.getAdvertiseUrl()); // never fetch from ourselves when a writer lists us
            return;
        }
        if (cfg.getRole() != NodeRole.WRITER) {
            throw new IllegalStateException("cluster.backend 'peer-pull' needs cluster.role 'writer' or 'reader'");
        }
        this.http = null;
        this.peerCatalog = null;
        this.peerSource = null;
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
    public PeerStore peerStore() {
        if (peer == null) throw new IllegalStateException("a peer-pull reader has no peer store");
        return peer;
    }

    /** True on a reader (pulls from writers); false on a writer (serves its store). */
    public boolean isReader() { return peer == null; }

    /** The reader's merged writer catalog; empty on a writer. */
    public Optional<PeerCatalog> peerCatalog() { return Optional.ofNullable(peerCatalog); }

    /** The reader's HTTP client to the writers; empty on a writer. */
    public Optional<PeerHttp> peerHttp() { return Optional.ofNullable(http); }

    /** The reader's source; empty on a writer. */
    public Optional<PeerReplicaSource> peerSource() { return Optional.ofNullable(peerSource); }

    /** The {@code /replication/*} routes of this writer, sized from {@code cluster.peers}. */
    public ReplicationEndpoint endpoint() {
        ClusterConfig cfg = config.getCluster();
        return new ReplicationEndpoint(peerStore(), cfg.effectiveNodeId(),
                com.pharos.cluster.internal.InternalApi.tokenMatcher(cfg.effectiveToken()),
                cfg.getPeers().getMaxTransfersPerWriter(), cfg.getPeers().getMaxLongPolls(), () -> knownPeers(cfg),
                new PeerReaderRegistry(Duration.ofSeconds(cfg.getReaderTtlSeconds()), System::nanoTime),
                new PeerStatusState(PeerStatusState.idleExpiry(cfg), System::nanoTime), System::currentTimeMillis,
                new ReaderAddressCheck(cfg.getPeers().getAllowHosts(), ReaderAddressCheck.JVM_RESOLVER));
    }

    /**
     * What this writer gossips at {@code /replication/peers}: its own {@code cluster.advertiseUrl}
     * (when set) and its {@code seeds:} (when its discovery is seeds); readers follow the lists
     * transitively.
     */
    static java.util.List<String> knownPeers(ClusterConfig cfg) {
        java.util.Set<String> out = new java.util.TreeSet<>();
        if (cfg.getAdvertiseUrl() != null && !cfg.getAdvertiseUrl().isBlank()) {
            try {
                out.add(PeerDiscovery.baseUrl(cfg.getAdvertiseUrl().strip()).toString());
            } catch (IllegalArgumentException e) {
                log.debug("cluster.advertiseUrl is not a writer URL: {}", e.getMessage());
            }
        }
        if (cfg.getPeers().getDiscovery() != null
                && PeerDiscovery.parse(cfg.getPeers().getDiscovery()) instanceof PeerDiscovery.Seeds s) {
            s.urls().forEach(u -> out.add(u.toString()));
        }
        return java.util.List.copyOf(out);
    }

    @Override public BackendKind kind() { return BackendKind.PEER_PULL; }

    @Override public IndexPublisher publisher() { return publisher; }

    @Override public LocalCache localCache() { return cache; }

    /** A writer: its own shards (those it published). A reader: every discovered writer's, merged. */
    @Override public ShardCatalog catalog() { return peer != null ? peer.catalog() : peerCatalog; }

    /** A writer: its own generations, read back from its store. A reader: pulled from the writers. */
    @Override public ReplicaSource source() { return peer != null ? peer.source() : peerSource; }

    @Override public WriterStateStore writerState() { return WriterStateStore.NONE; }

    @Override
    public void close() {
        if (peer != null) {
            peer.signal().close();
            peer.store().close();
        } else {
            peerSource.close();
            http.close();
        }
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
