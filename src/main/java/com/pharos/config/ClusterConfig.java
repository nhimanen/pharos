package com.pharos.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.pharos.cluster.BackendKind;
import com.pharos.cluster.NodeRole;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.function.Supplier;

/** The {@code cluster} section of config.json. Defaults describe a single-node setup. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterConfig {

    private NodeRole role = NodeRole.ALL;
    private BackendKind backend = BackendKind.LOCAL;
    /** Null until configured; {@link #effectiveNodeId} falls back to the host name lazily. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String nodeId;
    /** Host name looked up on first use only: {@code getLocalHost()} can stall for seconds. */
    private transient String resolvedNodeId;
    private int pollIntervalSeconds = 10;
    /**
     * Readers: how long a shard's directory is kept after the reader stops serving it (pinned away),
     * so queries and fan-out calls already reading it finish. Keep it at least as long as the longest
     * query or fan-out timeout; the effective value is never below {@code pollIntervalSeconds}.
     */
    private int retireGraceSeconds = 120;
    private Path cacheDir = IndexConfig.DEFAULT_BASE.resolve("cluster-cache");
    private int retentionGenerations = 3;
    private int retentionMinutes = 60;
    private ObjectStoreConfig objectStore = new ObjectStoreConfig();
    private ReaderConfig reader = new ReaderConfig();
    private WriterConfig writer = new WriterConfig();
    /** {@code /cluster/status}: proposal targets and windows, the events keep-alive and the webhook. */
    private StatusConfig status = new StatusConfig();
    /** {@code cluster.peers}: the peer-pull backend (discovery, transfer cap, long-poll). */
    private PeersConfig peers = new PeersConfig();
    /** {@code cluster.tls}: absent means plain HTTP. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private TlsConfig tls;
    /** Base URL other nodes reach this reader at; required on readers. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String advertiseUrl;
    /** Shared secret of the cluster's internal endpoints; required on readers. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String token;
    private int heartbeatSeconds = 10;
    private int readerTtlSeconds = 30;
    /** Timeout of one internal shard call (term statistics, shard search, snippets, callees). */
    private int fanoutTimeoutMillis = 5000;
    /** Connect timeout of an internal shard call; a dead reader fails this fast. */
    private int connectTimeoutMillis = 1000;
    /**
     * Budget of one fanned-out query, every shard round included; 0 means the default
     * ({@link #DEFAULT_QUERY_TIMEOUT_MILLIS}, or {@code fanoutTimeoutMillis} when that is larger).
     */
    private int queryTimeoutMillis = 0;
    /** How old the last good read of the shard catalog may get before unscoped routing is refused. */
    private int maxCatalogAgeSeconds = 600;
    /**
     * Readers: on shutdown (SIGTERM), how long the node keeps running while draining: it announces
     * {@code draining} (heartbeat and {@code /health} 503) at once and stops when its in-flight
     * requests reach 0, or after this many seconds. 0 stops at once (still announcing first).
     */
    private int drainSeconds = DEFAULT_DRAIN_SECONDS;

    public static final int DEFAULT_DRAIN_SECONDS = 20;
    static final int MAX_DRAIN_SECONDS = 3600;

    public static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 20_000;
    /** A pinned local reader is retired this long after a swap: a query must finish well before. */
    static final long RETIRE_DELAY_MILLIS = com.pharos.indexer.LuceneIndexer.RETIRE_DELAY.toMillis();

    /** How the default node id is found; package-private so tests can count lookups. */
    static Supplier<String> hostnameResolver = ClusterConfig::hostName;

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "pharos-node";
        }
    }

    /**
     * The configured {@code nodeId}, or else the host name, looked up once on first call (never
     * while loading the config, so single-node commands never pay for the lookup).
     */
    public synchronized String effectiveNodeId() {
        if (nodeId != null) return nodeId;
        if (resolvedNodeId == null) resolvedNodeId = hostnameResolver.get();
        return resolvedNodeId;
    }

    /** @throws IllegalStateException when the combination of settings is unusable */
    public void validate() {
        if (role != NodeRole.ALL && backend == BackendKind.LOCAL) {
            throw new IllegalStateException("cluster.role '" + role.json()
                    + "' requires cluster.backend 'object-store' or 'peer-pull'");
        }
        if (role == NodeRole.ALL && backend != BackendKind.LOCAL) {
            throw new IllegalStateException("cluster.role 'all' requires cluster.backend 'local'");
        }
        if (backend == BackendKind.OBJECT_STORE
                && (objectStore == null || objectStore.getBucket() == null || objectStore.getBucket().isBlank())) {
            throw new IllegalStateException("cluster.objectStore.bucket is required when cluster.backend is 'object-store'");
        }
        if (retentionGenerations < 2) {
            throw new IllegalStateException("cluster.retentionGenerations must be at least 2");
        }
        if (retentionMinutes < 0) {
            // a negative age would move the GC cutoff into the future and delete in-flight uploads
            throw new IllegalStateException("cluster.retentionMinutes must not be negative, got " + retentionMinutes);
        }
        if (pollIntervalSeconds <= 0) {
            throw new IllegalStateException("cluster.pollIntervalSeconds must be positive, got " + pollIntervalSeconds);
        }
        if (retireGraceSeconds <= 0) {
            throw new IllegalStateException("cluster.retireGraceSeconds must be positive, got " + retireGraceSeconds);
        }
        if (retireGraceSeconds < 2L * pollIntervalSeconds) {
            throw new IllegalStateException("cluster.retireGraceSeconds (" + retireGraceSeconds
                    + ") must be at least twice cluster.pollIntervalSeconds (" + pollIntervalSeconds + ")");
        }
        if (fanoutTimeoutMillis <= 0) {
            throw new IllegalStateException("cluster.fanoutTimeoutMillis must be positive, got " + fanoutTimeoutMillis);
        }
        if (connectTimeoutMillis <= 0) {
            throw new IllegalStateException("cluster.connectTimeoutMillis must be positive, got " + connectTimeoutMillis);
        }
        if (queryTimeoutMillis < 0) {
            throw new IllegalStateException("cluster.queryTimeoutMillis must not be negative (0 = default), got "
                    + queryTimeoutMillis);
        }
        if (queryTimeoutMillis > 0 && queryTimeoutMillis < fanoutTimeoutMillis) {
            throw new IllegalStateException("cluster.queryTimeoutMillis (" + queryTimeoutMillis
                    + ") must be at least cluster.fanoutTimeoutMillis (" + fanoutTimeoutMillis + ")");
        }
        if (effectiveQueryTimeoutMillis() >= RETIRE_DELAY_MILLIS) {
            throw new IllegalStateException("cluster.queryTimeoutMillis (effective " + effectiveQueryTimeoutMillis()
                    + " ms, from cluster.fanoutTimeoutMillis when unset) must stay below " + RETIRE_DELAY_MILLIS
                    + " ms, how long a swapped-out index reader stays open");
        }
        if (drainSeconds < 0 || drainSeconds > MAX_DRAIN_SECONDS) {
            throw new IllegalStateException("cluster.drainSeconds must be between 0 and " + MAX_DRAIN_SECONDS
                    + ", got " + drainSeconds);
        }
        if (maxCatalogAgeSeconds <= 0) {
            throw new IllegalStateException("cluster.maxCatalogAgeSeconds must be positive, got " + maxCatalogAgeSeconds);
        }
        if (retireGraceSeconds * 1000L < fanoutTimeoutMillis) {
            throw new IllegalStateException("cluster.retireGraceSeconds (" + retireGraceSeconds
                    + " s) must cover cluster.fanoutTimeoutMillis (" + fanoutTimeoutMillis
                    + " ms): a retired shard directory must outlive the longest shard call");
        }
        if (role != NodeRole.ALL && nodeId != null && nodeId.isBlank()) {
            throw new IllegalStateException("cluster.nodeId must not be blank (omit it to use the host name)");
        }
        if (heartbeatSeconds <= 0) {
            throw new IllegalStateException("cluster.heartbeatSeconds must be positive, got " + heartbeatSeconds);
        }
        if (readerTtlSeconds <= heartbeatSeconds) {
            throw new IllegalStateException("cluster.readerTtlSeconds must exceed cluster.heartbeatSeconds ("
                    + heartbeatSeconds + "), got " + readerTtlSeconds);
        }
        setWriter(writer); // a default section when absent; syncs the peer-mode work-queue override
        if (backend == BackendKind.PEER_PULL) {
            // P5-R6: no leases, no takeover in peer mode; writers own their registry's shards statically
            if (Boolean.TRUE.equals(writer.getWorkQueue())) {
                throw new IllegalStateException("cluster.writer.workQueue cannot be true with cluster.backend 'peer-pull'"
                        + " (peer mode has no leases or takeover; remove the setting)");
            }
            if (token == null || token.isBlank()) {
                throw new IllegalStateException("cluster.token is required with cluster.backend 'peer-pull'"
                        + " (every /replication/* route requires it)");
            }
        }
        if (peers == null) peers = new PeersConfig();
        peers.validate();
        if (tls != null) tls.validate();
        if (role == NodeRole.WRITER && writer.isWorkQueue() && heartbeatSeconds >= writer.getLeaseTtlSeconds()) {
            // a writer's heartbeat is its liveness (TTL = the lease TTL) and the bucket's clock for its leases
            throw new IllegalStateException("cluster.heartbeatSeconds (" + heartbeatSeconds
                    + ") must be below cluster.writer.leaseTtlSeconds (" + writer.getLeaseTtlSeconds()
                    + ") in work-queue mode");
        }
        if (role == NodeRole.READER) {
            if (advertiseUrl == null || advertiseUrl.isBlank()) {
                throw new IllegalStateException("cluster.advertiseUrl is required on a reader (the base URL other nodes reach it at)");
            }
            try {
                java.net.URI u = new java.net.URI(advertiseUrl.trim());
                String scheme = u.getScheme();
                if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))
                        || u.getHost() == null) {
                    throw new IllegalStateException("cluster.advertiseUrl must be an http:// or https:// URL with a host, got '"
                            + advertiseUrl + "'");
                }
            } catch (java.net.URISyntaxException e) {
                throw new IllegalStateException("cluster.advertiseUrl is not a valid URL: '" + advertiseUrl + "'");
            }
            if (token == null || token.isBlank()) {
                throw new IllegalStateException("cluster.token is required on a reader (shared secret of the cluster)");
            }
        }
        if (reader == null) reader = new ReaderConfig();
        reader.validate();
        if (writer == null) writer = new WriterConfig();
        writer.validate();
        if (status == null) status = new StatusConfig();
        status.validate();
        if (backend == BackendKind.OBJECT_STORE) {
            objectStore.validate();
            String prefix = objectStore.getPrefix();
            if (prefix != null && !prefix.isEmpty()) {
                if (prefix.startsWith("/")) {
                    throw new IllegalStateException("cluster.objectStore.prefix must not start with '/': '" + prefix + "'");
                }
                for (String segment : prefix.split("/", -1)) {
                    if (segment.equals("..") || segment.equals(".")) {
                        throw new IllegalStateException("cluster.objectStore.prefix must not contain '.' or '..' segments: '"
                                + prefix + "'");
                    }
                }
            }
        }
    }

    /** Redacts the token: this is what logs and error messages show. */
    @Override
    public String toString() {
        return "ClusterConfig[role=" + role + ", backend=" + backend + ", nodeId=" + nodeId
                + ", advertiseUrl=" + advertiseUrl + ", token=" + (token == null ? "unset" : "***") + "]";
    }

    public NodeRole getRole() { return role; }
    public void setRole(NodeRole role) { this.role = role; }
    public BackendKind getBackend() { return backend; }
    public void setBackend(BackendKind backend) {
        this.backend = backend;
        if (writer != null) writer.peerMode = backend == BackendKind.PEER_PULL;
    }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public int getRetireGraceSeconds() { return retireGraceSeconds; }
    public void setRetireGraceSeconds(int v) { this.retireGraceSeconds = v; }
    public int getPollIntervalSeconds() { return pollIntervalSeconds; }
    public void setPollIntervalSeconds(int v) { this.pollIntervalSeconds = v; }
    public Path getCacheDir() { return cacheDir; }
    public void setCacheDir(Path cacheDir) { this.cacheDir = cacheDir; }
    public int getRetentionGenerations() { return retentionGenerations; }
    public void setRetentionGenerations(int v) { this.retentionGenerations = v; }
    public int getRetentionMinutes() { return retentionMinutes; }
    public void setRetentionMinutes(int v) { this.retentionMinutes = v; }
    public ObjectStoreConfig getObjectStore() { return objectStore; }
    public void setObjectStore(ObjectStoreConfig objectStore) { this.objectStore = objectStore; }
    public ReaderConfig getReader() { return reader; }
    public WriterConfig getWriter() { return writer; }
    public void setWriter(WriterConfig writer) {
        this.writer = writer == null ? new WriterConfig() : writer;
        this.writer.peerMode = backend == BackendKind.PEER_PULL;
    }
    public PeersConfig getPeers() { return peers; }
    public void setPeers(PeersConfig p) { this.peers = p == null ? new PeersConfig() : p; }
    public TlsConfig getTls() { return tls; }
    public void setTls(TlsConfig t) { this.tls = t; }
    /** True when {@code cluster.tls.keystore} is set: the web server listens with TLS only. */
    public boolean tlsServer() { return tls != null && tls.getKeystore() != null; }
    public void setReader(ReaderConfig reader) { this.reader = reader; }
    public StatusConfig getStatus() { return status; }
    public void setStatus(StatusConfig status) { this.status = status == null ? new StatusConfig() : status; }
    public int getDrainSeconds() { return drainSeconds; }
    public void setDrainSeconds(int v) { this.drainSeconds = v; }
    public String getAdvertiseUrl() { return advertiseUrl; }
    public void setAdvertiseUrl(String v) { this.advertiseUrl = v; }
    /** Serialised for config persistence ({@code IndexConfig.save}); every output surface redacts it. */
    public String getToken() { return token; }
    public void setToken(String v) { this.token = v; }
    public int getHeartbeatSeconds() { return heartbeatSeconds; }
    public void setHeartbeatSeconds(int v) { this.heartbeatSeconds = v; }
    public int getFanoutTimeoutMillis() { return fanoutTimeoutMillis; }
    public void setFanoutTimeoutMillis(int v) { this.fanoutTimeoutMillis = v; }
    public int getConnectTimeoutMillis() { return connectTimeoutMillis; }
    public int getQueryTimeoutMillis() { return queryTimeoutMillis; }
    public void setQueryTimeoutMillis(int v) { this.queryTimeoutMillis = v; }
    /** {@code queryTimeoutMillis}, or its default when unset (0): 20000, or {@code fanoutTimeoutMillis} if larger. */
    public int effectiveQueryTimeoutMillis() {
        return queryTimeoutMillis > 0 ? queryTimeoutMillis : Math.max(DEFAULT_QUERY_TIMEOUT_MILLIS, fanoutTimeoutMillis);
    }
    public int getMaxCatalogAgeSeconds() { return maxCatalogAgeSeconds; }
    public void setMaxCatalogAgeSeconds(int v) { this.maxCatalogAgeSeconds = v; }
    public void setConnectTimeoutMillis(int v) { this.connectTimeoutMillis = v; }
    public int getReaderTtlSeconds() { return readerTtlSeconds; }
    public void setReaderTtlSeconds(int v) { this.readerTtlSeconds = v; }

    /**
     * {@code cluster.reader}: how a reader node serves a generation. {@code lazy} (the default) keeps
     * only the small Lucene files on disk and reads the rest in blocks through a size-capped local
     * cache; {@code full} copies every file, as before Phase 2. Only reader nodes use it.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReaderConfig {
        public static final String LAZY = "lazy";
        public static final String FULL = "full";
        static final long MIN_BLOCK = 64L * 1024;
        static final long MAX_BLOCK = 64L * 1024 * 1024;

        private String mode = LAZY;
        private long cacheMaxBytes = 8L << 30;
        private long blockSizeBytes = 8L << 20;
        private long eagerMaxBytes = 1L << 20;
        private java.util.List<String> prefetchExtensions = java.util.List.of("tip", "tmd", "tim", "vex", "vem");
        private int prefetchTimeoutSeconds = 30;
        private int maxConcurrentFetches = 16;
        /** Cap of a private block cache (a second process on the cache dir); 0 derives it. */
        private long privateCacheMaxBytes = 0;
        /** Project names this reader is pinned to; empty means every shard. */
        private java.util.List<String> shards = java.util.List.of();

        /** @throws IllegalStateException naming the offending field */
        public void validate() {
            if (shards == null) shards = java.util.List.of();
            for (String s : shards) {
                try {
                    new com.pharos.cluster.ShardId(s);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("cluster.reader.shards: " + e.getMessage());
                }
            }
            if (!LAZY.equals(mode) && !FULL.equals(mode)) {
                throw new IllegalStateException("cluster.reader.mode must be 'lazy' or 'full', got '" + mode + "'");
            }
            if (blockSizeBytes < MIN_BLOCK || blockSizeBytes > MAX_BLOCK || Long.bitCount(blockSizeBytes) != 1) {
                throw new IllegalStateException("cluster.reader.blockSizeBytes must be a power of two between "
                        + MIN_BLOCK + " (64 KiB) and " + MAX_BLOCK + " (64 MiB), got " + blockSizeBytes);
            }
            if (cacheMaxBytes / 4 < blockSizeBytes) {
                throw new IllegalStateException("cluster.reader.cacheMaxBytes must be at least 4 x"
                        + " cluster.reader.blockSizeBytes (" + 4 * blockSizeBytes + "), got " + cacheMaxBytes);
            }
            if (eagerMaxBytes < 0) {
                throw new IllegalStateException("cluster.reader.eagerMaxBytes must not be negative, got " + eagerMaxBytes);
            }
            if (prefetchExtensions == null || prefetchExtensions.stream().anyMatch(e -> e == null || e.isBlank())) {
                throw new IllegalStateException("cluster.reader.prefetchExtensions must be a list of file extensions"
                        + " (e.g. [\"tip\", \"tim\"]), got " + prefetchExtensions);
            }
            if (prefetchTimeoutSeconds < 0) {
                throw new IllegalStateException("cluster.reader.prefetchTimeoutSeconds must not be negative, got "
                        + prefetchTimeoutSeconds);
            }
            if (privateCacheMaxBytes < 0 || (privateCacheMaxBytes > 0 && privateCacheMaxBytes / 4 < blockSizeBytes)) {
                throw new IllegalStateException("cluster.reader.privateCacheMaxBytes must be 0 (derived) or at least"
                        + " 4 x cluster.reader.blockSizeBytes (" + 4 * blockSizeBytes + "), got " + privateCacheMaxBytes);
            }
            if (maxConcurrentFetches < 1) {
                throw new IllegalStateException("cluster.reader.maxConcurrentFetches must be at least 1, got "
                        + maxConcurrentFetches);
            }
        }

        @com.fasterxml.jackson.annotation.JsonIgnore
        public boolean isLazy() { return LAZY.equals(mode); }

        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
        public long getCacheMaxBytes() { return cacheMaxBytes; }
        public void setCacheMaxBytes(long v) { this.cacheMaxBytes = v; }
        public long getBlockSizeBytes() { return blockSizeBytes; }
        public void setBlockSizeBytes(long v) { this.blockSizeBytes = v; }
        public long getEagerMaxBytes() { return eagerMaxBytes; }
        public void setEagerMaxBytes(long v) { this.eagerMaxBytes = v; }
        public java.util.List<String> getPrefetchExtensions() { return prefetchExtensions; }
        public void setPrefetchExtensions(java.util.List<String> v) { this.prefetchExtensions = v; }
        public int getPrefetchTimeoutSeconds() { return prefetchTimeoutSeconds; }
        public void setPrefetchTimeoutSeconds(int v) { this.prefetchTimeoutSeconds = v; }
        public java.util.List<String> getShards() { return shards; }
        public void setShards(java.util.List<String> v) { this.shards = v == null ? java.util.List.of() : java.util.List.copyOf(v); }
        public int getMaxConcurrentFetches() { return maxConcurrentFetches; }
        public long getPrivateCacheMaxBytes() { return privateCacheMaxBytes; }
        public void setPrivateCacheMaxBytes(long v) { this.privateCacheMaxBytes = v; }

        /**
         * The cap of a private block cache: {@code privateCacheMaxBytes}, or when 0
         * {@code min(cacheMaxBytes, max(4 blocks, cacheMaxBytes / 8))}.
         */
        public long effectivePrivateCacheMaxBytes() {
            if (privateCacheMaxBytes > 0) return privateCacheMaxBytes;
            return Math.min(cacheMaxBytes, Math.max(4 * blockSizeBytes, cacheMaxBytes / 8));
        }
        public void setMaxConcurrentFetches(int v) { this.maxConcurrentFetches = v; }
    }

    /**
     * {@code cluster.writer}: where a writer keeps source checkouts and how it follows sources.
     * Only writer nodes use it.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WriterConfig {
        private Path workDir = IndexConfig.DEFAULT_BASE.resolve("work");
        /** How often a writer polls a shard's source (git ls-remote, directory state) for changes. */
        private int pollSeconds = 60;
        /**
         * Work-queue mode (leases, takeover, source polling): the default on {@code role: writer}
         * since the Phase 4 gate. {@code false} keeps the hand-run writer (index and publish by
         * hand under an implicit lease). Ignored on every other role.
         */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private Boolean workQueue;
        /**
         * Set by {@link ClusterConfig} on {@code cluster.backend: peer-pull}: work-queue mode is forced
         * off there (P5-R6). Not configuration: never serialised.
         */
        transient boolean peerMode;
        /**
         * Lease TTL in work-queue mode. A lease whose last write (by the bucket's LastModified) is
         * older than this may be taken over; the holder renews every TTL/3.
         */
        private int leaseTtlSeconds = 30;
        /**
         * Work-queue mode: a lease this writer acquired is never released for balance within this
         * many seconds of acquiring it (and only right after a publish of that shard, or when it has
         * nothing unpublished), so leases don't bounce while writers join and leave.
         */
        private int minHoldSeconds = 120;
        /**
         * Work-queue mode: after this many consecutive failed runs of a surplus shard (not pinned,
         * held at least {@code minHoldSeconds}) the writer releases it so another writer can try.
         */
        private int maxConsecutiveFailures = 3;
        /**
         * Work-queue mode: a change check's {@code git ls-remote} is killed (with its transport
         * helpers) after this many seconds, so an unreachable remote stalls only its own shard.
         */
        private int lsRemoteTimeoutSeconds = 30;
        /** Work-queue mode: the disk guard ({@code cluster.writer.disk}). */
        private DiskConfig disk = new DiskConfig();

        static final int MIN_LEASE_TTL_SECONDS = 5;

        public void validate() {
            if (leaseTtlSeconds < MIN_LEASE_TTL_SECONDS) {
                throw new IllegalStateException("cluster.writer.leaseTtlSeconds must be at least "
                        + MIN_LEASE_TTL_SECONDS + ", got " + leaseTtlSeconds);
            }
            if (workDir == null) {
                throw new IllegalStateException("cluster.writer.workDir must not be empty");
            }
            if (pollSeconds <= 0) {
                throw new IllegalStateException("cluster.writer.pollSeconds must be positive, got " + pollSeconds);
            }
            if (minHoldSeconds < 0) {
                throw new IllegalStateException("cluster.writer.minHoldSeconds must not be negative, got " + minHoldSeconds);
            }
            if (maxConsecutiveFailures < 1) {
                throw new IllegalStateException("cluster.writer.maxConsecutiveFailures must be at least 1, got "
                        + maxConsecutiveFailures);
            }
            if (lsRemoteTimeoutSeconds < 1) {
                throw new IllegalStateException("cluster.writer.lsRemoteTimeoutSeconds must be at least 1, got "
                        + lsRemoteTimeoutSeconds);
            }
            if (disk == null) disk = new DiskConfig();
            disk.validate();
        }

        public Path getWorkDir() { return workDir; }
        public void setWorkDir(Path workDir) { this.workDir = workDir; }
        public int getPollSeconds() { return pollSeconds; }
        public void setPollSeconds(int v) { this.pollSeconds = v; }
        /**
         * The effective work-queue mode: the configured value (default true), and always false with
         * {@code cluster.backend: peer-pull}.
         */
        @com.fasterxml.jackson.annotation.JsonIgnore
        public boolean isWorkQueue() { return !peerMode && (workQueue == null || workQueue); }
        /** The configured value; null when unset (the default applies). */
        @com.fasterxml.jackson.annotation.JsonProperty("workQueue")
        public Boolean getWorkQueue() { return workQueue; }
        @com.fasterxml.jackson.annotation.JsonProperty("workQueue")
        public void setWorkQueue(Boolean v) { this.workQueue = v; }
        public int getLeaseTtlSeconds() { return leaseTtlSeconds; }
        public void setLeaseTtlSeconds(int v) { this.leaseTtlSeconds = v; }
        public int getMinHoldSeconds() { return minHoldSeconds; }
        public void setMinHoldSeconds(int v) { this.minHoldSeconds = v; }
        public int getMaxConsecutiveFailures() { return maxConsecutiveFailures; }
        public void setMaxConsecutiveFailures(int v) { this.maxConsecutiveFailures = v; }
        public int getLsRemoteTimeoutSeconds() { return lsRemoteTimeoutSeconds; }
        public void setLsRemoteTimeoutSeconds(int v) { this.lsRemoteTimeoutSeconds = v; }
        public DiskConfig getDisk() { return disk; }
        public void setDisk(DiskConfig disk) { this.disk = disk == null ? new DiskConfig() : disk; }

        /** {@code leaseTtlSeconds}. */
        public java.time.Duration leaseTtl() { return java.time.Duration.ofSeconds(leaseTtlSeconds); }

        /** How often a held lease is renewed: TTL/3. */
        public java.time.Duration leaseRenewInterval() { return leaseTtl().dividedBy(3); }
    }

    /**
     * {@code cluster.writer.disk}: the writer disk guard. A watched file store is <i>low</i> when its
     * free space is below {@code minFreeBytes} or below {@code minFreePercent} of its size; it is
     * no longer low once free space is back above that threshold plus {@code hysteresisBytes}.
     * The guard probes every {@code checkSeconds}. {@code minFreePercent} defaults to 0: a
     * percentage of a large disk (10 % of 2 TB is 200 GB) left upgraded writers permanently low.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DiskConfig {
        private long minFreeBytes = 5L << 30;
        private int minFreePercent = 0;
        private int checkSeconds = 15;
        private long hysteresisBytes = 1L << 30;

        public void validate() {
            if (minFreeBytes < 0) {
                throw new IllegalStateException("cluster.writer.disk.minFreeBytes must not be negative, got " + minFreeBytes);
            }
            if (minFreePercent < 0 || minFreePercent > 100) {
                throw new IllegalStateException("cluster.writer.disk.minFreePercent must be between 0 and 100, got "
                        + minFreePercent);
            }
            if (checkSeconds <= 0) {
                throw new IllegalStateException("cluster.writer.disk.checkSeconds must be positive, got " + checkSeconds);
            }
            if (hysteresisBytes < 0) {
                throw new IllegalStateException("cluster.writer.disk.hysteresisBytes must not be negative, got "
                        + hysteresisBytes);
            }
        }

        public long getMinFreeBytes() { return minFreeBytes; }
        public void setMinFreeBytes(long v) { this.minFreeBytes = v; }
        public int getMinFreePercent() { return minFreePercent; }
        public void setMinFreePercent(int v) { this.minFreePercent = v; }
        public int getCheckSeconds() { return checkSeconds; }
        public void setCheckSeconds(int v) { this.checkSeconds = v; }
        public long getHysteresisBytes() { return hysteresisBytes; }
        public void setHysteresisBytes(long v) { this.hysteresisBytes = v; }
    }

    /**
     * {@code cluster.peers}: the peer-pull backend.
     * <ul>
     *   <li>{@code discovery}: {@code dns:<name>[:port]} or {@code seeds:<url>,<url>} (readers);</li>
     *   <li>{@code refreshSeconds} (15): how often readers re-resolve discovery and re-read catalogs;</li>
     *   <li>{@code maxTransfersPerWriter} (8): concurrent {@code /replication/<shard>/files} transfers a
     *       writer serves; more get 503 with {@code Retry-After};</li>
     *   <li>{@code longPollSeconds} (30): how long a reader's {@code latest} long-poll waits;</li>
     *   <li>{@code maxLongPolls} (256): concurrent long-polls a writer holds; more get 503;</li>
     *   <li>{@code storeDir}: where a writer keeps its published generations ({@code LATEST},
     *       manifests and content-named files) for readers to pull; on the same file system as the
     *       index directory, Lucene files are hard-linked rather than copied.</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PeersConfig {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String discovery;
        private int refreshSeconds = 15;
        private int maxTransfersPerWriter = 8;
        private int longPollSeconds = 30;
        private int maxLongPolls = 256;
        private Path storeDir = IndexConfig.DEFAULT_BASE.resolve("peer-store");

        /** The longest {@code wait} a writer accepts on {@code latest}. */
        public static final int MAX_LONG_POLL_SECONDS = 300;

        void validate() {
            if (refreshSeconds < 1) {
                throw new IllegalStateException("cluster.peers.refreshSeconds must be at least 1, got " + refreshSeconds);
            }
            if (maxTransfersPerWriter < 1) {
                throw new IllegalStateException("cluster.peers.maxTransfersPerWriter must be at least 1, got "
                        + maxTransfersPerWriter);
            }
            if (longPollSeconds < 1 || longPollSeconds > MAX_LONG_POLL_SECONDS) {
                throw new IllegalStateException("cluster.peers.longPollSeconds must be between 1 and "
                        + MAX_LONG_POLL_SECONDS + ", got " + longPollSeconds);
            }
            if (maxLongPolls < 1) {
                throw new IllegalStateException("cluster.peers.maxLongPolls must be at least 1, got " + maxLongPolls);
            }
            if (storeDir == null) {
                throw new IllegalStateException("cluster.peers.storeDir must not be empty");
            }
        }

        public String getDiscovery() { return discovery; }
        public void setDiscovery(String v) { this.discovery = v; }
        public int getRefreshSeconds() { return refreshSeconds; }
        public void setRefreshSeconds(int v) { this.refreshSeconds = v; }
        public int getMaxTransfersPerWriter() { return maxTransfersPerWriter; }
        public void setMaxTransfersPerWriter(int v) { this.maxTransfersPerWriter = v; }
        public int getLongPollSeconds() { return longPollSeconds; }
        public void setLongPollSeconds(int v) { this.longPollSeconds = v; }
        public int getMaxLongPolls() { return maxLongPolls; }
        public void setMaxLongPolls(int v) { this.maxLongPolls = v; }
        public Path getStoreDir() { return storeDir; }
        public void setStoreDir(Path v) { this.storeDir = v; }
    }

    /**
     * {@code cluster.tls}: {@code keystore} (PKCS12 or JKS, by {@code keystoreType}, default PKCS12)
     * makes the web server listen with TLS only; {@code truststore} is what this node's clients trust
     * (other nodes' certificates). Both are opened with the password in the environment variable
     * named by {@code passwordEnv}: the password itself never goes into config.json.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TlsConfig {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private Path keystore;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private Path truststore;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String passwordEnv;
        private String keystoreType = "PKCS12";

        void validate() {
            if ((keystore != null || truststore != null) && (passwordEnv == null || passwordEnv.isBlank())) {
                throw new IllegalStateException("cluster.tls.passwordEnv is required with cluster.tls.keystore or"
                        + " cluster.tls.truststore (the name of the environment variable holding the password)");
            }
            if (keystoreType == null || keystoreType.isBlank()) keystoreType = "PKCS12";
        }

        /**
         * The keystore password from the environment variable {@code passwordEnv}.
         *
         * @throws IllegalStateException when the variable is unset (never names the value)
         */
        public char[] password() {
            String v = passwordEnv == null ? null : System.getenv(passwordEnv);
            if (v == null) {
                throw new IllegalStateException("cluster.tls.passwordEnv names environment variable '" + passwordEnv
                        + "', which is not set");
            }
            return v.toCharArray();
        }

        public Path getKeystore() { return keystore; }
        public void setKeystore(Path v) { this.keystore = v; }
        public Path getTruststore() { return truststore; }
        public void setTruststore(Path v) { this.truststore = v; }
        public String getPasswordEnv() { return passwordEnv; }
        public void setPasswordEnv(String v) { this.passwordEnv = v; }
        public String getKeystoreType() { return keystoreType; }
        public void setKeystoreType(String v) { this.keystoreType = v; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ObjectStoreConfig {
        private String endpoint;
        private String region = "us-east-1";
        private String bucket;
        private String prefix = "pharos";
        private boolean pathStyle = true;
        private String accessKeyEnv;
        private String secretKeyEnv;
        /**
         * Bound of one attempt of a reader's ranged read (and the HTTP client's connect and idle-read
         * timeouts); a whole call, retries included, gets {@value #READ_ATTEMPTS} times this.
         */
        private int readTimeoutSeconds = 10;

        static final int READ_ATTEMPTS = 3;
        static final int MAX_READ_TIMEOUT_SECONDS = 3600;

        /** @throws IllegalStateException naming the offending field */
        void validate() {
            if (readTimeoutSeconds < 1 || readTimeoutSeconds > MAX_READ_TIMEOUT_SECONDS) {
                throw new IllegalStateException("cluster.objectStore.readTimeoutSeconds must be between 1 and "
                        + MAX_READ_TIMEOUT_SECONDS + ", got " + readTimeoutSeconds);
            }
        }

        public int getReadTimeoutSeconds() { return readTimeoutSeconds; }
        public void setReadTimeoutSeconds(int v) { this.readTimeoutSeconds = v; }

        /** One attempt of a ranged read: {@code readTimeoutSeconds}. */
        public java.time.Duration readAttemptTimeout() {
            return java.time.Duration.ofSeconds(readTimeoutSeconds);
        }

        /** A whole ranged read, retries included (and a block-cache waiter): 3 × {@code readTimeoutSeconds}. */
        public java.time.Duration readTotalTimeout() {
            return java.time.Duration.ofSeconds((long) READ_ATTEMPTS * readTimeoutSeconds);
        }

        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }
        public String getPrefix() { return prefix; }
        public void setPrefix(String prefix) { this.prefix = prefix; }
        public boolean isPathStyle() { return pathStyle; }
        public void setPathStyle(boolean pathStyle) { this.pathStyle = pathStyle; }
        public String getAccessKeyEnv() { return accessKeyEnv; }
        public void setAccessKeyEnv(String v) { this.accessKeyEnv = v; }
        public String getSecretKeyEnv() { return secretKeyEnv; }
        public void setSecretKeyEnv(String v) { this.secretKeyEnv = v; }
    }

    /**
     * {@code cluster.status}: what {@code /cluster/status} proposes and when.
     * <ul>
     *   <li>{@code targets.warmP95Millis} (default 500): a reader's p95 request latency target;</li>
     *   <li>{@code targets.qpsPerReader} (default 50): the query rate one reader is sized for;</li>
     *   <li>{@code targets.backlog} (default 600): seconds an unpublished source change may wait;</li>
     *   <li>{@code windows.*Seconds}: how long a rule's condition must hold before its proposal is
     *       raised (add readers 300, remove readers 1800, add writers 0, remove writers 1800, reshard 0,
     *       writer disk 0), and {@code windows.clearGraceSeconds} (default 60) how long it must be
     *       false before a raised proposal clears (or a pending condition resets);</li>
     *   <li>{@code eventsKeepAliveSeconds} (default 15): the SSE keep-alive comment interval of
     *       {@code /cluster/events} (the router cuts a stream idle for twice that);</li>
     *   <li>{@code webhookUrl} (optional): receives every event as a JSON POST.</li>
     * </ul>
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StatusConfig {
        private Targets targets = new Targets();
        private Windows windows = new Windows();
        private int eventsKeepAliveSeconds = 15;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String webhookUrl;

        static final int MAX_KEEP_ALIVE_SECONDS = 300;

        public void validate() {
            if (targets == null) targets = new Targets();
            if (windows == null) windows = new Windows();
            targets.validate();
            windows.validate();
            if (eventsKeepAliveSeconds < 1 || eventsKeepAliveSeconds > MAX_KEEP_ALIVE_SECONDS) {
                throw new IllegalStateException("cluster.status.eventsKeepAliveSeconds must be between 1 and "
                        + MAX_KEEP_ALIVE_SECONDS + ", got " + eventsKeepAliveSeconds);
            }
            if (webhookUrl != null && !webhookUrl.isBlank()) {
                try {
                    java.net.URI u = new java.net.URI(webhookUrl.trim());
                    String scheme = u.getScheme();
                    if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))
                            || u.getHost() == null) {
                        throw new IllegalStateException("cluster.status.webhookUrl must be an http:// or https:// URL with a host");
                    }
                } catch (java.net.URISyntaxException e) {
                    // never echo the value: it may carry a secret
                    throw new IllegalStateException("cluster.status.webhookUrl is not a valid URL");
                }
            }
        }

        public Targets getTargets() { return targets; }
        public void setTargets(Targets t) { this.targets = t == null ? new Targets() : t; }
        public Windows getWindows() { return windows; }
        public void setWindows(Windows w) { this.windows = w == null ? new Windows() : w; }
        public int getEventsKeepAliveSeconds() { return eventsKeepAliveSeconds; }
        public void setEventsKeepAliveSeconds(int v) { this.eventsKeepAliveSeconds = v; }
        public String getWebhookUrl() { return webhookUrl; }
        public void setWebhookUrl(String v) { this.webhookUrl = v; }
    }

    /** {@code cluster.status.targets}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Targets {
        private long warmP95Millis = 500;
        private double qpsPerReader = 50;
        /** Seconds the oldest unpublished source change of a shard may wait. */
        private long backlog = 600;

        void validate() {
            if (warmP95Millis <= 0) {
                throw new IllegalStateException("cluster.status.targets.warmP95Millis must be positive, got " + warmP95Millis);
            }
            if (!(qpsPerReader > 0) || Double.isInfinite(qpsPerReader)) {
                throw new IllegalStateException("cluster.status.targets.qpsPerReader must be positive, got " + qpsPerReader);
            }
            if (backlog <= 0) {
                throw new IllegalStateException("cluster.status.targets.backlog must be positive (seconds), got " + backlog);
            }
        }

        public long getWarmP95Millis() { return warmP95Millis; }
        public void setWarmP95Millis(long v) { this.warmP95Millis = v; }
        public double getQpsPerReader() { return qpsPerReader; }
        public void setQpsPerReader(double v) { this.qpsPerReader = v; }
        public long getBacklog() { return backlog; }
        public void setBacklog(long v) { this.backlog = v; }
    }

    /** {@code cluster.status.windows}: rule durations in seconds (shortened by the Phase 4 gate). */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Windows {
        private long addReadersSeconds = 300;
        private long removeReadersSeconds = 1800;
        private long addWritersSeconds = 0;
        private long removeWritersSeconds = 1800;
        private long reshardSeconds = 0;
        private long writerDiskSeconds = 0;
        private long clearGraceSeconds = 60;

        void validate() {
            long[] all = {addReadersSeconds, removeReadersSeconds, addWritersSeconds, removeWritersSeconds,
                    reshardSeconds, writerDiskSeconds, clearGraceSeconds};
            for (long v : all) {
                if (v < 0) throw new IllegalStateException("cluster.status.windows.* must not be negative, got " + v);
            }
        }

        public long getAddReadersSeconds() { return addReadersSeconds; }
        public void setAddReadersSeconds(long v) { this.addReadersSeconds = v; }
        public long getRemoveReadersSeconds() { return removeReadersSeconds; }
        public void setRemoveReadersSeconds(long v) { this.removeReadersSeconds = v; }
        public long getAddWritersSeconds() { return addWritersSeconds; }
        public void setAddWritersSeconds(long v) { this.addWritersSeconds = v; }
        public long getRemoveWritersSeconds() { return removeWritersSeconds; }
        public void setRemoveWritersSeconds(long v) { this.removeWritersSeconds = v; }
        public long getReshardSeconds() { return reshardSeconds; }
        public void setReshardSeconds(long v) { this.reshardSeconds = v; }
        public long getWriterDiskSeconds() { return writerDiskSeconds; }
        public void setWriterDiskSeconds(long v) { this.writerDiskSeconds = v; }
        public long getClearGraceSeconds() { return clearGraceSeconds; }
        public void setClearGraceSeconds(long v) { this.clearGraceSeconds = v; }
    }
}
