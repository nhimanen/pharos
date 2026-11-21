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
        if (role != NodeRole.ALL && backend != BackendKind.OBJECT_STORE) {
            throw new IllegalStateException("cluster.role '" + role.json() + "' requires cluster.backend 'object-store'");
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
    public void setBackend(BackendKind backend) { this.backend = backend; }
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
    public void setWriter(WriterConfig writer) { this.writer = writer == null ? new WriterConfig() : writer; }
    public void setReader(ReaderConfig reader) { this.reader = reader; }
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
        /** Work-queue mode (leases and takeover). Opt-in; nothing reads it before the work-queue task. */
        private boolean workQueue = false;
        /**
         * Lease TTL in work-queue mode. A lease whose last write (by the bucket's LastModified) is
         * older than this may be taken over; the holder renews every TTL/3.
         */
        private int leaseTtlSeconds = 30;

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
        }

        public Path getWorkDir() { return workDir; }
        public void setWorkDir(Path workDir) { this.workDir = workDir; }
        public int getPollSeconds() { return pollSeconds; }
        public void setPollSeconds(int v) { this.pollSeconds = v; }
        public boolean isWorkQueue() { return workQueue; }
        public void setWorkQueue(boolean v) { this.workQueue = v; }
        public int getLeaseTtlSeconds() { return leaseTtlSeconds; }
        public void setLeaseTtlSeconds(int v) { this.leaseTtlSeconds = v; }

        /** {@code leaseTtlSeconds}. */
        public java.time.Duration leaseTtl() { return java.time.Duration.ofSeconds(leaseTtlSeconds); }

        /** How often a held lease is renewed: TTL/3. */
        public java.time.Duration leaseRenewInterval() { return leaseTtl().dividedBy(3); }
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
}
