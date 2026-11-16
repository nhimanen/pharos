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
    private Path cacheDir = IndexConfig.DEFAULT_BASE.resolve("cluster-cache");
    private int retentionGenerations = 3;
    private int retentionMinutes = 60;
    private ObjectStoreConfig objectStore = new ObjectStoreConfig();
    private ReaderConfig reader = new ReaderConfig();

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
        if (role != NodeRole.ALL && nodeId != null && nodeId.isBlank()) {
            throw new IllegalStateException("cluster.nodeId must not be blank (omit it to use the host name)");
        }
        if (reader == null) reader = new ReaderConfig();
        reader.validate();
        if (backend == BackendKind.OBJECT_STORE) {
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

    public NodeRole getRole() { return role; }
    public void setRole(NodeRole role) { this.role = role; }
    public BackendKind getBackend() { return backend; }
    public void setBackend(BackendKind backend) { this.backend = backend; }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
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
    public void setReader(ReaderConfig reader) { this.reader = reader; }

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

        /** @throws IllegalStateException naming the offending field */
        public void validate() {
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
        public int getMaxConcurrentFetches() { return maxConcurrentFetches; }
        public void setMaxConcurrentFetches(int v) { this.maxConcurrentFetches = v; }
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
