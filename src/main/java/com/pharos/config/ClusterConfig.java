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
