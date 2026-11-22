package com.pharos.operator.render;

import com.pharos.operator.Names;
import com.pharos.operator.OperatorSettings;
import com.pharos.operator.api.PharosCluster;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.random.RandomGenerator;

/**
 * Everything the operator wants to exist for one cluster, keyed by dependent-resource name. It is
 * rendered once per reconcile and handed to the dependents through the reconcile context.
 *
 * <p>The two Secrets are not in it: their values are random and must be generated only when the
 * Secret does not exist yet, so the Secret dependents render them on create ({@link #secret}).
 */
public record DesiredState(Map<String, HasMetadata> objects) {

    /** The context key the reconciler stores the desired state under. */
    public static final String KEY = "pharos.desiredState";

    /** Dependent names. */
    public static final String TOKEN = "token";
    public static final String MINIO_CREDENTIALS = "minio-credentials";
    public static final String CONFIG_WRITER = "config-writer";
    public static final String CONFIG_READER = "config-reader";
    public static final String CONFIG_ROUTER = "config-router";
    public static final String WRITERS = "writers";
    public static final String READERS = "readers";
    public static final String ROUTER = "router";
    public static final String WRITERS_HEADLESS = "writers-headless";
    public static final String READERS_HEADLESS = "readers-headless";
    public static final String ROUTER_SERVICE = "router-service";
    public static final String PDB_WRITERS = "pdb-writers";
    public static final String PDB_READERS = "pdb-readers";
    public static final String NETWORK_POLICY = "network-policy";
    public static final String MINIO = "minio";
    public static final String MINIO_SERVICE = "minio-service";
    public static final String MINIO_PVC = "minio-pvc";
    public static final String MINIO_BUCKET = "minio-bucket";

    public DesiredState {
        objects = Map.copyOf(objects);
    }

    /** Renders the desired objects of a valid spec whose configs rendered without errors. */
    public static DesiredState render(PharosCluster cluster, ConfigRenderer.Result configs, OperatorSettings settings,
                                      int writerReplicas, int readerReplicas) {
        Map<String, HasMetadata> objects = new LinkedHashMap<>();
        objects.putAll(WorkloadRenderer.render(cluster, configs, settings, writerReplicas, readerReplicas));
        objects.putAll(StorageRenderer.render(cluster, settings));
        return new DesiredState(objects);
    }

    /** The desired object of dependent {@code key}, or null when it has none (e.g. minio objects in s3 mode). */
    public <T extends HasMetadata> T get(String key, Class<T> type) {
        HasMetadata o = objects.get(key);
        return o == null ? null : type.cast(o);
    }

    /** A Secret with fresh random values; only called when it does not exist yet. */
    public static Secret secret(String key, PharosCluster cluster, RandomGenerator random) {
        return switch (key) {
            case TOKEN -> WorkloadRenderer.tokenSecret(cluster, random);
            case MINIO_CREDENTIALS -> StorageRenderer.minioCredentials(cluster, random);
            default -> throw new IllegalArgumentException("not a Secret dependent: " + key);
        };
    }

    /** The name of Secret dependent {@code key}'s object. */
    public static String secretName(String key, PharosCluster cluster) {
        String name = cluster.getMetadata().getName();
        return switch (key) {
            case TOKEN -> Names.token(name);
            case MINIO_CREDENTIALS -> Names.minio(name);
            default -> throw new IllegalArgumentException("not a Secret dependent: " + key);
        };
    }
}
