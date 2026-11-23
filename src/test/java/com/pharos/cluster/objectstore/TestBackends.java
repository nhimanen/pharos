package com.pharos.cluster.objectstore;

import com.pharos.cluster.ReplicationBackend;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/** Test access to the package-private seams of the object-store backend. */
public final class TestBackends {
    private TestBackends() {}

    /** An {@link ObjectStoreBackend} for {@code config} on an injected store. */
    public static ReplicationBackend objectStore(IndexConfig config, ObjectStore store) {
        return new ObjectStoreBackend(config, store);
    }

    /** An {@link S3ObjectStore} with explicit credentials. */
    public static ObjectStore s3(ClusterConfig.ObjectStoreConfig cfg, AwsCredentialsProvider credentials) {
        return new S3ObjectStore(cfg, credentials);
    }
}
