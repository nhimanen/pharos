package com.pharos.cluster;

import com.pharos.config.IndexConfig;

public final class Backends {
    private Backends() {}

    public static ReplicationBackend create(IndexConfig config) {
        BackendKind kind = config.getCluster().getBackend();
        return switch (kind) {
            case LOCAL -> new LocalBackend(config);
            case OBJECT_STORE -> throw new UnsupportedOperationException(
                    "object-store backend is not available yet");
        };
    }
}
