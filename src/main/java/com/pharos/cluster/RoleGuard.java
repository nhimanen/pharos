package com.pharos.cluster;

import com.pharos.config.IndexConfig;

/** Refuses commands that the node's {@code cluster.role} must not run. */
public final class RoleGuard {

    static final String READER_INDEXING = "this node has role reader; run indexing on a writer node";

    private RoleGuard() {}

    /** Indexing, linking, embedding, mining and removing indexes happen on writers (and {@code all}). */
    public static void requireIndexing(IndexConfig config) {
        if (config.getCluster().getRole() == NodeRole.READER) {
            throw new IllegalStateException(READER_INDEXING);
        }
    }

    /** Publishing to the object store happens on writers only. */
    public static void requireWriter(IndexConfig config, String command) {
        NodeRole role = config.getCluster().getRole();
        if (role != NodeRole.WRITER) {
            throw new IllegalStateException("'" + command + "' requires role writer; this node has role "
                    + role.json() + " (set cluster.role and cluster.backend in config.json)");
        }
    }
}
