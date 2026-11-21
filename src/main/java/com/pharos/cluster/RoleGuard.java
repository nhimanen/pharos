package com.pharos.cluster;

import com.pharos.config.IndexConfig;

/** Refuses commands that the node's {@code cluster.role} must not run. */
public final class RoleGuard {

    static final String READER_INDEXING = "this node has role reader; run indexing on a writer node";
    static final String READER_UPGRADES =
            "this node has role reader; readers receive upgrades by syncing; upgrade the writer";

    private RoleGuard() {}

    /** Indexing, linking, embedding, mining and removing indexes happen on writers (and {@code all}). */
    public static void requireIndexing(IndexConfig config) {
        if (config.getCluster().getRole() == NodeRole.READER) {
            throw new IllegalStateException(READER_INDEXING);
        }
    }

    /**
     * Index format upgrades re-index, so they happen where indexing does. A reader's generations
     * are replaced by syncing once the writer has upgraded and re-published them.
     */
    public static void requireUpgrades(IndexConfig config) {
        if (config.getCluster().getRole() == NodeRole.READER) {
            throw new IllegalStateException(READER_UPGRADES);
        }
    }

    /**
     * Commands that change a project's local index outside an index run ({@code upgrade-indexes},
     * {@code remove-index}, {@code embed}) are refused on a work-queue writer
     * ({@code cluster.writer.workQueue: true}), as {@code index} and {@code publish} are: the work
     * loop of {@code pharos web} owns those indexes, and it upgrades them itself (an outdated index
     * format, chunking version or model makes its shard's next run a full one).
     */
    public static void requireNoWorkQueue(IndexConfig config, String command) {
        com.pharos.config.ClusterConfig c = config.getCluster();
        if (c.getRole() == NodeRole.WRITER && c.getWriter().isWorkQueue()) {
            throw new IllegalStateException(workQueueRefusal(command));
        }
    }

    /** Why {@code command} is refused on a work-queue writer. */
    public static String workQueueRefusal(String command) {
        return "Not running '" + command + "': this writer runs in work-queue mode (cluster.writer.workQueue: true):"
                + " shards are indexed, upgraded and published by the work loop of 'pharos web' on the writer holding"
                + " the lease; to upgrade, restart the writers on the new release. Set cluster.writer.workQueue to"
                + " false to index and publish by hand.";
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
