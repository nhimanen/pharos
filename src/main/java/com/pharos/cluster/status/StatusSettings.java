package com.pharos.cluster.status;

import com.pharos.config.ClusterConfig;

/**
 * The numbers the proposal rules use ({@code cluster.status.targets}, {@code .windows},
 * {@code .removeReadersLoad} and {@code .writers}), in seconds and milliseconds.
 *
 * @param backlogSeconds       {@code targets.backlog}: a pending shard older than this is
 *                             {@code shard-lagging} (health)
 * @param removeReadersLoad    remove readers below this share of {@code qpsPerReader}
 * @param backlogShards        add writers when at least this many shards are pending for longer than
 * @param oldestPendingSeconds this many seconds
 * @param removeWritersSeconds the remove-writers window: {@code writers.idleSeconds}, else
 *                             {@code windows.removeWritersSeconds}
 */
public record StatusSettings(long warmP95Millis, double qpsPerReader, long backlogSeconds,
                             long addReadersSeconds, long removeReadersSeconds, long addWritersSeconds,
                             long removeWritersSeconds, long reshardSeconds, long writerDiskSeconds,
                             long clearGraceSeconds, double removeReadersLoad, int backlogShards,
                             long oldestPendingSeconds) {
    /** Reshard a shard above this share of a reader's cache cap. */
    public static final double RESHARD_CACHE_SHARE = 0.5;

    public static StatusSettings defaults() {
        return of(new ClusterConfig.StatusConfig());
    }

    public static StatusSettings of(ClusterConfig.StatusConfig c) {
        ClusterConfig.Targets t = c.getTargets();
        ClusterConfig.Windows w = c.getWindows();
        ClusterConfig.StatusWriters sw = c.getWriters();
        return new StatusSettings(t.getWarmP95Millis(), t.getQpsPerReader(), t.getBacklog(),
                w.getAddReadersSeconds(), w.getRemoveReadersSeconds(), w.getAddWritersSeconds(),
                c.effectiveRemoveWritersSeconds(), w.getReshardSeconds(), w.getWriterDiskSeconds(),
                w.getClearGraceSeconds(), c.getRemoveReadersLoad(), sw.getBacklogShards(),
                sw.getOldestPendingSeconds());
    }

    /** The window of a rule kind ({@link Proposals}). */
    public long window(String kind) {
        return switch (kind) {
            case Proposals.ADD_READERS -> addReadersSeconds;
            case Proposals.REMOVE_READERS -> removeReadersSeconds;
            case Proposals.ADD_WRITERS -> addWritersSeconds;
            case Proposals.REMOVE_WRITERS -> removeWritersSeconds;
            case Proposals.RESHARD -> reshardSeconds;
            case Proposals.WRITER_DISK -> writerDiskSeconds;
            default -> 0;
        };
    }
}
