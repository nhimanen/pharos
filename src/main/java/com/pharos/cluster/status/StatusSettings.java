package com.pharos.cluster.status;

import com.pharos.config.ClusterConfig;

/**
 * The numbers the proposal rules use ({@code cluster.status.targets} and {@code .windows}), in
 * seconds and milliseconds.
 */
public record StatusSettings(long warmP95Millis, double qpsPerReader, long backlogSeconds,
                             long addReadersSeconds, long removeReadersSeconds, long addWritersSeconds,
                             long removeWritersSeconds, long reshardSeconds, long writerDiskSeconds,
                             long clearGraceSeconds) {

    /** Remove readers below this share of {@code qpsPerReader}. */
    public static final double REMOVE_READERS_LOAD = 0.2;
    /** Reshard a shard above this share of a reader's cache cap. */
    public static final double RESHARD_CACHE_SHARE = 0.5;

    public static StatusSettings defaults() {
        return of(new ClusterConfig.StatusConfig());
    }

    public static StatusSettings of(ClusterConfig.StatusConfig c) {
        ClusterConfig.Targets t = c.getTargets();
        ClusterConfig.Windows w = c.getWindows();
        return new StatusSettings(t.getWarmP95Millis(), t.getQpsPerReader(), t.getBacklog(),
                w.getAddReadersSeconds(), w.getRemoveReadersSeconds(), w.getAddWritersSeconds(),
                w.getRemoveWritersSeconds(), w.getReshardSeconds(), w.getWriterDiskSeconds(),
                w.getClearGraceSeconds());
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
