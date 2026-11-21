package com.pharos.cluster.status;

import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.writer.WriterHeartbeat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Everything {@code /cluster/status} is built from, as read from the bucket (or from local state on a
 * single node): the catalog's shards with their {@code LATEST} manifest and lease, and the live
 * writer and reader heartbeats with their bucket LastModified. Nothing in it is node-local, so two
 * nodes that read the same objects hold equal snapshots.
 *
 * <p>{@code asOf} is the bucket's "now" of the snapshot: the newest LastModified among the objects
 * read (heartbeats, leases, {@code LATEST}s), truncated to seconds. Every age in the status is
 * measured against it, never against a node clock.
 *
 * @param mode     {@value #CLUSTER} or {@value #SINGLE_NODE}
 * @param leaseTtl the writer lease TTL ({@code cluster.writer.leaseTtlSeconds}) expiry is judged by
 */
public record StatusSnapshot(String mode, Instant asOf, List<ShardObs> shards, List<WriterObs> writers,
                             List<ReaderObs> readers, Duration leaseTtl) {

    public static final String CLUSTER = "cluster";
    public static final String SINGLE_NODE = "single-node";

    public StatusSnapshot {
        Objects.requireNonNull(mode, "mode");
        asOf = Objects.requireNonNull(asOf, "asOf").truncatedTo(ChronoUnit.SECONDS);
        shards = shards == null ? List.of()
                : shards.stream().sorted(Comparator.comparing(ShardObs::shard)).toList();
        writers = writers == null ? List.of()
                : writers.stream().sorted(Comparator.comparing((WriterObs w) -> w.heartbeat().node())).toList();
        readers = readers == null ? List.of()
                : readers.stream().sorted(Comparator.comparing((ReaderObs r) -> r.heartbeat().node())).toList();
        leaseTtl = leaseTtl == null ? Duration.ofSeconds(30) : leaseTtl;
    }

    /**
     * One catalog shard.
     *
     * @param hasSource    the catalog entry names a {@code source} (any writer can index it)
     * @param generation   the generation {@code LATEST} points at; null when none is published
     * @param publishedAt  the bucket LastModified of {@code LATEST} (single node: the last index run)
     * @param publishedRevision the source revision the published generation holds; null when unknown
     * @param lease        the shard's lease object; null when there is none
     */
    public record ShardObs(String shard, boolean hasSource, Long generation, Long docCount, Long sizeBytes,
                           String writerNode, Instant publishedAt, String publishedRevision, LeaseObs lease) {}

    /** A lease object: who holds it, its bucket LastModified, and whether it was released. */
    public record LeaseObs(String writer, Instant lastModified, boolean released) {}

    /** A live writer heartbeat and its bucket LastModified. */
    public record WriterObs(WriterHeartbeat heartbeat, Instant lastModified) {}

    /** A live reader heartbeat and its bucket LastModified. */
    public record ReaderObs(ReaderHeartbeat heartbeat, Instant lastModified) {}

    /** Whole seconds from {@code t} to {@link #asOf} (never negative); null when {@code t} is. */
    public Long ageSeconds(Instant t) {
        if (t == null) return null;
        return Math.max(0, Duration.between(t.truncatedTo(ChronoUnit.SECONDS), asOf).toSeconds());
    }
}
