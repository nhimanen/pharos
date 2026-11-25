package com.pharos.cluster.status;

import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.status.ClusterStatus.Backlog;
import com.pharos.cluster.status.ClusterStatus.Health;
import com.pharos.cluster.status.StatusSnapshot.ReaderObs;
import com.pharos.cluster.status.StatusSnapshot.ShardObs;
import com.pharos.cluster.status.StatusSnapshot.WriterObs;
import com.pharos.cluster.writer.WriterHeartbeat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Builds {@link ClusterStatus} from a snapshot, the shared {@link StatusState} and the settings: a
 * pure function, so any node given the same inputs gives byte-identical JSON. Proposals are those
 * the state holds in force (hysteresis is {@link Proposals#advance}'s job), with the snapshot's
 * metrics.
 *
 * <p>Health findings, sorted by kind then target:
 * {@code lease-expired}, {@code no-readers}, {@code reader-degraded}, {@code reader-draining},
 * {@code reader-warming}, {@code shard-lagging} (oldest unpublished change older than
 * {@code targets.backlog}), {@code shard-unassigned} (a shard with a source no live work-queue
 * writer holds, while some run), {@code shard-unserved} (published, no live reader serves it),
 * {@code writer-low-disk} ("writer X low on disk") and {@code writer-stopping}.
 */
public final class StatusBuilder {

    /** Every shard is keyed by project until custom shard keys (Phase 6). */
    public static final int SPEC_VERSION = 1;

    private StatusBuilder() {}

    public static ClusterStatus build(StatusSnapshot s, StatusState state, StatusSettings cfg) {
        StatusState st = state == null ? StatusState.EMPTY : state;
        long now = s.asOf().getEpochSecond();
        List<Health> health = new ArrayList<>();
        boolean cluster = StatusSnapshot.CLUSTER.equals(s.mode());
        boolean workQueueWriters = s.writers().stream()
                .anyMatch(w -> w.heartbeat().workQueue() && !w.heartbeat().stopping());

        List<ClusterStatus.Shard> shards = new ArrayList<>();
        int pendingCount = 0;
        Long oldest = null;
        for (ShardObs sh : s.shards()) {
            ClusterStatus.Lease lease = null;
            boolean leaseValid = false;
            if (sh.lease() != null) {
                long age = s.ageSeconds(sh.lease().lastModified());
                boolean expired = age > s.leaseTtl().toSeconds();
                lease = new ClusterStatus.Lease(sh.lease().writer(), age, expired, sh.lease().released());
                leaseValid = !expired && !sh.lease().released();
                if (expired && !sh.lease().released()) {
                    health.add(new Health("lease-expired", sh.shard(), "lease of shard " + sh.shard() + " held by "
                            + sh.lease().writer() + " expired (last renewed " + age + " s ago)"));
                }
            }
            Optional<WriterObs> rep = Proposals.reporter(s, sh);
            WriterHeartbeat.Backlog reported = rep.map(w -> w.heartbeat().backlog().get(sh.shard())).orElse(null);
            Long since = Proposals.pendingSince(s, st, sh);
            Long pendingAge = since == null ? null : Math.max(0, now - since);
            Backlog backlog = null;
            if (reported != null) {
                backlog = new Backlog(reported.sourceRevision(), reported.publishedRevision(), reported.pending(),
                        reported.pending() ? pendingAge : null);
            } else if (since != null) { // last seen pending; no live writer reports it now
                backlog = new Backlog(null, sh.publishedRevision(), true, pendingAge);
            }
            if (pendingAge != null) {
                pendingCount++;
                oldest = oldest == null ? pendingAge : Math.max(oldest, pendingAge);
                if (pendingAge > cfg.backlogSeconds()) {
                    health.add(new Health("shard-lagging", sh.shard(), "shard " + sh.shard()
                            + " has a source change unpublished for " + pendingAge + " s (target "
                            + cfg.backlogSeconds() + " s)"));
                }
            }
            List<String> readers = new ArrayList<>();
            for (ReaderObs r : s.readers()) {
                if (r.heartbeat().shards().contains(sh.shard())) readers.add(r.heartbeat().node());
            }
            if (cluster && sh.generation() != null && readers.isEmpty() && !s.readers().isEmpty()) {
                health.add(new Health("shard-unserved", sh.shard(), "shard " + sh.shard()
                        + " is not served by any live reader"));
            }
            if (cluster && workQueueWriters && sh.hasSource() && !leaseValid && rep.isEmpty()) {
                health.add(new Health("shard-unassigned", sh.shard(), "shard " + sh.shard()
                        + " has no live writer holding its lease"));
            }
            String shardState = lease != null && lease.expired() && !lease.released() ? "lease-expired"
                    : pendingAge != null ? "lagging"
                    : sh.generation() == null && cluster ? "unpublished" : "current";
            Map<String, String> keys = new TreeMap<>();
            keys.put("project", sh.shard());
            shards.add(new ClusterStatus.Shard(sh.shard(), SPEC_VERSION, keys, sh.docCount(), sh.sizeBytes(),
                    sh.generation(), s.ageSeconds(sh.publishedAt()), sh.writerNode(), lease, readers, backlog,
                    shardState));
        }

        List<ClusterStatus.Writer> writers = new ArrayList<>();
        for (WriterObs w : s.writers()) {
            WriterHeartbeat h = w.heartbeat();
            writers.add(new ClusterStatus.Writer(h.node(), h.workQueue(), h.leases(), h.pinned(), h.indexing(),
                    h.startedAt(), h.stopping(), h.disk(), s.ageSeconds(w.lastModified())));
            if (h.lowOnDisk()) health.add(new Health("writer-low-disk", h.node(), "writer " + h.node() + " low on disk"));
            if (h.stopping()) health.add(new Health("writer-stopping", h.node(), "writer " + h.node() + " is stopping"));
        }

        List<ClusterStatus.Reader> readers = new ArrayList<>();
        for (ReaderObs r : s.readers()) {
            ReaderHeartbeat h = r.heartbeat();
            readers.add(new ClusterStatus.Reader(h.node(), h.advertiseUrl(), h.shards(), h.state(), h.degradedShards(),
                    h.readerCache() == null ? null : new TreeMap<>(h.readerCache()), h.inFlight(), h.qps(),
                    h.startedAt(), h.warmP95Millis(), h.catalogAgeSeconds(), s.ageSeconds(r.lastModified())));
            switch (h.state()) {
                case ReaderHeartbeat.DRAINING -> health.add(new Health("reader-draining", h.node(),
                        "reader " + h.node() + " is draining"));
                case ReaderHeartbeat.WARMING -> health.add(new Health("reader-warming", h.node(),
                        "reader " + h.node() + " is warming up"));
                default -> { }
            }
            if (!h.degradedShards().isEmpty()) {
                health.add(new Health("reader-degraded", h.node(), "reader " + h.node()
                        + " can't fully serve shard(s) " + String.join(", ", h.degradedShards())));
            }
        }
        if (cluster && s.readers().isEmpty() && !s.shards().isEmpty()) {
            health.add(new Health("no-readers", "readers", "no live reader"));
        }
        health.sort(Comparator.comparing(Health::kind).thenComparing(Health::target));

        Map<String, Proposals.Candidate> cands = Proposals.candidates(s, st, cfg);
        return new ClusterStatus(s.mode(), s.asOf().toString(),
                new ClusterStatus.Layout(Proposals.summary(s), shards, writers, readers),
                new ClusterStatus.BacklogSummary(pendingCount, oldest), health, Proposals.inForce(st, cands));
    }
}
