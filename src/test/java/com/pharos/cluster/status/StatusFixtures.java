package com.pharos.cluster.status;

import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.writer.WriterHeartbeat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builders of snapshots and heartbeats for the status tests. */
final class StatusFixtures {

    static final Instant T0 = Instant.parse("2026-11-21T10:00:00Z");

    private StatusFixtures() {}

    static final class MutableClock extends Clock {
        private volatile Instant now;

        MutableClock(Instant now) { this.now = now; }

        void advance(Duration d) { now = now.plus(d); }

        void set(Instant t) { now = t; }

        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }

        @Override public Clock withZone(java.time.ZoneId zone) { return this; }

        @Override public Instant instant() { return now; }
    }

    static Map<String, Object> cache(long maxBytes) {
        Map<String, Object> rc = new LinkedHashMap<>();
        rc.put("mode", "lazy");
        rc.put("cacheMaxBytes", maxBytes);
        rc.put("bytesOnDisk", 0L);
        rc.put("hits", 0L);
        rc.put("misses", 0L);
        rc.put("hitRate", 0.0);
        return rc;
    }

    static ReaderHeartbeat reader(String node, double qps, Long p95, List<String> shards) {
        return new ReaderHeartbeat(node, "http://" + node + ":7171", shards, false, cache(8L << 30), 0, qps, 1L,
                ReaderHeartbeat.READY, List.of(), p95, 3L);
    }

    static ReaderHeartbeat reader(String node, double qps, Long p95) {
        return reader(node, qps, p95, List.of("a"));
    }

    static WriterHeartbeat writer(String node, List<String> leases, Map<String, WriterHeartbeat.Backlog> backlog,
                                  WriterHeartbeat.Indexing indexing, WriterHeartbeat.Disk disk) {
        return new WriterHeartbeat(node, true, leases, List.of(), backlog, indexing, 1L, false, disk);
    }

    static WriterHeartbeat idleWriter(String node, List<String> leases) {
        Map<String, WriterHeartbeat.Backlog> b = new LinkedHashMap<>();
        for (String s : leases) b.put(s, new WriterHeartbeat.Backlog("r1", "r1", false, null));
        return writer(node, leases, b, null, null);
    }

    static WriterHeartbeat.Backlog pending(long ageSeconds) {
        return new WriterHeartbeat.Backlog("r2", "r1", true, ageSeconds);
    }

    static StatusSnapshot.ShardObs shard(String id, boolean source, Long size, String leaseHolder, Instant leaseAt) {
        return new StatusSnapshot.ShardObs(id, source, 3L, 100L, size, "w1", T0.minusSeconds(60), "r1",
                leaseHolder == null ? null : new StatusSnapshot.LeaseObs(leaseHolder, leaseAt, false));
    }

    /** A snapshot at {@code t}; every heartbeat written at {@code t}. */
    static final class Snap {
        final List<StatusSnapshot.ShardObs> shards = new ArrayList<>();
        final List<StatusSnapshot.WriterObs> writers = new ArrayList<>();
        final List<StatusSnapshot.ReaderObs> readers = new ArrayList<>();

        Snap shard(StatusSnapshot.ShardObs s) { shards.add(s); return this; }

        Snap writer(WriterHeartbeat h, Instant at) { writers.add(new StatusSnapshot.WriterObs(h, at)); return this; }

        Snap reader(ReaderHeartbeat h, Instant at) { readers.add(new StatusSnapshot.ReaderObs(h, at)); return this; }

        StatusSnapshot at(Instant t) {
            return new StatusSnapshot(StatusSnapshot.CLUSTER, t, shards, writers, readers, Duration.ofSeconds(30));
        }
    }
}
