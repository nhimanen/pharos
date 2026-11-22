package com.pharos.cluster.status;

import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.writer.WriterHeartbeat;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.pharos.cluster.status.StatusFixtures.T0;
import static com.pharos.cluster.status.StatusFixtures.idleWriter;
import static com.pharos.cluster.status.StatusFixtures.pending;
import static com.pharos.cluster.status.StatusFixtures.reader;
import static com.pharos.cluster.status.StatusFixtures.shard;
import static com.pharos.cluster.status.StatusFixtures.writer;
import static org.assertj.core.api.Assertions.assertThat;

/** Task 5: one test per proposal rule at its threshold and window (spec table), plus hysteresis. */
class ProposalsTest {

    private static final StatusSettings CFG = StatusSettings.defaults();

    /** Every heartbeat of the snapshot is written at its time {@code t}. */
    private static Function<Instant, StatusSnapshot> cluster(List<StatusSnapshot.ShardObs> shards,
                                                            List<WriterHeartbeat> writers,
                                                            List<ReaderHeartbeat> readers) {
        return t -> {
            List<StatusSnapshot.WriterObs> w = new ArrayList<>();
            for (WriterHeartbeat h : writers) w.add(new StatusSnapshot.WriterObs(h, t));
            List<StatusSnapshot.ReaderObs> r = new ArrayList<>();
            for (ReaderHeartbeat h : readers) r.add(new StatusSnapshot.ReaderObs(h, t));
            return new StatusSnapshot(StatusSnapshot.CLUSTER, t, shards, w, r, Duration.ofSeconds(30));
        };
    }

    /** Advances the state over snapshots every {@code step} s from T0 to T0 + {@code until} s. */
    private static StatusState run(Function<Instant, StatusSnapshot> snap, StatusState from, long startSec,
                                   long untilSec, long step) {
        StatusState st = from;
        for (long s = startSec; s <= untilSec; s += step) st = Proposals.advance(snap.apply(T0.plusSeconds(s)), st, CFG);
        return st;
    }

    private static List<ClusterStatus.Proposal> proposals(Function<Instant, StatusSnapshot> snap, StatusState st,
                                                          long atSec) {
        return StatusBuilder.build(snap.apply(T0.plusSeconds(atSec)), st, CFG).proposals();
    }

    private static List<String> kinds(List<ClusterStatus.Proposal> ps) {
        return ps.stream().map(ClusterStatus.Proposal::kind).toList();
    }

    @Test
    void addReadersAfter5MinAboveTarget() {
        // QPS per reader 60 > 50 (target)
        var hot = cluster(List.of(shard("a", true, 10L, null, null)), List.of(),
                List.of(reader("r1", 60, null), reader("r2", 60, null)));
        StatusState st = run(hot, null, 0, 290, 10);
        st = Proposals.advance(hot.apply(T0.plusSeconds(299)), st, CFG);
        assertThat(proposals(hot, st, 299)).as("4 min 59 s above target: not yet").isEmpty();
        st = Proposals.advance(hot.apply(T0.plusSeconds(300)), st, CFG);
        List<ClusterStatus.Proposal> ps = proposals(hot, st, 300);
        assertThat(kinds(ps)).containsExactly(Proposals.ADD_READERS);
        ClusterStatus.Proposal p = ps.get(0);
        assertThat(p.target()).isEqualTo("readers");
        assertThat(p.action()).isEqualTo("Start 1 reader");
        assertThat(p.reason()).isEqualTo("QPS per reader above target for 5 min");
        assertThat(p.since()).isEqualTo(T0.toString());
        assertThat(p.metrics()).containsEntry("qpsPerReader", 60.0).containsEntry("readers", 2)
                .containsEntry("targetQpsPerReader", 50.0);

        // exactly at target is not above it
        var atTarget = cluster(List.of(), List.of(), List.of(reader("r1", 50, 500L)));
        assertThat(proposals(atTarget, run(atTarget, null, 0, 600, 10), 600)).isEmpty();

        // warm p95 alone (600 > 500 ms), QPS low
        var slow = cluster(List.of(), List.of(), List.of(reader("r1", 5, 600L), reader("r2", 5, 100L)));
        StatusState s2 = run(slow, null, 0, 300, 10);
        List<ClusterStatus.Proposal> slowPs = proposals(slow, s2, 300);
        assertThat(kinds(slowPs)).containsExactly(Proposals.ADD_READERS);
        assertThat(slowPs.get(0).reason()).isEqualTo("warm p95 above target for 5 min");
        assertThat(slowPs.get(0).metrics()).containsEntry("warmP95Millis", 600L);

        // an older reader without warmP95Millis falls back to QPS only: no proposal at low QPS
        var old = cluster(List.of(), List.of(), List.of(new ReaderHeartbeat("r1", "http://r1", List.of("a"), false,
                null, 0, 5, 1L)));
        assertThat(proposals(old, run(old, null, 0, 600, 10), 600)).isEmpty();
    }

    @Test
    void removeReadersAfter30MinBelow20Percent() {
        // 3 readers at 9 QPS each: 18% of the 50 QPS target
        var quiet = cluster(List.of(), List.of(),
                List.of(reader("r1", 9, 20L), reader("r2", 9, 20L), reader("r3", 9, 20L)));
        StatusState st = run(quiet, null, 0, 1790, 10);
        st = Proposals.advance(quiet.apply(T0.plusSeconds(1799)), st, CFG);
        assertThat(proposals(quiet, st, 1799)).isEmpty();
        st = Proposals.advance(quiet.apply(T0.plusSeconds(1800)), st, CFG);
        List<ClusterStatus.Proposal> ps = proposals(quiet, st, 1800);
        assertThat(kinds(ps)).containsExactly(Proposals.REMOVE_READERS);
        assertThat(ps.get(0).action()).isEqualTo("Drain and stop 1 reader"); // 27 QPS needs 2 at half load
        assertThat(ps.get(0).reason()).isEqualTo("readers below 20% load for 30 min");

        // exactly 20% is not below it; a single reader is never removed
        var at20 = cluster(List.of(), List.of(), List.of(reader("r1", 10, 20L), reader("r2", 10, 20L)));
        assertThat(proposals(at20, run(at20, null, 0, 1900, 10), 1900)).isEmpty();
        var single = cluster(List.of(), List.of(), List.of(reader("r1", 0, 20L)));
        assertThat(proposals(single, run(single, null, 0, 1900, 10), 1900)).isEmpty();

        // draining readers do not count
        var draining = cluster(List.of(), List.of(), List.of(reader("r1", 1, 20L),
                new ReaderHeartbeat("r2", "http://r2", List.of("a"), true, null, 0, 1, 1L)));
        assertThat(proposals(draining, run(draining, null, 0, 1900, 10), 1900)).isEmpty();
    }

    @Test
    void addWritersWhenBacklogAndFewerWritersThanShards() {
        List<StatusSnapshot.ShardObs> shards = List.of(shard("a", true, 10L, "w1", T0),
                shard("b", true, 10L, "w1", T0), shard("c", true, 10L, "w1", T0));
        // one writer holds all three; shard a has waited 601 s (> the 600 s backlog target)
        var backlog = cluster(shards, List.of(writer("w1", List.of("a", "b", "c"),
                Map.of("a", pending(601), "b", pending(30), "c", new WriterHeartbeat.Backlog("r", "r", false, null)),
                null, null)), List.of());
        StatusState st = Proposals.advance(backlog.apply(T0), null, CFG);
        List<ClusterStatus.Proposal> ps = proposals(backlog, st, 0);
        assertThat(kinds(ps)).as("window 0: at once").containsExactly(Proposals.ADD_WRITERS);
        assertThat(ps.get(0).action()).isEqualTo("Start 1 writer; leases rebalance");
        assertThat(ps.get(0).metrics()).containsEntry("writers", 1).containsEntry("workableShards", 3)
                .containsEntry("shardsPendingOverTarget", 1).containsEntry("oldestPendingAgeSeconds", 601L);

        // at the target (600 s) nothing is proposed
        var atTarget = cluster(shards, List.of(writer("w1", List.of("a", "b", "c"), Map.of("a", pending(600)), null,
                null)), List.of());
        assertThat(proposals(atTarget, Proposals.advance(atTarget.apply(T0), null, CFG), 0)).isEmpty();

        // as many writers as shards: no proposal, even with the backlog
        var enough = cluster(shards, List.of(
                writer("w1", List.of("a"), Map.of("a", pending(900)), null, null),
                writer("w2", List.of("b"), Map.of(), null, null),
                writer("w3", List.of("c"), Map.of(), null, null)), List.of());
        assertThat(proposals(enough, Proposals.advance(enough.apply(T0), null, CFG), 0)).isEmpty();
    }

    @Test
    void removeWritersWhenIdle() {
        List<StatusSnapshot.ShardObs> shards = List.of(shard("a", true, 10L, "w1", T0), shard("b", true, 10L, "w2", T0));
        var idle = cluster(shards, List.of(idleWriter("w1", List.of("a")), idleWriter("w2", List.of("b"))), List.of());
        StatusState st = run(idle, null, 0, 1790, 10);
        assertThat(proposals(idle, st, 1790)).isEmpty();
        // a short run within the clear grace (60 s) does not reset the window
        var busy = cluster(shards, List.of(writer("w1", List.of("a"), Map.of(),
                new WriterHeartbeat.Indexing("a", WriterHeartbeat.EMBED, 0.5, 1L), null),
                idleWriter("w2", List.of("b"))), List.of());
        StatusState blip = Proposals.advance(busy.apply(T0.plusSeconds(1795)), st, CFG);
        assertThat(blip.rules().values()).allSatisfy(r -> assertThat(r.since()).isEqualTo(T0.getEpochSecond()));
        st = Proposals.advance(idle.apply(T0.plusSeconds(1800)), blip, CFG);
        List<ClusterStatus.Proposal> ps = proposals(idle, st, 1800);
        assertThat(kinds(ps)).containsExactly(Proposals.REMOVE_WRITERS);
        assertThat(ps.get(0).action()).isEqualTo("Stop 1 writer; leases move");

        // busy for longer than the grace resets it
        StatusState reset = run(busy, st, 1810, 1880, 10);
        assertThat(proposals(busy, reset, 1880)).isEmpty();
        assertThat(reset.rules()).isEmpty();

        // one writer is never proposed for removal
        var one = cluster(shards, List.of(idleWriter("w1", List.of("a", "b"))), List.of());
        assertThat(proposals(one, run(one, null, 0, 1900, 10), 1900)).isEmpty();
    }

    @Test
    void reshardAbove50PercentOfCacheCap() {
        ReaderHeartbeat small = new ReaderHeartbeat("r1", "http://r1", List.of("a", "b"), false,
                StatusFixtures.cache(1000), 0, 1, 1L, ReaderHeartbeat.READY, List.of(), 10L, 1L);
        var snap = cluster(List.of(shard("a", true, 501L, null, null), shard("b", true, 500L, null, null)),
                List.of(), List.of(small));
        StatusState st = Proposals.advance(snap.apply(T0), null, CFG);
        List<ClusterStatus.Proposal> ps = proposals(snap, st, 0);
        assertThat(kinds(ps)).containsExactly(Proposals.RESHARD);
        assertThat(ps.get(0).target()).as("501 bytes > 50% of 1000; 500 is not").isEqualTo("a");
        assertThat(ps.get(0).action()).isEqualTo("New shard spec [project, ring(path, shards=2)]:"
                + " pharos reshard a --spec \"[project, ring(path, shards=2)]\"");
        assertThat(ps.get(0).metrics()).containsEntry("sizeBytes", 501L).containsEntry("readerCacheMaxBytes", 1000L);

        var big = cluster(List.of(shard("a", true, 1900L, null, null)), List.of(), List.of(small));
        List<ClusterStatus.Proposal> bigPs = proposals(big, Proposals.advance(big.apply(T0), null, CFG), 0);
        assertThat(bigPs.get(0).action()).contains("--spec \"[project, ring(path, shards=4)]\"");
    }

    /** One catalog project for the reshard tests: its active version and shards, optionally an open request. */
    private static StatusSnapshot.ProjectObs project(String name, Integer version, String spec, List<String> shards,
                                                     boolean requested) {
        return new StatusSnapshot.ProjectObs(name, version, com.pharos.cluster.sharding.ShardSpec.parse(spec), shards,
                null, null, requested ? new StatusSnapshot.RequestObs(9,
                com.pharos.cluster.sharding.ShardSpec.parse("[project, ring(path, shards=9)]"), T0) : null,
                null, null, null);
    }

    private static StatusSnapshot withProjects(List<StatusSnapshot.ShardObs> shards, ReaderHeartbeat reader,
                                               List<StatusSnapshot.ProjectObs> projects) {
        return new StatusSnapshot(StatusSnapshot.CLUSTER, T0, shards, List.of(),
                List.of(new StatusSnapshot.ReaderObs(reader, T0)), Duration.ofSeconds(30), false, projects, null, null);
    }

    /**
     * Rulings P6-R14/R15: the proposal names a concrete spec and the exact command. An unringed project gets
     * the ring appended with {@code S = ceil(largestShardBytes / trigger)}, at least 2 (the
     * trigger: 50% of the smallest reader cache cap); a ringed project of N shards gets
     * {@code shards = max(N + 1, S)}. A split project is one proposal, targeting the project.
     */
    @Test
    void reshardProposalSuggestsConcreteRingSpec() {
        ReaderHeartbeat r = new ReaderHeartbeat("r1", "http://r1", List.of(), false, StatusFixtures.cache(2000), 0, 1,
                1L, ReaderHeartbeat.READY, List.of(), 10L, 1L); // trigger: 1000 bytes

        // unringed [project]: 3300 bytes → S = ceil(3.3) = 4
        StatusSnapshot plain = withProjects(List.of(shard("lucene", true, 3300L, null, null)), r,
                List.of(project("lucene", null, "[project]", List.of("lucene"), false)));
        ClusterStatus.Proposal p = StatusBuilder.build(plain, Proposals.advance(plain, null, CFG), CFG).proposals().get(0);
        assertThat(p.kind()).isEqualTo(Proposals.RESHARD);
        assertThat(p.target()).isEqualTo("lucene");
        assertThat(p.action()).isEqualTo("New shard spec [project, ring(path, shards=4)]:"
                + " pharos reshard lucene --spec \"[project, ring(path, shards=4)]\"");
        assertThat(p.metrics()).containsEntry("suggestedRingShards", 4).containsEntry("sizeBytes", 3300L);
        // the spec is one pharos reshard accepts
        assertThat(com.pharos.cluster.sharding.ShardSpec.parse("[project, ring(path, shards=4)]").ring()).isPresent();

        // unringed [project, module]: the ring is appended after the existing keys
        StatusSnapshot moduled = withProjects(List.of(shard("app~module=core", true, 1500L, null, null),
                        shard("app~module=web", true, 200L, null, null)), r,
                List.of(project("app", 3, "[project, module]", List.of("app~module=core", "app~module=web"), false)));
        ClusterStatus.Proposal pm = StatusBuilder.build(moduled, Proposals.advance(moduled, null, CFG), CFG)
                .proposals().get(0);
        assertThat(pm.target()).isEqualTo("app");
        assertThat(pm.action()).endsWith("pharos reshard app --spec \"[project, module, ring(path, shards=2)]\"");

        // fix round 1, I3 (P6-R15): [project, module] with 20 modules, one over the trigger: the ring splits
        // every module, so S comes from the largest shard (1500 → 2), not the project's 1500 + 19 × 300 (→ 8)
        List<StatusSnapshot.ShardObs> modules = new ArrayList<>();
        List<String> moduleIds = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            String id = String.format("big~module=m%02d", i);
            moduleIds.add(id);
            modules.add(shard(id, true, i == 0 ? 1500L : 300L, null, null));
        }
        StatusSnapshot twenty = withProjects(modules, r, List.of(project("big", 5, "[project, module]", moduleIds,
                false)));
        ClusterStatus.Proposal p20 = StatusBuilder.build(twenty, Proposals.advance(twenty, null, CFG), CFG)
                .proposals().get(0);
        assertThat(p20.target()).isEqualTo("big");
        assertThat(p20.metrics()).containsEntry("sizeBytes", 7200L).containsEntry("suggestedRingShards", 2);
        assertThat(p20.action()).endsWith("pharos reshard big --spec \"[project, module, ring(path, shards=2)]\"");

        // ringed, N = 4, the largest shard 1200 bytes → S = 2: max(N + 1, S) = 5
        List<String> ring4 = List.of("idx~ring=s01", "idx~ring=s02", "idx~ring=s03", "idx~ring=s04");
        StatusSnapshot ringed = withProjects(List.of(shard(ring4.get(0), true, 1200L, null, null),
                        shard(ring4.get(1), true, 100L, null, null), shard(ring4.get(2), true, 100L, null, null),
                        shard(ring4.get(3), true, 100L, null, null)), r,
                List.of(project("idx", 2, "[project, ring(path, shards=4)]", ring4, false)));
        List<ClusterStatus.Proposal> rps = StatusBuilder.build(ringed, Proposals.advance(ringed, null, CFG), CFG)
                .proposals();
        assertThat(rps).as("one proposal for the project, not one per shard").singleElement().satisfies(x -> {
            assertThat(x.target()).isEqualTo("idx");
            assertThat(x.action()).isEqualTo("New shard spec [project, ring(path, shards=5, vnodes=512)]:"
                    + " pharos reshard idx --spec \"[project, ring(path, shards=5, vnodes=512)]\"");
            assertThat(x.metrics()).containsEntry("shards", 4).containsEntry("sizeBytes", 1500L)
                    .containsEntry("largestShardBytes", 1200L).containsEntry("suggestedRingShards", 5);
        });

        // ringed, N = 4, the largest shard 9500 bytes → S = 10 > N + 1 (from the largest shard, not the project)
        StatusSnapshot heavy = withProjects(List.of(shard(ring4.get(0), true, 9500L, null, null),
                        shard(ring4.get(1), true, 2500L, null, null), shard(ring4.get(2), true, 2500L, null, null),
                        shard(ring4.get(3), true, 2500L, null, null)), r,
                List.of(project("idx", 2, "[project, ring(path, shards=4)]", ring4, false)));
        assertThat(StatusBuilder.build(heavy, Proposals.advance(heavy, null, CFG), CFG).proposals().get(0).action())
                .endsWith("--spec \"[project, ring(path, shards=10, vnodes=512)]\"");

        // a ring with a removed shard keeps its names: [s01, s02, s04] + one → s01..s04
        assertThat(Proposals.proposedSpec(com.pharos.cluster.sharding.ShardSpec.parse(
                "[project, ring(path, shards=[s01, s02, s04], vnodes=128)]"), 4))
                .isEqualTo("[project, ring(path, shards=4, vnodes=128)]");
        assertThat(Proposals.suggestedRingShards(100, 1000, 0)).as("at least 2").isEqualTo(2);
        assertThat(Proposals.suggestedRingShards(100, 1000, 7)).isEqualTo(8);

        // while a reshard of the project is open, nothing is proposed for it
        StatusSnapshot open = withProjects(List.of(shard("lucene", true, 3300L, null, null)), r,
                List.of(project("lucene", null, "[project]", List.of("lucene"), true)));
        assertThat(StatusBuilder.build(open, Proposals.advance(open, null, CFG), CFG).proposals()).isEmpty();
    }

    @Test
    void lowDiskWriterGetsHealthEntryAndProposal() {
        var low = cluster(List.of(shard("a", true, 10L, "w1", T0)), List.of(writer("w1", List.of("a"), Map.of(), null,
                new WriterHeartbeat.Disk(1L << 30, 100L << 30, true))), List.of());
        StatusState st = Proposals.advance(low.apply(T0), null, CFG);
        ClusterStatus status = StatusBuilder.build(low.apply(T0), st, CFG);
        assertThat(status.health()).contains(new ClusterStatus.Health("writer-low-disk", "w1", "writer w1 low on disk"));
        assertThat(status.proposals()).singleElement().satisfies(p -> {
            assertThat(p.kind()).isEqualTo(Proposals.WRITER_DISK);
            assertThat(p.target()).isEqualTo("w1");
            assertThat(p.action()).isEqualTo("add writer capacity / free disk on w1");
        });
    }

    @Test
    void proposalIdsStable() throws Exception {
        String expected = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-1")
                .digest("add-readersreaders".getBytes(StandardCharsets.UTF_8)));
        assertThat(Proposals.id(Proposals.ADD_READERS, "readers")).isEqualTo(expected);

        var hot = cluster(List.of(), List.of(), List.of(reader("r1", 80, null)));
        StatusState st = run(hot, null, 0, 300, 10);
        String first = proposals(hot, st, 300).get(0).id();
        // more refreshes with changing metrics (QPS 80 → 95): same id, same since
        var hotter = cluster(List.of(), List.of(), List.of(reader("r1", 95, null)));
        StatusState later = run(hotter, st, 310, 900, 10);
        ClusterStatus.Proposal p = proposals(hotter, later, 900).get(0);
        assertThat(p.id()).isEqualTo(first).isEqualTo(expected);
        assertThat(p.since()).isEqualTo(T0.toString());
        assertThat(Proposals.id(Proposals.RESHARD, "a")).isNotEqualTo(Proposals.id(Proposals.RESHARD, "b"));
    }

    @Test
    void staleSnapshotNeverOverwritesANewerState() {
        var hot = cluster(List.of(), List.of(), List.of(reader("r1", 80, null)));
        StatusState st = Proposals.advance(hot.apply(T0.plusSeconds(100)), null, CFG);
        var cold = cluster(List.of(), List.of(), List.of(reader("r1", 1, null)));
        assertThat(Proposals.advance(cold.apply(T0.plusSeconds(90)), st, CFG)).isSameAs(st);
        assertThat(Proposals.advance(hot.apply(T0.plusSeconds(110)), st, CFG)).as("nothing changed: no write")
                .isSameAs(st);
    }

    @Test
    void aSnapshotDatedByANodeClockNeverWrites() {
        StatusSnapshot s = new StatusSnapshot(StatusSnapshot.CLUSTER, T0.plusSeconds(86_400),
                List.of(shard("a", true, 10L, null, null)), List.of(), List.of(), Duration.ofSeconds(30), true);
        assertThat(Proposals.advance(s, null, CFG)).isSameAs(StatusState.EMPTY);
    }

    @Test
    void backlogAgeIsBucketTimeAndSurvivesATakeover() {
        List<StatusSnapshot.ShardObs> shards = List.of(shard("a", true, 10L, "w1", T0));
        // w1 saw the change 100 s before its heartbeat at T0 (its clock is irrelevant: an age is a duration)
        var w1 = cluster(shards, List.of(writer("w1", List.of("a"), Map.of("a", pending(100)), null, null)), List.of());
        StatusState st = Proposals.advance(w1.apply(T0), null, CFG);
        assertThat(st.pendingSince()).containsEntry("a", T0.getEpochSecond() - 100);
        // w1 dies: no writer reports a for a while; the stamp is kept
        var none = cluster(shards, List.of(), List.of());
        st = Proposals.advance(none.apply(T0.plusSeconds(40)), st, CFG);
        assertThat(st.pendingSince()).containsEntry("a", T0.getEpochSecond() - 100);
        ClusterStatus gap = StatusBuilder.build(none.apply(T0.plusSeconds(40)), st, CFG);
        assertThat(gap.layout().shards().get(0).backlog().oldestUnpublishedChangeAgeSeconds()).isEqualTo(140L);
        // w2 takes over and has only just seen it (age 5): the status keeps counting from the first sighting
        var w2 = cluster(shards, List.of(writer("w2", List.of("a"), Map.of("a", pending(5)), null, null)), List.of());
        st = Proposals.advance(w2.apply(T0.plusSeconds(60)), st, CFG);
        ClusterStatus after = StatusBuilder.build(w2.apply(T0.plusSeconds(60)), st, CFG);
        assertThat(after.layout().shards().get(0).backlog().oldestUnpublishedChangeAgeSeconds()).isEqualTo(160L);
        assertThat(after.backlogSummary()).isEqualTo(new ClusterStatus.BacklogSummary(1, 160L));
        // published: cleared
        var done = cluster(shards, List.of(idleWriter("w2", List.of("a"))), List.of());
        st = Proposals.advance(done.apply(T0.plusSeconds(70)), st, CFG);
        assertThat(st.pendingSince()).isEmpty();
        assertThat(StatusBuilder.build(done.apply(T0.plusSeconds(70)), st, CFG).backlogSummary())
                .isEqualTo(new ClusterStatus.BacklogSummary(0, null));
    }
}
