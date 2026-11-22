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
        return run(snap, from, startSec, untilSec, step, CFG);
    }

    private static StatusState run(Function<Instant, StatusSnapshot> snap, StatusState from, long startSec,
                                   long untilSec, long step, StatusSettings cfg) {
        StatusState st = from;
        for (long s = startSec; s <= untilSec; s += step) st = Proposals.advance(snap.apply(T0.plusSeconds(s)), st, cfg);
        return st;
    }

    private static List<ClusterStatus.Proposal> proposals(Function<Instant, StatusSnapshot> snap, StatusState st,
                                                          long atSec) {
        return proposals(snap, st, atSec, CFG);
    }

    private static List<ClusterStatus.Proposal> proposals(Function<Instant, StatusSnapshot> snap, StatusState st,
                                                          long atSec, StatusSettings cfg) {
        return StatusBuilder.build(snap.apply(T0.plusSeconds(atSec)), st, cfg).proposals();
    }

    /** Settings from a {@code cluster.status} section built by {@code edit}. */
    private static StatusSettings settings(java.util.function.Consumer<com.pharos.config.ClusterConfig.StatusConfig> edit) {
        var c = new com.pharos.config.ClusterConfig.StatusConfig();
        edit.accept(c);
        c.validate();
        return StatusSettings.of(c);
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
        // one writer holds all three; shard a has waited 301 s (> writers.oldestPendingSeconds, 300 s)
        var backlog = cluster(shards, List.of(writer("w1", List.of("a", "b", "c"),
                Map.of("a", pending(301), "b", pending(30), "c", new WriterHeartbeat.Backlog("r", "r", false, null)),
                null, null)), List.of());
        StatusState st = Proposals.advance(backlog.apply(T0), null, CFG);
        List<ClusterStatus.Proposal> ps = proposals(backlog, st, 0);
        assertThat(kinds(ps)).as("window 0: at once").containsExactly(Proposals.ADD_WRITERS);
        assertThat(ps.get(0).action()).isEqualTo("Start 1 writer; leases rebalance");
        assertThat(ps.get(0).metrics()).containsEntry("writers", 1).containsEntry("workableShards", 3)
                .containsEntry("shardsPendingOverTarget", 1).containsEntry("oldestPendingAgeSeconds", 301L);

        // at the threshold (300 s) nothing is proposed
        var atTarget = cluster(shards, List.of(writer("w1", List.of("a", "b", "c"), Map.of("a", pending(300)), null,
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
        assertThat(ps.get(0).action()).isEqualTo("New shard spec, e.g. [project, hash(path) % 2]");
        assertThat(ps.get(0).metrics()).containsEntry("sizeBytes", 501L).containsEntry("readerCacheMaxBytes", 1000L);

        var big = cluster(List.of(shard("a", true, 1900L, null, null)), List.of(), List.of(small));
        List<ClusterStatus.Proposal> bigPs = proposals(big, Proposals.advance(big.apply(T0), null, CFG), 0);
        assertThat(bigPs.get(0).action()).isEqualTo("New shard spec, e.g. [project, hash(path) % 4]");
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

    // ── delta and configurable thresholds (operator Task 1) ──────────────────────────────────────

    @Test
    void addReadersCarriesPositiveDelta() {
        // 2 readers at 120 QPS: 2.4 × target, so 5 readers are needed: start 3
        var hot = cluster(List.of(), List.of(), List.of(reader("r1", 120, null), reader("r2", 120, null)));
        ClusterStatus.Proposal p = proposals(hot, run(hot, null, 0, 300, 10), 300).get(0);
        assertThat(p.kind()).isEqualTo(Proposals.ADD_READERS);
        assertThat(p.action()).isEqualTo("Start 3 readers");
        assertThat(p.delta()).isEqualTo(3);

        // add-writers: two shards over the backlog age, one writer of three workable shards: start 2
        List<StatusSnapshot.ShardObs> shards = List.of(shard("a", true, 10L, "w1", T0),
                shard("b", true, 10L, "w1", T0), shard("c", true, 10L, "w1", T0));
        var backlog = cluster(shards, List.of(writer("w1", List.of("a", "b", "c"),
                Map.of("a", pending(900), "b", pending(900)), null, null)), List.of());
        ClusterStatus.Proposal w = proposals(backlog, Proposals.advance(backlog.apply(T0), null, CFG), 0).get(0);
        assertThat(w.kind()).isEqualTo(Proposals.ADD_WRITERS);
        assertThat(w.action()).isEqualTo("Start 2 writers; leases rebalance");
        assertThat(w.delta()).isEqualTo(2);

        // reshard and writer-disk carry no delta
        ReaderHeartbeat small = new ReaderHeartbeat("r1", "http://r1", List.of("a"), false,
                StatusFixtures.cache(1000), 0, 1, 1L, ReaderHeartbeat.READY, List.of(), 10L, 1L);
        var other = cluster(List.of(shard("a", true, 900L, "w1", T0)), List.of(writer("w1", List.of("a"), Map.of(),
                null, new WriterHeartbeat.Disk(1L << 30, 100L << 30, true))), List.of(small));
        List<ClusterStatus.Proposal> ps = proposals(other, Proposals.advance(other.apply(T0), null, CFG), 0);
        assertThat(kinds(ps)).containsExactly(Proposals.RESHARD, Proposals.WRITER_DISK);
        assertThat(ps).allSatisfy(x -> assertThat(x.delta()).isNull());
    }

    @Test
    void removeReadersCarriesNegativeDelta() {
        // 6 readers at 1 QPS: 6 QPS fits one reader at half load, so drain 5
        List<ReaderHeartbeat> six = new ArrayList<>();
        for (int i = 1; i <= 6; i++) six.add(reader("r" + i, 1, 20L));
        var quiet = cluster(List.of(), List.of(), six);
        ClusterStatus.Proposal p = proposals(quiet, run(quiet, null, 0, 1800, 10), 1800).get(0);
        assertThat(p.kind()).isEqualTo(Proposals.REMOVE_READERS);
        assertThat(p.action()).isEqualTo("Drain and stop 5 readers");
        assertThat(p.delta()).isEqualTo(-5);

        // remove-writers: three idle writers, stop 2
        List<StatusSnapshot.ShardObs> shards = List.of(shard("a", true, 10L, "w1", T0),
                shard("b", true, 10L, "w2", T0), shard("c", true, 10L, "w3", T0));
        var idle = cluster(shards, List.of(idleWriter("w1", List.of("a")), idleWriter("w2", List.of("b")),
                idleWriter("w3", List.of("c"))), List.of());
        ClusterStatus.Proposal w = proposals(idle, run(idle, null, 0, 1800, 10), 1800).get(0);
        assertThat(w.kind()).isEqualTo(Proposals.REMOVE_WRITERS);
        assertThat(w.action()).isEqualTo("Stop 2 writers; leases move");
        assertThat(w.delta()).isEqualTo(-2);

        // the delta is part of the shared state and of the events, like the action
        StatusState st = run(idle, null, 0, 1800, 10);
        assertThat(st.rules().values()).singleElement().satisfies(r -> assertThat(r.delta()).isEqualTo(-2));
        List<StatusEvent> events = Proposals.events(StatusState.EMPTY, st,
                Proposals.candidates(idle.apply(T0.plusSeconds(1800)), st, CFG));
        assertThat(events).singleElement().satisfies(e -> assertThat(e.proposal().delta()).isEqualTo(-2));

        // a node older than delta rewrites the state without it: no event, and the status still shows the
        // delta (from the candidate, whose action matches the recorded one)
        java.util.TreeMap<String, StatusState.Rule> stripped = new java.util.TreeMap<>();
        st.rules().forEach((id, r) -> stripped.put(id, new StatusState.Rule(r.kind(), r.target(), r.since(),
                r.falseSince(), r.active(), r.reason(), r.action(), null)));
        StatusState old = new StatusState(StatusState.VERSION, st.asOf(), stripped, st.pendingSince(), st.layout());
        var cands = Proposals.candidates(idle.apply(T0.plusSeconds(1800)), old, CFG);
        assertThat(Proposals.events(old, st, cands)).isEmpty();
        assertThat(proposals(idle, old, 1800).get(0).delta()).isEqualTo(-2);
    }

    @Test
    void removeReadersLoadIsConfigurable() {
        // 5 readers at 20 QPS each: 40% of the target; 100 QPS needs 4 readers at half load
        List<ReaderHeartbeat> five = new ArrayList<>();
        for (int i = 1; i <= 5; i++) five.add(reader("r" + i, 20, 20L));
        var at40 = cluster(List.of(), List.of(), five);
        assertThat(proposals(at40, run(at40, null, 0, 1900, 10), 1900)).as("default 20%: not below").isEmpty();

        StatusSettings half = settings(c -> c.setRemoveReadersLoad(0.5));
        List<ClusterStatus.Proposal> ps = proposals(at40, run(at40, null, 0, 1800, 10, half), 1800, half);
        assertThat(kinds(ps)).containsExactly(Proposals.REMOVE_READERS);
        assertThat(ps.get(0).reason()).isEqualTo("readers below 50% load for 30 min");
        assertThat(ps.get(0).delta()).isEqualTo(-1);
    }

    @Test
    void writerBacklogTriggersUseConfiguredShardsAndAge() {
        List<StatusSnapshot.ShardObs> shards = List.of(shard("a", true, 10L, "w1", T0),
                shard("b", true, 10L, "w1", T0), shard("c", true, 10L, "w1", T0));
        // a has waited 400 s, b 350 s, c nothing pending; one writer holds all three
        var backlog = cluster(shards, List.of(writer("w1", List.of("a", "b", "c"),
                Map.of("a", pending(400), "b", pending(350), "c", new WriterHeartbeat.Backlog("r", "r", false, null)),
                null, null)), List.of());
        java.util.function.Function<StatusSettings, List<ClusterStatus.Proposal>> at0 =
                cfg -> proposals(backlog, Proposals.advance(backlog.apply(T0), null, cfg), 0, cfg);

        // defaults: older than 300 s, at least 1 shard: both count, start 2
        List<ClusterStatus.Proposal> d = at0.apply(CFG);
        assertThat(kinds(d)).containsExactly(Proposals.ADD_WRITERS);
        assertThat(d.get(0).delta()).isEqualTo(2);
        assertThat(d.get(0).metrics()).containsEntry("shardsPendingOverTarget", 2)
                .containsEntry("targetOldestPendingSeconds", 300L).containsEntry("targetBacklogShards", 1);

        // 3 shards over the age needed: only 2 are
        assertThat(at0.apply(settings(c -> c.getWriters().setBacklogShards(3)))).isEmpty();
        assertThat(kinds(at0.apply(settings(c -> c.getWriters().setBacklogShards(2)))))
                .containsExactly(Proposals.ADD_WRITERS);
        // older than 360 s: only a, start 1; older than 400 s: none (at the threshold is not above it)
        List<ClusterStatus.Proposal> one = at0.apply(settings(c -> c.getWriters().setOldestPendingSeconds(360L)));
        assertThat(one).singleElement().satisfies(p -> {
            assertThat(p.action()).isEqualTo("Start 1 writer; leases rebalance");
            assertThat(p.delta()).isEqualTo(1);
        });
        assertThat(at0.apply(settings(c -> c.getWriters().setOldestPendingSeconds(400L)))).isEmpty();

        // writers.idleSeconds is the remove-writers window
        StatusSettings quick = settings(c -> c.getWriters().setIdleSeconds(600L));
        var idle = cluster(shards, List.of(idleWriter("w1", List.of("a", "b")), idleWriter("w2", List.of("c"))),
                List.of());
        assertThat(proposals(idle, run(idle, null, 0, 590, 10, quick), 590, quick)).isEmpty();
        List<ClusterStatus.Proposal> rm = proposals(idle, run(idle, null, 0, 600, 10, quick), 600, quick);
        assertThat(kinds(rm)).containsExactly(Proposals.REMOVE_WRITERS);
        assertThat(rm.get(0).reason()).isEqualTo("writers idle for 10 min");
    }
}
