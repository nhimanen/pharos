package com.pharos.cluster.status;

import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.cluster.writer.WriterHeartbeat;
import com.pharos.web.WebServer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.pharos.cluster.status.StatusFixtures.T0;

/**
 * Statuses for the Cluster page beyond the golden fixture ({@link GoldenStatusTest}): a single node
 * and a large cluster (10 writers, 10 readers, 8 shards, the size the diagram must stay legible at).
 * {@link #main} serves any of them on a local web server for a visual check in a browser.
 */
public final class ClusterPageFixtures {

    private static final long GIB = 1L << 30;

    private ClusterPageFixtures() {}

    /** The public {@code /cluster/status} JSON of a snapshot, its proposals advanced once. */
    public static String json(StatusSnapshot s) throws IOException {
        StatusSettings cfg = StatusSettings.defaults();
        return new String(StatusBuilder.build(s, Proposals.advance(s, null, cfg), cfg).withoutAddresses().toJson(),
                StandardCharsets.UTF_8);
    }

    /** {@code role: all}: one node, writer and reader, holding four projects (one never indexed). */
    public static StatusSnapshot singleNode() {
        List<String> all = List.of("docs", "pharos", "sample-app", "scratch");
        List<StatusSnapshot.ShardObs> shards = List.of(
                new StatusSnapshot.ShardObs("docs", false, null, 2_310L, 18_874_368L, "laptop",
                        T0.minusSeconds(7_200), "a41f0c2", null),
                new StatusSnapshot.ShardObs("pharos", false, null, 31_877L, 412_090_368L, "laptop",
                        T0.minusSeconds(300), "e1d9b7a", null),
                new StatusSnapshot.ShardObs("sample-app", false, null, 214L, 1_048_576L, "laptop",
                        T0.minusSeconds(86_400 * 3), null, null),
                new StatusSnapshot.ShardObs("scratch", false, null, null, null, "laptop", null, null, null));
        WriterHeartbeat w = new WriterHeartbeat("laptop", false, all, List.of(), null, null,
                T0.minusSeconds(3_600).toEpochMilli(), false);
        ReaderHeartbeat r = new ReaderHeartbeat("laptop", null, all, false, null, 0, 1.4,
                T0.minusSeconds(3_600).toEpochMilli(), ReaderHeartbeat.READY, List.of(), 85L, null);
        return new StatusSnapshot(StatusSnapshot.SINGLE_NODE, T0, shards,
                List.of(new StatusSnapshot.WriterObs(w, T0)), List.of(new StatusSnapshot.ReaderObs(r, T0)), null, true);
    }

    private static final List<String> BIG_SHARDS = List.of("billing", "catalog", "checkout", "identity",
            "inventory", "notifications", "payments", "search-api");

    /** 10 writers (8 holding a shard, 2 idle), 10 readers serving 4–8 shards each, 8 shards. */
    public static StatusSnapshot large() {
        List<StatusSnapshot.ShardObs> shards = new ArrayList<>();
        List<StatusSnapshot.WriterObs> writers = new ArrayList<>();
        for (int i = 0; i < BIG_SHARDS.size(); i++) {
            String s = BIG_SHARDS.get(i);
            String w = String.format("w%02d", i + 1);
            boolean dead = i == 5; // notifications: its writer stopped beating
            shards.add(new StatusSnapshot.ShardObs(s, true, 10L + i, 20_000L * (i + 1), (120L << 20) * (i + 1), w,
                    T0.minusSeconds(600L * (i + 1)), "rev" + i,
                    new StatusSnapshot.LeaseObs(w, dead ? T0.minusSeconds(200) : T0.minusSeconds(3), false)));
            if (dead) continue;
            Map<String, WriterHeartbeat.Backlog> backlog = new LinkedHashMap<>();
            boolean pending = i % 3 == 0;
            backlog.put(s, new WriterHeartbeat.Backlog(pending ? "rev" + i + "b" : "rev" + i, "rev" + i, pending,
                    pending ? 60L * (i + 2) : null));
            WriterHeartbeat.Indexing ix = i == 0
                    ? new WriterHeartbeat.Indexing(s, WriterHeartbeat.PARSE, 0.18, T0.minusSeconds(95).toEpochMilli())
                    : i == 3 ? new WriterHeartbeat.Indexing(s, WriterHeartbeat.PUBLISH, 0.93,
                    T0.minusSeconds(610).toEpochMilli()) : null;
            writers.add(new StatusSnapshot.WriterObs(new WriterHeartbeat(w, true, List.of(s), List.of(), backlog, ix,
                    T0.minusSeconds(9_000).toEpochMilli(), false,
                    new WriterHeartbeat.Disk((i == 7 ? 2L : 300L) * GIB, 500L * GIB, i == 7)), T0.minusSeconds(2)));
        }
        for (String w : List.of("w09", "w10")) {
            writers.add(new StatusSnapshot.WriterObs(new WriterHeartbeat(w, true, List.of(), List.of(), Map.of(), null,
                    T0.minusSeconds(600).toEpochMilli(), w.equals("w10"),
                    new WriterHeartbeat.Disk(400L * GIB, 500L * GIB, false)), T0.minusSeconds(4)));
        }
        List<StatusSnapshot.ReaderObs> readers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<String> served = i < 4 ? BIG_SHARDS : BIG_SHARDS.subList(i % 2 == 0 ? 0 : 4, (i % 2 == 0 ? 0 : 4) + 4);
            String state = i == 6 ? ReaderHeartbeat.WARMING : i == 9 ? ReaderHeartbeat.DEGRADED : ReaderHeartbeat.READY;
            Map<String, Object> rc = new LinkedHashMap<>();
            rc.put("mode", "lazy");
            rc.put("cacheMaxBytes", 8L * GIB);
            rc.put("bytesOnDisk", (3L + i % 5) * GIB);
            rc.put("hitRate", 0.9 + i / 100.0);
            readers.add(new StatusSnapshot.ReaderObs(new ReaderHeartbeat(String.format("r%02d", i + 1), "http://r" + i,
                    served, false, rc, i % 3, 20.0 + 3 * i, T0.minusSeconds(5_000).toEpochMilli(), state,
                    i == 9 ? List.of(served.get(1)) : List.of(), 180L + 20 * i, 3L), T0.minusSeconds(i % 4)));
        }
        return new StatusSnapshot(StatusSnapshot.CLUSTER, T0, shards, writers, readers, Duration.ofSeconds(30));
    }

    /**
     * A reshard in progress: project {@code lucene} on {@code [project, ring(path, shards=2)]} (spec
     * version 1) being split to three ring shards (version 2, staged, building: 2 of 3 shards and 10 of 40
     * files done, w1 holding the reshard lease), {@code s03} only in the staged version and {@code s01}
     * rewritten in it; {@code docs} on the implicit {@code [project]}.
     */
    public static StatusSnapshot resharding() {
        return resharding(false);
    }

    /**
     * As {@link #resharding()}; {@code pinned}: every shard of version 2 published (s01 rewritten at
     * generation 5, s02 kept at 2, s03 new at 1), all 40 files written, the flip waiting for the readers;
     * and project {@code web}, flipped an hour ago from {@code [project, ring(path, shards=1)]} (version 3,
     * retired and still kept) to {@code [project, module]} (version 4).
     */
    public static StatusSnapshot resharding(boolean pinned) {
        String s01 = "lucene~ring=s01", s02 = "lucene~ring=s02", s03 = "lucene~ring=s03";
        List<StatusSnapshot.ShardObs> shards = new ArrayList<>(List.of(
                new StatusSnapshot.ShardObs("docs", true, 7L, 5_120L, 40L << 20, "w1", T0.minusSeconds(900), "d1",
                        new StatusSnapshot.LeaseObs("w1", T0.minusSeconds(3), false)),
                new StatusSnapshot.ShardObs(s01, true, 4L, 61_000L, 700L << 20, "w1", T0.minusSeconds(1_800), "r9",
                        new StatusSnapshot.LeaseObs("w1", T0.minusSeconds(3), false)),
                new StatusSnapshot.ShardObs(s02, true, 2L, 58_000L, 650L << 20, "w1", T0.minusSeconds(1_800), "r9",
                        new StatusSnapshot.LeaseObs("w1", T0.minusSeconds(3), false)),
                new StatusSnapshot.ShardObs(s03, true, null, null, null, null, null, null, null)));
        if (pinned) {
            shards.add(new StatusSnapshot.ShardObs("web~module=api", true, 3L, 900L, 9L << 20, "w1",
                    T0.minusSeconds(3_000), "w2", new StatusSnapshot.LeaseObs("w1", T0.minusSeconds(3), false)));
            shards.add(new StatusSnapshot.ShardObs("web~ring=s01", true, 6L, 880L, 8L << 20, "w1",
                    T0.minusSeconds(4_000), "w1", null));
        }
        Map<String, WriterHeartbeat.Backlog> backlog = new LinkedHashMap<>();
        for (String s : List.of("docs", s01, s02)) backlog.put(s, new WriterHeartbeat.Backlog("r9", "r9", false, null));
        WriterHeartbeat w = new WriterHeartbeat("w1", true, List.of("docs", s01, s02), List.of(), backlog, null,
                T0.minusSeconds(7_200).toEpochMilli(), false, new WriterHeartbeat.Disk(300L * GIB, 500L * GIB, false));
        Map<String, Object> rc = new LinkedHashMap<>();
        rc.put("mode", "lazy");
        rc.put("cacheMaxBytes", 4L * GIB);
        rc.put("bytesOnDisk", GIB);
        rc.put("hitRate", 0.97);
        ReaderHeartbeat r = new ReaderHeartbeat("r1", "http://r1", List.of("docs", s01, s02), false, rc, 0, 12.0,
                T0.minusSeconds(5_000).toEpochMilli(), ReaderHeartbeat.READY, List.of(), 120L, 2L);
        ShardSpec ring2 = ShardSpec.parse("[project, ring(path, shards=2)]");
        ShardSpec ring3 = ShardSpec.parse("[project, ring(path, shards=3)]");
        StatusSnapshot.ProjectObs lucene = new StatusSnapshot.ProjectObs("lucene", 1, ring2, List.of(s01, s02),
                new StatusSnapshot.StagedObs(2, ring3, List.of(s01, s02, s03),
                        pinned ? Map.of(s01, 5L, s02, 2L, s03, 1L) : Map.of(), null),
                null,
                new StatusSnapshot.RequestObs(2, ring3, T0.minusSeconds(600)), null,
                pinned ? new StatusSnapshot.JobObs(2, "waiting", "w1", 2, 1, 40, 40, "reader r1 (lucene~ring=s03)",
                        null, T0.minusSeconds(20))
                        : new StatusSnapshot.JobObs(2, "staged", "w1", 1, 1, 10, 40, null, null, T0.minusSeconds(20)),
                new StatusSnapshot.LeaseObs("w1", T0.minusSeconds(4), false));
        List<StatusSnapshot.ProjectObs> projects = new ArrayList<>(List.of(StatusSnapshot.ProjectObs.identity("docs"),
                lucene));
        List<StatusSnapshot.SpecObs> specs = new ArrayList<>(List.of(new StatusSnapshot.SpecObs(1, "lucene", ring2),
                new StatusSnapshot.SpecObs(2, "lucene", ring3)));
        if (pinned) {
            ShardSpec ring1 = ShardSpec.parse("[project, ring(path, shards=1)]");
            ShardSpec module = ShardSpec.parse("[project, module]");
            projects.add(new StatusSnapshot.ProjectObs("web", 4, module, List.of("web~module=api"), null,
                    new StatusSnapshot.PreviousObs(3, ring1, List.of("web~ring=s01"), T0.minusSeconds(3_600)), null,
                    null, null, null));
            specs.add(new StatusSnapshot.SpecObs(3, "web", ring1));
            specs.add(new StatusSnapshot.SpecObs(4, "web", module));
        }
        return new StatusSnapshot(StatusSnapshot.CLUSTER, T0, shards, List.of(new StatusSnapshot.WriterObs(w, T0)),
                List.of(new StatusSnapshot.ReaderObs(r, T0)), Duration.ofSeconds(30), false, projects, specs, null);
    }

    /**
     * Serves a fixture status on a local web server, for a visual check only:
     * {@code ClusterPageFixtures <golden|single|large|resharding> <port>}. {@code golden} replays the golden
     * cluster from T0 to T0 + 400 s, one 10 s step per refresh, so proposals appear and
     * {@code /cluster/events} fires. Run it with {@code -Dpharos.home} and {@code -Duser.home} set to
     * temp dirs.
     */
    public static void main(String[] args) throws Exception {
        String scenario = args.length > 0 ? args[0] : "golden";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 7390;
        AtomicLong step = new AtomicLong();
        StatusSource source = switch (scenario) {
            case "golden" -> () -> GoldenStatusTest.cluster(Math.min(400, 10 * step.getAndIncrement()));
            case "single" -> ClusterPageFixtures::singleNode;
            case "large" -> ClusterPageFixtures::large;
            case "resharding" -> ClusterPageFixtures::resharding;
            default -> throw new IllegalArgumentException("scenario: golden, single, large or resharding");
        };
        StatusService status = new StatusService(source, StatusStateStore.memory(), StatusSettings.defaults(),
                new StatusEvents(Duration.ofSeconds(15), null), Duration.ofMillis(300), Duration.ofSeconds(1),
                System::nanoTime);
        WebServer server = new WebServer(null, null, null, null, null);
        server.setClusterStatus(status);
        status.start();
        server.start(port);
        System.out.println("Cluster page fixture '" + scenario + "' at http://127.0.0.1:" + port + "/#cluster");
        Thread.currentThread().join();
    }
}
