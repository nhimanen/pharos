package com.pharos.cluster.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.writer.WriterHeartbeat;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.pharos.cluster.status.StatusFixtures.T0;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The golden {@code /cluster/status} fixture ({@code src/test/resources/cluster/status-golden.json},
 * fields described in {@code src/test/resources/cluster/README.md}) that the Cluster page (Task 6)
 * reuses. Regenerate with {@code -Dpharos.regenerateStatusGolden=true} after a deliberate change.
 */
class GoldenStatusTest {

    /** The public view (no token): what the Cluster page gets. */
    static final Path GOLDEN = Path.of("src/test/resources/cluster/status-golden.json");
    /** The token view: the same, plus every reader's {@code advertiseUrl}. */
    static final Path GOLDEN_TOKEN = Path.of("src/test/resources/cluster/status-golden-token.json");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long GIB = 1L << 30;

    private static Map<String, Object> cache(long max, long onDisk, long hits, long misses) {
        Map<String, Object> rc = new java.util.LinkedHashMap<>();
        rc.put("mode", "lazy");
        rc.put("cacheMaxBytes", max);
        rc.put("bytesOnDisk", onDisk);
        rc.put("hits", hits);
        rc.put("misses", misses);
        rc.put("hitRate", hits + misses == 0 ? 0.0 : Math.round(10_000.0 * hits / (hits + misses)) / 10_000.0);
        rc.put("bytesFetched", 3L * GIB / 4);
        rc.put("evictions", 41L);
        rc.put("leasesOpen", 2L);
        rc.put("shared", true);
        rc.put("source", "process");
        return rc;
    }

    /**
     * A three-shard cluster at {@code T0 + sec}: w1 indexes alpha (embed phase, a change waiting),
     * w2 holds gamma and is low on disk, w3 held beta and stopped beating at T0 + 60 (its lease
     * expires, beta's backlog keeps its first sighting), two readers above the QPS target, and alpha
     * above half of the readers' 1 GiB cache.
     */
    static StatusSnapshot cluster(long sec) {
        Instant t = T0.plusSeconds(sec);
        Instant w3Last = T0.plusSeconds(Math.min(sec, 60));
        List<StatusSnapshot.ShardObs> shards = List.of(
                new StatusSnapshot.ShardObs("alpha", true, 12L, 48_210L, 734_003_200L, "w1", T0.minusSeconds(240),
                        "4f1c2e9", new StatusSnapshot.LeaseObs("w1", t.minusSeconds(4), false)),
                new StatusSnapshot.ShardObs("beta", true, 7L, 9_120L, 88_080_384L, "w3", T0.minusSeconds(3_600),
                        "b77d0a1", new StatusSnapshot.LeaseObs("w3", w3Last, false)),
                new StatusSnapshot.ShardObs("gamma", false, 3L, 1_204L, 6_291_456L, "w2", T0.minusSeconds(86_400),
                        "/srv/src/gamma@3e9a", new StatusSnapshot.LeaseObs("w2", t.minusSeconds(8), false)));
        List<StatusSnapshot.WriterObs> writers = new ArrayList<>();
        writers.add(new StatusSnapshot.WriterObs(new WriterHeartbeat("w1", true, List.of("alpha"), List.of(),
                Map.of("alpha", new WriterHeartbeat.Backlog("9a0b7c3", "4f1c2e9", true, 90L + sec)),
                new WriterHeartbeat.Indexing("alpha", WriterHeartbeat.EMBED, 0.42, T0.toEpochMilli()),
                T0.minusSeconds(7_200).toEpochMilli(), false,
                new WriterHeartbeat.Disk(412L * GIB, 1_000L * GIB, false)), t.minusSeconds(3)));
        writers.add(new StatusSnapshot.WriterObs(new WriterHeartbeat("w2", true, List.of("gamma"), List.of("gamma"),
                Map.of("gamma", new WriterHeartbeat.Backlog("/srv/src/gamma@3e9a", "/srv/src/gamma@3e9a", false, null)),
                null, T0.minusSeconds(3_000).toEpochMilli(), false,
                new WriterHeartbeat.Disk(3L * GIB, 100L * GIB, true)), t.minusSeconds(6)));
        if (sec <= 60) {
            writers.add(new StatusSnapshot.WriterObs(new WriterHeartbeat("w3", true, List.of("beta"), List.of(),
                    Map.of("beta", new WriterHeartbeat.Backlog("c0ffee1", "b77d0a1", true, 20L + sec)), null,
                    T0.minusSeconds(900).toEpochMilli(), false, null), w3Last));
        }
        List<StatusSnapshot.ReaderObs> readers = List.of(
                new StatusSnapshot.ReaderObs(new ReaderHeartbeat("r1", "http://r1.cluster.local:7171",
                        List.of("alpha", "beta", "gamma"), false, cache(GIB, 812L << 20, 97_310, 2_690), 3, 72.4,
                        T0.minusSeconds(5_000).toEpochMilli(), ReaderHeartbeat.READY, List.of(), 640L, 4L), t),
                new StatusSnapshot.ReaderObs(new ReaderHeartbeat("r2", "http://r2.cluster.local:7171",
                        List.of("alpha", "gamma"), false, cache(GIB, 530L << 20, 40_022, 9_978), 1, 66.1,
                        T0.minusSeconds(4_000).toEpochMilli(), ReaderHeartbeat.DEGRADED, List.of("gamma"), null, null),
                        t.minusSeconds(2)));
        return new StatusSnapshot(StatusSnapshot.CLUSTER, t, shards, writers, readers, Duration.ofSeconds(30));
    }

    static ClusterStatus golden() {
        StatusSettings cfg = StatusSettings.defaults();
        StatusState st = null;
        for (long s = 0; s <= 400; s += 10) st = Proposals.advance(cluster(s), st, cfg);
        return StatusBuilder.build(cluster(400), st, cfg);
    }

    @Test
    void goldenStatusFixtureMatches() throws Exception {
        ClusterStatus st = golden();
        JsonNode token = JSON.readTree(st.toJson());
        JsonNode pub = JSON.readTree(st.withoutAddresses().toJson());
        if (Boolean.getBoolean("pharos.regenerateStatusGolden")) {
            Files.createDirectories(GOLDEN.getParent());
            Files.writeString(GOLDEN, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(pub) + "\n");
            Files.writeString(GOLDEN_TOKEN, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(token) + "\n");
        }
        assertThat(pub).as("regenerate with -Dpharos.regenerateStatusGolden=true after a deliberate change")
                .isEqualTo(JSON.readTree(Files.readString(GOLDEN)));
        assertThat(token).isEqualTo(JSON.readTree(Files.readString(GOLDEN_TOKEN)));
        assertThat(Files.readString(GOLDEN)).as("the public view has no addresses").doesNotContain("advertiseUrl")
                .doesNotContain("cluster.local");
        assertThat(token.path("layout").path("readers").get(0).path("advertiseUrl").asText())
                .isEqualTo("http://r1.cluster.local:7171");

        // what the fixture is for: every section populated
        assertThat(st.proposals()).extracting(ClusterStatus.Proposal::kind).containsExactly(
                Proposals.ADD_READERS, Proposals.RESHARD, Proposals.WRITER_DISK);
        assertThat(st.health()).extracting(ClusterStatus.Health::kind).contains("lease-expired", "reader-degraded",
                "shard-unassigned", "writer-low-disk");
        assertThat(st.layout().shards()).extracting(ClusterStatus.Shard::state)
                .containsExactly("lagging", "lease-expired", "current");
        assertThat(st.backlogSummary().shardsPending()).isEqualTo(2);
        assertThat(st.layout().writers().get(0).indexing()).isNotNull();
    }
}
