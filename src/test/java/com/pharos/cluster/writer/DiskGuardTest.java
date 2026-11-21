package com.pharos.cluster.writer;

import com.pharos.config.ClusterConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DiskGuardTest {

    private static final long GIB = 1L << 30;
    private static final long MIB = 1L << 20;
    private static final DiskGuard.Settings SETTINGS = new DiskGuard.Settings(5 * GIB, 10, GIB, Duration.ofSeconds(15));

    /** Paths map to named stores; each store has a settable free space. */
    static final class Stores implements DiskGuard.Probe {
        final Map<Path, String> storeOf = new HashMap<>();
        final Map<String, long[]> space = new HashMap<>(); // {free, total}

        Stores store(String name, long free, long total, Path... paths) {
            space.put(name, new long[]{free, total});
            for (Path p : paths) storeOf.put(p, name);
            return this;
        }

        void free(String name, long free) { space.get(name)[0] = free; }

        @Override public Object storeOf(Path path) { return storeOf.get(path); }

        @Override public DiskGuard.Space space(Path path) throws IOException {
            long[] s = space.get(storeOf.get(path));
            if (s == null) throw new IOException("no such store");
            return new DiskGuard.Space(s[0], s[1]);
        }
    }

    private static final Path DATA = Path.of("/data/indexes");
    private static final Path WORK = Path.of("/data/work");
    private static final Path MODELS = Path.of("/home/models");

    @Test
    void thresholdIsTheLargerOfBytesAndPercent() {
        DiskGuard g = new DiskGuard(DATA, List.of(), SETTINGS, new Stores().store("a", 50 * GIB, 100 * GIB, DATA));
        assertThat(g.threshold(100 * GIB)).isEqualTo(10 * GIB);
        assertThat(g.threshold(20 * GIB)).as("5 GiB beats 10 % of 20 GiB").isEqualTo(5 * GIB);
    }

    @Test
    void refusesRunThatWouldCrossThreshold() {
        Stores stores = new Stores().store("a", 12 * GIB, 100 * GIB, DATA);
        DiskGuard g = new DiskGuard(DATA, List.of(), SETTINGS, stores);
        assertThat(g.low()).isFalse();
        // 2 GiB above the threshold: a run needing 1 GiB fits, one needing 3 GiB doesn't
        DiskGuard.EstimatedRun small = new DiskGuard.EstimatedRun("s", false, 10 * GIB, GIB, GIB, 100 * GIB, GIB);
        DiskGuard.EstimatedRun big = new DiskGuard.EstimatedRun("s", true, GIB, 0, 0, 0, 0);
        assertThat(small.peakBytes()).isLessThan(2 * GIB);
        assertThat(big.peakBytes()).isGreaterThan(2 * GIB);
        assertThat(g.admits(small)).isTrue();
        assertThat(g.admits(big)).isFalse();
        // a low writer admits nothing
        stores.free("a", 9 * GIB);
        g.check();
        assertThat(g.admits(new DiskGuard.EstimatedRun("s", false, 0, 0, 0, 1, 0))).isFalse();
    }

    @Test
    void estimateIsTwiceTheIndexForAFullRunAndTheChangedShareForAnIncrementalOne() {
        long lucene = 1000 * MIB, graph = 200 * MIB, cache = 300 * MIB, source = 100 * MIB;
        DiskGuard.EstimatedRun full = new DiskGuard.EstimatedRun("s", true, lucene, graph, cache, source, 0);
        assertThat(full.peakBytes()).isEqualTo(2 * lucene + graph + source + cache + DiskGuard.EstimatedRun.OVERHEAD_BYTES);
        // a tenth of the source changed: a tenth of every term
        DiskGuard.EstimatedRun incr = new DiskGuard.EstimatedRun("s", false, lucene, graph, cache, source, source / 10);
        assertThat(incr.peakBytes()).isEqualTo((lucene + graph + cache + source) / 10
                + DiskGuard.EstimatedRun.OVERHEAD_BYTES);
        // nothing known about the source: the whole thing, once
        DiskGuard.EstimatedRun unknown = new DiskGuard.EstimatedRun("s", false, lucene, graph, cache, 0, 0);
        assertThat(unknown.peakBytes()).isEqualTo(lucene + graph + cache + DiskGuard.EstimatedRun.OVERHEAD_BYTES);
        // a restore comes on top
        DiskGuard.EstimatedRun restore = new DiskGuard.EstimatedRun("s", true, lucene, graph, cache, source, source,
                lucene + graph + cache, 0);
        assertThat(restore.peakBytes()).isEqualTo(full.peakBytes() + lucene + graph + cache);
        // a first index: a multiple of the source
        assertThat(DiskGuard.EstimatedRun.firstIndex("s", source).peakBytes()).isEqualTo(
                (1 + DiskGuard.EstimatedRun.FIRST_INDEX_LUCENE_FACTOR + DiskGuard.EstimatedRun.FIRST_INDEX_CACHE_FACTOR)
                        * source + DiskGuard.EstimatedRun.OVERHEAD_BYTES);
        // a floor wins over a smaller estimate, never over a larger one
        assertThat(incr.withFloor(5 * GIB).peakBytes()).isEqualTo(5 * GIB);
        assertThat(full.withFloor(MIB).peakBytes()).isEqualTo(full.peakBytes());
        assertThat(incr.withFloor(5 * GIB).withFloor(GIB).floorBytes()).as("a floor only grows").isEqualTo(5 * GIB);
    }

    @Test
    void recoversWithHysteresis() {
        Stores stores = new Stores().store("a", 20 * GIB, 100 * GIB, DATA);
        DiskGuard g = new DiskGuard(DATA, List.of(), SETTINGS, stores);
        assertThat(g.low()).isFalse();
        stores.free("a", 10 * GIB - 1);
        assertThat(g.check().low()).isTrue();
        stores.free("a", 10 * GIB);           // at the threshold: not yet out
        assertThat(g.check().low()).isTrue();
        stores.free("a", 11 * GIB - 1);       // within the 1 GiB margin
        assertThat(g.check().low()).isTrue();
        stores.free("a", 11 * GIB);           // threshold + margin
        assertThat(g.check().low()).isFalse();
        stores.free("a", 10 * GIB + 1);       // above the threshold again: entering low needs < threshold
        assertThat(g.check().low()).isFalse();
    }

    @Test
    void watchesOtherFileStoresAndReportsTheMostConstrained() {
        Stores stores = new Stores().store("data", 60 * GIB, 100 * GIB, DATA, WORK)
                .store("home", 30 * GIB, 50 * GIB, MODELS);
        DiskGuard g = new DiskGuard(DATA, List.of(WORK, MODELS), SETTINGS, stores);
        DiskGuard.Status s = g.status();
        assertThat(s.low()).isFalse();
        assertThat(s.freeBytes()).as("home has less headroom above its threshold").isEqualTo(30 * GIB);
        assertThat(s.totalBytes()).isEqualTo(50 * GIB);
        stores.free("home", 4 * GIB);
        s = g.check();
        assertThat(s.low()).as("any watched store low makes the writer low").isTrue();
        assertThat(s.freeBytes()).isEqualTo(4 * GIB);
        assertThat(s.toHeartbeat()).isEqualTo(new WriterHeartbeat.Disk(4 * GIB, 50 * GIB, true));
        // runs are estimated against the index store, but a low writer admits none
        assertThat(g.indexStoreFreeBytes()).isEqualTo(60 * GIB);
        assertThat(g.admits(new DiskGuard.EstimatedRun("s", false, 0, 0, 0, 1, 0))).isFalse();
    }

    @Test
    void aFailedProbeKeepsTheLastState() {
        Stores stores = new Stores().store("a", 5 * GIB, 100 * GIB, DATA);
        DiskGuard g = new DiskGuard(DATA, List.of(), SETTINGS, stores);
        assertThat(g.low()).isTrue();
        stores.space.clear();
        assertThat(g.check().low()).isTrue();
    }

    @Test
    void settingsComeFromTheClusterConfig() {
        ClusterConfig.DiskConfig c = new ClusterConfig.DiskConfig();
        assertThat(c.getMinFreeBytes()).isEqualTo(5 * GIB);
        assertThat(c.getMinFreePercent()).isEqualTo(10);
        assertThat(c.getCheckSeconds()).isEqualTo(15);
        assertThat(c.getHysteresisBytes()).isEqualTo(GIB);
        assertThat(DiskGuard.Settings.of(c)).isEqualTo(SETTINGS);
        c.setMinFreePercent(101);
        assertThatThrownBy(c::validate).hasMessageContaining("cluster.writer.disk.minFreePercent");
        c.setMinFreePercent(10);
        c.setCheckSeconds(0);
        assertThatThrownBy(c::validate).hasMessageContaining("cluster.writer.disk.checkSeconds");
        ClusterConfig.WriterConfig w = new ClusterConfig.WriterConfig();
        assertThat(w.getMaxConsecutiveFailures()).isEqualTo(3);
        w.setMaxConsecutiveFailures(0);
        assertThatThrownBy(w::validate).hasMessageContaining("cluster.writer.maxConsecutiveFailures");
    }

    @Test
    void realFileStoreProbeWorksOnAMissingDir() throws IOException {
        Path missing = Path.of(System.getProperty("java.io.tmpdir"), "pharos-disk-guard-missing", "deeper");
        DiskGuard.Space s = DiskGuard.Probe.FILE_STORES.space(missing);
        assertThat(s.totalBytes()).isPositive();
        assertThat(DiskGuard.Probe.FILE_STORES.storeOf(missing)).isNotNull();
    }
}
