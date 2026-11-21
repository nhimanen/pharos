package com.pharos.cluster.writer;

import com.pharos.config.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Keeps a work-queue writer from running out of disk. Running out mid-index can corrupt a Lucene
 * commit, an ArcadeDB graph or the embedding cache, and on a shared node it starves other processes.
 *
 * <p>The guard watches the file store of the writer's index directory, plus the file stores of the
 * source checkouts and the model cache when they are different ones. A store is <b>low</b> when its
 * free (usable) space is below {@code minFreeBytes} or below {@code minFreePercent} of its size
 * (whichever is larger: {@link #threshold}); the writer is low when any watched store is. Once low,
 * a store stays low until its free space is back above the threshold plus {@code hysteresisBytes},
 * so the writer doesn't flap around the threshold.
 *
 * <p>{@link #admits(EstimatedRun)}: before a shard's index run the work loop asks whether the run's
 * estimated peak extra space fits on the index store without crossing the threshold.
 *
 * <p>The guard only measures and decides; {@link WorkLoop} acts on it (no new runs while low, abort
 * the run in progress at a safe point, release the leases of shards it cannot index, garbage-collect
 * local leftovers before refusing). Free space comes from an injectable {@link Probe}.
 */
public final class DiskGuard {

    private static final Logger log = LoggerFactory.getLogger(DiskGuard.class);

    /** The {@code cluster.writer.disk} thresholds. */
    public record Settings(long minFreeBytes, int minFreePercent, long hysteresisBytes, Duration check) {
        public Settings {
            if (minFreeBytes < 0 || hysteresisBytes < 0) throw new IllegalArgumentException("negative bytes");
            if (minFreePercent < 0 || minFreePercent > 100) throw new IllegalArgumentException("minFreePercent");
            Objects.requireNonNull(check, "check");
        }

        public static Settings of(ClusterConfig.DiskConfig c) {
            return new Settings(c.getMinFreeBytes(), c.getMinFreePercent(), c.getHysteresisBytes(),
                    Duration.ofSeconds(c.getCheckSeconds()));
        }
    }

    /** Free (usable) and total bytes of one file store. */
    public record Space(long freeBytes, long totalBytes) {}

    /** How the guard measures a watched path; tests inject a fake. */
    public interface Probe {
        /** A key that is equal for two paths on the same file store. */
        Object storeOf(Path path) throws IOException;

        Space space(Path path) throws IOException;

        /** {@link Files#getFileStore}: usable space, as the JVM may write it. */
        Probe FILE_STORES = new Probe() {
            @Override public Object storeOf(Path path) throws IOException {
                return Files.getFileStore(existing(path));
            }

            @Override public Space space(Path path) throws IOException {
                FileStore fs = Files.getFileStore(existing(path));
                return new Space(fs.getUsableSpace(), fs.getTotalSpace());
            }
        };
    }

    /**
     * The estimated peak extra space of one index run, in bytes ({@link #peakBytes}). Conservative
     * on purpose: a run refused for a few spare GiB is cheap, a full disk mid-commit is not.
     *
     * <ul>
     *   <li><b>Full run:</b> {@code 2 × luceneBytes} (a full re-index writes a whole new set of
     *       segments next to the live commit, which stays until the new one is committed; merges
     *       can briefly need about as much again) {@code + graphBytes} (a new graph generation is
     *       built next to the live one) {@code + spool} (the embedding texts, about the source size,
     *       plus the run's temporary vector caches, about the size of the persistent embedding
     *       cache).</li>
     *   <li><b>Incremental run:</b> the changed files' share {@code f = changedBytes / sourceBytes}
     *       (by size, files the file-state tracker sees as dirty) of each of those terms: new
     *       segments {@code f × luceneBytes}, the graph patch {@code f × graphBytes}, the spool
     *       {@code f × (sourceBytes + embedCacheBytes)}.</li>
     *   <li>Plus {@link #OVERHEAD_BYTES} for everything not modelled (segment merges of small
     *       commits, the publish's temp files, the writer-state backup snapshot, logs).</li>
     *   <li>Plus {@code restoreBytes}: a shard with no local index is first restored from the
     *       published generation (Lucene files and graph from its manifest, plus an embedding cache
     *       assumed as large as its Lucene files, which hold the same vectors), and is then estimated
     *       as a full run over those sizes.</li>
     * </ul>
     */
    public record EstimatedRun(String shard, boolean full, long luceneBytes, long graphBytes, long embedCacheBytes,
                               long sourceBytes, long changedBytes, long restoreBytes) {

        /** Fixed allowance for what the estimate doesn't model (64 MiB). */
        public static final long OVERHEAD_BYTES = 64L << 20;

        public EstimatedRun(String shard, boolean full, long luceneBytes, long graphBytes, long embedCacheBytes,
                            long sourceBytes, long changedBytes) {
            this(shard, full, luceneBytes, graphBytes, embedCacheBytes, sourceBytes, changedBytes, 0);
        }

        public long peakBytes() {
            long run;
            if (full) {
                run = 2 * luceneBytes + graphBytes + sourceBytes + embedCacheBytes;
            } else {
                double f = sourceBytes <= 0 ? 1.0 : Math.min(1.0, Math.max(0.0, (double) changedBytes / sourceBytes));
                run = (long) Math.ceil(f * (luceneBytes + graphBytes + sourceBytes + embedCacheBytes));
            }
            return run + restoreBytes + OVERHEAD_BYTES;
        }
    }

    /** One watched file store (the first path given for it names it in logs). */
    private static final class Watched {
        final Path path;
        final boolean index;
        volatile boolean low;
        volatile Space last;

        Watched(Path path, boolean index) {
            this.path = path;
            this.index = index;
        }
    }

    /** The guard's last view: the most constrained store, and whether the writer is low. */
    public record Status(long freeBytes, long totalBytes, boolean low) {
        public WriterHeartbeat.Disk toHeartbeat() {
            return new WriterHeartbeat.Disk(freeBytes, totalBytes, low);
        }

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("freeBytes", freeBytes);
            m.put("totalBytes", totalBytes);
            m.put("low", low);
            return m;
        }
    }

    private final Settings settings;
    private final Probe probe;
    private final List<Watched> watched;
    private volatile Status status;

    /**
     * @param dataDir   the writer's index directory: its store also gets the run estimates
     * @param otherDirs the source checkout dir, the model cache: watched only when on another store
     */
    public DiskGuard(Path dataDir, List<Path> otherDirs, Settings settings, Probe probe) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.probe = Objects.requireNonNull(probe, "probe");
        List<Watched> w = new ArrayList<>();
        List<Object> stores = new ArrayList<>();
        w.add(new Watched(Objects.requireNonNull(dataDir, "dataDir"), true));
        stores.add(storeKey(dataDir));
        for (Path p : otherDirs) {
            if (p == null) continue;
            Object key = storeKey(p);
            if (key != null && stores.contains(key)) continue;
            stores.add(key);
            w.add(new Watched(p, false));
        }
        this.watched = List.copyOf(w);
        check();
    }

    private Object storeKey(Path p) {
        try {
            return probe.storeOf(p);
        } catch (IOException | RuntimeException e) {
            log.warn("Disk guard: cannot tell the file store of {} ({}); watching it separately", p, e.toString());
            return p;
        }
    }

    /** {@code max(minFreeBytes, totalBytes × minFreePercent / 100)}. */
    public long threshold(long totalBytes) {
        return Math.max(settings.minFreeBytes(), (long) (totalBytes * (settings.minFreePercent() / 100.0)));
    }

    public Settings settings() { return settings; }

    /**
     * Probes every watched store now and updates the low state (with hysteresis). A store that can't
     * be probed keeps its previous state.
     *
     * @return the new status
     */
    public Status check() {
        for (Watched w : watched) {
            Space s;
            try {
                s = probe.space(w.path);
            } catch (IOException | RuntimeException e) {
                log.warn("Disk guard: probing {} failed ({}); keeping its last state", w.path, e.toString());
                continue;
            }
            long threshold = threshold(s.totalBytes());
            boolean wasLow = w.low;
            boolean low = wasLow ? s.freeBytes() < threshold + settings.hysteresisBytes() : s.freeBytes() < threshold;
            w.last = s;
            w.low = low;
            if (low && !wasLow) {
                log.warn("Writer low on disk: {} has {} free of {} (threshold {}); no new index runs until it is back"
                                + " above {}", w.path, bytes(s.freeBytes()), bytes(s.totalBytes()), bytes(threshold),
                        bytes(threshold + settings.hysteresisBytes()));
            } else if (!low && wasLow) {
                log.info("Writer disk recovered: {} has {} free of {}", w.path, bytes(s.freeBytes()),
                        bytes(s.totalBytes()));
            }
        }
        status = summarize();
        return status;
    }

    /** The most constrained store (least free space above its threshold), and whether any is low. */
    private Status summarize() {
        boolean low = false;
        Space worst = null;
        long worstMargin = Long.MAX_VALUE;
        for (Watched w : watched) {
            low |= w.low;
            Space s = w.last;
            if (s == null) continue;
            long margin = s.freeBytes() - threshold(s.totalBytes());
            if (worst == null || margin < worstMargin) {
                worst = s;
                worstMargin = margin;
            }
        }
        return worst == null ? new Status(0, 0, low) : new Status(worst.freeBytes(), worst.totalBytes(), low);
    }

    /** The last {@link #check}'s status (no probe). */
    public Status status() {
        return status;
    }

    /** True when the last check found the writer low on disk. */
    public boolean low() {
        return status.low();
    }

    /**
     * True when {@code run} may start: the writer is not low and the run's {@link
     * EstimatedRun#peakBytes estimated peak} fits on the index store without its free space crossing
     * the threshold. Uses the last check's numbers; call {@link #check} first for fresh ones.
     */
    public boolean admits(EstimatedRun run) {
        if (low()) return false;
        Watched data = watched.get(0);
        Space s = data.last;
        if (s == null) return true; // never measured: nothing to go by (the probe logs why)
        return s.freeBytes() - run.peakBytes() >= threshold(s.totalBytes());
    }

    /** The index store's free bytes as of the last check (for log messages). */
    public long indexStoreFreeBytes() {
        Space s = watched.get(0).last;
        return s == null ? -1 : s.freeBytes();
    }

    /** The index store's threshold as of the last check. */
    public long indexStoreThreshold() {
        Space s = watched.get(0).last;
        return s == null ? settings.minFreeBytes() : threshold(s.totalBytes());
    }

    /** The nearest existing ancestor of {@code p} (a watched dir may not exist yet). */
    static Path existing(Path p) {
        Path q = p.toAbsolutePath();
        while (q != null && !Files.exists(q)) q = q.getParent();
        return q == null ? p.toAbsolutePath().getRoot() : q;
    }

    static String bytes(long b) {
        if (b < 0) return "?";
        if (b >= 1L << 30) return String.format(java.util.Locale.ROOT, "%.1f GiB", b / (double) (1L << 30));
        if (b >= 1L << 20) return String.format(java.util.Locale.ROOT, "%.1f MiB", b / (double) (1L << 20));
        return b + " B";
    }
}
