package com.pharos.cluster.status;

import com.pharos.cluster.status.StatusSnapshot.JobObs;
import com.pharos.cluster.status.StatusSnapshot.ProjectObs;
import com.pharos.cluster.status.StatusSnapshot.StagedObs;

/**
 * A project's reshard as {@code /cluster/status} shows it ({@link ClusterStatus.Reshard}), from the catalog
 * (authoritative for the phase) and the job record (the progress). Pure, like {@link StatusBuilder}.
 *
 * <p>Phase, in order of precedence:
 * <ol>
 *   <li>an open request: {@code pinned} when the staged version of its spec version has its generations,
 *       {@code building} when it is staged, else {@code requested};</li>
 *   <li>a cancelled staged version: {@code abandoned};</li>
 *   <li>a retired version still kept (within retention): {@code flipped};</li>
 *   <li>otherwise no reshard is shown.</li>
 * </ol>
 * The job record counts only when it is of the shown spec version (an older reshard's record is ignored).
 */
final class ReshardProgress {

    static final String REQUESTED = "requested";
    static final String BUILDING = "building";
    static final String PINNED = "pinned";
    static final String FLIPPED = "flipped";
    static final String ABANDONED = "abandoned";

    private ReshardProgress() {}

    /** The staged version's state: {@code abandoned}, {@code pinned} or {@code building}. */
    static String stagedState(StagedObs st) {
        return st.abandonedAt() != null ? ABANDONED : st.generations().isEmpty() ? BUILDING : PINNED;
    }

    /** {@code p}'s reshard on {@code s}, or null when none is visible. */
    static ClusterStatus.Reshard of(StatusSnapshot s, ProjectObs p) {
        StagedObs st = p.staged();
        int version;
        String spec;
        Integer from;
        String phase;
        Integer shardsTotal;
        if (p.request() != null) {
            version = p.request().specVersion();
            spec = p.request().spec().toString();
            from = p.specVersion();
            if (st != null && st.abandonedAt() == null && st.specVersion() == version) {
                phase = stagedState(st);
                shardsTotal = st.shards().size();
            } else {
                phase = REQUESTED;
                shardsTotal = null;
            }
        } else if (st != null && st.abandonedAt() != null) {
            version = st.specVersion();
            spec = st.spec().toString();
            from = p.specVersion();
            phase = ABANDONED;
            shardsTotal = st.shards().size();
        } else if (st != null) { // staged without a request: only between writes of an older writer
            version = st.specVersion();
            spec = st.spec().toString();
            from = p.specVersion();
            phase = stagedState(st);
            shardsTotal = st.shards().size();
        } else if (p.previous() != null && p.specVersion() != null) {
            version = p.specVersion();
            spec = p.spec().toString();
            from = p.previous().specVersion();
            phase = FLIPPED;
            shardsTotal = p.shards().size();
        } else {
            return null;
        }

        JobObs job = p.job() != null && p.job().specVersion() == version ? p.job() : null;
        boolean complete = PINNED.equals(phase) || FLIPPED.equals(phase);
        Integer shardsDone;
        if (complete) {
            shardsDone = shardsTotal;
        } else if (REQUESTED.equals(phase)) {
            shardsDone = 0;
        } else {
            int settled = job == null ? 0 : job.shardsRewritten() + job.shardsKept();
            shardsDone = shardsTotal == null ? settled : Math.min(settled, shardsTotal);
        }
        Integer filesTotal = job == null ? null : job.filesTotal();
        Integer filesDone = job == null ? null
                : complete && filesTotal != null ? filesTotal
                : job.filesDone() == null ? null
                : filesTotal == null ? job.filesDone() : Math.min(job.filesDone(), filesTotal);
        if (REQUESTED.equals(phase)) { // a record of this version from before the request was re-staged
            filesDone = null;
            filesTotal = null;
        }

        ClusterStatus.Lease lease = null;
        String holder = null;
        if (p.reshardLease() != null) {
            long age = s.ageSeconds(p.reshardLease().lastModified());
            boolean expired = age > s.leaseTtl().toSeconds();
            lease = new ClusterStatus.Lease(p.reshardLease().writer(), age, expired, p.reshardLease().released());
            if (!expired && !p.reshardLease().released()) holder = p.reshardLease().writer();
        }
        return new ClusterStatus.Reshard(version, spec, from, phase,
                p.request() == null || p.request().requestedAt() == null ? null : p.request().requestedAt().toString(),
                shardsDone, shardsTotal,
                job == null || REQUESTED.equals(phase) ? null : job.shardsRewritten(),
                job == null || REQUESTED.equals(phase) ? null : job.shardsKept(),
                filesDone, filesTotal, holder, lease,
                job == null ? null : job.phase(), job == null ? null : job.writer(),
                job == null ? null : s.ageSeconds(job.lastModified()),
                job == null ? null : job.waitingFor(), job == null ? null : job.error());
    }

    /** Coarse progress steps per measure (quarters): an event at each, never one per file or shard. */
    static final int STEPS = 4;

    /**
     * The coarse step of a reshard's progress, for {@code /cluster/events}: the quarter of its shards
     * done plus {@code STEPS + 1} times the quarter of its files done (0 when a count is unknown).
     */
    static int step(ClusterStatus.Reshard r) {
        return quarter(r.shardsDone(), r.shardsTotal()) + (STEPS + 1) * quarter(r.filesDone(), r.filesTotal());
    }

    private static int quarter(Integer done, Integer total) {
        if (done == null || total == null || total <= 0) return 0;
        return (int) Math.min(STEPS, (long) done * STEPS / total);
    }
}
