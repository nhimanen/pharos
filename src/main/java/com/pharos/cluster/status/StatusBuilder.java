package com.pharos.cluster.status;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.status.ClusterStatus.Backlog;
import com.pharos.cluster.status.ClusterStatus.Health;
import com.pharos.cluster.status.StatusSnapshot.ProjectObs;
import com.pharos.cluster.status.StatusSnapshot.ReaderObs;
import com.pharos.cluster.status.StatusSnapshot.ShardObs;
import com.pharos.cluster.status.StatusSnapshot.WriterObs;
import com.pharos.cluster.writer.WriterHeartbeat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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
 * {@code writer-low-disk} ("writer X low on disk") and {@code writer-stopping}. The shard findings
 * other than {@code lease-expired} are about the shards queries route to: a shard only a reshard's
 * staged version, a retired or an abandoned version has gets none of them.
 *
 * <p>Phase 6: {@code projects} groups the shards by project with each project's spec versions and its
 * reshard ({@link ReshardProgress}); {@code specs} lists the catalog's spec versions and their state.
 */
public final class StatusBuilder {

    /** The spec version a shard of the implicit {@code [project]} spec is shown under. */
    public static final int SPEC_VERSION = 1;

    static final String ROLE_ACTIVE = "active";
    static final String ROLE_STAGED = "staged";
    static final String ROLE_PREVIOUS = "previous";
    static final String ROLE_ABANDONED = "abandoned";
    static final String ROLE_ORPHAN = "orphan";

    /** Which version of which project a shard belongs to (the first role that names it, in role order). */
    private record Membership(String project, String role, int specVersion, boolean staged, Long stagedGeneration) {}

    private static Map<String, Membership> memberships(List<ProjectObs> projects) {
        Map<String, Membership> out = new HashMap<>();
        Map<String, Boolean> inStaged = new HashMap<>();
        Map<String, Long> stagedGen = new HashMap<>();
        for (ProjectObs p : projects) {
            if (p.staged() != null && p.staged().abandonedAt() == null) {
                for (String sh : p.staged().shards()) {
                    inStaged.put(sh, true);
                    Long g = p.staged().generations().get(sh);
                    if (g != null) stagedGen.put(sh, g);
                }
            }
        }
        for (ProjectObs p : projects) { // active first: a shard both versions share is the active one's
            int v = p.specVersion() == null ? SPEC_VERSION : p.specVersion();
            for (String sh : p.shards()) out.putIfAbsent(sh, new Membership(p.project(), ROLE_ACTIVE, v,
                    inStaged.getOrDefault(sh, false), stagedGen.get(sh)));
        }
        for (ProjectObs p : projects) {
            if (p.staged() == null || p.staged().abandonedAt() != null) continue;
            for (String sh : p.staged().shards()) out.putIfAbsent(sh, new Membership(p.project(), ROLE_STAGED,
                    p.staged().specVersion(), true, stagedGen.get(sh)));
        }
        for (ProjectObs p : projects) {
            if (p.previous() == null) continue;
            int v = p.previous().specVersion() == null ? SPEC_VERSION : p.previous().specVersion();
            for (String sh : p.previous().shards()) out.putIfAbsent(sh, new Membership(p.project(), ROLE_PREVIOUS, v,
                    false, null));
        }
        for (ProjectObs p : projects) {
            if (p.staged() != null && p.staged().abandonedAt() != null) {
                for (String sh : p.staged().shards()) out.putIfAbsent(sh, new Membership(p.project(), ROLE_ABANDONED,
                        p.staged().specVersion(), false, null));
            }
            for (StatusSnapshot.AbandonedObs a : p.abandoned()) {
                int v = a.specVersion() == null ? SPEC_VERSION : a.specVersion();
                for (String sh : a.shards()) out.putIfAbsent(sh, new Membership(p.project(), ROLE_ABANDONED, v, false,
                        null));
            }
        }
        return out;
    }

    /**
     * True when queries route to {@code shard} on {@code s}: its role is {@code active} (or it is an
     * orphan, as before Phase 6), not only a staged, retired or abandoned version's.
     */
    static java.util.function.Predicate<String> routed(StatusSnapshot s) {
        Map<String, Membership> m = memberships(s.projectsOrIdentity());
        return shard -> {
            Membership x = m.get(shard);
            return x == null || ROLE_ACTIVE.equals(x.role());
        };
    }

    /** The key values of a shard of {@code project}: {@code project} plus the id's keys. */
    private static Map<String, String> keys(String shard, String project) {
        Map<String, String> keys = new TreeMap<>();
        if (project == null || !ShardId.isValid(shard)) {
            keys.put("project", shard);
            return keys;
        }
        keys.putAll(new ShardId(shard).keyValues());
        keys.put("project", project);
        return keys;
    }

    private StatusBuilder() {}

    public static ClusterStatus build(StatusSnapshot s, StatusState state, StatusSettings cfg) {
        StatusState st = state == null ? StatusState.EMPTY : state;
        long now = s.asOf().getEpochSecond();
        List<Health> health = new ArrayList<>();
        boolean cluster = StatusSnapshot.CLUSTER.equals(s.mode());
        boolean workQueueWriters = s.writers().stream()
                .anyMatch(w -> w.heartbeat().workQueue() && !w.heartbeat().stopping());

        List<ProjectObs> projects = s.projectsOrIdentity();
        Map<String, Membership> member = memberships(projects);
        List<ClusterStatus.Shard> shards = new ArrayList<>();
        int pendingCount = 0;
        Long oldest = null;
        java.util.Set<String> pendingShards = new java.util.HashSet<>();
        for (ShardObs sh : s.shards()) {
            Membership m = member.getOrDefault(sh.shard(),
                    new Membership(null, ROLE_ORPHAN, SPEC_VERSION, false, null));
            boolean routed = ROLE_ACTIVE.equals(m.role()) || ROLE_ORPHAN.equals(m.role());
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
                pendingShards.add(sh.shard());
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
            if (cluster && routed && sh.generation() != null && readers.isEmpty() && !s.readers().isEmpty()) {
                health.add(new Health("shard-unserved", sh.shard(), "shard " + sh.shard()
                        + " is not served by any live reader"));
            }
            if (cluster && routed && workQueueWriters && sh.hasSource() && !leaseValid && rep.isEmpty()) {
                health.add(new Health("shard-unassigned", sh.shard(), "shard " + sh.shard()
                        + " has no live writer holding its lease"));
            }
            String shardState = lease != null && lease.expired() && !lease.released() ? "lease-expired"
                    : pendingAge != null ? "lagging"
                    : sh.generation() == null && ROLE_STAGED.equals(m.role()) ? "staged"
                    : sh.generation() == null && cluster ? "unpublished" : "current";
            shards.add(new ClusterStatus.Shard(sh.shard(), m.specVersion(), keys(sh.shard(), m.project()),
                    sh.docCount(), sh.sizeBytes(), sh.generation(), s.ageSeconds(sh.publishedAt()), sh.writerNode(),
                    lease, readers, backlog, shardState, m.project(), m.role(), m.staged(), m.stagedGeneration()));
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
                new ClusterStatus.BacklogSummary(pendingCount, oldest), health, Proposals.inForce(st, cands),
                projects(s, projects, pendingShards), specs(s));
    }

    private static List<ClusterStatus.Project> projects(StatusSnapshot s, List<ProjectObs> projects,
                                                        java.util.Set<String> pendingShards) {
        Map<String, ShardObs> byId = new HashMap<>();
        for (ShardObs sh : s.shards()) byId.put(sh.shard(), sh);
        List<ClusterStatus.Project> out = new ArrayList<>();
        for (ProjectObs p : projects) {
            Long docs = null;
            Long bytes = null;
            Long generation = null;
            boolean published = !p.shards().isEmpty();
            boolean pending = false;
            boolean cluster = StatusSnapshot.CLUSTER.equals(s.mode());
            for (String id : p.shards()) { // the active shards only
                ShardObs sh = byId.get(id);
                pending |= pendingShards.contains(id);
                // a single node has no generations: there a project is published once it was indexed
                if (sh == null || (cluster ? sh.generation() == null : sh.publishedAt() == null)) published = false;
                if (sh == null) continue;
                if (sh.generation() != null) {
                    generation = generation == null ? sh.generation() : Math.min(generation, sh.generation());
                }
                if (sh.docCount() != null) docs = (docs == null ? 0 : docs) + sh.docCount();
                if (sh.sizeBytes() != null) bytes = (bytes == null ? 0 : bytes) + sh.sizeBytes();
            }
            ClusterStatus.StagedVersion staged = p.staged() == null ? null : new ClusterStatus.StagedVersion(
                    p.staged().specVersion(), p.staged().spec().toString(), p.staged().shards(),
                    ReshardProgress.stagedState(p.staged()));
            ClusterStatus.PreviousVersion previous = p.previous() == null ? null : new ClusterStatus.PreviousVersion(
                    p.previous().specVersion(), p.previous().spec().toString(), p.previous().shards(),
                    p.previous().retiredAt() == null ? null : p.previous().retiredAt().toString());
            out.add(new ClusterStatus.Project(p.project(), p.specVersion(), p.spec().toString(), p.shards(), docs,
                    bytes, staged, previous, ReshardProgress.of(s, p), published, generation, pending));
        }
        return out;
    }

    /** Spec version states in precedence order: the first a version qualifies for is its state. */
    private static final List<String> SPEC_STATES = List.of("active", "staged", "requested", "retired", "abandoned");

    private static List<ClusterStatus.SpecVersion> specs(StatusSnapshot s) {
        Map<Integer, Integer> rank = new HashMap<>();
        java.util.function.BiConsumer<Integer, String> mark = (v, state) -> {
            if (v == null) return;
            rank.merge(v, SPEC_STATES.indexOf(state), Math::min);
        };
        if (s.activeVersion() != null) mark.accept(s.activeVersion(), "active");
        for (ProjectObs p : s.projects()) {
            mark.accept(p.specVersion(), "active");
            if (p.staged() != null) {
                mark.accept(p.staged().specVersion(), p.staged().abandonedAt() == null ? "staged" : "abandoned");
            }
            if (p.request() != null) mark.accept(p.request().specVersion(), "requested");
            if (p.previous() != null) mark.accept(p.previous().specVersion(), "retired");
            for (StatusSnapshot.AbandonedObs a : p.abandoned()) mark.accept(a.specVersion(), "abandoned");
        }
        List<ClusterStatus.SpecVersion> out = new ArrayList<>();
        for (StatusSnapshot.SpecObs v : s.specs()) {
            Integer r = rank.get(v.version());
            out.add(new ClusterStatus.SpecVersion(v.version(), v.project(), v.spec().toString(),
                    r == null ? "superseded" : SPEC_STATES.get(r)));
        }
        return out;
    }
}
