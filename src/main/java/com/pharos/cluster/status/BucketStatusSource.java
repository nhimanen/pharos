package com.pharos.cluster.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardEntry;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.objectstore.BucketClock;
import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.objectstore.StoredObject;
import com.pharos.cluster.sharding.ProjectShards;
import com.pharos.cluster.sharding.VersionedSpec;
import com.pharos.cluster.writer.WriterHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads a {@link StatusSnapshot} from the bucket: the catalog's shards (with each {@code LATEST}
 * manifest, the {@code LATEST} object's LastModified and the lease object), the reader heartbeats
 * ({@code catalog/readers/}) and the writer heartbeats ({@code catalog/writers/}).
 *
 * <p>The snapshot's "now" is the newest LastModified among every object read; liveness is judged
 * against it with the registries' own rule ({@link BucketClock#unexpired}: readers by
 * {@code readerTtlSeconds}, writers by the lease TTL), so the result depends on the bucket's
 * objects only, never on this node's clock. Only an empty bucket (no timestamp at all) falls back
 * to the node clock. A shard whose manifest or lease can't be read is listed with what is known.
 *
 * <p>The projects come from the same catalog read ({@link ObjectStoreCatalog#view}): spec versions,
 * staged and retired versions ({@link ProjectShards}). For a project with a reshard to show (requested,
 * staged, or flipped and still within retention) it also reads the job record
 * ({@code projects/<p>/reshard/job.json}) and the reshard lease; their LastModified count towards the
 * snapshot's "now" like the shards' leases.
 */
public final class BucketStatusSource implements StatusSource {

    private static final Logger log = LoggerFactory.getLogger(BucketStatusSource.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ObjectStore store;
    private final ObjectStoreLayout layout;
    private final ObjectStoreCatalog catalog;
    private final ReplicaSource replicas;
    private final Duration readerTtl;
    private final Duration leaseTtl;
    private final Clock fallback;
    /** Manifests by shard, for the generation last read (manifests are immutable). */
    private final Map<String, ShardManifest> manifests = new ConcurrentHashMap<>();

    public BucketStatusSource(ObjectStore store, ObjectStoreLayout layout, ObjectStoreCatalog catalog,
                              ReplicaSource replicas, Duration readerTtl, Duration leaseTtl, Clock fallback) {
        this.store = store;
        this.layout = layout;
        this.catalog = catalog;
        this.replicas = replicas;
        this.readerTtl = readerTtl;
        this.leaseTtl = leaseTtl;
        this.fallback = fallback;
    }

    private record ShardRead(ShardEntry entry, Generation gen, ShardManifest manifest, Instant latestAt,
                             StatusSnapshot.LeaseObs lease) {}

    @Override
    public StatusSnapshot read() throws IOException {
        List<ObjectInfo> readerInfos = heartbeats(layout.readersPrefix());
        List<ObjectInfo> writerInfos = heartbeats(layout.writersPrefix());
        ObjectStoreCatalog.View view = catalog.view();
        List<ShardEntry> entries = view.entries();
        List<ShardRead> reads = new ArrayList<>();
        Instant now = null;
        for (ObjectInfo i : readerInfos) now = max(now, i.lastModified());
        for (ObjectInfo i : writerInfos) now = max(now, i.lastModified());
        for (ShardEntry e : entries) {
            ShardRead r = readShard(e);
            reads.add(r);
            now = max(now, r.latestAt());
            if (r.lease() != null) now = max(now, r.lease().lastModified());
        }
        List<StatusSnapshot.ProjectObs> projects = new ArrayList<>();
        for (String p : view.projects().projects()) {
            StatusSnapshot.ProjectObs o = project(view.projects(), p);
            projects.add(o);
            if (o.job() != null) now = max(now, o.job().lastModified());
            if (o.reshardLease() != null) now = max(now, o.reshardLease().lastModified());
        }
        List<StatusSnapshot.SpecObs> specs = new ArrayList<>();
        for (VersionedSpec v : view.projects().specs()) {
            specs.add(new StatusSnapshot.SpecObs(v.version(), v.project().orElse(null), v.spec()));
        }
        OptionalInt active = view.projects().activeVersion();
        boolean nodeClock = now == null;
        if (nodeClock) now = fallback.instant();

        List<StatusSnapshot.ReaderObs> readers = new ArrayList<>();
        for (BucketClock.Aged a : BucketClock.unexpired(readerInfos, now, readerTtl)) {
            read(a.info().key(), ReaderHeartbeat.class)
                    .ifPresent(h -> readers.add(new StatusSnapshot.ReaderObs(h, a.info().lastModified())));
        }
        List<StatusSnapshot.WriterObs> writers = new ArrayList<>();
        for (BucketClock.Aged a : BucketClock.unexpired(writerInfos, now, leaseTtl)) {
            read(a.info().key(), WriterHeartbeat.class)
                    .ifPresent(h -> writers.add(new StatusSnapshot.WriterObs(h, a.info().lastModified())));
        }
        List<StatusSnapshot.ShardObs> shards = new ArrayList<>();
        for (ShardRead r : reads) {
            ShardManifest m = r.manifest();
            String rev = m == null || m.projectMeta() == null ? null : m.projectMeta().getSourceRevision();
            shards.add(new StatusSnapshot.ShardObs(r.entry().id().value(), r.entry().source().isPresent(),
                    r.gen() == null ? null : r.gen().value(), m == null ? null : m.docCount(),
                    m == null ? null : m.sizeBytes(), m == null ? null : m.writerNode(), r.latestAt(), rev, r.lease()));
        }
        return new StatusSnapshot(StatusSnapshot.CLUSTER, now, shards, writers, readers, leaseTtl, nodeClock,
                projects, specs, active.isPresent() ? active.getAsInt() : null);
    }

    private static List<String> names(List<ShardId> ids) {
        return ids.stream().map(ShardId::value).toList();
    }

    private static Integer version(OptionalInt v) {
        return v.isPresent() ? v.getAsInt() : null;
    }

    /** One project of the catalog read, with its reshard's job record and lease when one is visible. */
    private StatusSnapshot.ProjectObs project(ProjectShards map, String p) {
        StatusSnapshot.StagedObs staged = map.stagedOf(p).map(st -> {
            Map<String, Long> gens = new TreeMap<>();
            st.generations().forEach((id, g) -> gens.put(id.value(), g));
            return new StatusSnapshot.StagedObs(st.specVersion(), st.spec(), names(st.shards()), gens,
                    st.abandonedAt().orElse(null));
        }).orElse(null);
        StatusSnapshot.PreviousObs previous = map.previousOf(p).map(pr -> new StatusSnapshot.PreviousObs(
                version(pr.specVersion()), pr.spec(), names(pr.shards()), pr.retiredAt())).orElse(null);
        StatusSnapshot.RequestObs request = map.reshardRequestOf(p).map(r -> new StatusSnapshot.RequestObs(
                r.specVersion(), r.spec(), r.requestedAt())).orElse(null);
        List<StatusSnapshot.AbandonedObs> abandoned = map.abandonedOf(p).stream()
                .map(a -> new StatusSnapshot.AbandonedObs(version(a.specVersion()), names(a.shards()))).toList();
        StatusSnapshot.JobObs job = null;
        StatusSnapshot.LeaseObs lease = null;
        if (staged != null || previous != null || request != null) {
            job = job(p);
            lease = lease(layout.reshardLeaseKey(p), "reshard lease of project " + p);
        }
        return new StatusSnapshot.ProjectObs(p, version(map.specVersionOf(p)), map.specOf(p), names(map.shardsOf(p)),
                staged, previous, request, abandoned, job, lease);
    }

    /** The job record of {@code project}'s reshard; null when there is none or it can't be read. */
    private StatusSnapshot.JobObs job(String project) {
        String key = layout.reshardJobKey(project);
        try {
            Optional<ObjectInfo> info = store.stat(key);
            if (info.isEmpty() || info.get().lastModified() == null) return null;
            Optional<StoredObject> body = store.get(key);
            if (body.isEmpty()) return null;
            JsonNode n = MAPPER.readTree(body.get().data());
            return new StatusSnapshot.JobObs(n.path("spec").asInt(-1), n.path("phase").asText(null),
                    n.path("writer").asText(null), n.path("affected").size(), n.path("unaffected").size(),
                    n.hasNonNull("filesDone") ? n.get("filesDone").asInt() : null,
                    n.hasNonNull("filesTotal") ? n.get("filesTotal").asInt() : null,
                    n.hasNonNull("waitingFor") ? n.get("waitingFor").asText() : null,
                    n.hasNonNull("error") ? n.get("error").asText() : null, info.get().lastModified());
        } catch (IOException | RuntimeException e) {
            log.debug("Status: reading the reshard job record of project {} failed: {}", project, e.toString());
            return null;
        }
    }

    /** A lease object ({@code writer}, {@code released}) with its LastModified; null when absent or unreadable. */
    private StatusSnapshot.LeaseObs lease(String key, String what) {
        try {
            Optional<ObjectInfo> info = store.stat(key);
            if (info.isPresent() && info.get().lastModified() != null) {
                Optional<StoredObject> body = store.get(key);
                if (body.isPresent()) {
                    JsonNode n = MAPPER.readTree(body.get().data());
                    return new StatusSnapshot.LeaseObs(n.path("writer").asText(null), info.get().lastModified(),
                            n.path("released").asBoolean(false));
                }
            }
        } catch (IOException | RuntimeException ex) {
            log.debug("Status: reading the {} failed: {}", what, ex.toString());
        }
        return null;
    }

    private static Instant max(Instant a, Instant b) {
        if (b == null) return a;
        return a == null || b.isAfter(a) ? b : a;
    }

    private List<ObjectInfo> heartbeats(String prefix) throws IOException {
        return store.list(prefix).stream().filter(i -> i.key().endsWith(".json") && i.lastModified() != null).toList();
    }

    private <T> Optional<T> read(String key, Class<T> type) {
        try {
            Optional<StoredObject> o = store.get(key);
            if (o.isEmpty()) return Optional.empty();
            return Optional.of(MAPPER.readValue(o.get().data(), type));
        } catch (IOException | RuntimeException e) {
            log.debug("Status: skipping the unreadable heartbeat {}: {}", key, e.toString());
            return Optional.empty();
        }
    }

    private ShardRead readShard(ShardEntry e) {
        ShardId id = e.id();
        Generation gen = null;
        ShardManifest manifest = null;
        Instant latestAt = null;
        StatusSnapshot.LeaseObs lease = null;
        try {
            Optional<Generation> latest = replicas.latest(id);
            if (latest.isPresent()) {
                gen = latest.get();
                latestAt = store.stat(layout.latestKey(id)).map(ObjectInfo::lastModified).orElse(null);
                ShardManifest cached = manifests.get(id.value());
                if (cached != null && cached.generation() == gen.value()) {
                    manifest = cached;
                } else {
                    manifest = replicas.manifest(id, gen);
                    manifests.put(id.value(), manifest);
                }
            }
        } catch (IOException | RuntimeException ex) {
            log.debug("Status: reading LATEST of shard {} failed: {}", id, ex.toString());
        }
        lease = lease(layout.leaseKey(id), "lease of shard " + id);
        return new ShardRead(e, gen, manifest, latestAt, lease);
    }
}
