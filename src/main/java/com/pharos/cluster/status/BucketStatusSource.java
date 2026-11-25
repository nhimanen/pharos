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
        List<ShardEntry> entries = catalog.entries();
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
        return new StatusSnapshot(StatusSnapshot.CLUSTER, now, shards, writers, readers, leaseTtl, nodeClock);
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
        try {
            Optional<ObjectInfo> info = store.stat(layout.leaseKey(id));
            if (info.isPresent() && info.get().lastModified() != null) {
                Optional<StoredObject> body = store.get(layout.leaseKey(id));
                if (body.isPresent()) {
                    JsonNode n = MAPPER.readTree(body.get().data());
                    lease = new StatusSnapshot.LeaseObs(n.path("writer").asText(null), info.get().lastModified(),
                            n.path("released").asBoolean(false));
                }
            }
        } catch (IOException | RuntimeException ex) {
            log.debug("Status: reading the lease of shard {} failed: {}", id, ex.toString());
        }
        return new ShardRead(e, gen, manifest, latestAt, lease);
    }
}
