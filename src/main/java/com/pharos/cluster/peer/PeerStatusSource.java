package com.pharos.cluster.peer;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.status.StatusSnapshot;
import com.pharos.cluster.status.StatusSource;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * {@code /cluster/status} without a bucket (Ruling P5-R1): a {@link StatusSnapshot} built from the
 * merged {@link PeerCatalog} (each shard's owner and {@code LATEST}, its manifest through the
 * {@link ReplicaSource}), every live writer's reader registry ({@code GET /replication/readers},
 * merged as {@link PeerMembership#merge} does) and the live writers themselves.
 *
 * <p><b>Clock.</b> The snapshot is dated by the clock of the writer that holds the status state
 * (the lowest node id among the writers the catalog tracks, live or missing but not dropped,
 * {@link PeerStatusState#holder}): its {@code now}, plus the time since its answer arrived on this
 * node's monotonic clock. Every node therefore stamps the shared state with the holder's clock, as
 * bucket nodes stamp it with the bucket's. Reader ages are the writers' own. When the holder did not
 * answer, {@link #read} throws: the status service keeps its previous status and writes nothing, and
 * the holder does not change until the catalog drops it. A {@code partial} source (a writer seeing
 * only itself) dates every snapshot by this node's clock ({@link StatusSnapshot#nodeClock}), so it
 * never writes the state.
 *
 * <p>Lease fields are null (there are no leases in peer mode), as are {@code publishedAt} (no
 * bucket LastModified) and every writer heartbeat field (peer writers send none: they appear with
 * {@code workQueue} false, so no writer proposal fires).
 */
public final class PeerStatusSource implements StatusSource {

    private static final Logger log = LoggerFactory.getLogger(PeerStatusSource.class);

    private final PeerCatalog catalog;
    private final ReplicaSource replicas;
    private final PeerHttp http;
    private final Duration timeout;
    private final Duration leaseTtl;
    private final LongSupplier nanoClock;
    private final Clock fallback;
    private final boolean partial;
    /** Manifests by shard, for the generation last read (manifests are immutable). */
    private final Map<String, ShardManifest> manifests = new ConcurrentHashMap<>();

    public PeerStatusSource(PeerCatalog catalog, ReplicaSource replicas, PeerHttp http, Duration timeout,
                            Duration leaseTtl, LongSupplier nanoClock, Clock fallback) {
        this(catalog, replicas, http, timeout, leaseTtl, nanoClock, fallback, false);
    }

    /**
     * @param partial the catalog sees part of the cluster only (a writer without
     *                {@code cluster.peers.discovery}): every snapshot is dated by this node's clock, so
     *                it reads the shared state but never writes it
     */
    public PeerStatusSource(PeerCatalog catalog, ReplicaSource replicas, PeerHttp http, Duration timeout,
                            Duration leaseTtl, LongSupplier nanoClock, Clock fallback, boolean partial) {
        this.partial = partial;
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.replicas = Objects.requireNonNull(replicas, "replicas");
        this.http = Objects.requireNonNull(http, "http");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.leaseTtl = leaseTtl;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.fallback = Objects.requireNonNull(fallback, "fallback");
    }

    @Override
    public StatusSnapshot read() throws IOException {
        List<ShardId> ids = catalog.shards(); // refreshes when due; throws before any writer answered
        List<PeerCatalog.WriterRef> live = catalog.liveWriters();
        List<PeerMembership.WriterReaders> answers = PeerMembership.fetch(http, live, timeout, nanoClock);
        if (!live.isEmpty() && answers.isEmpty()) {
            throw new IOException("no live writer answered GET /replication/readers");
        }
        long nowNanos = nanoClock.getAsLong();
        Instant asOf;
        boolean nodeClock = partial;
        if (partial) {
            asOf = fallback.instant();
        } else {
            // the holder is the lowest id the catalog still tracks (live or missing, not dropped): one
            // missed refresh must not move the state, so while it is unreachable the status stays as is
            PeerCatalog.WriterRef holder = PeerStatusState.holder(PeerStatusState.tracked(catalog)).orElseThrow(() ->
                    new IOException("no writer to hold the cluster status state"));
            PeerMembership.WriterReaders clock = answers.stream()
                    .filter(a -> a.writer().node().equals(holder.node())).findFirst().orElseThrow(() ->
                            new IOException("the status-state holder, writer " + holder + ", did not answer;"
                                    + " keeping the previous status"));
            asOf = Instant.ofEpochMilli(clock.nowMillis()).plusNanos(Math.max(0, nowNanos - clock.answeredNanos()));
        }

        List<StatusSnapshot.ReaderObs> readers = new ArrayList<>();
        for (PeerMembership.Merged m : PeerMembership.merge(answers).values()) {
            readers.add(new StatusSnapshot.ReaderObs(m.heartbeat(),
                    asOf.minusNanos(Math.max(0, nowNanos - m.receivedNanos()))));
        }
        List<StatusSnapshot.WriterObs> writers = new ArrayList<>();
        for (PeerCatalog.WriterRef w : live) {
            writers.add(new StatusSnapshot.WriterObs(new WriterHeartbeat(w.node(), false, List.of(), List.of(),
                    Map.of(), null, 0L, false), asOf));
        }
        List<StatusSnapshot.ShardObs> shards = new ArrayList<>();
        for (ShardId id : ids) shards.add(shard(id));
        return new StatusSnapshot(StatusSnapshot.CLUSTER, asOf, shards, writers, readers, leaseTtl, nodeClock);
    }

    private StatusSnapshot.ShardObs shard(ShardId id) {
        Long gen = catalog.listedLatest(id).orElse(null);
        String owner = catalog.owner(id).map(PeerCatalog.WriterRef::node).orElse(null);
        ShardManifest m = null;
        if (gen != null) {
            ShardManifest cached = manifests.get(id.value());
            if (cached != null && cached.generation() == gen) {
                m = cached;
            } else {
                try {
                    m = replicas.manifest(id, new Generation(gen));
                    manifests.put(id.value(), m);
                } catch (IOException | RuntimeException e) {
                    log.debug("Status: reading the manifest of shard {} gen {} failed: {}", id, gen, e.toString());
                }
            }
        }
        String rev = m == null || m.projectMeta() == null ? null : m.projectMeta().getSourceRevision();
        return new StatusSnapshot.ShardObs(id.value(), false, gen, m == null ? null : m.docCount(),
                m == null ? null : m.sizeBytes(), m != null && m.writerNode() != null ? m.writerNode() : owner,
                null, rev, null);
    }
}
