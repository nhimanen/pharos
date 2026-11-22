package com.pharos.cluster.sharding;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ObjectStoreWriterStateStore;
import com.pharos.cluster.writer.Lease;
import com.pharos.cluster.writer.SourceCheckout;
import com.pharos.cluster.writer.WorkLoop;
import com.pharos.cluster.writer.WorkQueueLeaseGuard;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Found by the Phase 6 gate: a reshard build runs inside a work-loop tick, and the per-project reshard
 * lease was renewed only by the tick, so a build longer than the lease TTL lost the lease ("the lease of
 * writer w on shard p expired") at its first publish and the step had to run again. The work loop now
 * renews the reshard leases on their own thread, like the shard leases, whatever the loop thread does.
 */
class ReshardLeaseRenewalTest {

    @TempDir Path tmp;

    @Test
    void reshardLeaseIsRenewedWhileNoTickRuns() throws Exception {
        Duration ttl = Duration.ofSeconds(3);
        InMemoryObjectStore store = new InMemoryObjectStore(Clock.systemUTC());
        ObjectStoreLayout layout = new ObjectStoreLayout("pharos");
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        ObjectStoreReplicaSource replicas = new ObjectStoreReplicaSource(store, layout);
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        LuceneIndexer lucene = new LuceneIndexer(config);
        ObjectStoreWriterStateStore state = new ObjectStoreWriterStateStore(store, layout, replicas, "w1");
        GenerationGc gc = new GenerationGc(store, layout, 1, Duration.ofMinutes(10));
        WorkQueueLeaseGuard guard = new WorkQueueLeaseGuard();
        ObjectStorePublisher publisher = new ObjectStorePublisher(store, layout, catalog, gc, "w1", null, guard, state);
        ProjectIndexManager manager = new ProjectIndexManager(config, lucene, registry, List.of(),
                new ModuleGraphBuilder(registry), List.of(new JavaCodeParser()), publisher);
        // poll 1 h: after the first (empty) tick no tick runs during the test, as during a long reshard build
        WorkLoop loop = new WorkLoop(new WorkLoop.Settings("w1", ttl, Duration.ofSeconds(1), Duration.ofHours(1),
                Duration.ofSeconds(120), false, false), store, layout, catalog, replicas, state, manager, lucene,
                registry, new SourceCheckout(tmp.resolve("work")), Clock.systemUTC());
        guard.attach(loop);
        loop.enableResharding(publisher, gc, List::of, new ReshardJob.Settings(Duration.ofSeconds(600),
                Duration.ofMinutes(10)));
        try {
            loop.start();
            Thread.sleep(500); // the first tick (nothing to do) has run
            ReshardJob job = loop.reshardJob().orElseThrow();
            ShardId unit = ShardId.ofProject("lib");
            Optional<Lease> lease = job.leases().tryAcquire(unit);
            assertThat(lease).isPresent();
            String etag = store.get(layout.reshardLeaseKey("lib")).orElseThrow().etag();

            Thread.sleep(ttl.multipliedBy(3).toMillis()); // three TTLs with no tick

            assertThat(store.get(layout.reshardLeaseKey("lib")).orElseThrow().etag())
                    .as("the reshard lease object was rewritten (renewed)").isNotEqualTo(etag);
            assertThatCode(() -> job.leases().assertHeld(unit)).doesNotThrowAnyException();
        } finally {
            loop.close();
            lucene.close();
        }
    }
}
