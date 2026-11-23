package com.pharos.cluster.writer;

import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ObjectStoreWriterStateStore;
import com.pharos.cluster.sharding.ReshardJob;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.IndexAbort;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The per-project reshard lease, renewed on the work loop's own thread (found by the Phase 6 gate: a
 * reshard build runs inside a tick, which renewed it only between builds, so any build longer than the
 * TTL lost it) and only while the writer holds the project's work unit (Phase 6 review): once the unit's
 * shard leases are lost, the running build is aborted and the reshard lease released at once, so the
 * writer holding the project now takes the reshard over without waiting out the TTL.
 */
class ReshardLeaseRenewalTest {

    private static final Duration TTL = Duration.ofSeconds(3);
    private static final ShardId UNIT = ShardId.ofProject("lib");

    @TempDir Path tmp;
    private InMemoryObjectStore store;
    private ObjectStoreLayout layout;
    private WorkLoop loop;
    private LuceneIndexer lucene;

    @BeforeEach
    void startWriterHoldingTheProject() throws Exception {
        store = new InMemoryObjectStore(Clock.systemUTC());
        layout = new ObjectStoreLayout("pharos");
        ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
        ObjectStoreReplicaSource replicas = new ObjectStoreReplicaSource(store, layout);
        Path src = tmp.resolve("src");
        Files.createDirectories(src.resolve("com/example"));
        Files.writeString(src.resolve("com/example/Lib.java"),
                "package com.example;\npublic class Lib {\n    public int f(int x) { return x; }\n}\n",
                StandardCharsets.UTF_8);
        catalog.register(UNIT, Optional.of(ShardSource.path(src.toString())));
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        lucene = new LuceneIndexer(config);
        ObjectStoreWriterStateStore state = new ObjectStoreWriterStateStore(store, layout, replicas, "w1");
        GenerationGc gc = new GenerationGc(store, layout, 1, Duration.ofMinutes(10));
        WorkQueueLeaseGuard guard = new WorkQueueLeaseGuard();
        ObjectStorePublisher publisher = new ObjectStorePublisher(store, layout, catalog, gc, "w1", null, guard, state);
        ProjectIndexManager manager = new ProjectIndexManager(config, lucene, registry, List.of(),
                new ModuleGraphBuilder(registry), List.of(new JavaCodeParser()), publisher);
        // poll 1 h: the first tick takes the project (and publishes it); no tick runs after it, as during a
        // long reshard build
        loop = new WorkLoop(new WorkLoop.Settings("w1", TTL, Duration.ofSeconds(1), Duration.ofHours(1),
                Duration.ofSeconds(120), false, false), store, layout, catalog, replicas, state, manager, lucene,
                registry, new SourceCheckout(tmp.resolve("work")), Clock.systemUTC());
        guard.attach(loop);
        loop.enableResharding(publisher, gc, List::of, new ReshardJob.Settings(Duration.ofSeconds(600),
                Duration.ofMinutes(10)));
        loop.start();
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (!loop.heldShards().contains(UNIT) && System.nanoTime() < deadline) Thread.sleep(50);
        assertThat(loop.heldShards()).as("the writer holds the project").contains(UNIT);
    }

    @AfterEach
    void stop() {
        if (loop != null) loop.close();
        if (lucene != null) lucene.close();
    }

    @Test
    void reshardLeaseIsRenewedWhileNoTickRuns() throws Exception {
        ReshardJob job = loop.reshardJob().orElseThrow();
        assertThat(job.leases().tryAcquire(UNIT)).isPresent();
        String etag = store.get(layout.reshardLeaseKey("lib")).orElseThrow().etag();

        Thread.sleep(TTL.multipliedBy(3).toMillis()); // three TTLs with no tick

        assertThat(store.get(layout.reshardLeaseKey("lib")).orElseThrow().etag())
                .as("the reshard lease object was rewritten (renewed)").isNotEqualTo(etag);
        assertThatCode(() -> job.leases().assertHeld(UNIT)).doesNotThrowAnyException();
    }

    @Test
    void losingTheProjectReleasesTheReshardLeaseAndAbortsTheBuild() throws Exception {
        ReshardJob job = loop.reshardJob().orElseThrow();
        assertThat(job.leases().tryAcquire(UNIT)).isPresent();
        IndexAbort build = loop.runningForTest(UNIT); // the reshard build of lib is running

        // another writer takes the project's shard lease over (its object no longer carries w1's ETag)
        store.put(layout.leaseKey(UNIT), "{\"writer\":\"w2\",\"released\":false}".getBytes(StandardCharsets.UTF_8));

        long interval = TTL.dividedBy(3).toMillis();
        long deadline = System.nanoTime() + (3 * interval + 1000) * 1_000_000L; // loss seen, then one interval
        while (job.leases().isHeld(UNIT) && System.nanoTime() < deadline) Thread.sleep(20);
        assertThat(job.leases().isHeld(UNIT)).as("the reshard lease is released").isFalse();
        assertThat(build.requested()).as("the running build is aborted").isTrue();

        // B takes the reshard lease at once, without waiting out the TTL
        LeaseManager b = new LeaseManager(store, layout, "w2", TTL, LeaseManager.BucketNow.NONE, Clock.systemUTC(),
                id -> layout.reshardLeaseKey(id.value()));
        assertThat(b.tryAcquire(UNIT)).as("B proceeds").isPresent();
    }
}
