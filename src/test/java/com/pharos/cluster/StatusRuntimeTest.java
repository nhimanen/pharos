package com.pharos.cluster;

import com.pharos.cluster.membership.LatencyWindow;
import com.pharos.cluster.membership.LoadCounter;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.cluster.status.ClusterStatus;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.LuceneIndexer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Task 5: the runtime wiring of {@code /cluster/status} and the reader heartbeat's new fields. */
class StatusRuntimeTest {

    @TempDir Path tmp;

    @Test
    void readerHeartbeatCarriesWarmP95() throws Exception {
        IndexConfig config = ClusterFixture.config(tmp.resolve("r"), NodeRole.READER, "b");
        InMemoryObjectStore store = new InMemoryObjectStore();
        ReplicationBackend backend = TestBackends.objectStore(config, store);
        try (LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
             ClusterRuntime runtime = new ClusterRuntime(config, backend,
                     ClusterRuntime.registryFor(config), lucene)) {
            AtomicLong now = new AtomicLong(10_000_000_000L);
            LoadCounter load = new LoadCounter(new LatencyWindow(now::get));
            runtime.setLoad(load);
            ReaderHeartbeat before = runtime.selfHeartbeat();
            assertThat(before.warmP95Millis()).as("no request yet").isNull();
            assertThat(before.catalogAgeSeconds()).as("no coordinator yet").isNull();
            load.begin();
            load.end(42_000_000L);
            assertThat(runtime.selfHeartbeat().warmP95Millis()).isEqualTo(50L);

            // a reader's status comes from the bucket: its own state object lands there
            ClusterStatus st = runtime.statusService().refresh();
            assertThat(st.mode()).isEqualTo("cluster");
            assertThat(store.get(new ObjectStoreLayout(config.getCluster().getObjectStore().getPrefix())
                    .statusStateKey())).isPresent();
        }
    }

    @Test
    void singleNodeStatusFromLocalState() throws Exception {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        try (ReplicationBackend backend = Backends.create(config);
             LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
             ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene)) {
            ClusterStatus st = runtime.statusService().refresh();
            assertThat(st.mode()).isEqualTo("single-node");
            assertThat(st.layout().writers()).hasSize(1);
            assertThat(st.layout().readers()).hasSize(1);
            assertThat(runtime.statusService()).as("one service per runtime").isSameAs(runtime.statusService());
        }
    }
}
