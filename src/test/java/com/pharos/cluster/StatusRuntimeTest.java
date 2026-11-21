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

            // a reader's status comes from the bucket
            ObjectStoreLayout layout = new ObjectStoreLayout(config.getCluster().getObjectStore().getPrefix());
            ClusterStatus empty = runtime.statusService().refresh();
            assertThat(empty.mode()).isEqualTo("cluster");
            assertThat(store.get(layout.statusStateKey())).as("an empty bucket has no clock: nothing is recorded")
                    .isEmpty();
            store.put(layout.readerKey("r9"), new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsBytes(before));
            ClusterStatus st = runtime.statusService().refresh();
            assertThat(st.layout().readers()).extracting(ClusterStatus.Reader::node)
                    .containsExactly(before.node());
            assertThat(store.get(layout.statusStateKey())).isPresent();
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
            var service = runtime.statusService();
            assertThat(service.timerRunning()).as("a single node starts no status timer of its own").isFalse();
            assertThat(service.events().keepAliveRunning()).as("nor a keep-alive timer").isFalse();
            assertThat(new String(service.currentJson(), java.nio.charset.StandardCharsets.UTF_8))
                    .contains("\"single-node\"");
            assertThat(service.events().keepAliveRunning()).isFalse();
            assertThat(service.timerRunning()).isFalse();

            // an SSE client starts the refresh and keep-alive timers; they stop when it leaves
            java.util.concurrent.atomic.AtomicReference<Runnable> onClose = new java.util.concurrent.atomic.AtomicReference<>();
            assertThat(service.events().tryReserve()).isTrue();
            service.events().add(new com.pharos.cluster.status.StatusEvents.Sink() {
                @Override public void event(String type, String data) { }
                @Override public void comment(String text) { }
                @Override public void onClose(Runnable r) { onClose.set(r); }
                @Override public void close() { }
            });
            assertThat(service.timerRunning()).isTrue();
            assertThat(service.events().keepAliveRunning()).isTrue();
            onClose.get().run(); // the client disconnects
            assertThat(service.timerRunning()).isFalse();
            assertThat(service.events().keepAliveRunning()).isFalse();

            ClusterStatus st = service.refresh();
            assertThat(st.mode()).isEqualTo("single-node");
            assertThat(st.layout().writers()).hasSize(1);
            assertThat(st.layout().readers()).hasSize(1);
            assertThat(runtime.statusService()).as("one service per runtime").isSameAs(runtime.statusService());
        }
    }
}
