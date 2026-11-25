package com.pharos.cluster;

import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.CountingEmbedder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link ClusterRuntime}'s writer branch: the work loop runs only with {@code cluster.writer.workQueue: true}. */
class WorkQueueRuntimeTest {

    @TempDir Path tmp;

    private record Node(ClusterRuntime runtime, ProjectIndexManager manager, LuceneIndexer lucene) {}

    private Node node(IndexConfig config, InMemoryObjectStore store) {
        ReplicationBackend backend = TestBackends.objectStore(config, store);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve(config.getCluster().getNodeId() + "-registry.json"));
        LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene);
        ProjectIndexManager pim = new ProjectIndexManager(config, lucene, registry, List.of(new CountingEmbedder()),
                ClusterFixture.noopModules(registry), List.of(new JavaCodeParser()), runtime.publisher());
        runtime.setIndexManager(pim);
        return new Node(runtime, pim, lucene);
    }

    private Path project() throws Exception {
        Path dir = tmp.resolve("src/svc");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("Svc.java"), "public class Svc {\n    public int a(int x) { return x; }\n}\n");
        return dir;
    }

    @Test
    void workQueueWriterRunsTheLoopAndRefusesManualPublishes() throws Exception {
        IndexConfig config = ClusterFixture.config(tmp.resolve("w"), NodeRole.WRITER, "b");
        config.getCluster().getWriter().setWorkQueue(true);
        config.getCluster().getWriter().setWorkDir(tmp.resolve("work"));
        config.getCluster().getWriter().setPollSeconds(3600);
        InMemoryObjectStore store = new InMemoryObjectStore();
        Node n = node(config, store);
        try {
            assertThat(n.runtime().isWorkQueueWriter()).isTrue();
            assertThat(n.runtime().manualPublishRefusal("svc")).get().asString().contains("work-queue mode");
            // outside the loop the publisher refuses: no lease is held by this thread's run
            Path dir = project();
            assertThatThrownBy(() -> n.manager().index(dir, "svc", false, false, false, false, ProgressListener.SILENT))
                    .isInstanceOf(PublishException.class).hasMessageContaining("does not hold the lease");
            assertThat(store.get(new ObjectStoreLayout("pharos").latestKey(new ShardId("svc")))).isEmpty();

            assertThat(n.runtime().beginWriting()).isTrue();
            assertThat(n.runtime().beginWriting()).isTrue(); // idempotent
            assertThat(n.runtime().workLoop()).isPresent();
            // the loop's first tick runs at once on its thread: the heartbeat appears
            String hb = new ObjectStoreLayout("pharos").writerKey("writer-1");
            long until = System.currentTimeMillis() + 10_000;
            while (store.get(hb).isEmpty() && System.currentTimeMillis() < until) Thread.sleep(20);
            assertThat(store.get(hb)).isPresent();
        } finally {
            n.runtime().close();
            n.lucene().close();
        }
        // close stopped the loop: heartbeat gone
        assertThat(store.get(new ObjectStoreLayout("pharos").writerKey("writer-1"))).isEmpty();
    }

    /**
     * Final review I3: a second process with the same writer config (the daemon ./pharos starts on
     * demand next to a systemd writer) must not run a second work loop on the same node id, index
     * dir and clones. It warns and serves without one; the loop's lock is free once the first stops.
     */
    @Test
    void secondProcessOnTheSameNodeRunsNoWorkLoop() throws Exception {
        IndexConfig config = ClusterFixture.config(tmp.resolve("w"), NodeRole.WRITER, "b");
        config.getCluster().getWriter().setWorkQueue(true);
        config.getCluster().getWriter().setWorkDir(tmp.resolve("work"));
        config.getCluster().getWriter().setPollSeconds(3600);
        InMemoryObjectStore store = new InMemoryObjectStore();
        var log = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ClusterRuntime.class);
        var appender = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        appender.start();
        log.addAppender(appender);
        Node first = node(config, store);
        Node second = node(config, store);
        Node third = null;
        try {
            assertThat(first.runtime().beginWriting()).isTrue();
            assertThat(tmp.resolve("work").resolve(ClusterRuntime.WORK_LOOP_LOCK)).isRegularFile();

            assertThat(second.runtime().beginWriting()).as("the node already has a writer").isFalse();
            assertThat(second.runtime().workLoop()).isEmpty();
            assertThat(appender.list).anyMatch(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN
                    && e.getFormattedMessage().contains("another process is the writer for node writer-1"));
            // it still serves: status works, and it stays a work-queue writer (hand runs refused)
            assertThat(second.runtime().statusService()).isNotNull();
            assertThat(second.runtime().manualPublishRefusal("svc")).isPresent();

            first.runtime().close();
            third = node(config, store);
            assertThat(third.runtime().beginWriting()).as("the lock is free once the first stopped").isTrue();
        } finally {
            log.detachAppender(appender);
            first.runtime().close();
            second.runtime().close();
            if (third != null) third.runtime().close();
            first.lucene().close();
            second.lucene().close();
            if (third != null) third.lucene().close();
        }
    }

    @Test
    void writerWithoutWorkQueueKeepsTheImplicitLease() throws Exception {
        IndexConfig config = ClusterFixture.config(tmp.resolve("w"), NodeRole.WRITER, "b");
        InMemoryObjectStore store = new InMemoryObjectStore();
        Node n = node(config, store);
        try {
            assertThat(n.runtime().isWorkQueueWriter()).isFalse();
            assertThat(n.runtime().manualPublishRefusal("svc")).isEmpty();
            assertThat(n.runtime().beginWriting()).isFalse();
            assertThat(n.runtime().workLoop()).isEmpty();
            n.manager().index(project(), "svc", false, false, false, false, ProgressListener.SILENT);
            assertThat(store.get(new ObjectStoreLayout("pharos").latestKey(new ShardId("svc")))).isPresent();
            assertThat(store.list(new ObjectStoreLayout("pharos").writersPrefix())).isEmpty();
        } finally {
            n.runtime().close();
            n.lucene().close();
        }
    }

    @Test
    void readersAndSingleNodeNeverStartALoop() throws Exception {
        for (NodeRole role : List.of(NodeRole.READER, NodeRole.ALL)) {
            IndexConfig config = ClusterFixture.config(tmp.resolve(role.json()), role, "b");
            config.getCluster().getWriter().setWorkQueue(true); // ignored off role writer
            ReplicationBackend backend = role == NodeRole.ALL ? Backends.create(config)
                    : TestBackends.objectStore(config, new InMemoryObjectStore());
            LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
            ClusterRuntime rt = new ClusterRuntime(config, backend, ClusterRuntime.registryFor(config), lucene);
            try {
                assertThat(rt.isWorkQueueWriter()).isFalse();
                assertThat(rt.beginWriting()).isFalse();
                assertThat(rt.manualPublishRefusal("x")).isEqualTo(Optional.empty());
            } finally {
                rt.close();
                lucene.close();
            }
        }
    }

    @Test
    void workQueueNeedsHeartbeatsBelowTheLeaseTtl() {
        IndexConfig config = ClusterFixture.config(tmp.resolve("w"), NodeRole.WRITER, "b");
        config.getCluster().getWriter().setWorkQueue(true);
        config.getCluster().setHeartbeatSeconds(30);
        config.getCluster().setReaderTtlSeconds(60);
        assertThatThrownBy(() -> config.getCluster().validate()).hasMessageContaining("leaseTtlSeconds");
        config.getCluster().getWriter().setMinHoldSeconds(-1);
        config.getCluster().setHeartbeatSeconds(10);
        assertThatThrownBy(() -> config.getCluster().validate()).hasMessageContaining("minHoldSeconds");
    }
}
