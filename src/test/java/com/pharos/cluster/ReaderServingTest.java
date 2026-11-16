package com.pharos.cluster;

import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.StoredObject;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static org.assertj.core.api.Assertions.assertThat;

/** Reader startup: the sync lock, followers, activation from the registry, non-blocking start. */
class ReaderServingTest {

    @TempDir Path tmp;

    private ClusterFixture cluster;
    private final List<AutoCloseable> extra = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : extra.reversed()) c.close();
        if (cluster != null) cluster.close();
    }

    /** A second reader process on the same cache dir: same config, its own backend, registry, indexer. */
    private record Reader(ClusterRuntime runtime, SearchEngine engine, LuceneIndexer lucene) {}

    private Reader secondReader(InMemoryObjectStore store) {
        ReplicationBackend backend = TestBackends.objectStore(cluster.readerConfig, store);
        ProjectRegistry registry = ClusterRuntime.registryFor(cluster.readerConfig);
        LuceneIndexer lucene = new LuceneIndexer(cluster.readerConfig, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(cluster.readerConfig, backend, registry, lucene);
        extra.add(lucene);
        extra.add(runtime);
        return new Reader(runtime, new SearchEngine(lucene, EmbeddingProvider.create(cluster.readerConfig), registry),
                lucene);
    }

    private static List<String> ids(SearchEngine engine, String query) throws IOException {
        return ids(engine, query, JAVA);
    }

    private static List<String> ids(SearchEngine engine, String query, String project) throws IOException {
        return engine.search(new SearchRequest(query, SearchRequest.SearchType.KEYWORD, project, null, 10,
                "text", null, null, 0)).stream().map(SearchResult::id).toList();
    }

    private void addMethodAndReindex(String method) throws IOException {
        Path calc = cluster.source.resolve(JAVA).resolve("src/main/java/com/example/Calculator.java");
        Files.writeString(calc, Files.readString(calc).replace("class Calculator {",
                "class Calculator {\n    public int " + method + "(int x) { return 3 * x; }\n"));
        Files.setLastModifiedTime(calc, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        cluster.index(JAVA, true);
    }

    @Test
    void secondProcessOnSameCacheDirFollowsInsteadOfSyncing() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        cluster.readerRuntime.startServing();
        assertThat(cluster.readerRuntime.ownsSync()).isTrue();

        Reader second = secondReader(store);
        second.runtime().startServing();
        assertThat(second.runtime().ownsSync()).as("lock held by the first reader").isFalse();
        assertThat(second.runtime().ready()).isTrue();
        assertThat(ids(second.engine(), "Calculator")).isNotEmpty();

        // A partial download of the owner must survive the follower (it never deletes anything).
        Path partial = Files.createDirectories(cluster.readerConfig.getCluster().getCacheDir()
                .resolve(JAVA).resolve("gen-99.partial"));
        addMethodAndReindex("tripleTheValue");
        second.runtime().syncNow(); // follower: re-reads the registry only
        assertThat(partial).isDirectory();
        assertThat(ids(second.engine(), "tripleTheValue")).isEmpty();

        cluster.readerRuntime.syncNow(); // the owner syncs generation 2 and registers it
        second.runtime().syncNow();
        assertThat(ids(second.engine(), "tripleTheValue"))
                .contains("sample-app:com.example.Calculator#tripleTheValue(int)");

        // once the owner is gone, the follower takes over the lock and syncs itself
        cluster.readerRuntime.close();
        second.runtime().followerTick();
        assertThat(second.runtime().ownsSync()).isTrue();
        second.runtime().syncNow();
        assertThat(second.runtime().served()).containsEntry(new ShardId(JAVA), new Generation(2));
    }

    @Test
    void readerWithoutSyncerServesRegisteredGenerations() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        cluster.readerRuntime.startServing();
        cluster.readerRuntime.close();

        // A one-shot command (java -jar … search) on the reader: never starts serving.
        Reader oneShot = secondReader(store);
        assertThat(oneShot.runtime().ownsSync()).isFalse();
        assertThat(oneShot.lucene().getProjectIndexDir(JAVA).getFileName().toString()).isEqualTo("gen-1");
        assertThat(ids(oneShot.engine(), "Calculator")).isNotEmpty();
    }

    /** A store whose catalog read blocks until released: the first sync cannot finish. */
    private static final class BlockingStore extends InMemoryObjectStore {
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch entered = new CountDownLatch(1);
        volatile boolean block = false;

        @Override
        public Optional<StoredObject> get(String key) {
            if (block && key.endsWith("catalog/shards.json")) {
                entered.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.get(key);
        }
    }

    @Test
    void beginServingDoesNotWaitForTheFirstSync() throws Exception {
        BlockingStore store = new BlockingStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        store.block = true;

        try {
            cluster.readerRuntime.beginServing(); // returns while the first sync is blocked
            assertThat(store.entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(cluster.readerRuntime.ready()).isFalse();
            assertThat(ids(cluster.readerEngine, "Calculator", null)).as("no projects yet").isEmpty();
        } finally {
            store.release.countDown();
        }
        cluster.readerRuntime.awaitFirstSync();
        assertThat(cluster.readerRuntime.ready()).isTrue();
        assertThat(ids(cluster.readerEngine, "Calculator")).isNotEmpty();
    }

    @Test
    void nonReaderIsAlwaysReady() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        assertThat(cluster.writerRuntime.ready()).isTrue();
        cluster.writerRuntime.beginServing(); // no-op
        assertThat(cluster.writerRuntime.ownsSync()).isFalse();
    }
}
