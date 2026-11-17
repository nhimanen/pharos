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

    private ReaderLocalCache lastCache;

    private Reader secondReader(InMemoryObjectStore store) {
        return secondReader(store, cluster.readerConfig);
    }

    /** A follower with its own, independent config (same home, so the same cache dir). */
    private Reader independentFollower(InMemoryObjectStore store, List<String> pin) {
        com.pharos.config.IndexConfig own = ClusterFixture.config(tmp.resolve("reader"), NodeRole.READER, "test-bucket");
        own.getCluster().getReader().setShards(pin);
        return secondReader(store, own);
    }

    private Reader secondReader(InMemoryObjectStore store, com.pharos.config.IndexConfig cfg) {
        ReplicationBackend backend = TestBackends.objectStore(cfg, store);
        lastCache = (ReaderLocalCache) backend.localCache();
        ProjectRegistry registry = ClusterRuntime.registryFor(cfg);
        LuceneIndexer lucene = new LuceneIndexer(cfg, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(cfg, backend, registry, lucene);
        extra.add(lucene);
        extra.add(runtime);
        return new Reader(runtime, new SearchEngine(lucene, EmbeddingProvider.create(cfg), registry),
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

    /**
     * A reader is ready, and logs "serving N shard(s)", only once its first sync is served AND its
     * search models have loaded: the router and load balancers send traffic on that signal.
     */
    @Test
    void readyWaitsForTheModelsAsWellAsTheFirstSync() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        java.util.concurrent.CompletableFuture<Void> models = new java.util.concurrent.CompletableFuture<>();
        var log = captureRuntimeLog();
        try {
            cluster.readerRuntime.beginServing(models);
            cluster.readerRuntime.awaitFirstSync();
            assertThat(cluster.readerRuntime.ready()).as("first sync done, models still loading").isFalse();
            assertThat(servingLines(log)).isEmpty();
            models.complete(null);
            assertThat(cluster.readerRuntime.ready()).isTrue();
            assertThat(servingLines(log)).singleElement().asString().contains("serving 1 shard(s)");
        } finally {
            detach(log);
        }
    }

    /** Models that settle first (loaded, failed or timed out): ready as soon as the first sync is served. */
    @Test
    void modelsSettledBeforeTheSyncDoNotDelayReadiness() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        cluster.readerRuntime.beginServing(java.util.concurrent.CompletableFuture.failedFuture(
                new IllegalStateException("model failed to load")));
        cluster.readerRuntime.awaitFirstSync();
        assertThat(cluster.readerRuntime.ready()).as("a failed model load never blocks readiness").isTrue();
    }

    @Test
    void followerAlsoWaitsForTheModels() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        cluster.readerRuntime.startServing();
        Reader second = secondReader(store);
        java.util.concurrent.CompletableFuture<Void> models = new java.util.concurrent.CompletableFuture<>();
        second.runtime().beginServing(models);
        assertThat(second.runtime().ownsSync()).isFalse();
        assertThat(second.runtime().ready()).isFalse();
        models.complete(null);
        assertThat(second.runtime().ready()).isTrue();
    }

    private static List<String> servingLines(ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> a) {
        return a.list.stream().map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
                .filter(m -> m.contains(" serving ")).toList();
    }

    private static void detach(ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> a) {
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ClusterRuntime.class);
        logger.detachAppender(a);
        logger.setLevel(null);
    }

    @Test
    void nonReaderIsAlwaysReady() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        assertThat(cluster.writerRuntime.ready()).isTrue();
        cluster.writerRuntime.beginServing(); // no-op
        assertThat(cluster.writerRuntime.ownsSync()).isFalse();
    }

    // ── Pre-PR fix 1: a restarted (e.g. downgraded) reader only activates servable generations ──

    private static void restamp(Path genDir, int format) throws IOException {
        com.pharos.indexer.IndexFormat.write(genDir, new com.pharos.indexer.IndexFormat.Stamp(format, "9.9.9", null));
        Files.setLastModifiedTime(genDir.resolve(com.pharos.indexer.IndexFormat.FILE_NAME),
                FileTime.fromMillis(System.currentTimeMillis() + 120_000)); // IndexFormat caches by mtime
    }

    private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> captureRuntimeLog() {
        var appender = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        appender.start();
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ClusterRuntime.class);
        logger.addAppender(appender);
        if (!logger.isInfoEnabled()) logger.setLevel(ch.qos.logback.classic.Level.INFO);
        return appender;
    }

    private static List<String> warnings(ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> a) {
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ClusterRuntime.class)).detachAppender(a);
        return a.list.stream().filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
                .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage).toList();
    }

    /** gen-1 and gen-2 on disk, gen-2 registered; the reader runtime closed again. */
    private Path twoGenerationsOnDisk(InMemoryObjectStore store) throws Exception {
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        cluster.readerRuntime.startServing();
        addMethodAndReindex("tripleTheValue");
        cluster.readerRuntime.syncNow();
        cluster.readerRuntime.close();
        Path shardRoot = cluster.readerConfig.getCluster().getCacheDir().resolve(JAVA);
        assertThat(ClusterRuntime.registryFor(cluster.readerConfig).find(JAVA).orElseThrow().getIndexPath())
                .isEqualTo(shardRoot.resolve("gen-2").toString());
        assertThat(shardRoot.resolve("gen-1")).isDirectory();
        return shardRoot;
    }

    @Test
    void restartFallsBackToTheNewestServableGeneration() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        Path shardRoot = twoGenerationsOnDisk(store);
        int newer = com.pharos.indexer.IndexVersions.INDEX_FORMAT + 1;
        restamp(shardRoot.resolve("gen-2"), newer); // published by a newer pharos, then this reader was downgraded

        var log = captureRuntimeLog();
        Reader restarted = secondReader(store);
        List<String> warns = warnings(log);

        assertThat(restarted.lucene().getProjectIndexDir(JAVA)).isEqualTo(shardRoot.resolve("gen-1").toAbsolutePath().normalize());
        assertThat(ClusterRuntime.registryFor(cluster.readerConfig).find(JAVA).orElseThrow().getIndexPath())
                .isEqualTo(shardRoot.resolve("gen-1").toAbsolutePath().normalize().toString());
        assertThat(ids(restarted.engine(), "Calculator")).isNotEmpty();
        assertThat(warns).anySatisfy(w -> assertThat(w).contains(JAVA, "gen-2", "index format " + newer,
                "Serving gen-1 (index format " + com.pharos.indexer.IndexVersions.INDEX_FORMAT + ")"));
    }

    @Test
    void restartWithoutAServableGenerationWarnsAndServesNothing() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        Path shardRoot = twoGenerationsOnDisk(store);
        int newer = com.pharos.indexer.IndexVersions.INDEX_FORMAT + 1;
        restamp(shardRoot.resolve("gen-1"), newer);
        restamp(shardRoot.resolve("gen-2"), newer);

        var log = captureRuntimeLog();
        Reader restarted = secondReader(store);
        List<String> warns = warnings(log);

        assertThat(restarted.lucene().getProjectIndexDir(JAVA).getFileName().toString()).isEqualTo("none");
        assertThat(warns).anySatisfy(w -> assertThat(w).contains(JAVA, "gen-2", "index format " + newer,
                "reads index formats", "No servable generation on disk"));
    }

    @Test
    void unpinnedFollowerStopsServingAShardTheOwnerPinnedAway() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        cluster.readerRuntime.startServing();
        Reader follower = independentFollower(store, List.of()); // its own config: no pin
        follower.runtime().startServing();
        assertThat(follower.runtime().ownsSync()).isFalse();
        assertThat(lastCache.active(JAVA)).isPresent();
        Path dir = lastCache.active(JAVA).orElseThrow();

        cluster.readerConfig.getCluster().getReader().setShards(List.of("elsewhere")); // only the owner
        cluster.readerRuntime.repin();
        cluster.readerRuntime.syncNow();
        follower.runtime().followerTick();

        assertThat(lastCache.active(JAVA)).as("follower deactivated it via the registry").isEmpty();
        assertThat(dir).as("grace not elapsed").isDirectory();
    }

    @Test
    void pinnedFollowerDeactivatesAShardTheRegistryStillLists() throws Exception {
        InMemoryObjectStore store = new InMemoryObjectStore();
        cluster = new ClusterFixture(tmp, store, store, "test-bucket");
        cluster.index(JAVA, false);
        cluster.readerRuntime.startServing();
        Reader follower = independentFollower(store, List.of());
        follower.runtime().startServing();
        assertThat(lastCache.active(JAVA)).isPresent();

        Path dir = lastCache.active(JAVA).orElseThrow();

        // a follower pinned elsewhere, holding the shard active (as after a pin change), registry still lists it
        Reader pinned = independentFollower(store, List.of("elsewhere"));
        pinned.runtime().startServing();
        assertThat(lastCache.active(JAVA)).as("a pinned follower never activates it").isEmpty();
        lastCache.activate(JAVA, dir);
        pinned.runtime().followerTick();
        assertThat(cluster.readerRegistry.find(JAVA)).isPresent();
        assertThat(lastCache.active(JAVA)).isEmpty();
    }
}
