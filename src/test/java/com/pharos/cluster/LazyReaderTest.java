package com.pharos.cluster;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.objectstore.TestBackends;
import com.pharos.cluster.warm.BlockCache;
import com.pharos.cluster.warm.BlockKey;
import com.pharos.cluster.warm.LazyGenerations;
import com.pharos.cluster.warm.RemoteDirectory;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.GraphViews;
import com.pharos.indexer.LuceneDirectoryFactory;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.pharos.cluster.ClusterFixture.JAVA;
import static org.assertj.core.api.Assertions.assertThat;

/** Phase 2: reader generations served lazily through the block cache. */
class LazyReaderTest {

    private static final ShardId SHARD = new ShardId(JAVA);
    private static final long KIB = 1024;
    private static final long EAGER = 512;
    private static final String BUCKET = "test-bucket";

    @TempDir Path tmp;

    private InMemoryObjectStore store;
    private ObjectStoreLayout layout;
    private ClusterFixture cluster; // the writer; its own reader is never started
    private final List<AutoCloseable> open = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryObjectStore();
        layout = new ObjectStoreLayout("pharos");
        cluster = new ClusterFixture(tmp, store, store, BUCKET);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : open.reversed()) c.close();
        cluster.close();
    }

    /** A reader node wired like {@code Main}, in its own home dir. */
    private record Node(IndexConfig config, ReplicationBackend backend, ClusterRuntime runtime, LuceneIndexer lucene,
                        SearchEngine engine) {
        Path genDir(long gen) {
            return config.getCluster().getCacheDir().resolve(JAVA).resolve("gen-" + gen);
        }

        void close() {
            runtime.close();
            lucene.close();
        }
    }

    private Node reader(String home, Consumer<ClusterConfig.ReaderConfig> tune) {
        IndexConfig config = ClusterFixture.config(tmp.resolve(home), NodeRole.READER, BUCKET);
        tune.accept(config.getCluster().getReader());
        config.getCluster().validate();
        ReplicationBackend backend = TestBackends.objectStore(config, store);
        ProjectRegistry registry = ClusterRuntime.registryFor(config);
        LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
        ClusterRuntime runtime = new ClusterRuntime(config, backend, registry, lucene);
        Node node = new Node(config, backend, runtime, lucene,
                new SearchEngine(lucene, EmbeddingProvider.create(config), registry));
        open.add(node::close);
        return node;
    }

    private static void small(ClusterConfig.ReaderConfig r) {
        r.setEagerMaxBytes(EAGER);
        r.setBlockSizeBytes(64 * KIB);
        r.setCacheMaxBytes(64 * KIB * KIB);
    }

    /**
     * Indexes sample-app, rewrites the writer's index as one segment without compound files (so the
     * terms dictionary is in its own .tim/.tip/.tmd files, as in a large shard) and publishes it.
     */
    private long publishWithoutCompoundFiles() throws IOException {
        cluster.index(JAVA, false);
        Path projectDir = cluster.writerLucene.getProjectIndexDir(JAVA);
        TieredMergePolicy mp = new TieredMergePolicy();
        mp.setNoCFSRatio(0.0);
        IndexWriterConfig iwc = new IndexWriterConfig(LuceneIndexer.buildAnalyzer())
                .setUseCompoundFile(false).setMergePolicy(mp);
        try (FSDirectory d = FSDirectory.open(projectDir.resolve("lucene")); IndexWriter w = new IndexWriter(d, iwc)) {
            w.forceMerge(1);
            w.commit();
        }
        // forceMerge of an index that is already one segment can leave its compound file alone
        // (seen intermittently): copy the segment into a fresh index that never uses compound files.
        if (hasCompoundFile(projectDir.resolve("lucene"))) rewriteWithoutCompoundFiles(projectDir.resolve("lucene"));
        ProjectMeta meta = cluster.writerRegistry.find(JAVA).orElseThrow();
        long gen = cluster.writerRuntime.publisher().publish(new LocalSnapshot(JAVA, projectDir, meta, "none"))
                .orElseThrow().value();
        List<String> lucene = manifest(gen).files().stream().filter(f -> ManifestFile.LUCENE.equals(f.role()))
                .map(ManifestFile::name).toList();
        assertThat(lucene).as("fixture: terms dictionary in its own file").anyMatch(n -> n.endsWith(".tim"));
        assertThat(lucene).as("fixture: no compound file").noneMatch(n -> n.endsWith(".cfs"));
        return gen;
    }

    private static boolean hasCompoundFile(Path luceneDir) throws IOException {
        return names(luceneDir).stream().anyMatch(n -> n.endsWith(".cfs"));
    }

    private static void rewriteWithoutCompoundFiles(Path luceneDir) throws IOException {
        Path fresh = luceneDir.resolveSibling("lucene.rewrite");
        IndexWriterConfig iwc = new IndexWriterConfig(LuceneIndexer.buildAnalyzer())
                .setUseCompoundFile(false).setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE);
        try (FSDirectory src = FSDirectory.open(luceneDir); DirectoryReader r = DirectoryReader.open(src);
             FSDirectory dst = FSDirectory.open(fresh); IndexWriter w = new IndexWriter(dst, iwc)) {
            org.apache.lucene.index.CodecReader[] leaves = r.leaves().stream()
                    .map(c -> (org.apache.lucene.index.CodecReader) c.reader())
                    .toArray(org.apache.lucene.index.CodecReader[]::new);
            w.addIndexes(leaves);
            w.commit();
        }
        try (Stream<Path> s = Files.list(luceneDir)) {
            for (Path p : s.toList()) Files.delete(p);
        }
        try (Stream<Path> s = Files.list(fresh)) {
            for (Path p : s.toList()) Files.move(p, luceneDir.resolve(p.getFileName()));
        }
        Files.delete(fresh);
    }

    private void addMethodAndReindex(String method) throws IOException {
        Path calc = cluster.source.resolve(JAVA).resolve("src/main/java/com/example/Calculator.java");
        Files.writeString(calc, Files.readString(calc).replace("class Calculator {",
                "class Calculator {\n    public int " + method + "(int x) { return 3 * x; }\n"));
        Files.setLastModifiedTime(calc, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        cluster.index(JAVA, true);
    }

    private ShardManifest manifest(long gen) throws IOException {
        return new ObjectStoreReplicaSource(store, layout).manifest(SHARD, new Generation(gen));
    }

    private static boolean alwaysEager(String name) {
        return name.startsWith("segments_") || name.endsWith(".si") || name.endsWith(".fnm") || name.endsWith(".cfe");
    }

    private static String extension(String name) {
        return name.substring(name.lastIndexOf('.') + 1);
    }

    private static List<String> ids(SearchEngine engine, String query) throws IOException {
        return engine.search(new SearchRequest(query, SearchRequest.SearchType.KEYWORD, JAVA, null, 10,
                "text", null, null, 0)).stream().map(SearchResult::id).toList();
    }

    private static List<String> names(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    // ---------------------------------------------------------------------------------------

    @Test
    void lazyGenerationHasOnlySmallFiles() throws Exception {
        long gen = publishWithoutCompoundFiles();
        // eagerMaxBytes 0: only the always-eager names (and empty files) are local, whatever the file sizes
        Node lazy = reader("lazy", r -> { small(r); r.setEagerMaxBytes(0); });
        Node full = reader("full", r -> r.setMode(ClusterConfig.ReaderConfig.FULL));
        lazy.runtime().startServing();
        full.runtime().startServing();
        assertThat(lazy.runtime().served()).isEqualTo(Map.of(SHARD, new Generation(gen)));
        assertThat(full.runtime().served()).isEqualTo(Map.of(SHARD, new Generation(gen)));

        Path dir = lazy.genDir(gen);
        assertThat(names(dir)).containsExactlyInAnyOrder("manifest.json", "index-format.json",
                GraphViews.COMPACT_FILE, LazyGenerations.MARKER, "lucene");
        ShardManifest m = manifest(gen);
        List<ManifestFile> remote = new ArrayList<>();
        for (ManifestFile f : m.files()) {
            if (!ManifestFile.LUCENE.equals(f.role())) continue;
            Path local = dir.resolve("lucene").resolve(f.name());
            if (alwaysEager(f.name()) || f.length() == 0) {
                assertThat(local).as(f.name()).isRegularFile().hasSize(f.length());
            } else {
                assertThat(local).as(f.name()).doesNotExist();
                remote.add(f);
            }
        }
        List<String> prefetch = lazy.config().getCluster().getReader().getPrefetchExtensions();
        List<ManifestFile> prefetched = remote.stream().filter(f -> prefetch.contains(extension(f.name()))).toList();
        assertThat(prefetched).as("remote .tim/.tip files among %s", m.files()).isNotEmpty();
        assertThat(remote).hasSizeGreaterThan(prefetched.size());

        BlockCache blocks = lazy.runtime().lazyGenerations().blockCache();
        long bs = blocks.blockSize();
        for (ManifestFile f : prefetched) {
            for (long i = 0; i * bs < f.length(); i++) {
                assertThat(blocks.isCached(new BlockKey(f.key(), i))).as("%s block %d", f.name(), i).isTrue();
            }
        }

        // The full-mode reader of the same generation: every file local, no marker.
        assertThat(full.genDir(gen).resolve(LazyGenerations.MARKER)).doesNotExist();
        assertThat(full.lucene().directoryFactory()).isSameAs(LuceneDirectoryFactory.DEFAULT);
        int withHits = 0;
        for (String q : List.of("Calculator", "greeting service", "add two numbers", "user email",
                "division by zero", "getName", "absolute value")) {
            List<String> want = ids(full.engine(), q);
            assertThat(ids(lazy.engine(), q)).as(q).isEqualTo(want);
            if (!want.isEmpty()) withHits++;
        }
        assertThat(withHits).isGreaterThan(3);
    }

    /** Review Focus 5: a swap and a prune never pull bytes from under an in-flight read. */
    @Test
    void swapDuringReadKeepsOldGenerationReadable() throws Exception {
        long g1 = publishWithoutCompoundFiles();
        Node node = reader("lazy", r -> {
            r.setEagerMaxBytes(0);
            r.setBlockSizeBytes(64 * KIB);
            r.setCacheMaxBytes(4 * 64 * KIB); // the minimum: generations compete for the cache
        });
        node.runtime().startServing();
        LazyGenerations lazy = node.runtime().lazyGenerations();

        Directory d1 = lazy.open(node.genDir(g1).resolve("lucene"));
        assertThat(d1).isInstanceOf(RemoteDirectory.class);
        DirectoryReader old = DirectoryReader.open(d1);
        int docs = new IndexSearcher(old).count(new MatchAllDocsQuery());
        String doc0 = old.storedFields().document(0).toString();
        ManifestFile big = manifest(g1).files().stream()
                .filter(f -> ManifestFile.LUCENE.equals(f.role()) && !alwaysEager(f.name()))
                .max(Comparator.comparingLong(ManifestFile::length)).orElseThrow();
        byte[] expected = store.get(big.key()).orElseThrow().data();
        IndexInput in = d1.openInput(big.name(), IOContext.DEFAULT);
        byte[] got = new byte[expected.length];
        int half = expected.length / 2;
        in.readBytes(got, 0, half);

        addMethodAndReindex("triple");
        node.runtime().syncNow();
        addMethodAndReindex("quadruple");
        node.runtime().syncNow();
        assertThat(node.runtime().served()).isEqualTo(Map.of(SHARD, new Generation(g1 + 2)));
        assertThat(node.genDir(g1)).as("generation %d pruned", g1).doesNotExist();
        assertThat(ids(node.engine(), "triple")).isNotEmpty();
        assertThat(ids(node.engine(), "Calculator")).isNotEmpty();
        // Fill the (4-block) cache with other objects: every unleased block of generation g1 is evicted.
        BlockCache blocks = lazy.blockCache();
        long fetchedBefore = blocks.stats().bytesFetched();
        for (int i = 0; i < 5; i++) {
            blocks.acquire(new BlockKey("filler-" + i, 0), blocks.blockSize(),
                    (key, offset, length, target) -> Files.write(target, new byte[(int) length])).close();
        }
        assertThat(blocks.isCached(new BlockKey(big.key(), 0))).isFalse();
        assertThat(blocks.stats().evictions()).isPositive();

        in.readBytes(got, half, expected.length - half);
        assertThat(got).isEqualTo(expected);
        assertThat(new IndexSearcher(old).count(new MatchAllDocsQuery())).isEqualTo(docs);
        assertThat(old.storedFields().document(0).toString()).isEqualTo(doc0);
        assertThat(blocks.stats().bytesFetched() - fetchedBefore)
                .as("evicted blocks of the old generation fetched again")
                .isGreaterThan(5L * blocks.blockSize());
        in.close();
        old.close();
        d1.close();
        assertThat(blocks.stats().leasesOpen()).isZero();
    }

    @Test
    void prefetchTimeoutActivatesAnyway() throws Exception {
        long gen = publishWithoutCompoundFiles();
        CountDownLatch release = new CountDownLatch(1);
        ReplicaSource real = new ObjectStoreReplicaSource(store, layout);
        ReplicaSource slow = new SlowRanges(real, release);
        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        rc.setPrefetchTimeoutSeconds(1);
        Path cacheDir = tmp.resolve("slow-cache");
        ReaderLocalCache cache = new ReaderLocalCache(cacheDir);
        var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReplicaSyncer.class);
        ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        logger.addAppender(log);
        try (LazyGenerations lazy = new LazyGenerations(cacheDir, rc, slow);
             ReplicaSyncer syncer = new ReplicaSyncer(new ObjectStoreCatalog(store, layout), slow, cache,
                     ProjectRegistry.at(tmp.resolve("slow-registry.json")), cacheDir, null,
                     Duration.ofSeconds(10), lazy)) {
            long start = System.nanoTime();
            syncer.syncOnce();
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            release.countDown();

            assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(gen)));
            assertThat(cache.active(JAVA)).contains(cacheDir.resolve(JAVA).resolve("gen-" + gen));
            assertThat(millis).isBetween(900L, 20_000L);
        } finally {
            release.countDown();
            logger.detachAppender(log);
        }
        List<String> warns = log.list.stream().filter(e -> e.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage).toList();
        assertThat(warns).anySatisfy(m -> assertThat(m).contains(JAVA, "generation " + gen, "prefetch", "1 s",
                "warm open", "5 s"));
        // Final review M4: the warm open is still attempted after a prefetch timeout, with a small budget.
        assertThat(warns).anySatisfy(m -> assertThat(m).contains("opening generation " + gen,
                "did not finish", "activating it anyway"));
    }

    /**
     * I1: a sync downloads each remote file's head and warm-opens the generation before activating
     * it, so neither that open nor a query's later open fetches a block.
     */
    @Test
    void freshLazyGenerationOpensWithoutFetching() throws Exception {
        long gen = publishWithoutCompoundFiles();
        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        rc.setEagerMaxBytes(0);
        rc.setPrefetchExtensions(List.of());
        Path cacheDir = tmp.resolve("edges-cache");
        ReplicaSource source = new ObjectStoreReplicaSource(store, layout);
        var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReplicaSyncer.class);
        ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        logger.addAppender(log);
        try (LazyGenerations lazy = new LazyGenerations(cacheDir, rc, source);
             ReplicaSyncer syncer = new ReplicaSyncer(new ObjectStoreCatalog(store, layout), source,
                     new ReaderLocalCache(cacheDir), ProjectRegistry.at(tmp.resolve("edges-registry.json")),
                     cacheDir, null, Duration.ofSeconds(10), lazy)) {
            syncer.syncOnce();
            assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(gen)));
            Path lucene = cacheDir.resolve(JAVA).resolve("gen-" + gen).resolve("lucene");
            for (ManifestFile f : manifest(gen).files()) {
                if (ManifestFile.LUCENE.equals(f.role()) && !alwaysEager(f.name())) {
                    assertThat(RemoteDirectory.headFile(lucene, f.name())).as(f.name())
                            .hasSize(Math.min(f.length(), RemoteDirectory.HEAD_BYTES));
                }
            }
            BlockCache blocks = lazy.blockCache();
            assertThat(blocks.stats().bytesFetched()).as("the warm open fetched nothing").isZero();
            try (Directory dir = lazy.open(lucene); DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.maxDoc()).isPositive();
            }
            assertThat(blocks.stats().bytesFetched()).isZero();
        } finally {
            logger.detachAppender(log);
        }
        assertThat(log.list.stream().map(ILoggingEvent::getFormattedMessage))
                .anySatisfy(m -> assertThat(m).contains("opened generation " + gen, "before activating"));
    }

    /** M8: the per-query existence check of a lazy generation reads its manifest, not the block cache. */
    @Test
    void indexExistsOnALazyGenerationBuildsNoRemoteDirectory() throws Exception {
        long gen = publishWithoutCompoundFiles();
        Node node = reader("exists", LazyReaderTest::small);
        node.runtime().startServing();
        Path lucene = node.genDir(gen).resolve("lucene");
        try (LazyGenerations fresh = new LazyGenerations(tmp.resolve("other-cache"),
                node.config().getCluster().getReader(), new ObjectStoreReplicaSource(store, layout))) {
            assertThat(fresh.indexExists(lucene)).isTrue();
            assertThat(fresh.blockCacheDir()).as("no remote directory, so no block cache").isNull();
            assertThat(fresh.indexExists(tmp.resolve("nowhere").resolve("lucene"))).isFalse();
        }
    }

    /** A synthetic generation whose files are random objects of the given sizes, published to the store. */
    private ShardManifest syntheticGeneration(long gen, Map<String, Integer> sizes) {
        List<ManifestFile> files = new ArrayList<>();
        java.util.Random rnd = new java.util.Random(gen);
        for (var e : sizes.entrySet()) {
            byte[] data = new byte[e.getValue()];
            rnd.nextBytes(data);
            String key = "pharos/shards/" + JAVA + "/objects/gen" + gen + "-" + e.getKey();
            store.put(key, data);
            files.add(new ManifestFile(ManifestFile.LUCENE, e.getKey(), key, data.length, "0"));
        }
        return new ShardManifest(ShardManifest.FORMAT_VERSION, JAVA, gen, null, java.time.Instant.now(), "writer", 0,
                0, null, files, null, null);
    }

    /** M4: a generation's prefetch takes at most half the cache, most important extensions first. */
    @Test
    void prefetchIsCappedAtHalfTheCache() throws Exception {
        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        rc.setCacheMaxBytes(4 * 64 * KIB); // half: 128 KiB
        ShardManifest m = syntheticGeneration(5, new java.util.TreeMap<>(Map.of(
                "_0.tim", 100 * 1024, "_0.tip", 60 * 1024, "_0.tmd", 50 * 1024, "_0.doc", 90 * 1024)));
        Path lucene = Files.createDirectories(tmp.resolve("capped").resolve("lucene"));
        try (LazyGenerations lazy = new LazyGenerations(tmp.resolve("capped-cache"), rc,
                new ObjectStoreReplicaSource(store, layout))) {
            LazyGenerations.Prefetch p = lazy.prefetch(m, lucene);
            p.done().get(10, TimeUnit.SECONDS);
            assertThat(p.bytes()).isLessThanOrEqualTo(128 * KIB);
            BlockCache blocks = lazy.blockCache();
            ManifestFile tip = m.files().stream().filter(f -> f.name().equals("_0.tip")).findFirst().orElseThrow();
            ManifestFile tim = m.files().stream().filter(f -> f.name().equals("_0.tim")).findFirst().orElseThrow();
            // default order tip, tmd, tim, vex, vem: tip (60 KiB) and tmd (50 KiB) fit, tim does not
            assertThat(blocks.isCached(new BlockKey(tip.key(), 0))).isTrue();
            assertThat(blocks.isCached(new BlockKey(tim.key(), 0))).isFalse();
            assertThat(p.bytes()).isEqualTo(110 * KIB);
        }
    }

    /** M4: a new generation's prefetch cancels what the previous one still has queued. */
    @Test
    void newPrefetchCancelsThePreviousGenerationsQueuedBlocks() throws Exception {
        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        rc.setMaxConcurrentFetches(2); // a prefetch pool of 2 threads
        CountDownLatch release = new CountDownLatch(1);
        ReplicaSource slow = new SlowRanges(new ObjectStoreReplicaSource(store, layout), release);
        Map<String, Integer> sizes = new java.util.TreeMap<>(Map.of("_0.tim", 600 * 1024, "_0.tip", 100 * 1024));
        ShardManifest first = syntheticGeneration(6, sizes);
        ShardManifest second = syntheticGeneration(7, sizes);
        try (LazyGenerations lazy = new LazyGenerations(tmp.resolve("cancel-cache"), rc, slow)) {
            LazyGenerations.Prefetch a = lazy.prefetch(first, Files.createDirectories(tmp.resolve("a").resolve("lucene")));
            LazyGenerations.Prefetch b = lazy.prefetch(second, Files.createDirectories(tmp.resolve("b").resolve("lucene")));
            assertThat(b.cancelledPrevious()).as("generation 6's queued blocks").isPositive();
            release.countDown();
            b.done().get(20, TimeUnit.SECONDS);
            assertThatThrownByCancelled(a);
        } finally {
            release.countDown();
        }
    }

    private static void assertThatThrownByCancelled(LazyGenerations.Prefetch p) {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> p.done().get(20, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(java.util.concurrent.CancellationException.class);
    }

    @Test
    void singleNodeNeverBuildsRemoteClasses() throws Exception {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("single").resolve("indexes"));
        assertThat(config.getCluster().getRole()).isEqualTo(NodeRole.ALL);
        ReplicationBackend backend = Backends.create(config);
        try (LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
             ClusterRuntime runtime = new ClusterRuntime(config, backend,
                     ProjectRegistry.at(tmp.resolve("single").resolve("registry.json")), lucene)) {
            runtime.startServing(); // a no-op off readers
            assertThat(lucene.directoryFactory()).isSameAs(LuceneDirectoryFactory.DEFAULT);
            assertThat(runtime.lazyGenerations()).isNull();
        }

        Node full = reader("full", r -> r.setMode(ClusterConfig.ReaderConfig.FULL));
        assertThat(full.lucene().directoryFactory()).isSameAs(LuceneDirectoryFactory.DEFAULT);
        assertThat(full.runtime().lazyGenerations()).isNull();
    }

    /** One block cache dir per process: a second one on the cache dir gets a private, temporary one. */
    @Test
    void secondProcessGetsAPrivateBlockCache() throws Exception {
        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        Path cacheDir = tmp.resolve("shared-cache");
        ReplicaSource source = new ObjectStoreReplicaSource(store, layout);
        try (LazyGenerations first = new LazyGenerations(cacheDir, rc, source)) {
            first.blockCache();
            assertThat(first.blockCacheDir()).isEqualTo(cacheDir.resolve(LazyGenerations.BLOCKS_DIR));
            Path privateDir;
            try (LazyGenerations second = new LazyGenerations(cacheDir, rc, source)) {
                second.blockCache();
                privateDir = second.blockCacheDir();
                assertThat(privateDir.getParent()).isEqualTo(cacheDir.resolve(LazyGenerations.PRIVATE_BLOCKS_DIR));
                assertThat(privateDir).isDirectory();
                assertThat(second.blockCache().maxBytes()).as("a private cache is smaller")
                        .isEqualTo(rc.effectivePrivateCacheMaxBytes()).isLessThan(first.blockCache().maxBytes());
            }
            assertThat(privateDir).doesNotExist();
        }
    }

    /** A process that began on a private cache moves new opens to the shared one once the lock is free. */
    @Test
    void privateCacheProcessSwitchesToTheSharedCacheWhenItCanTakeIt() throws Exception {
        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        Path cacheDir = tmp.resolve("switch-cache");
        ReplicaSource source = new ObjectStoreReplicaSource(store, layout);
        var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LazyGenerations.class);
        ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        logger.addAppender(log);
        LazyGenerations first = new LazyGenerations(cacheDir, rc, source);
        Path privateDir;
        try (LazyGenerations second = new LazyGenerations(cacheDir, rc, source)) {
            first.blockCache();
            BlockCache privateCache = second.blockCache();
            privateDir = second.blockCacheDir();
            assertThat(privateDir.getParent()).isEqualTo(cacheDir.toAbsolutePath().resolve(LazyGenerations.PRIVATE_BLOCKS_DIR));
            second.sweep();
            assertThat(second.blockCacheDir()).as("the lock is still held").isEqualTo(privateDir);
            privateCache.acquire(new BlockKey("some/object", 0), 1000, (k, o, l, t) -> Files.write(t, new byte[(int) l]))
                    .close();
            long privateBytes = privateCache.stats().bytesOnDisk();
            assertThat(privateBytes).isPositive();

            first.close();
            second.sweep();
            assertThat(second.blockCacheDir()).isEqualTo(cacheDir.toAbsolutePath().resolve(LazyGenerations.BLOCKS_DIR));
            assertThat(second.blockCache().maxBytes()).isEqualTo(rc.getCacheMaxBytes());
            assertThat(second.blockCache()).isNotSameAs(privateCache);
            assertThat(second.stats().orElseThrow().bytesOnDisk()).as("retired private cache still counted")
                    .isEqualTo(privateBytes);
            assertThat(privateDir).as("retired readers may still use it").isDirectory();
            assertThat(log.list.stream().map(ILoggingEvent::getFormattedMessage))
                    .anySatisfy(m -> assertThat(m).contains("shared block cache", "private"));
        } finally {
            first.close();
            logger.detachAppender(log);
        }
        assertThat(privateDir).as("deleted on close").doesNotExist();
    }

    /** The runtime's operator stats count this process's cache: non-zero hits after a query. */
    @Test
    void readerCacheStatsCountHitsAfterAQuery() throws Exception {
        publishWithoutCompoundFiles();
        Node lazy = reader("stats", r -> { small(r); r.setEagerMaxBytes(0); });
        assertThat(lazy.runtime().readerCacheStats()).as("before serving: zeros, not absent")
                .containsEntry("hits", 0L).containsEntry("mode", "lazy");
        lazy.runtime().startServing();
        assertThat(ids(lazy.engine(), "Calculator")).isNotEmpty();
        assertThat(ids(lazy.engine(), "greeting service")).isNotNull();

        Map<String, Object> rc = lazy.runtime().readerCacheStats();
        assertThat((Long) rc.get("hits")).isPositive();
        assertThat((Long) rc.get("bytesOnDisk")).isPositive();
        assertThat((Long) rc.get("bytesFetched")).isPositive();
        assertThat(rc).containsEntry("mode", "lazy").containsEntry("cacheMaxBytes", 64 * KIB * KIB)
                .containsEntry("shared", true).containsKeys("misses", "hitRate", "evictions", "leasesOpen");
        assertThat((Double) rc.get("hitRate")).isBetween(0.0, 1.0);
    }

    /** A file the next generation shares by object key keeps its head sidecar: it is not fetched again. */
    @Test
    void unchangedFileHeadIsNotRefetchedOnTheNextGeneration() throws Exception {
        long g1 = publishWithoutCompoundFiles();
        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        rc.setEagerMaxBytes(0);
        Path cacheDir = tmp.resolve("heads-cache");
        ReplicaSource real = new ObjectStoreReplicaSource(store, layout);
        java.util.Map<String, Integer> headReads = new java.util.concurrent.ConcurrentHashMap<>();
        ReplicaSource counting = new ReplicaSource() {
            @Override public Optional<Generation> latest(ShardId s) throws IOException { return real.latest(s); }
            @Override public ShardManifest manifest(ShardId s, Generation g) throws IOException {
                return real.manifest(s, g);
            }
            @Override public void download(ManifestFile f, Path t) throws IOException { real.download(f, t); }
            @Override public void downloadRange(ManifestFile f, long o, long l, Path t) throws IOException {
                real.downloadRange(f, o, l, t);
            }
            @Override public InputStream openRange(ManifestFile f, long o, long l) throws IOException {
                if (o == 0 && l == RemoteDirectory.HEAD_BYTES) headReads.merge(f.key(), 1, Integer::sum);
                return real.openRange(f, o, l);
            }
            @Override public Optional<byte[]> synonyms() throws IOException { return real.synonyms(); }
        };
        ReaderLocalCache cache = new ReaderLocalCache(cacheDir);
        try (LazyGenerations lazy = new LazyGenerations(cacheDir, rc, counting);
             ReplicaSyncer syncer = new ReplicaSyncer(new ObjectStoreCatalog(store, layout), counting, cache,
                     ProjectRegistry.at(tmp.resolve("heads-registry.json")), cacheDir, null,
                     Duration.ofSeconds(10), lazy)) {
            syncer.syncOnce();
            assertThat(cache.active(JAVA)).contains(cacheDir.toAbsolutePath().resolve(JAVA).resolve("gen-" + g1));
            // the same files again, as a new generation: every object key is shared
            ProjectMeta meta = cluster.writerRegistry.find(JAVA).orElseThrow();
            long published = cluster.writerRuntime.publisher().publish(
                    new LocalSnapshot(JAVA, cluster.writerLucene.getProjectIndexDir(JAVA), meta, "none"))
                    .orElseThrow().value();
            syncer.syncOnce();
            long g2 = syncer.served().get(SHARD).value();
            assertThat(g2).isEqualTo(published);
            assertThat(g2).isGreaterThan(g1);

            java.util.Set<String> keys1 = new java.util.HashSet<>();
            for (ManifestFile f : manifest(g1).files()) {
                if (ManifestFile.LUCENE.equals(f.role()) && f.length() > RemoteDirectory.HEAD_BYTES) keys1.add(f.key());
            }
            List<ManifestFile> shared = manifest(g2).files().stream()
                    .filter(f -> ManifestFile.LUCENE.equals(f.role()) && keys1.contains(f.key())
                            && !alwaysEager(f.name())).toList();
            assertThat(shared).as("files both generations share").isNotEmpty();
            for (ManifestFile f : shared) {
                assertThat(headReads.get(f.key())).as("head reads of %s", f.name()).isEqualTo(1);
                assertThat(RemoteDirectory.headFile(cacheDir.resolve(JAVA).resolve("gen-" + g2).resolve("lucene"),
                        f.name())).as(f.name()).isRegularFile().hasSize(RemoteDirectory.HEAD_BYTES);
            }
        }
    }

    /** I2: the syncing reader owns the shared cache from the start and sweeps private caches every pass. */
    @Test
    void syncingReaderOwnsTheSharedCacheAndSweepsStalePrivateOnes() throws Exception {
        cluster.index(JAVA, false); // every file of sample-app is eager: nothing would create the cache
        Node node = reader("owner", r -> { });
        node.runtime().startServing();
        Path cacheDir = node.config().getCluster().getCacheDir().toAbsolutePath();
        assertThat(node.runtime().lazyGenerations().blockCacheDir())
                .isEqualTo(cacheDir.resolve(LazyGenerations.BLOCKS_DIR));

        Process gone = new ProcessBuilder("true").start();
        gone.waitFor();
        Path stale = cacheDir.resolve(LazyGenerations.PRIVATE_BLOCKS_DIR).resolve(gone.pid() + "-0123abcd");
        Path alive = cacheDir.resolve(LazyGenerations.PRIVATE_BLOCKS_DIR)
                .resolve(ProcessHandle.current().pid() + "-4567cdef");
        Files.createDirectories(stale.resolve("ab"));
        try (SyncLock owner = SyncLock.tryAcquire(alive, LazyGenerations.PRIVATE_OWNER_LOCK).orElseThrow()) {
            node.runtime().syncNow();
            assertThat(stale).doesNotExist();
            assertThat(alive).isDirectory();
        }
    }

    /**
     * Final review M3: a private cache is stale when nobody holds the lock inside it, whatever its
     * PID says (PIDs of another PID namespace look dead here; a reused PID looks alive).
     */
    @Test
    void stalePrivateCachesAreFoundByTheirLockNotTheirPid() throws Exception {
        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        Path cacheDir = tmp.resolve("locks-cache").toAbsolutePath();
        Path privates = cacheDir.resolve(LazyGenerations.PRIVATE_BLOCKS_DIR);
        ReplicaSource source = new ObjectStoreReplicaSource(store, layout);
        Process gone = new ProcessBuilder("true").start();
        gone.waitFor();
        try (LazyGenerations owner = new LazyGenerations(cacheDir, rc, source);
             LazyGenerations second = new LazyGenerations(cacheDir, rc, source)) {
            owner.blockCache();
            second.blockCache(); // a real private cache of a live holder
            Path secondDir = second.blockCacheDir();
            assertThat(secondDir.resolve(LazyGenerations.PRIVATE_OWNER_LOCK)).isRegularFile();
            // a holder in another PID namespace: its PID is dead here, but it holds its lock
            Path otherNamespace = privates.resolve(gone.pid() + "-11111111");
            // a crashed holder whose PID was reused by a live process: nobody holds its lock
            Path reusedPid = privates.resolve(ProcessHandle.current().pid() + "-22222222");
            SyncLock released = SyncLock.tryAcquire(reusedPid, LazyGenerations.PRIVATE_OWNER_LOCK).orElseThrow();
            released.close();
            Files.createDirectories(reusedPid.resolve("ab"));
            try (SyncLock held = SyncLock.tryAcquire(otherNamespace, LazyGenerations.PRIVATE_OWNER_LOCK)
                    .orElseThrow()) {
                owner.sweep();
                assertThat(reusedPid).as("released lock: stale").doesNotExist();
                assertThat(otherNamespace).as("held lock: alive").isDirectory();
                assertThat(secondDir).as("held by a live LazyGenerations").isDirectory();
            }
            owner.sweep();
            assertThat(otherNamespace).as("released now").doesNotExist();
            assertThat(secondDir).isDirectory();
        }
    }

    /** Switching a reader from lazy to full re-materialises the generation instead of serving a stub. */
    @Test
    void fullModeRematerialisesALazyGeneration() throws Exception {
        long gen = publishWithoutCompoundFiles();
        Node lazy = reader("switch", LazyReaderTest::small);
        lazy.runtime().startServing();
        assertThat(lazy.genDir(gen).resolve(LazyGenerations.MARKER)).exists();
        List<String> want = ids(lazy.engine(), "Calculator");
        lazy.close();

        Node full = reader("switch", r -> r.setMode(ClusterConfig.ReaderConfig.FULL));
        assertThat(((ReaderLocalCache) full.backend().localCache()).active(JAVA))
                .as("a lazy dir is not activated in full mode").isEmpty();
        full.runtime().startServing();
        assertThat(full.genDir(gen).resolve(LazyGenerations.MARKER)).doesNotExist();
        for (ManifestFile f : manifest(gen).files()) {
            if (ManifestFile.LUCENE.equals(f.role())) {
                assertThat(full.genDir(gen).resolve("lucene").resolve(f.name())).hasSize(f.length());
            }
        }
        assertThat(ids(full.engine(), "Calculator")).isEqualTo(want).isNotEmpty();
    }

    /** The remote (block-read) Lucene files of a generation with at least {@code min} bytes. */
    private List<ManifestFile> remoteFiles(long gen, long min) throws IOException {
        return manifest(gen).files().stream()
                .filter(f -> ManifestFile.LUCENE.equals(f.role()) && !alwaysEager(f.name()) && f.length() >= min)
                .toList();
    }

    private static int status(io.javalin.Javalin app) throws Exception {
        return java.net.http.HttpClient.newHttpClient().send(java.net.http.HttpRequest.newBuilder(
                        java.net.URI.create("http://127.0.0.1:" + app.port() + "/health")).build(),
                java.net.http.HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    /**
     * Final review I1(b): GC removed objects of the generation a lagging lazy reader still serves.
     * The miss logs one ERROR per generation and /health answers 503 degraded (routers drain the
     * node) until a newer generation is activated.
     */
    @Test
    void missingObjectOfTheServedGenerationDegradesHealthUntilANewerOneActivates() throws Exception {
        long g1 = publishWithoutCompoundFiles();
        Node node = reader("gone", r -> { small(r); r.setEagerMaxBytes(0); r.setPrefetchExtensions(List.of()); });
        var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LazyGenerations.class);
        ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        logger.addAppender(log);
        com.pharos.web.WebServer web = new com.pharos.web.WebServer(null, null, null, null, null);
        web.setDegradation(node.runtime()::degradation);
        io.javalin.Javalin app = web.start(0);
        try {
            node.runtime().startServing();
            assertThat(node.runtime().degradation()).isNull();
            assertThat(status(app)).isEqualTo(200);

            // The bucket's GC deletes generation g1's objects while this reader still serves it.
            Map<String, byte[]> removed = new java.util.HashMap<>();
            for (ManifestFile f : remoteFiles(g1, 1)) {
                removed.put(f.key(), store.get(f.key()).orElseThrow().data());
                store.delete(f.key());
            }
            ManifestFile big = remoteFiles(g1, 0).stream().max(Comparator.comparingLong(ManifestFile::length))
                    .orElseThrow();
            for (int i = 0; i < 2; i++) {
                try (Directory d = node.runtime().lazyGenerations().open(node.genDir(g1).resolve("lucene"));
                     IndexInput in = d.openInput(big.name(), IOContext.DEFAULT)) {
                    byte[] all = new byte[(int) big.length()];
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> in.readBytes(all, 0, all.length))
                            .isInstanceOf(IOException.class).hasMessageContaining(big.key());
                }
            }
            assertThat(node.runtime().degradation()).contains(JAVA, "generation " + g1);
            assertThat(status(app)).isEqualTo(503);
            assertThat(log.list.stream().filter(e -> e.getLevel() == Level.ERROR)
                    .map(ILoggingEvent::getFormattedMessage).toList())
                    .singleElement().satisfies(m -> assertThat(m).contains("generation " + g1, JAVA,
                            "garbage-collected", "behind retention", "mode full"));

            removed.forEach(store::put); // the writer publishes again: the objects are back
            addMethodAndReindex("triple");
            node.runtime().syncNow();
            assertThat(node.runtime().served().get(SHARD).value()).isGreaterThan(g1);
            assertThat(node.runtime().degradation()).isNull();
            assertThat(status(app)).isEqualTo(200);
        } finally {
            app.stop();
            logger.detachAppender(log);
        }
    }

    /**
     * Final review M9: switching a reader from lazy to full leaves {@code <cacheDir>/.blocks}; the
     * full-mode syncer deletes it on start, unless a process still holds the block cache's lock.
     */
    @Test
    void fullModeSyncerDeletesTheUnusedLazyBlockCache() throws Exception {
        publishWithoutCompoundFiles();
        Node lazy = reader("to-full", r -> { small(r); r.setEagerMaxBytes(0); });
        lazy.runtime().startServing();
        assertThat(ids(lazy.engine(), "Calculator")).isNotEmpty();
        Path cacheDir = lazy.config().getCluster().getCacheDir().toAbsolutePath();
        Path blocks = cacheDir.resolve(LazyGenerations.BLOCKS_DIR);
        assertThat(blocks).isDirectory();
        lazy.close();

        ClusterConfig.ReaderConfig rc = new ClusterConfig.ReaderConfig();
        small(rc);
        try (LazyGenerations holder = new LazyGenerations(cacheDir, rc, new ObjectStoreReplicaSource(store, layout))) {
            holder.blockCache(); // another process still serves lazily from the shared block cache
            Node busy = reader("to-full", r -> r.setMode(ClusterConfig.ReaderConfig.FULL));
            busy.runtime().startServing();
            assertThat(blocks).as("still locked by another process").isDirectory();
            busy.close();
        }
        Node full = reader("to-full", r -> r.setMode(ClusterConfig.ReaderConfig.FULL));
        full.runtime().startServing();
        assertThat(blocks).as("nobody holds its lock: deleted").doesNotExist();
        assertThat(ids(full.engine(), "Calculator")).isNotEmpty();
    }

    /** Ranged reads (prefetch and block fetches) wait for {@code release}; whole-file downloads do not. */
    private static final class SlowRanges implements ReplicaSource {
        private final ReplicaSource d;
        private final CountDownLatch release;

        SlowRanges(ReplicaSource d, CountDownLatch release) {
            this.d = d;
            this.release = release;
        }

        private void await() throws IOException {
            try {
                if (!release.await(30, TimeUnit.SECONDS)) throw new IOException("never released");
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        @Override public Optional<Generation> latest(ShardId shard) throws IOException { return d.latest(shard); }
        @Override public ShardManifest manifest(ShardId shard, Generation gen) throws IOException {
            return d.manifest(shard, gen);
        }
        @Override public void download(ManifestFile file, Path target) throws IOException { d.download(file, target); }
        @Override public void downloadRange(ManifestFile file, long offset, long length, Path target)
                throws IOException {
            await();
            d.downloadRange(file, offset, length, target);
        }
        @Override public InputStream openRange(ManifestFile file, long offset, long length) throws IOException {
            await();
            return d.openRange(file, offset, length);
        }
        @Override public Optional<byte[]> synonyms() throws IOException { return d.synonyms(); }
    }
}
