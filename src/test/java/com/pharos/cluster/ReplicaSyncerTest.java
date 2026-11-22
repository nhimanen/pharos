package com.pharos.cluster;

import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.warm.LazyGenerations;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.GraphViews;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Full mode (Phase 1); {@link LazyModeReplicaSyncerTest} runs every case again in lazy mode. */
class ReplicaSyncerTest {

    private static final String PROJECT = "sample-app";
    private static final ShardId SHARD = new ShardId(PROJECT);

    @TempDir Path tmp;

    private Path source;
    private InMemoryObjectStore store;
    private ObjectStoreLayout layout;
    private ObjectStoreCatalog catalog;
    private ProjectIndexManager writer;
    private ObjectStorePublisher publisher;
    private Path writerSynonyms;

    private Path cacheDir;
    private Path readerSynonyms;
    private ReaderLocalCache cache;
    private ProjectRegistry readerRegistry;
    private ReplicaSyncer syncer;
    /** Lazy mode's directory factory and block cache; null in full mode. */
    LazyGenerations lazy;

    /** The lazy-mode hook for the syncers of this test; null runs full mode. */
    LazyGenerations lazyGenerations(Path cacheDir, ReplicaSource source) {
        return null;
    }

    @BeforeEach
    void setUp() throws IOException {
        source = tmp.resolve("src-copy");
        Path from = Path.of("src/test/resources/test-projects/sample-app");
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path dest = source.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(dest);
                else Files.copy(p, dest);
            }
        }
        store = new InMemoryObjectStore();
        layout = new ObjectStoreLayout("pharos");
        catalog = new ObjectStoreCatalog(store, layout);
        writerSynonyms = tmp.resolve("writer-synonyms.txt");
        Files.writeString(writerSynonyms, "auth, authentication\n");
        ClusterConfig cfg = new ClusterConfig();
        GenerationGc gc = new GenerationGc(store, layout, cfg.getRetentionGenerations(),
                Duration.ofMinutes(cfg.getRetentionMinutes()));
        publisher = new ObjectStorePublisher(store, layout, catalog, gc, "writer", writerSynonyms);
        writer = writerManager(publisher);

        cacheDir = tmp.resolve("reader-cache");
        readerSynonyms = tmp.resolve("reader-home").resolve("synonyms.txt");
        cache = new ReaderLocalCache(cacheDir);
        readerRegistry = ProjectRegistry.at(tmp.resolve("reader-registry.json"));
        lazy = lazyGenerations(cacheDir, new ObjectStoreReplicaSource(store, layout));
        syncer = new ReplicaSyncer(catalog, new ObjectStoreReplicaSource(store, layout), cache,
                readerRegistry, cacheDir, readerSynonyms, Duration.ofSeconds(10), lazy);
    }

    @AfterEach
    void tearDown() {
        syncer.close();
        if (lazy != null) lazy.close();
    }

    private ProjectIndexManager writerManager(IndexPublisher pub) {
        return writerManager(pub, "writer");
    }

    /** A writer node's index manager: its own index dir and registry under {@code name}. */
    private ProjectIndexManager writerManager(IndexPublisher pub, String name) {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve(name + "-indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve(name + "-registry.json"));
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(
                    Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        return new ProjectIndexManager(config, new LuceneIndexer(config), registry,
                List.of(EmbeddingProvider.create(config)), noopModules,
                List.of(new JavaCodeParser()), pub);
    }

    private void publishFull() throws IOException {
        writer.index(source, PROJECT, false, false, false, false, ProgressListener.SILENT);
    }

    private void publishIncremental(String methodName) throws IOException {
        Path file;
        try (Stream<Path> s = Files.walk(source)) {
            file = s.filter(p -> p.toString().endsWith("Calculator.java")).findFirst().orElseThrow();
        }
        Files.writeString(file, Files.readString(file).replace("class Calculator {",
                "class Calculator {\n    public int " + methodName + "(int x) { return 3 * x; }\n"));
        Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        writer.index(source, PROJECT, true, false, false, false, ProgressListener.SILENT);
    }

    private Path genDir(long gen) {
        return cacheDir.resolve(PROJECT).resolve("gen-" + gen);
    }

    private static int numDocs(Path luceneDir) throws IOException {
        try (FSDirectory dir = FSDirectory.open(luceneDir); DirectoryReader r = DirectoryReader.open(dir)) {
            return r.numDocs();
        }
    }

    private ShardManifest manifest(long gen) throws IOException {
        return ShardManifest.read(store.get(layout.manifestKey(SHARD, new Generation(gen))).orElseThrow().data());
    }

    private List<String> genDirNames() throws IOException {
        try (Stream<Path> s = Files.list(cacheDir.resolve(PROJECT))) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    private static Object fileKey(Path p) throws IOException {
        return Files.readAttributes(p, BasicFileAttributes.class).fileKey();
    }

    @Test
    void firstSyncMaterialisesGeneration() throws Exception {
        publishFull();
        int writerDocs = numDocs(tmp.resolve("writer-indexes").resolve(PROJECT).resolve("lucene"));

        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(1)));
        assertThat(numDocs(genDir(1).resolve("lucene"))).isEqualTo(writerDocs).isPositive();
        assertThat(genDir(1).resolve(GraphViews.COMPACT_FILE)).isRegularFile();
        assertThat(genDir(1).resolve("manifest.json")).isRegularFile();
        assertThat(Files.exists(genDir(1).resolve(LazyGenerations.MARKER))).as("lazy marker").isEqualTo(lazy != null);
        assertThat(cache.active(PROJECT)).contains(genDir(1));
        assertThat(cache.shardDir(PROJECT)).isEqualTo(genDir(1));
        ProjectMeta meta = readerRegistry.find(PROJECT).orElseThrow();
        assertThat(meta.getIndexPath()).isEqualTo(genDir(1).toString());
        // the reader's LuceneIndexer, pointed at the cache, opens the generation
        IndexConfig readerConfig = IndexConfig.defaults();
        try (LuceneIndexer indexer = new LuceneIndexer(readerConfig, cache)) {
            assertThat(indexer.openReader(PROJECT).numDocs()).isEqualTo(writerDocs);
        }
        // nothing to do when LATEST has not moved
        syncer.syncOnce();
        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(1)));
    }

    @Test
    void beforeFirstActivationShardDirIsNone() {
        assertThat(cache.shardDir(PROJECT)).isEqualTo(cacheDir.resolve(PROJECT).resolve("none"));
        assertThat(cache.active(PROJECT)).isEmpty();
    }

    @Test
    void secondGenerationSwapsAndReusesFiles() throws Exception {
        publishFull();
        syncer.syncOnce();
        publishIncremental("triple");
        ShardManifest m1 = manifest(1);
        ShardManifest m2 = manifest(2);

        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(2)));
        assertThat(cache.active(PROJECT)).contains(genDir(2));
        assertThat(genDir(1)).isDirectory(); // previous generation kept
        assertThat(readerRegistry.find(PROJECT).orElseThrow().getIndexPath()).isEqualTo(genDir(2).toString());
        for (ManifestFile f : m2.files()) {
            if (!f.role().equals(ManifestFile.LUCENE)) continue;
            Path now = genDir(2).resolve("lucene").resolve(f.name());
            assertThat(Files.size(now)).isEqualTo(f.length());
            for (ManifestFile p : m1.files()) {
                if (p.key().equals(f.key())) {
                    assertThat(fileKey(now)).as(f.name())
                            .isEqualTo(fileKey(genDir(1).resolve("lucene").resolve(p.name())));
                }
            }
        }
        assertThat(numDocs(genDir(2).resolve("lucene"))).isEqualTo((int) m2.docCount());
    }

    /** Republishing the same commit reuses every Lucene key, so linking is always exercised. */
    @Test
    void sameCommitRepublishedIsFullyHardLinked() throws Exception {
        publishFull();
        syncer.syncOnce();
        ProjectMeta meta = ProjectRegistry.at(tmp.resolve("writer-registry.json")).find(PROJECT).orElseThrow();
        assertThat(publisher.publish(new LocalSnapshot(SHARD.value(),
                tmp.resolve("writer-indexes").resolve(PROJECT), meta, "none"))).contains(new Generation(2));
        ShardManifest m1 = manifest(1);
        ShardManifest m2 = manifest(2);

        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(2)));
        int linked = 0;
        for (ManifestFile f : m2.files()) {
            if (!f.role().equals(ManifestFile.LUCENE)) continue;
            ManifestFile prev = m1.files().stream().filter(p -> p.key().equals(f.key())).findFirst().orElseThrow();
            assertThat(fileKey(genDir(2).resolve("lucene").resolve(f.name())))
                    .isEqualTo(fileKey(genDir(1).resolve("lucene").resolve(prev.name())));
            linked++;
        }
        assertThat(linked).isPositive();
        assertThat(genDir(1)).isDirectory();
    }

    @Test
    void onlyActiveAndPreviousKept() throws Exception {
        publishFull();
        syncer.syncOnce();
        publishIncremental("triple");
        syncer.syncOnce();
        publishIncremental("quadruple");
        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(3)));
        assertThat(genDirNames()).containsExactly("gen-2", "gen-3");
    }

    @Test
    void failedDownloadKeepsServingPrevious() throws Exception {
        publishFull();
        syncer.syncOnce();
        publishIncremental("triple");
        ShardManifest m1 = manifest(1);
        boolean gen2HasNewLucene = manifest(2).files().stream()
                .filter(f -> f.role().equals(ManifestFile.LUCENE))
                .anyMatch(f -> m1.files().stream().noneMatch(p -> p.key().equals(f.key())));
        assertThat(gen2HasNewLucene).isTrue();

        store.failOn("lucene/");
        syncer.syncOnce(); // logged, not thrown

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(1)));
        assertThat(cache.active(PROJECT)).contains(genDir(1));
        assertThat(genDir(2)).doesNotExist();
        assertThat(cacheDir.resolve(PROJECT).resolve("gen-2.partial")).isDirectory(); // left for the next sync
        assertThat(readerRegistry.find(PROJECT).orElseThrow().getIndexPath()).isEqualTo(genDir(1).toString());

        store.clearFailures();
        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(2)));
        assertThat(cacheDir.resolve(PROJECT).resolve("gen-2.partial")).doesNotExist();
        assertThat(genDirNames()).containsExactly("gen-1", "gen-2");
    }

    @Test
    void corruptedDownloadIsRejected() throws Exception {
        publishFull();
        syncer.syncOnce();
        publishIncremental("triple");
        ShardManifest m1 = manifest(1);
        ManifestFile fresh = manifest(2).files().stream()
                .filter(f -> f.role().equals(ManifestFile.LUCENE))
                .filter(f -> m1.files().stream().noneMatch(p -> p.key().equals(f.key())))
                .findFirst().orElseThrow();
        store.put(fresh.key(), new byte[] {1, 2, 3});

        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(1)));
        assertThat(genDir(2)).doesNotExist();
    }

    /** Same length and an intact footer, a flipped byte in the body: only a full checksum sees it. */
    @Test
    void bodyCorruptedLuceneFileWithIntactFooterIsRejected() throws Exception {
        publishFull();
        ManifestFile victim = manifest(1).files().stream()
                .filter(f -> f.role().equals(ManifestFile.LUCENE))
                .filter(f -> !f.name().startsWith("segments") && !f.name().endsWith(".si"))
                .filter(f -> f.length() > 256)
                .findFirst().orElseThrow();
        byte[] bytes = store.get(victim.key()).orElseThrow().data();
        bytes[bytes.length / 2] ^= 0x5a; // body only: header and 16-byte footer untouched
        store.put(victim.key(), bytes);

        syncer.syncOnce();

        assertThat(syncer.served()).isEmpty();
        assertThat(genDir(1)).doesNotExist();
        assertThat(cache.active(PROJECT)).isEmpty();
    }

    @Test
    void latestGoingBackwardsIsServedAsNewLineage() throws Exception {
        publishFull();
        syncer.syncOnce();
        publishIncremental("triple");
        syncer.syncOnce();
        publishIncremental("quadruple");
        syncer.syncOnce();
        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(3)));

        // the bucket is wiped (catalog kept) and the writer publishes again from generation 1
        for (var o : store.list(layout.shardPrefix(SHARD))) store.delete(o.key());
        ProjectMeta meta = ProjectRegistry.at(tmp.resolve("writer-registry.json")).find(PROJECT).orElseThrow();
        assertThat(publisher.publish(new LocalSnapshot(PROJECT,
                tmp.resolve("writer-indexes").resolve(PROJECT), meta, "none"))).contains(Generation.FIRST);

        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, Generation.FIRST));
        assertThat(cache.active(PROJECT)).contains(genDir(1));
        assertThat(readerRegistry.find(PROJECT).orElseThrow().getIndexPath()).isEqualTo(genDir(1).toString());
        assertThat(genDirNames()).containsExactly("gen-1");
        assertThat(numDocs(genDir(1).resolve("lucene"))).isEqualTo((int) manifest(1).docCount());
    }

    @Test
    void synonymsFailureDoesNotStopTheSync() throws Exception {
        publishFull();
        ObjectStoreReplicaSource real = new ObjectStoreReplicaSource(store, layout);
        ReplicaSource failingSynonyms = new ReplicaSource() {
            @Override public java.util.Optional<Generation> latest(ShardId shard) throws IOException { return real.latest(shard); }
            @Override public ShardManifest manifest(ShardId shard, Generation gen) throws IOException { return real.manifest(shard, gen); }
            @Override public void download(ManifestFile file, Path target) throws IOException { real.download(file, target); }
            @Override public void downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException { real.downloadRange(file, offset, length, target); }
            @Override public java.io.InputStream openRange(ManifestFile file, long offset, long length) throws IOException { return real.openRange(file, offset, length); }
            @Override public java.util.Optional<byte[]> synonyms() throws IOException { throw new IOException("synonyms unreadable"); }
        };
        try (ReplicaSyncer s = new ReplicaSyncer(catalog, failingSynonyms, cache, readerRegistry, cacheDir,
                readerSynonyms, Duration.ofSeconds(10), lazy)) {
            s.syncOnce(); // logged, not thrown
            assertThat(s.served()).isEqualTo(Map.of(SHARD, Generation.FIRST));
        }
        assertThat(readerSynonyms).doesNotExist();
    }

    @Test
    void invalidCatalogIdDoesNotStopTheSync() throws Exception {
        publishFull();
        byte[] withBad = new String(store.get(layout.catalogKey()).orElseThrow().data(), StandardCharsets.UTF_8)
                .replace("\"shards\" : [", "\"shards\" : [ {\"id\": \"bad id\"}, ")
                .getBytes(StandardCharsets.UTF_8);
        assertThat(new String(withBad, StandardCharsets.UTF_8)).contains("bad id");
        store.put(layout.catalogKey(), withBad);

        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, Generation.FIRST));
    }

    @Test
    void corruptedGraphIsRejected() throws Exception {
        publishFull();
        ManifestFile graph = manifest(1).files().stream()
                .filter(f -> f.role().equals(ManifestFile.GRAPH)).findFirst().orElseThrow();
        byte[] bytes = store.get(graph.key()).orElseThrow().data();
        bytes[bytes.length / 2] ^= 0x5a; // same length, different CRC
        store.put(graph.key(), bytes);

        syncer.syncOnce();

        assertThat(syncer.served()).isEmpty();
        assertThat(genDir(1)).doesNotExist();
        assertThat(cache.active(PROJECT)).isEmpty();
    }

    @Test
    void unsafeFileNameInManifestIsRejected() throws Exception {
        publishFull();
        ShardManifest m = manifest(1);
        List<ManifestFile> files = new ArrayList<>(m.files());
        ManifestFile first = files.get(0);
        files.set(0, new ManifestFile(first.role(), "../../escape", first.key(), first.length(), first.checksum()));
        ShardManifest evil = new ShardManifest(m.formatVersion(), m.shard(), m.generation(), m.parentGeneration(),
                m.createdAt(), m.writerNode(), m.docCount(), m.sizeBytes(), m.modelFingerprint(), files,
                m.projectMeta(), m.indexFormat());
        store.put(layout.manifestKey(SHARD, Generation.FIRST), evil.toJson());

        syncer.syncOnce();

        assertThat(syncer.served()).isEmpty();
        assertThat(cacheDir.resolve("escape")).doesNotExist();
        assertThat(cacheDir.resolve(PROJECT).resolve("escape")).doesNotExist();
    }

    @Test
    void latestPointingAtMismatchedManifestIsRejected() throws Exception {
        publishFull();
        store.put(layout.latestKey(SHARD),
                "{\"generation\":1,\"manifest\":\"../gen-1.json\"}".getBytes(StandardCharsets.UTF_8));
        syncer.syncOnce();
        assertThat(syncer.served()).isEmpty();

        store.put(layout.latestKey(SHARD),
                "{\"generation\":1,\"manifest\":\"gen-2.json\"}".getBytes(StandardCharsets.UTF_8));
        syncer.syncOnce();
        assertThat(syncer.served()).isEmpty();

        store.put(layout.latestKey(SHARD),
                "{\"generation\":1,\"manifest\":\"gen-1.json\"}".getBytes(StandardCharsets.UTF_8));
        syncer.syncOnce();
        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, Generation.FIRST));
    }

    @Test
    void oneShardFailingDoesNotStopOthers() throws Exception {
        catalog.register(new ShardId("aaa-broken")); // sorted before sample-app; LATEST is garbage
        store.put(layout.latestKey(new ShardId("aaa-broken")), "not json".getBytes(StandardCharsets.UTF_8));
        publishFull();

        syncer.syncOnce();

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, Generation.FIRST));
    }

    @Test
    void restartAdoptsExistingGeneration() throws Exception {
        publishFull();
        syncer.syncOnce();
        Object before = fileKey(genDir(1).resolve("manifest.json"));

        ReaderLocalCache freshCache = new ReaderLocalCache(cacheDir);
        try (ReplicaSyncer restarted = new ReplicaSyncer(catalog, new ObjectStoreReplicaSource(store, layout),
                freshCache, readerRegistry, cacheDir, readerSynonyms, Duration.ofSeconds(10), lazy)) {
            restarted.syncOnce();
            assertThat(restarted.served()).isEqualTo(Map.of(SHARD, Generation.FIRST));
            assertThat(freshCache.active(PROJECT)).contains(genDir(1));
            assertThat(fileKey(genDir(1).resolve("manifest.json"))).isEqualTo(before);
        }
    }

    /**
     * Final review I1: two writers hold different shards and each mines only its own. Every publish
     * must keep the other shard's rules, and every reader must end up with the same bytes (the
     * coordinator and shards compare synonyms hashes).
     */
    @Test
    void synonymsOfTwoWritersAreMergedAndIdenticalOnEveryReader() throws Exception {
        String date = "2026-11-21";
        Files.writeString(writerSynonyms, "auth, authentication\n\n# ── Auto-mined from '" + PROJECT + "' on "
                + date + " (1 rules) ─\nledger                           => accountbook  # auto:" + PROJECT + ":"
                + date + "\n");
        // writer B: another shard, plus a stale rule of A's shard from when B held it
        Path bSynonyms = tmp.resolve("b-synonyms.txt");
        Files.writeString(bSynonyms, "auth, authentication\n\n# ── Auto-mined from 'other' on " + date
                + " (1 rules) ─\nwidget                           => gizmo  # auto:other:" + date + "\n\n"
                + "# ── Auto-mined from '" + PROJECT + "' on 2026-11-01 (1 rules) ─\n"
                + "stale                            => oldname  # auto:" + PROJECT + ":2026-11-01\n");
        ClusterConfig cfg = new ClusterConfig();
        ObjectStorePublisher publisherB = new ObjectStorePublisher(store, layout, catalog,
                new GenerationGc(store, layout, cfg.getRetentionGenerations(),
                        Duration.ofMinutes(cfg.getRetentionMinutes())), "writer-b", bSynonyms);
        ProjectIndexManager writerB = writerManager(publisherB, "writer-b");

        publishFull();                                                                   // A: sample-app
        writerB.index(source, "other", false, false, false, false, ProgressListener.SILENT); // B: other
        String afterB = new String(store.get(layout.synonymsKey()).orElseThrow().data(), StandardCharsets.UTF_8);
        publishIncremental("again");                                                     // A again
        String afterA = new String(store.get(layout.synonymsKey()).orElseThrow().data(), StandardCharsets.UTF_8);
        assertThat(afterA).as("the same rules whichever writer published last").isEqualTo(afterB);
        assertThat(afterA).contains("auth, authentication", "=> accountbook", "=> gizmo").doesNotContain("oldname");

        Path cache2 = tmp.resolve("reader2-cache");
        Path synonyms2 = tmp.resolve("reader2-home").resolve("synonyms.txt");
        try (ReplicaSyncer reader2 = new ReplicaSyncer(catalog, new ObjectStoreReplicaSource(store, layout),
                new ReaderLocalCache(cache2), ProjectRegistry.at(tmp.resolve("reader2-registry.json")), cache2,
                synonyms2, Duration.ofSeconds(10), null)) {
            syncer.syncOnce();
            reader2.syncOnce();
        }
        String hash1 = new com.pharos.search.KeywordSearchStrategy(readerSynonyms,
                new org.apache.lucene.search.SearcherFactory()).synonymsHash();
        String hash2 = new com.pharos.search.KeywordSearchStrategy(synonyms2,
                new org.apache.lucene.search.SearcherFactory()).synonymsHash();
        assertThat(hash1).isEqualTo(hash2).isNotEqualTo("none");
        assertThat(Files.readString(readerSynonyms)).contains("=> accountbook", "=> gizmo");
    }

    @Test
    void synonymsWrittenWhenChanged() throws Exception {
        publishFull();
        syncer.syncOnce();

        assertThat(readerSynonyms).isRegularFile();
        assertThat(Files.readAllBytes(readerSynonyms))
                .isEqualTo(store.get(layout.synonymsKey()).orElseThrow().data());
        FileTime mtime = FileTime.fromMillis(1_000_000L);
        Files.setLastModifiedTime(readerSynonyms, mtime);

        syncer.syncOnce(); // unchanged bytes: not rewritten
        assertThat(Files.getLastModifiedTime(readerSynonyms)).isEqualTo(mtime);

        store.put(layout.synonymsKey(), "db, database\n".getBytes(StandardCharsets.UTF_8));
        syncer.syncOnce();
        assertThat(Files.readString(readerSynonyms)).isEqualTo("db, database\n");
        try (Stream<Path> s = Files.list(readerSynonyms.getParent())) {
            assertThat(s.map(p -> p.getFileName().toString()).toList()).containsExactly("synonyms.txt");
        }
    }

    @Test
    void startPollsInBackground() throws Exception {
        publishFull();
        try (ReplicaSyncer polling = new ReplicaSyncer(catalog, new ObjectStoreReplicaSource(store, layout),
                new ReaderLocalCache(cacheDir), readerRegistry, cacheDir, readerSynonyms, Duration.ofMillis(50), lazy)) {
            polling.start();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (polling.served().isEmpty() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
                Thread.yield();
            }
            assertThat(polling.served()).isEqualTo(Map.of(SHARD, Generation.FIRST));
        }
    }

    // ── Adaptations 4 and 5: the format stamp ships in the manifest; readers gate on it ─────────

    /** Rewrites manifest {@code gen} in the store with {@code stamp} (null = a pre-stamp manifest). */
    private void restampManifest(long gen, com.pharos.indexer.IndexFormat.Stamp stamp) throws IOException {
        ShardManifest m = manifest(gen);
        ShardManifest re = new ShardManifest(m.formatVersion(), m.shard(), m.generation(), m.parentGeneration(),
                m.createdAt(), m.writerNode(), m.docCount(), m.sizeBytes(), m.modelFingerprint(), m.files(),
                m.projectMeta(), stamp);
        store.put(layout.manifestKey(SHARD, new Generation(gen)), re.toJson());
    }

    private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> captureLog() {
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ReplicaSyncer.class);
        var appender = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        return appender;
    }

    private static void releaseLog(ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> a) {
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ReplicaSyncer.class)).detachAppender(a);
    }

    @Test
    void manifestCarriesTheStampAndTheReaderWritesItIntoTheGenerationDir() throws Exception {
        publishFull();
        var writerStamp = com.pharos.indexer.IndexFormat.read(tmp.resolve("writer-indexes").resolve(PROJECT));
        assertThat(writerStamp.format()).isEqualTo(com.pharos.indexer.IndexVersions.INDEX_FORMAT);
        assertThat(manifest(1).indexFormat()).isEqualTo(writerStamp);

        syncer.syncOnce();

        assertThat(genDir(1).resolve("index-format.json")).isRegularFile();
        assertThat(com.pharos.indexer.IndexFormat.read(genDir(1))).isEqualTo(writerStamp);
        assertThat(com.pharos.indexer.IndexFormat.status(genDir(1))).isEqualTo(com.pharos.indexer.IndexFormat.Status.OK);

        // index-status on the reader reads the stamp of the served generation
        var buf = new java.io.ByteArrayOutputStream();
        var out = System.out;
        System.setOut(new java.io.PrintStream(buf, true, StandardCharsets.UTF_8));
        int rc;
        try {
            rc = new picocli.CommandLine(new com.pharos.cli.IndexStatusCommand(readerRegistry)).execute("--json");
        } finally {
            System.setOut(out);
        }
        assertThat(rc).isZero();
        var row = new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(buf.toString(StandardCharsets.UTF_8)).get("projects").get(0);
        assertThat(row.get("status").asText()).isEqualTo("ok");
        assertThat(row.get("format").asInt()).isEqualTo(com.pharos.indexer.IndexVersions.INDEX_FORMAT);
    }

    @Test
    void manifestWithoutStampCountsAsFormatZero() throws Exception {
        publishFull();
        restampManifest(1, null);

        syncer.syncOnce();

        // format 0 is N-1 for this build: materialised and served, status upgrade-available
        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(1)));
        assertThat(genDir(1).resolve("index-format.json")).doesNotExist();
        assertThat(com.pharos.indexer.IndexFormat.status(genDir(1)))
                .isEqualTo(com.pharos.indexer.IndexFormat.statusOf(0))
                .isEqualTo(com.pharos.indexer.IndexFormat.Status.UPGRADE_AVAILABLE);
    }

    @Test
    void newerFormatManifestLeavesTheOldGenerationServing() throws Exception {
        publishFull();
        syncer.syncOnce();
        publishIncremental("triple");
        int newer = com.pharos.indexer.IndexVersions.INDEX_FORMAT + 1;
        restampManifest(2, new com.pharos.indexer.IndexFormat.Stamp(newer, "9.9.9", java.time.Instant.now()));
        var log = captureLog();
        try {
            syncer.syncOnce();
            syncer.syncOnce(); // the WARN is not repeated for the same generation
        } finally {
            releaseLog(log);
        }

        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(1)));
        assertThat(cache.active(PROJECT)).contains(genDir(1));
        assertThat(genDir(2)).doesNotExist();
        assertThat(genDirNames()).containsExactly("gen-1");
        assertThat(readerRegistry.find(PROJECT).orElseThrow().getIndexPath()).isEqualTo(genDir(1).toString());
        var warns = log.list.stream()
                .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
                .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage).toList();
        assertThat(warns).hasSize(1);
        assertThat(warns.get(0)).contains(PROJECT, "generation 2", "index format " + newer, "newer",
                "reads index formats " + com.pharos.indexer.IndexFormat.MIN_READABLE_FORMAT + "-"
                        + com.pharos.indexer.IndexVersions.INDEX_FORMAT,
                "still serving generation 1", "upgrade pharos on this reader");

        // the writer re-publishes with a format this reader serves: the swap happens
        restampManifest(2, manifest(1).indexFormat());
        syncer.syncOnce();
        assertThat(syncer.served()).isEqualTo(Map.of(SHARD, new Generation(2)));
    }

    @Test
    void outdatedFormatManifestIsNotMaterialised() throws Exception {
        publishFull();
        int outdated = com.pharos.indexer.IndexFormat.MIN_READABLE_FORMAT - 1;
        restampManifest(1, new com.pharos.indexer.IndexFormat.Stamp(outdated, "0.0.1", null));
        var log = captureLog();
        try {
            syncer.syncOnce();
        } finally {
            releaseLog(log);
        }
        assertThat(syncer.served()).isEmpty();
        assertThat(genDir(1)).doesNotExist();
        assertThat(log.list.stream().map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage))
                .anySatisfy(m -> assertThat(m).contains("outdated", "still serving nothing yet",
                        "pharos index --full " + PROJECT));
    }

    // ── Pre-PR fix 5: registry before cache; prune keeps what is still referenced ────────────

    /** A registry whose writes can fail, and whose view of the shard can be pinned (another process). */
    private static final class FlakyRegistry extends ProjectRegistry {
        volatile boolean fail;
        volatile Path pinned;

        FlakyRegistry(Path file) { super(file); }

        @Override
        public synchronized void register(ProjectMeta meta) {
            if (fail) throw new IllegalStateException("disk full (injected)");
            super.register(meta);
        }

        @Override
        public synchronized java.util.Optional<ProjectMeta> find(String name) {
            if (pinned == null) return super.find(name);
            ProjectMeta m = new ProjectMeta();
            m.setName(name);
            m.setIndexPath(pinned.toString());
            return java.util.Optional.of(m);
        }
    }

    private ReplicaSyncer syncerWith(ProjectRegistry registry) {
        return new ReplicaSyncer(catalog, new ObjectStoreReplicaSource(store, layout), cache,
                registry, cacheDir, readerSynonyms, Duration.ofSeconds(10), lazy);
    }

    @Test
    void failedRegistrationDoesNotActivateTheNewGeneration() throws Exception {
        FlakyRegistry registry = new FlakyRegistry(tmp.resolve("flaky-registry.json"));
        try (ReplicaSyncer s = syncerWith(registry)) {
            publishFull();
            s.syncOnce();
            publishIncremental("triple");
            registry.fail = true;
            s.syncOnce();

            // gen-2 is materialised, but graph (registry) and Lucene (cache) both stay on gen-1
            assertThat(genDir(2)).isDirectory();
            assertThat(s.served()).isEqualTo(Map.of(SHARD, new Generation(1)));
            assertThat(cache.active(PROJECT)).contains(genDir(1));
            assertThat(registry.find(PROJECT).orElseThrow().getIndexPath()).isEqualTo(genDir(1).toString());

            registry.fail = false;
            s.syncOnce(); // the next poll adopts gen-2 and registers it, then activates it
            assertThat(s.served()).isEqualTo(Map.of(SHARD, new Generation(2)));
            assertThat(cache.active(PROJECT)).contains(genDir(2));
            assertThat(registry.find(PROJECT).orElseThrow().getIndexPath()).isEqualTo(genDir(2).toString());
        }
    }

    @Test
    void pruneKeepsTheGenerationTheRegistryStillPointsAt() throws Exception {
        FlakyRegistry registry = new FlakyRegistry(tmp.resolve("flaky-registry.json"));
        try (ReplicaSyncer s = syncerWith(registry)) {
            publishFull();
            s.syncOnce();
            registry.pinned = genDir(1); // e.g. another process on this cacheDir still names gen-1
            publishIncremental("triple");
            s.syncOnce();
            publishIncremental("quadruple");
            s.syncOnce();

            assertThat(s.served()).isEqualTo(Map.of(SHARD, new Generation(3)));
            assertThat(genDirNames()).containsExactly("gen-1", "gen-2", "gen-3");

            registry.pinned = null;
            publishIncremental("quintuple");
            s.syncOnce();
            assertThat(genDirNames()).containsExactly("gen-3", "gen-4");
        }
    }

    // ---- pinning ----

    @Test
    void pinnedReaderSyncsOnlyItsShards() throws Exception {
        publishFull();
        syncer.pin(java.util.Set.of(new ShardId("elsewhere")));
        syncer.syncOnce();
        assertThat(syncer.served()).isEmpty();
        assertThat(readerRegistry.find(PROJECT)).isEmpty();
        assertThat(cache.active(PROJECT)).isEmpty();
        assertThat(cacheDir.resolve(PROJECT)).doesNotExist();
    }

    @Test
    void unpinningSyncsTheRest() throws Exception {
        publishFull();
        syncer.pin(java.util.Set.of(new ShardId("elsewhere")));
        syncer.syncOnce();
        syncer.pin(java.util.Set.of());
        syncer.syncOnce();
        assertThat(syncer.served()).containsKey(SHARD);
        assertThat(readerRegistry.find(PROJECT)).isPresent();
    }

    @Test
    void repinningAwayUnregistersAndPrunesAfterGrace() throws Exception {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(1_000_000);
        syncer.setClock(now::get);
        syncer.setRetireGrace(Duration.ofSeconds(120));
        publishFull();
        syncer.syncOnce();
        Path dir = genDir(1);
        assertThat(dir).isDirectory();
        syncer.pin(java.util.Set.of(new ShardId("elsewhere")));
        syncer.syncOnce();
        assertThat(syncer.served()).isEmpty();
        assertThat(readerRegistry.find(PROJECT)).isEmpty();
        assertThat(cache.active(PROJECT)).isEmpty();
        now.addAndGet(119_000);
        syncer.syncOnce();
        assertThat(dir).as("kept for the grace period").isDirectory();
        now.addAndGet(2_000);
        syncer.syncOnce();
        assertThat(dir).doesNotExist();
    }

    @Test
    void graceIsNeverBelowPollInterval() throws Exception {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(1_000_000);
        syncer.setClock(now::get);
        syncer.setRetireGrace(Duration.ofSeconds(1)); // poll interval is 10 s
        publishFull();
        syncer.syncOnce();
        syncer.pin(java.util.Set.of(new ShardId("elsewhere")));
        syncer.syncOnce();
        now.addAndGet(5_000);
        syncer.syncOnce();
        assertThat(genDir(1)).isDirectory();
        now.addAndGet(6_000);
        syncer.syncOnce();
        assertThat(genDir(1)).doesNotExist();
    }

    @Test
    void startupSweepRemovesDirsOfShardsNeitherWantedNorRegistered() throws Exception {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(1_000_000);
        syncer.setClock(now::get);
        syncer.setRetireGrace(Duration.ofSeconds(60));
        publishFull();
        Path stray = Files.createDirectories(cacheDir.resolve("stray-shard").resolve("gen-3"));
        Path hidden = Files.createDirectories(cacheDir.resolve(".blocks"));
        syncer.syncOnce();
        assertThat(stray).isDirectory();
        now.addAndGet(61_000);
        syncer.syncOnce();
        assertThat(stray).doesNotExist();
        assertThat(hidden).isDirectory();
        assertThat(genDir(1)).isDirectory();
    }

    @Test
    void absentCatalogKeepsServingButAnEmptyOneDropsEverything() throws Exception {
        publishFull();
        syncer.syncOnce();
        // no catalog object at all (a store being reset, a wrong prefix): may be transient, drop nothing
        ReplicaSyncer absent = new ReplicaSyncer(new ObjectStoreCatalog(new InMemoryObjectStore(), layout),
                new ObjectStoreReplicaSource(store, layout), cache, readerRegistry, cacheDir, readerSynonyms,
                Duration.ofSeconds(10), lazy);
        try {
            absent.syncOnce();
        } finally {
            absent.close();
        }
        assertThat(readerRegistry.find(PROJECT)).isPresent();
        assertThat(cache.active(PROJECT)).isPresent();

        // the catalog exists and lists no shard (its last project was removed): authoritative
        assertThat(new ObjectStoreCatalog(store, layout).unregister(new ShardId(PROJECT))).isTrue();
        assertThat(new ObjectStoreCatalog(store, layout).authoritativeShards()).contains(List.of());
        syncer.syncOnce();
        assertThat(readerRegistry.find(PROJECT)).isEmpty();
        assertThat(cache.active(PROJECT)).isEmpty();
    }

    @Test
    void emptyCatalogReadDoesNotDropServedShards() throws Exception {
        publishFull();
        syncer.syncOnce();
        ReplicaSyncer empty = new ReplicaSyncer(() -> List.of(), new ObjectStoreReplicaSource(store, layout), cache,
                readerRegistry, cacheDir, readerSynonyms, Duration.ofSeconds(10), lazy);
        try {
            empty.syncOnce();
        } finally {
            empty.close();
        }
        assertThat(readerRegistry.find(PROJECT)).isPresent();
        assertThat(cache.active(PROJECT)).isPresent();
    }
}
