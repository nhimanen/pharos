package com.pharos.cluster.objectstore;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.WriterStateStore;
import com.pharos.cluster.writer.LeaseGuard;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Writer-state backup after publish and restore on takeover: two writer nodes (separate index dirs
 * and registries) indexing the same checkout path against one bucket.
 */
class ObjectStoreWriterStateStoreTest {

    private static final String PROJECT = "sample-app";
    private static final ShardId SHARD = new ShardId(PROJECT);

    @TempDir Path tmp;

    private Path source;
    private final AtomicLong offsetSeconds = new AtomicLong();
    private InMemoryObjectStore store;
    private ObjectStoreLayout layout;
    private ObjectStoreCatalog catalog;
    private ObjectStoreReplicaSource replicas;
    private final List<Node> nodes = new ArrayList<>();
    private ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> logs;

    /** The bucket's clock: real time plus an offset a test can move forward. */
    private final Clock clock = new Clock() {
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId z) { return this; }
        @Override public Instant instant() { return Instant.now().plusSeconds(offsetSeconds.get()); }
    };

    /** Deterministic embedder counting every text it embeds. */
    static final class CountingEmbedder implements EmbeddingProvider {
        final AtomicInteger calls = new AtomicInteger();

        @Override public String modelId() { return "test-counting"; }
        @Override public int dimensions() { return 8; }
        @Override public boolean isAvailable() { return true; }

        @Override public float[] embed(String text) {
            calls.incrementAndGet();
            float[] v = new float[8];
            int h = text.hashCode();
            for (int i = 0; i < 8; i++) v[i] = ((h >>> (i * 4)) & 0xF) / 15f + 0.01f;
            return v;
        }

        @Override public float[][] embedBatch(List<String> texts) {
            float[][] out = new float[texts.size()][];
            for (int i = 0; i < texts.size(); i++) out[i] = embed(texts.get(i));
            return out;
        }
    }

    /** One writer node: its own index dir and registry, the shared bucket. */
    final class Node {
        final IndexConfig config;
        final ProjectRegistry registry;
        final LuceneIndexer lucene;
        final CountingEmbedder embedder = new CountingEmbedder();
        final ObjectStoreWriterStateStore state;
        final ProjectIndexManager manager;

        Node(String name, WriterStateStore stateOverride, int keepGenerations) {
            config = IndexConfig.defaults();
            config.setIndexDir(tmp.resolve(name).resolve("indexes"));
            registry = ProjectRegistry.at(tmp.resolve(name).resolve("registry.json"));
            lucene = new LuceneIndexer(config);
            state = new ObjectStoreWriterStateStore(store, layout, replicas, name);
            GenerationGc gc = new GenerationGc(store, layout, keepGenerations, Duration.ofMinutes(10));
            ObjectStorePublisher publisher = new ObjectStorePublisher(store, layout, catalog, gc, name, null,
                    LeaseGuard.IMPLICIT, stateOverride != null ? stateOverride : state);
            ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
                @Override
                public synchronized List<String> incorporate(Path root, ProjectMeta meta, MavenPomReader.PomInfo pom) {
                    return List.of();
                }
            };
            manager = new ProjectIndexManager(config, lucene, registry, List.of(embedder), noopModules,
                    List.of(new JavaCodeParser()), publisher);
            nodes.add(this);
        }

        Node(String name) { this(name, null, 5); }

        Path indexDir() { return lucene.getProjectIndexDir(PROJECT); }

        ProjectMeta index(boolean incremental) throws IOException {
            return manager.index(source, PROJECT, incremental, false, true, false, ProgressListener.SILENT);
        }

        /** What a new owner does with the restored generation: register it under this node's paths. */
        void registerRestored(Generation gen) throws IOException {
            ProjectMeta meta = ShardManifest.stripRefs(replicas.manifest(SHARD, gen).projectMeta());
            meta.setName(PROJECT);
            meta.setRootPath(source.toString());
            meta.setIndexPath(indexDir().toString());
            registry.register(meta);
        }

        int docsFor(Path file) throws IOException {
            try (DirectoryReader r = DirectoryReader.open(FSDirectory.open(indexDir().resolve("lucene")))) {
                return new IndexSearcher(r).count(new TermQuery(new Term(DocumentMapper.F_FILE_PATH,
                        file.toAbsolutePath().toString())));
            }
        }

        int docCount() throws IOException {
            try (DirectoryReader r = DirectoryReader.open(FSDirectory.open(indexDir().resolve("lucene")))) {
                return r.numDocs();
            }
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        source = tmp.resolve("checkout");
        Path from = Path.of("src/test/resources/test-projects/sample-app");
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path dest = source.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(dest);
                else Files.copy(p, dest);
            }
        }
        store = new InMemoryObjectStore(clock);
        layout = new ObjectStoreLayout("pharos");
        catalog = new ObjectStoreCatalog(store, layout);
        replicas = new ObjectStoreReplicaSource(store, layout);
        logs = new ch.qos.logback.core.read.ListAppender<>();
        logs.start();
        logger().addAppender(logs);
    }

    @AfterEach
    void tearDown() throws IOException {
        logger().detachAppender(logs);
        for (Node n : nodes) n.lucene.close();
    }

    private static ch.qos.logback.classic.Logger logger() {
        return (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ObjectStoreWriterStateStore.class);
    }

    private List<String> warnings() {
        return logs.list.stream().filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
                .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage).toList();
    }

    private Path calculator() {
        return source.resolve("src/main/java/com/example/Calculator.java");
    }

    /** Adds a method to Calculator (a real change: new method text, changed class text). */
    private void addMethod(String name) throws IOException {
        Path f = calculator();
        String src = Files.readString(f);
        int end = src.lastIndexOf('}');
        Files.writeString(f, src.substring(0, end) + "\n    public int " + name + "(int x) {\n        return x * 31 + "
                + name.length() + ";\n    }\n}\n");
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 5_000));
    }

    /** A fresh checkout: same bytes, new mtimes everywhere. */
    private void touchAll() throws IOException {
        long t = System.currentTimeMillis() + 60_000;
        try (Stream<Path> s = Files.walk(source)) {
            for (Path p : s.filter(Files::isRegularFile).toList()) Files.setLastModifiedTime(p, FileTime.fromMillis(t));
        }
    }

    private Set<String> keys(String prefix) {
        return store.list(prefix).stream().map(ObjectInfo::key).collect(Collectors.toCollection(java.util.TreeSet::new));
    }

    private static Map<String, byte[]> stateFiles(Path dir) throws IOException {
        Map<String, byte[]> out = new TreeMap<>();
        try (Stream<Path> s = Files.list(dir)) {
            for (Path p : s.toList()) {
                String n = p.getFileName().toString();
                if (n.equals(ObjectStoreWriterStateStore.FILE_STATE)
                        || ObjectStoreWriterStateStore.EMBED_CACHE.matcher(n).matches()) {
                    out.put(n, Files.readAllBytes(p));
                }
            }
        }
        return out;
    }

    private static Set<String> luceneFiles(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir.resolve("lucene"))) {
            return s.map(p -> p.getFileName().toString()).filter(n -> !n.equals("write.lock"))
                    .collect(Collectors.toCollection(java.util.TreeSet::new));
        }
    }

    private static long methodCount(Path indexDir) throws IOException {
        assertThat(CallGraph.pointerProblem(indexDir)).isNull();
        try (CallGraph g = CallGraph.openExisting(CallGraph.writerGraphDir(indexDir))) {
            return g.methodCount();
        }
    }

    // ---------------------------------------------------------------------------------------

    @Test
    void backupAfterPublishRestoresIdentically() throws Exception {
        Node a = new Node("node-a");
        a.index(false);
        assertThat(replicas.latest(SHARD)).contains(Generation.FIRST);
        String prefix = layout.writerStatePrefix(SHARD);
        assertThat(keys(prefix)).contains(prefix + "gen-1/index.json", prefix + "gen-1/file-state.json");
        assertThat(keys(prefix + "blobs/")).hasSize(2); // embed-cache.test-counting.{bin,meta}

        Node b = new Node("node-b");
        Files.createDirectories(b.indexDir());
        Files.writeString(b.indexDir().resolve("stale-leftover"), "x"); // replaced as a whole
        // a stale local Lucene dir: one file reusable, one of the same name and length but other bytes
        Path staleLucene = Files.createDirectories(b.indexDir().resolve("lucene"));
        List<String> names = new ArrayList<>(luceneFiles(a.indexDir()));
        Files.copy(a.indexDir().resolve("lucene").resolve(names.get(0)), staleLucene.resolve(names.get(0)));
        byte[] other = Files.readAllBytes(a.indexDir().resolve("lucene").resolve(names.get(1)));
        other[other.length - 1] ^= 0x01; // footer checksum differs
        Files.write(staleLucene.resolve(names.get(1)), other);
        assertThat(b.state.restore(SHARD, b.indexDir())).contains(Generation.FIRST);

        Map<String, byte[]> want = stateFiles(a.indexDir()), got = stateFiles(b.indexDir());
        assertThat(got.keySet()).isEqualTo(want.keySet()).contains("file-state.json", "embed-cache.test-counting.bin");
        want.forEach((name, bytes) -> assertThat(got.get(name)).as(name).isEqualTo(bytes));
        assertThat(luceneFiles(b.indexDir())).isEqualTo(luceneFiles(a.indexDir()));
        assertThat(b.docCount()).isEqualTo(a.docCount());
        assertThat(methodCount(b.indexDir())).isEqualTo(methodCount(a.indexDir())).isPositive();
        assertThat(IndexFormat.status(b.indexDir())).isEqualTo(IndexFormat.Status.OK);
        Path refs = a.indexDir().resolve(ObjectStorePublisher.REFS_FILE);
        if (Files.exists(refs)) {
            assertThat(b.indexDir().resolve(ObjectStorePublisher.REFS_FILE)).hasSameBinaryContentAs(refs);
        }
        assertThat(b.indexDir().resolve("stale-leftover")).doesNotExist();
        try (Stream<Path> s = Files.list(b.indexDir().getParent())) {
            assertThat(s.map(p -> p.getFileName().toString())).containsExactly(PROJECT); // no staging left
        }

        // A second backup of unchanged caches uploads only the file state and the index.
        int blobPuts = keys(prefix + "blobs/").size();
        a.state.backup(SHARD, Generation.FIRST, a.indexDir());
        assertThat(keys(prefix + "blobs/")).hasSize(blobPuts);
    }

    @Test
    void restoreThenIncrementalIndexReembedsNothingUnchanged() throws Exception {
        Node a = new Node("node-a");
        a.index(false);
        assertThat(a.embedder.calls.get()).isPositive();

        Node b = new Node("node-b");
        Optional<Generation> restored = b.state.restore(SHARD, b.indexDir());
        assertThat(restored).contains(Generation.FIRST);
        b.registerRestored(restored.get());

        // a fresh checkout: new mtimes, same bytes — nothing to re-index, nothing to embed
        touchAll();
        b.index(true);
        assertThat(b.embedder.calls.get()).isZero();
        assertThat(replicas.latest(SHARD)).contains(new Generation(2));

        // one changed file: only its new texts are embedded, exactly what the old owner would embed
        addMethod("triple");
        b.index(true);
        int bCalls = b.embedder.calls.get();
        assertThat(bCalls).isPositive();
        // the control is the old owner itself: same change, same cache, same count
        a.embedder.calls.set(0);
        a.index(true);
        assertThat(bCalls).isEqualTo(a.embedder.calls.get());
        assertThat(b.docsFor(calculator())).isEqualTo(a.docsFor(calculator()));
        assertThat(b.docCount()).isEqualTo(a.docCount());
    }

    /**
     * Ruling R8's takeover (restore, then a FULL index): the restored embedding cache alone makes
     * every unchanged text cost 0 provider calls, even though the full run re-parses everything and
     * rebuilds Lucene and the graph.
     */
    @Test
    void restoreThenFullIndexReembedsNothingUnchanged() throws Exception {
        Node a = new Node("node-a");
        a.index(false);
        assertThat(a.embedder.calls.get()).isPositive();

        Node b = new Node("node-b");
        assertThat(b.state.restore(SHARD, b.indexDir())).contains(Generation.FIRST);
        touchAll();
        b.index(false); // full
        assertThat(b.embedder.calls.get()).isZero();
        assertThat(b.docCount()).isEqualTo(a.docCount());
        assertThat(methodCount(b.indexDir())).isEqualTo(methodCount(a.indexDir()));
        assertThat(replicas.latest(SHARD)).contains(new Generation(2));

        // a cold node (no restore) pays for every text
        Node cold = new Node("node-cold", WriterStateStore.NONE, 5);
        cold.index(false);
        assertThat(cold.embedder.calls.get()).isEqualTo(a.embedder.calls.get());
    }

    @Test
    void restoreMaterialisesLatestThenOverlaysState() throws Exception {
        Node a = new Node("node-a");
        a.index(false); // gen 1, backed up
        byte[] gen1State = Files.readAllBytes(a.indexDir().resolve("file-state.json"));
        addMethod("quadruple");
        a.index(true); // gen 2, its backup then lost
        String gen2 = layout.writerStateGenPrefix(SHARD, new Generation(2));
        for (String k : keys(gen2)) store.delete(k);
        assertThat(replicas.latest(SHARD)).contains(new Generation(2));

        Node b = new Node("node-b");
        assertThat(b.state.restore(SHARD, b.indexDir())).contains(new Generation(2));
        // the index is LATEST (gen 2: it has the new method) ...
        Set<String> gen2Lucene = replicas.manifest(SHARD, new Generation(2)).files().stream()
                .filter(f -> ManifestFile.LUCENE.equals(f.role())).map(ManifestFile::name)
                .collect(Collectors.toCollection(java.util.TreeSet::new));
        assertThat(luceneFiles(b.indexDir())).isEqualTo(gen2Lucene);
        assertThat(b.docCount()).isEqualTo(a.docCount());
        try (CallGraph g = CallGraph.openExisting(CallGraph.writerGraphDir(b.indexDir()))) {
            assertThat(g.allFqns().anyMatch(f -> f.contains("quadruple"))).isTrue();
        }
        // ... overlaid with the older (gen 1) writer state
        assertThat(b.indexDir().resolve("file-state.json")).hasBinaryContent(gen1State);
        assertThat(warnings()).anyMatch(w -> w.contains("writer state of generation 1"));

        // the file changed since gen 1 is dirty: re-indexed (no duplicate docs), its new texts re-embedded
        b.registerRestored(new Generation(2));
        b.index(true);
        assertThat(b.embedder.calls.get()).isPositive();
        assertThat(b.docsFor(calculator())).isEqualTo(a.docsFor(calculator()));
        assertThat(b.docCount()).isEqualTo(a.docCount());
        assertThat(replicas.latest(SHARD)).contains(new Generation(3));
    }

    @Test
    void missingBackupFallsBackToFullIndex() throws Exception {
        Node a = new Node("node-a", WriterStateStore.NONE, 5);
        a.index(false);
        assertThat(keys(layout.writerStatePrefix(SHARD))).isEmpty();

        Node b = new Node("node-b");
        assertThat(b.state.restore(SHARD, b.indexDir())).isEmpty();
        assertThat(warnings()).anyMatch(w -> w.contains("no writer-state backup") && w.contains("full index"));
        assertThat(b.indexDir()).doesNotExist();

        // the shard is not blocked: the full index publishes a valid generation on top of LATEST
        b.index(true);
        assertThat(b.embedder.calls.get()).isPositive();
        assertThat(replicas.latest(SHARD)).contains(new Generation(2));
        ShardManifest m = replicas.manifest(SHARD, new Generation(2));
        assertThat(m.parentGeneration()).isEqualTo(1L);
        assertThat(m.writerNode()).isEqualTo("node-b");
        assertThat(m.docCount()).isEqualTo(a.docCount());
        assertThat(keys(layout.writerStatePrefix(SHARD))).contains(layout.writerStateIndexKey(SHARD, new Generation(2)));
    }

    @Test
    void corruptBackupDetectedByChecksumAndIgnored() throws Exception {
        Node a = new Node("node-a");
        a.index(false);
        Node b = new Node("node-b");
        Files.createDirectories(b.indexDir());
        Path marker = Files.writeString(b.indexDir().resolve("untouched"), "keep");

        // 1. a blob whose bytes no longer match the SHA-256 in the index
        String blob = keys(layout.writerStatePrefix(SHARD) + "blobs/").stream()
                .filter(k -> store.get(k).orElseThrow().data().length > 100).findFirst().orElseThrow();
        byte[] good = store.get(blob).orElseThrow().data();
        byte[] bad = good.clone();
        bad[bad.length / 2] ^= 0x55;
        store.put(blob, bad);
        assertThat(b.state.restore(SHARD, b.indexDir())).isEmpty();
        assertThat(warnings()).anyMatch(w -> w.contains("checksum mismatch") && w.contains("full index"));
        assertThat(marker).hasContent("keep");
        assertThat(luceneFilesOrNone(b.indexDir())).isEmpty();
        assertThat(store.get(blob)).isEmpty(); // garbage removed: the next backup re-uploads it

        // 2. a tampered file state
        a.state.backup(SHARD, Generation.FIRST, a.indexDir());
        String fileState = layout.writerStateGenPrefix(SHARD, Generation.FIRST) + "file-state.json";
        store.put(fileState, "{}".getBytes());
        logs.list.clear();
        assertThat(b.state.restore(SHARD, b.indexDir())).isEmpty();
        assertThat(warnings()).anyMatch(w -> w.contains("checksum mismatch"));

        // 3. an unparseable index
        a.state.backup(SHARD, Generation.FIRST, a.indexDir());
        store.put(layout.writerStateIndexKey(SHARD, Generation.FIRST), "{not json".getBytes());
        logs.list.clear();
        assertThat(b.state.restore(SHARD, b.indexDir())).isEmpty();
        assertThat(warnings()).anyMatch(w -> w.contains("unparseable"));
        assertThat(marker).hasContent("keep");

        // the shard still indexes (full) and publishes
        b.index(true);
        assertThat(replicas.latest(SHARD)).contains(new Generation(2));
    }

    private static Set<String> luceneFilesOrNone(Path dir) throws IOException {
        return Files.isDirectory(dir.resolve("lucene")) ? luceneFiles(dir) : Set.of();
    }

    @Test
    void gcRemovesOldWriterState() throws Exception {
        Node a = new Node("node-a", null, 2);
        a.index(false); // gen 1
        for (String m : List.of("alpha", "beta", "gamma")) {
            addMethod(m);
            a.index(true); // gens 2..4, each with a changed embedding cache
        }
        String prefix = layout.writerStatePrefix(SHARD);
        Map<Long, Set<String>> blobsByGen = new TreeMap<>();
        for (long g = 1; g <= 4; g++) {
            WriterStateIndex idx = WriterStateIndex.read(
                    store.get(layout.writerStateIndexKey(SHARD, new Generation(g))).orElseThrow().data());
            blobsByGen.put(g, idx.files().stream().map(WriterStateIndex.Entry::key)
                    .filter(k -> k.startsWith(prefix + "blobs/")).collect(Collectors.toSet()));
        }
        assertThat(blobsByGen.get(1L)).isNotEqualTo(blobsByGen.get(4L));

        // everything is older than minAge now: the next publish's GC keeps gens 4 and 5 only
        offsetSeconds.set(3600);
        addMethod("delta");
        a.index(true); // gen 5
        Set<String> left = keys(prefix);
        for (long g = 1; g <= 3; g++) {
            String genPrefix = prefix + "gen-" + g + "/";
            assertThat(left).as("gen " + g).noneMatch(k -> k.startsWith(genPrefix));
        }
        for (long g = 4; g <= 5; g++) {
            assertThat(left).contains(layout.writerStateIndexKey(SHARD, new Generation(g)),
                    prefix + "gen-" + g + "/file-state.json");
        }
        WriterStateIndex five = WriterStateIndex.read(
                store.get(layout.writerStateIndexKey(SHARD, new Generation(5))).orElseThrow().data());
        Set<String> keptBlobs = new java.util.HashSet<>(blobsByGen.get(4L));
        five.files().forEach(e -> keptBlobs.add(e.key()));
        keptBlobs.removeIf(k -> !k.startsWith(prefix + "blobs/"));
        assertThat(keys(prefix + "blobs/")).isEqualTo(new java.util.TreeSet<>(keptBlobs));
        Set<String> onlyOld = new java.util.HashSet<>(blobsByGen.get(1L));
        onlyOld.removeAll(keptBlobs);
        assertThat(onlyOld).isNotEmpty().allMatch(k -> store.get(k).isEmpty());

        // the retained backup still restores
        Node b = new Node("node-b");
        assertThat(b.state.restore(SHARD, b.indexDir())).contains(new Generation(5));
    }

    @Test
    void backupFailureNeverFailsThePublish() throws Exception {
        var publisherLog = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ObjectStorePublisher.class);
        publisherLog.addAppender(logs);
        try {
            Node a = new Node("node-a", new WriterStateStore() {
                @Override public void backup(ShardId shard, Generation gen, Path indexDir) throws IOException {
                    throw new IOException("bucket full");
                }

                @Override public Optional<Generation> restore(ShardId shard, Path indexDir) {
                    return Optional.empty();
                }
            }, 5);
            a.index(false);
            assertThat(replicas.latest(SHARD)).contains(Generation.FIRST);
            assertThat(warnings()).anyMatch(w -> w.contains("Writer-state backup") && w.contains("bucket full"));
        } finally {
            publisherLog.detachAppender(logs);
        }
    }

    @Test
    void nothingPublishedRestoresNothing() throws Exception {
        Node b = new Node("node-b");
        assertThat(b.state.restore(SHARD, b.indexDir())).isEmpty();
        assertThat(b.indexDir()).doesNotExist();
    }
}
