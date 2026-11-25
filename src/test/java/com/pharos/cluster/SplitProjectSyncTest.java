package com.pharos.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.routing.ShardFilter;
import com.pharos.cluster.routing.ShardRouter;
import com.pharos.cluster.sharding.DocKeys;
import com.pharos.cluster.sharding.ShardPlan;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.cluster.warm.LazyGenerations;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.GraphViews;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.IndexAbort;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6 Task 3, the reader side of split projects: a split project's shards are activated only as a
 * set at one index run, its graph is downloaded once per run per reader, and the shards of a staged
 * spec version are synced (and served) before the flip routes to them. Full mode;
 * {@link LazySplitProjectSyncTest} runs every case again in lazy mode.
 */
class SplitProjectSyncTest {

    static final String PROJECT = "lib";
    static final ShardSpec RING2 = ShardSpec.parse("[project, ring(path, shards=2)]");
    static final ShardId S01 = new ShardId("lib~ring=s01");
    static final ShardId S02 = new ShardId("lib~ring=s02");
    static final List<ShardId> SHARDS = List.of(S01, S02);

    @TempDir Path tmp;
    private Path source;
    private InMemoryObjectStore store;
    private ObjectStoreLayout layout;
    private ObjectStoreCatalog catalog;
    private ProjectIndexManager writer;
    private Path cacheDir;
    private ReaderLocalCache cache;
    private ProjectRegistry readerRegistry;
    private HeldSource replicas;
    private ReplicaSyncer syncer;
    LazyGenerations lazy;

    /** The lazy-mode hook; null runs full mode. */
    LazyGenerations lazyGenerations(Path cacheDir, ReplicaSource source) {
        return null;
    }

    /**
     * The bucket as a reader sees it, with one knob: a held shard's {@code LATEST} stays at what it was
     * when it was held (its writer published every shard, but this reader polled between the CASes).
     */
    static final class HeldSource implements ReplicaSource {
        final ReplicaSource inner;
        final Map<ShardId, Optional<Generation>> held = new ConcurrentHashMap<>();
        final List<String> downloads = new CopyOnWriteArrayList<>();

        HeldSource(ReplicaSource inner) { this.inner = inner; }

        void hold(ShardId shard) throws IOException { held.put(shard, inner.latest(shard)); }
        void release(ShardId shard) { held.remove(shard); }

        @Override public Optional<Generation> latest(ShardId shard) throws IOException {
            Optional<Generation> h = held.get(shard);
            return h != null ? h : inner.latest(shard);
        }
        @Override public ShardManifest manifest(ShardId shard, Generation gen) throws IOException {
            return inner.manifest(shard, gen);
        }
        @Override public void download(ManifestFile file, Path target) throws IOException {
            downloads.add(file.key());
            inner.download(file, target);
        }
        @Override public void downloadRange(ManifestFile file, long offset, long length, Path target) throws IOException {
            inner.downloadRange(file, offset, length, target);
        }
        @Override public InputStream openRange(ManifestFile file, long offset, long length) throws IOException {
            return inner.openRange(file, offset, length);
        }
        @Override public Optional<byte[]> synonyms() throws IOException { return inner.synonyms(); }
    }

    @BeforeEach
    void setUp() throws IOException {
        source = tmp.resolve("lib-src");
        for (int i = 0; i < 12; i++) writeClass("src/main/java/com/example/p" + (i % 3) + "/C" + i + ".java", i);
        store = new InMemoryObjectStore();
        layout = new ObjectStoreLayout("pharos");
        catalog = new ObjectStoreCatalog(store, layout);
        GenerationGc gc = new GenerationGc(store, layout, 2, Duration.ofMinutes(10));
        ObjectStorePublisher publisher = new ObjectStorePublisher(store, layout, catalog, gc, "writer", null);
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("writer-indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("writer-registry.json"));
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta, MavenPomReader.PomInfo pom) {
                return List.of();
            }
        };
        writer = new ProjectIndexManager(config, new LuceneIndexer(config), registry,
                List.of(EmbeddingProvider.create(config)), noopModules, List.of(new JavaCodeParser()), publisher);

        cacheDir = tmp.resolve("reader-cache");
        cache = new ReaderLocalCache(cacheDir);
        readerRegistry = ProjectRegistry.at(tmp.resolve("reader-registry.json"));
        ObjectStoreReplicaSource plain = new ObjectStoreReplicaSource(store, layout);
        lazy = lazyGenerations(cacheDir, plain);
        replicas = new HeldSource(plain);
        syncer = new ReplicaSyncer(catalog, replicas, cache, readerRegistry, cacheDir, null, Duration.ofSeconds(10), lazy);
    }

    @AfterEach
    void tearDown() {
        syncer.close();
        if (lazy != null) lazy.close();
    }

    /** A class calling the next one; {@code marker} is a word only this class contains. */
    private Path writeClass(String rel, int seed) throws IOException {
        String pkg = rel.substring("src/main/java/".length(), rel.lastIndexOf('/')).replace('/', '.');
        String name = rel.substring(rel.lastIndexOf('/') + 1, rel.length() - ".java".length());
        String body = "package " + pkg + ";\n\n/** Helper " + seed + ". */\npublic class " + name + " {\n"
                + "    /** marker" + seed + " work. */\n    public int run" + seed + "(int x) { return x + " + seed + "; }\n}\n";
        Path file = source.resolve(rel);
        Files.createDirectories(file.getParent());
        Files.writeString(file, body, StandardCharsets.UTF_8);
        return file;
    }

    private void splitProject() throws IOException {
        int v = catalog.addSpec(Optional.of(PROJECT), RING2);
        catalog.putProject(PROJECT, v, SHARDS);
    }

    private void index(boolean incremental, ShardPlan plan) throws IOException {
        writer.index(source, PROJECT, incremental, false, false, false, ProgressListener.SILENT, IndexAbort.none(),
                null, plan);
    }

    /** Ids of the live documents a query on this reader would see: every active generation of the project. */
    private Map<String, Integer> visibleIds() throws IOException {
        Map<String, Integer> out = new java.util.TreeMap<>();
        for (ShardId s : SHARDS) {
            Optional<Path> dir = cache.active(s.value());
            if (dir.isEmpty()) continue;
            try (FSDirectory d = FSDirectory.open(dir.get().resolve("lucene"));
                 DirectoryReader r = DirectoryReader.open(d)) {
                StoredFields sf = r.storedFields();
                org.apache.lucene.util.Bits live = org.apache.lucene.index.MultiBits.getLiveDocs(r);
                for (int doc = 0; doc < r.maxDoc(); doc++) {
                    if (live != null && !live.get(doc)) continue;
                    out.merge(sf.document(doc).get(DocumentMapper.F_ID), 1, Integer::sum);
                }
            }
        }
        return out;
    }

    private static long count(Map<String, Integer> ids, String fragment) {
        return ids.entrySet().stream().filter(e -> e.getKey().contains(fragment)).mapToLong(Map.Entry::getValue).sum();
    }

    /** A path the ring places in {@code from}'s shard, and one it places in the other shard. */
    private static String[] movePaths() {
        for (int i = 0; i < 500; i++) {
            String a = "src/main/java/com/example/move/Mover.java".replace("move/", "move" + i + "/");
            for (int j = 0; j < 500; j++) {
                String b = "src/main/java/com/example/moved" + j + "/Mover.java";
                if (!RING2.assign(DocKeys.of(PROJECT, a)).equals(RING2.assign(DocKeys.of(PROJECT, b)))) {
                    return new String[]{a, b};
                }
            }
        }
        throw new AssertionError("no move across the ring");
    }

    /**
     * The carry from Task 2 (a): a file moves to the other shard; the writer publishes both shards, but
     * the reader polls when only the file's new shard has flipped. It keeps serving the previous set
     * (the file once, from its old shard) until the old shard flips too; then the file is once in its
     * new shard. At no point is it visible twice or not at all.
     */
    @Test
    void splitProjectActivatesOnlyAsASet() throws IOException {
        String[] paths = movePaths();
        ShardId oldShard = RING2.assign(DocKeys.of(PROJECT, paths[0]));
        ShardId newShard = RING2.assign(DocKeys.of(PROJECT, paths[1]));
        Path mover = writeClass(paths[0], 777);
        splitProject();
        ShardPlan plan = new ShardPlan(PROJECT, RING2, SHARDS);
        index(false, plan);
        syncer.syncOnce();
        assertThat(syncer.served()).containsOnlyKeys(S01, S02);
        assertThat(count(visibleIds(), "Mover")).as("the class and its method, once each").isEqualTo(2);
        Map<ShardId, Generation> run1 = syncer.served();

        Path moved = source.resolve(paths[1]);
        Files.createDirectories(moved.getParent());
        Files.writeString(moved, Files.readString(mover).replace("package com.example." + mover.getParent().getFileName(),
                "package com.example." + moved.getParent().getFileName()));
        Files.delete(mover);
        Files.setLastModifiedTime(moved, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        replicas.hold(oldShard); // the reader polls after the new shard's CAS, before the old shard's
        index(true, plan);

        syncer.syncOnce();
        assertThat(syncer.served()).as("the new shard's generation waits for its set").isEqualTo(run1);
        assertThat(Files.isDirectory(cacheDir.resolve(newShard.pathSegment())
                .resolve("gen-" + replicas.inner.latest(newShard).orElseThrow().value())))
                .as("but it is materialised already").isTrue();
        Map<String, Integer> during = visibleIds();
        assertThat(count(during, "Mover")).as("the moved file, once (its old shard)").isEqualTo(2);
        assertThat(during.keySet()).allMatch(id -> during.get(id) == 1);

        replicas.release(oldShard);
        syncer.syncOnce();
        assertThat(syncer.served().get(S01)).isNotEqualTo(run1.get(S01));
        assertThat(syncer.served().get(S02)).isNotEqualTo(run1.get(S02));
        Map<String, Integer> after = visibleIds();
        assertThat(count(after, "Mover")).as("once, in its new shard").isEqualTo(2);
        assertThat(after.keySet()).allMatch(id -> after.get(id) == 1);
        assertThat(cache.active(newShard.value()).orElseThrow()).isEqualTo(cacheDir.resolve(newShard.pathSegment())
                .resolve("gen-" + syncer.served().get(newShard).value()));
    }

    /**
     * The carry from Task 2 (b): the project graph is downloaded once per index run by a reader, whatever
     * its number of shards, and every shard's generation serves that one file (hard links). Graph files
     * no generation links any more are swept.
     */
    @Test
    void projectGraphDownloadedOncePerRun() throws IOException {
        splitProject();
        ShardPlan plan = new ShardPlan(PROJECT, RING2, SHARDS);
        index(false, plan);
        syncer.syncOnce();
        String graphKey = replicas.inner.manifest(S01, replicas.inner.latest(S01).orElseThrow()).graphFile()
                .orElseThrow().key();
        assertThat(graphKey).contains("/projects/lib/graph/");
        assertThat(replicas.downloads.stream().filter(graphKey::equals)).hasSize(1);
        Path g1 = cache.active(S01.value()).orElseThrow().resolve(GraphViews.COMPACT_FILE);
        Path g2 = cache.active(S02.value()).orElseThrow().resolve(GraphViews.COMPACT_FILE);
        assertThat(fileKey(g1)).as("one file on disk").isEqualTo(fileKey(g2));
        assertThat(GraphViews.open(PROJECT, cache.active(S01.value()).orElseThrow()).callees(
                "com.example.p0.C0#run0(int)")).isNotNull();

        for (int run = 2; run <= 4; run++) {
            writeClass("src/main/java/com/example/p9/Extra" + run + ".java", 100 + run);
            index(true, plan);
            syncer.syncOnce();
        }
        assertThat(replicas.downloads.stream().filter(k -> k.contains("/projects/lib/graph/"))).hasSize(4);
        try (Stream<Path> s = Files.list(cacheDir.resolve(ReplicaSyncer.PROJECT_FILES_DIR))) {
            List<String> left = s.map(p -> p.getFileName().toString()).toList();
            assertThat(left).as("runs 3 and 4 (the active and the previous generations); runs 1 and 2 swept")
                    .hasSize(2);
            assertThat(left).noneMatch(n -> n.contains(java.net.URLEncoder.encode(graphKey, StandardCharsets.UTF_8)));
        }
    }

    /**
     * A reshard in progress: the catalog lists a staged spec version for the project. The reader syncs
     * and serves the staged shards ahead (its heartbeat lists them), the router does not route to them;
     * at the flip the router routes to them and the reader downloads nothing more.
     */
    @Test
    void stagedShardsSyncedBeforeFlip() throws IOException {
        index(false, ShardPlan.single(PROJECT));          // version N: [project]
        splitProject();
        index(false, new ShardPlan(PROJECT, RING2, SHARDS)); // version N+1's shards, published
        int staged = catalog.projectShards().specVersionOf(PROJECT).orElseThrow();
        rewriteCatalog(root -> {                          // ... and the catalog back to N, N+1 staged
            ArrayNode projects = root.putArray("projects");
            ObjectNode lib = projects.addObject();
            lib.put("name", PROJECT);
            lib.putArray("shards").add(PROJECT);
            ObjectNode st = lib.putObject("staged");
            st.put("spec", staged);
            ArrayNode ids = st.putArray("shards");
            SHARDS.forEach(s -> ids.add(s.value()));
        });
        ShardRouter router = new ShardRouter(() -> List.of(new ReaderHeartbeat("r", "http://r",
                List.of(PROJECT, S01.value(), S02.value()), false, Map.of(), 0, 0, 0L)), catalog,
                new ReaderHealth(30), s -> false);

        syncer.syncOnce();
        assertThat(syncer.served()).as("version N and the staged shards").containsOnlyKeys(new ShardId(PROJECT), S01, S02);
        assertThat(cache.active(S01.value())).isPresent();
        assertThat(cache.active(S02.value())).isPresent();
        assertThat(router.plan(List.of(PROJECT), ShardFilter.NONE).shards())
                .as("nothing routes to the staged shards before the flip")
                .extracting(s -> s.shard().value()).containsExactly(PROJECT);

        int downloads = replicas.downloads.size();
        Map<ShardId, Generation> stagedGens = Map.of(S01, syncer.served().get(S01), S02, syncer.served().get(S02));
        catalog.putProject(PROJECT, staged, SHARDS);     // the flip: one CAS
        rewriteCatalog(root -> { });
        syncer.syncOnce();
        assertThat(router.plan(List.of(PROJECT), ShardFilter.NONE).shards())
                .extracting(s -> s.shard().value()).containsExactly(S01.value(), S02.value());
        assertThat(replicas.downloads).as("the staged shards were synced already").hasSize(downloads);
        assertThat(syncer.served()).as("version N's shard is no longer served").containsExactlyInAnyOrderEntriesOf(stagedGens);
    }

    /** Pin wildcards (P6-R5): {@code lib~*} keeps every shard of lib, through the catalog mapping. */
    @Test
    void projectWildcardPinsEveryShardOfTheProject() throws IOException {
        splitProject();
        index(false, new ShardPlan(PROJECT, RING2, SHARDS));
        writer.index(tmp.resolve("lib-src"), "other", false, false, false, false, ProgressListener.SILENT);
        syncer.pin(ReaderPin.of(List.of(ShardId.wildcardOf(PROJECT))));
        syncer.syncOnce();
        assertThat(syncer.served()).containsOnlyKeys(S01, S02);
        syncer.pin(ReaderPin.of(List.of(S02.value(), "other")));
        syncer.syncOnce();
        assertThat(syncer.served()).containsOnlyKeys(S02, new ShardId("other"));
    }

    private void rewriteCatalog(java.util.function.Consumer<ObjectNode> edit) throws IOException {
        ObjectMapper json = new ObjectMapper();
        ObjectNode root = (ObjectNode) json.readTree(store.get(layout.catalogKey()).orElseThrow().data());
        edit.accept(root);
        store.put(layout.catalogKey(), json.writeValueAsBytes(root));
    }

    private static Object fileKey(Path p) throws IOException {
        return Files.readAttributes(p, BasicFileAttributes.class).fileKey();
    }
}
