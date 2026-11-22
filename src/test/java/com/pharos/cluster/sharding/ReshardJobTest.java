package com.pharos.cluster.sharding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cli.ReshardCommand;
import com.pharos.cluster.Generation;
import com.pharos.cluster.ReaderLocalCache;
import com.pharos.cluster.ReplicaSource;
import com.pharos.cluster.ReplicaSyncer;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.BackendKind;
import com.pharos.cluster.NodeRole;
import com.pharos.cluster.membership.ReaderHealth;
import com.pharos.cluster.membership.ReaderHeartbeat;
import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ManifestFile;
import com.pharos.cluster.objectstore.ObjectInfo;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ObjectStoreWriterStateStore;
import com.pharos.cluster.objectstore.ShardManifest;
import com.pharos.cluster.routing.ShardFilter;
import com.pharos.cluster.routing.ShardRouter;
import com.pharos.cluster.writer.Lease;
import com.pharos.cluster.writer.LeaseManager;
import com.pharos.cluster.writer.SourceCheckout;
import com.pharos.cluster.writer.WorkLoop;
import com.pharos.cluster.writer.WorkQueueLeaseGuard;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.CountingEmbedder;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import com.pharos.parser.model.ParsedFile;
import com.pharos.parser.model.ParsedProject;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 6 Task 4, the resharding job, with in-JVM work-loop writers and replica-syncer readers on one
 * {@link InMemoryObjectStore}; a fake clock drives the bucket and the writers, and the tests tick and sync
 * by hand. Embeddings come from a counting fake, parses are counted at the parser.
 */
class ReshardJobTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    static final String P = "lib";
    static final ShardSpec RING2 = ShardSpec.parse("[project, ring(path, shards=2)]");
    static final ShardSpec RING3 = ShardSpec.parse("[project, ring(path, shards=3)]");
    static final ShardSpec RING4 = ShardSpec.parse("[project, ring(path, shards=4)]");
    static final ShardSpec RING5 = ShardSpec.parse("[project, ring(path, shards=5)]");
    static final ShardSpec RING6 = ShardSpec.parse("[project, ring(path, shards=6)]");
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration HEARTBEAT = Duration.ofSeconds(10);
    private static final Duration MIN_HOLD = Duration.ofSeconds(120);
    private static final Duration RETENTION = Duration.ofMinutes(10);
    private static final Duration SYNC_TIMEOUT = Duration.ofSeconds(600);
    private static final Duration ACTIVATION_HOLD = Duration.ofSeconds(15);

    /** One clock for the bucket and every writer; only the test moves it. */
    static final class TestClock extends Clock {
        final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-22T10:00:00Z"));
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId z) { return this; }
        @Override public Instant instant() { return now.get(); }
        void advance(long seconds) { now.updateAndGet(i -> i.plusSeconds(seconds)); }
    }

    /** The Java parser, counting the files it parses. */
    static final class CountingParser extends JavaCodeParser {
        final List<Path> parsed = new CopyOnWriteArrayList<>();

        @Override
        public ParsedFile parseFile(Path file, String projectName) throws IOException {
            parsed.add(file);
            return super.parseFile(file, projectName);
        }

        @Override
        public ParsedProject parseFiles(List<Path> files, Path projectRoot, String projectName) throws IOException {
            parsed.addAll(files);
            return super.parseFiles(files, projectRoot, projectName);
        }
    }

    @TempDir Path tmp;
    private final TestClock clock = new TestClock();
    private InMemoryObjectStore store;
    private ObjectStoreLayout layout;
    private ObjectStoreCatalog catalog;
    private ObjectStoreReplicaSource replicas;
    private Path src;
    /** Manifest writes of shards whose key contains one of these fail (an interrupted publish). */
    private final Set<String> failManifestsOf = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** The project's spec version after each catalog write that changed it. */
    private final List<Integer> activeVersions = new CopyOnWriteArrayList<>();
    private volatile List<ReaderHeartbeat> heartbeats = List.of();
    private final List<Writer> writers = new ArrayList<>();
    private final List<Reader> readers = new ArrayList<>();
    private int seq;

    /** One writer node: its own index dir, registry, work dir, embedder and parser; the shared bucket. */
    final class Writer {
        final String node;
        final LuceneIndexer lucene;
        final CountingEmbedder embedder = new CountingEmbedder();
        final CountingParser parser = new CountingParser();
        final WorkLoop loop;
        ProjectIndexManager manager;

        Writer(String node, Duration minPinned) {
            this(node, minPinned, false);
        }

        /** @param handRun publishes under the implicit lease (a hand-run writer racing the work loop) */
        Writer(String node, Duration minPinned, boolean handRun) {
            this.node = node;
            Path home = tmp.resolve("w" + (++seq) + "-" + node);
            IndexConfig config = IndexConfig.defaults();
            config.setIndexDir(home.resolve("indexes"));
            ProjectRegistry registry = ProjectRegistry.at(home.resolve("registry.json"));
            lucene = new LuceneIndexer(config);
            ObjectStoreWriterStateStore state = new ObjectStoreWriterStateStore(store, layout, replicas, node);
            WorkQueueLeaseGuard guard = new WorkQueueLeaseGuard();
            GenerationGc gc = new GenerationGc(store, layout, 1, RETENTION);
            ObjectStorePublisher publisher = new ObjectStorePublisher(store, layout, catalog, gc, node, null,
                    handRun ? com.pharos.cluster.writer.LeaseGuard.IMPLICIT : guard, state);
            ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
                @Override
                public synchronized List<String> incorporate(Path root, ProjectMeta meta, MavenPomReader.PomInfo pom) {
                    return List.of();
                }
            };
            manager = new ProjectIndexManager(config, lucene, registry, List.of(embedder),
                    noopModules, List.of(parser), publisher);
            loop = new WorkLoop(new WorkLoop.Settings(node, TTL, HEARTBEAT, Duration.ofSeconds(60), MIN_HOLD, true, false),
                    store, layout, catalog, replicas, state, manager, lucene, registry,
                    new SourceCheckout(home.resolve("work")), clock);
            guard.attach(loop);
            loop.enableResharding(publisher, gc, () -> heartbeats, new ReshardJob.Settings(SYNC_TIMEOUT, RETENTION,
                    minPinned));
            writers.add(this);
        }

        Writer(String node) {
            this(node, Duration.ZERO);
        }
    }

    /** The bucket as a reader sees it, counting what it downloads. */
    static final class CountingSource implements ReplicaSource {
        final ReplicaSource inner;
        final List<String> downloads = new CopyOnWriteArrayList<>();

        CountingSource(ReplicaSource inner) { this.inner = inner; }

        @Override public Optional<Generation> latest(ShardId shard) throws IOException { return inner.latest(shard); }
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

    /** One reader node: a replica syncer over its own cache dir. */
    final class Reader {
        final String node;
        final ReaderLocalCache cache;
        final CountingSource source;
        final ReplicaSyncer syncer;

        Reader(String node) {
            this.node = node;
            Path cacheDir = tmp.resolve("r-" + node);
            cache = new ReaderLocalCache(cacheDir);
            source = new CountingSource(new ObjectStoreReplicaSource(store, layout));
            syncer = new ReplicaSyncer(catalog, source, cache, ProjectRegistry.at(tmp.resolve("r-" + node + ".json")),
                    cacheDir, null, Duration.ofSeconds(10));
            syncer.setClock(clock::millis);
            syncer.setActivationHold(ACTIVATION_HOLD);
            readers.add(this);
        }

        /** Syncs, then publishes this reader's heartbeat (the only reader the writers see). */
        void sync() throws IOException {
            syncer.syncOnce();
            heartbeats = List.of(heartbeat());
        }

        ReaderHeartbeat heartbeat() {
            Map<String, Long> gens = new TreeMap<>();
            syncer.served().forEach((s, g) -> gens.put(s.value(), g.value()));
            Map<String, Long> prepared = new TreeMap<>();
            syncer.preparedGenerations().forEach((s, g) -> prepared.put(s.value(), g.value()));
            return new ReaderHeartbeat(node, "http://" + node, List.copyOf(gens.keySet()), false, Map.of(), 0, 0, 0L,
                    null, null, null, null, Boolean.TRUE, gens, prepared);
        }

        /**
         * What a query of {@code P} reads on this reader: the version the router plans (one catalog read),
         * each planned shard at the generation the query pins (else what it serves). Per file: in how many
         * of the read shards its documents are. A planned shard this reader does not serve at its pinned
         * generation fails the check (the coordinator would leave it out).
         */
        Map<String, Integer> fileCopies() throws IOException {
            return fileCopies(catalog);
        }

        /** As {@link #fileCopies()}, routed with {@code routing} (a coordinator's catalog, maybe an old read). */
        Map<String, Integer> fileCopies(com.pharos.cluster.ShardCatalog routing) throws IOException {
            ShardRouter router = new ShardRouter(() -> List.of(heartbeat()), routing, new ReaderHealth(30), s -> false);
            List<ShardRouter.PlannedShard> planned = router.plan(List.of(P), ShardFilter.NONE).shards();
            List<ShardRouter.PlannedShard> primary = planned.stream().filter(ps -> !ps.alternate()).toList();
            List<ShardRouter.PlannedShard> alternate = planned.stream().filter(ShardRouter.PlannedShard::alternate).toList();
            // as the coordinator: the version the router chose, else its alternate when only that is served as pinned
            List<ShardRouter.PlannedShard> read = servedAsPinned(primary) || alternate.isEmpty() ? primary : alternate;
            Map<String, Integer> out = new TreeMap<>();
            for (ShardRouter.PlannedShard ps : read) {
                Generation served = syncer.served().get(ps.shard());
                assertThat(served).as("shard " + ps.shard() + " served").isNotNull();
                if (ps.generation() != null) {
                    assertThat(served.value()).as("shard " + ps.shard() + " at its pinned generation")
                            .isEqualTo(ps.generation());
                }
                Path dir = cache.active(ps.shard().value()).orElseThrow();
                for (String f : filesIn(dir.resolve("lucene"))) out.merge(f, 1, Integer::sum);
            }
            return out;
        }

        private boolean servedAsPinned(List<ShardRouter.PlannedShard> version) {
            for (ShardRouter.PlannedShard ps : version) {
                Generation served = syncer.served().get(ps.shard());
                if (served == null || (ps.generation() != null && served.value() != ps.generation())) return false;
            }
            return true;
        }

        long downloadsOf(ShardId shard) {
            String prefix = layout.shardPrefix(shard);
            return source.downloads.stream().filter(k -> k.startsWith(prefix)).count();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        layout = new ObjectStoreLayout("pharos");
        store = new InMemoryObjectStore(clock) {
            @Override
            public Optional<String> putIfAbsent(String key, byte[] data) {
                for (String s : failManifestsOf) {
                    if (key.contains(s) && key.contains("/manifests/")) {
                        throw new UncheckedIOException(new IOException("injected manifest write failure: " + key));
                    }
                }
                Optional<String> out = super.putIfAbsent(key, data);
                if (out.isPresent() && key.equals(layout.catalogKey())) recordVersion(data);
                return out;
            }

            @Override
            public Optional<String> putIfMatch(String key, byte[] data, String etag) {
                Optional<String> out = super.putIfMatch(key, data, etag);
                if (out.isPresent() && key.equals(layout.catalogKey())) recordVersion(data);
                return out;
            }
        };
        catalog = new ObjectStoreCatalog(store, layout);
        replicas = new ObjectStoreReplicaSource(store, layout);
        src = tmp.resolve("sources").resolve(P);
    }

    @AfterEach
    void tearDown() {
        for (Writer w : writers) {
            w.loop.close();
            w.lucene.close();
        }
        for (Reader r : readers) r.syncer.close();
    }

    private void recordVersion(byte[] catalogJson) {
        try {
            for (JsonNode p : JSON.readTree(catalogJson).path("projects")) {
                if (!P.equals(p.path("name").asText())) continue;
                int v = p.path("spec").asInt(0);
                if (activeVersions.isEmpty() || activeVersions.getLast() != v) activeVersions.add(v);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------------------- fixtures

    static List<ShardId> ring(int n) {
        List<ShardId> out = new ArrayList<>();
        for (int i = 1; i <= n; i++) out.add(new ShardId(P + "~ring=" + ShardSpec.ringName(i)));
        return out;
    }

    static ShardId shardOf(ShardSpec spec, String rel) {
        return spec.assign(DocKeys.of(P, rel));
    }

    /**
     * {@code n} source files where {@code from} → {@code to} moves at least one file, from at most two of
     * {@code from}'s shards, and every shard of {@code from} has files: some shards are left untouched.
     */
    static List<String> filesFor(ShardSpec from, ShardSpec to, int fromShards) {
        for (int n = 8; n < 60; n++) {
            List<String> files = new ArrayList<>();
            for (int i = 0; i < n; i++) files.add("src/main/java/com/example/p" + i + "/Part" + i + ".java");
            Set<ShardId> sources = new HashSet<>(), holders = new HashSet<>();
            int moved = 0;
            for (String f : files) {
                holders.add(shardOf(from, f));
                if (!shardOf(from, f).equals(shardOf(to, f))) {
                    moved++;
                    sources.add(shardOf(from, f));
                }
            }
            if (moved >= 1 && sources.size() <= 2 && holders.size() == fromShards) return files;
        }
        throw new AssertionError("no file set with untouched shards");
    }

    private void writeFiles(List<String> files) throws IOException {
        for (int i = 0; i < files.size(); i++) {
            Path f = src.resolve(files.get(i));
            Files.createDirectories(f.getParent());
            String pkg = "com.example.p" + i;
            Files.writeString(f, "package " + pkg + ";\n\n/** Part " + i + " of the library. */\npublic class Part" + i
                    + " {\n    /** Computes part " + i + ". */\n    public int f" + i + "(int x) { return x + " + i
                    + "; }\n    public int g" + i + "(int x) { return f" + i + "(x) * 2; }\n}\n", StandardCharsets.UTF_8);
        }
    }

    /** The project split by {@code spec} into {@code shards}, its source registered, published by {@code w}. */
    private void startSplit(Writer w, List<String> files, ShardSpec spec, List<ShardId> shards) throws IOException {
        writeFiles(files);
        catalog.register(new ShardId(P), Optional.of(ShardSource.path(src.toString())));
        int v = catalog.addSpec(Optional.of(P), spec);
        catalog.putProject(P, v, shards);
        w.loop.tick();
        for (ShardId s : shards) assertThat(replicas.latest(s)).as("published " + s).isPresent();
    }

    private Map<ShardId, Generation> latestOf(List<ShardId> shards) throws IOException {
        Map<ShardId, Generation> out = new HashMap<>();
        for (ShardId s : shards) replicas.latest(s).ifPresent(g -> out.put(s, g));
        return out;
    }

    /** filePath of every live document of a Lucene dir. */
    static Set<String> filesIn(Path luceneDir) throws IOException {
        Set<String> out = new LinkedHashSet<>();
        try (FSDirectory d = FSDirectory.open(luceneDir); DirectoryReader r = DirectoryReader.open(d)) {
            StoredFields sf = r.storedFields();
            Bits live = MultiBits.getLiveDocs(r);
            for (int doc = 0; doc < r.maxDoc(); doc++) {
                if (live != null && !live.get(doc)) continue;
                out.add(sf.document(doc).get(DocumentMapper.F_FILE_PATH));
            }
        }
        return out;
    }

    private Set<String> sourcePaths(List<String> files) {
        Set<String> out = new HashSet<>();
        for (String f : files) out.add(src.resolve(f).toAbsolutePath().normalize().toString());
        return out;
    }

    /** Every source file is read exactly once by a query on {@code r}. */
    private void assertEveryFileOnce(Reader r, List<String> files, String when) throws IOException {
        assertEveryFileOnce(r, files, when, catalog);
    }

    private void assertEveryFileOnce(Reader r, List<String> files, String when, com.pharos.cluster.ShardCatalog routing)
            throws IOException {
        Map<String, Integer> copies = r.fileCopies(routing);
        assertThat(copies.keySet()).as(when + ": every file").isEqualTo(sourcePaths(files));
        assertThat(copies.values()).as(when + ": once").allMatch(c -> c == 1);
    }

    private static Set<ShardId> movedFrom(List<String> files, ShardSpec from, ShardSpec to) {
        Set<ShardId> out = new HashSet<>();
        for (String f : files) if (!shardOf(from, f).equals(shardOf(to, f))) out.add(shardOf(from, f));
        return out;
    }

    // ------------------------------------------------------------------------------------ tests

    /**
     * The job writes the new version as staged, builds and publishes it while the active version keeps
     * routing, waits for the reader to have it, and flips with exactly one catalog change of the project's
     * version; a query on the reader reads every file once before, during and after.
     */
    @Test
    void reshardBuildsThenFlipsOnce() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));
        Reader r = new Reader("r1");
        r.sync();
        assertEveryFileOnce(r, files, "before");
        int n = catalog.projectShards().specVersionOf(P).orElseThrow();
        Map<ShardId, Generation> before = latestOf(ring(4));

        int v = catalog.requestReshard(P, RING5, clock.instant());
        assertThat(v).isEqualTo(n + 1);
        a.loop.tick();
        ProjectShards map = catalog.projectShards();
        assertThat(map.specVersionOf(P)).as("still version N").hasValue(n);
        assertThat(map.shardsOf(P)).isEqualTo(ring(4));
        ProjectShards.Staged staged = map.stagedOf(P).orElseThrow();
        assertThat(staged.specVersion()).isEqualTo(v);
        assertThat(staged.shards()).isEqualTo(ring(5));
        assertThat(staged.pinned()).as("published and pinned").isTrue();
        assertThat(latestOf(ring(4))).as("no LATEST moved before the flip").isEqualTo(before);
        assertThat(map.pinsOf(P)).as("the active version pinned at what it serves")
                .isEqualTo(Map.of(ring(4).get(0), before.get(ring(4).get(0)).value(),
                        ring(4).get(1), before.get(ring(4).get(1)).value(),
                        ring(4).get(2), before.get(ring(4).get(2)).value(),
                        ring(4).get(3), before.get(ring(4).get(3)).value()));
        assertEveryFileOnce(r, files, "staged, reader not synced");

        r.sync(); // materialises the rewritten shards, activates the new one; the active version stays
        assertEveryFileOnce(r, files, "staged, reader synced");
        for (ShardId s : movedFrom(files, RING4, RING5)) {
            assertThat(r.syncer.served().get(s)).as("rewritten " + s + " not served before the flip").isEqualTo(before.get(s));
            assertThat(r.syncer.preparedGenerations().get(s).value()).isEqualTo(staged.generations().get(s));
        }

        a.loop.tick(); // the reader has it: flip
        map = catalog.projectShards();
        assertThat(map.specVersionOf(P)).hasValue(v);
        assertThat(map.shardsOf(P)).isEqualTo(ring(5));
        assertThat(map.stagedOf(P)).isEmpty();
        assertThat(map.reshardRequestOf(P)).isEmpty();
        assertThat(map.pinsOf(P)).isEqualTo(staged.generations());
        assertThat(map.previousOf(P).orElseThrow().shards()).isEqualTo(ring(4));
        for (ShardId s : ring(5)) {
            assertThat(replicas.latest(s).orElseThrow().value()).as("LATEST of " + s + " at its pin")
                    .isEqualTo(staged.generations().get(s));
        }
        assertEveryFileOnce(r, files, "flipped, reader not synced (reads the previous version)");
        r.sync();
        assertEveryFileOnce(r, files, "flipped, reader synced");

        // later rounds change nothing; a source change is one normal run of the new version
        for (int i = 0; i < 3; i++) {
            clock.advance(10);
            a.loop.tick();
        }
        Path f = src.resolve(files.getFirst());
        Files.writeString(f, Files.readString(f).replace("Part 0 of", "Part zero of"));
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        clock.advance(10);
        a.loop.tick();
        map = catalog.projectShards();
        assertThat(map.pinsOf(P)).as("the run published every shard: pins dropped").isEmpty();
        long runs = ring(5).stream().map(s -> {
            try {
                return replicas.manifest(s, replicas.latest(s).orElseThrow()).projectGeneration();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).distinct().count();
        assertThat(runs).as("one index run across the new version").isEqualTo(1);
        r.sync();
        assertEveryFileOnce(r, files, "after the next run");
        assertThat(activeVersions).as("the project's version changed exactly once").containsExactly(n, v);
    }

    /**
     * 4 → 5 ring shards: only the files that move are parsed again; the shards that neither gain nor lose
     * files keep their generations byte-identical, the new version references them, and the reader never
     * downloads anything of them again.
     */
    @Test
    void ringAddReindexesOnlyMovedFiles() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));
        Reader r = new Reader("r1");
        r.sync();
        Set<ShardId> sources = movedFrom(files, RING4, RING5);
        List<ShardId> untouched = ring(4).stream().filter(s -> !sources.contains(s)).toList();
        assertThat(untouched).as("the fixture leaves shards untouched").isNotEmpty();
        Set<Path> moved = new HashSet<>();
        for (String f : files) if (!shardOf(RING4, f).equals(shardOf(RING5, f))) moved.add(src.resolve(f));
        Map<ShardId, Generation> before = latestOf(ring(4));
        Map<ShardId, byte[]> manifests = new HashMap<>();
        for (ShardId s : untouched) {
            manifests.put(s, store.get(layout.manifestKey(s, before.get(s))).orElseThrow().data());
        }
        int downloads = r.source.downloads.size();
        Map<ShardId, Long> untouchedDownloads = new HashMap<>();
        for (ShardId s : untouched) untouchedDownloads.put(s, r.downloadsOf(s));
        a.parser.parsed.clear();

        catalog.requestReshard(P, RING5, clock.instant());
        a.loop.tick();
        r.sync();
        a.loop.tick();
        r.sync();

        assertThat(catalog.projectShards().shardsOf(P)).isEqualTo(ring(5));
        assertThat(new HashSet<>(a.parser.parsed)).as("only the moved files were parsed").isEqualTo(moved);
        assertThat(a.parser.parsed).hasSize(moved.size());
        // the job record's progress (status): every moved file written, every shard settled
        JsonNode job = JSON.readTree(store.get(layout.reshardJobKey(P)).orElseThrow().data());
        assertThat(job.path("filesTotal").asInt()).isEqualTo(moved.size());
        assertThat(job.path("filesDone").asInt()).isEqualTo(moved.size());
        assertThat(job.path("affected").size() + job.path("unaffected").size()).isEqualTo(5);
        assertThat(job.path("unaffected").size()).isEqualTo(untouched.size());
        Map<ShardId, Long> pins = catalog.projectShards().pinsOf(P);
        for (ShardId s : untouched) {
            assertThat(replicas.latest(s)).as(s + " keeps its generation").contains(before.get(s));
            assertThat(store.get(layout.manifestKey(s, before.get(s))).orElseThrow().data())
                    .as(s + " byte-identical").isEqualTo(manifests.get(s));
            assertThat(pins.get(s)).as("the new version references " + s + " as it was").isEqualTo(before.get(s).value());
            assertThat(r.syncer.served().get(s)).as("the reader serves " + s + " as before").isEqualTo(before.get(s));
            assertThat(r.downloadsOf(s)).as("nothing of " + s + " downloaded again").isEqualTo(untouchedDownloads.get(s));
        }
        for (ShardId s : sources) {
            assertThat(replicas.latest(s).orElseThrow().value()).as(s + " rewritten").isGreaterThan(before.get(s).value());
        }
        assertThat(r.source.downloads.size()).as("the rewritten and new shards only").isGreaterThan(downloads);
        assertThat(r.source.downloads.subList(downloads, r.source.downloads.size()))
                .as("the project graph is the same run's: not downloaded again")
                .noneMatch(k -> k.contains("/projects/" + P + "/graph/"));
        assertEveryFileOnce(r, files, "after");
    }

    /**
     * Review Focus 1: the publish of version N+1 is interrupted; N keeps serving and the half-built N+1 is
     * invisible; the writer resumes, reusing what it published; it then dies, and another writer takes
     * the project over and finishes the reshard from the shards already published.
     */
    @Test
    void interruptedReshardKeepsVersionNServingAndResumes() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));
        Reader r = new Reader("r1");
        r.sync();
        int n = catalog.projectShards().specVersionOf(P).orElseThrow();
        Map<ShardId, Generation> before = latestOf(ring(4));

        // the publish of s05 (published last) fails: the rewritten shards are staged, the version is not
        failManifestsOf.add(layout.shardPrefix(ring(5).get(4)));
        int v = catalog.requestReshard(P, RING5, clock.instant());
        a.loop.tick();
        ProjectShards map = catalog.projectShards();
        assertThat(map.specVersionOf(P)).hasValue(n);
        assertThat(map.stagedOf(P).orElseThrow().pinned()).as("half-built: not pinned").isFalse();
        assertThat(latestOf(ring(4))).isEqualTo(before);
        Map<ShardId, Long> firstAttempt = jobGenerations();
        assertThat(firstAttempt).as("the rewritten shards were published").isNotEmpty();
        r.sync();
        assertThat(r.syncer.served()).as("the reader serves N only").isEqualTo(before);
        assertThat(r.syncer.preparedGenerations()).as("nothing of the half-built version").isEmpty();
        assertEveryFileOnce(r, files, "interrupted");

        // the next attempt (same writer) reuses what it published and completes the version
        failManifestsOf.clear();
        heartbeats = List.of(r.heartbeat()); // the reader lags: the flip waits
        clock.advance(10);
        a.loop.tick();
        map = catalog.projectShards();
        assertThat(map.stagedOf(P).orElseThrow().pinned()).isTrue();
        Map<ShardId, Long> pinnedNow = map.stagedOf(P).orElseThrow().generations();
        firstAttempt.forEach((s, g) -> assertThat(pinnedNow.get(s)).as(s + " reused").isEqualTo(g));
        assertThat(map.specVersionOf(P)).hasValue(n);

        // the writer dies; its leases run out; another writer takes the project and flips it
        Writer b = new Writer("node-b");
        clock.advance(TTL.toSeconds() + 5);
        b.loop.tick();
        assertThat(catalog.projectShards().specVersionOf(P)).as("waits for the reader").hasValue(n);
        assertEveryFileOnce(r, files, "taken over, not flipped");
        r.sync();
        clock.advance(10);
        b.loop.tick();
        map = catalog.projectShards();
        assertThat(map.specVersionOf(P)).hasValue(v);
        assertThat(map.pinsOf(P)).as("the version A published").isEqualTo(jobGenerations());
        assertThat(b.parser.parsed).as("B rebuilt nothing: the version was published").isEmpty();
        r.sync();
        assertEveryFileOnce(r, files, "after");
        assertThat(activeVersions).containsExactly(n, v);

        // B's next run continues from the new version (its restore takes the pinned generations)
        Path f = src.resolve(files.getFirst());
        Files.writeString(f, Files.readString(f).replace("Part 0 of", "Part zero of"));
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        clock.advance(10);
        b.loop.tick();
        b.loop.tick();
        assertThat(catalog.projectShards().pinsOf(P)).isEmpty();
        r.sync();
        assertEveryFileOnce(r, files, "after B's run");
    }

    /**
     * Review Focus 5: a reshard calls the embedding provider for nothing, on the writer that indexed the
     * project and on a writer that takes a reshard over with the restored writer state.
     */
    @Test
    void reshardReembedsNothing() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));
        assertThat(a.embedder.calls.get()).as("the first run embedded").isPositive();

        int calls = a.embedder.calls.get();
        catalog.requestReshard(P, RING5, clock.instant());
        a.loop.tick();
        assertThat(catalog.projectShards().shardsOf(P)).isEqualTo(ring(5));
        assertThat(a.embedder.calls.get() - calls).as("provider calls during the reshard").isZero();
        assertThat(a.parser.parsed).as("the moved files were parsed again").isNotEmpty();

        // a takeover: B restores the project (cache included) and runs the next reshard
        a.loop.close();
        catalog.requestReshard(P, RING6, clock.instant());
        Writer b = new Writer("node-b");
        clock.advance(TTL.toSeconds() + 5);
        b.loop.tick();
        clock.advance(10);
        b.loop.tick();
        assertThat(catalog.projectShards().shardsOf(P)).isEqualTo(ring(6));
        assertThat(b.parser.parsed).as("B re-indexed the files moving to s06").isNotEmpty();
        assertThat(b.embedder.calls.get()).as("provider calls on B: the restored cache served every text").isZero();
        Reader r = new Reader("r1");
        r.sync();
        assertEveryFileOnce(r, files, "after two reshards");
    }

    /**
     * One reshard of a project at a time: a request for another spec while one is open is refused (the
     * same spec again is the same request), also through {@code pharos reshard}; and while another writer
     * holds the project's reshard lease, the job does not run.
     */
    @Test
    void concurrentReshardsOfOneProjectRefused() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));

        int v = catalog.requestReshard(P, RING5, clock.instant());
        assertThat(catalog.requestReshard(P, RING5, clock.instant())).as("the same request").isEqualTo(v);
        assertThatThrownBy(() -> catalog.requestReshard(P, RING6, clock.instant()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("in progress");
        assertThat(cli(P, "--spec", RING6.toString())).as("pharos reshard refuses it").isEqualTo(1);
        assertThat(cli(P, "--status")).isZero();

        // another writer runs this project's reshard (holds its lease): this writer's job stays out
        LeaseManager x = new LeaseManager(store, layout, "node-x", TTL, LeaseManager.BucketNow.NONE, clock,
                id -> layout.reshardLeaseKey(id.value()));
        Lease held = x.tryAcquire(ShardId.ofProject(P)).orElseThrow();
        clock.advance(10);
        a.loop.tick();
        assertThat(catalog.projectShards().stagedOf(P)).as("refused: nothing staged").isEmpty();
        assertThat(a.parser.parsed.size()).as("nothing built").isEqualTo(files.size());
        assertThat(a.loop.reshardJob().orElseThrow().step(P, new ShardPlan(P, RING4, ring(4)),
                ShardSource.path(src.toString()), src, true, com.pharos.indexer.IndexAbort.none()))
                .isEqualTo(ReshardJob.Step.REFUSED);

        x.release(held);
        clock.advance(10);
        a.loop.tick();
        assertThat(catalog.projectShards().specVersionOf(P)).hasValue(v);
        x.close();

        // with nothing open, cancelling is refused too
        assertThat(cli(P, "--cancel")).isEqualTo(1);
    }

    /**
     * The retired version is GC'd after the retention window: a {@code [project]} project split into a
     * ring keeps serving its plain shard (the previous version) until then; afterwards the plain shard's
     * objects are deleted and it leaves the catalog. A cancelled (abandoned) reshard's staged shards go the
     * same way.
     */
    @Test
    void retiredVersionGcAfterRetention() throws IOException {
        List<String> files = filesFor(RING2, RING3, 2);
        writeFiles(files);
        ShardId plain = new ShardId(P);
        catalog.register(plain, Optional.of(ShardSource.path(src.toString())));
        Writer a = new Writer("node-a");
        a.loop.tick();
        assertThat(replicas.latest(plain)).isPresent();
        Reader r = new Reader("r1");
        r.sync();

        catalog.requestReshard(P, RING2, clock.instant());
        a.loop.tick();
        r.sync();
        a.loop.tick();
        ProjectShards map = catalog.projectShards();
        assertThat(map.shardsOf(P)).isEqualTo(ring(2));
        assertThat(map.previousOf(P).orElseThrow().shards()).containsExactly(plain);
        r.sync();
        assertThat(r.syncer.served()).as("the retired [project] shard is still served").containsKey(plain);
        assertEveryFileOnce(r, files, "split, within retention");

        clock.advance(RETENTION.toSeconds() - 60);
        a.loop.tick();
        assertThat(store.list(layout.shardPrefix(plain))).as("kept within the retention window").isNotEmpty();

        clock.advance(120);
        a.loop.tick();
        assertThat(store.list(layout.shardPrefix(plain))).as("the retired shard's objects are gone").isEmpty();
        map = catalog.projectShards();
        assertThat(map.previousOf(P)).isEmpty();
        assertThat(map.listed()).doesNotContain(plain);
        r.sync();
        assertThat(r.syncer.served()).containsOnlyKeys(ring(2));
        assertEveryFileOnce(r, files, "after retention");

        // an abandoned reshard: staged, published, cancelled; GC'd after the window
        heartbeats = List.of(r.heartbeat()); // the reader lags: the job waits
        catalog.requestReshard(P, RING3, clock.instant());
        clock.advance(10);
        a.loop.tick();
        ShardId s03 = ring(3).get(2);
        assertThat(catalog.projectShards().stagedOf(P).orElseThrow().pinned()).isTrue();
        assertThat(store.list(layout.shardPrefix(s03))).isNotEmpty();
        assertThat(catalog.cancelReshard(P, clock.instant())).as("pharos reshard --cancel").isTrue();
        clock.advance(10);
        a.loop.tick();
        assertThat(catalog.projectShards().shardsOf(P)).as("cancelled: not flipped").isEqualTo(ring(2));
        clock.advance(RETENTION.toSeconds() + 10);
        a.loop.tick();
        assertThat(store.list(layout.shardPrefix(s03))).as("the abandoned shard's objects are gone").isEmpty();
        map = catalog.projectShards();
        assertThat(map.stagedOf(P)).isEmpty();
        assertThat(map.listed()).doesNotContain(s03);
        r.sync();
        assertEveryFileOnce(r, files, "after the abandoned reshard");
    }

    /** A catalog frozen at one read: a coordinator whose cached catalog predates a change. */
    static com.pharos.cluster.ShardCatalog frozen(ProjectShards snapshot) {
        return new com.pharos.cluster.ShardCatalog() {
            @Override public List<ShardId> shards() { return snapshot.listed(); }
            @Override public ProjectShards projectShards() { return snapshot; }
        };
    }

    /**
     * Fix round 1, C1: an index run of the active version published while the staged version is pinned
     * (a run racing the reshard, on another node) never makes the flip serve a moved file twice: its
     * generations skip the staged numbers, so no staged manifest is overwritten, and the flip finds the
     * active version changed, withdraws the staged generations and builds them again from the new run.
     * The writer holding the project starts no index run of it while the reshard is pinned.
     */
    @Test
    void aPublishDuringThePinnedWindowIsRebuiltNotServedTwice() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));
        Reader r = new Reader("r1");
        r.sync();
        int v = catalog.requestReshard(P, RING5, clock.instant());
        a.loop.tick(); // staged, published, pinned; the reader lags: waits
        ProjectShards.Staged staged = catalog.projectShards().stagedOf(P).orElseThrow();
        assertThat(staged.pinned()).isTrue();
        Map<ShardId, byte[]> stagedManifests = new HashMap<>();
        for (Map.Entry<ShardId, Long> e : staged.generations().entrySet()) {
            stagedManifests.put(e.getKey(), store.get(layout.manifestKey(e.getKey(), new Generation(e.getValue())))
                    .orElseThrow().data());
        }

        // a source change: the holder runs no index of the pinned project
        Path f = src.resolve(files.getFirst());
        Files.writeString(f, Files.readString(f).replace("Part 0 of", "Part zero of"));
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        Map<ShardId, Generation> beforeRun = latestOf(ring(4));
        clock.advance(10);
        a.loop.tick();
        assertThat(latestOf(ring(4))).as("no index run while the reshard is pinned").isEqualTo(beforeRun);

        // ... but a run racing it from another node publishes the active version anyway
        Writer rogue = new Writer("node-rogue", Duration.ZERO, true);
        rogue.manager.index(src, P, false, false, true, false, com.pharos.indexer.ProgressListener.SILENT,
                com.pharos.indexer.IndexAbort.none(), null, new ShardPlan(P, RING4, ring(4)));
        for (Map.Entry<ShardId, byte[]> e : stagedManifests.entrySet()) {
            assertThat(store.get(layout.manifestKey(e.getKey(), new Generation(staged.generations().get(e.getKey()))))
                    .orElseThrow().data()).as("staged manifest of " + e.getKey() + " untouched").isEqualTo(e.getValue());
        }
        heartbeats = List.of(); // no reader to wait for: the next step flips if it can
        clock.advance(10);
        a.loop.tick();
        ProjectShards map = catalog.projectShards();
        assertThat(map.specVersionOf(P)).as("not flipped onto the stale version").isNotEqualTo(java.util.OptionalInt.of(v));
        for (int i = 0; i < 4 && !map.specVersionOf(P).equals(java.util.OptionalInt.of(v)); i++) {
            clock.advance(10);
            a.loop.tick();
            map = catalog.projectShards();
        }
        assertThat(map.specVersionOf(P)).as("rebuilt from the new run and flipped").hasValue(v);
        clock.advance(ACTIVATION_HOLD.toSeconds() + 1);
        r.sync();
        assertEveryFileOnce(r, files, "after the rebuilt flip");
        Path changed = src.resolve(files.getFirst()).toAbsolutePath().normalize();
        ShardId holder = shardOf(RING5, files.getFirst());
        assertThat(filesIn(r.cache.active(holder.value()).orElseThrow().resolve("lucene"))).contains(changed.toString());
    }

    /**
     * Fix round 1, I1: the reshard lease lives only as long as the project's work unit. The writer waiting
     * for the flip loses the unit (it stops renewing, another writer takes it over); the new owner
     * completes the reshard, and the old one, back, writes nothing.
     */
    @Test
    void aWriterThatLostTheUnitWritesNothingOfTheReshard() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));
        Reader r = new Reader("r1");
        r.sync();
        int v = catalog.requestReshard(P, RING5, clock.instant());
        a.loop.tick();
        assertThat(catalog.projectShards().stagedOf(P).orElseThrow().pinned()).isTrue();

        Writer b = new Writer("node-b");
        clock.advance(TTL.toSeconds() + 5); // A is stuck: its leases run out
        b.loop.tick();
        assertThat(b.loop.heldShards()).containsAll(ring(4));
        assertThat(catalog.projectShards().specVersionOf(P)).as("B waits for the reader").isNotEqualTo(
                java.util.OptionalInt.of(v));

        byte[] catalogBefore = store.get(layout.catalogKey()).orElseThrow().data();
        byte[] jobBefore = store.get(layout.reshardJobKey(P)).orElseThrow().data();
        a.loop.tick(); // A is back: it lost the unit, and its reshard lease with it
        assertThat(a.loop.heldShards()).isEmpty();
        assertThat(a.loop.reshardJob().orElseThrow().leases().held()).isEmpty();
        assertThat(store.get(layout.catalogKey()).orElseThrow().data()).as("A wrote nothing to the catalog")
                .isEqualTo(catalogBefore);
        assertThat(store.get(layout.reshardJobKey(P)).orElseThrow().data()).as("nor to the job").isEqualTo(jobBefore);
        assertThat(a.loop.reshardJob().orElseThrow().step(P, new ShardPlan(P, RING4, ring(4)),
                ShardSource.path(src.toString()), src, true, com.pharos.indexer.IndexAbort.none(), shard -> {
                    throw new com.pharos.cluster.PublishConflictException("node-a does not hold " + shard);
                })).as("B holds the reshard lease").isEqualTo(ReshardJob.Step.REFUSED);

        r.sync();
        clock.advance(10);
        b.loop.tick();
        assertThat(catalog.projectShards().specVersionOf(P)).hasValue(v);
        clock.advance(ACTIVATION_HOLD.toSeconds() + 1);
        r.sync();
        assertEveryFileOnce(r, files, "after B's flip");
    }

    /**
     * Fix round 2: a writer that takes a project over while its reshard is pinned, with a source commit
     * pending, flips the reshard first (it needs neither a local build nor the source), then indexes the
     * commit on the new version.
     */
    @Test
    void aTakeoverWithAPendingCommitFlipsThenIndexesIt() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));
        Reader r = new Reader("r1");
        r.sync();
        int v = catalog.requestReshard(P, RING5, clock.instant());
        a.loop.tick();
        assertThat(catalog.projectShards().stagedOf(P).orElseThrow().pinned()).isTrue();

        Path f = src.resolve(files.getFirst());
        Files.writeString(f, Files.readString(f).replace("Part 0 of", "Part zero of"));
        Files.setLastModifiedTime(f, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        Writer b = new Writer("node-b");
        clock.advance(TTL.toSeconds() + 5); // A is gone
        b.loop.tick();
        assertThat(b.loop.heldShards()).containsAll(ring(4));
        r.sync(); // the reader prepares the new version
        for (int i = 0; i < 3 && !catalog.projectShards().specVersionOf(P).equals(java.util.OptionalInt.of(v)); i++) {
            clock.advance(10);
            b.loop.tick();
        }
        assertThat(catalog.projectShards().specVersionOf(P)).as("flipped despite the pending commit").hasValue(v);
        for (int i = 0; i < 4 && !catalog.projectShards().pinsOf(P).isEmpty(); i++) {
            clock.advance(10);
            b.loop.tick();
        }
        assertThat(catalog.projectShards().pinsOf(P)).as("the commit was indexed on the new version").isEmpty();
        r.sync();
        clock.advance(ACTIVATION_HOLD.toSeconds() + 1);
        r.sync();
        assertEveryFileOnce(r, files, "after the flip and the commit");
        ShardId holder = shardOf(RING5, files.getFirst());
        try (FSDirectory d = FSDirectory.open(r.cache.active(holder.value()).orElseThrow().resolve("lucene"));
             DirectoryReader dr = DirectoryReader.open(d)) {
            boolean found = false;
            StoredFields sf = dr.storedFields();
            for (int doc = 0; doc < dr.maxDoc() && !found; doc++) {
                String body = sf.document(doc).get("javadoc");
                found = body != null && body.contains("Part zero of");
            }
            assertThat(found).as("the committed text is served").isTrue();
        }
    }

    /**
     * Fix round 1, I2: right after the flip a reader keeps serving the rewritten shards at their previous
     * generations for the activation hold, so a coordinator still routing the previous version (its catalog
     * read predates the flip) reads it complete; once the hold is over the reader serves the new version.
     */
    @Test
    void aCoordinatorWithThePreFlipCatalogStillReadsEveryFile() throws IOException {
        List<String> files = filesFor(RING4, RING5, 4);
        Writer a = new Writer("node-a");
        startSplit(a, files, RING4, ring(4));
        Reader r = new Reader("r1");
        r.sync();
        Map<ShardId, Generation> before = latestOf(ring(4));
        catalog.requestReshard(P, RING5, clock.instant());
        a.loop.tick();
        r.sync();
        ProjectShards preFlip = catalog.projectShards(); // a coordinator's last read before the flip
        a.loop.tick();
        assertThat(catalog.projectShards().shardsOf(P)).isEqualTo(ring(5));

        r.sync(); // within the hold
        Set<ShardId> rewritten = movedFrom(files, RING4, RING5);
        for (ShardId s : rewritten) {
            assertThat(r.syncer.served().get(s)).as(s + " keeps its previous generation").isEqualTo(before.get(s));
        }
        assertEveryFileOnce(r, files, "stale coordinator, within the hold", frozen(preFlip));
        assertEveryFileOnce(r, files, "fresh coordinator, within the hold");

        clock.advance(ACTIVATION_HOLD.toSeconds() + 1);
        r.sync();
        Map<ShardId, Long> pins = catalog.projectShards().pinsOf(P);
        for (ShardId s : rewritten) {
            assertThat(r.syncer.served().get(s).value()).as(s + " at the new version").isEqualTo(pins.get(s));
        }
        assertEveryFileOnce(r, files, "fresh coordinator, after the hold");
    }

    /** The job's record of the generations it published. */
    private Map<ShardId, Long> jobGenerations() throws IOException {
        JsonNode n = JSON.readTree(store.get(layout.reshardJobKey(P)).orElseThrow().data());
        Map<ShardId, Long> out = new HashMap<>();
        n.path("generations").fields().forEachRemaining(e -> out.put(new ShardId(e.getKey()), e.getValue().asLong()));
        return out;
    }

    /** {@code pharos reshard} against this test's bucket. */
    private int cli(String... args) {
        IndexConfig config = IndexConfig.defaults();
        ClusterConfig c = new ClusterConfig();
        c.setRole(NodeRole.WRITER);
        c.setBackend(BackendKind.OBJECT_STORE);
        c.getObjectStore().setBucket("test");
        c.getObjectStore().setPrefix("pharos");
        config.setCluster(c);
        ReshardCommand cmd = new ReshardCommand(config, cc -> new InMemoryObjectStoreView(store));
        return new picocli.CommandLine(cmd).execute(args);
    }

    /** The test's store, not closed by the command. */
    static final class InMemoryObjectStoreView implements com.pharos.cluster.objectstore.ObjectStore {
        private final InMemoryObjectStore inner;

        InMemoryObjectStoreView(InMemoryObjectStore inner) { this.inner = inner; }

        @Override public Optional<com.pharos.cluster.objectstore.StoredObject> get(String key) throws IOException {
            return inner.get(key);
        }
        @Override public void download(String key, Path target) throws IOException { inner.download(key, target); }
        @Override public void downloadRange(String key, long offset, long length, Path target) throws IOException {
            inner.downloadRange(key, offset, length, target);
        }
        @Override public InputStream openRange(String key, long offset, long length) throws IOException {
            return inner.openRange(key, offset, length);
        }
        @Override public String put(String key, Path source) throws IOException { return inner.put(key, source); }
        @Override public String put(String key, byte[] data) throws IOException { return inner.put(key, data); }
        @Override public Optional<String> putIfAbsent(String key, byte[] data) throws IOException {
            return inner.putIfAbsent(key, data);
        }
        @Override public Optional<String> putIfMatch(String key, byte[] data, String etag) throws IOException {
            return inner.putIfMatch(key, data, etag);
        }
        @Override public List<ObjectInfo> list(String prefix) throws IOException { return inner.list(prefix); }
        @Override public void delete(String key) throws IOException { inner.delete(key); }
        @Override public void close() { }
    }
}
