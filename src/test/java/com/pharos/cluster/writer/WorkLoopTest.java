package com.pharos.cluster.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.PublishException;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.objectstore.GenerationGc;
import com.pharos.cluster.objectstore.InMemoryObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.ObjectStorePublisher;
import com.pharos.cluster.objectstore.ObjectStoreReplicaSource;
import com.pharos.cluster.objectstore.ObjectStoreWriterStateStore;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.CountingEmbedder;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The work queue with in-JVM writers on one {@link InMemoryObjectStore}: a fake clock drives the
 * bucket's LastModified and the writers' own clocks, and the tests call {@link WorkLoop#tick} by
 * hand (no threads). Embeddings come from a counting fake, never ONNX; git sources are local bare
 * repositories.
 */
class WorkLoopTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Duration HEARTBEAT = Duration.ofSeconds(10);
    private static final Duration MIN_HOLD = Duration.ofSeconds(120);

    static final long GIB = 1L << 30;
    /** 100 GiB disks: threshold max(5 GiB, 10 %) = 10 GiB, back from low above 11 GiB. */
    static final DiskGuard.Settings DISK = new DiskGuard.Settings(5 * GIB, 10, GIB, Duration.ofSeconds(15));
    static final long THRESHOLD = 10 * GIB;

    /** A disk whose free space the test sets; every path is on it. */
    static final class FakeDisk implements DiskGuard.Probe {
        final java.util.concurrent.atomic.AtomicLong free = new java.util.concurrent.atomic.AtomicLong(50 * GIB);
        volatile java.util.function.LongSupplier dynamic;
        @Override public Object storeOf(Path path) { return "disk"; }
        @Override public DiskGuard.Space space(Path path) {
            java.util.function.LongSupplier d = dynamic;
            return new DiskGuard.Space(d != null ? d.getAsLong() : free.get(), 100 * GIB);
        }
        void set(long bytes) { free.set(bytes); }
    }

    /** One clock for the bucket and every writer; only the test moves it. */
    static final class TestClock extends Clock {
        final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-21T10:00:00Z"));
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId z) { return this; }
        @Override public Instant instant() { return now.get(); }
        void advance(long seconds) { now.updateAndGet(i -> i.plusSeconds(seconds)); }
    }

    @TempDir Path tmp;
    private final TestClock clock = new TestClock();
    private com.pharos.cluster.objectstore.StallingObjectStore store;
    private ObjectStoreLayout layout;
    private ObjectStoreCatalog catalog;
    private ObjectStoreReplicaSource replicas;
    private final List<Writer> writers = new ArrayList<>();
    private final Map<ShardId, Path> sourceDirs = new HashMap<>();
    private int writerSeq;

    /** One writer node: its own index dir, registry, work dir and embedder; the shared bucket. */
    final class Writer {
        final String node;
        final LuceneIndexer lucene;
        final ProjectRegistry registry;
        final CountingEmbedder embedder = new CountingEmbedder();
        final WorkQueueLeaseGuard guard = new WorkQueueLeaseGuard();
        final ProjectIndexManager manager;
        /** The same node's index without a publisher: what a Phase 1-3 hand run left behind. */
        final ProjectIndexManager localOnly;
        final WorkLoop loop;
        final DiskGuard disk;

        Writer(String node) {
            this(node, null);
        }

        /** @param probe the disk guard's probe; null runs without a guard */
        Writer(String node, DiskGuard.Probe probe) {
            this(node, probe, SourceCheckout::new);
        }

        /** @param checkouts the checkout for the writer's work dir */
        Writer(String node, DiskGuard.Probe probe, java.util.function.Function<Path, SourceCheckout> checkouts) {
            this(node, probe, checkouts, new JavaCodeParser());
        }

        /** @param parser the code parser of its index runs */
        Writer(String node, DiskGuard.Probe probe, java.util.function.Function<Path, SourceCheckout> checkouts,
               com.pharos.parser.CodeParser parser) {
            this.node = node;
            Path home = tmp.resolve("w" + (++writerSeq) + "-" + node);
            IndexConfig config = IndexConfig.defaults();
            config.setIndexDir(home.resolve("indexes"));
            registry = ProjectRegistry.at(home.resolve("registry.json"));
            lucene = new LuceneIndexer(config);
            ObjectStoreWriterStateStore state = new ObjectStoreWriterStateStore(store, layout, replicas, node);
            ObjectStorePublisher publisher = new ObjectStorePublisher(store, layout, catalog,
                    new GenerationGc(store, layout, 5, Duration.ofMinutes(10)), node, null, guard, state);
            ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
                @Override
                public synchronized List<String> incorporate(Path root, ProjectMeta meta, MavenPomReader.PomInfo pom) {
                    return List.of();
                }
            };
            manager = new ProjectIndexManager(config, lucene, registry, List.of(embedder), noopModules,
                    List.of(parser), publisher);
            localOnly = new ProjectIndexManager(config, lucene, registry, List.of(embedder), noopModules,
                    List.of(new JavaCodeParser()));
            disk = probe == null ? null : new DiskGuard(home.resolve("indexes"), List.of(home.resolve("work")),
                    DISK, probe);
            loop = new WorkLoop(new WorkLoop.Settings(node, TTL, HEARTBEAT, Duration.ofSeconds(60), MIN_HOLD, true, false),
                    store, layout, catalog, replicas, state, manager, lucene, registry,
                    checkouts.apply(home.resolve("work")), clock, disk);
            guard.attach(loop);
            writers.add(this);
        }

        Set<ShardId> held() { return Set.copyOf(loop.heldShards()); }

        int docsUnder(ShardId shard, String pathPrefix) throws IOException {
            try (DirectoryReader r = DirectoryReader.open(FSDirectory.open(
                    lucene.getProjectIndexDir(shard.value()).resolve("lucene")))) {
                return new IndexSearcher(r).count(new PrefixQuery(new Term(DocumentMapper.F_FILE_PATH, pathPrefix)));
            }
        }

        int docs(ShardId shard) throws IOException {
            try (DirectoryReader r = DirectoryReader.open(FSDirectory.open(
                    lucene.getProjectIndexDir(shard.value()).resolve("lucene")))) {
                return r.numDocs();
            }
        }
    }

    @BeforeEach
    void setUp() {
        store = new com.pharos.cluster.objectstore.StallingObjectStore(clock);
        layout = new ObjectStoreLayout("pharos");
        catalog = new ObjectStoreCatalog(store, layout);
        replicas = new ObjectStoreReplicaSource(store, layout);
        logs = new ch.qos.logback.core.read.ListAppender<>();
        logs.start();
        writerLogger().addAppender(logs);
    }

    private ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> logs;

    private static ch.qos.logback.classic.Logger writerLogger() {
        return (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("com.pharos.cluster.writer");
    }

    private List<String> warnings() {
        synchronized (logs.list) {
            return logs.list.stream().filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
                    .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage).toList();
        }
    }

    @AfterEach
    void tearDown() {
        store.release();
        writerLogger().detachAppender(logs);
        for (Writer w : writers) {
            w.loop.close();
            w.lucene.close();
        }
    }

    // ----------------------------------------------------------------------------- helpers

    /** A small Java project with a path source registered in the catalog. */
    private ShardId pathShard(String name) throws IOException {
        ShardId shard = new ShardId(name);
        Path dir = tmp.resolve("sources").resolve(name);
        Path pkg = dir.resolve("src/main/java/com/example/" + name.replace('-', '_'));
        Files.createDirectories(pkg);
        Files.writeString(pkg.resolve("Service.java"), "package com.example." + name.replace('-', '_') + ";\n\n"
                + "public class Service {\n    public int one(int x) { return x + 1; }\n"
                + "    public int two(int x) { return one(x) * 2; }\n}\n");
        sourceDirs.put(shard, dir);
        catalog.register(shard, Optional.of(ShardSource.path(dir.toString())));
        return shard;
    }

    private List<ShardId> pathShards(int n) throws IOException {
        List<ShardId> out = new ArrayList<>();
        for (int i = 0; i < n; i++) out.add(pathShard("shard-" + i));
        return out;
    }

    /** A real change: a new method (new method text, changed class text), with a newer mtime. */
    private void change(ShardId shard, String method) throws IOException {
        Path f;
        try (var s = Files.walk(sourceDirs.get(shard))) {
            f = s.filter(p -> p.toString().endsWith("Service.java")).findFirst().orElseThrow();
        }
        String src = Files.readString(f);
        int end = src.lastIndexOf('}');
        Files.writeString(f, src.substring(0, end) + "    public int " + method + "(int x) { return x * "
                + method.length() + "; }\n}\n");
        Files.setLastModifiedTime(f, FileTime.fromMillis(Files.getLastModifiedTime(f).toMillis() + 2_000));
    }

    /** {@code n} rounds: the clock moves {@code seconds}, then each writer ticks. */
    private void rounds(int n, long seconds, Writer... ws) {
        for (int i = 0; i < n; i++) {
            clock.advance(seconds);
            for (Writer w : ws) w.loop.tick();
        }
    }

    private Optional<Generation> latest(ShardId shard) throws IOException {
        return replicas.latest(shard);
    }

    private JsonNode lease(ShardId shard) throws IOException {
        return JSON.readTree(store.get(layout.leaseKey(shard)).orElseThrow().data());
    }

    private Optional<JsonNode> heartbeatOf(String node) throws IOException {
        Optional<com.pharos.cluster.objectstore.StoredObject> o = store.get(layout.writerKey(node));
        return o.isEmpty() ? Optional.empty() : Optional.of(JSON.readTree(o.get().data()));
    }

    // ------------------------------------------------------------------------------- tests

    @Test
    void oneWriterTakesAllShards() throws IOException {
        List<ShardId> shards = pathShards(3);
        Writer a = new Writer("node-a");

        a.loop.tick();

        assertThat(a.held()).containsExactlyInAnyOrderElementsOf(shards);
        for (ShardId s : shards) {
            assertThat(latest(s)).contains(Generation.FIRST);
            assertThat(a.loop.publishes(s)).isEqualTo(1);
            ProjectMeta published = replicas.manifest(s, Generation.FIRST).projectMeta();
            assertThat(published.getSourceRevision()).isNotBlank();
            assertThat(lease(s).path("writer").asText()).isEqualTo("node-a");
            // backed up after the publish
            assertThat(store.list(layout.writerStateIndexKey(s, Generation.FIRST))).hasSize(1);
        }
        JsonNode hb = heartbeatOf("node-a").orElseThrow();
        assertThat(hb.path("leases")).hasSize(3);
        assertThat(hb.path("workQueue").asBoolean()).isTrue();
        assertThat(hb.path("indexing").isNull()).isTrue();
        JsonNode backlog = hb.path("backlog").path("shard-0");
        assertThat(backlog.path("pending").asBoolean()).isFalse();
        assertThat(backlog.path("sourceRevision").asText()).isEqualTo(backlog.path("publishedRevision").asText());
        assertThat(backlog.path("oldestUnpublishedChangeAgeSeconds").isNull()).isTrue();

        // nothing changed: no new generation
        rounds(3, 10, a);
        for (ShardId s : shards) assertThat(latest(s)).contains(Generation.FIRST);

        // a change is picked up and published once
        change(shards.get(1), "three");
        rounds(1, 10, a);
        assertThat(latest(shards.get(1))).contains(new Generation(2));
        assertThat(latest(shards.get(0))).contains(Generation.FIRST);
    }

    @Test
    void shardRecordedWithoutEmbeddingsIsIndexedWithoutThem() throws IOException {
        ShardId plain = pathShard("plain");
        ShardId bare = pathShard("bare");
        catalog.register(bare, Optional.of(ShardSource.path(sourceDirs.get(bare).toString()).withEmbed(false)));
        Writer a = new Writer("node-a");

        a.loop.tick();
        assertThat(latest(plain)).contains(Generation.FIRST);
        assertThat(latest(bare)).contains(Generation.FIRST);
        int afterFirst = a.embedder.calls.get();
        assertThat(afterFirst).as("the embedded shard was embedded").isPositive();

        change(bare, "three");
        rounds(1, 10, a);
        assertThat(latest(bare)).contains(new Generation(2));
        assertThat(a.embedder.calls.get()).as("a change of the no-embed shard embeds nothing").isEqualTo(afterFirst);
        change(plain, "three");
        rounds(1, 10, a);
        assertThat(a.embedder.calls.get()).as("the embedded shard still embeds").isGreaterThan(afterFirst);
    }

    /** Runs {@code body} once, the first time shard {@code shard}'s run embeds (the run is parked there). */
    private static java.util.function.Consumer<Integer> duringRunOf(Writer w, ShardId shard, IoRunnable body) {
        java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean();
        return n -> {
            if (!w.loop.running().equals(Optional.of(shard)) || !done.compareAndSet(false, true)) return;
            try {
                body.run();
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };
    }

    interface IoRunnable { void run() throws IOException; }

    @Test
    void changeOnIdleShardPendingWhileAnotherShardRuns() throws IOException {
        ShardId busy = pathShard("busy");
        ShardId idle = pathShard("idle");
        Writer a = new Writer("node-a");
        a.loop.tick();
        assertThat(latest(idle)).contains(Generation.FIRST);
        AtomicReference<JsonNode> seen = new AtomicReference<>();
        a.embedder.onBatch = duringRunOf(a, busy, () -> {
            change(idle, "pushed");     // while busy's run is in progress
            clock.advance(10);          // one poll later the detector fires (its own thread in production)
            a.loop.detectChanges();
            seen.set(heartbeatOf("node-a").orElseThrow());
        });
        change(busy, "two");
        rounds(1, 10, a);
        assertThat(seen.get()).as("busy's run reached the embed pass").isNotNull();
        JsonNode backlog = seen.get().path("backlog").path("idle");
        assertThat(backlog.path("pending").asBoolean()).as("idle shard pending during the other run").isTrue();
        assertThat(seen.get().path("indexing").path("shard").asText()).isEqualTo("busy");
        assertThat(backlog.path("sourceRevision").asText()).isNotEqualTo(backlog.path("publishedRevision").asText());
        a.embedder.onBatch = n -> {};
        rounds(1, 10, a);
        assertThat(latest(idle)).contains(new Generation(2));
    }

    @Test
    void pendingHeartbeatWrittenImmediately() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        assertThat(heartbeatOf("node-a").orElseThrow().path("backlog").path("solo").path("pending").asBoolean()).isFalse();
        change(s, "pushed");
        clock.advance(10);
        a.loop.detectChanges(); // no tick, no heartbeat timer: the detector writes the heartbeat itself
        JsonNode b = heartbeatOf("node-a").orElseThrow().path("backlog").path("solo");
        assertThat(b.path("pending").asBoolean()).isTrue();
        assertThat(b.path("sourceRevision").asText()).isNotEqualTo(b.path("publishedRevision").asText());
        // nothing new: no further write
        String etag = store.get(layout.writerKey("node-a")).orElseThrow().etag();
        a.loop.detectChanges();
        assertThat(store.get(layout.writerKey("node-a")).orElseThrow().etag()).isEqualTo(etag);
    }

    @Test
    void newerChangeOfAPendingShardWrittenImmediately() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        change(s, "first");
        clock.advance(10);
        a.loop.detectChanges();
        String first = heartbeatOf("node-a").orElseThrow().path("backlog").path("solo").path("sourceRevision").asText();
        change(s, "second");    // still unpublished: the shard was already pending
        clock.advance(10);
        a.loop.detectChanges();
        JsonNode b = heartbeatOf("node-a").orElseThrow().path("backlog").path("solo");
        assertThat(b.path("pending").asBoolean()).isTrue();
        assertThat(b.path("sourceRevision").asText()).as("the newer change is announced at once").isNotEqualTo(first);
    }

    @Test
    void changeFoundByTheLoopsOwnPollIsAnnouncedBeforeItsRun() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        AtomicReference<JsonNode> seen = new AtomicReference<>();
        a.embedder.onBatch = duringRunOf(a, s, () -> seen.set(heartbeatOf("node-a").orElseThrow()));
        change(s, "pushed");
        rounds(1, 10, a);   // the tick's poll finds the change, then runs it at once
        assertThat(seen.get()).isNotNull();
        assertThat(seen.get().path("backlog").path("solo").path("pending").asBoolean())
                .as("the heartbeat written before the run shows the change").isTrue();
    }

    @Test
    void changeDuringItsOwnRunStaysPendingAfterThePublish() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        a.embedder.onBatch = duringRunOf(a, s, () -> {
            change(s, "later");         // pushed after the run checked out its revision
            a.loop.detectChanges();
        });
        change(s, "first");
        rounds(1, 10, a);
        assertThat(latest(s)).contains(new Generation(2));
        JsonNode b = heartbeatOf("node-a").orElseThrow().path("backlog").path("solo");
        assertThat(b.path("pending").asBoolean()).as("the run did not include the later change").isTrue();
        String head = b.path("sourceRevision").asText();
        a.embedder.onBatch = n -> {};
        rounds(1, 10, a);
        assertThat(latest(s)).contains(new Generation(3));
        assertThat(replicas.manifest(s, new Generation(3)).projectMeta().getSourceRevision()).isEqualTo(head);
        assertThat(heartbeatOf("node-a").orElseThrow().path("backlog").path("solo").path("pending").asBoolean())
                .isFalse();
    }

    @Test
    void heartbeatReportsPendingBacklogAndIndexingPhase() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        change(s, "later");
        List<WriterHeartbeat.Indexing> seen = new ArrayList<>();
        a.embedder.onBatch = n -> seen.add(a.loop.heartbeat().indexing());
        AtomicReference<WriterHeartbeat> pending = new AtomicReference<>();
        // the poll marks it pending before the run starts: seen from the heartbeat written by the first beat
        rounds(1, 10, a);
        assertThat(seen).isNotEmpty();
        assertThat(seen.get(0).shard()).isEqualTo("solo");
        assertThat(seen.get(0).phase()).isEqualTo(WriterHeartbeat.EMBED);
        assertThat(seen.get(0).progress()).isBetween(0.0, 1.0);
        WriterHeartbeat after = a.loop.heartbeat();
        assertThat(after.indexing()).isNull();
        assertThat(after.backlog().get("solo").pending()).isFalse();

        // a change the writer has seen but not yet published: pending with an age
        change(s, "evenLater");
        a.embedder.onBatch = n -> pending.compareAndSet(null, a.loop.heartbeat());
        clock.advance(10);
        a.loop.tick();
        WriterHeartbeat.Backlog b = pending.get().backlog().get("solo");
        assertThat(b.pending()).isTrue();
        assertThat(b.sourceRevision()).isNotEqualTo(b.publishedRevision());
        assertThat(b.oldestUnpublishedChangeAgeSeconds()).isNotNull().isGreaterThanOrEqualTo(0);
        assertThat(WorkLoop.phaseOf("Parsing")).isEqualTo("parse");
        assertThat(WorkLoop.phaseOf("Indexing")).isEqualTo("commit");
        assertThat(WorkLoop.phaseOf("Done")).isEqualTo("publish");
    }

    @Test
    void secondWriterGetsFairShareAfterPublishes() throws IOException {
        List<ShardId> shards = pathShards(4);
        Writer a = new Writer("node-a");
        a.loop.tick();
        assertThat(a.held()).hasSize(4);

        Writer b = new Writer("node-b");
        b.loop.tick();
        assertThat(b.held()).as("every lease is held by a live writer").isEmpty();

        // within minHold A keeps everything, even while it publishes
        for (int i = 0; i < 8; i++) {
            for (ShardId s : shards) change(s, "m" + i);
            rounds(1, 10, a, b);
            assertThat(a.held()).hasSize(4);
        }
        Map<ShardId, Integer> publishesBefore = new HashMap<>();
        for (ShardId s : shards) publishesBefore.put(s, a.loop.publishes(s));

        // past minHold: A releases its surplus right after publishing it, B takes it
        for (int i = 0; i < 6; i++) {
            for (ShardId s : shards) change(s, "n" + i);
            rounds(1, 10, a, b);
        }
        assertThat(a.held()).hasSize(2);
        assertThat(b.held()).hasSize(2);
        assertThat(a.held()).doesNotContainAnyElementsOf(b.held());
        for (ShardId moved : b.held()) {
            assertThat(a.loop.publishes(moved)).as("A published %s before releasing it", moved)
                    .isGreaterThan(publishesBefore.get(moved));
        }
        // B keeps them published
        ShardId moved = b.held().iterator().next();
        Generation before = latest(moved).orElseThrow();
        change(moved, "byB");
        rounds(1, 10, a, b);
        assertThat(latest(moved).orElseThrow().value()).isEqualTo(before.value() + 1);
        assertThat(replicas.manifest(moved, latest(moved).orElseThrow()).writerNode()).isEqualTo("node-b");
    }

    @Test
    void noReleaseMidCommit() throws IOException {
        List<ShardId> shards = pathShards(2);
        Writer a = new Writer("node-a");
        a.loop.tick();
        Writer b = new Writer("node-b");
        b.loop.tick();
        assertThat(a.held()).hasSize(2); // more than its share (1) from now on
        // every run of A, before and when it becomes eligible to release: the lease stays A's
        // (never marked released) and B cannot take the shard being committed
        List<String> violations = new ArrayList<>();
        int[] runsChecked = {0};
        a.embedder.onBatch = n -> {
            try {
                ShardId running = a.loop.running().orElseThrow();
                JsonNode l = lease(running);
                if (!l.path("writer").asText().equals("node-a") || l.path("released").asBoolean(false)) {
                    violations.add("lease of " + running + " during its run: " + l);
                }
                if (b.held().isEmpty()) b.loop.tick();
                if (b.held().contains(running)) violations.add("B took " + running + " mid-run");
                if (!a.held().contains(running)) violations.add("A dropped " + running + " mid-run");
                runsChecked[0]++;
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };
        for (int i = 0; i < 14 && a.held().size() > 1; i++) {
            for (ShardId s : shards) change(s, "mid" + i);
            rounds(1, 10, a, b);
        }
        assertThat(violations).isEmpty();
        assertThat(runsChecked[0]).isGreaterThan(10);
        // after a publish (past minHold) A is down to its share and B has the other one
        assertThat(a.held()).hasSize(1);
        assertThat(b.held()).hasSize(1);
        ShardId released = b.held().iterator().next();
        assertThat(latest(released).orElseThrow().value()).isGreaterThan(10); // A published it right before
    }

    @Test
    void crashedWriterLeasesExpireAndAreTakenOver() throws IOException {
        List<ShardId> shards = pathShards(2);
        Writer a = new Writer("node-a");
        a.loop.tick();
        assertThat(a.held()).hasSize(2);
        // A crashes: no more heartbeats or renewals, nothing released
        Writer b = new Writer("node-b");
        b.loop.tick();
        assertThat(b.held()).isEmpty();
        rounds(2, 10, b);
        assertThat(b.held()).as("A's leases are not expired yet").isEmpty();
        rounds(2, 10, b); // 40 s after A's last write: its heartbeat and leases are past the 30 s TTL
        assertThat(b.held()).containsExactlyInAnyOrderElementsOf(shards);
        for (ShardId s : shards) assertThat(lease(s).path("writer").asText()).isEqualTo("node-b");
        // nothing changed: B resumes from LATEST without publishing a redundant generation
        for (ShardId s : shards) assertThat(latest(s)).contains(Generation.FIRST);
        // the crashed writer, if it wakes up, cannot publish
        assertThatThrownBy(() -> a.loop.leaseManager().assertHeld(shards.get(0)))
                .isInstanceOf(com.pharos.cluster.PublishConflictException.class);
    }

    @Test
    void restartedWriterWaitsOutItsOwnOldLease() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        // the process dies; a new one starts with the same node id
        Writer again = new Writer("node-a");
        again.loop.tick();
        assertThat(again.held()).as("its own old lease is waited out like anyone's").isEmpty();
        rounds(3, 11, again);
        assertThat(again.held()).containsExactly(s);
    }

    @Test
    void flappingWritersDoNotBounceLeases() throws IOException {
        List<ShardId> shards = pathShards(4);
        Writer a = new Writer("node-a");
        a.loop.tick();
        // since when A holds each shard, by the test clock; a release before MIN_HOLD is a bounce
        Map<ShardId, Instant> aSince = new HashMap<>();
        for (ShardId s : a.held()) aSince.put(s, clock.instant());
        int handoffs = 0;
        int joins = 0;
        Map<ShardId, Integer> releases = new HashMap<>();
        Instant start = clock.instant();
        Writer b = null;
        for (int step = 0; step < 200; step++) {
            // B is up for 50 s, down for 50 s; every other time it crashes instead of stopping
            int phase = step % 10;
            if (phase == 0) {
                b = new Writer("node-b" + step);
                joins++;
            } else if (phase == 5 && b != null) {
                if ((step / 10) % 2 == 0) b.loop.close(); // graceful: releases at once
                b = null;                               // otherwise a crash: its leases expire
            }
            for (ShardId s : shards) change(s, "s" + step);
            clock.advance(10);
            Set<ShardId> before = a.held();
            a.loop.tick();
            if (b != null) b.loop.tick();
            Set<ShardId> after = a.held();
            for (ShardId s : before) {
                if (!after.contains(s)) {
                    handoffs++;
                    releases.merge(s, 1, Integer::sum);
                    assertThat(Duration.between(aSince.get(s), clock.instant()))
                            .as("A released %s before holding it %s", s, MIN_HOLD).isGreaterThanOrEqualTo(MIN_HOLD);
                }
            }
            for (ShardId s : after) if (!before.contains(s)) aSince.put(s, clock.instant());
        }
        // B joined 20 times and got at most its share (2) each time. A lease A got back is kept for
        // minHold (checked above), so no lease moves more than once per minHold however often B flaps.
        assertThat(joins).isEqualTo(20);
        assertThat(handoffs).isLessThanOrEqualTo(2 * joins);
        long windows = Duration.between(start, clock.instant()).dividedBy(MIN_HOLD) + 1;
        for (Map.Entry<ShardId, Integer> e : releases.entrySet()) {
            assertThat(e.getValue()).as("releases of %s by A", e.getKey()).isLessThanOrEqualTo((int) windows);
        }
        // every shard stayed published throughout
        for (ShardId s : shards) assertThat(latest(s).orElseThrow().value()).isGreaterThan(60);
    }

    @Test
    void takeoverResumesFromLatestWithoutFullReembed() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        int fullEmbeds = a.embedder.calls.get();
        assertThat(fullEmbeds).isPositive();
        // the control: what the old owner itself embeds for one new method on a warm cache
        change(s, "first");
        rounds(1, 10, a);
        change(s, "second");
        int before = a.embedder.calls.get();
        rounds(1, 10, a);
        int warm = a.embedder.calls.get() - before;
        assertThat(warm).isPositive();
        int docsA = a.docs(s);
        a.loop.stop(Duration.ofSeconds(5)); // graceful: releases at once
        assertThat(heartbeatOf("node-a")).isEmpty();
        assertThat(lease(s).path("released").asBoolean()).isTrue();

        change(s, "afterTakeover");
        Writer b = new Writer("node-b");
        b.loop.tick();

        assertThat(b.held()).containsExactly(s);
        assertThat(latest(s)).contains(new Generation(4));
        assertThat(replicas.manifest(s, new Generation(4)).writerNode()).isEqualTo("node-b");
        assertThat(replicas.manifest(s, new Generation(4)).parentGeneration()).isEqualTo(3L);
        // exactly what the old owner embeds for the same change: the restored cache serves the rest
        assertThat(b.embedder.calls.get()).isEqualTo(warm);
        assertThat(b.docs(s)).isEqualTo(docsA + 1);
        // restored and registered under this writer's paths
        ProjectMeta meta = b.registry.find("solo").orElseThrow();
        assertThat(Path.of(meta.getRootPath())).isEqualTo(sourceDirs.get(s).toAbsolutePath().normalize());
        assertThat(Path.of(meta.getIndexPath())).isEqualTo(b.lucene.getProjectIndexDir("solo"));
    }

    @Test
    void takeoverAtAnotherCheckoutPathRunsFullWithoutDuplicates() throws Exception {
        Path pusher = tmp.resolve("pusher");
        Path bare = tmp.resolve("origin.git");
        Files.createDirectories(bare);
        git(bare, "init", "--bare", "--initial-branch=main");
        Files.createDirectories(pusher.resolve("src"));
        Files.writeString(pusher.resolve("src/Svc.java"),
                "public class Svc {\n    public int a(int x) { return x; }\n    public int b(int x) { return a(x) + 1; }\n}\n");
        git(pusher, "init", "--initial-branch=main");
        git(pusher, "add", ".");
        git(pusher, "commit", "-m", "one");
        git(pusher, "push", bare.toString(), "main");
        ShardId s = new ShardId("gitshard");
        catalog.register(s, Optional.of(ShardSource.git(bare.toString(), "main")));

        Writer a = new Writer("node-a");
        a.loop.tick();
        assertThat(latest(s)).contains(Generation.FIRST);
        int fullEmbeds = a.embedder.calls.get();
        String aRoot = replicas.manifest(s, Generation.FIRST).projectMeta().getRootPath();
        a.loop.stop(Duration.ofSeconds(5));

        // a new commit; B clones under its own work dir: another absolute path than A's
        Files.writeString(pusher.resolve("src/Svc.java"),
                "public class Svc {\n    public int a(int x) { return x; }\n    public int b(int x) { return a(x) + 1; }\n"
                        + "    public int c(int x) { return b(x) * 3; }\n}\n");
        git(pusher, "commit", "-am", "two");
        git(pusher, "push", bare.toString(), "main");
        Writer b = new Writer("node-b");
        b.loop.tick();

        assertThat(latest(s)).contains(new Generation(2));
        String bRoot = b.registry.find("gitshard").orElseThrow().getRootPath();
        assertThat(bRoot).isNotEqualTo(aRoot);
        assertThat(b.docsUnder(s, aRoot)).as("no document left under the old owner's paths").isZero();
        assertThat(b.docsUnder(s, bRoot)).isEqualTo(b.docs(s));
        // a full run, but the restored cache serves every unchanged text
        assertThat(b.embedder.calls.get()).isLessThan(fullEmbeds);
        assertThat(replicas.manifest(s, new Generation(2)).projectMeta().getSourceRevision())
                .isEqualTo(git(pusher, "rev-parse", "HEAD"));
    }

    /**
     * Phase 4 gate re-run: a writer restored a shard indexed under another checkout path, and its full
     * run died before the commit (killed, aborted or failed). The local index still holds the old
     * owner's documents, so the next run must be full again, never an incremental run that adds the
     * documents a second time under this writer's paths.
     */
    @Test
    void restoredShardWhoseFullRunDiedRunsFullAgain() throws Exception {
        Path pusher = tmp.resolve("pusher");
        Path bare = tmp.resolve("origin.git");
        Files.createDirectories(bare);
        git(bare, "init", "--bare", "--initial-branch=main");
        Files.createDirectories(pusher.resolve("src"));
        Files.writeString(pusher.resolve("src/Svc.java"),
                "public class Svc {\n    public int a(int x) { return x; }\n    public int b(int x) { return a(x) + 1; }\n}\n");
        git(pusher, "init", "--initial-branch=main");
        git(pusher, "add", ".");
        git(pusher, "commit", "-m", "one");
        git(pusher, "push", bare.toString(), "main");
        ShardId s = new ShardId("gitshard");
        catalog.register(s, Optional.of(ShardSource.git(bare.toString(), "main")));
        Writer a = new Writer("node-a");
        a.loop.tick();
        String aRoot = replicas.manifest(s, Generation.FIRST).projectMeta().getRootPath();
        int docs = a.docs(s);
        a.loop.stop(Duration.ofSeconds(5));

        Files.writeString(pusher.resolve("src/Svc.java"),
                "public class Svc {\n    public int a(int x) { return x; }\n    public int b(int x) { return a(x) + 1; }\n"
                        + "    public int c(int x) { return b(x) * 3; }\n}\n");
        git(pusher, "commit", "-am", "two");
        git(pusher, "push", bare.toString(), "main");
        Writer b = new Writer("node-b");
        java.util.concurrent.atomic.AtomicBoolean crash = new java.util.concurrent.atomic.AtomicBoolean(true);
        b.embedder.onBatch = n -> {
            if (crash.getAndSet(false)) throw new IllegalStateException("the run dies before its commit");
        };
        b.loop.tick();
        assertThat(latest(s)).as("nothing published by the dead run").contains(Generation.FIRST);
        rounds(1, 10, b);   // the retry
        assertThat(latest(s)).contains(new Generation(2));
        String bRoot = b.registry.find("gitshard").orElseThrow().getRootPath();
        assertThat(b.docsUnder(s, aRoot)).as("no document left under the old owner's paths").isZero();
        assertThat(b.docs(s)).as("no document indexed twice").isEqualTo(b.docsUnder(s, bRoot)).isGreaterThan(docs);
        assertThat(replicas.manifest(s, new Generation(2)).docCount()).isEqualTo(b.docs(s));
    }

    /**
     * Final review C1: the newest backup (gen 1) is older than LATEST (gen 2, whose backup was lost).
     * A file added in gen 2 and deleted since is not in gen 1's file state, so an incremental run on
     * the restored index would never delete its documents. Path shards index the same absolute root
     * on every writer, so without the fix this takeover runs incrementally. Its first (full) run dies
     * before the commit: the retry must be full too.
     */
    @Test
    void takeoverWithAnOlderBackupThanLatestRunsFull() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        int gen1Docs = a.docs(s);
        Path extra = sourceDirs.get(s).resolve("src/main/java/com/example/solo/Extra.java");
        Files.writeString(extra, "package com.example.solo;\n\npublic class Extra {\n"
                + "    public int gone(int x) { return x - 1; }\n}\n");
        rounds(1, 10, a);
        assertThat(latest(s)).contains(new Generation(2));
        assertThat(a.docsUnder(s, extra.toString())).isPositive();
        a.loop.stop(Duration.ofSeconds(5));
        // gen 2's backup never made it (the writer died uploading it, or S3 failed)
        for (var o : store.list(layout.writerStateGenPrefix(s, new Generation(2)))) store.delete(o.key());
        Files.delete(extra);
        change(s, "afterTakeover");

        Writer b = new Writer("node-b");
        AtomicBoolean crash = new AtomicBoolean(true);
        b.embedder.onBatch = n -> {
            if (crash.getAndSet(false)) throw new IllegalStateException("the run dies before its commit");
        };
        b.loop.tick();
        assertThat(latest(s)).as("nothing published by the dead run").contains(new Generation(2));
        rounds(1, 10, b);

        assertThat(latest(s)).contains(new Generation(3));
        assertThat(b.docsUnder(s, extra.toString())).as("no document of the deleted file").isZero();
        assertThat(b.docs(s)).isEqualTo(gen1Docs + 1);
        assertThat(replicas.manifest(s, new Generation(3)).docCount()).isEqualTo(b.docs(s));
        assertThat(b.lucene.getProjectIndexDir("solo").resolve(WorkLoop.FULL_RUN_REQUIRED)).doesNotExist();
    }

    /** The WARN and INFO lines {@code logger} writes while {@code body} runs. */
    private static List<String> logsOf(Class<?> logger, IoRunnable body) throws IOException {
        var log = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(logger);
        var appender = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        appender.start();
        log.addAppender(appender);
        try {
            body.run();
        } finally {
            log.detachAppender(appender);
        }
        return appender.list.stream().map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage).toList();
    }

    /**
     * Final review I2: a cluster upgrades by restarting its writers on the new release. A held shard
     * whose source is unchanged is still pending, and gets a FULL run, when its local index needs one
     * (another index format), its files were embedded under another chunking version, or its LATEST
     * generation has an older index format. Once rebuilt it is idle again.
     */
    @Test
    void idleShardIsRebuiltWhenThisReleaseNeedsAFullRun() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        assertThat(latest(s)).contains(Generation.FIRST);
        rounds(1, 10, a);
        assertThat(latest(s)).as("idle").contains(Generation.FIRST);
        Path dir = a.lucene.getProjectIndexDir(s.value());

        // 1. the local index has an older format
        com.pharos.indexer.IndexFormat.write(dir, new com.pharos.indexer.IndexFormat.Stamp(
                com.pharos.indexer.IndexVersions.INDEX_FORMAT - 1, "older", Instant.now()));
        List<String> logs = logsOf(WorkLoop.class, () -> rounds(1, 10, a));
        assertThat(latest(s)).contains(new Generation(2));
        assertThat(logs).anyMatch(l -> l.contains("running a FULL index") && l.contains("index format"));

        // 2. every file was embedded under another chunking version
        Path state = dir.resolve("file-state.json");
        JsonNode tree = JSON.readTree(state.toFile());
        tree.fields().forEachRemaining(e -> ((com.fasterxml.jackson.databind.node.ObjectNode) e.getValue())
                .put("chunkingVersion", com.pharos.indexer.IndexVersions.CHUNKING_VERSION - 1));
        JSON.writeValue(state.toFile(), tree);
        logs = logsOf(WorkLoop.class, () -> rounds(1, 10, a));
        assertThat(latest(s)).contains(new Generation(3));
        assertThat(logs).anyMatch(l -> l.contains("running a FULL index") && l.contains("chunking version"));

        // 3. LATEST has an older index format (published by the previous release)
        String key = layout.manifestKey(s, new Generation(3));
        var manifest = (com.fasterxml.jackson.databind.node.ObjectNode) JSON.readTree(store.get(key).orElseThrow().data());
        ((com.fasterxml.jackson.databind.node.ObjectNode) manifest.get("indexFormat"))
                .put("format", com.pharos.indexer.IndexVersions.INDEX_FORMAT - 1);
        store.put(key, JSON.writeValueAsBytes(manifest));
        logs = logsOf(WorkLoop.class, () -> rounds(1, 10, a));
        assertThat(latest(s)).contains(new Generation(4));
        assertThat(logs).anyMatch(l -> l.contains("running a FULL index") && l.contains("published generation 3"));
        assertThat(replicas.manifest(s, new Generation(4)).indexFormatVersion())
                .isEqualTo(com.pharos.indexer.IndexVersions.INDEX_FORMAT);

        // rebuilt: idle again
        rounds(3, 10, a);
        assertThat(latest(s)).contains(new Generation(4));
    }

    /**
     * A work-queue writer refuses {@code pharos index --full}, so it must repair a call graph with a
     * damaged property dictionary (written before 1.10.0) by itself: the shard gets a FULL run, which
     * builds a new graph generation and publishes it.
     */
    @Test
    void damagedGraphDictionaryGetsAFullRunAndPublishes() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        rounds(1, 10, a);
        assertThat(latest(s)).as("idle").contains(Generation.FIRST);
        Path dir = a.lucene.getProjectIndexDir(s.value());
        Path live = com.pharos.graph.CallGraph.projectGraphDir(dir);
        try (java.util.stream.Stream<Path> w = Files.walk(live)) {
            for (Path p : w.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(p);
        }
        com.pharos.graph.DamagedGraphFixture.unpackCallGraph(live);

        List<String> logs = logsOf(WorkLoop.class, () -> rounds(1, 10, a));
        assertThat(logs).anyMatch(l -> l.contains("running a FULL index") && l.contains("damaged property dictionary"));
        assertThat(latest(s)).contains(new Generation(2));
        Path rebuilt = com.pharos.graph.CallGraph.projectGraphDir(dir);
        assertThat(rebuilt).isNotEqualTo(live);
        assertThat(com.pharos.graph.CallGraph.dictionaryProblem(rebuilt)).isNull();
        rounds(3, 10, a);
        assertThat(latest(s)).as("idle again").contains(new Generation(2));
    }

    /**
     * Final review I4: one shard's remote black-holes (its ls-remote hangs in a child process). Its
     * checks time out after the short ls-remote timeout, run in parallel with the other shards', so
     * the other shard's change is still seen and published at once; the failed checks count toward
     * maxConsecutiveFailures, and the surplus shard is released to a writer that can reach it.
     */
    @Test
    void unreachableRemoteStallsOnlyItsOwnShard() throws Exception {
        Path pusher = tmp.resolve("pusher");
        Path bare = tmp.resolve("dead.git");
        Files.createDirectories(bare);
        git(bare, "init", "--bare", "--initial-branch=main");
        Files.createDirectories(pusher.resolve("src"));
        Files.writeString(pusher.resolve("src/Svc.java"), "public class Svc {\n    public int a(int x) { return x; }\n}\n");
        git(pusher, "init", "--initial-branch=main");
        git(pusher, "add", ".");
        git(pusher, "commit", "-m", "one");
        git(pusher, "push", bare.toString(), "main");
        ShardId dead = new ShardId("dead");
        catalog.register(dead, Optional.of(ShardSource.git(bare.toString(), "main")));
        ShardId live = pathShard("live");
        Path fake = SourceCheckoutTest.hangingLsRemoteGit(Files.createDirectories(tmp.resolve("fake")),
                tmp.resolve("fake/child.pid"));
        Duration lsRemote = Duration.ofSeconds(2);
        Writer a = new Writer("node-a", null, dir -> new SourceCheckout(dir, Duration.ofSeconds(60), lsRemote,
                fake.toString()));

        a.loop.tick();
        assertThat(a.held()).containsExactlyInAnyOrder(dead, live);
        assertThat(latest(live)).contains(Generation.FIRST);
        assertThat(latest(dead)).as("its checks time out").isEmpty();

        // the detector: live's change is announced while dead's check still hangs
        change(live, "pushed");
        clock.advance(10);
        Thread detector = Thread.ofVirtual().start(a.loop::detectChanges);
        long until = System.currentTimeMillis() + lsRemote.toMillis();
        boolean seen = false;
        while (!seen && System.currentTimeMillis() < until) {
            seen = heartbeatOf("node-a").map(h -> h.path("backlog").path("live").path("pending").asBoolean())
                    .orElse(false);
            if (!seen) Thread.sleep(20);
        }
        assertThat(seen).as("live pending before dead's check timed out").isTrue();
        assertThat(detector.isAlive()).as("dead's check was still running").isTrue();
        detector.join();
        long t0 = System.nanoTime();
        rounds(1, 10, a);
        assertThat(latest(live)).contains(new Generation(2));
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).as("one ls-remote timeout per tick, not one per shard")
                .isLessThan(lsRemote.multipliedBy(3));

        // a second writer: dead is a surplus shard of A, and its failed checks release it to B
        Writer b = new Writer("node-b");
        b.loop.tick();
        clock.advance(MIN_HOLD.toSeconds() - 20);
        rounds(3, 10, a, b);
        assertThat(a.held()).containsExactly(live);
        assertThat(b.held()).containsExactly(dead);
        assertThat(latest(dead)).as("B reaches the remote").contains(Generation.FIRST);
    }

    @Test
    void restoreStoreOutageRetriesInsteadOfAFullReembed() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        a.loop.stop(Duration.ofSeconds(5));
        change(s, "afterOutage");

        store.failOn("writer-state/blobs/"); // downloads of the backup fail: an outage, not a corrupt backup
        Writer b = new Writer("node-b");
        b.loop.tick();
        assertThat(b.held()).containsExactly(s);
        assertThat(latest(s)).as("nothing indexed or published during the outage").contains(Generation.FIRST);
        assertThat(b.embedder.calls.get()).isZero();
        assertThat(b.lucene.indexExists("solo")).isFalse();

        store.clearFailures();
        rounds(1, 10, b);
        assertThat(latest(s)).contains(new Generation(2));
        assertThat(b.embedder.calls.get()).isPositive().isLessThanOrEqualTo(4);
    }

    @Test
    void missingBackupFallsBackToAFullIndex() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        int fullEmbeds = a.embedder.calls.get();
        a.loop.stop(Duration.ofSeconds(5));
        for (var o : store.list(layout.writerStatePrefix(s))) store.delete(o.key());
        change(s, "noBackup");

        Writer b = new Writer("node-b");
        b.loop.tick();
        assertThat(latest(s)).contains(new Generation(2));
        assertThat(b.embedder.calls.get()).isGreaterThan(fullEmbeds - 1); // cold: everything embedded
        assertThat(b.docs(s)).isEqualTo(a.docs(s) + 1);
    }

    @Test
    void restoreRefusedWhileTheShardHasLocalStateOrARunIsInProgress() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        assertThatThrownBy(() -> a.loop.requireRestorable(s)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("local index");
        ShardId other = pathShard("other");
        change(s, "during");
        AtomicReference<Throwable> during = new AtomicReference<>();
        a.embedder.onBatch = n -> {
            try {
                a.loop.requireRestorable(other);
            } catch (Throwable t) {
                during.compareAndSet(null, t);
            }
        };
        rounds(1, 10, a);
        assertThat(during.get()).isInstanceOf(IllegalStateException.class).hasMessageContaining("in progress");
    }

    @Test
    void sourcelessShardIsPinnedToTheWriterThatHasItsPath() throws IOException {
        ShardId sourced = pathShard("sourced");
        // a shard indexed by hand on A before work-queue mode: in the catalog without a source
        Writer a = new Writer("node-a");
        Path dir = tmp.resolve("sources").resolve("handmade");
        Files.createDirectories(dir.resolve("src"));
        Files.writeString(dir.resolve("src/Hand.java"), "public class Hand {\n    public int h(int x) { return x; }\n}\n");
        a.localOnly.index(dir, "handmade", false, false, true, false, ProgressListener.SILENT);
        ShardId hand = new ShardId("handmade");
        catalog.register(hand);
        Writer b = new Writer("node-b");

        rounds(1, 10, a, b);
        assertThat(a.held()).contains(hand);
        assertThat(b.held()).doesNotContain(hand);
        assertThat(heartbeatOf("node-a").orElseThrow().path("pinned").get(0).asText()).isEqualTo("handmade");
        // the pinned shard counts in A's share (2 workable / 2 writers = 1): B gets the sourced one
        rounds(14, 10, a, b);
        assertThat(a.held()).containsExactly(hand);
        assertThat(b.held()).containsExactly(sourced);
        // the pinned shard is polled and published from its root
        assertThat(latest(hand)).isPresent();
        Generation g = latest(hand).orElseThrow();
        Files.writeString(dir.resolve("src/Hand.java"), "public class Hand {\n    public int h(int x) { return x; }\n"
                + "    public int k(int x) { return h(x); }\n}\n");
        Files.setLastModifiedTime(dir.resolve("src/Hand.java"), FileTime.fromMillis(System.currentTimeMillis() + 9_000));
        rounds(1, 10, a, b);
        assertThat(latest(hand).orElseThrow().value()).isEqualTo(g.value() + 1);
        // never released for balance, whatever the share
        b.loop.close();
        rounds(14, 10, a);
        assertThat(a.held()).contains(hand);
    }

    @Test
    void publishOutsideTheWorkLoopIsRefused() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        // a hand-run index in the same process (another thread than the loop's run): refused at publish
        change(s, "byHand");
        assertThatThrownBy(() -> a.manager.index(sourceDirs.get(s), "solo", true, false, true, false,
                ProgressListener.SILENT))
                .isInstanceOf(PublishException.class)
                .hasMessageContaining("work-queue mode");
        assertThat(latest(s)).contains(Generation.FIRST);
        // a process without a work loop holds no lease
        WorkQueueLeaseGuard detached = new WorkQueueLeaseGuard();
        assertThatThrownBy(() -> detached.assertHeld(s)).hasMessageContaining("does not hold the lease");
    }

    /**
     * A project split into two ring shards whose source sits on the catalog entry named after it (its
     * source before the split): {@code n} files in different packages, so both shards get some.
     */
    private List<ShardId> splitProject(String name, int files) throws IOException {
        ShardId plain = pathShard(name);
        Path dir = sourceDirs.get(plain);
        for (int i = 0; i < files; i++) {
            Path pkg = dir.resolve("src/main/java/com/example/p" + i);
            Files.createDirectories(pkg);
            Files.writeString(pkg.resolve("Part" + i + ".java"), "package com.example.p" + i + ";\n\n"
                    + "public class Part" + i + " {\n    public int f" + i + "(int x) { return x + " + i + "; }\n}\n");
        }
        com.pharos.cluster.sharding.ShardSpec spec =
                com.pharos.cluster.sharding.ShardSpec.parse("[project, ring(path, shards=2)]");
        int v = catalog.addSpec(Optional.of(name), spec);
        List<ShardId> ring = List.of(new ShardId(name + "~ring=s01"), new ShardId(name + "~ring=s02"));
        catalog.putProject(name, v, ring);
        sourceDirs.put(ShardId.ofProject(name), dir);
        return ring;
    }

    @Test
    void splitProjectIsOneWorkUnit() throws IOException {
        List<ShardId> ring = splitProject("big", 12);
        ShardId other = pathShard("other");
        Writer a = new Writer("node-a");

        a.loop.tick();
        // both shard leases of the project, plus the [project] one
        assertThat(a.held()).containsExactlyInAnyOrder(ring.get(0), ring.get(1), other);
        for (ShardId s : ring) {
            assertThat(latest(s)).contains(Generation.FIRST);
            assertThat(replicas.manifest(s, Generation.FIRST).projectGeneration()).isEqualTo(1L);
            assertThat(lease(s).path("writer").asText()).isEqualTo("node-a");
        }
        assertThat(latest(new ShardId("big"))).as("the project's plain id is never published").isEmpty();
        assertThat(a.loop.publishes(new ShardId("big"))).isEqualTo(1);
        JsonNode hb = heartbeatOf("node-a").orElseThrow();
        assertThat(hb.path("leases")).hasSize(3);
        assertThat(hb.path("backlog").has("big")).isTrue();
        int docsA = a.lucene.openMultiReader(List.of("big")).numDocs();

        // idle: no new generation of any shard
        rounds(2, 10, a);
        for (ShardId s : ring) assertThat(latest(s)).contains(Generation.FIRST);

        // a change publishes every shard of the project once, as one run
        change(new ShardId("big"), "three");
        rounds(1, 10, a);
        for (ShardId s : ring) {
            assertThat(latest(s)).contains(new Generation(2));
            assertThat(replicas.manifest(s, new Generation(2)).projectGeneration()).isEqualTo(2L);
        }

        // all or none: with one shard's lease taken elsewhere, a new owner takes neither
        a.loop.stop(Duration.ofSeconds(5));
        LeaseManager x = new LeaseManager(store, layout, "node-x", TTL, LeaseManager.BucketNow.NONE, clock);
        Lease taken = x.tryAcquire(ring.get(1)).orElseThrow();
        Writer b = new Writer("node-b");
        change(new ShardId("big"), "four");
        b.loop.tick();
        assertThat(b.held()).containsExactly(other);
        assertThat(lease(ring.get(0)).path("released").asBoolean()).as("the partial lease went back").isTrue();
        assertThat(lease(ring.get(0)).path("writer").asText()).as("taken, then released by B").isEqualTo("node-b");
        assertThat(latest(ring.get(0))).contains(new Generation(2));

        // once free, the new owner takes both, restores the project and resumes incrementally
        x.release(taken);
        clock.advance(10);
        int before = b.embedder.calls.get();
        b.loop.tick();
        assertThat(b.held()).containsExactlyInAnyOrder(ring.get(0), ring.get(1), other);
        for (ShardId s : ring) {
            assertThat(latest(s)).contains(new Generation(3));
            assertThat(replicas.manifest(s, new Generation(3)).writerNode()).isEqualTo("node-b");
            assertThat(replicas.manifest(s, new Generation(3)).projectGeneration()).isEqualTo(3L);
        }
        assertThat(b.embedder.calls.get() - before).as("the restored cache serves the unchanged texts")
                .isLessThan(a.embedder.calls.get() / 2);
        assertThat(b.lucene.openMultiReader(List.of("big")).numDocs()).isEqualTo(docsA + 2);
        x.close();
    }

    @Test
    void gracefulStopAbortsARunThatCannotFinishAndReleasesLeases() throws Exception {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.tick();
        int docs = a.docs(s);
        change(s, "slow");
        AtomicBoolean stopAsked = new AtomicBoolean();
        java.util.concurrent.CountDownLatch inRun = new java.util.concurrent.CountDownLatch(1);
        a.embedder.onBatch = n -> {
            inRun.countDown();
            long until = System.nanoTime() + 10_000_000_000L;
            while (!stopAsked.get() && System.nanoTime() < until) Thread.onSpinWait();
            try {
                Thread.sleep(1_500); // longer than the stop's budget minus its reserve
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        clock.advance(10);
        Thread runner = new Thread(a.loop::tick);
        runner.start();
        assertThat(inRun.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        stopAsked.set(true);
        a.loop.stop(Duration.ofSeconds(2));
        runner.join(10_000);

        assertThat(latest(s)).as("aborted before its commit: nothing published").contains(Generation.FIRST);
        assertThat(a.docs(s)).isEqualTo(docs);
        assertThat(lease(s).path("released").asBoolean()).isTrue();
        assertThat(heartbeatOf("node-a")).isEmpty();
        // a new owner takes it at once (released, not expired) and publishes the change
        Writer b = new Writer("node-b");
        b.loop.tick();
        assertThat(latest(s)).contains(new Generation(2));
    }

    // ------------------------------------------------------------------ stop on a hung bucket

    /** Runs {@code stop(budget)} on its own thread; returns its duration, or -1 if it is still running. */
    private long timedStop(Writer w, Duration budget, long waitMillis) throws InterruptedException {
        long t0 = System.nanoTime();
        Thread stopper = new Thread(() -> w.loop.stop(budget), "test-stop-" + w.node);
        stopper.setDaemon(true);
        stopper.start();
        stopper.join(waitMillis);
        return stopper.isAlive() ? -1 : (System.nanoTime() - t0) / 1_000_000;
    }

    /**
     * Gate run 2 for 1.9.0, w5: an idle writer on a saturated MinIO did not exit within 120 s of
     * SIGTERM. Its heartbeat thread sat in an S3 put (minutes: 30 s socket timeout, 3 attempts)
     * holding the registry's lock, and stop() began with a synchronous heartbeat of its own on that
     * lock, then deleted the heartbeat on it again, with no deadline.
     */
    @Test
    void idleWriterStopsWithinBudgetPlusCleanupWhileTheBucketHangs() throws Exception {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a");
        a.loop.shutdownCleanup(Duration.ofSeconds(2));
        a.loop.tick(); // holds the shard, published, idle
        assertThat(a.held()).containsExactly(s);

        store.stallOn("pharos/"); // every bucket call hangs from now on
        clock.advance(10);
        Thread beat = new Thread(a.loop::tick, "test-tick"); // like the heartbeat thread, stuck mid-beat
        beat.setDaemon(true);
        beat.start();
        assertThat(store.awaitStalled(1, 10)).isTrue();

        long took = timedStop(a, Duration.ofSeconds(2), 15_000);

        assertThat(took).as("stop returned (ms)").isNotNegative().isLessThan(2_000 + 2_000 + 1_500);
        assertThat(warnings()).anyMatch(m -> m.contains("gave up") && m.contains("heartbeat"));
        assertThat(warnings()).anyMatch(m -> m.contains("gave up") && m.contains("lease"));
    }

    /**
     * Gate run 2 for 1.9.0, w6: a writer in the parse phase of a long full run, on a hung bucket,
     * gets SIGTERM. The stop ends within its budget plus the cleanup bound, and the run is aborted
     * inside the parse (not after the parser's last file).
     */
    @Test
    void writerMidParseStopsWithinBudgetPlusCleanupWhileTheBucketHangs() throws Exception {
        ShardId s = pathShard("solo");
        SlowParser parser = new SlowParser();
        Writer a = new Writer("node-a", null, SourceCheckout::new, parser);
        a.loop.shutdownCleanup(Duration.ofSeconds(2));
        a.loop.tick();
        Optional<Generation> before = latest(s);
        change(s, "slow");
        // a FULL run, as w6's (takeover at another checkout path): its parse is one parseFiles call
        Files.writeString(a.lucene.getProjectIndexDir(s.value()).resolve(WorkLoop.FULL_RUN_REQUIRED), "");
        clock.advance(10);
        parser.armed.set(true);
        Thread runner = new Thread(a.loop::tick, "test-tick");
        runner.setDaemon(true);
        runner.start();
        assertThat(parser.inParse.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        store.stallOn("pharos/");
        long took = timedStop(a, Duration.ofSeconds(2), 15_000);

        assertThat(took).as("stop returned (ms)").isNotNegative().isLessThan(2_000 + 2_000 + 1_500);
        runner.join(5_000);
        assertThat(parser.aborted).as("the run was aborted inside its parse").isTrue();
        store.release();
        assertThat(latest(s)).isEqualTo(before);
    }

    /**
     * Gate run 2 for 1.9.0, w6: the lucene lease was lost 55 s into a full run, and the run went on
     * parsing (and would have embedded and committed) for nothing: its publish fails the lease check.
     * A lost lease aborts the run at its next safe point.
     */
    @Test
    void lostLeaseAbortsTheRunInProgress() throws Exception {
        ShardId s = pathShard("solo");
        SlowParser parser = new SlowParser();
        Writer a = new Writer("node-a", null, SourceCheckout::new, parser);
        a.loop.tick();
        Optional<Generation> before = latest(s);
        change(s, "slow");
        // a FULL run, as w6's (takeover at another checkout path): its parse is one parseFiles call
        Files.writeString(a.lucene.getProjectIndexDir(s.value()).resolve(WorkLoop.FULL_RUN_REQUIRED), "");
        clock.advance(10);
        parser.armed.set(true);
        Thread runner = new Thread(a.loop::tick, "test-tick");
        runner.setDaemon(true);
        runner.start();
        assertThat(parser.inParse.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        // another writer took the lease meanwhile: the renewal finds another ETag
        store.put(layout.leaseKey(s), "{\"writer\":\"node-x\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        a.loop.leaseManager().renewAll();

        runner.join(10_000);
        assertThat(runner.isAlive()).as("the run stopped").isFalse();
        assertThat(parser.aborted).as("aborted inside its parse").isTrue();
        assertThat(latest(s)).isEqualTo(before);
        assertThat(warnings()).anyMatch(m -> m.contains("lost") && m.contains("aborting the run"));
    }

    /**
     * 1.11.0 merge (1.9.0's abort on a definite loss × Phase 6's split projects): a split project's
     * run holds one lease per ring shard, under the project's work unit. Another writer taking any of
     * them aborts the run, as for an unsplit shard; matching the lost lease against the unit's id only
     * (the project name, never a ring shard's) let the run parse on for nothing.
     */
    @Test
    void lostLeaseOfOneRingShardAbortsTheSplitProjectsRun() throws Exception {
        List<ShardId> ring = splitProject("big", 12);
        SlowParser parser = new SlowParser();
        Writer a = new Writer("node-a", null, SourceCheckout::new, parser);
        a.loop.tick();
        Optional<Generation> before = latest(ring.get(1));
        assertThat(before).contains(Generation.FIRST);
        change(new ShardId("big"), "slow");
        Files.writeString(a.lucene.getProjectIndexDir("big").resolve(WorkLoop.FULL_RUN_REQUIRED), "");
        clock.advance(10);
        parser.armed.set(true);
        Thread runner = new Thread(a.loop::tick, "test-tick");
        runner.setDaemon(true);
        runner.start();
        assertThat(parser.inParse.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        // another writer took the second ring shard's lease meanwhile
        store.put(layout.leaseKey(ring.get(1)), "{\"writer\":\"node-x\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        a.loop.leaseManager().renewAll();

        runner.join(10_000);
        assertThat(runner.isAlive()).as("the run stopped").isFalse();
        assertThat(parser.aborted).as("aborted inside its parse").isTrue();
        for (ShardId s : ring) assertThat(latest(s)).isEqualTo(before);
        assertThat(warnings()).anyMatch(m -> m.contains("lost") && m.contains("aborting the run"));
    }

    /**
     * Review I1: a renewal that fails without a definite answer (an I/O error or its deadline on a
     * slow bucket) may leave the lease ours. Aborting would throw a full run away and the next run
     * would be full again; on a bucket slow often enough the shard would never publish. Such a run
     * goes on to its commit; its publish re-checks the lease, and once renewals work again the
     * writer reclaims its untouched lease and publishes incrementally.
     */
    @Test
    void ambiguousRenewalFailureDoesNotAbortTheRun() throws Exception {
        ShardId s = pathShard("solo");
        SlowParser parser = new SlowParser();
        Writer a = new Writer("node-a", null, SourceCheckout::new, parser);
        a.loop.tick();
        Generation before = latest(s).orElseThrow();
        change(s, "slow");
        Files.writeString(a.lucene.getProjectIndexDir(s.value()).resolve(WorkLoop.FULL_RUN_REQUIRED), "");
        clock.advance(10);
        parser.armed.set(true);
        Thread runner = new Thread(a.loop::tick, "test-tick");
        runner.setDaemon(true);
        runner.start();
        assertThat(parser.inParse.await(30, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        store.failPutIfMatchOn(layout.leaseKey(s)); // the renewal fails; the lease object stays ours
        a.loop.leaseManager().renewAll();
        assertThat(a.loop.leaseManager().isHeld(s)).as("seen lost").isFalse();
        Thread.sleep(300);
        assertThat(parser.aborted).as("not aborted on an ambiguous failure").isFalse();

        store.release(); // renewals work again
        parser.armed.set(false);
        parser.finish = true;
        runner.join(30_000);
        assertThat(runner.isAlive()).isFalse();
        assertThat(parser.aborted).isFalse();
        assertThat(latest(s)).as("its publish failed the lease re-check").contains(before);

        rounds(1, 10, a); // reclaims its untouched lease and publishes, incrementally
        assertThat(a.held()).containsExactly(s);
        assertThat(latest(s).orElseThrow().value()).isGreaterThan(before.value());
        assertThat(warnings()).noneMatch(m -> m.contains("aborting the run"));
    }

    /**
     * A parser whose parse of the armed run takes up to 30 s, checking the run's abort every 10 ms
     * like {@link JavaCodeParser} does between files.
     */
    static final class SlowParser extends JavaCodeParser {
        final AtomicBoolean armed = new AtomicBoolean();
        final java.util.concurrent.CountDownLatch inParse = new java.util.concurrent.CountDownLatch(1);
        volatile boolean aborted;
        /** Ends the armed parse normally (not aborted). */
        volatile boolean finish;

        @Override
        public com.pharos.parser.model.ParsedProject parseFiles(List<Path> files, Path projectRoot, String projectName,
                                                                com.pharos.indexer.IndexAbort abort) throws IOException {
            if (armed.get()) {
                inParse.countDown();
                long until = System.nanoTime() + 30_000_000_000L;
                while (System.nanoTime() < until && !finish) {
                    try {
                        abort.checkpoint();
                    } catch (com.pharos.indexer.IndexAbortedException e) {
                        aborted = true;
                        throw e;
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new java.io.InterruptedIOException("interrupted");
                    }
                }
            }
            return super.parseFiles(files, projectRoot, projectName, abort);
        }
    }

    // --------------------------------------------------------------------------- disk guard

    private Set<String> luceneFiles(Writer w, ShardId s) throws IOException {
        try (var f = Files.list(w.lucene.getProjectIndexDir(s.value()).resolve("lucene"))) {
            return f.map(p -> p.getFileName().toString()).filter(n -> !n.equals("write.lock"))
                    .collect(java.util.stream.Collectors.toSet());
        }
    }

    private long graphGenerations(Writer w, ShardId s) throws IOException {
        try (var f = Files.list(w.lucene.getProjectIndexDir(s.value()))) {
            return f.filter(p -> p.getFileName().toString().matches("callgraph-\\d+\\.arcadedb")).count();
        }
    }

    @Test
    void refusesRunThatWouldCrossThreshold() throws IOException {
        ShardId s = pathShard("solo");
        FakeDisk fake = new FakeDisk();
        fake.set(THRESHOLD + (16L << 20)); // not low, but a run's estimate (>= 64 MiB) would cross it
        Writer a = new Writer("node-a", fake);

        a.loop.tick();

        assertThat(latest(s)).as("refused before anything was written").isEmpty();
        assertThat(a.lucene.indexExists(s.value())).isFalse();
        assertThat(a.held()).as("released so a writer with space can take it").isEmpty();
        assertThat(heartbeatOf("node-a").orElseThrow().path("disk").path("low").asBoolean()).isFalse();

        // with space, another writer takes it at once and publishes it
        Writer b = new Writer("node-b", new FakeDisk());
        b.loop.tick();
        assertThat(b.held()).containsExactly(s);
        assertThat(latest(s)).contains(Generation.FIRST);
        // A, which refused it, doesn't race B for it
        rounds(1, 10, a);
        assertThat(a.held()).isEmpty();
    }

    @Test
    void lowDiskAbortsAtSafePointWithoutHalfCommit() throws IOException {
        ShardId s = pathShard("solo");
        FakeDisk fake = new FakeDisk();
        Writer a = new Writer("node-a", fake);
        a.loop.tick();
        assertThat(latest(s)).contains(Generation.FIRST);
        int docs = a.docs(s);
        Set<String> commit = luceneFiles(a, s);
        Path indexDir = a.lucene.getProjectIndexDir(s.value());
        byte[] fileState = Files.readAllBytes(indexDir.resolve("file-state.json"));
        String pointer = Files.readString(indexDir.resolve("callgraph.current"));

        change(s, "big");
        AtomicBoolean fired = new AtomicBoolean();
        a.embedder.onBatch = n -> {
            if (fired.compareAndSet(false, true)) {
                fake.set(THRESHOLD - GIB); // the disk fills up mid-run
                assertThat(a.loop.checkDisk()).isTrue(); // what the disk-guard thread does every checkSeconds
            }
        };
        rounds(1, 10, a);

        assertThat(fired).isTrue();
        assertThat(latest(s)).as("aborted before its commit: nothing published").contains(Generation.FIRST);
        assertThat(a.docs(s)).isEqualTo(docs);
        assertThat(luceneFiles(a, s)).as("the previous commit, exactly").isEqualTo(commit);
        assertThat(Files.readAllBytes(indexDir.resolve("file-state.json"))).isEqualTo(fileState);
        assertThat(Files.readString(indexDir.resolve("callgraph.current"))).isEqualTo(pointer);
        assertThat(graphGenerations(a, s)).isEqualTo(1);
        assertThat(heartbeatOf("node-a").orElseThrow().path("disk").path("low").asBoolean()).isTrue();

        // the previous generation stays publishable: once there is space, the change is published from it
        a.embedder.onBatch = n -> {};
        fake.set(50 * GIB);
        Writer b = new Writer("node-b");
        rounds(1, 10, a, b);
        assertThat(latest(s)).contains(new Generation(2));
    }

    @Test
    void lowWriterReleasesLeasesAndAnotherTakesThem() throws IOException {
        List<ShardId> shards = pathShards(3);
        FakeDisk fake = new FakeDisk();
        Writer a = new Writer("node-a", fake);
        a.loop.tick();
        assertThat(a.held()).hasSize(3);
        ShardId idle = shards.get(0);
        change(shards.get(1), "x");
        change(shards.get(2), "y");
        fake.set(THRESHOLD - GIB);

        rounds(1, 10, a);
        assertThat(a.held()).as("the idle shard's last publish is still served: kept").containsExactly(idle);
        assertThat(latest(shards.get(1))).contains(Generation.FIRST);
        JsonNode hb = heartbeatOf("node-a").orElseThrow();
        assertThat(hb.path("disk").path("low").asBoolean()).isTrue();

        // B doesn't count the low writer: share = 3, it takes both released shards and publishes them
        Writer b = new Writer("node-b");
        rounds(1, 10, a, b);
        assertThat(b.held()).containsExactlyInAnyOrder(shards.get(1), shards.get(2));
        assertThat(latest(shards.get(1))).contains(new Generation(2));
        assertThat(latest(shards.get(2))).contains(new Generation(2));
        // A keeps renewing the idle one and acquires nothing while low
        rounds(10, 10, a, b);
        assertThat(a.held()).containsExactly(idle);
        assertThat(lease(idle).path("writer").asText()).isEqualTo("node-a");
        assertThat(latest(idle)).contains(Generation.FIRST);
    }

    @Test
    void lowWriterKeepsItsPinnedShardAndRefusesItsRuns() throws IOException {
        FakeDisk fake = new FakeDisk();
        Writer a = new Writer("node-a", fake);
        Path dir = tmp.resolve("sources").resolve("handmade");
        Files.createDirectories(dir.resolve("src"));
        Files.writeString(dir.resolve("src/Hand.java"), "public class Hand {\n    public int h(int x) { return x; }\n}\n");
        a.localOnly.index(dir, "handmade", false, false, true, false, ProgressListener.SILENT);
        ShardId hand = new ShardId("handmade");
        catalog.register(hand);
        a.loop.tick();
        Generation g = latest(hand).orElseThrow();
        Files.writeString(dir.resolve("src/Hand.java"), "public class Hand {\n    public int h(int x) { return x; }\n"
                + "    public int k(int x) { return h(x); }\n}\n");
        Files.setLastModifiedTime(dir.resolve("src/Hand.java"), FileTime.fromMillis(System.currentTimeMillis() + 9_000));
        fake.set(THRESHOLD - GIB);
        rounds(3, 10, a);
        assertThat(a.held()).containsExactly(hand);
        assertThat(latest(hand)).contains(g);
        fake.set(50 * GIB);
        rounds(1, 10, a);
        assertThat(latest(hand).orElseThrow().value()).isEqualTo(g.value() + 1);
    }

    /** Final review I5: a low writer says so at WARN once per low episode, not at DEBUG every tick. */
    @Test
    void lowDiskIsAWarnOncePerEpisode() throws IOException {
        pathShard("solo");
        FakeDisk fake = new FakeDisk();
        Writer a = new Writer("node-a", fake);
        a.loop.tick();
        fake.set(THRESHOLD - 1);
        List<String> logs = logsOf(WorkLoop.class, () -> rounds(3, 10, a));
        assertThat(logs.stream().filter(l -> l.contains("is low on disk") && l.contains("starting no run until it recovers")))
                .hasSize(1);
        fake.set(50 * GIB);
        logs = logsOf(WorkLoop.class, () -> rounds(1, 10, a));
        assertThat(logs).anyMatch(l -> l.contains("no longer low on disk"));
        fake.set(THRESHOLD - 1);
        logs = logsOf(WorkLoop.class, () -> rounds(2, 10, a));
        assertThat(logs.stream().filter(l -> l.contains("is low on disk") && l.contains("starting no run until it recovers")))
                .as("a new episode").hasSize(1);
    }

    @Test
    void recoversWithHysteresis() throws IOException {
        ShardId s = pathShard("solo");
        FakeDisk fake = new FakeDisk();
        Writer a = new Writer("node-a", fake);
        a.loop.tick();
        change(s, "later");
        fake.set(THRESHOLD - 1);
        rounds(1, 10, a);
        assertThat(a.disk.low()).isTrue();
        // back above the threshold, but within the 1 GiB margin: still low, nothing runs or is acquired
        fake.set(THRESHOLD + GIB / 2);
        rounds(1, 10, a);
        assertThat(a.disk.low()).isTrue();
        assertThat(heartbeatOf("node-a").orElseThrow().path("disk").path("low").asBoolean()).isTrue();
        assertThat(latest(s)).contains(Generation.FIRST);
        // above threshold + margin: recovered; after its avoid period A takes the shard back and publishes
        fake.set(THRESHOLD + GIB + 1);
        rounds(1, 10, a);
        assertThat(a.disk.low()).isFalse();
        assertThat(heartbeatOf("node-a").orElseThrow().path("disk").path("low").asBoolean()).isFalse();
        fake.set(50 * GIB);
        rounds(13, 10, a);
        assertThat(a.held()).containsExactly(s);
        assertThat(latest(s)).contains(new Generation(2));
    }

    @Test
    void gcRunsBeforeRefusing() throws IOException {
        ShardId s = pathShard("solo");
        ShardId other = pathShard("other");
        FakeDisk fake = new FakeDisk();
        Writer a = new Writer("node-a", fake);
        a.loop.tick();
        Path dir = a.lucene.getProjectIndexDir(other.value());
        // what a crashed run of "other" left: a retired graph generation and its spools
        Path retired = Files.createDirectories(dir.resolve("callgraph-1.arcadedb"));
        Files.write(retired.resolve("data"), new byte[4096]);
        Files.write(dir.resolve("embed.spool"), new byte[1024]);
        Files.write(dir.resolve("embed-incr.test-counting.cache"), new byte[1024]);
        Set<String> live = luceneFiles(a, other);
        String pointer = Files.readString(dir.resolve("callgraph.current"));
        List<String> caches;
        try (var f = Files.list(dir)) {
            caches = f.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("embed-cache")).sorted().toList();
        }
        assertThat(caches).isNotEmpty();
        // low while the leftovers are there; enough room once they are gone
        fake.dynamic = () -> Files.exists(retired) ? THRESHOLD - GIB : 50 * GIB;
        change(s, "afterGc");

        rounds(1, 10, a);

        assertThat(retired).doesNotExist();
        assertThat(dir.resolve("embed.spool")).doesNotExist();
        assertThat(dir.resolve("embed-incr.test-counting.cache")).doesNotExist();
        assertThat(luceneFiles(a, other)).isEqualTo(live);
        assertThat(Files.readString(dir.resolve("callgraph.current"))).isEqualTo(pointer);
        assertThat(dir.resolve(pointer.trim())).isDirectory();
        try (var f = Files.list(dir)) {
            assertThat(f.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("embed-cache")).sorted()
                    .toList()).as("the persistent embedding cache is kept").isEqualTo(caches);
        }
        assertThat(a.disk.low()).isFalse();
        assertThat(a.held()).containsExactlyInAnyOrder(s, other);
        assertThat(latest(s)).as("not refused: the GC made room").contains(new Generation(2));
    }

    @Test
    void heartbeatReportsDisk() throws IOException {
        pathShard("solo");
        FakeDisk fake = new FakeDisk();
        fake.set(40 * GIB);
        Writer a = new Writer("node-a", fake);
        a.loop.tick();
        JsonNode disk = heartbeatOf("node-a").orElseThrow().path("disk");
        assertThat(disk.path("freeBytes").asLong()).isEqualTo(40 * GIB);
        assertThat(disk.path("totalBytes").asLong()).isEqualTo(100 * GIB);
        assertThat(disk.path("low").asBoolean()).isFalse();
        WriterHeartbeat parsed = JSON.readValue(store.get(layout.writerKey("node-a")).orElseThrow().data(),
                WriterHeartbeat.class);
        assertThat(parsed.takesShare()).isTrue();

        fake.set(GIB);
        rounds(1, 10, a);
        disk = heartbeatOf("node-a").orElseThrow().path("disk");
        assertThat(disk.path("freeBytes").asLong()).isEqualTo(GIB);
        assertThat(disk.path("low").asBoolean()).isTrue();
        parsed = JSON.readValue(store.get(layout.writerKey("node-a")).orElseThrow().data(), WriterHeartbeat.class);
        assertThat(parsed.takesShare()).as("the balancer skips a low writer").isFalse();
        assertThat(a.loop.diskStatus().orElseThrow().toMap()).containsEntry("low", true)
                .containsEntry("freeBytes", GIB);

        // a writer without a guard writes no disk field
        Writer b = new Writer("node-b");
        b.loop.tick();
        assertThat(heartbeatOf("node-b").orElseThrow().has("disk")).isFalse();
    }

    // ------------------------------------------------- run estimates (fix round 1, I1)

    private Path root(ShardId s) {
        return sourceDirs.get(s).toAbsolutePath().normalize();
    }

    @Test
    void estimateIsFullWhenTheIndexFormatDiffers() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a", new FakeDisk());
        a.loop.tick();
        Path dir = a.lucene.getProjectIndexDir(s.value());
        DiskGuard.EstimatedRun before = a.loop.estimate(s, dir, root(s), false);
        assertThat(before.full()).isFalse();
        assertThat(before.changedBytes()).as("nothing changed").isZero();

        // what a release bumping INDEX_FORMAT leaves: the indexer runs this one full
        com.pharos.indexer.IndexFormat.write(dir, new com.pharos.indexer.IndexFormat.Stamp(
                com.pharos.indexer.IndexVersions.INDEX_FORMAT - 1, "older", Instant.now()));
        Path stamp = dir.resolve(com.pharos.indexer.IndexFormat.FILE_NAME);
        Files.setLastModifiedTime(stamp, FileTime.fromMillis(Files.getLastModifiedTime(stamp).toMillis() + 5_000));
        assertThat(a.manager.fullRunReason(s.value())).isNotNull();
        DiskGuard.EstimatedRun after = a.loop.estimate(s, dir, root(s), false);
        assertThat(after.full()).isTrue();
        assertThat(after.peakBytes()).isGreaterThan(2 * after.luceneBytes());
    }

    @Test
    void estimateCountsFilesEmbeddedUnderAnotherChunkingVersionAsChanged() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a", new FakeDisk());
        a.loop.tick();
        Path dir = a.lucene.getProjectIndexDir(s.value());
        assertThat(a.loop.estimate(s, dir, root(s), false).changedBytes()).isZero();

        // what a release bumping CHUNKING_VERSION leaves: every tracked file is re-embedded
        Path state = dir.resolve("file-state.json");
        JsonNode tree = JSON.readTree(state.toFile());
        tree.fields().forEachRemaining(e -> ((com.fasterxml.jackson.databind.node.ObjectNode) e.getValue())
                .put("chunkingVersion", com.pharos.indexer.IndexVersions.CHUNKING_VERSION - 1));
        JSON.writeValue(state.toFile(), tree);

        DiskGuard.EstimatedRun e = a.loop.estimate(s, dir, root(s), false);
        assertThat(e.full()).isFalse();
        assertThat(e.changedBytes()).as("the tracked source counts as changed").isPositive()
                .isEqualTo(e.sourceBytes());
    }

    @Test
    void firstIndexEstimateUsesTheSourceMultiplier() throws IOException {
        ShardId s = pathShard("solo");
        Writer a = new Writer("node-a", new FakeDisk());
        DiskGuard.EstimatedRun e = a.loop.estimateWithoutLocalIndex(s, root(s));
        long source = e.sourceBytes();
        assertThat(source).isPositive();
        assertThat(e.full()).isTrue();
        int lucene = DiskGuard.EstimatedRun.FIRST_INDEX_LUCENE_FACTOR, cache = DiskGuard.EstimatedRun.FIRST_INDEX_CACHE_FACTOR;
        assertThat(e.peakBytes()).as("spool + %dx Lucene + %dx cache + overhead", lucene, cache)
                .isEqualTo((1 + lucene + cache) * source + DiskGuard.EstimatedRun.OVERHEAD_BYTES);
    }

    @Test
    void abortedRunRaisesTheNextEstimate() throws IOException {
        ShardId s = pathShard("solo");
        FakeDisk fake = new FakeDisk(); // 50 GiB free
        Writer a = new Writer("node-a", fake);
        a.loop.tick();
        Path dir = a.lucene.getProjectIndexDir(s.value());
        change(s, "huge");
        DiskGuard.EstimatedRun small = a.loop.estimate(s, dir, root(s), false);
        assertThat(small.peakBytes()).isLessThan(GIB);

        // the run eats 41 GiB before the guard aborts it
        AtomicBoolean fired = new AtomicBoolean();
        a.embedder.onBatch = n -> {
            if (fired.compareAndSet(false, true)) {
                fake.set(THRESHOLD - GIB);
                a.loop.checkDisk();
            }
        };
        rounds(1, 10, a);
        assertThat(fired).isTrue();
        assertThat(latest(s)).contains(Generation.FIRST);
        assertThat(a.loop.estimateFloor(s)).isEqualTo(41 * GIB);

        // space is back (40 GiB above the threshold), but the run is known to need more: not admitted
        a.embedder.onBatch = n -> {};
        fake.set(50 * GIB);
        a.disk.check();
        DiskGuard.EstimatedRun next = a.loop.estimate(s, dir, root(s), false);
        assertThat(next.peakBytes()).isEqualTo(41 * GIB);
        assertThat(a.disk.admits(next)).isFalse();
        assertThat(a.disk.admits(small)).as("the old estimate would have been admitted again").isTrue();
        assertThat(a.loop.estimateWithoutLocalIndex(s, root(s)).peakBytes()).isGreaterThanOrEqualTo(41 * GIB);
    }

    // ----------------------------------------------------------- release rules (M4, M5)

    @Test
    void failingPollNeverLeadsToAnIdleRelease() throws IOException {
        List<ShardId> shards = pathShards(2);
        Writer a = new Writer("node-a");
        a.loop.tick();
        Writer b = new Writer("node-b");
        // both of A's polls fail from now on: it can't confirm LATEST covers either source
        for (ShardId s : shards) store.failGetOn(layout.latestKey(s));
        List<String> logs = logsOf(WorkLoop.class, () -> rounds(20, 10, a, b)); // 200 s: past minHold, 2 > 1
        // never as an idle shard (M5); the failed checks release one as a failing shard (final review I4)
        assertThat(logs).noneMatch(l -> l.startsWith("Releasing idle shard"));
        assertThat(logs).anyMatch(l -> l.contains("failed runs or source checks in a row"));
        assertThat(a.held()).hasSize(1);
        assertThat(b.held()).hasSize(1);
        // polls succeed again: nothing more moves
        store.clearFailures();
        rounds(2, 10, a, b);
        assertThat(a.held()).hasSize(1);
        assertThat(b.held()).hasSize(1);
    }

    @Test
    void failingSurplusShardIsReleasedAfterMaxConsecutiveFailures() throws IOException {
        List<ShardId> shards = pathShards(2);
        Writer a = new Writer("node-a");
        a.loop.tick();
        rounds(13, 10, a); // past minHold
        for (ShardId s : shards) change(s, "broken");
        a.embedder.onBatch = n -> { throw new IllegalStateException("injected embed failure"); };
        Writer b = new Writer("node-b");
        b.loop.tick();

        rounds(2, 10, a, b);
        assertThat(a.held()).as("two failures in a row: retried, kept").hasSize(2);
        assertThat(b.held()).isEmpty();
        for (ShardId s : shards) assertThat(latest(s)).contains(Generation.FIRST);

        rounds(1, 10, a, b); // third failure: one surplus shard released (the other is A's share); B takes it
        assertThat(a.held()).hasSize(1);
        assertThat(b.held()).hasSize(1).doesNotContainAnyElementsOf(a.held());
        ShardId moved = b.held().iterator().next();
        assertThat(latest(moved)).as("another writer could index it").contains(new Generation(2));
        assertThat(replicas.manifest(moved, new Generation(2)).writerNode()).isEqualTo("node-b");
        // the one A keeps is not released for failing: it is its share
        rounds(5, 10, a, b);
        assertThat(a.held()).hasSize(1).doesNotContain(moved);
    }

    private static String git(Path dir, String... args) throws Exception {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "git";
        System.arraycopy(args, 0, cmd, 1, args.length);
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true);
        pb.environment().put("GIT_CONFIG_GLOBAL", "/dev/null");
        pb.environment().put("GIT_CONFIG_SYSTEM", "/dev/null");
        pb.environment().put("GIT_AUTHOR_NAME", "t");
        pb.environment().put("GIT_AUTHOR_EMAIL", "t@example.org");
        pb.environment().put("GIT_COMMITTER_NAME", "t");
        pb.environment().put("GIT_COMMITTER_EMAIL", "t@example.org");
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes());
        assertThat(p.waitFor()).as("git %s: %s", String.join(" ", args), out).isZero();
        return out.trim();
    }
}
