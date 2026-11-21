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
    private InMemoryObjectStore store;
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

        Writer(String node) {
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
                    List.of(new JavaCodeParser()), publisher);
            localOnly = new ProjectIndexManager(config, lucene, registry, List.of(embedder), noopModules,
                    List.of(new JavaCodeParser()));
            loop = new WorkLoop(new WorkLoop.Settings(node, TTL, HEARTBEAT, Duration.ofSeconds(60), MIN_HOLD, true, false),
                    store, layout, catalog, replicas, state, manager, lucene, registry,
                    new SourceCheckout(home.resolve("work")), clock);
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
        store = new InMemoryObjectStore(clock);
        layout = new ObjectStoreLayout("pharos");
        catalog = new ObjectStoreCatalog(store, layout);
        replicas = new ObjectStoreReplicaSource(store, layout);
    }

    @AfterEach
    void tearDown() {
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
