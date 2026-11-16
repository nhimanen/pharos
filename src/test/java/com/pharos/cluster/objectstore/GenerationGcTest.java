package com.pharos.cluster.objectstore;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class GenerationGcTest {

    private static final ShardId SHARD = new ShardId("proj");
    private static final Instant T0 = Instant.parse("2026-11-15T12:00:00Z");
    private static final Duration MIN_AGE = Duration.ofHours(1);

    /** Settable clock driving the store's lastModified. */
    static final class MutableClock extends Clock {
        Instant now;
        MutableClock(Instant now) { this.now = now; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    private MutableClock clock;
    private InMemoryObjectStore store;
    private ObjectStoreLayout layout;
    private GenerationGc gc;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(T0);
        store = new InMemoryObjectStore(clock);
        layout = new ObjectStoreLayout("pharos");
        gc = new GenerationGc(store, layout, 3, MIN_AGE);
    }

    // --- fixtures -------------------------------------------------------------------------------

    private String shared() { return layout.luceneKey(SHARD, "a", "1"); }
    private String unique(long g) { return layout.luceneKey(SHARD, "u" + g, Long.toHexString(g)); }
    private String graph(long g) { return layout.graphKey(SHARD, new Generation(g), "c" + g); }
    private String refs(long g) { return layout.refsKey(SHARD, new Generation(g), "d" + g); }
    private String manifestKey(long g) { return layout.manifestKey(SHARD, new Generation(g)); }

    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    /** Writes gen g's files and manifest at store time {@code at} (createdAt = at). */
    private void writeGeneration(long g, Instant at) throws IOException {
        clock.now = at;
        store.put(shared(), bytes("shared"));
        store.put(unique(g), bytes("u" + g));
        store.put(graph(g), bytes("g" + g));
        store.put(refs(g), bytes("r" + g));
        List<ManifestFile> files = List.of(
                new ManifestFile(ManifestFile.LUCENE, "a", shared(), 6, "1"),
                new ManifestFile(ManifestFile.LUCENE, "u" + g, unique(g), 2, Long.toHexString(g)),
                new ManifestFile(ManifestFile.GRAPH, "graph.cgf", graph(g), 2, "c" + g),
                new ManifestFile(ManifestFile.REFS, "unresolved-refs.json.gz", refs(g), 2, "d" + g));
        ShardManifest m = new ShardManifest(ShardManifest.FORMAT_VERSION, SHARD.value(), g,
                g == 1 ? null : g - 1, at, "node-a", 1, 12, "none", files, null);
        store.put(manifestKey(g), m.toJson());
    }

    private void writeLatest(long g, Instant at) {
        clock.now = at;
        store.put(layout.latestKey(SHARD), bytes("{\"generation\": " + g + ", \"manifest\": \"gen-" + g + ".json\"}"));
    }

    /** Gens 1..n written at {@code at}, LATEST -> n. */
    private void generations(int n, Instant at) throws IOException {
        for (long g = 1; g <= n; g++) writeGeneration(g, at);
        writeLatest(n, at);
    }

    private boolean exists(String key) { return store.get(key).isPresent(); }

    private void assertGenerationGone(long g) {
        assertThat(exists(manifestKey(g))).as("manifest %d", g).isFalse();
        assertThat(exists(unique(g))).as("unique %d", g).isFalse();
        assertThat(exists(graph(g))).as("graph %d", g).isFalse();
        assertThat(exists(refs(g))).as("refs %d", g).isFalse();
    }

    private void assertGenerationIntact(long g) {
        assertThat(exists(manifestKey(g))).as("manifest %d", g).isTrue();
        assertThat(exists(unique(g))).as("unique %d", g).isTrue();
        assertThat(exists(graph(g))).as("graph %d", g).isTrue();
        assertThat(exists(refs(g))).as("refs %d", g).isTrue();
    }

    // --- brief tests ----------------------------------------------------------------------------

    @Test
    void keepsLastThreeAndYoung() throws Exception {
        generations(5, T0.minus(Duration.ofHours(2)));

        int deleted = gc.collect(SHARD, T0);

        for (long g = 3; g <= 5; g++) assertGenerationIntact(g);
        assertGenerationGone(1);
        assertGenerationGone(2);
        assertThat(exists(shared())).isTrue();
        assertThat(exists(layout.latestKey(SHARD))).isTrue();
        assertThat(deleted).isEqualTo(8); // 2 manifests + 2 unique + 2 graph + 2 refs
    }

    @Test
    void youngGenerationsSurviveEvenBeyondCount() throws Exception {
        generations(5, T0.minus(Duration.ofMinutes(10)));

        assertThat(gc.collect(SHARD, T0)).isZero();

        for (long g = 1; g <= 5; g++) assertGenerationIntact(g);
    }

    @Test
    void orphanUploadsYoungerThanMinAgeSurvive() throws Exception {
        generations(3, T0.minus(Duration.ofHours(2)));
        String orphan = layout.luceneKey(SHARD, "x", "9");
        clock.now = T0.minus(Duration.ofMinutes(10));
        store.put(orphan, bytes("x"));

        assertThat(gc.collect(SHARD, T0)).isZero();
        assertThat(exists(orphan)).isTrue();

        clock.now = T0.minus(Duration.ofHours(2));
        store.put(orphan, bytes("x"));
        assertThat(gc.collect(SHARD, T0)).isEqualTo(1);
        assertThat(exists(orphan)).isFalse();
    }

    @Test
    void usesStoreClockNotLocal() throws Exception {
        // The store's clock is a day behind this machine's: everything is 10 minutes old by the
        // store's clock but a day old by Instant.now().
        Instant storeNow = Instant.now().minus(Duration.ofDays(1));
        generations(5, storeNow.minus(Duration.ofMinutes(10)));

        assertThat(gc.collect(SHARD, storeNow)).isZero();
        for (long g = 1; g <= 5; g++) assertGenerationIntact(g);

        // and by the store's clock two hours later, gens 1-2 go
        assertThat(gc.collect(SHARD, storeNow.plus(Duration.ofHours(2)))).isEqualTo(8);
        assertGenerationGone(1);
        assertGenerationGone(2);
    }

    // --- review decisions -----------------------------------------------------------------------

    @Test
    void latestTargetNeverDeleted() throws Exception {
        Instant old = T0.minus(Duration.ofHours(5));
        generations(5, old);
        // LATEST points at gen 2 (e.g. 3-5 are orphans of crashed runs, all old)
        writeLatest(2, old);
        GenerationGc keepTwo = new GenerationGc(store, layout, 2, MIN_AGE);

        keepTwo.collect(SHARD, T0);

        assertGenerationIntact(2);
        assertGenerationIntact(1); // second-newest at or below LATEST
        for (long g = 3; g <= 5; g++) assertGenerationGone(g);
        assertThat(exists(shared())).isTrue();
    }

    @Test
    void latestTargetProtectedEvenWithOtherManifestNamedInLatest() throws Exception {
        Instant old = T0.minus(Duration.ofHours(5));
        generations(5, old);
        // LATEST names an explicit manifest file whose generation is lower than the "generation" field
        clock.now = old;
        store.put(layout.latestKey(SHARD), bytes("{\"generation\": 5, \"manifest\": \"gen-1.json\"}"));

        gc.collect(SHARD, T0);

        assertGenerationIntact(1);
        assertThat(exists(shared())).isTrue();
    }

    @Test
    void orphanNewerManifestYoungKeptOldDeleted() throws Exception {
        generations(3, T0.minus(Duration.ofHours(2)));
        writeGeneration(4, T0.minus(Duration.ofMinutes(10))); // in-flight publish, LATEST still 3

        assertThat(gc.collect(SHARD, T0)).isZero();
        assertGenerationIntact(4);

        // the same orphan, now old: deleted along with its unique files
        assertThat(gc.collect(SHARD, T0.plus(Duration.ofHours(1)))).isEqualTo(4);
        assertGenerationGone(4);
        for (long g = 1; g <= 3; g++) assertGenerationIntact(g);
        assertThat(exists(layout.latestKey(SHARD))).isTrue();
    }

    @Test
    void unreferencedGraphAndRefsDeletedAfterMinAge() throws Exception {
        generations(3, T0.minus(Duration.ofHours(2)));
        // losing racers for gen 3 left content-named extras
        String extraGraph = layout.graphKey(SHARD, new Generation(3), "ffff");
        String extraRefs = layout.refsKey(SHARD, new Generation(3), "eeee");
        clock.now = T0.minus(Duration.ofMinutes(10));
        store.put(extraGraph, bytes("eg"));
        store.put(extraRefs, bytes("er"));

        assertThat(gc.collect(SHARD, T0)).isZero();
        assertThat(exists(extraGraph)).isTrue();
        assertThat(exists(extraRefs)).isTrue();

        assertThat(gc.collect(SHARD, T0.plus(Duration.ofHours(1)))).isEqualTo(2);
        assertThat(exists(extraGraph)).isFalse();
        assertThat(exists(extraRefs)).isFalse();
        for (long g = 1; g <= 3; g++) assertGenerationIntact(g);
    }

    @Test
    void missingLatestDeletesNothing() throws Exception {
        generations(5, T0.minus(Duration.ofHours(2)));
        store.delete(layout.latestKey(SHARD));
        String orphan = layout.luceneKey(SHARD, "x", "9");
        store.put(orphan, bytes("x"));

        assertThat(gc.collect(SHARD, T0)).isZero();
        for (long g = 1; g <= 5; g++) assertGenerationIntact(g);
        assertThat(exists(orphan)).isTrue();
    }

    @Test
    void unreadableLatestDeletesNothing() throws Exception {
        generations(5, T0.minus(Duration.ofHours(2)));
        store.put(layout.latestKey(SHARD), bytes("not json"));

        assertThat(gc.collect(SHARD, T0)).isZero();
        for (long g = 1; g <= 5; g++) assertGenerationIntact(g);
    }

    @Test
    void unparseableManifestAbortsGc() throws Exception {
        generations(5, T0.minus(Duration.ofHours(2)));
        store.put(manifestKey(1), bytes("{ truncated"));

        assertThat(gc.collect(SHARD, T0)).isZero();
        assertThat(exists(manifestKey(1))).isTrue();
        assertThat(exists(unique(1))).isTrue();
        for (long g = 2; g <= 5; g++) assertGenerationIntact(g);
    }

    @Test
    void listingFailureDeletesNothing() throws Exception {
        generations(5, T0.minus(Duration.ofHours(2)));
        ThrowingStore spy = new ThrowingStore(store);
        spy.failListPrefix = layout.shardPrefix(SHARD);

        assertThat(new GenerationGc(spy, layout, 3, MIN_AGE).collect(SHARD, T0)).isZero();
        assertThat(spy.deletes).isEmpty();
        for (long g = 1; g <= 5; g++) assertGenerationIntact(g);
    }

    @Test
    void manifestReadFailureDeletesNothing() throws Exception {
        generations(5, T0.minus(Duration.ofHours(2)));
        ThrowingStore spy = new ThrowingStore(store);
        spy.failGetKey = manifestKey(4);

        assertThat(new GenerationGc(spy, layout, 3, MIN_AGE).collect(SHARD, T0)).isZero();
        assertThat(spy.deletes).isEmpty();
    }

    @Test
    void deleteFailureIsBestEffort() throws Exception {
        generations(5, T0.minus(Duration.ofHours(2)));
        ThrowingStore spy = new ThrowingStore(store);
        spy.failDeleteKey = unique(1);

        assertThat(new GenerationGc(spy, layout, 3, MIN_AGE).collect(SHARD, T0)).isEqualTo(7);
        assertThat(exists(unique(1))).isTrue();
        assertGenerationGone(2);
    }

    /** Delegating store whose list/get/delete can be made to throw. */
    static final class ThrowingStore implements ObjectStore {
        final ObjectStore delegate;
        final List<String> deletes = new ArrayList<>();
        String failListPrefix;
        String failGetKey;
        String failDeleteKey;

        ThrowingStore(ObjectStore delegate) { this.delegate = delegate; }

        @Override public Optional<StoredObject> get(String key) throws IOException {
            if (key.equals(failGetKey)) throw new IOException("injected get failure: " + key);
            return delegate.get(key);
        }
        @Override public void download(String key, Path target) throws IOException { delegate.download(key, target); }
        @Override public String put(String key, Path source) throws IOException { return delegate.put(key, source); }
        @Override public String put(String key, byte[] data) throws IOException { return delegate.put(key, data); }
        @Override public Optional<String> putIfAbsent(String key, byte[] data) throws IOException {
            return delegate.putIfAbsent(key, data);
        }
        @Override public Optional<String> putIfMatch(String key, byte[] data, String etag) throws IOException {
            return delegate.putIfMatch(key, data, etag);
        }
        @Override public List<ObjectInfo> list(String prefix) throws IOException {
            if (failListPrefix != null && (failListPrefix.startsWith(prefix) || prefix.startsWith(failListPrefix))) {
                throw new IOException("injected list failure: " + prefix);
            }
            return delegate.list(prefix);
        }
        @Override public void delete(String key) throws IOException {
            if (key.equals(failDeleteKey)) throw new IOException("injected delete failure: " + key);
            deletes.add(key);
            delegate.delete(key);
        }
        @Override public void close() {}
    }
}
