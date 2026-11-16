package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.IndexPublisher;
import com.pharos.cluster.LocalSnapshot;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.PublishException;
import com.pharos.cluster.ShardId;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.EmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObjectStorePublisherTest {

    private static final String PROJECT = "sample-app";
    private static final ShardId SHARD = new ShardId(PROJECT);
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir Path tmp;

    private Path source;
    private Path synonyms;
    private SpyStore store;
    private ObjectStoreLayout layout;
    private ObjectStoreCatalog catalog;
    private ObjectStorePublisher publisher;

    /** Delegating store that records unconditional puts and runs hooks before each write. */
    static final class SpyStore implements ObjectStore {
        final InMemoryObjectStore delegate = new InMemoryObjectStore();
        final List<String> putKeys = new ArrayList<>();
        /** Keys whose next {@code get} returns empty once (a stale read). */
        final Set<String> hideOnce = new HashSet<>();
        Consumer<String> beforePut = k -> {};
        Consumer<String> beforeConditionalPut = k -> {};

        @Override public Optional<StoredObject> get(String key) {
            return hideOnce.remove(key) ? Optional.empty() : delegate.get(key);
        }
        @Override public void download(String key, Path target) throws IOException { delegate.download(key, target); }
        @Override public String put(String key, Path src) throws IOException {
            beforePut.accept(key);
            putKeys.add(key);
            return delegate.put(key, src);
        }
        @Override public String put(String key, byte[] data) {
            beforePut.accept(key);
            putKeys.add(key);
            return delegate.put(key, data);
        }
        @Override public Optional<String> putIfAbsent(String key, byte[] data) {
            beforeConditionalPut.accept(key);
            return delegate.putIfAbsent(key, data);
        }
        @Override public Optional<String> putIfMatch(String key, byte[] data, String etag) {
            beforeConditionalPut.accept(key);
            return delegate.putIfMatch(key, data, etag);
        }
        @Override public List<ObjectInfo> list(String prefix) { return delegate.list(prefix); }
        @Override public void delete(String key) { delegate.delete(key); }
        @Override public void close() {}
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
        synonyms = tmp.resolve("synonyms.txt");
        store = new SpyStore();
        layout = new ObjectStoreLayout("pharos");
        catalog = new ObjectStoreCatalog(store, layout);
        publisher = new ObjectStorePublisher(store, layout, catalog, gc(), "node-a", synonyms);
    }

    /** The real GC with the default retention, as production wires it. */
    private GenerationGc gc() {
        ClusterConfig cfg = new ClusterConfig();
        return new GenerationGc(store, layout, cfg.getRetentionGenerations(),
                Duration.ofMinutes(cfg.getRetentionMinutes()));
    }

    private ProjectIndexManager manager(IndexPublisher pub) {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        LuceneIndexer lucene = new LuceneIndexer(config);
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(
                    Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        return new ProjectIndexManager(config, lucene, registry,
                List.of(EmbeddingProvider.create(config)), noopModules,
                List.of(new JavaCodeParser()), pub);
    }

    private void indexFull(ProjectIndexManager m) throws IOException {
        m.index(source, PROJECT, false, false, false, false, ProgressListener.SILENT);
    }

    private void touchOneFileAndReindex(ProjectIndexManager m) throws IOException {
        Path file;
        try (Stream<Path> s = Files.walk(source)) {
            file = s.filter(p -> p.toString().endsWith("Calculator.java")).findFirst().orElseThrow();
        }
        Files.writeString(file, Files.readString(file).replace("class Calculator {",
                "class Calculator {\n    public int triple(int x) { return 3 * x; }\n"));
        Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        m.index(source, PROJECT, true, false, false, false, ProgressListener.SILENT);
    }

    private Path luceneDir() {
        return tmp.resolve("indexes").resolve(PROJECT).resolve("lucene");
    }

    private JsonNode json(String key) throws IOException {
        return JSON.readTree(store.get(key).orElseThrow(() -> new AssertionError("missing " + key)).data());
    }

    private JsonNode manifest(long gen) throws IOException {
        return json(layout.manifestKey(SHARD, new Generation(gen)));
    }

    private static Set<String> keys(JsonNode manifest) {
        Set<String> out = new HashSet<>();
        manifest.get("files").forEach(f -> out.add(f.get("key").asText()));
        return out;
    }

    @Test
    void firstPublishWritesGenerationOne() throws Exception {
        indexFull(manager(publisher));

        JsonNode latest = json(layout.latestKey(SHARD));
        assertThat(latest.get("generation").asLong()).isEqualTo(1);
        assertThat(latest.get("manifest").asText()).isEqualTo("gen-1.json");
        assertThat(latest.size()).isEqualTo(2);

        JsonNode m = manifest(1);
        assertThat(m.get("formatVersion").asInt()).isEqualTo(1);
        assertThat(m.get("shard").asText()).isEqualTo(PROJECT);
        assertThat(m.get("generation").asLong()).isEqualTo(1);
        assertThat(m.get("parentGeneration").isNull()).isTrue();
        assertThat(m.get("writerNode").asText()).isEqualTo("node-a");
        assertThat(m.get("createdAt").isTextual()).isTrue();
        assertThat(m.get("docCount").asLong()).isPositive();
        assertThat(m.get("projectMeta").get("name").asText()).isEqualTo(PROJECT);
        assertThat(m.get("projectMeta").get("unresolvedRefs")).isEmpty();

        Collection<String> commitFiles;
        try (FSDirectory dir = FSDirectory.open(luceneDir())) {
            commitFiles = SegmentInfos.readLatestCommit(dir).files(true);
        }
        List<String> luceneNames = new ArrayList<>();
        int graphFiles = 0;
        for (JsonNode f : m.get("files")) {
            String role = f.get("role").asText();
            if (role.equals("lucene")) luceneNames.add(f.get("name").asText());
            if (role.equals("graph")) graphFiles++;
            assertThat(store.get(f.get("key").asText())).as(f.get("key").asText()).isPresent();
        }
        assertThat(luceneNames).containsExactlyInAnyOrderElementsOf(commitFiles);
        assertThat(graphFiles).isEqualTo(1);
        assertThat(catalog.shards()).containsExactly(SHARD);

        // round-trip through the typed record
        ShardManifest typed = ShardManifest.read(store.get(layout.manifestKey(SHARD, Generation.FIRST))
                .orElseThrow().data());
        assertThat(typed.generation()).isEqualTo(1);
        assertThat(typed.files()).hasSize(m.get("files").size());
        assertThat(typed.projectMeta().getName()).isEqualTo(PROJECT);
        assertThat(new String(ShardManifest.read(typed.toJson()).toJson(), StandardCharsets.UTF_8))
                .isEqualTo(new String(typed.toJson(), StandardCharsets.UTF_8));
    }

    @Test
    void secondPublishUploadsOnlyNewFiles() throws Exception {
        ProjectIndexManager m = manager(publisher);
        indexFull(m);
        Set<String> gen1 = keys(manifest(1));
        store.putKeys.clear();

        touchOneFileAndReindex(m);

        JsonNode m2 = manifest(2);
        assertThat(m2.get("parentGeneration").asLong()).isEqualTo(1);
        assertThat(json(layout.latestKey(SHARD)).get("generation").asLong()).isEqualTo(2);
        Set<String> gen2 = keys(m2);
        Set<String> expectedNew = new HashSet<>(gen2);
        expectedNew.removeAll(gen1);
        assertThat(store.putKeys).doesNotHaveDuplicates();
        assertThat(new HashSet<>(store.putKeys)).isEqualTo(expectedNew);

        // A file with the same name and checksum as in gen 1 keeps gen 1's key. (On this tiny
        // sample the incremental commit merges every segment, so this may share nothing.)
        Set<String> gen1Lucene = luceneEntries(manifest(1));
        for (String entry : luceneEntries(m2)) {
            String nameAndChecksum = entry.substring(0, entry.indexOf(" -> "));
            gen1Lucene.stream().filter(e -> e.startsWith(nameAndChecksum + " -> "))
                    .forEach(e -> assertThat(entry).isEqualTo(e));
        }

        // Deterministic reuse: republishing the same commit reuses every Lucene key of gen 2
        // and uploads only the per-generation graph.
        store.putKeys.clear();
        Path shardDir = tmp.resolve("indexes").resolve(PROJECT);
        ProjectMeta meta = ProjectRegistry.at(tmp.resolve("registry.json")).find(PROJECT).orElseThrow();
        assertThat(publisher.publish(new LocalSnapshot(SHARD.value(), shardDir, meta, "none")))
                .contains(new Generation(3));
        JsonNode m3 = manifest(3);
        assertThat(m3.get("parentGeneration").asLong()).isEqualTo(2);
        assertThat(luceneEntries(m3)).isEqualTo(luceneEntries(m2));
        Set<String> expectedNew3 = keys(m3);
        expectedNew3.removeAll(gen2);
        assertThat(expectedNew3).singleElement().asString()
                .startsWith("pharos/shards/sample-app/graph/gen-3@").endsWith(".cgf.gz");
        assertThat(new HashSet<>(store.putKeys)).isEqualTo(expectedNew3);
    }

    private static JsonNode fileWithRole(JsonNode manifest, String role) {
        for (JsonNode f : manifest.get("files")) {
            if (f.get("role").asText().equals(role)) return f;
        }
        throw new AssertionError("no " + role + " file in manifest");
    }

    private static String crc32Hex(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return Long.toHexString(crc.getValue());
    }

    @Test
    void orphanManifestIsReplacedWhenLatestDidNotMove() throws Exception {
        ProjectIndexManager m = manager(publisher);
        indexFull(m);
        String orphanKey = layout.manifestKey(SHARD, new Generation(2));
        store.delegate.put(orphanKey, "{\"orphan\": true}".getBytes(StandardCharsets.UTF_8));

        touchOneFileAndReindex(m);

        JsonNode latest = json(layout.latestKey(SHARD));
        assertThat(latest.get("generation").asLong()).isEqualTo(2);
        JsonNode m2 = manifest(2);
        assertThat(m2.has("orphan")).isFalse();
        assertThat(m2.get("generation").asLong()).isEqualTo(2);
        assertThat(m2.get("parentGeneration").asLong()).isEqualTo(1);
        assertThat(m2.get("writerNode").asText()).isEqualTo("node-a");
        for (String key : keys(m2)) assertThat(store.get(key)).as(key).isPresent();
    }

    @Test
    void orphanFirstManifestIsReplacedWhenLatestAbsent() throws Exception {
        store.delegate.put(layout.manifestKey(SHARD, Generation.FIRST), "{}".getBytes(StandardCharsets.UTF_8));
        indexFull(manager(publisher));
        assertThat(json(layout.latestKey(SHARD)).get("generation").asLong()).isEqualTo(1);
        assertThat(manifest(1).get("shard").asText()).isEqualTo(PROJECT);
    }

    @Test
    void existingManifestWithMovedLatestIsAConflict() throws Exception {
        ProjectIndexManager m = manager(publisher);
        indexFull(m);
        String manifest2 = layout.manifestKey(SHARD, new Generation(2));
        byte[] foreignManifest = "{\"foreign\": true}".getBytes(StandardCharsets.UTF_8);
        byte[] foreignLatest = "{\"generation\": 2, \"manifest\": \"gen-2.json\"}".getBytes(StandardCharsets.UTF_8);
        store.beforeConditionalPut = key -> {
            if (key.equals(manifest2) && store.delegate.get(manifest2).isEmpty()) {
                // another writer commits gen 2 right before this run's manifest put
                store.delegate.put(manifest2, foreignManifest);
                store.delegate.put(layout.latestKey(SHARD), foreignLatest);
            }
        };

        assertThatThrownBy(() -> touchOneFileAndReindex(m))
                .isInstanceOf(PublishConflictException.class)
                .hasMessageContaining("LATEST moved");
        assertThat(store.get(manifest2).orElseThrow().data()).isEqualTo(foreignManifest);
        assertThat(store.get(layout.latestKey(SHARD)).orElseThrow().data()).isEqualTo(foreignLatest);
    }

    @Test
    void racingRunCannotOverwriteCommittedGraph() throws Exception {
        // Run A commits gen 1.
        ProjectIndexManager mA = manager(publisher);
        indexFull(mA);
        JsonNode graphA = fileWithRole(manifest(1), "graph");
        byte[] bytesA = store.get(graphA.get("key").asText()).orElseThrow().data();

        // The local graph changes; run B read LATEST before A committed (stale: absent), so it
        // also targets gen 1 and uploads a different graph for it before losing at the manifest.
        ProjectIndexManager mLocal = manager(snapshot -> Optional.empty());
        touchOneFileAndReindex(mLocal);
        store.hideOnce.add(layout.latestKey(SHARD));
        store.putKeys.clear();
        ObjectStorePublisher runB = new ObjectStorePublisher(store, layout, catalog, gc(), "node-b", synonyms);
        Path shardDir = tmp.resolve("indexes").resolve(PROJECT);
        ProjectMeta meta = ProjectRegistry.at(tmp.resolve("registry.json")).find(PROJECT).orElseThrow();

        assertThatThrownBy(() -> runB.publish(new LocalSnapshot(SHARD.value(), shardDir, meta, "none")))
                .isInstanceOf(PublishConflictException.class);

        List<String> graphPutsB = store.putKeys.stream().filter(k -> k.contains("/graph/gen-1@")).toList();
        assertThat(graphPutsB).hasSize(1).doesNotContain(graphA.get("key").asText());
        assertThat(store.get(graphPutsB.get(0)).orElseThrow().data()).isNotEqualTo(bytesA);
        // A's committed generation still resolves to A's graph bytes
        JsonNode stillA = fileWithRole(manifest(1), "graph");
        assertThat(stillA.get("key").asText()).isEqualTo(graphA.get("key").asText());
        byte[] now = store.get(stillA.get("key").asText()).orElseThrow().data();
        assertThat(now).isEqualTo(bytesA);
        assertThat(crc32Hex(now)).isEqualTo(stillA.get("checksum").asText());
        assertThat(json(layout.latestKey(SHARD)).get("generation").asLong()).isEqualTo(1);
    }

    private static Set<String> luceneEntries(JsonNode manifest) {
        Set<String> out = new HashSet<>();
        manifest.get("files").forEach(f -> {
            if (f.get("role").asText().equals("lucene")) {
                out.add(f.get("name").asText() + "@" + f.get("checksum").asText() + " -> " + f.get("key").asText());
            }
        });
        return out;
    }

    @Test
    void staleLatestIsAConflict() throws Exception {
        ProjectIndexManager m = manager(publisher);
        indexFull(m);
        String foreign = "{\"generation\": 9, \"manifest\": \"gen-9.json\"}";
        String manifest2 = layout.manifestKey(SHARD, new Generation(2));
        store.beforeConditionalPut = key -> {
            if (key.equals(manifest2)) {
                store.delegate.put(layout.latestKey(SHARD), foreign.getBytes(StandardCharsets.UTF_8));
            }
        };

        assertThatThrownBy(() -> touchOneFileAndReindex(m))
                .isInstanceOf(PublishConflictException.class)
                .hasMessageContaining("LATEST moved")
                .hasMessageContaining(PROJECT);
        assertThat(new String(store.get(layout.latestKey(SHARD)).orElseThrow().data(), StandardCharsets.UTF_8))
                .isEqualTo(foreign);
    }

    @Test
    void missingCommitFileFailsBeforeManifest() throws Exception {
        boolean[] deleted = {false};
        store.beforePut = key -> {
            if (!deleted[0] && key.contains("/lucene/")) {
                String name = key.substring(key.lastIndexOf('/') + 1, key.lastIndexOf('@'));
                try {
                    Files.delete(luceneDir().resolve(name));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                deleted[0] = true;
            }
        };

        assertThatThrownBy(() -> indexFull(manager(publisher))).isInstanceOf(PublishException.class);
        assertThat(deleted[0]).isTrue();
        assertThat(store.get(layout.manifestKey(SHARD, Generation.FIRST))).isEmpty();
        assertThat(store.get(layout.latestKey(SHARD))).isEmpty();
        assertThat(store.list("pharos/shards/").stream()
                .map(ObjectInfo::key).collect(Collectors.toList()))
                .noneMatch(k -> k.contains("/manifests/") || k.endsWith("/LATEST"));
    }

    @Test
    void unsafeProjectNameRejectedBeforeUpload() throws Exception {
        assertThatThrownBy(() -> publisher.publish(new LocalSnapshot("my project",
                tmp, new ProjectMeta("my project", tmp.toString(), tmp.toString()), "none")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(store.list("")).isEmpty();
    }

    @Test
    void writerRejectsUnsafeProjectNameBeforeIndexing() {
        ProjectIndexManager m = manager(publisher);
        assertThatThrownBy(() -> m.index(source, "my project", false, false, false, false,
                ProgressListener.SILENT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be a shard id");
        assertThat(ProjectRegistry.at(tmp.resolve("registry.json")).find("my project")).isEmpty();
        assertThat(tmp.resolve("indexes").resolve("my project")).doesNotExist();
        assertThat(store.list("")).isEmpty();
    }

    @Test
    void refsShippedGzippedPerGeneration() throws Exception {
        indexFull(manager(snapshot -> Optional.empty()));
        Path shardDir = tmp.resolve("indexes").resolve(PROJECT);
        byte[] refs = "[{\"callerFqn\":\"a.B#c()\",\"calleeMethodName\":\"d\"}]".getBytes(StandardCharsets.UTF_8);
        Files.write(shardDir.resolve("unresolved-refs.json"), refs);
        ProjectMeta meta = ProjectRegistry.at(tmp.resolve("registry.json")).find(PROJECT).orElseThrow();
        meta.setUnresolvedRefs(new ArrayList<>(List.of(new ProjectMeta.UnresolvedRef("a.B#c()", "d", 1))));

        publisher.publish(new LocalSnapshot(SHARD.value(), shardDir, meta, "none"));

        JsonNode refsEntry = fileWithRole(manifest(1), "refs");
        String refsKey = refsEntry.get("key").asText();
        assertThat(refsKey).isEqualTo(layout.refsKey(SHARD, Generation.FIRST, refsEntry.get("checksum").asText()));
        assertThat(refsEntry.get("checksum").asText())
                .isEqualTo(crc32Hex(store.get(refsKey).orElseThrow().data()));
        try (var in = new java.util.zip.GZIPInputStream(
                new java.io.ByteArrayInputStream(store.get(refsKey).orElseThrow().data()))) {
            assertThat(in.readAllBytes()).isEqualTo(refs);
        }
        JsonNode m = manifest(1);
        assertThat(keys(m)).contains(refsKey);
        assertThat(m.get("projectMeta").get("unresolvedRefs")).isEmpty();
        assertThat(meta.getUnresolvedRefs()).as("caller's meta not mutated").hasSize(1);
    }

    @Test
    void publishRunsGenerationGc() throws Exception {
        ObjectStorePublisher gcPublisher = new ObjectStorePublisher(store, layout, catalog,
                new GenerationGc(store, layout, 2, Duration.ZERO), "node-a", synonyms);
        indexFull(manager(gcPublisher));
        Set<String> gen1 = keys(manifest(1));
        Path shardDir = tmp.resolve("indexes").resolve(PROJECT);
        ProjectMeta meta = ProjectRegistry.at(tmp.resolve("registry.json")).find(PROJECT).orElseThrow();
        gcPublisher.publish(new LocalSnapshot(SHARD.value(), shardDir, meta, "none"));
        gcPublisher.publish(new LocalSnapshot(SHARD.value(), shardDir, meta, "none"));

        assertThat(store.get(layout.manifestKey(SHARD, Generation.FIRST))).isEmpty();
        Set<String> live = new HashSet<>(keys(manifest(2)));
        live.addAll(keys(manifest(3)));
        for (String key : live) assertThat(store.get(key)).as(key).isPresent();
        for (String key : gen1) {
            if (!live.contains(key)) assertThat(store.get(key)).as(key).isEmpty();
        }
        assertThat(json(layout.latestKey(SHARD)).get("generation").asLong()).isEqualTo(3);
    }

    @Test
    void synonymsShipped() throws Exception {
        Files.writeString(synonyms, "calc, calculator\nusr => user\n");
        indexFull(manager(publisher));
        assertThat(store.get(layout.synonymsKey()).orElseThrow().data())
                .isEqualTo(Files.readAllBytes(synonyms));
    }
}
