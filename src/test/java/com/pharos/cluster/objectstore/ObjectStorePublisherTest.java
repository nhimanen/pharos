package com.pharos.cluster.objectstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.cluster.Generation;
import com.pharos.cluster.IndexPublisher;
import com.pharos.cluster.LocalSnapshot;
import com.pharos.cluster.PublishConflictException;
import com.pharos.cluster.PublishException;
import com.pharos.cluster.ShardId;
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
        Consumer<String> beforePut = k -> {};
        Consumer<String> beforeConditionalPut = k -> {};

        @Override public Optional<StoredObject> get(String key) { return delegate.get(key); }
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
        publisher = new ObjectStorePublisher(store, layout, catalog, null, "node-a", synonyms);
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
        assertThat(publisher.publish(new LocalSnapshot(SHARD, shardDir, meta, "none")))
                .contains(new Generation(3));
        JsonNode m3 = manifest(3);
        assertThat(m3.get("parentGeneration").asLong()).isEqualTo(2);
        assertThat(luceneEntries(m3)).isEqualTo(luceneEntries(m2));
        Set<String> expectedNew3 = keys(m3);
        expectedNew3.removeAll(gen2);
        assertThat(expectedNew3).containsExactly(layout.graphKey(SHARD, new Generation(3)));
        assertThat(new HashSet<>(store.putKeys)).isEqualTo(expectedNew3);
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
        assertThatThrownBy(() -> publisher.publish(new LocalSnapshot(ShardId.ofProject("my project"),
                tmp, new ProjectMeta("my project", tmp.toString(), tmp.toString()), "none")))
                .isInstanceOf(IllegalArgumentException.class);
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

        publisher.publish(new LocalSnapshot(SHARD, shardDir, meta, "none"));

        String refsKey = layout.refsKey(SHARD, Generation.FIRST);
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
    void synonymsShipped() throws Exception {
        Files.writeString(synonyms, "calc, calculator\nusr => user\n");
        indexFull(manager(publisher));
        assertThat(store.get(layout.synonymsKey()).orElseThrow().data())
                .isEqualTo(Files.readAllBytes(synonyms));
    }
}
