package com.pharos.cluster.objectstore;

import com.pharos.cluster.Generation;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.WriterStateStore;
import com.pharos.cluster.sharding.DocKeys;
import com.pharos.cluster.sharding.ShardPlan;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.cluster.writer.LeaseGuard;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.indexer.CountingEmbedder;
import com.pharos.indexer.DocumentMapper;
import com.pharos.indexer.IndexAbort;
import com.pharos.indexer.LocalShardLayout;
import com.pharos.indexer.LuceneIndexer;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.parser.JavaCodeParser;
import com.pharos.parser.MavenPomReader;
import com.pharos.search.distributed.GlobalStats;
import com.pharos.search.distributed.GlobalStatsSearcher;
import com.pharos.cluster.internal.TermStatsResponse;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6 Task 2: one parse pass of a split project feeds one Lucene writer per shard, the project's
 * call graph is published once and referenced from every shard manifest, the writer state is backed up
 * once per project, and a {@code [project]} project is exactly what it was.
 */
class PartitionedIndexingTest {

    private static final String PROJECT = "lib";
    private static final ShardSpec RING3 = ShardSpec.parse("[project, ring(path, shards=3)]");
    private static final List<ShardId> SHARDS = List.of(new ShardId("lib~ring=s01"), new ShardId("lib~ring=s02"),
            new ShardId("lib~ring=s03"));
    private static final ShardPlan PLAN = new ShardPlan(PROJECT, RING3, SHARDS);

    /** Words the generated classes draw their identifiers and comments from, unevenly. */
    private static final String[] WORDS = {"parse", "token", "cache", "index", "merge", "query", "score",
            "buffer", "stream", "codec", "segment", "reader", "writer", "field", "term", "vector"};

    @TempDir Path tmp;
    private Path source;
    private final AtomicLong offsetSeconds = new AtomicLong();
    private final Clock clock = new Clock() {
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId z) { return this; }
        @Override public Instant instant() { return Instant.now().plusSeconds(offsetSeconds.get()); }
    };
    private InMemoryObjectStore store;
    private ObjectStoreLayout layout;
    private ObjectStoreCatalog catalog;
    private ObjectStoreReplicaSource replicas;
    private final List<Node> nodes = new ArrayList<>();

    /** One node: its own index dir and registry; publishes to the shared bucket unless {@code local}. */
    final class Node {
        final LuceneIndexer lucene;
        final ProjectRegistry registry;
        final CountingEmbedder embedder = new CountingEmbedder();
        final ObjectStoreWriterStateStore state;
        final ProjectIndexManager manager;

        Node(String name, boolean local) {
            IndexConfig config = IndexConfig.defaults();
            config.setIndexDir(tmp.resolve(name).resolve("indexes"));
            registry = ProjectRegistry.at(tmp.resolve(name).resolve("registry.json"));
            lucene = new LuceneIndexer(config);
            state = new ObjectStoreWriterStateStore(store, layout, replicas, name);
            ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
                @Override
                public synchronized List<String> incorporate(Path root, ProjectMeta meta, MavenPomReader.PomInfo pom) {
                    return List.of();
                }
            };
            if (local) {
                manager = new ProjectIndexManager(config, lucene, registry, List.of(embedder), noopModules,
                        List.of(new JavaCodeParser()));
            } else {
                GenerationGc gc = new GenerationGc(store, layout, 1, Duration.ofMinutes(10));
                ObjectStorePublisher publisher = new ObjectStorePublisher(store, layout, catalog, gc, name, null,
                        LeaseGuard.IMPLICIT, state);
                manager = new ProjectIndexManager(config, lucene, registry, List.of(embedder), noopModules,
                        List.of(new JavaCodeParser()), publisher);
            }
            nodes.add(this);
        }

        ProjectMeta index(boolean incremental, ShardPlan plan) throws IOException {
            return manager.index(source, PROJECT, incremental, false, false, false, ProgressListener.SILENT,
                    IndexAbort.none(), null, plan);
        }

        Path dir() { return lucene.getProjectIndexDir(PROJECT); }

        Path shardLucene(ShardId shard) { return LocalShardLayout.luceneDir(dir(), shard); }
    }

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryObjectStore(clock);
        layout = new ObjectStoreLayout("pharos");
        catalog = new ObjectStoreCatalog(store, layout);
        replicas = new ObjectStoreReplicaSource(store, layout);
        source = tmp.resolve("checkout");
        for (int i = 0; i < 24; i++) writeClass("pkg" + (i % 5), "C" + i, i);
    }

    @AfterEach
    void tearDown() {
        for (Node n : nodes) n.lucene.close();
    }

    /** A class with three methods whose names and comments mix the words unevenly; it calls the next class. */
    private Path writeClass(String pkg, String name, int seed) throws IOException {
        return writeClassAt("src/main/java/com/example/" + pkg + "/" + name + ".java", pkg, name, seed);
    }

    private Path writeClassAt(String relPath, String pkg, String name, int seed) throws IOException {
        StringBuilder sb = new StringBuilder("package com.example." + pkg + ";\n\n/** " + WORDS[seed % WORDS.length]
                + " helper for " + WORDS[(seed * 7) % WORDS.length] + " work. */\npublic class " + name + " {\n");
        for (int m = 0; m < 3; m++) {
            String w1 = WORDS[(seed + m) % WORDS.length];
            String w2 = WORDS[(seed * 3 + m * 5) % WORDS.length];
            sb.append("    /** Does ").append(w1).append(' ').append(w2).append(" work, ").append(w1)
                    .append(" first. */\n    public int ").append(w1).append(Character.toUpperCase(w2.charAt(0)))
                    .append(w2.substring(1)).append(m).append("(int x) { int ").append(w2).append(" = x * ")
                    .append(seed + m).append("; return ").append(w2).append(" + ").append(m).append("; }\n");
        }
        sb.append("}\n");
        Path file = source.resolve(relPath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
        return file;
    }

    private void registerSplitProject() throws IOException {
        int v = catalog.addSpec(Optional.of(PROJECT), RING3);
        catalog.putProject(PROJECT, v, SHARDS);
    }

    /** id → filePath of every document in {@code luceneDir}. */
    private static Map<String, String> docs(Path luceneDir) throws IOException {
        Map<String, String> out = new TreeMap<>();
        try (DirectoryReader r = DirectoryReader.open(FSDirectory.open(luceneDir))) {
            StoredFields sf = r.storedFields();
            org.apache.lucene.util.Bits live = org.apache.lucene.index.MultiBits.getLiveDocs(r);
            for (int d = 0; d < r.maxDoc(); d++) {
                if (live != null && !live.get(d)) continue;
                Document doc = sf.document(d);
                String prev = out.put(doc.get(DocumentMapper.F_ID), doc.get(DocumentMapper.F_FILE_PATH));
                assertThat(prev).as("id %s twice in %s", doc.get(DocumentMapper.F_ID), luceneDir).isNull();
            }
        }
        return out;
    }

    private ShardManifest latestManifest(ShardId shard) throws IOException {
        return replicas.manifest(shard, replicas.latest(shard).orElseThrow());
    }

    private List<String> keysUnder(String prefix) throws IOException {
        return store.list(prefix).stream().map(ObjectInfo::key).toList();
    }

    // ------------------------------------------------------------------------------- tests

    @Test
    void partitionedIndexUnionEqualsUnpartitioned() throws IOException {
        Node whole = new Node("whole", true);
        Node split = new Node("split", true);
        whole.index(false, ShardPlan.single(PROJECT));
        split.index(false, PLAN);

        Map<String, String> all = docs(whole.lucene.getLucenePath(PROJECT));
        Map<String, String> union = new TreeMap<>();
        Map<String, ShardId> shardOfFile = new HashMap<>();
        int nonEmpty = 0;
        for (ShardId shard : SHARDS) {
            Map<String, String> part = docs(split.shardLucene(shard));
            if (!part.isEmpty()) nonEmpty++;
            for (Map.Entry<String, String> e : part.entrySet()) {
                assertThat(union.put(e.getKey(), e.getValue())).as("doc %s in two shards", e.getKey()).isNull();
                ShardId before = shardOfFile.put(e.getValue(), shard);
                assertThat(before == null || before.equals(shard)).as("file %s spans shards", e.getValue()).isTrue();
                // every document sits in the shard the ring assigns its file
                assertThat(RING3.assign(DocKeys.of(PROJECT, DocKeys.normalizePath(source, Path.of(e.getValue())))))
                        .isEqualTo(shard);
            }
        }
        assertThat(nonEmpty).as("the fixture spreads over the ring").isGreaterThanOrEqualTo(2);
        assertThat(union).isEqualTo(all);
        assertThat(Files.exists(split.dir().resolve("lucene"))).as("no project-level Lucene dir").isFalse();
        assertThat(LocalShardLayout.read(split.dir()).orElseThrow().matches(PLAN)).isTrue();
        assertThat(split.lucene.indexExists(PROJECT)).isTrue();
        // one call graph for the project, not one per shard
        assertThat(CallGraph.pointerProblem(split.dir())).isNull();
        assertThat(Files.isDirectory(CallGraph.projectGraphDir(split.dir()))).isTrue();

        // BM25 with the statistics summed over the shards (the coordinator's term-stats round) gives
        // every hit the score it has in the unsplit index, bit for bit
        List<Query> queries = List.of(
                termsQuery(DocumentMapper.F_BODY, "token", "cache"),
                termsQuery(DocumentMapper.F_BODY, "vector"),
                termsQuery(DocumentMapper.F_JAVADOC, "merge", "segment", "work"),
                termsQuery(DocumentMapper.F_JAVADOC, "codec", "reader", "helper"));
        try (DirectoryReader wholeReader = DirectoryReader.open(FSDirectory.open(whole.lucene.getLucenePath(PROJECT)))) {
            List<DirectoryReader> parts = new ArrayList<>();
            for (ShardId s : SHARDS) parts.add(DirectoryReader.open(FSDirectory.open(split.shardLucene(s))));
            try {
                boolean anyLocalDiffers = false;
                for (Query q : queries) {
                    Map<String, Float> expected = scores(new IndexSearcher(wholeReader), q);
                    assertThat(expected).as("query %s matches", q).isNotEmpty();
                    List<TermStatsResponse> perShard = new ArrayList<>();
                    for (DirectoryReader r : parts) {
                        perShard.add(new TermStatsResponse(0, GlobalStats.of(new IndexSearcher(r),
                                GlobalStats.queryTerms(q))));
                    }
                    GlobalStats global = GlobalStats.merge(perShard);
                    Map<String, Float> distributed = new HashMap<>();
                    Map<String, Float> local = new HashMap<>();
                    for (DirectoryReader r : parts) {
                        distributed.putAll(scores(new GlobalStatsSearcher(r, global), q));
                        local.putAll(scores(new IndexSearcher(r), q));
                    }
                    assertThat(distributed.keySet()).as("%s hits", q).isEqualTo(expected.keySet());
                    for (Map.Entry<String, Float> e : expected.entrySet()) {
                        assertThat(Float.floatToRawIntBits(distributed.get(e.getKey())))
                                .as("%s: %s split %s vs whole %s", q, e.getKey(), distributed.get(e.getKey()), e.getValue())
                                .isEqualTo(Float.floatToRawIntBits(e.getValue()));
                    }
                    if (!local.equals(expected)) anyLocalDiffers = true;
                    // and Lucene's own view of the shards together agrees
                    try (MultiReader both = new MultiReader(parts.toArray(IndexReader[]::new), false)) {
                        Map<String, Float> multi = scores(new IndexSearcher(both), q);
                        for (Map.Entry<String, Float> e : expected.entrySet()) {
                            assertThat(Float.floatToRawIntBits(multi.get(e.getKey()))).as("%s multi", q)
                                    .isEqualTo(Float.floatToRawIntBits(e.getValue()));
                        }
                    }
                }
                assertThat(anyLocalDiffers).as("shard-local statistics alone would score differently").isTrue();
            } finally {
                for (DirectoryReader r : parts) r.close();
            }
        }
        // the writer's own reader over a split project spans its shards
        assertThat(split.lucene.openMultiReader(List.of(PROJECT)).numDocs()).isEqualTo(all.size());
        assertThat(split.lucene.committedDocCount(PROJECT)).isEqualTo(all.size());
        assertThat(split.registry.find(PROJECT).orElseThrow().getMethodCount())
                .isEqualTo(whole.registry.find(PROJECT).orElseThrow().getMethodCount());
    }

    private static Query termsQuery(String field, String... terms) {
        BooleanQuery.Builder b = new BooleanQuery.Builder();
        for (String t : terms) b.add(new TermQuery(new Term(field, t)), BooleanClause.Occur.SHOULD);
        return b.build();
    }

    private static Map<String, Float> scores(IndexSearcher searcher, Query q) throws IOException {
        TopDocs top = searcher.search(q, 10_000);
        StoredFields sf = searcher.getIndexReader().storedFields();
        Map<String, Float> out = new HashMap<>();
        for (ScoreDoc sd : top.scoreDocs) out.put(sf.document(sd.doc).get(DocumentMapper.F_ID), sd.score);
        return out;
    }

    @Test
    void graphPublishedOncePerProject() throws IOException {
        registerSplitProject();
        Node a = new Node("node-a", false);
        a.index(false, PLAN);

        String projectPrefix = layout.projectPrefix(PROJECT);
        List<String> graphs = keysUnder(projectPrefix + "graph/");
        assertThat(graphs).hasSize(1);
        assertThat(graphs.getFirst()).startsWith(projectPrefix + "graph/gen-1@");
        assertThat(keysUnder(projectPrefix + "refs/")).hasSizeLessThanOrEqualTo(1);
        for (ShardId shard : SHARDS) {
            ShardManifest m = latestManifest(shard);
            assertThat(m.generation()).isEqualTo(1);
            assertThat(m.projectGeneration()).isEqualTo(1L);
            assertThat(m.graphFile().orElseThrow().key()).isEqualTo(graphs.getFirst());
            assertThat(m.files()).allMatch(f -> ManifestFile.LUCENE.equals(f.role()));
            assertThat(keysUnder(layout.shardPrefix(shard) + "graph/")).isEmpty();
            assertThat(keysUnder(layout.writerStatePrefix(shard))).as("no per-shard writer state").isEmpty();
        }
        // the writer state once, under the project
        assertThat(store.get(layout.projectWriterStatePrefix(PROJECT) + "gen-1/index.json")).isPresent();
        assertThat(replicas.latest(ShardId.ofProject(PROJECT))).as("no [project] shard is published").isEmpty();

        // a second run: every shard publishes again, all with the same new run number and graph
        offsetSeconds.addAndGet(3600);
        Path changed = source.resolve("src/main/java/com/example/pkg1/C1.java");
        Files.writeString(changed, Files.readString(changed).replace("}\n}\n",
                "}\n    public int extra(int x) { return x; }\n}\n"));
        Files.setLastModifiedTime(changed, FileTime.fromMillis(Files.getLastModifiedTime(changed).toMillis() + 5000));
        a.index(true, PLAN);
        Set<String> secondGraph = new HashSet<>();
        for (ShardId shard : SHARDS) {
            ShardManifest m = latestManifest(shard);
            assertThat(m.generation()).isEqualTo(2);
            assertThat(m.projectGeneration()).isEqualTo(2L);
            secondGraph.add(m.graphFile().orElseThrow().key());
        }
        assertThat(secondGraph).hasSize(1);
        assertThat(secondGraph.iterator().next()).startsWith(projectPrefix + "graph/gen-2@");
        // the first run's graph and writer state went with its manifests (keepGenerations 1, past minAge)
        assertThat(keysUnder(projectPrefix + "graph/")).containsExactlyElementsOf(secondGraph);
        assertThat(store.get(layout.projectWriterStatePrefix(PROJECT) + "gen-1/index.json")).isEmpty();
        assertThat(store.get(layout.projectWriterStatePrefix(PROJECT) + "gen-2/index.json")).isPresent();

        // a new owner restores the project once: every shard, the graph and the writer state
        Node b = new Node("node-b", false);
        Optional<WriterStateStore.Restored> restored = b.state.restoreProject(PLAN, b.dir());
        assertThat(restored).contains(new WriterStateStore.Restored(new Generation(2), new Generation(2)));
        for (ShardId shard : SHARDS) assertThat(docs(b.shardLucene(shard))).isEqualTo(docs(a.shardLucene(shard)));
        assertThat(LocalShardLayout.read(b.dir()).orElseThrow().matches(PLAN)).isTrue();
        assertThat(CallGraph.pointerProblem(b.dir())).isNull();
        assertThat(b.lucene.indexExists(PROJECT)).isTrue();
        assertThat(b.manager.fullRunReason(PROJECT, PLAN)).isNull();
    }

    @Test
    void movedFileChangesShardInOneCycle() throws IOException {
        // a file the ring places in one shard, and a new path for it that the ring places in another
        String from = null, to = null;
        for (int i = 0; i < 500 && to == null; i++) {
            String a = "src/main/java/com/example/move/Mover" + i + ".java";
            for (int j = 0; j < 500 && to == null; j++) {
                String b = "src/main/java/com/example/moved" + j + "/Mover" + i + ".java";
                if (!RING3.assign(DocKeys.of(PROJECT, a)).equals(RING3.assign(DocKeys.of(PROJECT, b)))) {
                    from = a;
                    to = b;
                }
            }
        }
        assertThat(to).isNotNull();
        ShardId oldShard = RING3.assign(DocKeys.of(PROJECT, from));
        ShardId newShard = RING3.assign(DocKeys.of(PROJECT, to));
        String cls = from.substring(from.lastIndexOf('/') + 1, from.length() - ".java".length());
        Path oldFile = writeClassAt(from, "move", cls, 99);
        registerSplitProject();
        Node a = new Node("node-a", false);
        a.index(false, PLAN);
        Map<ShardId, Long> genBefore = new HashMap<>();
        for (ShardId s : SHARDS) genBefore.put(s, latestManifest(s).generation());
        Set<String> moverIds = new HashSet<>();
        docs(a.shardLucene(oldShard)).forEach((id, path) -> {
            if (path.equals(oldFile.toString())) moverIds.add(id);
        });
        assertThat(moverIds).as("the file's documents start in its ring shard").isNotEmpty();

        // the move: same content (same package, same document ids), another path
        Path newFile = source.resolve(to);
        Files.createDirectories(newFile.getParent());
        Files.move(oldFile, newFile);
        Files.setLastModifiedTime(newFile, FileTime.fromMillis(System.currentTimeMillis() + 5000));
        offsetSeconds.addAndGet(60);
        a.index(true, PLAN);

        // both shards published in this cycle, as one run with the rest of the project
        ShardManifest oldM = latestManifest(oldShard), newM = latestManifest(newShard);
        assertThat(oldM.generation()).isEqualTo(genBefore.get(oldShard) + 1);
        assertThat(newM.generation()).isEqualTo(genBefore.get(newShard) + 1);
        for (ShardId s : SHARDS) assertThat(latestManifest(s).projectGeneration()).isEqualTo(2L);

        // after both publishes every document of the file is in exactly one shard: the new one
        Map<String, ShardId> where = new HashMap<>();
        for (ShardId s : SHARDS) {
            for (Map.Entry<String, String> e : docs(a.shardLucene(s)).entrySet()) {
                assertThat(where.put(e.getKey(), s)).as("doc %s seen twice (%s in %s)", e.getKey(), e.getValue(), s)
                        .isNull();
                assertThat(e.getValue()).isNotEqualTo(oldFile.toString());
            }
        }
        for (String id : moverIds) assertThat(where.get(id)).as("doc %s", id).isEqualTo(newShard);
        // what the published generations hold is that commit: the old shard's commit changed too
        assertThat(luceneKeys(oldM)).isNotEqualTo(luceneKeys(replicas.manifest(oldShard,
                new Generation(genBefore.get(oldShard)))));
        assertThat(luceneKeys(newM)).isNotEqualTo(luceneKeys(replicas.manifest(newShard,
                new Generation(genBefore.get(newShard)))));
        assertThat(oldM.docCount() + newM.docCount() + otherDocCount(oldShard, newShard))
                .isEqualTo(where.size());
    }

    private long otherDocCount(ShardId... except) throws IOException {
        long n = 0;
        for (ShardId s : SHARDS) if (!List.of(except).contains(s)) n += latestManifest(s).docCount();
        return n;
    }

    private static Set<String> luceneKeys(ShardManifest m) {
        Set<String> out = new HashSet<>();
        for (ManifestFile f : m.files()) out.add(f.key());
        return out;
    }

    @Test
    void singleShardProjectUnchanged() throws IOException {
        Node a = new Node("node-a", false);
        a.index(false, ShardPlan.single(PROJECT));
        ShardId shard = ShardId.ofProject(PROJECT);

        // local layout: the Phases 1–5 lucene/ dir, nothing of the split layout
        assertThat(Files.isDirectory(a.dir().resolve("lucene"))).isTrue();
        assertThat(Files.exists(a.dir().resolve(LocalShardLayout.FILE_NAME))).isFalse();
        assertThat(Files.exists(a.dir().resolve(LocalShardLayout.SHARDS_DIR))).isFalse();
        assertThat(Files.exists(a.dir().resolve(LocalShardLayout.COMMITTING))).isFalse();

        // bucket: everything under shards/lib/, nothing under projects/
        assertThat(keysUnder(layout.projectPrefix(PROJECT))).isEmpty();
        ShardManifest m = latestManifest(shard);
        assertThat(m.projectGeneration()).isNull();
        assertThat(m.projectFiles()).isNull();
        assertThat(m.files()).anyMatch(f -> ManifestFile.GRAPH.equals(f.role()));
        String json = new String(store.get(layout.manifestKey(shard, Generation.FIRST)).orElseThrow().data(),
                StandardCharsets.UTF_8);
        assertThat(json).doesNotContain("projectGeneration").doesNotContain("projectFiles");
        assertThat(ShardManifest.read(json.getBytes(StandardCharsets.UTF_8)).toJson())
                .isEqualTo(json.getBytes(StandardCharsets.UTF_8));
        assertThat(store.get(layout.writerStateIndexKey(shard, Generation.FIRST))).isPresent();

        // the old entry point is the [project] plan
        Node b = new Node("node-b", true);
        b.manager.index(source, PROJECT, false, false, false, false, ProgressListener.SILENT);
        assertThat(docs(b.lucene.getLucenePath(PROJECT))).isEqualTo(docs(a.lucene.getLucenePath(PROJECT)));
    }

    @Test
    void layoutChangeRunsFullAndDropsTheOtherLayout() throws IOException {
        Node a = new Node("node-a", true);
        a.index(false, ShardPlan.single(PROJECT));
        Map<String, String> whole = docs(a.lucene.getLucenePath(PROJECT));

        assertThat(a.manager.fullRunReason(PROJECT, PLAN)).contains("now split");
        a.index(true, PLAN); // incremental requested, the layout differs: full
        Map<String, String> union = new TreeMap<>();
        for (ShardId s : SHARDS) union.putAll(docs(a.shardLucene(s)));
        assertThat(union).isEqualTo(whole);
        assertThat(Files.exists(a.dir().resolve("lucene"))).as("the [project] Lucene dir is gone").isFalse();

        // a crash between two shard commits makes the next run full
        Files.writeString(a.dir().resolve(LocalShardLayout.COMMITTING), "");
        assertThat(a.manager.fullRunReason(PROJECT, PLAN)).contains("between the commits");
        a.index(true, PLAN);
        assertThat(Files.exists(a.dir().resolve(LocalShardLayout.COMMITTING))).isFalse();

        // back to [project]
        assertThat(a.manager.fullRunReason(PROJECT, ShardPlan.single(PROJECT))).contains("is split");
        a.index(true, ShardPlan.single(PROJECT));
        assertThat(docs(a.lucene.getLucenePath(PROJECT))).isEqualTo(whole);
        assertThat(Files.exists(a.dir().resolve(LocalShardLayout.SHARDS_DIR))).isFalse();
        assertThat(Files.exists(a.dir().resolve(LocalShardLayout.FILE_NAME))).isFalse();
    }

    @Test
    void publishRefusesARunWhoseShardsTheCatalogDoesNotList() throws IOException {
        registerSplitProject();
        Node a = new Node("node-a", false);
        // a [project] run of a project the catalog splits would publish an orphan under the plain id
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> a.index(false, ShardPlan.single(PROJECT)))
                .isInstanceOf(com.pharos.cluster.PublishConflictException.class)
                .hasMessageContaining("splits project lib");
        assertThat(replicas.latest(ShardId.ofProject(PROJECT))).isEmpty();
        // a split run under another shard set than the catalog's (resharded since it started)
        ShardPlan two = new ShardPlan(PROJECT, ShardSpec.parse("[project, ring(path, shards=2)]"),
                List.of(new ShardId("lib~ring=s01"), new ShardId("lib~ring=s02")));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> a.index(false, two))
                .isInstanceOf(com.pharos.cluster.PublishConflictException.class)
                .hasMessageContaining("resharded");
        for (ShardId s : SHARDS) assertThat(replicas.latest(s)).isEmpty();
    }
}
