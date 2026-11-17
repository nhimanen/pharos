package com.pharos.search.distributed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
import com.pharos.parser.PythonCodeParser;
import com.pharos.search.FstQueryClassifier;
import com.pharos.search.KeywordSearchStrategy;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import com.pharos.search.VectorSearchStrategy;
import com.pharos.search.pipeline.KeywordRetrievalStage;
import com.pharos.search.pipeline.RetrievalStage;
import com.pharos.search.pipeline.UnifiedRetrievalStage;
import com.pharos.search.pipeline.VectorRetrievalStage;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * The pipeline split (raw candidates + {@link CandidateShaper}) must not move a single local result:
 * ids, order and float scores are compared bit for bit against a fixture recorded from the code
 * before the split, for the eval set's pharos queries (over a frozen pharos source snapshot) and the
 * sample-app queries, on every pipeline type.
 *
 * <p>Regenerate (only when ranking is meant to change):
 * {@code mvn test -Dtest=PipelineSplitParityTest -Dpharos.regenerateParity=true}.
 */
class PipelineSplitParityTest {

    private static final Path FIXTURE = Path.of("src/test/resources/parity/pipeline-split-parity.json");
    private static final Path INDEX_ZIP = Path.of("src/test/resources/parity/pipeline-split-indexes.zip");
    private static final Path PHAROS_CORPUS = Path.of("src/test/resources/parity/pharos-src.zip");

    private static final String JAVA = "sample-app";
    private static final String PY = "sample-python-app";
    private static final String PHAROS = "pharos";

    /** eval/benchmark.py QUERIES whose project is pharos (category + index in the comment). */
    private static final List<String> PHAROS_QUERIES = List.of(
            "BordaMerger", // A21
            "CrossEncoderProvider", // A22
            "CallGraphBuilder", // A23
            "LuceneIndexer", // A24
            "SearchEngine", // A25
            "FileStateTracker", // A26
            "DocumentMapper", // A27
            "McpToolRegistry", // A28
            "ProjectRegistry", // A29
            "SynonymProvider", // A30
            "BordaMerger Borda count agreement bonus", // B51
            "cross-project link resolution", // B52
            "incremental index file state", // B53
            "MCP server tool registration", // B54
            "query hint project language modifier", // B55
            "scores result with cross-encoder model", // C85
            "find code that picks the best result from two lists without duplicates", // D116
            "find code that decides how much to boost a result from the target project", // D117
            "find code that splits a compound query into project and language hints", // D118
            "find code that converts a method body to a short passage for scoring", // D119
            "find code that connects methods across project boundaries in the call graph", // D120
            "find code that skips re-indexing files that have not changed", // D121
            "find code that registers all CLI subcommands in one place", // D122
            "find code that maps a parsed method to a Lucene document", // D123
            "find code that expands search results with related callee methods", // D124
            "find code that detects whether a query is a class name or natural language", // D125
            "close Lucene index reader after search", // E141
            "handle embedding model load failure", // E142
            "daemon startup health check", // E143
            "handle cross-encoder scoring exception", // E144
            "recover from corrupt index state", // E145
            "configure embedding model URL", // F160
            "set heap size for JVM indexing", // F161
            "configure parse thread count", // F162
            "set HNSW max connections per node", // F163
            "configure search result limit", // F164
            "enable cross encoder reranking", // F165
            "interface for embedding generation", // G178
            "interface for search pipeline stage retrieval", // G179
            "interface for cross-encoder scoring", // G180
            "pipeline stage retrieval merge rerank", // H196
            "adapter pattern cross encoder provider", // H197
            "builder pattern search pipeline construction", // H198
            "strategy pattern search type selection", // H199
            "facade pattern search engine orchestration"); // H200

    /** The sample-app eval categories of {@code SingleNodeGoldenTest}. */
    private static final List<String[]> SAMPLE_QUERIES = List.of(
            new String[]{JAVA, "Calculator"},
            new String[]{JAVA, "greeting service"},
            new String[]{JAVA, "Computes the absolute value of an integer"},
            new String[]{JAVA, "generate a greeting message for a user"},
            new String[]{JAVA, "division by zero is not allowed"},
            new String[]{JAVA, "user display name and email address"},
            new String[]{JAVA, "checks whether user has a valid email"},
            new String[]{JAVA, "methods that call getName"},
            new String[]{PY, "abs_value"},
            new String[]{PY, "has_valid_email"},
            new String[]{PY, "formal_greet"},
            new String[]{PY, "multiply"});

    private static Path tmp;
    private static LuceneIndexer lucene;
    private static SearchEngine engine;

    @BeforeAll
    static void openFrozenIndexes() throws Exception {
        tmp = Files.createTempDirectory("pipeline-split-parity");
        // The indexer is not deterministic run to run (doc ids, colliding overload ids), so the
        // fixture freezes the indexes themselves; only the search path is under test here.
        if (Boolean.getBoolean("pharos.regenerateParity")) buildIndexZip(tmp.resolve("build"));

        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("run/indexes"));
        lucene = new LuceneIndexer(config);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("run/registry.json"));
        for (String project : List.of(JAVA, PY, PHAROS)) {
            Path indexDir = lucene.getProjectIndexDir(project);
            Files.createDirectories(indexDir);
            ProjectMeta meta = new ProjectMeta(project, tmp.resolve("run/src/" + project).toString(),
                    indexDir.toString());
            meta.setEmbeddedModels(List.of(HashEmbeddings.MODEL));
            registry.register(meta);
        }
        unzip(INDEX_ZIP, config.getIndexDir());
        engine = new SearchEngine(lucene, new HashEmbeddings(), registry);
    }

    /** Indexes the three corpora with this build and zips what search reads of the index dirs. */
    private static void buildIndexZip(Path base) throws Exception {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(base.resolve("indexes"));
        config.setParseThreads(1);
        ProjectRegistry registry = ProjectRegistry.at(base.resolve("registry.json"));
        ModuleGraphBuilder noopModules = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(
                    Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        try (LuceneIndexer indexer = new LuceneIndexer(config)) {
            ProjectIndexManager manager = new ProjectIndexManager(
                    config, indexer, registry, new HashEmbeddings(), noopModules,
                    List.of(new JavaCodeParser(), new PythonCodeParser()));
            Path projects = Path.of("src/test/resources/test-projects");
            manager.index(projects.resolve("sample-app"), JAVA, false, false, true, false, ProgressListener.SILENT);
            manager.index(projects.resolve("sample-python-app"), PY, false, false, true, false,
                    ProgressListener.SILENT);
            Path pharosRoot = base.resolve("pharos-src");
            unzip(PHAROS_CORPUS, pharosRoot);
            manager.index(pharosRoot, PHAROS, false, false, true, false, ProgressListener.SILENT);
        }
        Path indexes = config.getIndexDir();
        try (ZipOutputStream zout = new ZipOutputStream(Files.newOutputStream(INDEX_ZIP));
             Stream<Path> walk = Files.walk(indexes)) {
            zout.setLevel(9);
            for (Path p : walk.sorted().toList()) {
                String rel = indexes.relativize(p).toString().replace('\\', '/');
                // search reads only the Lucene index and the format stamp (no expansion: no graph)
                boolean keep = rel.contains("/lucene/") ? !rel.endsWith("write.lock")
                        : rel.endsWith("/index-format.json");
                if (!Files.isRegularFile(p) || !keep) continue;
                zout.putNextEntry(new ZipEntry(rel));
                Files.copy(p, zout);
                zout.closeEntry();
            }
        }
    }

    @AfterAll
    static void cleanup() throws IOException {
        if (lucene != null) lucene.close();
        if (tmp != null) {
            try (Stream<Path> s = Files.walk(tmp)) {
                s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    @Test
    @DisabledIfSystemProperty(named = "pharos.regenerateParity", matches = "true")
    void localResultsBitwiseIdentical() throws Exception {
        if (!Files.exists(FIXTURE)) fail("parity fixture missing — record it with -Dpharos.regenerateParity=true");
        String actual = snapshot();
        String expected = Files.readString(FIXTURE);
        if (!actual.equals(expected)) {
            Path out = Path.of("target/pipeline-split-parity.actual.json");
            Files.writeString(out, actual);
            fail("local results moved — diff " + FIXTURE + " " + out);
        }
        assertThat(actual).isEqualTo(expected);
    }

    /** The stage-level raw API a shard serves, shaped by the coordinator, equals the stage's own results. */
    @Test
    void rawRetrievalPlusShapingEqualsStageRetrieval() throws Exception {
        HashEmbeddings embedder = new HashEmbeddings();
        KeywordSearchStrategy kw = new KeywordSearchStrategy();
        VectorSearchStrategy vec = new VectorSearchStrategy(embedder);
        KeywordRetrievalStage keywordStage = new KeywordRetrievalStage(kw);
        VectorRetrievalStage vectorStage = new VectorRetrievalStage(vec);
        UnifiedRetrievalStage unifiedStage = new UnifiedRetrievalStage(kw, embedder);
        FstQueryClassifier router = new FstQueryClassifier();
        IndexReader reader = lucene.openMultiReader(List.of(JAVA, PY, PHAROS));

        for (String q : List.of("BordaMerger", "interface for embedding generation",
                "find code that skips re-indexing files that have not changed", "greeting service")) {
            SearchRequest req = new SearchRequest(q, SearchRequest.SearchType.UNIFIED, null, null, 10,
                    "text", null, null, 0).withClassification(router.classify(q));
            SearchTrace trace = new SearchTrace();

            assertSame(keywordStage.retrieve(reader, req, trace), CandidateShaper.shape(
                    keywordStage.retrieveRaw(new IndexSearcher(reader), req, trace),
                    KeywordSearchStrategy.TYPE, KeywordSearchStrategy.shapingIntent(req)), q);
            assertSame(vectorStage.retrieve(reader, req, trace), CandidateShaper.shape(
                    vectorStage.retrieveRaw(new IndexSearcher(reader), req, trace),
                    VectorSearchStrategy.TYPE, null), q);
            assertSame(unifiedStage.retrieve(reader, req, trace), CandidateShaper.shape(
                    unifiedStage.retrieveRaw(new IndexSearcher(reader), req, embedder.embed(q), null),
                    UnifiedRetrievalStage.TYPE, UnifiedRetrievalStage.shapingIntent(req, true)), q);
        }

        RetrievalStage plain = (r, q, t) -> List.of();
        assertThatThrownBy(() -> plain.retrieveRaw(new IndexSearcher(reader), null, null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static void assertSame(List<SearchResult> expected, List<SearchResult> actual, String q) {
        assertThat(actual).as(q).isNotEmpty();
        assertThat(actual.stream().map(r -> r.id() + " " + Float.floatToRawIntBits(r.score()) + " " + r.searchType())
                .toList()).as(q)
                .isEqualTo(expected.stream()
                        .map(r -> r.id() + " " + Float.floatToRawIntBits(r.score()) + " " + r.searchType()).toList());
    }

    @Test
    @EnabledIfSystemProperty(named = "pharos.regenerateParity", matches = "true")
    void regenerate() throws Exception {
        Files.createDirectories(FIXTURE.getParent());
        Files.writeString(FIXTURE, snapshot());
    }

    private static String snapshot() throws Exception {
        List<String[]> queries = new ArrayList<>();
        for (String q : PHAROS_QUERIES) queries.add(new String[]{PHAROS, q});
        queries.addAll(SAMPLE_QUERIES);

        Map<String, Object> searches = new LinkedHashMap<>();
        for (SearchRequest.SearchType type : SearchRequest.SearchType.values()) {
            for (String[] pq : queries) {
                SearchRequest req = new SearchRequest(pq[1], type, pq[0], null, 10, "text", null, null, 0);
                List<SearchResult> results = new ArrayList<>(engine.search(req));
                // Only runs of bit-identical scores are reordered (by id): Lucene's tie order follows doc
                // ids, which the multi-threaded indexer does not fix. Any other order change fails.
                for (int i = 0; i < results.size(); ) {
                    int j = i + 1;
                    int bits = Float.floatToRawIntBits(results.get(i).score());
                    while (j < results.size() && Float.floatToRawIntBits(results.get(j).score()) == bits) j++;
                    results.subList(i, j).sort(Comparator.comparing(SearchResult::id));
                    i = j;
                }
                List<String> rows = new ArrayList<>();
                for (SearchResult r : results) {
                    rows.add(r.id() + " " + Integer.toHexString(Float.floatToRawIntBits(r.score()))
                            + " " + r.score() + " " + r.searchType());
                }
                searches.put(type.name().toLowerCase() + "|" + pq[0] + "|" + pq[1], rows);
            }
        }
        return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(searches) + "\n";
    }

    private static void unzip(Path zip, Path target) throws IOException {
        try (InputStream in = Files.newInputStream(zip); ZipInputStream zin = new ZipInputStream(in)) {
            ZipEntry e;
            while ((e = zin.getNextEntry()) != null) {
                Path out = target.resolve(e.getName()).normalize();
                if (!out.startsWith(target)) throw new IOException("zip entry escapes target: " + e.getName());
                if (e.isDirectory()) {
                    Files.createDirectories(out);
                } else {
                    Files.createDirectories(out.getParent());
                    Files.copy(zin, out);
                }
            }
        }
    }

    /** Deterministic fake embeddings: a unit vector seeded by the text. */
    static final class HashEmbeddings implements EmbeddingProvider {
        static final int DIMS = 16;

        static float[] vector(String text) {
            Random r = new Random(text.hashCode());
            float[] v = new float[DIMS];
            double norm = 0;
            for (int i = 0; i < DIMS; i++) {
                v[i] = (float) r.nextGaussian();
                norm += v[i] * v[i];
            }
            for (int i = 0; i < DIMS; i++) v[i] /= (float) Math.sqrt(norm);
            return v;
        }

        static final String MODEL = "test-hash";

        @Override public String modelId() { return MODEL; }
        @Override public float[] embed(String text) { return text == null || text.isBlank() ? null : vector(text); }
        @Override public int dimensions() { return DIMS; }
        @Override public boolean isAvailable() { return true; }
    }
}
