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
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * The frozen corpus and the snapshot of {@link PipelineSplitParityTest}. Kept free of APIs newer
 * than the pre-split code (8629976) so the expected outputs can be recorded by that build: copy this
 * file into a clone at that commit and call {@link #snapshot} on its {@code SearchEngine}.
 *
 * <p>The indexes are frozen (the indexer is not deterministic run to run: doc ids, colliding
 * overload ids), so tie order is fixed and compared exactly. They carry an index format stamp:
 * when {@code INDEX_FORMAT} moves past that format + 1, the stamp falls out of the N-1 read window,
 * and the zip and the expected outputs must be regenerated
 * ({@code -Dpharos.regenerateParity=true}, which rebuilds the indexes from {@code pharos-src.zip}).
 */
final class ParityCorpus {

    static final Path FIXTURE = Path.of("src/test/resources/parity/pipeline-split-parity.json");
    static final Path INDEX_ZIP = Path.of("src/test/resources/parity/pipeline-split-indexes.zip");
    static final Path PHAROS_CORPUS = Path.of("src/test/resources/parity/pharos-src.zip");

    static final String JAVA = "sample-app";
    static final String PY = "sample-python-app";
    static final String PHAROS = "pharos";

    /** eval/benchmark.py QUERIES whose project is pharos (category + index in the comment). */
    static final List<String> PHAROS_QUERIES = List.of(
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


    /** Queries also run at a deeper cut-off, where more ties and tail candidates show. */
    static final List<String[]> DEEP_QUERIES = List.of(
            new String[]{PHAROS, "SearchEngine"},
            new String[]{PHAROS, "pipeline stage retrieval merge rerank"});
    static final int DEEP_LIMIT = 25;

    private ParityCorpus() {}

    /** Unzips the frozen indexes under {@code base} and registers the three projects. */
    static ProjectRegistry openFrozen(Path base, LuceneIndexer lucene) throws IOException {
        ProjectRegistry registry = ProjectRegistry.at(base.resolve("registry.json"));
        for (String project : List.of(JAVA, PY, PHAROS)) {
            Path indexDir = lucene.getProjectIndexDir(project);
            Files.createDirectories(indexDir);
            ProjectMeta meta = new ProjectMeta(project, base.resolve("src/" + project).toString(),
                    indexDir.toString());
            meta.setEmbeddedModels(List.of(HashEmbeddings.MODEL));
            registry.register(meta);
        }
        unzip(INDEX_ZIP, lucene.getIndexDir());
        return registry;
    }

    /** Ids, raw float bits and search type of every query × pipeline type, in engine order. */
    static String snapshot(SearchEngine engine) throws Exception {
        List<String[]> queries = new ArrayList<>();
        for (String q : PHAROS_QUERIES) queries.add(new String[]{PHAROS, q});
        queries.addAll(SAMPLE_QUERIES);

        Map<String, Object> searches = new LinkedHashMap<>();
        for (SearchRequest.SearchType type : SearchRequest.SearchType.values()) {
            for (String[] pq : queries) {
                searches.put(type.name().toLowerCase() + "|" + pq[0] + "|" + pq[1], rows(engine, type, pq, 10));
            }
            for (String[] pq : DEEP_QUERIES) {
                searches.put(type.name().toLowerCase() + "|" + pq[0] + "|" + pq[1] + "|limit=" + DEEP_LIMIT,
                        rows(engine, type, pq, DEEP_LIMIT));
            }
        }
        return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(searches) + "\n";
    }

    private static List<String> rows(SearchEngine engine, SearchRequest.SearchType type, String[] pq, int limit)
            throws Exception {
        SearchRequest req = new SearchRequest(pq[1], type, pq[0], null, limit, "text", null, null, 0);
        List<String> rows = new ArrayList<>();
        for (SearchResult r : engine.search(req)) {
            rows.add(r.id() + " " + Integer.toHexString(Float.floatToRawIntBits(r.score()))
                    + " " + r.score() + " " + r.searchType());
        }
        return rows;
    }

    /** Indexes the three corpora with this build and zips what search reads of the index dirs. */
    static void buildIndexZip(Path base) throws Exception {
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

    static void unzip(Path zip, Path target) throws IOException {
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
    static class HashEmbeddings implements EmbeddingProvider {
        static final int DIMS = 16;
        static final String MODEL = "test-hash";

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

        @Override public String modelId() { return MODEL; }
        @Override public float[] embed(String text) { return text == null || text.isBlank() ? null : vector(text); }
        @Override public int dimensions() { return DIMS; }
        @Override public boolean isAvailable() { return true; }
    }
}
