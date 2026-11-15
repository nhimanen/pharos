package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.search.SearchEngine;
import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Checked-in indexes written by earlier formats ({@code src/test/resources/compat/format-<N>.zip},
 * the sample-app project) must stay searchable while {@code N >= MIN_READABLE_FORMAT}.
 *
 * <p>Each fixture is a zip of a project index dir (Lucene dir, call graph and stamp). Zipped
 * because ArcadeDB preallocates 2 MB index pages: unzipped one fixture is ~12 MB, zipped ~30 KB.
 *
 * <p>On an {@link IndexVersions#INDEX_FORMAT} bump, regenerate the fixture for the new format:
 * {@code mvn test -Dtest=IndexCompatTest -Dpharos.regenerateCompat=true}, and keep the previous one.
 */
class IndexCompatTest {

    private static final Path FIXTURES = Paths.get("src/test/resources/compat");
    private static final Pattern FIXTURE_NAME = Pattern.compile("format-(-?\\d+)\\.zip");
    /** The project the fixtures were indexed as; document ids carry this prefix. */
    private static final String PROJECT = "sample-app";
    private static final String CLASS_ID = PROJECT + ":com.example.service.GreetingService";
    private static final String GET_NAME = "com.example.model.User#getName()";

    @TempDir Path tmp;

    /** format → fixture zip, ascending. */
    private static TreeMap<Integer, Path> fixtures() throws Exception {
        TreeMap<Integer, Path> out = new TreeMap<>();
        if (!Files.isDirectory(FIXTURES)) return out;
        try (var s = Files.list(FIXTURES)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                Matcher m = FIXTURE_NAME.matcher(p.getFileName().toString());
                if (Files.isRegularFile(p) && m.matches()) out.put(Integer.parseInt(m.group(1)), p);
            }
        }
        return out;
    }

    private static void copyTree(Path src, Path dst) throws Exception {
        try (var walk = Files.walk(src)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path t = dst.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
    }

    private static void unzip(Path zip, Path dst) throws Exception {
        try (var in = new ZipInputStream(Files.newInputStream(zip))) {
            for (ZipEntry e; (e = in.getNextEntry()) != null; ) {
                Path t = dst.resolve(e.getName()).normalize();
                if (!t.startsWith(dst)) throw new IllegalStateException("bad entry " + e.getName());
                if (e.isDirectory()) { Files.createDirectories(t); continue; }
                Files.createDirectories(t.getParent());
                Files.copy(in, t);
            }
        }
    }

    /** A search engine over a private copy of {@code fixture}, registered as {@link #PROJECT}. */
    private SearchEngine engineOver(Path fixture, String label) throws Exception {
        Path base = tmp.resolve(label);
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(base.resolve("indexes"));
        LuceneIndexer luceneIndexer = new LuceneIndexer(config);
        Path indexDir = luceneIndexer.getProjectIndexDir(PROJECT);
        unzip(fixture, indexDir);
        ProjectRegistry registry = ProjectRegistry.at(base.resolve("registry.json"));
        registry.register(new ProjectMeta(PROJECT, base.resolve("src").toString(), indexDir.toString()));
        return new SearchEngine(luceneIndexer, new NoOpEmbeddingProvider(), registry);
    }

    private static SearchRequest keyword(String q) {
        return new SearchRequest(q, SearchRequest.SearchType.KEYWORD, PROJECT, null, 20, "text", null, null, 0);
    }

    @Test
    void searchesEveryReadableFixture() throws Exception {
        TreeMap<Integer, Path> all = fixtures();
        // The N-1 obligation: a fixture for this format and for the previous one are checked in.
        assertThat(all).containsKeys(IndexVersions.INDEX_FORMAT, IndexFormat.MIN_READABLE_FORMAT);

        for (var e : all.tailMap(IndexFormat.MIN_READABLE_FORMAT, true).entrySet()) {
            int format = e.getKey();
            if (format > IndexVersions.INDEX_FORMAT) continue;
            SearchEngine engine = engineOver(e.getValue(), "f" + format);
            List<String> warnings = new ArrayList<>();

            List<SearchResult> hits = engine.searchWithTrace(keyword("GreetingService"), false).results();
            assertThat(hits).as("format-%d keyword search", format).extracting(SearchResult::id).contains(CLASS_ID);

            assertThat(engine.findCallers(GET_NAME, PROJECT, warnings))
                    .as("format-%d findCallers", format).isNotEmpty();
            assertThat(engine.findUsages(GET_NAME, "callers", PROJECT, warnings).callers())
                    .as("format-%d call graph callers", format).isNotEmpty();
            assertThat(warnings).as("format-%d warnings", format).isEmpty();
        }
    }

    @Test
    void refusesFixturesBelowMinimum() throws Exception {
        var below = fixtures().headMap(IndexFormat.MIN_READABLE_FORMAT, false);
        Assumptions.assumeFalse(below.isEmpty(), "no fixture older than the N-1 read window");
        for (var e : below.entrySet()) {
            SearchEngine engine = engineOver(e.getValue(), "f" + e.getKey());
            assertThatThrownBy(() -> engine.searchWithTrace(keyword("GreetingService"), false))
                    .as("format-%d", e.getKey())
                    .isInstanceOf(IncompatibleIndexException.class);
        }
    }

    /**
     * Writes {@code compat/format-<INDEX_FORMAT>} from sample-app with this build's code: the
     * Lucene index, the call graph and the stamp. No embeddings, so the fixture stays small.
     */
    @Test
    @EnabledIfSystemProperty(named = "pharos.regenerateCompat", matches = "true")
    void regenerate() throws Exception {
        Path root = tmp.resolve("sample-app");
        copyTree(Paths.get("src/test/resources/test-projects/sample-app"), root);
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        LuceneIndexer luceneIndexer = new LuceneIndexer(config);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path r, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        new ProjectIndexManager(config, luceneIndexer, registry, new NoOpEmbeddingProvider(), noopGraph)
                .index(root, PROJECT, false, false, false, false, ProgressListener.SILENT);
        luceneIndexer.close();

        Path built = luceneIndexer.getProjectIndexDir(PROJECT);
        assertThat(built.resolve("callgraph.arcadedb/database.lck")).as("graph closed cleanly").doesNotExist();
        Path out = FIXTURES.resolve("format-" + IndexVersions.INDEX_FORMAT + ".zip");
        Files.createDirectories(FIXTURES);
        List<Path> files;
        try (var w = Files.walk(built)) {
            files = w.filter(Files::isRegularFile).sorted().toList();
        }
        try (var zip = new ZipOutputStream(Files.newOutputStream(out))) {
            for (Path p : files) {
                String rel = built.relativize(p).toString().replace('\\', '/');
                String top = rel.contains("/") ? rel.substring(0, rel.indexOf('/')) : rel;
                boolean keep = top.equals("lucene") || top.equals(IndexFormat.FILE_NAME)
                        || top.startsWith("callgraph.arcadedb");
                // Lock files are recreated on open.
                if (!keep || rel.endsWith("write.lock")) continue;
                zip.putNextEntry(new ZipEntry(rel));
                Files.copy(p, zip);
                zip.closeEntry();
            }
        }
    }
}
