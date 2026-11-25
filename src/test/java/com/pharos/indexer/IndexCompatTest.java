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
 * <p>Graph layouts: {@code format-0} has the legacy {@code callgraph.arcadedb} dir and no
 * {@code callgraph.current} pointer, so it exercises the reader fallback; {@code format-1} has the
 * pointer and a {@code callgraph-<gen>.arcadedb} generation dir, as this build writes.
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

    /**
     * P6-R12: key filters need the key fields of format 2. A readable older fixture still answers
     * unfiltered queries, and refuses a filtered one with the fix; the current fixture filters.
     */
    @Test
    void keyFiltersNeedTheKeyFieldsFormat() throws Exception {
        TreeMap<Integer, Path> all = fixtures();
        SearchEngine current = engineOver(all.get(IndexVersions.INDEX_FORMAT), "kf-current");
        assertThat(current.searchWithTrace(keyword("GreetingService").withKeyFilter(
                new com.pharos.cluster.routing.ShardFilter(null, null, "java", null)), false).results())
                .extracting(SearchResult::id).contains(CLASS_ID);
        assertThat(current.searchWithTrace(keyword("GreetingService").withKeyFilter(
                new com.pharos.cluster.routing.ShardFilter(null, null, "py", null)), false).results()).isEmpty();

        int older = IndexFormat.MIN_READABLE_FORMAT;
        Assumptions.assumeTrue(older < com.pharos.search.KeyFiltersUnsupportedException.KEY_FIELDS_FORMAT);
        SearchEngine old = engineOver(all.get(older), "kf-old");
        assertThat(old.searchWithTrace(keyword("GreetingService"), false).results())
                .as("unfiltered search is unchanged").extracting(SearchResult::id).contains(CLASS_ID);
        assertThatThrownBy(() -> old.searchWithTrace(keyword("GreetingService").withKeyFilter(
                new com.pharos.cluster.routing.ShardFilter(null, null, "java", null)), false))
                .isInstanceOf(com.pharos.search.KeyFiltersUnsupportedException.class)
                .hasMessageContaining("index format " + older)
                .hasMessageContaining("pharos index --full " + PROJECT);
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

    // --- the real 1.3 → 1.4 migration path: a format-0 (legacy layout) index upgraded by `pharos index`

    private record Migration(IndexConfig config, LuceneIndexer luceneIndexer, ProjectRegistry registry,
                             ProjectIndexManager manager, Path root, Path indexDir) {}

    /** The format-0 fixture unzipped and registered with a copy of its source, as a 1.3 user has it. */
    private Migration legacyInstall(String label) throws Exception {
        Path base = tmp.resolve(label);
        Path root = base.resolve("sample-app");
        copyTree(Paths.get("src/test/resources/test-projects/sample-app"), root);
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(base.resolve("indexes"));
        LuceneIndexer luceneIndexer = new LuceneIndexer(config);
        Path indexDir = luceneIndexer.getProjectIndexDir(PROJECT);
        unzip(FIXTURES.resolve("format-0.zip"), indexDir);
        ProjectRegistry registry = ProjectRegistry.at(base.resolve("registry.json"));
        registry.register(new ProjectMeta(PROJECT, root.toString(), indexDir.toString()));
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path r, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        ProjectIndexManager manager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopGraph);

        assertThat(indexDir.resolve(IndexFormat.FILE_NAME)).as("fixture is unstamped").doesNotExist();
        assertThat(indexDir.resolve(com.pharos.graph.CallGraph.LEGACY_DIR)).isDirectory();
        assertThat(indexDir.resolve(com.pharos.graph.CallGraph.POINTER_FILE)).doesNotExist();
        return new Migration(config, luceneIndexer, registry, manager, root, indexDir);
    }

    /** What a plain `pharos index <path>` does on an existing project: an incremental request. */
    private static List<String> indexIncrementally(Migration m) throws Exception {
        List<String> messages = new ArrayList<>();
        m.manager().index(m.root(), PROJECT, true, false, false, false,
                (stage, done, total) -> messages.add(stage));
        return messages;
    }

    private static void assertMigrated(Migration m, List<String> messages) throws Exception {
        assertThat(messages).anyMatch(s -> s.startsWith("index format changed (0 → " + IndexVersions.INDEX_FORMAT));
        assertThat(m.indexDir().resolve(com.pharos.graph.CallGraph.POINTER_FILE)).isRegularFile();
        assertThat(com.pharos.graph.CallGraph.pointerProblem(m.indexDir())).isNull();
        assertThat(m.indexDir().resolve(com.pharos.graph.CallGraph.LEGACY_DIR))
                .as("legacy graph removed").doesNotExist();
        assertThat(IndexFormat.read(m.indexDir()).format()).isEqualTo(IndexVersions.INDEX_FORMAT);
        assertThat(IndexFormat.status(m.indexDir())).isEqualTo(IndexFormat.Status.OK);

        SearchEngine engine = new SearchEngine(m.luceneIndexer(), new NoOpEmbeddingProvider(), m.registry());
        List<String> warnings = new ArrayList<>();
        assertThat(engine.searchWithTrace(keyword("GreetingService"), false).results())
                .extracting(SearchResult::id).contains(CLASS_ID);
        assertThat(engine.findCallers(GET_NAME, PROJECT, warnings)).isNotEmpty();
        assertThat(engine.findUsages(GET_NAME, "callers", PROJECT, warnings).callers()).isNotEmpty();
        assertThat(warnings).isEmpty();
    }

    /** Method count of the same sources indexed from scratch by this build. */
    private long freshBuildMethodCount() throws Exception {
        Migration fresh = legacyInstall("fresh");
        fresh.manager().index(fresh.root(), "fresh", false, false, false, false, ProgressListener.SILENT);
        Path dir = fresh.luceneIndexer().getProjectIndexDir("fresh");
        try (var g = com.pharos.graph.CallGraph.openExisting(com.pharos.graph.CallGraph.projectGraphDir(dir))) {
            return g.methodCount();
        }
    }

    @Test
    void format0IndexIsUpgradedByAPlainIndexRun() throws Exception {
        Migration m = legacyInstall("migrate");
        assertMigrated(m, indexIncrementally(m));
    }

    @Test
    void readerHoldingTheLegacyGraphAcrossTheUpgradeDoesNotCorruptTheNewGeneration() throws Exception {
        Migration m = legacyInstall("migrate-reader");
        Path legacy = m.indexDir().resolve(com.pharos.graph.CallGraph.LEGACY_DIR);
        var reader = com.pharos.graph.CallGraph.openExisting(legacy);
        assertThat(reader.callers(GET_NAME)).isNotEmpty();

        List<String> messages = indexIncrementally(m);
        assertThat(reader.callers(GET_NAME)).as("old reader keeps working").isNotEmpty();
        reader.close();  // a request in flight finishing after the swap

        assertMigrated(m, messages);
        assertThat(legacy).as("closing the old reader recreates nothing").doesNotExist();
        Path live = com.pharos.graph.CallGraph.projectGraphDir(m.indexDir());
        try (var g = com.pharos.graph.CallGraph.openExisting(live)) {
            assertThat(g.methodCount()).isEqualTo(freshBuildMethodCount());
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
        assertThat(com.pharos.graph.CallGraph.projectGraphDir(built).resolve("database.lck"))
                .as("graph closed cleanly").doesNotExist();
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
                        || top.startsWith("callgraph");  // pointer, generation dir, sidecars
                // Lock files are recreated on open.
                if (!keep || rel.endsWith("write.lock")) continue;
                zip.putNextEntry(new ZipEntry(rel));
                Files.copy(p, zip);
                zip.closeEntry();
            }
        }
    }
}
