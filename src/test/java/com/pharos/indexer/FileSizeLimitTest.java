package com.pharos.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.graph.ModuleGraphBuilder;
import com.pharos.parser.CodeParser;
import com.pharos.parser.GenericFileParser;
import com.pharos.parser.JavaCodeParser;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Non-code files become one document holding the whole file, so a large data file
 * (eval/benchmark_cache.json was 457 MB) used to be read, spooled and indexed in full — which
 * ran the indexer out of memory. Files over the configured limits are now skipped.
 */
class FileSizeLimitTest {

    @TempDir Path tmp;

    private Path projectRoot;
    private IndexConfig config;
    private LuceneIndexer luceneIndexer;

    @BeforeEach
    void setUp() throws Exception {
        projectRoot = tmp.resolve("project");
        Path pkg = Files.createDirectories(projectRoot.resolve("src/main/java/com/example"));
        Files.writeString(pkg.resolve("A.java"), "package com.example;\npublic class A { public void alpha() {} }");
        Files.writeString(projectRoot.resolve("README.md"), "# Example\nSmall readme.\n");
        Files.writeString(projectRoot.resolve("data.json"), "{\"k\": \"" + "x".repeat(3000) + "\"}");

        config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        config.setMaxDocumentFileSizeKb(2);           // data.json (~3 KB) is over, README is under
        luceneIndexer = new LuceneIndexer(config);
    }

    @Test
    void fullIndex_skipsDocumentFilesOverTheLimit() throws Exception {
        manager().index(projectRoot, "test", false, false);

        assertThat(indexedFiles()).contains("A.java", "README.md").doesNotContain("data.json");
    }

    @Test
    void incrementalIndex_removesAFileThatGrewOverTheLimit() throws Exception {
        config.setMaxDocumentFileSizeKb(8);
        manager().index(projectRoot, "test", false, false);
        assertThat(indexedFiles()).contains("data.json");

        Files.writeString(projectRoot.resolve("data.json"), "{\"k\": \"" + "x".repeat(20_000) + "\"}");
        manager().index(projectRoot, "test", true, false);

        assertThat(indexedFiles()).contains("A.java", "README.md").doesNotContain("data.json");
    }

    @Test
    void sourceFileCap_appliesToCode_andZeroMeansUnlimited() throws Exception {
        Path big = projectRoot.resolve("src/main/java/com/example/Big.java");
        Files.writeString(big, "package com.example;\npublic class Big {\n"
                + "  public void m() {}\n".repeat(200) + "}\n");   // ~4 KB
        config.setMaxSourceFileSizeKb(2);
        manager().index(projectRoot, "test", false, false);
        assertThat(indexedFiles()).contains("A.java").doesNotContain("Big.java");

        config.setMaxSourceFileSizeKb(0);
        config.setMaxDocumentFileSizeKb(0);
        manager().index(projectRoot, "test", false, false);
        assertThat(indexedFiles()).contains("A.java", "Big.java", "data.json");
    }

    @Test
    void genericParser_refusesOversizedFiles_evenWhenCalledDirectly() throws Exception {
        GenericFileParser parser = new GenericFileParser(1, 2 * 1024);
        var parsed = parser.parseFile(projectRoot.resolve("data.json"), "test");
        assertThat(parsed.classes()).isEmpty();
        assertThat(parsed.methods()).isEmpty();
        assertThat(parser.parseFile(projectRoot.resolve("README.md"), "test").classes()).isNotEmpty();
    }

    @Test
    void config_defaultsAndJsonRoundTrip() throws Exception {
        IndexConfig defaults = IndexConfig.defaults();
        assertThat(defaults.getMaxDocumentFileSizeKb()).isEqualTo(10 * 1024);
        assertThat(defaults.getMaxSourceFileSizeKb()).isEqualTo(100 * 1024);
        assertThat(defaults.maxFileSizeBytes(true)).isEqualTo(10L * 1024 * 1024);

        IndexConfig parsed = new ObjectMapper().readValue(
                "{\"maxDocumentFileSizeKb\": 64, \"maxSourceFileSizeKb\": 0}", IndexConfig.class);
        assertThat(parsed.maxFileSizeBytes(true)).isEqualTo(64L * 1024);
        assertThat(parsed.maxFileSizeBytes(false)).isEqualTo(Long.MAX_VALUE);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private ProjectIndexManager manager() {
        SelectiveIndexUpdateTest.TestRegistry registry = new SelectiveIndexUpdateTest.TestRegistry();
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        List<CodeParser> parsers = List.of(new JavaCodeParser(),
                new GenericFileParser(1, config.maxFileSizeBytes(true)));
        return new ProjectIndexManager(config, luceneIndexer, registry, List.of(), noopGraph, parsers);
    }

    private Set<String> indexedFiles() throws Exception {
        Set<String> names = new HashSet<>();
        try (DirectoryReader r = DirectoryReader.open(new NIOFSDirectory(luceneIndexer.getLucenePath("test")))) {
            for (LeafReaderContext ctx : r.leaves()) {
                var sf = ctx.reader().storedFields();
                var live = ctx.reader().getLiveDocs();
                for (int d = 0; d < ctx.reader().maxDoc(); d++) {
                    if (live != null && !live.get(d)) continue;
                    String fp = sf.document(d, Set.of(DocumentMapper.F_FILE_PATH)).get(DocumentMapper.F_FILE_PATH);
                    if (fp != null) names.add(Path.of(fp).getFileName().toString());
                }
            }
        }
        return names;
    }
}
