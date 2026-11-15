package com.pharos.indexer;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.embedding.NoOpEmbeddingProvider;
import com.pharos.graph.ModuleGraphBuilder;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IndexFormatWriteTest {

    private static final String PROJECT = "sample-app";

    @TempDir Path tmp;

    private Path projectRoot;
    private LuceneIndexer luceneIndexer;
    private ProjectIndexManager manager;

    @BeforeEach
    void setUp() throws Exception {
        // Copy the sample so touching a file never modifies the checked-in resources.
        Path src = Paths.get("src/test/resources/test-projects/sample-app");
        projectRoot = tmp.resolve("sample-app");
        try (var walk = Files.walk(src)) {
            for (Path p : (Iterable<Path>) walk::iterator) {
                Path t = projectRoot.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(t); else Files.copy(p, t);
            }
        }
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(tmp.resolve("indexes"));
        luceneIndexer = new LuceneIndexer(config);
        ProjectRegistry registry = ProjectRegistry.at(tmp.resolve("registry.json"));
        ModuleGraphBuilder noopGraph = new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(Path root, ProjectMeta meta,
                    com.pharos.parser.MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
        manager = new ProjectIndexManager(config, luceneIndexer, registry,
                new NoOpEmbeddingProvider(), noopGraph);
    }

    private Path dir() { return luceneIndexer.getProjectIndexDir(PROJECT); }

    private void index(boolean incremental) throws Exception {
        manager.index(projectRoot, PROJECT, incremental, false, false, false, ProgressListener.SILENT);
    }

    @Test
    void fullIndexWritesStamp() throws Exception {
        index(false);
        assertThat(IndexFormat.read(dir()).format()).isEqualTo(IndexVersions.INDEX_FORMAT);
    }

    @Test
    void incrementalIndexDoesNotRewriteStamp() throws Exception {
        index(false);
        var t1 = IndexFormat.read(dir()).writtenAt();
        assertThat(t1).isNotNull();

        Path file;
        try (var w = Files.walk(projectRoot)) {
            file = w.filter(p -> p.toString().endsWith(".java")).findFirst().orElseThrow();
        }
        Files.writeString(file, Files.readString(file) + "\n// touched\n");
        Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.fromMillis(
                System.currentTimeMillis() + 5000));
        index(true);

        assertThat(IndexFormat.read(dir()).writtenAt()).isEqualTo(t1);
    }

    @Test
    void incrementalOnOutdatedRunsFull() throws Exception {
        index(false);
        Files.delete(dir().resolve("index-format.json"));

        String ghost = "sample-app:stale.Ghost#x()";
        try (IndexWriter w = luceneIndexer.openWriter(PROJECT)) {
            Document d = new Document();
            d.add(new StringField(DocumentMapper.F_ID, ghost, Field.Store.YES));
            w.addDocument(d);
            w.commit();
        }
        assertThat(ghostHits(ghost)).isEqualTo(1);

        index(true);

        assertThat(IndexFormat.read(dir()).format()).isEqualTo(IndexVersions.INDEX_FORMAT);
        assertThat(ghostHits(ghost)).as("full rebuild drops the stray doc").isZero();
    }

    private int ghostHits(String id) throws Exception {
        try (DirectoryReader r = DirectoryReader.open(new NIOFSDirectory(luceneIndexer.getLucenePath(PROJECT)))) {
            return new IndexSearcher(r).count(new TermQuery(new Term(DocumentMapper.F_ID, id)));
        }
    }
}
