package com.pharos.cluster;

import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.TestBackends;
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
import com.pharos.parser.PythonCodeParser;
import com.pharos.search.SearchEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * A writer and a reader node in one JVM, in separate temp dirs, sharing one object store
 * (through one store instance each). Both nodes are wired like {@code Main} wires them.
 */
final class ClusterFixture implements AutoCloseable {

    static final String JAVA = "sample-app";
    static final String PY = "sample-python-app";

    final Path source;
    final IndexConfig writerConfig;
    final IndexConfig readerConfig;
    final ReplicationBackend writerBackend;
    final ReplicationBackend readerBackend;
    final ProjectRegistry writerRegistry;
    final ProjectRegistry readerRegistry;
    final LuceneIndexer writerLucene;
    final LuceneIndexer readerLucene;
    final ClusterRuntime writerRuntime;
    final ClusterRuntime readerRuntime;
    final ProjectIndexManager writerManager;
    final SearchEngine writerEngine;
    final SearchEngine readerEngine;

    ClusterFixture(Path tmp, ObjectStore writerStore, ObjectStore readerStore, String bucket) throws IOException {
        source = tmp.resolve("src");
        copy(Path.of("src/test/resources/test-projects").resolve(JAVA), source.resolve(JAVA));
        copy(Path.of("src/test/resources/test-projects").resolve(PY), source.resolve(PY));

        writerConfig = config(tmp.resolve("writer"), NodeRole.WRITER, bucket);
        readerConfig = config(tmp.resolve("reader"), NodeRole.READER, bucket);

        writerBackend = TestBackends.objectStore(writerConfig, writerStore);
        writerRegistry = ProjectRegistry.at(tmp.resolve("writer").resolve("registry.json"));
        writerLucene = new LuceneIndexer(writerConfig, writerBackend.localCache());
        writerRuntime = new ClusterRuntime(writerConfig, writerBackend, writerRegistry, writerLucene);
        EmbeddingProvider writerEmbedder = EmbeddingProvider.create(writerConfig); // NoOp: no model URL
        writerManager = new ProjectIndexManager(writerConfig, writerLucene, writerRegistry,
                List.of(writerEmbedder), noopModules(writerRegistry),
                List.of(new JavaCodeParser(), new PythonCodeParser()), writerRuntime.publisher());
        writerEngine = new SearchEngine(writerLucene, writerEmbedder, writerRegistry);

        readerBackend = TestBackends.objectStore(readerConfig, readerStore);
        readerRegistry = ClusterRuntime.registryFor(readerConfig);
        readerLucene = new LuceneIndexer(readerConfig, readerBackend.localCache());
        readerRuntime = new ClusterRuntime(readerConfig, readerBackend, readerRegistry, readerLucene);
        readerEngine = new SearchEngine(readerLucene, EmbeddingProvider.create(readerConfig), readerRegistry);
    }

    static IndexConfig config(Path home, NodeRole role, String bucket) {
        IndexConfig config = IndexConfig.defaults();
        config.setIndexDir(home.resolve("indexes"));
        config.setSynonymsFile(home.resolve("synonyms.txt"));
        ClusterConfig c = new ClusterConfig();
        c.setRole(role);
        c.setBackend(role == NodeRole.ALL ? BackendKind.LOCAL : BackendKind.OBJECT_STORE);
        c.setNodeId(role.json() + "-1");
        c.setAdvertiseUrl("http://" + role.json() + "-1:7171");
        c.setToken("test-token");
        c.setCacheDir(home.resolve("cluster-cache"));
        c.setPollIntervalSeconds(3600); // tests drive syncs explicitly
        c.getObjectStore().setBucket(bucket);
        c.getObjectStore().setPrefix("pharos");
        c.validate();
        config.setCluster(c);
        return config;
    }

    static ModuleGraphBuilder noopModules(ProjectRegistry registry) {
        return new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(
                    Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
    }

    void index(String project, boolean incremental) throws IOException {
        writerManager.index(source.resolve(project), project, incremental, false, false, false,
                ProgressListener.SILENT);
    }

    void indexBoth() throws IOException {
        index(JAVA, false);
        index(PY, false);
    }

    private static void copy(Path from, Path to) throws IOException {
        try (Stream<Path> s = Files.walk(from)) {
            for (Path p : s.toList()) {
                Path dest = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(dest);
                else Files.copy(p, dest);
            }
        }
    }

    @Override
    public void close() throws IOException {
        readerRuntime.close();
        writerRuntime.close();
        readerLucene.close();
        writerLucene.close();
    }
}
