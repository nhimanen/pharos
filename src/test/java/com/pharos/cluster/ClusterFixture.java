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
public final class ClusterFixture implements AutoCloseable {

    public static final String JAVA = "sample-app";
    public static final String PY = "sample-python-app";

    public final Path source;
    public final IndexConfig writerConfig;
    public final IndexConfig readerConfig;
    final ReplicationBackend writerBackend;
    final ReplicationBackend readerBackend;
    public final ProjectRegistry writerRegistry;
    final ProjectRegistry readerRegistry;
    public final LuceneIndexer writerLucene;
    final LuceneIndexer readerLucene;
    final ClusterRuntime writerRuntime;
    final ClusterRuntime readerRuntime;
    final ProjectIndexManager writerManager;
    final SearchEngine writerEngine;
    final SearchEngine readerEngine;
    /** True when an embedder was given: indexing then embeds. */
    private final boolean embed;

    ClusterFixture(Path tmp, ObjectStore writerStore, ObjectStore readerStore, String bucket) throws IOException {
        this(tmp, writerStore, readerStore, bucket, null);
    }

    /**
     * @param embedder the writer's (and the fixture reader's) embedder; null: the configured one
     *                 (a no-op, so no vectors are indexed)
     */
    public ClusterFixture(Path tmp, ObjectStore writerStore, ObjectStore readerStore, String bucket,
                          EmbeddingProvider embedder) throws IOException {
        source = tmp.resolve("src");
        copy(Path.of("src/test/resources/test-projects").resolve(JAVA), source.resolve(JAVA));
        copy(Path.of("src/test/resources/test-projects").resolve(PY), source.resolve(PY));

        writerConfig = config(tmp.resolve("writer"), NodeRole.WRITER, bucket);
        readerConfig = config(tmp.resolve("reader"), NodeRole.READER, bucket);

        writerBackend = TestBackends.objectStore(writerConfig, writerStore);
        writerRegistry = ProjectRegistry.at(tmp.resolve("writer").resolve("registry.json"));
        writerLucene = new LuceneIndexer(writerConfig, writerBackend.localCache());
        writerRuntime = new ClusterRuntime(writerConfig, writerBackend, writerRegistry, writerLucene);
        embed = embedder != null;
        EmbeddingProvider writerEmbedder = embedder != null ? embedder
                : EmbeddingProvider.create(writerConfig); // NoOp: no model URL
        writerManager = new ProjectIndexManager(writerConfig, writerLucene, writerRegistry,
                List.of(writerEmbedder), noopModules(writerRegistry),
                List.of(new JavaCodeParser(), new PythonCodeParser()), writerRuntime.publisher());
        writerEngine = new SearchEngine(writerLucene, writerEmbedder, writerRegistry);

        readerBackend = TestBackends.objectStore(readerConfig, readerStore);
        readerRegistry = ClusterRuntime.registryFor(readerConfig);
        readerLucene = new LuceneIndexer(readerConfig, readerBackend.localCache());
        readerRuntime = new ClusterRuntime(readerConfig, readerBackend, readerRegistry, readerLucene);
        readerEngine = new SearchEngine(readerLucene,
                embedder != null ? embedder : EmbeddingProvider.create(readerConfig), readerRegistry);
    }

    public static IndexConfig config(Path home, NodeRole role, String bucket) {
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
        c.setRetireGraceSeconds(7200);
        c.getObjectStore().setBucket(bucket);
        c.getObjectStore().setPrefix("pharos");
        c.getWriter().setWorkQueue(false); // fixtures index and publish by hand; work-queue tests opt in
        c.validate();
        config.setCluster(c);
        return config;
    }

    public static ModuleGraphBuilder noopModules(ProjectRegistry registry) {
        return new ModuleGraphBuilder(registry) {
            @Override
            public synchronized List<String> incorporate(
                    Path root, ProjectMeta meta, MavenPomReader.PomInfo pomInfo) {
                return List.of();
            }
        };
    }

    public void index(String project, boolean incremental) throws IOException {
        index(project, incremental, embed);
    }

    /** Indexes a bundled project; {@code withVectors} false indexes it without embeddings ({@code --no-embed}). */
    public void index(String project, boolean incremental, boolean withVectors) throws IOException {
        writerManager.index(source.resolve(project), project, incremental, false, withVectors && embed, false,
                ProgressListener.SILENT);
    }

    /** Indexes {@code root} as {@code project} on the writer (and publishes it). */
    public void indexSource(Path root, String project) throws IOException {
        writerManager.index(root, project, false, false, embed, false, ProgressListener.SILENT);
    }

    /**
     * Splits {@code project} by {@code spec} into {@code shards} in the catalog (a new spec version and
     * the project entry), then indexes {@code root} as that split project and publishes every shard.
     * {@code incremental} runs after the first only re-index what changed.
     */
    public void indexSplit(Path root, String project, com.pharos.cluster.sharding.ShardSpec spec, List<ShardId> shards,
                           boolean incremental) throws IOException {
        var catalog = (com.pharos.cluster.objectstore.ObjectStoreCatalog) writerBackend.catalog();
        var current = catalog.projectShards();
        if (!current.specOf(project).equals(spec) || !current.shardsOf(project).equals(shards)) {
            int v = catalog.addSpec(java.util.Optional.of(project), spec);
            catalog.putProject(project, v, shards);
        }
        writerManager.index(root, project, incremental, false, embed, false, ProgressListener.SILENT,
                com.pharos.indexer.IndexAbort.none(), null, new com.pharos.cluster.sharding.ShardPlan(project, spec, shards));
    }

    /**
     * The resharding job of this fixture's writer (Phase 6 Task 4), driven by hand: its reshard lease, the
     * writer's index manager and publisher, {@code readers} as the live readers the flip waits for.
     */
    public com.pharos.cluster.sharding.ReshardJob reshardJob(
            java.util.function.Supplier<List<com.pharos.cluster.membership.ReaderHeartbeat>> readers,
            java.time.Clock clock, java.time.Duration syncTimeout) {
        var os = (com.pharos.cluster.objectstore.ObjectStoreBackend) writerBackend;
        var leases = new com.pharos.cluster.writer.LeaseManager(os.store(), os.layout(), "writer-1",
                java.time.Duration.ofSeconds(30), com.pharos.cluster.writer.LeaseManager.BucketNow.NONE, clock,
                id -> os.layout().reshardLeaseKey(id.value()));
        return new com.pharos.cluster.sharding.ReshardJob("writer-1", os.store(), os.layout(), os.objectStoreCatalog(),
                (com.pharos.cluster.objectstore.ObjectStorePublisher) os.publisher(), os.source(), os.writerState(),
                writerManager, writerLucene, leases, readers,
                new com.pharos.cluster.objectstore.GenerationGc(os.store(), os.layout(), 2, java.time.Duration.ofMinutes(10)),
                clock, new com.pharos.cluster.sharding.ReshardJob.Settings(syncTimeout, java.time.Duration.ofMinutes(10)));
    }

    /** The writer's catalog, with its write side. */
    public com.pharos.cluster.objectstore.ObjectStoreCatalog writerCatalog() {
        return (com.pharos.cluster.objectstore.ObjectStoreCatalog) writerBackend.catalog();
    }

    /**
     * A reader node of this cluster's bucket in its own home, as {@code Main} wires a serving
     * reader: its backend over {@code store}, registry, indexer and runtime. Not started.
     */
    public static ReaderParts reader(Path home, ObjectStore store, String bucket, String nodeId, List<String> pin) {
        return reader(home, store, bucket, nodeId, pin, c -> { });
    }

    /** As above, {@code tune} adjusting the cluster config (timeouts) before it is validated. */
    public static ReaderParts reader(Path home, ObjectStore store, String bucket, String nodeId, List<String> pin,
                                     java.util.function.Consumer<com.pharos.config.ClusterConfig> tune) {
        IndexConfig config = config(home, NodeRole.READER, bucket);
        config.getCluster().setNodeId(nodeId);
        config.getCluster().getReader().setShards(pin);
        config.getCluster().setHeartbeatSeconds(1);
        config.getCluster().setReaderTtlSeconds(30);
        config.getCluster().setFanoutTimeoutMillis(3000);
        config.getCluster().setConnectTimeoutMillis(500);
        tune.accept(config.getCluster());
        config.getCluster().validate();
        ReplicationBackend backend = TestBackends.objectStore(config, store);
        ProjectRegistry registry = ClusterRuntime.registryFor(config);
        LuceneIndexer lucene = new LuceneIndexer(config, backend.localCache());
        return new ReaderParts(config, registry, lucene, new ClusterRuntime(config, backend, registry, lucene));
    }

    /** What {@link #reader} builds. */
    public record ReaderParts(IndexConfig config, ProjectRegistry registry, LuceneIndexer lucene,
                              ClusterRuntime runtime) {}

    /** Closes the writer side only (for fixtures whose reader was never used). */
    public void closeWriter() throws IOException {
        writerRuntime.close();
        writerLucene.close();
    }

    public void indexBoth() throws IOException {
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
