package com.pharos.cli;

import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.Generation;
import com.pharos.cluster.LocalFsCache;
import com.pharos.cluster.LocalSnapshot;
import com.pharos.cluster.RoleGuard;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CallGraph;
import com.pharos.indexer.FileStateTracker;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.IndexVersions;
import picocli.CommandLine.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Publishes a project's committed local index to the object store without re-indexing: the
 * retry after {@code pharos index} exited with code 3. Exit codes: 0 published, 1 not indexed,
 * 2 wrong node role or unusable source options, 3 publish failed, 4 the local index is not publishable as it is (its call
 * graph pointer is broken, or its index format is not this build's: re-index first).
 */
@Command(
        name = "publish",
        description = "Publish a project's existing local index to the cluster object store (writer nodes)",
        mixinStandardHelpOptions = true
)
public class PublishCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Project name (as shown in 'pharos projects')")
    private String projectName;

    @Mixin
    private SourceOptions sourceOptions = new SourceOptions();

    private final IndexConfig config;
    private final ProjectRegistry registry;
    private final ClusterRuntime runtime;

    public PublishCommand(IndexConfig config, ProjectRegistry registry, ClusterRuntime runtime) {
        this.config = config;
        this.registry = registry;
        this.runtime = runtime;
    }

    @Override
    public Integer call() {
        try {
            RoleGuard.requireWriter(config, "pharos publish");
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            return 2;
        }
        Optional<com.pharos.cluster.ShardSource> source;
        try {
            source = sourceOptions.source(runtime);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return 2;
        }
        Optional<ProjectMeta> meta = registry.find(projectName);
        Path shardDir = new LocalFsCache(config).shardDir(projectName);
        if (meta.isEmpty() || !Files.isDirectory(shardDir.resolve("lucene"))) {
            System.err.println("Project not indexed on this node: " + projectName);
            System.err.println("Run 'pharos index <path>' first.");
            return 1;
        }
        String unpublishable = unpublishableReason(shardDir);
        if (unpublishable != null) {
            System.err.println("Not publishing " + projectName + ": " + unpublishable);
            System.err.println("Re-index first: pharos index --full --project " + projectName + " "
                    + (meta.get().getRootPath() == null ? "<path>" : meta.get().getRootPath()));
            return 4;
        }
        try {
            LocalSnapshot snapshot = new LocalSnapshot(projectName, shardDir, meta.get(),
                    modelFingerprint(shardDir));
            Optional<Generation> gen = runtime.publisher().publish(snapshot);
            if (gen.isEmpty()) {
                System.out.println("nothing published for " + projectName + " (backend has no publisher)");
            } else {
                System.out.println("published " + projectName + " generation " + gen.get().value());
            }
            if (source.isPresent()) SourceOptions.record(runtime, projectName, source.get());
            return 0;
        } catch (IllegalArgumentException e) { // project name unusable as a shard id
            System.err.println("Publishing " + projectName + " failed: " + e.getMessage());
            return 3;
        } catch (IOException e) {
            System.err.println("Publishing " + projectName + " failed: " + e.getMessage());
            System.err.println("The local index is unchanged; retry with: pharos publish " + projectName);
            if (Boolean.getBoolean("pharos.verbose")) e.printStackTrace();
            return 3;
        }
    }

    /**
     * Why the local index must not be published, or null. A writer only publishes what it would
     * serve itself at the current format: an index that needs upgrading (even an N-1 one that this
     * build still serves) is re-indexed first, so readers never receive an old format from it.
     */
    private static String unpublishableReason(Path shardDir) {
        String pointer = CallGraph.pointerProblem(shardDir);
        if (pointer != null) return pointer;
        IndexFormat.Status status = IndexFormat.status(shardDir);
        if (status != IndexFormat.Status.OK) {
            return "its index format is " + IndexFormat.read(shardDir).format() + " (" + status.wireName()
                    + "); this pharos writes and publishes index format " + IndexVersions.INDEX_FORMAT;
        }
        return null;
    }

    /**
     * The fingerprint the last index run stamped into file-state.json (no model is loaded);
     * {@code "unknown"} when files carry different ones.
     */
    private static String modelFingerprint(Path shardDir) {
        Set<String> recorded = new FileStateTracker(shardDir).recordedModelFingerprints();
        if (recorded.isEmpty()) return "none";
        return recorded.size() == 1 ? recorded.iterator().next() : "unknown";
    }
}
