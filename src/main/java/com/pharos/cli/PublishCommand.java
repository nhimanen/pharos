package com.pharos.cli;

import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.Generation;
import com.pharos.cluster.LocalFsCache;
import com.pharos.cluster.LocalSnapshot;
import com.pharos.cluster.RoleGuard;
import com.pharos.cluster.ShardId;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.FileStateTracker;
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
 * 2 wrong node role, 3 publish failed.
 */
@Command(
        name = "publish",
        description = "Publish a project's existing local index to the cluster object store (writer nodes)",
        mixinStandardHelpOptions = true
)
public class PublishCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Project name (as shown in 'pharos projects')")
    private String projectName;

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
        Optional<ProjectMeta> meta = registry.find(projectName);
        Path shardDir = new LocalFsCache(config).shardDir(projectName);
        if (meta.isEmpty() || !Files.isDirectory(shardDir.resolve("lucene"))) {
            System.err.println("Project not indexed on this node: " + projectName);
            System.err.println("Run 'pharos index <path>' first.");
            return 1;
        }
        try {
            LocalSnapshot snapshot = new LocalSnapshot(ShardId.ofProject(projectName), shardDir, meta.get(),
                    modelFingerprint(shardDir));
            Optional<Generation> gen = runtime.publisher().publish(snapshot);
            if (gen.isEmpty()) {
                System.out.println("nothing published for " + projectName + " (backend has no publisher)");
            } else {
                System.out.println("published " + projectName + " generation " + gen.get().value());
            }
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
     * The fingerprint the last index run stamped into file-state.json (no model is loaded);
     * {@code "unknown"} when files carry different ones.
     */
    private static String modelFingerprint(Path shardDir) {
        Set<String> recorded = new FileStateTracker(shardDir).recordedModelFingerprints();
        if (recorded.isEmpty()) return "none";
        return recorded.size() == 1 ? recorded.iterator().next() : "unknown";
    }
}
