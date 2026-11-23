package com.pharos.cli;

import com.pharos.cluster.ClusterRuntime;
import com.pharos.cluster.ShardSource;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * {@code --source-git <url> --ref <ref>} or {@code --source-path <dir>}: where the cluster's writers
 * get this project's source. Recorded in the shard catalog, so it needs a cluster object-store backend;
 * on a single node it is refused rather than ignored.
 */
public class SourceOptions {

    @Option(names = "--source-git", paramLabel = "URL",
            description = "Git clone URL writers fetch this project's source from (needs --ref; no credentials in the URL)")
    String git;

    @Option(names = "--ref", paramLabel = "REF",
            description = "Branch, tag or commit of --source-git")
    String ref;

    @Option(names = "--source-path", paramLabel = "DIR",
            description = "Directory every writer can read this project's source at (shared volume)")
    Path sourcePath;

    boolean requested() {
        return git != null || ref != null || sourcePath != null;
    }

    /**
     * The source to record, or empty when no source option was given.
     *
     * @throws IllegalArgumentException with a message for the user when the options are unusable
     */
    Optional<ShardSource> source(ClusterRuntime runtime) {
        if (!requested()) return Optional.empty();
        if (runtime == null || runtime.sourceCatalog().isEmpty()) {
            throw new IllegalArgumentException("--source-git, --ref and --source-path record where cluster writers"
                    + " get the source, which needs a cluster object-store backend (cluster.backend"
                    + " 'object-store', role writer); this node has none. Remove them to index locally.");
        }
        if (git != null && sourcePath != null) {
            throw new IllegalArgumentException("use either --source-git or --source-path, not both");
        }
        if (git == null && ref != null) {
            throw new IllegalArgumentException("--ref needs --source-git");
        }
        if (git != null && ref == null) {
            throw new IllegalArgumentException("--source-git needs --ref (branch, tag or commit)");
        }
        return Optional.of(git != null ? ShardSource.git(git, ref)
                : ShardSource.path(sourcePath.toAbsolutePath().normalize().toString()));
    }

    /**
     * Records {@code source} for {@code project} in the catalog: on its entry and on every shard of a split
     * project that carries one (integration carry I5), in one write.
     */
    static void record(ClusterRuntime runtime, String project, ShardSource source) throws IOException {
        ObjectStoreCatalog catalog = runtime.sourceCatalog().orElseThrow();
        catalog.recordSource(project, source);
        System.out.println("recorded source " + source + " for " + project);
    }
}
