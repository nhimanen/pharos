package com.pharos.cli;

import com.pharos.cluster.BackendKind;
import com.pharos.cluster.ShardId;
import com.pharos.cluster.objectstore.ObjectStore;
import com.pharos.cluster.objectstore.ObjectStoreCatalog;
import com.pharos.cluster.objectstore.ObjectStoreLayout;
import com.pharos.cluster.objectstore.S3ObjectStore;
import com.pharos.cluster.sharding.ProjectShards;
import com.pharos.cluster.sharding.ReshardJob;
import com.pharos.cluster.sharding.ShardSpec;
import com.pharos.config.ClusterConfig;
import com.pharos.config.IndexConfig;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * {@code pharos reshard <project> --spec "<spec>"}: requests a reshard of a cluster project to a new
 * shard spec (a new spec version in the catalog); the writer holding the project builds it while readers
 * keep serving the current version, then flips routing ({@link ReshardJob}). {@code --status} shows the
 * project's versions and the job's progress; {@code --cancel} abandons an open reshard (its shards are
 * GC'd after the retention window). Object-store clusters only (P6-R9). Exit codes: 0 done, 1 refused
 * (another reshard of the project is open, unknown project, nothing to cancel), 2 bad usage or config,
 * 3 the bucket failed.
 */
@Command(
        name = "reshard",
        description = "Split a cluster project by a new shard spec, live (object-store clusters)",
        mixinStandardHelpOptions = true
)
public class ReshardCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Project name (as in the cluster catalog)")
    private String project;

    @Option(names = "--spec", description = "The new shard spec, e.g. \"[project, ring(path, shards=4)]\" or"
            + " \"[project, module]\"")
    private String spec;

    @Option(names = "--status", description = "Show the project's spec versions and the reshard's progress")
    private boolean status;

    @Option(names = "--cancel", description = "Abandon the project's open reshard (its shards are GC'd later)")
    private boolean cancel;

    private final IndexConfig config;
    private final Function<ClusterConfig, ObjectStore> stores;

    public ReshardCommand(IndexConfig config) {
        this(config, c -> new S3ObjectStore(c.getObjectStore()));
    }

    /** @param stores opens the cluster's bucket (tests inject one) */
    public ReshardCommand(IndexConfig config, Function<ClusterConfig, ObjectStore> stores) {
        this.config = config;
        this.stores = stores;
    }

    @Override
    public Integer call() {
        int modes = (spec != null ? 1 : 0) + (status ? 1 : 0) + (cancel ? 1 : 0);
        if (modes != 1) {
            System.err.println("pharos reshard: give exactly one of --spec \"<spec>\", --status or --cancel");
            return 2;
        }
        ClusterConfig c = config.getCluster();
        if (c == null || c.getBackend() != BackendKind.OBJECT_STORE || c.getObjectStore() == null
                || c.getObjectStore().getBucket() == null || c.getObjectStore().getBucket().isBlank()) {
            System.err.println("pharos reshard needs cluster.backend \"object-store\" and cluster.objectStore.bucket:"
                    + " splitting a project into several shards is supported on object-store clusters only");
            return 2;
        }
        if (!ShardId.isProjectName(project)) {
            System.err.println("'" + project + "' is not a valid cluster project name");
            return 2;
        }
        ShardSpec target = null;
        if (spec != null) {
            try {
                target = ShardSpec.parse(spec);
            } catch (IllegalArgumentException e) {
                System.err.println(e.getMessage());
                return 2;
            }
            if (target.isDefault()) {
                System.err.println("pharos reshard: resharding back to [project] is not supported; a single-shard ring"
                        + " serves the project from one shard: --spec \"[project, ring(path, shards=1)]\"");
                return 2;
            }
        }
        try (ObjectStore store = stores.apply(c)) {
            ObjectStoreLayout layout = new ObjectStoreLayout(c.getObjectStore().getPrefix());
            ObjectStoreCatalog catalog = new ObjectStoreCatalog(store, layout);
            ProjectShards map = catalog.projectShards();
            if (!map.projects().contains(project)) {
                System.err.println("project " + project + " is not in the cluster catalog (publish it first)");
                return 1;
            }
            if (status) {
                ReshardJob.status(store, layout, map, project).forEach(System.out::println);
                return 0;
            }
            if (cancel) {
                if (!catalog.cancelReshard(project, Instant.now())) {
                    System.out.println("project " + project + " has no open reshard");
                    return 1;
                }
                System.out.println("cancelled the reshard of " + project + "; its staged shards are removed after the"
                        + " retention window (cluster.retentionMinutes)");
                return 0;
            }
            int version;
            try {
                // final review I2: refused while a live reader runs a release without sharding
                List<com.pharos.cluster.membership.ReaderHeartbeat> live =
                        com.pharos.cluster.membership.ReaderRegistry.observer(store, layout, c).refreshNow();
                version = catalog.requestReshard(project, target, Instant.now(), live);
            } catch (IllegalStateException e) {
                System.err.println(e.getMessage());
                return 1;
            }
            System.out.println("requested: project " + project + " to spec version " + version + " " + target
                    + " (now " + map.specOf(project) + "); the writer holding the project builds it while readers serve"
                    + " the current version, then flips. Follow it with: pharos reshard " + project + " --status");
            return 0;
        } catch (IOException | RuntimeException e) {
            System.err.println("pharos reshard " + project + " failed: " + e.getMessage());
            return 3;
        }
    }
}
