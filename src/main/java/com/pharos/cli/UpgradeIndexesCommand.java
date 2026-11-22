package com.pharos.cli;

import com.pharos.cluster.RoleGuard;
import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.indexer.IndexFormat;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import com.pharos.indexer.ProjectNames;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Re-indexes (full) every project whose index is {@code upgrade-available}, {@code outdated}
 * or {@code newer}, so the whole set reaches the current index format. Refused on reader nodes:
 * they receive upgraded generations by syncing from the writer.
 *
 * <p>Exit codes: {@value #EXIT_OK} nothing to do, all upgraded, or aborted at the prompt;
 * {@value #EXIT_INCOMPLETE} a project was skipped (unknown, missing index, root gone) or failed;
 * {@value #EXIT_NEEDS_YES} confirmation needed but no terminal (use --yes);
 * {@value #EXIT_READER} refused on this node: a reader, or a work-queue writer (its work loop upgrades).
 *
 * <p>{@code --check} changes nothing: it reports whether this build would serve every index. With an
 * object-store backend it reads each catalog shard's {@code LATEST} manifest from the bucket (any
 * role, work-queue writers included: the Kubernetes operator runs it with the new image before an
 * upgrade); otherwise it checks the local registry. A shard or project whose index format this build
 * refuses (older than N-1, or newer) exits {@value #EXIT_REFUSED}; one that cannot be read exits
 * {@value #EXIT_INCOMPLETE}.
 */
@Command(
        name = "upgrade-indexes",
        description = "Re-index projects whose index format is not current (full re-index, embeddings included "
                + "unless --no-embed)",
        mixinStandardHelpOptions = true,
        exitCodeListHeading = "%nExit codes:%n",
        exitCodeList = {
                "0:nothing to upgrade, all upgraded, or aborted at the prompt",
                "1:a project was skipped (unknown, missing index, root gone) or failed",
                "2:confirmation needed but no terminal; re-run with --yes",
                "3:--check: an index (or a bucket shard) has a format this pharos refuses (older than N-1, or newer)",
                "5:refused on a reader node (readers receive upgrades by syncing) or on a work-queue writer (its work loop upgrades: restart it on the new release)"}
)
public class UpgradeIndexesCommand implements Callable<Integer> {

    static final int EXIT_OK = 0;
    static final int EXIT_INCOMPLETE = 1;
    static final int EXIT_NEEDS_YES = 2;
    static final int EXIT_REFUSED = 3;
    static final int EXIT_READER = 5;

    @Parameters(arity = "0..*", description = "Only these projects (default: all that need upgrading)")
    private List<String> names = new ArrayList<>();

    @Option(names = "--dry-run", description = "Print the plan and stop")
    private boolean dryRun;

    @Option(names = {"--yes", "-y"}, description = "Do not ask for confirmation")
    private boolean yes;

    @Option(names = "--no-embed", description = "Skip embedding generation (keyword-only re-index)")
    private boolean noEmbed;

    @Option(names = "--check", description = "Change nothing: exit 3 when this pharos would refuse an index"
            + " (cluster: any shard published in the bucket), 0 when it serves them all")
    private boolean check;

    private final ProjectIndexManager indexManager;
    private final ProjectRegistry registry;
    private final IndexConfig config;
    private final Function<String, String> prompter;
    private final boolean interactive;
    /** The cluster runtime ({@code --check} reads the bucket through it); null when not wired. */
    private final com.pharos.cluster.ClusterRuntime runtime;

    public UpgradeIndexesCommand(ProjectIndexManager indexManager, ProjectRegistry registry,
                                 IndexConfig config) {
        this(indexManager, registry, config, null);
    }

    public UpgradeIndexesCommand(ProjectIndexManager indexManager, ProjectRegistry registry,
                                 IndexConfig config, com.pharos.cluster.ClusterRuntime runtime) {
        this(indexManager, registry, config, UpgradeIndexesCommand::consolePrompt,
                interactiveConsole(), runtime);
    }

    /** True only on a real terminal; since JDK 22 System.console() is non-null even when redirected. */
    static boolean interactiveConsole() {
        var c = System.console();
        return c != null && c.isTerminal();
    }

    /** Test seam: injectable prompt source and interactivity. */
    UpgradeIndexesCommand(ProjectIndexManager indexManager, ProjectRegistry registry,
                          IndexConfig config, Function<String, String> prompter, boolean interactive) {
        this(indexManager, registry, config, prompter, interactive, null);
    }

    UpgradeIndexesCommand(ProjectIndexManager indexManager, ProjectRegistry registry,
                          IndexConfig config, Function<String, String> prompter, boolean interactive,
                          com.pharos.cluster.ClusterRuntime runtime) {
        this.indexManager = indexManager;
        this.registry = registry;
        this.config = config;
        this.prompter = prompter;
        this.interactive = interactive;
        this.runtime = runtime;
    }

    private String registryPath() {
        return registry.getPath() == null ? "registry.json" : registry.getPath().toString();
    }

    private static String consolePrompt(String prompt) {
        var c = System.console();
        return c == null ? null : c.readLine("%s", prompt);
    }

    @Override
    public Integer call() {
        if (check) {
            var bucket = runtime == null ? java.util.Optional.<com.pharos.cluster.ReplicationBackend>empty()
                    : runtime.objectStoreBackend();
            return bucket.isPresent() ? checkShards(bucket.get().catalog(), bucket.get().source()) : checkLocal();
        }
        com.pharos.ShutdownSequence.install(); // a signal stops it in order (drain, model calls, hooks)
        try {
            RoleGuard.requireUpgrades(config);
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            return EXIT_READER;
        }
        try {
            RoleGuard.requireNoWorkQueue(config, "pharos upgrade-indexes");
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            return EXIT_READER;
        }
        Set<String> wanted = new HashSet<>(names);
        List<ProjectMeta> todo = new ArrayList<>();
        List<ProjectMeta> missing = new ArrayList<>();
        int skipped = 0;
        int invalidCount = 0;
        Set<String> known = new HashSet<>();
        for (ProjectMeta p : registry.listAll()) {
            known.add(p.getName());
            if (!wanted.isEmpty() && !wanted.contains(p.getName())) continue;
            if (!ProjectNames.isValid(p.getName())) {
                System.out.printf("Ignoring invalid project name '%s': its index directory would be the "
                        + "index root, so pharos will not touch it. Run 'pharos remove %s' to unregister "
                        + "it safely (registry: %s); see UPGRADING.md, section \"Invalid project names\".%n",
                        p.getName(), p.getName(), registryPath());
                invalidCount++;
                continue;
            }
            switch (IndexFormat.status(IndexFormat.dir(p.getIndexPath()))) {
                case UPGRADE_AVAILABLE, OUTDATED, NEWER -> todo.add(p);
                case MISSING -> missing.add(p);
                case OK, INVALID -> { }
            }
        }
        for (String n : names) {
            if (!known.contains(n)) {
                System.err.println("Unknown project: " + n);
                skipped++;
            }
        }

        boolean embed = !noEmbed && !config.getEmbeddingProviders().isEmpty();
        if (invalidCount > 0) {
            System.out.printf("%d invalid registration%s ignored (warning only).%n",
                    invalidCount, invalidCount == 1 ? "" : "s");
        }
        if (todo.isEmpty() && missing.isEmpty() && skipped == 0) {
            System.out.println("All indexes are current; nothing to upgrade.");
            return 0;
        }

        if (!todo.isEmpty()) {
            System.out.println("Upgrade plan (" + todo.size() + " project" + (todo.size() == 1 ? "" : "s") + "):");
            for (ProjectMeta p : todo) {
                System.out.printf("  %-25s  %10s  %s  [%s]  embeddings: %s%n", p.getName(),
                        IndexStatusCommand.humanBytes(IndexStatusCommand.sizeOf(IndexFormat.dir(p.getIndexPath()))),
                        p.getRootPath(), IndexFormat.status(IndexFormat.dir(p.getIndexPath())).wireName(),
                        embed ? "yes" : "no");
            }
        }
        for (ProjectMeta p : missing) {
            System.out.printf("  %-25s  missing index dir; run pharos index --full %s %s%n",
                    p.getName(), p.getName(), p.getRootPath());
            skipped++;
        }

        if (dryRun) return skipped > 0 ? 1 : 0;
        if (todo.isEmpty()) return skipped > 0 ? 1 : 0;

        if (!yes) {
            if (!interactive) {
                System.err.println("use --yes to run non-interactively");
                return 2;
            }
            String answer = prompter.apply("Re-index " + todo.size() + " projects? [y/N] ");
            if (answer == null) { // EOF: no one to ask
                System.err.println("use --yes to run non-interactively");
                return 2;
            }
            if (!(answer.trim().equalsIgnoreCase("y")
                    || answer.trim().equalsIgnoreCase("yes"))) {
                System.out.println("Aborted.");
                return 0;
            }
        }

        int upgraded = 0, failed = 0;
        for (ProjectMeta p : todo) {
            Path root = p.getRootPath() == null ? null : Path.of(p.getRootPath());
            if (root == null || !Files.isDirectory(root)) {
                System.out.printf("Skipping '%s': root path no longer exists. re-index manually: "
                        + "pharos index --full %s <path>%n", p.getName(), p.getName());
                skipped++;
                continue;
            }
            System.out.printf("Re-indexing '%s' ...%n", p.getName());
            try {
                indexManager.index(root, p.getName(), false, false, embed, true, ProgressListener.SILENT);
                upgraded++;
                System.out.printf("  upgraded '%s'%n", p.getName());
            } catch (Exception e) {
                failed++;
                System.err.printf("  FAILED '%s': %s%n", p.getName(), e.getMessage());
                if (Boolean.getBoolean("pharos.verbose")) e.printStackTrace();
            }
        }
        System.out.printf("%nSummary: %d upgraded, %d skipped, %d failed%n", upgraded, skipped, failed);
        return skipped == 0 && failed == 0 ? 0 : 1;
    }

    /**
     * {@code --check} against the bucket: the format of every catalog shard's {@code LATEST} manifest,
     * as a reader of this build would judge it. A shard without a {@code LATEST} yet has nothing to
     * refuse. Exits {@value #EXIT_REFUSED} when any is refused, {@value #EXIT_INCOMPLETE} when the
     * catalog or a manifest cannot be read, else {@value #EXIT_OK}.
     */
    static int checkShards(com.pharos.cluster.ShardCatalog catalog, com.pharos.cluster.ReplicaSource source) {
        List<com.pharos.cluster.ShardId> shards;
        try {
            shards = catalog.shards();
        } catch (java.io.IOException e) {
            System.err.println("Reading the shard catalog failed: " + e.getMessage());
            return EXIT_INCOMPLETE;
        }
        int refused = 0;
        int unreadable = 0;
        for (com.pharos.cluster.ShardId shard : shards) {
            try {
                var gen = source.latest(shard);
                if (gen.isEmpty()) {
                    System.out.printf("  %-25s  not published yet%n", shard.value());
                    continue;
                }
                int format = source.manifest(shard, gen.get()).indexFormatVersion();
                IndexFormat.Status status = IndexFormat.statusOf(format);
                System.out.printf("  %-25s  generation %d, index format %d: %s%n", shard.value(), gen.get().value(),
                        format, status.wireName());
                if (!IndexFormat.servable(status)) refused++;
            } catch (java.io.IOException e) {
                System.err.printf("  %-25s  unreadable: %s%n", shard.value(), e.getMessage());
                unreadable++;
            }
        }
        return checkSummary(shards.size(), refused, unreadable, "shard");
    }

    /** {@code --check} without a bucket: the registry's projects, as this build would judge their indexes. */
    private int checkLocal() {
        int refused = 0;
        int total = 0;
        for (ProjectMeta p : registry.listAll()) {
            if (!ProjectNames.isValid(p.getName())) continue;
            IndexFormat.Status status = IndexFormat.status(IndexFormat.dir(p.getIndexPath()));
            if (status == IndexFormat.Status.MISSING || status == IndexFormat.Status.INVALID) continue;
            total++;
            System.out.printf("  %-25s  %s%n", p.getName(), status.wireName());
            if (!IndexFormat.servable(status)) refused++;
        }
        return checkSummary(total, refused, 0, "project");
    }

    private static int checkSummary(int total, int refused, int unreadable, String what) {
        System.out.printf("Checked %d %s%s with pharos %s (index formats %d-%d): %d refused, %d unreadable.%n",
                total, what, total == 1 ? "" : "s", com.pharos.PharosVersion.get(), IndexFormat.MIN_READABLE_FORMAT,
                com.pharos.indexer.IndexVersions.INDEX_FORMAT, refused, unreadable);
        if (refused > 0) {
            System.out.println("This pharos would refuse " + refused + " " + what + (refused == 1 ? "" : "s")
                    + ": re-index them on the current release first (see UPGRADING.md), or keep the current release.");
            return EXIT_REFUSED;
        }
        return unreadable > 0 ? EXIT_INCOMPLETE : EXIT_OK;
    }
}
