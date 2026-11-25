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
 * or {@code newer}, so the whole set reaches the current index format, and every current one whose
 * call graph has a damaged property dictionary (written before 1.10.0). Refused on reader nodes:
 * they receive upgraded generations by syncing from the writer.
 *
 * <p>Exit codes: {@value #EXIT_OK} nothing to do, all upgraded, or aborted at the prompt;
 * {@value #EXIT_INCOMPLETE} a project was skipped (unknown, missing index, root gone) or failed;
 * {@value #EXIT_NEEDS_YES} confirmation needed but no terminal (use --yes);
 * {@value #EXIT_READER} refused on this node: a reader, or a work-queue writer (its work loop upgrades).
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
                "5:refused on a reader node (readers receive upgrades by syncing) or on a work-queue writer (its work loop upgrades: restart it on the new release)"}
)
public class UpgradeIndexesCommand implements Callable<Integer> {

    static final int EXIT_OK = 0;
    static final int EXIT_INCOMPLETE = 1;
    static final int EXIT_NEEDS_YES = 2;
    static final int EXIT_READER = 5;

    @Parameters(arity = "0..*", description = "Only these projects (default: all that need upgrading)")
    private List<String> names = new ArrayList<>();

    @Option(names = "--dry-run", description = "Print the plan and stop")
    private boolean dryRun;

    @Option(names = {"--yes", "-y"}, description = "Do not ask for confirmation")
    private boolean yes;

    @Option(names = "--no-embed", description = "Skip embedding generation (keyword-only re-index)")
    private boolean noEmbed;

    private final ProjectIndexManager indexManager;
    private final ProjectRegistry registry;
    private final IndexConfig config;
    private final Function<String, String> prompter;
    private final boolean interactive;

    public UpgradeIndexesCommand(ProjectIndexManager indexManager, ProjectRegistry registry,
                                 IndexConfig config) {
        this(indexManager, registry, config, UpgradeIndexesCommand::consolePrompt,
                interactiveConsole());
    }

    /** True only on a real terminal; since JDK 22 System.console() is non-null even when redirected. */
    static boolean interactiveConsole() {
        var c = System.console();
        return c != null && c.isTerminal();
    }

    /** Test seam: injectable prompt source and interactivity. */
    UpgradeIndexesCommand(ProjectIndexManager indexManager, ProjectRegistry registry,
                          IndexConfig config, Function<String, String> prompter, boolean interactive) {
        this.indexManager = indexManager;
        this.registry = registry;
        this.config = config;
        this.prompter = prompter;
        this.interactive = interactive;
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
        List<ProjectMeta> reembed = new ArrayList<>();
        Set<String> damaged = new HashSet<>();
        boolean embeddingsConfigured = !config.getEmbeddingProviders().isEmpty();
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
            Path idx = IndexFormat.dir(p.getIndexPath());
            if (embeddingsConfigured && idx != null
                    && com.pharos.indexer.LegacyVectorFields.hasLiveMisnamedVectors(idx.resolve("lucene"))) {
                reembed.add(p);
            }
            switch (IndexFormat.status(idx)) {
                case UPGRADE_AVAILABLE, OUTDATED, NEWER -> todo.add(p);
                case MISSING -> missing.add(p);
                case OK -> {
                    // a current format with a call graph that decodes wrongly (pre-1.10.0 dictionary
                    // race): the full re-index rebuilds the graph
                    String damage = idx == null ? null
                            : com.pharos.graph.CallGraph.dictionaryProblem(com.pharos.graph.CallGraph.projectGraphDir(idx));
                    if (damage != null) {
                        System.out.printf("%s: call graph has a %s; it is re-indexed%n", p.getName(), damage);
                        damaged.add(p.getName());
                        todo.add(p);
                    }
                }
                case INVALID -> { }
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
        if (!reembed.isEmpty()) {
            // Listed, never run: a legacy-vector migration re-embeds the whole project (hours on CPU
            // for a large one), which is not what "upgrade the index format" promises.
            System.out.println("Legacy-model vectors need re-embedding (upgrade-indexes does not migrate them); run:");
            for (ProjectMeta p : reembed) {
                System.out.println("  " + com.pharos.indexer.ReindexHint.fullIndexCommand(p.getName(), p.getRootPath()));
            }
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
                        p.getRootPath(), damaged.contains(p.getName()) ? "damaged-graph"
                                : IndexFormat.status(IndexFormat.dir(p.getIndexPath())).wireName(),
                        embed ? "yes" : "no");
            }
        }
        for (ProjectMeta p : missing) {
            System.out.printf("  %-25s  missing index dir; run %s%n",
                    p.getName(), com.pharos.indexer.ReindexHint.fullIndexCommand(p.getName(), p.getRootPath()));
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
                System.out.printf("Skipping '%s': root path no longer exists. re-index manually: %s%n",
                        p.getName(), com.pharos.indexer.ReindexHint.fullIndexCommand(p.getName(), null));
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
}
