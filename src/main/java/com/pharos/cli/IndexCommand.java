package com.pharos.cli;

import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.graph.CrossProjectLinker;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectDiscovery;
import com.pharos.indexer.ProjectDiscovery.DiscoveredProject;
import com.pharos.indexer.ProjectIndexManager;
import picocli.CommandLine.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "index",
        description = "Index a project's source code (Java, Python) and documentation (Markdown, text, config files). " +
                "When pointed at a workspace root containing multiple projects, each sub-project is discovered " +
                "and indexed separately. Supports Maven, Gradle, Python, Node.js, Rust, Go, Ruby, .NET, PHP, and more.",
        mixinStandardHelpOptions = true
)
public class IndexCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Path to a project root or workspace containing multiple projects")
    private Path path;

    @Option(names = {"--project", "-p"},
            description = "Project name (default: directory name). Ignored when multiple projects are discovered.")
    private String projectName;

    @Option(names = {"--incremental"},
            description = "Only re-index changed files (default: full re-index)")
    private boolean incremental = false;

    @Option(names = {"--force"},
            description = "Delete existing index before re-indexing (required after Lucene upgrades or to fix corrupt indexes)")
    private boolean force = false;

    @Option(names = {"--no-embed"},
            description = "Skip vector embedding generation")
    private boolean noEmbed = false;

    @Option(names = {"--no-progress"},
            description = "Suppress progress indicator (useful when stdout is not a terminal)")
    private boolean noProgress = false;

    @Option(names = {"--single"},
            description = "Treat the given path as a single project even if sub-projects are detected")
    private boolean single = false;

    @Option(names = {"--depth"},
            description = "Maximum directory depth to search for sub-projects (default: 3)",
            defaultValue = "3")
    private int depth = 3;

    private final ProjectIndexManager indexManager;
    private final CrossProjectLinker crossProjectLinker;
    private final ProjectRegistry registry;

    public IndexCommand(ProjectIndexManager indexManager,
                        CrossProjectLinker crossProjectLinker,
                        ProjectRegistry registry) {
        this.indexManager = indexManager;
        this.crossProjectLinker = crossProjectLinker;
        this.registry = registry;
    }

    @Override
    public Integer call() {
        Path root = path.toAbsolutePath();

        try {
            if (!single) {
                List<DiscoveredProject> projects = ProjectDiscovery.discover(root, depth);
                if (projects.size() > 1) {
                    return indexMultiple(projects);
                }
                // Single project discovered (or root itself is the project) — fall through
                // to single-project path with the discovered name if no --project was given
                if (projects.size() == 1 && projectName == null) {
                    projectName = projects.get(0).name();
                    root = projects.get(0).path();
                }
            }
            return indexSingle(root, projectName != null ? projectName : root.getFileName().toString());

        } catch (Exception e) {
            System.err.println("\nError indexing project: " + e.getMessage());
            if (Boolean.getBoolean("pharos.verbose")) e.printStackTrace();
            return 1;
        }
    }

    private int indexMultiple(List<DiscoveredProject> projects) {
        System.out.printf("Discovered %d projects — indexing each separately%n%n", projects.size());
        int failures = 0;
        long wallStart = System.currentTimeMillis();
        List<String> indexed = new ArrayList<>();

        for (DiscoveredProject p : projects) {
            try {
                indexSingle(p.path(), p.name());
                indexed.add(p.name());
            } catch (Exception e) {
                System.err.printf("  [%s] FAILED: %s%n", p.name(), e.getMessage());
                failures++;
            }
        }

        long elapsed = System.currentTimeMillis() - wallStart;
        System.out.printf("%nDone. %d/%d projects indexed in %.1fs%n",
                indexed.size(), projects.size(), elapsed / 1000.0);

        // Auto-link all successfully indexed projects
        if (indexed.size() >= 2) {
            autoLinkAll(indexed);
        }

        return failures > 0 ? 1 : 0;
    }

    private void autoLinkAll(List<String> projectNames) {
        System.out.printf("%nLinking %d projects...%n", projectNames.size());
        try {
            // Register all pairs as linked in the registry
            for (int i = 0; i < projectNames.size(); i++) {
                for (int j = i + 1; j < projectNames.size(); j++) {
                    registry.link(projectNames.get(i), projectNames.get(j));
                }
            }
            // Build the merged cross-project graph in one pass
            var crossGraph = crossProjectLinker.buildCrossProjectGraph(projectNames);
            System.out.printf("Cross-project graph: %d nodes, %d edges%n",
                    crossGraph.nodeCount(), crossGraph.edgeCount());
        } catch (Exception e) {
            System.err.printf("Warning: cross-project linking failed: %s%n", e.getMessage());
            if (Boolean.getBoolean("pharos.verbose")) e.printStackTrace();
        }
    }

    private int indexSingle(Path projectPath, String name) throws Exception {
        long start = System.currentTimeMillis();
        ProgressListener listener = noProgress ? ProgressListener.SILENT : new CliProgressPrinter(name);
        ProjectMeta meta = indexManager.index(projectPath, name, incremental, force, !noEmbed, listener);
        long elapsed = System.currentTimeMillis() - start;

        if (!noProgress) {
            System.err.print("\r" + " ".repeat(80) + "\r");
        }
        System.out.printf("Indexed project '%s': %d methods, %d classes in %d files (%.1fs)%n",
                name, meta.getMethodCount(), meta.getClassCount(),
                meta.getFileCount(), elapsed / 1000.0);
        return 0;
    }

    /**
     * Writes in-place progress lines to stderr using carriage return.
     * Falls back to newline-separated lines when output is not a terminal.
     */
    private static class CliProgressPrinter implements ProgressListener {

        private final String projectName;
        private final boolean isTty;
        private final long startMs = System.currentTimeMillis();
        private String lastStage = "";

        CliProgressPrinter(String projectName) {
            this.projectName = projectName;
            this.isTty = System.console() != null;
        }

        @Override
        public void onProgress(String stage, int current, int total) {
            String line = buildLine(stage, current, total);
            if (isTty) {
                System.err.printf("\r%-89s", line);
            } else {
                if (!stage.equals(lastStage)) {
                    System.err.println(line);
                    lastStage = stage;
                }
            }
        }

        private String buildLine(String stage, int current, int total) {
            if ("Done".equals(stage)) {
                return "[" + projectName + "] Done.";
            }
            if (total <= 0) {
                return String.format("[%s] %s...  elapsed %s", projectName, stage, elapsed());
            }
            int pct = (int) (100L * current / total);
            int barLen = 20;
            int filled = (int) ((long) barLen * current / total);
            String bar = "#".repeat(filled) + "-".repeat(barLen - filled);
            String eta = etaString(current, total);
            return String.format("[%s] %s  [%s] %d/%d (%d%%)  %s",
                    projectName, stage, bar, current, total, pct, eta);
        }

        /** Elapsed time as a short human string: "0s", "12s", "1m 4s", "2h 15m". */
        private String elapsed() {
            long secs = (System.currentTimeMillis() - startMs) / 1000;
            if (secs < 60) return secs + "s";
            long mins = secs / 60;
            long hrs = mins / 60;
            return hrs > 0 ? (hrs + "h " + (mins % 60) + "m") : mins + "m " + (secs % 60) + "s";
        }

        /**
         * ETA string based on linear extrapolation from elapsed time.
         * Shows "ETA ~Xs" when meaningful (current > 0), or "elapsed Xs" at 0%.
         */
        private String etaString(int current, int total) {
            long elapsedMs = System.currentTimeMillis() - startMs;
            if (current <= 0) return "elapsed " + elapsed();
            long etaMs = (long) elapsedMs * (total - current) / current;
            long etaSecs = etaMs / 1000;
            if (etaSecs < 1) return "elapsed " + elapsed();
            long etaMins = etaSecs / 60;
            long etaHrs = etaMins / 60;
            String etaStr = etaHrs > 0
                    ? (etaHrs + "h " + (etaMins % 60) + "m")
                    : (etaMins > 0 ? etaMins + "m " + (etaSecs % 60) + "s" : etaSecs + "s");
            return "ETA ~" + etaStr + "  elapsed " + elapsed();
        }
    }
}
