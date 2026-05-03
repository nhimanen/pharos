package com.pharos.cli;

import com.pharos.config.ProjectMeta;
import com.pharos.indexer.ProgressListener;
import com.pharos.indexer.ProjectIndexManager;
import picocli.CommandLine.*;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(
        name = "index",
        description = "Index a project's source code (Java, Python) and documentation (Markdown, text, config files)",
        mixinStandardHelpOptions = true
)
public class IndexCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Path to the project root directory")
    private Path path;

    @Option(names = {"--project", "-p"},
            description = "Project name (default: directory name)")
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

    private final ProjectIndexManager indexManager;

    public IndexCommand(ProjectIndexManager indexManager) {
        this.indexManager = indexManager;
    }

    @Override
    public Integer call() {
        String name = projectName != null ? projectName : path.getFileName().toString();
        try {
            long start = System.currentTimeMillis();

            ProgressListener listener = noProgress ? ProgressListener.SILENT : new CliProgressPrinter(name);
            ProjectMeta meta = indexManager.index(path.toAbsolutePath(), name, incremental, force, !noEmbed, listener);
            long elapsed = System.currentTimeMillis() - start;

            // Clear the progress line and print final summary
            if (!noProgress) {
                System.err.print("\r" + " ".repeat(80) + "\r");
            }
            System.out.printf("Indexed project '%s': %d methods, %d classes in %d files (%.1fs)%n",
                    name, meta.getMethodCount(), meta.getClassCount(),
                    meta.getFileCount(), elapsed / 1000.0);
            return 0;
        } catch (Exception e) {
            System.err.println("\nError indexing project: " + e.getMessage());
            if (Boolean.getBoolean("pharos.verbose")) e.printStackTrace();
            return 1;
        }
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
