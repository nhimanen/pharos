package com.pharos.cli;

import com.pharos.config.IndexConfig;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Option(names = {"--no-progress"},
            description = "Suppress progress indicator (useful when stdout is not a terminal)")
    private boolean noProgress = false;

    private final ProjectIndexManager indexManager;
    private final CrossProjectLinker crossProjectLinker;
    private final ProjectRegistry registry;

    public IndexCommand(ProjectIndexManager indexManager,
                        CrossProjectLinker crossProjectLinker,
                        ProjectRegistry registry,
                        IndexConfig config) {
        this.indexManager = indexManager;
        this.crossProjectLinker = crossProjectLinker;
        this.registry = registry;
    }

    @Override
    public Integer call() {
        Path root = path.toAbsolutePath();

        try {
            boolean single = false;
            if (!single) {
                int depth = 3;
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
        int projectThreads = 1;
        System.out.printf("Discovered %d projects — indexing each separately (parallelism: %d)%n",
                projects.size(), projectThreads);
        System.out.println();

        long wallStart = System.currentTimeMillis();
        List<String> indexed = new CopyOnWriteArrayList<>();
        // Completion summaries are buffered when the ANSI display is active (printing to stdout
        // while the display is live would scroll the terminal and break cursor-up math).
        // They are flushed to stdout after the display clears.
        List<String> deferred = new CopyOnWriteArrayList<>();
        AtomicInteger failures = new AtomicInteger(0);

        List<String> names = projects.stream().map(DiscoveredProject::name).toList();
        MultiProjectDisplay display = new MultiProjectDisplay(names, System.console() != null, noProgress);

        ExecutorService pool = Executors.newFixedThreadPool(projectThreads,
                r -> { Thread t = new Thread(r, "project-indexer"); t.setDaemon(true); return t; });
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < projects.size(); i++) {
                final int slot = i;
                final DiscoveredProject p = projects.get(i);
                futures.add(pool.submit(() -> {
                    long projStart = System.currentTimeMillis();
                    ProgressListener listener = display.listenerFor(slot, p.name(), projStart);
                    try {
                        ProjectMeta meta = doIndexCore(p.path(), p.name(), listener);
                        long projElapsed = System.currentTimeMillis() - projStart;
                        String summary = String.format(
                                "Indexed project '%s': %d methods, %d classes in %d files (%.1fs)",
                                p.name(), meta.getMethodCount(), meta.getClassCount(),
                                meta.getFileCount(), projElapsed / 1000.0);
                        display.markDone(slot, String.format("  [%s] %d methods, %d classes in %d files (%.1fs)",
                                p.name(), meta.getMethodCount(), meta.getClassCount(),
                                meta.getFileCount(), projElapsed / 1000.0));
                        if (display.isActive()) deferred.add(summary);
                        else System.out.println(summary);
                        indexed.add(p.name());
                    } catch (Exception e) {
                        String errLine = "  [" + p.name() + "] FAILED: " + e.getMessage();
                        display.markDone(slot, errLine);
                        if (!display.isActive()) System.err.println(errLine);
                        if (Boolean.getBoolean("pharos.verbose")) e.printStackTrace();
                        failures.incrementAndGet();
                    }
                }));
            }
            for (Future<?> f : futures) {
                try { f.get(); }
                catch (Exception e) { failures.incrementAndGet(); }
            }
        } finally {
            pool.shutdown();
        }

        display.clear();
        deferred.forEach(System.out::println);

        long elapsed = System.currentTimeMillis() - wallStart;
        System.out.printf("%nDone. %d/%d projects indexed in %.1fs%n",
                indexed.size(), projects.size(), elapsed / 1000.0);

        if (indexed.size() >= 2) {
            autoLinkAll(new ArrayList<>(indexed));
        }

        return failures.get() > 0 ? 1 : 0;
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
        doIndex(projectPath, name);
        refreshCrossProjectGraph(name);
        return 0;
    }

    /**
     * Single-project indexing — manages its own progress display lifecycle.
     * Does NOT rebuild the cross-project graph (callers handle that separately).
     */
    private void doIndex(Path projectPath, String name) throws Exception {
        long start = System.currentTimeMillis();
        ProgressListener listener = noProgress ? ProgressListener.SILENT : new CliProgressPrinter(name);
        ProjectMeta meta = doIndexCore(projectPath, name, listener);
        long elapsed = System.currentTimeMillis() - start;
        if (!noProgress) {
            System.err.print("\r" + " ".repeat(80) + "\r");
        }
        System.out.printf("Indexed project '%s': %d methods, %d classes in %d files (%.1fs)%n",
                name, meta.getMethodCount(), meta.getClassCount(),
                meta.getFileCount(), elapsed / 1000.0);
    }

    /**
     * Core indexing work — parses, embeds, and writes Lucene + call graph for one project.
     * The caller is responsible for progress display lifecycle and printing the completion line.
     */
    private ProjectMeta doIndexCore(Path projectPath, String name, ProgressListener listener) throws Exception {
        boolean force = false;
        boolean full = false;
        boolean incremental = !full && !force && registry.find(name).isPresent()
                && indexManager.indexExists(name);
        boolean noEmbed = false;
        return indexManager.index(projectPath, name, incremental, force, !noEmbed, listener);
    }

    /**
     * If the just-indexed project is linked to others, rebuild the cross-project graph
     * so it reflects the fresh call graph rather than the stale pre-reindex version.
     */
    private void refreshCrossProjectGraph(String projectName) {
        List<String> linked = registry.find(projectName)
                .map(m -> m.getLinkedProjects())
                .orElse(List.of());
        if (linked.isEmpty()) return;

        List<String> all = new ArrayList<>();
        all.add(projectName);
        all.addAll(linked);
        try {
            var crossGraph = crossProjectLinker.buildCrossProjectGraph(all);
            System.out.printf("Refreshed cross-project graph: %d nodes, %d edges%n",
                    crossGraph.nodeCount(), crossGraph.edgeCount());
        } catch (Exception e) {
            System.err.printf("Warning: cross-project graph refresh failed: %s%n", e.getMessage());
            if (Boolean.getBoolean("pharos.verbose")) e.printStackTrace();
        }
    }

    // -------------------------------------------------------------------------
    // Multi-project ANSI display
    // -------------------------------------------------------------------------

    /**
     * Renders a fixed block of N lines on stderr (one per project) and updates them
     * in-place using ANSI escape codes. Each project gets its own row; unstarted
     * projects show as "pending" at the bottom of the block.
     *
     * <p>All mutations are synchronized — safe to call from concurrent indexing threads.
     *
     * <p>When not running on a TTY ({@code active == false}) the display is a no-op:
     * listeners return {@link ProgressListener#SILENT} and completion lines are printed
     * to stdout by the caller immediately when each project finishes.
     */
    private static class MultiProjectDisplay {

        private final int total;
        private final String[] lines;
        /** True when we're on a real TTY and progress is enabled. */
        private final boolean active;
        private boolean initialized = false;

        MultiProjectDisplay(List<String> names, boolean isTty, boolean noProgress) {
            this.total = names.size();
            this.lines = new String[total];
            this.active = isTty && !noProgress;
            for (int i = 0; i < total; i++) {
                lines[i] = String.format("  %-28s pending", "[" + names.get(i) + "]");
            }
            if (active) {
                for (String l : lines) System.err.println(l);
                initialized = true;
            }
        }

        boolean isActive() { return active; }

        synchronized void update(int slot, String line) {
            lines[slot] = line;
            if (active) redraw();
        }

        synchronized void markDone(int slot, String line) {
            lines[slot] = line;
            if (active) redraw();
        }

        /**
         * Clears the display block from the terminal and leaves the cursor at the top
         * of the cleared region so subsequent stdout writes appear there.
         */
        synchronized void clear() {
            if (!active || !initialized) return;
            System.err.flush();
            // Move cursor to top of block, clear each line, then return cursor to top
            System.err.print("\033[" + total + "A");
            for (int i = 0; i < total; i++) {
                System.err.print("\r\033[K\n");
            }
            System.err.print("\033[" + total + "A");
            System.err.flush();
            initialized = false;
        }

        /** Redraws all lines in-place. Must be called while holding {@code this}. */
        private void redraw() {
            if (!initialized) return;
            System.err.print("\033[" + total + "A");
            for (String line : lines) {
                System.err.printf("\r\033[K%-89s%n", line);
            }
        }

        /**
         * Returns a {@link ProgressListener} that updates the given slot.
         * {@code startMs} is used for elapsed/ETA calculation.
         */
        ProgressListener listenerFor(int slot, String name, long startMs) {
            if (!active) return ProgressListener.SILENT;
            return (stage, current, total2) ->
                    update(slot, buildLine(name, stage, current, total2, startMs));
        }

        private static String buildLine(String name, String stage, int current, int total, long startMs) {
            if ("Done".equals(stage)) {
                return String.format("  [%s] Done.", name);
            }
            if (total <= 0) {
                return String.format("  [%s] %s...  elapsed %s", name, stage, elapsed(startMs));
            }
            int pct = (int) (100L * current / total);
            int barLen = 20;
            int filled = (int) ((long) barLen * current / total);
            String bar = "#".repeat(filled) + "-".repeat(barLen - filled);
            return String.format("  [%s] %-10s [%s] %d/%d (%d%%)  %s",
                    name, stage, bar, current, total, pct, etaString(current, total, startMs));
        }

        private static String elapsed(long startMs) {
            long secs = (System.currentTimeMillis() - startMs) / 1000;
            if (secs < 60) return secs + "s";
            long mins = secs / 60; long hrs = mins / 60;
            return hrs > 0 ? (hrs + "h " + (mins % 60) + "m") : mins + "m " + (secs % 60) + "s";
        }

        private static String etaString(int current, int total, long startMs) {
            long elapsedMs = System.currentTimeMillis() - startMs;
            if (current <= 0) return "elapsed " + elapsed(startMs);
            long etaMs = elapsedMs * (total - current) / current;
            long etaSecs = etaMs / 1000;
            if (etaSecs < 1) return "elapsed " + elapsed(startMs);
            long etaMins = etaSecs / 60; long etaHrs = etaMins / 60;
            String etaStr = etaHrs > 0
                    ? (etaHrs + "h " + (etaMins % 60) + "m")
                    : (etaMins > 0 ? etaMins + "m " + (etaSecs % 60) + "s" : etaSecs + "s");
            return "ETA ~" + etaStr + "  elapsed " + elapsed(startMs);
        }
    }

    // -------------------------------------------------------------------------
    // Single-project progress printer
    // -------------------------------------------------------------------------

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

        private String elapsed() {
            long secs = (System.currentTimeMillis() - startMs) / 1000;
            if (secs < 60) return secs + "s";
            long mins = secs / 60; long hrs = mins / 60;
            return hrs > 0 ? (hrs + "h " + (mins % 60) + "m") : mins + "m " + (secs % 60) + "s";
        }

        private String etaString(int current, int total) {
            long elapsedMs = System.currentTimeMillis() - startMs;
            if (current <= 0) return "elapsed " + elapsed();
            long etaMs = (long) elapsedMs * (total - current) / current;
            long etaSecs = etaMs / 1000;
            if (etaSecs < 1) return "elapsed " + elapsed();
            long etaMins = etaSecs / 60; long etaHrs = etaMins / 60;
            String etaStr = etaHrs > 0
                    ? (etaHrs + "h " + (etaMins % 60) + "m")
                    : (etaMins > 0 ? etaMins + "m " + (etaSecs % 60) + "s" : etaSecs + "s");
            return "ETA ~" + etaStr + "  elapsed " + elapsed();
        }
    }
}
