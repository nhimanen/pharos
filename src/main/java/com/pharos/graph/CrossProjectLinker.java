package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Links call graphs across projects by resolving previously-unresolved call references.
 *
 * When project A calls a method from project B, JavaParser cannot resolve the type
 * during A's indexing pass.  This linker runs after both projects are indexed and
 * matched, resolving unresolved calls in A against known packages in B.
 *
 * <h3>Performance design</h3>
 * <ul>
 *   <li><b>GraphIndex</b> — each project graph is indexed by simple method name once.
 *       Lookup drops from O(M) linear scan to O(1).</li>
 *   <li><b>Parallel pair resolution</b> — (i,j) project pairs resolved concurrently.</li>
 *   <li><b>Freshness guard</b> — cross-project graph is skipped when already newer than
 *       every involved project's {@code graph.stamp} sentinel file.</li>
 *   <li><b>Save guard</b> — cross-project graph only rewritten when edges changed.</li>
 * </ul>
 */
public class CrossProjectLinker {

    private static final Logger log = LoggerFactory.getLogger(CrossProjectLinker.class);

    /**
     * One lock per cross-project DB path.  Serialises concurrent calls to
     * buildCrossProjectGraph() when multiple projects are indexed in parallel
     * (--project-threads > 1) and each triggers a cross-project rebuild on
     * the same directory.
     */
    private static final ConcurrentHashMap<Path, ReentrantLock> REBUILD_LOCKS =
            new ConcurrentHashMap<>();

    private final ProjectRegistry registry;
    private final Path crossDbDir;
    private final Path crossStamp;

    public CrossProjectLinker(IndexConfig config, ProjectRegistry registry) {
        this.registry = registry;
        Path base = (config != null && config.getIndexDir() != null)
                ? config.getIndexDir().getParent()
                : IndexConfig.DEFAULT_BASE;
        this.crossDbDir = base.resolve("cross-project.arcadedb");
        this.crossStamp = base.resolve("cross-project.stamp");
    }

    /** Convenience overload for exactly two projects. */
    public void buildCrossProjectGraph(String project1, String project2) throws IOException {
        buildCrossProjectGraph(List.of(project1, project2));
    }

    /**
     * Build the cross-project call graph by combining all given projects and
     * resolving cross-project call edges into
     * {@code ~/.pharos/cross-project.arcadedb/}.
     */
    public void buildCrossProjectGraph(List<String> projectNames) throws IOException {
        if (projectNames.size() < 2) {
            throw new IllegalArgumentException("Need at least 2 projects to link");
        }

        // Serialise concurrent rebuilds on the same DB directory.
        // When indexing a workspace with --project-threads > 1, multiple projects
        // finish simultaneously and all call buildCrossProjectGraph; without this
        // lock they race to delete+recreate the same directory.
        ReentrantLock lock = REBUILD_LOCKS.computeIfAbsent(crossDbDir, k -> new ReentrantLock());
        lock.lock();
        try {
            buildCrossProjectGraphLocked(projectNames);
        } finally {
            lock.unlock();
        }
    }

    private void buildCrossProjectGraphLocked(List<String> projectNames) throws IOException {
        // Freshness guard: skip if cross-project DB is already up to date
        if (isCrossGraphFresh(crossStamp, projectNames)) {
            log.info("Cross-project graph is already up to date for {}, skipping rebuild",
                    projectNames);
            return;
        }

        // Load per-project graphs and build lookup indices
        List<ProjectMeta> metas = new ArrayList<>();
        Map<String, GraphIndex> indices = new LinkedHashMap<>();

        for (String name : projectNames) {
            ProjectMeta meta = registry.find(name)
                    .orElseThrow(() -> new IllegalArgumentException("Project not found: " + name));
            metas.add(meta);
            Path dbDir = Path.of(meta.getIndexPath()).resolve("callgraph.arcadedb");
            if (Files.isDirectory(dbDir)) {
                try (CallGraph g = CallGraph.open(dbDir)) {
                    indices.put(name, new GraphIndex(g));
                }
            }
        }

        // Parallel (i,j) pair resolution
        List<int[]> pairs = new ArrayList<>();
        for (int i = 0; i < metas.size(); i++) {
            for (int j = 0; j < metas.size(); j++) {
                if (i != j) pairs.add(new int[]{i, j});
            }
        }

        int threads = Math.min(pairs.size(), Math.max(1,
                Runtime.getRuntime().availableProcessors()));
        ExecutorService pool = Executors.newFixedThreadPool(threads,
                r -> { Thread t = new Thread(r, "cross-linker"); t.setDaemon(true); return t; });

        List<Future<List<String[]>>> futures = new ArrayList<>(pairs.size());
        for (int[] pair : pairs) {
            final ProjectMeta srcMeta = metas.get(pair[0]);
            final GraphIndex  tgtIdx  = indices.get(metas.get(pair[1]).getName());
            if (tgtIdx == null) continue;
            futures.add(pool.submit(() -> {
                List<String[]> edges = new ArrayList<>();
                for (ProjectMeta.UnresolvedRef ref : srcMeta.getUnresolvedRefs()) {
                    String candidate = findInIndex(ref, tgtIdx);
                    if (candidate != null) {
                        edges.add(new String[]{ref.callerFqn, candidate});
                        log.debug("Cross-project link: {} → {}", ref.callerFqn, candidate);
                    }
                }
                return edges;
            }));
        }
        pool.shutdown();

        // Collect resolved edges
        List<String[]> resolvedEdges = new ArrayList<>();
        try {
            for (Future<List<String[]>> f : futures) {
                resolvedEdges.addAll(f.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Cross-project linking interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Cross-project linking failed: " + e.getCause().getMessage(),
                    e.getCause());
        }

        log.info("Cross-project linking: {} new edges across {} projects",
                resolvedEdges.size(), projectNames.size());

        // Save guard: only rewrite if something changed
        if (!resolvedEdges.isEmpty() || !Files.isDirectory(crossDbDir)) {
            // Delete and recreate instead of clear() — avoids the O(M) DELETE VERTEX
            // scan and the LSM compaction-file-removal race that causes
            // "File with id N has been removed" errors on concurrent writes.
            if (Files.isDirectory(crossDbDir)) {
                try (var s = Files.walk(crossDbDir)) {
                    s.sorted(java.util.Comparator.reverseOrder())
                     .forEach(p -> { try { Files.delete(p); } catch (java.io.IOException ignored) {} });
                }
            }
            try (CallGraph crossGraph = CallGraph.open(crossDbDir)) {
                // Seed all project-owned methods from each per-project graph
                for (String name : projectNames) {
                    Path dbDir = Path.of(
                            registry.find(name).map(ProjectMeta::getIndexPath).orElse(""))
                            .resolve("callgraph.arcadedb");
                    if (Files.isDirectory(dbDir)) {
                        try (CallGraph g = CallGraph.open(dbDir)) {
                            g.allFqns().forEach(fqn -> crossGraph.addMethod(fqn, fqn));
                            crossGraph.flush();
                        }
                    }
                }
                // Add resolved cross-project edges
                for (String[] edge : resolvedEdges) {
                    crossGraph.addCall(edge[0], edge[1]);
                }
                crossGraph.flush();
            }
            touchStamp(crossStamp);
        }
    }

    /**
     * Opens the cross-project graph for querying (e.g., by PathCommand).
     * Caller must close the returned graph.
     */
    public CallGraph loadCrossProjectGraph() {
        return CallGraph.open(crossDbDir);
    }

    // -------------------------------------------------------------------------
    // Freshness check
    // -------------------------------------------------------------------------

    private boolean isCrossGraphFresh(Path stamp, List<String> projectNames) {
        if (!Files.exists(stamp)) return false;
        try {
            long crossMtime = Files.getLastModifiedTime(stamp).toMillis();
            for (String name : projectNames) {
                Optional<ProjectMeta> opt = registry.find(name);
                if (opt.isEmpty()) return false;
                ProjectMeta m = opt.get();

                if (m.getLastIndexed() == null) return false;
                if (m.getLastIndexed().toEpochMilli() > crossMtime) return false;

                if (m.getIndexPath() != null) {
                    Path graphStamp = Path.of(m.getIndexPath()).resolve("graph.stamp");
                    if (Files.exists(graphStamp)
                            && Files.getLastModifiedTime(graphStamp).toMillis() > crossMtime) {
                        return false;
                    }
                    Path refsFile = Path.of(m.getIndexPath()).resolve("unresolved-refs.json");
                    if (Files.exists(refsFile)
                            && Files.getLastModifiedTime(refsFile).toMillis() > crossMtime) {
                        return false;
                    }
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // -------------------------------------------------------------------------
    // Method-name index
    // -------------------------------------------------------------------------

    static class GraphIndex {
        private final Map<String, List<String>> byName;

        GraphIndex(CallGraph graph) {
            Map<String, List<String>> map = new HashMap<>();
            graph.allFqns().forEach(fqn ->
                    map.computeIfAbsent(simpleMethodName(fqn), k -> new ArrayList<>()).add(fqn));
            this.byName = map;
        }

        List<String> byMethodName(String name) {
            return byName.getOrDefault(name, List.of());
        }
    }

    // -------------------------------------------------------------------------
    // Reference resolution
    // -------------------------------------------------------------------------

    private String findInIndex(ProjectMeta.UnresolvedRef ref, GraphIndex index) {
        List<String> byName = index.byMethodName(ref.calleeMethodName);
        if (byName.isEmpty()) return null;

        List<String> candidates = byName;
        if (ref.packageHint != null) {
            List<String> inPackage = byName.stream()
                    .filter(fqn -> extractPackage(fqn).startsWith(ref.packageHint))
                    .collect(Collectors.toList());
            if (!inPackage.isEmpty()) candidates = inPackage;
        }

        return candidates.stream()
                .max(Comparator
                        .comparingInt((String fqn) -> softScore(fqn, ref))
                        .thenComparingInt(fqn -> -fqn.length()))
                .orElse(null);
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    public static void touchStamp(Path stampFile) {
        try {
            if (Files.exists(stampFile)) {
                Files.setLastModifiedTime(stampFile, FileTime.fromMillis(System.currentTimeMillis()));
            } else {
                Files.createDirectories(stampFile.getParent());
                Files.createFile(stampFile);
            }
        } catch (IOException e) {
            // non-fatal: freshness check will just rebuild next time
        }
    }

    private static String simpleMethodName(String fqn) {
        int hash = fqn.indexOf('#');
        if (hash < 0) return fqn;
        String methodPart = fqn.substring(hash + 1);
        int paren = methodPart.indexOf('(');
        return paren > 0 ? methodPart.substring(0, paren) : methodPart;
    }

    private static String extractPackage(String fqn) {
        int hash = fqn.indexOf('#');
        String qualifiedClass = hash > 0 ? fqn.substring(0, hash) : fqn;
        int dot = qualifiedClass.lastIndexOf('.');
        return dot > 0 ? qualifiedClass.substring(0, dot) : "";
    }

    private static String extractClassName(String fqn) {
        int hash = fqn.indexOf('#');
        String qualifiedClass = hash > 0 ? fqn.substring(0, hash) : fqn;
        int dot = qualifiedClass.lastIndexOf('.');
        return dot > 0 ? qualifiedClass.substring(dot + 1) : qualifiedClass;
    }

    private static int extractParamCount(String fqn) {
        int lp = fqn.indexOf('(');
        int rp = fqn.lastIndexOf(')');
        if (lp < 0 || rp <= lp + 1) return 0;
        String params = fqn.substring(lp + 1, rp).trim();
        if (params.isEmpty()) return 0;
        int count = 1, depth = 0;
        for (char c : params.toCharArray()) {
            if      (c == '<') depth++;
            else if (c == '>') depth--;
            else if (c == ',' && depth == 0) count++;
        }
        return count;
    }

    private static int softScore(String fqn, ProjectMeta.UnresolvedRef ref) {
        int score = 0;
        if (ref.receiverTypeName != null && extractClassName(fqn).equals(ref.receiverTypeName))
            score += 2;
        if (ref.paramCount > 0 && extractParamCount(fqn) == ref.paramCount)
            score += 1;
        return score;
    }
}
