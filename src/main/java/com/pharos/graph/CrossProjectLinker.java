package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Links call graphs across projects by resolving previously-unresolved call references.
 *
 * When project A calls a method from project B (an external dependency),
 * JavaParser cannot resolve the type during A's indexing pass.
 * This linker runs after both projects are indexed and linked via {@code pharos link},
 * matching unresolved calls in A against known packages in B.
 *
 * <h3>Performance design</h3>
 * <ul>
 *   <li><b>Method-name index ({@link GraphIndex})</b> — each loaded call graph is indexed
 *       by simple method name once.  Per-ref lookup drops from O(M) linear scan to O(1).</li>
 *   <li><b>Parallel pair resolution</b> — (i, j) project pairs are resolved concurrently;
 *       each pair is independent so no synchronisation is needed until merge.</li>
 *   <li><b>Freshness guard</b> — if the cross-project graph file is newer than the
 *       {@code lastIndexed} timestamp of every involved project, the rebuild is skipped
 *       entirely and the cached file is returned.</li>
 *   <li><b>Save guard</b> — the cross-project graph is only re-serialised when at least
 *       one new edge was resolved; a deletion-only incremental run never touches the file.</li>
 * </ul>
 */
public class CrossProjectLinker {

    private static final Logger log = LoggerFactory.getLogger(CrossProjectLinker.class);

    private final ProjectRegistry registry;
    private final CallGraphSerializer serializer;

    public CrossProjectLinker(IndexConfig config, ProjectRegistry registry) {
        this.registry = registry;
        this.serializer = new CallGraphSerializer();
    }

    /**
     * Build a merged cross-project call graph by combining graphs from
     * all mutually-linked projects and resolving cross-project call edges.
     */
    public CallGraph buildCrossProjectGraph(String project1, String project2) throws IOException {
        return buildCrossProjectGraph(List.of(project1, project2));
    }

    /**
     * Build a merged cross-project call graph across all given projects.
     * Resolves unresolved refs in each project against all other projects.
     * All projects must already be registered and indexed.
     *
     * @param projectNames names of projects to link (must be ≥ 2)
     */
    public CallGraph buildCrossProjectGraph(List<String> projectNames) throws IOException {
        if (projectNames.size() < 2) {
            throw new IllegalArgumentException("Need at least 2 projects to link");
        }

        Path crossGraphPath = IndexConfig.DEFAULT_BASE.resolve("cross-project-graph.graphml");

        // Freshness guard (#5): if the cross-project graph was written after every
        // involved project's last index, the existing file is already up to date.
        if (Files.exists(crossGraphPath) && isCrossGraphFresh(crossGraphPath, projectNames)) {
            log.info("Cross-project graph is already up to date for {}, skipping rebuild",
                    projectNames);
            return serializer.load(crossGraphPath);
        }

        // --- Load per-project graphs and build lookup indices (#1) ---
        List<ProjectMeta> metas = new ArrayList<>();
        Map<String, CallGraph> graphs = new LinkedHashMap<>();
        CallGraph merged = new CallGraph();

        for (String name : projectNames) {
            ProjectMeta meta = registry.find(name)
                    .orElseThrow(() -> new IllegalArgumentException("Project not found: " + name));
            metas.add(meta);
            Path graphPath = Path.of(meta.getIndexPath()).resolve("graph.graphml");
            if (Files.exists(graphPath)) {
                CallGraph g = serializer.load(graphPath);
                graphs.put(name, g);
                mergeInto(merged, g);
            }
        }

        // Build O(1)-lookup indices from each loaded graph (#1)
        Map<String, GraphIndex> indices = new LinkedHashMap<>();
        for (Map.Entry<String, CallGraph> e : graphs.entrySet()) {
            indices.put(e.getKey(), new GraphIndex(e.getValue()));
        }

        // --- Parallel project-pair resolution (#6) ---
        // Each (i,j) pair is independent: source project i resolved against target j.
        // Workers collect their edges locally; we merge into the graph serially at the end.
        List<int[]> pairs = new ArrayList<>();
        for (int i = 0; i < metas.size(); i++) {
            for (int j = 0; j < metas.size(); j++) {
                if (i != j) pairs.add(new int[]{i, j});
            }
        }

        int threads = Math.min(pairs.size(),
                Math.max(1, Runtime.getRuntime().availableProcessors()));
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
        int resolved = 0;
        try {
            for (Future<List<String[]>> f : futures) {
                for (String[] edge : f.get()) {
                    merged.addCall(edge[0], edge[1]);
                    resolved++;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Cross-project linking interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Cross-project linking failed: " + e.getCause().getMessage(),
                    e.getCause());
        }

        log.info("Cross-project linking: {} new edges across {} projects",
                resolved, projectNames.size());

        // Save guard (#4): only rewrite the file when something actually changed
        if (resolved > 0 || !Files.exists(crossGraphPath)) {
            serializer.save(merged, crossGraphPath);
        }

        return merged;
    }

    /**
     * Loads the cross-project graph (previously built via link command).
     * Falls back to an empty graph if not yet built.
     */
    public CallGraph loadCrossProjectGraph() throws IOException {
        Path crossGraphPath = IndexConfig.DEFAULT_BASE.resolve("cross-project-graph.graphml");
        if (!Files.exists(crossGraphPath)) {
            return new CallGraph();
        }
        return serializer.load(crossGraphPath);
    }

    // -------------------------------------------------------------------------
    // Freshness check (#5)
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} when the existing cross-project graph file is newer than
     * all relevant input artefacts for every involved project:
     * <ul>
     *   <li>{@code lastIndexed} timestamp stored in the registry</li>
     *   <li>The project's own {@code graph.graphml} file</li>
     *   <li>The project's {@code unresolved-refs.json} file (written by
     *       {@link ProjectRegistry#saveRefs} whenever refs change)</li>
     * </ul>
     * Checking the files directly means that a refs update without a full re-index
     * (e.g. a manual {@code registry.register()} call) is still detected correctly.
     */
    private boolean isCrossGraphFresh(Path crossGraphPath, List<String> projectNames) {
        try {
            long crossMtime = Files.getLastModifiedTime(crossGraphPath).toMillis();
            for (String name : projectNames) {
                Optional<ProjectMeta> opt = registry.find(name);
                if (opt.isEmpty()) return false;
                ProjectMeta m = opt.get();

                // lastIndexed in registry
                if (m.getLastIndexed() == null) return false;
                if (m.getLastIndexed().toEpochMilli() > crossMtime) return false;

                if (m.getIndexPath() != null) {
                    // project call graph file
                    Path graphFile = Path.of(m.getIndexPath()).resolve("graph.graphml");
                    if (Files.exists(graphFile)
                            && Files.getLastModifiedTime(graphFile).toMillis() > crossMtime) {
                        return false;
                    }
                    // unresolved-refs file (touched any time refs change)
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
    // Method-name index (#1)
    // -------------------------------------------------------------------------

    /**
     * Per-graph lookup structure built once after loading.
     * Eliminates the O(M) linear scan in the original {@code findInGraph} —
     * name lookups are now O(1) map gets.
     */
    static class GraphIndex {
        /** Simple method name → all FQNs in this graph that have that name. */
        private final Map<String, List<String>> byName;

        GraphIndex(CallGraph graph) {
            Map<String, List<String>> map = new HashMap<>();
            for (String fqn : graph.allMethods()) {
                map.computeIfAbsent(simpleMethodName(fqn), k -> new ArrayList<>()).add(fqn);
            }
            this.byName = map;
        }

        List<String> byMethodName(String name) {
            return byName.getOrDefault(name, List.of());
        }
    }

    // -------------------------------------------------------------------------
    // Reference resolution (uses GraphIndex)
    // -------------------------------------------------------------------------

    /**
     * Find the best-matching method in the target project's index for the given ref.
     *
     * <p>Matching strategy:
     * <ol>
     *   <li>Name lookup: O(1) map get via {@link GraphIndex} (was O(M) stream scan).</li>
     *   <li>Package filter (hard): if a {@code packageHint} is available, keep only
     *       candidates whose class package starts with that hint; fall back to the full
     *       name-match list when nothing survives.</li>
     *   <li>Soft score: +2 for class name match, +1 for param count match.</li>
     *   <li>Tie-break: shortest FQN.</li>
     * </ol>
     */
    private String findInIndex(ProjectMeta.UnresolvedRef ref, GraphIndex index) {
        // 1. O(1) name lookup
        List<String> byName = index.byMethodName(ref.calleeMethodName);
        if (byName.isEmpty()) return null;

        // 2. Package filter (hard, with fallback)
        List<String> candidates = byName;
        if (ref.packageHint != null) {
            List<String> inPackage = byName.stream()
                    .filter(fqn -> extractPackage(fqn).startsWith(ref.packageHint))
                    .collect(Collectors.toList());
            if (!inPackage.isEmpty()) candidates = inPackage;
        }

        // 3. Soft score + tie-break by FQN length
        return candidates.stream()
                .max(Comparator
                        .comparingInt((String fqn) -> softScore(fqn, ref))
                        .thenComparingInt(fqn -> -fqn.length()))
                .orElse(null);
    }

    // -------------------------------------------------------------------------
    // FQN utilities
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Graph merge
    // -------------------------------------------------------------------------

    private void mergeInto(CallGraph target, CallGraph source) {
        source.allMethods().forEach(target::addMethod);
        source.getInternalGraph().edgeSet().forEach(edge -> {
            String src = source.getInternalGraph().getEdgeSource(edge);
            String tgt = source.getInternalGraph().getEdgeTarget(edge);
            target.addCall(src, tgt);
        });
    }
}
