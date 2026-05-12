package com.pharos.graph;

import com.pharos.config.IndexConfig;
import com.pharos.config.ProjectMeta;
import com.pharos.config.ProjectRegistry;
import com.pharos.parser.MavenPomReader;
import com.pharos.parser.MavenPomReader.MavenDependency;
import com.pharos.parser.MavenPomReader.PomInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Builds and maintains the module-level dependency graph (ArcadeDB-backed).
 *
 * The graph is stored at {@code ~/.pharos/module-graph.arcadedb/} and is
 * automatically updated whenever a project is indexed.
 *
 * External nodes (Maven dependencies without indexed source) live only in this
 * graph — they are never written to {@link ProjectRegistry}.
 *
 * This class is thread-safe: each mutating method opens, modifies and closes
 * the database within a synchronized block.
 */
public class ModuleGraphBuilder {

    private static final Logger log = LoggerFactory.getLogger(ModuleGraphBuilder.class);

    static final Path DB_DIR = IndexConfig.DEFAULT_BASE.resolve("module-graph.arcadedb");

    private final ProjectRegistry registry;

    public ModuleGraphBuilder(ProjectRegistry registry) {
        this.registry = registry;
    }

    /** Open the module graph (creates the DB if it does not exist yet). */
    public ModuleGraph open() {
        return ModuleGraph.open(DB_DIR);
    }

    /**
     * Incorporate a newly-indexed project into the module graph.
     *
     * Steps:
     *  1. Open the ArcadeDB graph.
     *  2. Add/upgrade the project's own node to INDEXED.
     *  3. For each dependency in pomInfo: add an EXTERNAL node if not already present.
     *  4. Add dependency edges (deduplicated by scope).
     *  5. Auto-upgrade any EXTERNAL nodes whose moduleKey matches an already-indexed project.
     *  6. Update ProjectMeta with Maven coordinates and re-register.
     *
     * @param projectRoot absolute project root path (used only for logging)
     * @param meta        the ProjectMeta that was just written to the registry
     * @param pomInfo     parsed pom.xml; call is a no-op if null
     * @return project names that were auto-upgraded from EXTERNAL to INDEXED
     */
    public synchronized List<String> incorporate(Path projectRoot, ProjectMeta meta,
                                                  PomInfo pomInfo) throws IOException {
        if (pomInfo == null) {
            log.debug("No pom.xml for '{}', skipping module graph update", meta.getName());
            return List.of();
        }

        MavenPomReader.MavenCoordinates coords = pomInfo.coordinates();
        String ownKey = coords.moduleKey();

        try (ModuleGraph graph = open()) {
            // Register this project as INDEXED
            graph.addOrUpdate(
                    ownKey,
                    coords.groupId(),
                    coords.artifactId(),
                    coords.version(),
                    "INDEXED",
                    meta.getName());

            // Add dependency nodes and edges
            for (MavenDependency dep : pomInfo.dependencies()) {
                Optional<ModuleNodeData> existing = graph.findByKey(dep.moduleKey());
                if (existing.isEmpty()) {
                    graph.addOrUpdate(
                            dep.moduleKey(), dep.groupId(), dep.artifactId(),
                            dep.version(), "EXTERNAL", null);
                }
                graph.addDependency(ownKey, dep.moduleKey(),
                        dep.effectiveScope(), dep.version());
            }

            // Auto-upgrade EXTERNAL nodes that match already-indexed projects
            List<String> autoLinked = autoLink(graph);

            log.info("Module graph updated for '{}' ({}): {} total nodes, auto-linked: {}",
                    meta.getName(), ownKey, graph.moduleCount(), autoLinked);

            // Store Maven coordinates in ProjectMeta
            meta.setGroupId(coords.groupId());
            meta.setArtifactId(coords.artifactId());
            meta.setMavenVersion(coords.version());
            registry.register(meta);

            return autoLinked;
        }
    }

    /**
     * Scan all EXTERNAL nodes against the registry.
     * When a match is found (same groupId:artifactId), upgrade the node to INDEXED.
     *
     * @return project names of newly-upgraded nodes
     */
    public List<String> autoLink(ModuleGraph graph) {
        List<String> linked = new ArrayList<>();
        for (ProjectMeta p : registry.listAll()) {
            if (p.getGroupId() == null || p.getArtifactId() == null) continue;
            String key = p.getGroupId() + ":" + p.getArtifactId();
            Optional<ModuleNodeData> nodeOpt = graph.findByKey(key);
            if (nodeOpt.isPresent() && !nodeOpt.get().isIndexed()) {
                String version = p.getMavenVersion() != null ? p.getMavenVersion() : "unknown";
                graph.upgradeToIndexed(key, version, p.getName());
                linked.add(p.getName());
                log.info("Auto-upgraded EXTERNAL node '{}' to INDEXED (project '{}')",
                        key, p.getName());
            }
        }
        return linked;
    }
}
