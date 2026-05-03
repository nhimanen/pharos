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

/**
 * Builds and maintains the module-level dependency graph.
 *
 * The graph is stored at {@code ~/.pharos/module-graph.graphml} and is
 * automatically updated whenever a project is indexed.
 *
 * External nodes (Maven dependencies without indexed source) live only in this
 * graph — they are never written to {@link ProjectRegistry}.
 *
 * This class is thread-safe via synchronized methods on the load-modify-save cycle.
 */
public class ModuleGraphBuilder {

    private static final Logger log = LoggerFactory.getLogger(ModuleGraphBuilder.class);

    static final Path GRAPH_FILE =
            IndexConfig.DEFAULT_BASE.resolve("module-graph.graphml");

    private final ModuleGraphSerializer serializer;
    private final ProjectRegistry registry;

    public ModuleGraphBuilder(ProjectRegistry registry) {
        this.registry   = registry;
        this.serializer = new ModuleGraphSerializer();
    }

    /** Load the persisted module graph (empty graph if file does not exist yet). */
    public ModuleGraph load() throws IOException {
        return serializer.load(GRAPH_FILE);
    }

    /** Atomically persist the module graph. */
    public void save(ModuleGraph graph) throws IOException {
        serializer.save(graph, GRAPH_FILE);
    }

    /**
     * Incorporate a newly-indexed project into the module graph.
     *
     * Steps:
     *  1. Load the existing graph.
     *  2. Add/upgrade the project's own node to INDEXED.
     *  3. For each dependency in pomInfo: add an EXTERNAL node if not already present
     *     (leave existing INDEXED nodes untouched).
     *  4. Add dependency edges (deduplicated by scope).
     *  5. Auto-upgrade any EXTERNAL nodes whose moduleKey matches an already-indexed project.
     *  6. Save the graph.
     *  7. Update ProjectMeta with Maven coordinates and re-register.
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

        ModuleGraph graph = load();
        MavenPomReader.MavenCoordinates coords = pomInfo.coordinates();

        // Step 2: register this project as INDEXED
        ModuleNode self = ModuleNode.indexed(
                coords.groupId(), coords.artifactId(), coords.version(), meta.getName());
        ModuleNode canonicalSelf = graph.addOrUpdate(self);

        // Step 3 & 4: add dependency nodes and edges
        for (MavenDependency dep : pomInfo.dependencies()) {
            ModuleNode depNode = graph.findByKey(dep.moduleKey());
            if (depNode == null) {
                depNode = graph.addOrUpdate(
                        ModuleNode.external(dep.groupId(), dep.artifactId(), dep.version()));
            }
            // depNode may already be INDEXED — that's fine, addDependency just adds an edge
            graph.addDependency(canonicalSelf, depNode,
                    ModuleDep.of(dep.effectiveScope(), dep.version()));
        }

        // Step 5: auto-upgrade EXTERNAL nodes that match already-indexed projects
        List<String> autoLinked = autoLink(graph);

        // Step 6: persist
        save(graph);

        // Step 7: store Maven coordinates in ProjectMeta
        meta.setGroupId(coords.groupId());
        meta.setArtifactId(coords.artifactId());
        meta.setMavenVersion(coords.version());
        registry.register(meta);

        log.info("Module graph updated for '{}' ({}): {} total nodes, auto-linked: {}",
                meta.getName(), coords.moduleKey(), graph.nodeCount(), autoLinked);
        return autoLinked;
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
            ModuleNode node = graph.findByKey(key);
            if (node != null && !node.isIndexed()) {
                node.upgrade(p.getMavenVersion() != null ? p.getMavenVersion() : "unknown",
                        p.getName());
                linked.add(p.getName());
                log.info("Auto-upgraded EXTERNAL node '{}' to INDEXED (project '{}')",
                        key, p.getName());
            }
        }
        return linked;
    }
}
