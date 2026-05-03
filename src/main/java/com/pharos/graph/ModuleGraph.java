package com.pharos.graph;

import org.jgrapht.Graph;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DirectedMultigraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Module-level dependency graph.
 *
 * Nodes: {@link ModuleNode} identified by moduleKey = "groupId:artifactId"
 * Edges: {@link ModuleDep} (directed, source DEPENDS ON target)
 *
 * {@code DirectedMultigraph} allows multiple edges between the same pair of nodes
 * (e.g., a project depending on another via both compile and test scopes),
 * while still deduplicating nodes by {@link ModuleNode#equals}.
 *
 * Stored at {@code ~/.pharos/module-graph.graphml}.
 */
public class ModuleGraph {

    private final DirectedMultigraph<ModuleNode, ModuleDep> graph;

    // Secondary index: moduleKey → node for O(1) lookup without graph scan
    private final Map<String, ModuleNode> nodeIndex;

    public ModuleGraph() {
        this.graph     = new DirectedMultigraph<>(ModuleDep.class);
        this.nodeIndex = new LinkedHashMap<>();
    }

    /**
     * Add a module node, deduplicating by moduleKey.
     * If a node with the same key already exists and the incoming node is INDEXED,
     * the existing node is upgraded in place.
     *
     * @return the canonical node held by this graph (always use the returned value)
     */
    public ModuleNode addOrUpdate(ModuleNode node) {
        ModuleNode existing = nodeIndex.get(node.getModuleKey());
        if (existing != null) {
            if (node.isIndexed() && !existing.isIndexed()) {
                existing.upgrade(node.getVersion(), node.getProjectName());
            }
            return existing;
        }
        graph.addVertex(node);
        nodeIndex.put(node.getModuleKey(), node);
        return node;
    }

    /**
     * Add a dependency edge from {@code from} to {@code to}.
     * Both nodes must already be present (call {@link #addOrUpdate} first).
     * Deduplicates by scope: skips if an edge of the same scope already exists between the pair.
     */
    public void addDependency(ModuleNode from, ModuleNode to, ModuleDep dep) {
        if (!graph.containsVertex(from) || !graph.containsVertex(to)) return;
        boolean alreadyExists = graph.getAllEdges(from, to).stream()
                .anyMatch(e -> e.getScope().equals(dep.getScope()));
        if (!alreadyExists) {
            graph.addEdge(from, to, dep);
        }
    }

    /** Find a node by moduleKey ("groupId:artifactId"). Returns null if absent. */
    public ModuleNode findByKey(String moduleKey) {
        return nodeIndex.get(moduleKey);
    }

    /** Find an INDEXED node by its registry project name. */
    public Optional<ModuleNode> findByProjectName(String projectName) {
        return nodeIndex.values().stream()
                .filter(n -> projectName.equals(n.getProjectName()))
                .findFirst();
    }

    /** All direct dependencies of a node (outgoing edges → targets). */
    public Set<ModuleNode> getDependencies(ModuleNode node) {
        if (!graph.containsVertex(node)) return Set.of();
        return graph.outgoingEdgesOf(node).stream()
                .map(graph::getEdgeTarget)
                .collect(Collectors.toSet());
    }

    /** All modules that depend on this node (incoming edges → sources). */
    public Set<ModuleNode> getDependents(ModuleNode node) {
        if (!graph.containsVertex(node)) return Set.of();
        return graph.incomingEdgesOf(node).stream()
                .map(graph::getEdgeSource)
                .collect(Collectors.toSet());
    }

    /** All edges between two nodes (multiple scopes possible). */
    public Set<ModuleDep> getEdges(ModuleNode from, ModuleNode to) {
        if (!graph.containsVertex(from) || !graph.containsVertex(to)) return Set.of();
        return graph.getAllEdges(from, to);
    }

    /**
     * Shortest dependency path between two modules using Dijkstra.
     * Returns an empty list if no path exists or either node is absent.
     */
    public List<ModuleNode> findPath(String fromKey, String toKey) {
        ModuleNode from = nodeIndex.get(fromKey);
        ModuleNode to   = nodeIndex.get(toKey);
        if (from == null || to == null) return List.of();

        var pathResult = new DijkstraShortestPath<>(graph).getPath(from, to);
        return pathResult == null ? List.of() : pathResult.getVertexList();
    }

    public Collection<ModuleNode> allNodes() { return Collections.unmodifiableCollection(nodeIndex.values()); }
    public int nodeCount()                    { return graph.vertexSet().size(); }
    public int edgeCount()                    { return graph.edgeSet().size(); }

    /** Returns the underlying JGraphT graph — used by the serializer only. */
    public Graph<ModuleNode, ModuleDep> getInternalGraph() { return graph; }
}
