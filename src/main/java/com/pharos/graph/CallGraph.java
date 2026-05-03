package com.pharos.graph;

import org.jgrapht.Graph;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedPseudograph;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Method call graph using JGraphT DirectedPseudograph.
 *
 * Nodes: fully qualified method IDs — "com.example.MyClass#myMethod(String,int)"
 * Edges: directed CALLS edge from A to B means "method A calls method B"
 *
 * DirectedPseudograph (vs DirectedGraph) allows multiple edges between the same
 * two vertices — a method can call another method multiple times at different lines.
 */
public class CallGraph {

    private final DirectedPseudograph<String, DefaultEdge> graph;

    public CallGraph() {
        this.graph = new DirectedPseudograph<>(DefaultEdge.class);
    }

    /** Add a method node (idempotent). */
    public void addMethod(String fqn) {
        graph.addVertex(fqn);
    }

    /** Add a CALLS edge from callerFqn to calleeFqn. */
    public void addCall(String callerFqn, String calleeFqn) {
        graph.addVertex(callerFqn);
        graph.addVertex(calleeFqn);
        graph.addEdge(callerFqn, calleeFqn);
    }

    /** Returns all methods that call the given FQN (in-edges). */
    public Set<String> getCallers(String fqn) {
        if (!graph.containsVertex(fqn)) return Set.of();
        return graph.incomingEdgesOf(fqn).stream()
                .map(graph::getEdgeSource)
                .collect(Collectors.toSet());
    }

    /** Returns all methods called by the given FQN (out-edges). */
    public Set<String> getCallees(String fqn) {
        if (!graph.containsVertex(fqn)) return Set.of();
        return graph.outgoingEdgesOf(fqn).stream()
                .map(graph::getEdgeTarget)
                .collect(Collectors.toSet());
    }

    /**
     * Finds the shortest call path between two methods using Dijkstra.
     * Returns empty list if no path exists.
     */
    public List<String> findPath(String fromFqn, String toFqn) {
        if (!graph.containsVertex(fromFqn) || !graph.containsVertex(toFqn)) {
            return List.of();
        }
        DijkstraShortestPath<String, DefaultEdge> dijkstra = new DijkstraShortestPath<>(graph);
        var path = dijkstra.getPath(fromFqn, toFqn);
        return path == null ? List.of() : path.getVertexList();
    }

    /** Returns true if the graph contains this FQN. */
    public boolean contains(String fqn) {
        return graph.containsVertex(fqn);
    }

    public int nodeCount() {
        return graph.vertexSet().size();
    }

    public int edgeCount() {
        return graph.edgeSet().size();
    }

    /** Returns the underlying JGraphT graph for serialization. */
    public Graph<String, DefaultEdge> getInternalGraph() {
        return graph;
    }

    /** Returns all method FQNs in the graph. */
    public Set<String> allMethods() {
        return graph.vertexSet();
    }
}
